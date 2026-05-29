import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from database.connection import get_db
from database.crud import (
    save_changes, save_changes_no_serial, save_changes_mid,
    delete_historical_prices, insert_prices_from_transactions, normalize_investment_prices,
)
from database.queries import get_price_anomalies, get_investments_with_dummy_prices
from ui.components import copy_df_button
from data.downloaders import (
    download_historical_fx,
    download_historical_prices_from_tradingview,
    download_historical_prices_from_yahoo,
    download_bond_prices_from_solidus,
    download_securities_info_from_yahoo,
    download_securities_info_from_tradingview,
    download_dividend_history,
)


# ── Cached reference-data loaders ─────────────────────────────────────────────

@st.cache_data(ttl=600)
def _md_load_currencies():
    with get_db() as conn:
        return pd.read_sql(
            "SELECT Currencies_Id, Currencies_ShortName FROM Currencies ORDER BY Currencies_ShortName ASC", conn)

@st.cache_data(ttl=600)
def _md_load_securities_list():
    with get_db() as conn:
        return pd.read_sql(
            "SELECT Securities_Id, Securities_Name FROM Securities ORDER BY Securities_Name ASC", conn)

@st.cache_data(ttl=3600)
def _md_load_credit_ratings():
    with get_db() as conn:
        return pd.read_sql(
            "SELECT Moodys, S_P, Fitch FROM Credit_Ratings_LT ORDER BY Credit_Ratings_LT_Id ASC", conn)

@st.cache_data(ttl=120)
def _md_load_securities_with_price_stats():
    """Securities with price count and latest price date — for the security selector dropdown."""
    with get_db() as conn:
        return pd.read_sql("""
            SELECT s.Securities_Id, s.Securities_Name,
                   s.Yahoo_Ticker, s.TV_Symbol, s.TV_Exchange, s.ISIN,
                   COALESCE(hp.NoOfRecords, 0)          AS NoOfRecords,
                   COALESCE(hp.MaxDate, '1900-01-01')   AS MaxDate
            FROM Securities s
            LEFT JOIN (
                SELECT Securities_Id,
                       COUNT(*)   AS NoOfRecords,
                       MAX(Date)  AS MaxDate
                FROM   Historical_Prices
                GROUP  BY Securities_Id
            ) hp ON hp.Securities_Id = s.Securities_Id
            ORDER BY s.Securities_Name ASC
        """, conn)


def render_market_data():
    """Render the Market Data page.

    Tab 1 — Currencies: currency master data editor + historical FX rates.
    Tab 2 — Securities: security master data editor + per-security price history,
             price anomaly detection, and dummy-price normalization.
    """
    st.title("Market Data")
    t_curr, t_sec = st.tabs(["💱 Currencies", "📈 Securities"])

    # ── Shared lookups (all cached) ───────────────────────────────────────────
    df_curr_list = _md_load_currencies()
    df_sec_list  = _md_load_securities_list()
    df_ratings   = _md_load_credit_ratings()

    curr_options   = df_curr_list.set_index('currencies_id')['currencies_shortname'].to_dict()
    sec_options    = df_sec_list.set_index('securities_id')['securities_name'].to_dict()
    moodys_options = dict(zip(df_ratings['moodys'], df_ratings['moodys']))
    s_p_options    = dict(zip(df_ratings['s_p'],    df_ratings['s_p']))
    fitch_options  = dict(zip(df_ratings['fitch'],  df_ratings['fitch']))

    period_options = {
        "1 Day": "1d", "5 Days": "5d", "1 Month": "1mo", "3 Months": "3mo",
        "6 Months": "6mo", "1 Year": "1y", "3 Years": "3y", "5 Years": "5y",
        "10 Years": "10y", "15 Years": "15y", "20 Years": "20y",
        "25 Years": "25y", "30 Years": "30y",
    }

    # =========================================================================
    # TAB 1 — CURRENCIES
    # =========================================================================
    with t_curr:

        # ── Currency master data editor ───────────────────────────────────────
        st.subheader("🪙 Currency Master Data")
        with get_db() as conn:
            df_curr = pd.read_sql("SELECT * FROM Currencies ORDER BY Currencies_ShortName ASC", conn)
        edited_curr = st.data_editor(
            df_curr,
            num_rows="dynamic",
            key="mkt_curr",
            column_config={
                "currencies_id":        None,
                "currencies_shortname": st.column_config.TextColumn("Currency ISO Code", width="small"),
                "currencies_name":      st.column_config.TextColumn("Currency Name",     width="large"),
                "embedding":            None,
            },
        )
        if not edited_curr.equals(df_curr):
            save_changes(df_curr, edited_curr, "Currencies", "currencies_id")
            _md_load_currencies.clear()

        st.divider()

        # ── FX Rates ──────────────────────────────────────────────────────────
        st.subheader("💱 Historical FX Rates")
        with get_db() as conn:
            df_fx = pd.read_sql(
                "SELECT * FROM Historical_FX ORDER BY Date DESC, Currencies_Id_1 ASC", conn)
        edited_hfx = st.data_editor(
            df_fx,
            num_rows="dynamic",
            key="mkt_hfx",
            column_config={
                "currencies_id_1": st.column_config.SelectboxColumn(
                    "Base Currency",
                    options=list(curr_options.keys()),
                    format_func=lambda x: curr_options.get(x, "Unknown"),
                ),
                "currencies_id_2": st.column_config.SelectboxColumn(
                    "Target Currency",
                    options=list(curr_options.keys()),
                    format_func=lambda x: curr_options.get(x, "Unknown"),
                ),
            },
        )
        save_changes_no_serial(df_fx, edited_hfx, "Historical_FX", "date")
        copy_df_button(df_fx, key="mkt_dl_fx")

        # _sel_ccy_id / _sel_ccy_name track the currency of the pair shown in the
        # chart so the single-pair download button can reference it.
        _sel_ccy_id   = None
        _sel_ccy_name = None

        if not df_fx.empty:
            st.subheader("📈 Exchange Rate Chart")
            df_plot = df_fx.copy()
            df_plot['Pair'] = df_plot.apply(
                lambda row: (
                    f"{curr_options.get(row['currencies_id_1'], '??')}"
                    f"/{curr_options.get(row['currencies_id_2'], '??')}"
                ),
                axis=1,
            )
            available_pairs = df_plot['Pair'].unique()
            selected_pair   = st.selectbox("Select pair to display:", available_pairs, key="mkt_fx_pair_select")
            chart_data      = df_plot[df_plot['Pair'] == selected_pair].sort_values('date')
            if not chart_data.empty:
                st.line_chart(
                    data=chart_data, x='date', y='fx_rate',
                    x_label="Date", y_label="Rate", width='stretch',
                )

            # Capture the base currency of the selected pair for the targeted download button
            _pair_rows = df_plot[df_plot['Pair'] == selected_pair]
            if not _pair_rows.empty:
                _sel_ccy_id   = int(_pair_rows['currencies_id_1'].iloc[0])
                _sel_ccy_name = selected_pair.split('/')[0]   # e.g. "USD"

        st.subheader("🔄 Update Exchange Rates")
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_label_fx = st.selectbox(
                "Select time period:", list(period_options.keys()), index=1, key="mkt_fx_period_select")
            ts_period_fx = period_options[selected_label_fx]
        with col2:
            if st.button("🚀 Download All Rates", key="mkt_download_all_fx_btn", width="stretch"):
                with st.spinner("Downloading rates for all currencies…"):
                    download_historical_fx(ts_period_fx)
                    st.balloons()
                    st.rerun()
            if _sel_ccy_id is not None:
                if st.button(
                        f"🚀 Download Rates for {_sel_ccy_name}",
                        key="mkt_download_one_fx_btn", width="stretch"):
                    with st.spinner(f"Downloading rates for {_sel_ccy_name}…"):
                        download_historical_fx(ts_period_fx, currencies_id=_sel_ccy_id)
                        st.balloons()
                        st.rerun()

    # =========================================================================
    # TAB 2 — SECURITIES
    # =========================================================================
    with t_sec:

        # ── Securities master data editor ─────────────────────────────────────
        st.subheader("📋 Securities Master Data")
        with get_db() as conn:
            df_sec = pd.read_sql("""
                SELECT s.Securities_Id, s.Ticker, s.Securities_Name, s.Securities_Type,
                       s.Currencies_Id, s.Sector, s.Industry, s.Analyst_Rating,
                       s.Analyst_Target_Price, s.Is_Active,
                       COALESCE(s.Is_Tax_Exempt, FALSE) AS Is_Tax_Exempt,
                       s.Yahoo_Ticker, s.TV_Symbol, s.TV_Exchange, s.ISIN,
                       s.Maturity_Date, s.Coupon_Rate, s.Face_Value, s.Coupon_Frequency,
                       s.Dividend_Yield, s.Dividend_Frequency, s.Ex_Dividend_Date,
                       s.embedding,
                       COALESCE(ic.cnt, 0) AS investment_count
                FROM Securities s
                LEFT JOIN (
                    SELECT Securities_Id, COUNT(*) AS cnt
                    FROM   Investments
                    GROUP  BY Securities_Id
                ) ic ON ic.Securities_Id = s.Securities_Id
                ORDER BY s.Securities_Name
            """, conn)

        _sec_sort_labels = {
            "ticker":             "Ticker",
            "securities_name":    "Security Name",
            "securities_type":    "Type",
            "investment_count":   "Investment Count",
            "dividend_yield":     "Div. Yield (%)",
            "dividend_frequency": "Div. Frequency",
            "ex_dividend_date":   "Ex-Div Date",
        }
        _c1, _c2 = st.columns([2, 1])
        with _c1:
            _sec_sort_col = st.selectbox(
                "Sort by", options=list(_sec_sort_labels.keys()),
                format_func=lambda x: _sec_sort_labels[x], index=1, key="mkt_sec_sort_col")
        with _c2:
            _sec_sort_asc = st.radio(
                "Direction", options=["Ascending", "Descending"],
                horizontal=True, key="mkt_sec_sort_dir") == "Ascending"

        df_sec = df_sec.sort_values(_sec_sort_col, ascending=_sec_sort_asc).reset_index(drop=True)

        edited_sec = st.data_editor(
            df_sec,
            num_rows="dynamic",
            key="mkt_set_sec",
            column_config={
                "securities_id":        None,
                "ticker":               st.column_config.TextColumn("Ticker Symbol",  width="medium", pinned=True),
                "securities_name":      st.column_config.TextColumn("Security Name",  width="medium", pinned=True),
                "securities_type":      st.column_config.SelectboxColumn("Type",
                    options=['Stock','ETF','Bond','CD','CFD','Closed-End Fund',
                             'Emp. Stock Opt.','FX Spot','Market Index','Mutual Fund',
                             'Crypto','Option','Commodity','PF_Unit','Other'],
                    width="small"),
                "currencies_id":        st.column_config.SelectboxColumn("Currency",
                    options=list(curr_options.keys()),
                    format_func=lambda x: curr_options.get(x, "Unknown"), width="small"),
                "sector":               st.column_config.TextColumn("Sector",           width="small"),
                "industry":             st.column_config.TextColumn("Industry",         width="small"),
                "analyst_rating":       st.column_config.TextColumn("Rating",           width="small"),
                "analyst_target_price": st.column_config.NumberColumn("Target Price",   width="auto", format="%,.2f"),
                "is_active":            st.column_config.CheckboxColumn("Is Active",    width="small"),
                "is_tax_exempt":        st.column_config.CheckboxColumn("Tax Exempt",   width="small",
                    help="Income (dividends, interest) from this security is exempt from "
                         "income tax — e.g. Hellenic T-Bills purchased at the primary market."),
                "yahoo_ticker":         st.column_config.TextColumn("Yahoo Ticker",     width="small"),
                "tv_symbol":            st.column_config.TextColumn("TV Symbol",        width="small"),
                "tv_exchange":          st.column_config.TextColumn("TV Exchange",      width="small"),
                "isin":                 st.column_config.TextColumn("ISIN",             width="small"),
                "maturity_date":        st.column_config.DateColumn("Maturity Date",    width="small"),
                "coupon_rate":          st.column_config.NumberColumn("Coupon Rate (%)",width="small", format="%.4f"),
                "face_value":           st.column_config.NumberColumn("Face Value",     width="small", format="%,.2f"),
                "coupon_frequency":     st.column_config.SelectboxColumn("Coupon Frequency",
                    options=["Annual","Semi-Annual","Quarterly","Monthly","At Maturity"], width="small"),
                "dividend_yield":       st.column_config.NumberColumn("Div. Yield (%)",   width="small", format="%.4f", disabled=True),
                "dividend_frequency":   st.column_config.TextColumn("Div. Frequency",     width="small", disabled=True),
                "ex_dividend_date":     st.column_config.DateColumn("Ex-Div Date",        width="small", disabled=True),
                "investment_count":     st.column_config.NumberColumn("Investment Count", width="small", disabled=True),
                "embedding":            None,
            },
        )
        _sec_computed   = ["investment_count", "dividend_yield", "dividend_frequency", "ex_dividend_date"]
        df_sec_save     = df_sec.drop(columns=[c for c in _sec_computed if c in df_sec.columns])
        edited_sec_save = edited_sec.drop(columns=[c for c in _sec_computed if c in edited_sec.columns])
        if not edited_sec_save.equals(df_sec_save):
            save_changes(df_sec_save, edited_sec_save, "Securities", "securities_id")
            _md_load_securities_list.clear()
            _md_load_securities_with_price_stats.clear()

        if st.button("🚀 Update Securities Information from Yahoo", key="mkt_download_sec_info", width="stretch"):
            with st.spinner("Fetching sector, industry, analyst rating, target price & dividend summary from Yahoo…"):
                download_securities_info_from_yahoo()
            _md_load_securities_list.clear()
            st.rerun()

        if st.button("📅 Download Dividend History from Yahoo", key="mkt_download_div_history", width="stretch"):
            with st.spinner("Fetching full dividend history & inferring frequency from Yahoo…"):
                download_dividend_history()
            _md_load_securities_list.clear()
            st.rerun()

        if st.button("🚀 Update Securities Information from TradingView", key="mkt_download_tv_info", width="stretch"):
            with st.spinner("Fetching sector, industry, rating & target price from TradingView…"):
                download_securities_info_from_tradingview(target_sec_id=None, overwrite=False)
            _md_load_securities_list.clear()
            st.rerun()

        st.divider()

        # ── Per-security selector ─────────────────────────────────────────────
        st.subheader("📊 Security Details")
        df_inv_secs = _md_load_securities_with_price_stats()

        if df_inv_secs.empty:
            st.warning("⚠️ No Securities found. Add one in the master data editor above.")
        else:
            records = df_inv_secs.to_dict('records')
            default_sec_idx = 0
            if "mkt_selected_sec_id" in st.session_state:
                for i, rec in enumerate(records):
                    if rec['securities_id'] == st.session_state.mkt_selected_sec_id:
                        default_sec_idx = i
                        break

            selected_inv_sec = st.selectbox(
                "Select Security:",
                records,
                index=default_sec_idx,
                format_func=lambda x: (
                    f"{x['securities_name']} "
                    f"({x['noofrecords']:,.0f} prices · last: {x['maxdate']})"
                ),
                key="mkt_security_select",
            )
            st.session_state.mkt_selected_sec_id = selected_inv_sec['securities_id']
            inv_sec_id = selected_inv_sec['securities_id']

            # ── Inner sub-tabs ────────────────────────────────────────────────
            st_prices, st_inv_txs, st_anomalies, st_dummy, st_divs = st.tabs([
                "📈 Prices",
                "🧾 Investment Transactions",
                "🔍 Price Anomalies",
                "⚖ Dummy Prices",
                "💰 Dividends",
            ])

            # ─────────────────────────────────────────────────────────────────
            # SUB-TAB: Prices
            # ─────────────────────────────────────────────────────────────────
            with st_prices:
                with get_db() as conn:
                    df_hpr_tx = pd.read_sql(
                        "SELECT Securities_Id, Date, Close, High, Low, Volume, embedding "
                        "FROM Historical_Prices "
                        f"WHERE Securities_Id = {inv_sec_id} ORDER BY Date DESC",
                        conn,
                    )
                edited_hpr_tx = st.data_editor(
                    df_hpr_tx,
                    num_rows="dynamic",
                    key=f"mkt_hpr_editor_{inv_sec_id}",
                    width="stretch",
                    column_config={
                        'securities_id': None,
                        'date':   st.column_config.DateColumn('Date', format="DD/MM/YYYY"),
                        'close':  st.column_config.NumberColumn('Close',  format="%,.8f"),
                        'high':   st.column_config.NumberColumn('High',   format="%,.8f"),
                        'low':    st.column_config.NumberColumn('Low',    format="%,.8f"),
                        'volume': st.column_config.NumberColumn('Volume', format="%,.0f"),
                        'embedding': None,
                    },
                )
                save_changes_mid(
                    edited_hpr_tx, "Historical_Prices",
                    id_cols=["securities_id", "date"],
                    filter_col="securities_id", filter_val=inv_sec_id,
                )
                copy_df_button(df_hpr_tx, key=f"mkt_dl_prices_{inv_sec_id}")

                if not df_hpr_tx.empty:
                    st.subheader("📈 Price Chart")
                    with get_db() as conn:
                        df_tx_marks = pd.read_sql(
                            "SELECT Date AS Transaction_Date, Action, Quantity "
                            "FROM Investments "
                            f"WHERE Securities_Id = {inv_sec_id} AND Action IN ('Buy','Sell')",
                            conn,
                        )
                    df_tx_marks['transaction_date'] = pd.to_datetime(df_tx_marks['transaction_date'])

                    c1, c2 = st.columns([1, 1])
                    with c1:
                        chart_view_options = {
                            "1 Week": 7, "1 Month": 30, "3 Months": 90, "6 Months": 180,
                            "YTD": "YTD", "1 Year": 365, "3 Years": 1095, "5 Years": 1825,
                            "All Time": None,
                        }
                        selected_view = st.selectbox(
                            "Chart Range:", list(chart_view_options.keys()),
                            index=8, key=f"mkt_chart_range_{inv_sec_id}",
                        )
                    with c2:
                        window_size = st.slider(
                            "Moving Average (MA) Days:", 2, 30, 5,
                            key=f"mkt_ma_{inv_sec_id}",
                        )

                    chart_data = df_hpr_tx.sort_values('date').copy()
                    chart_data['date'] = pd.to_datetime(chart_data['date'])
                    if chart_view_options[selected_view] is not None:
                        max_date = chart_data['date'].max()
                        if chart_view_options[selected_view] == "YTD":
                            start_date = pd.Timestamp(year=max_date.year, month=1, day=1)
                        else:
                            start_date = max_date - pd.Timedelta(days=chart_view_options[selected_view])
                        chart_data = chart_data[chart_data['date'] >= start_date]

                    chart_data = chart_data.merge(
                        df_tx_marks, left_on='date', right_on='transaction_date', how='left')
                    chart_data['MA'] = chart_data['close'].rolling(window=window_size).mean()

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=chart_data['date'], y=chart_data['close'],
                        mode='lines', name='Price', line=dict(color='blue', width=2),
                    ))
                    fig.add_trace(go.Scatter(
                        x=chart_data['date'], y=chart_data['MA'],
                        mode='lines', name=f'MA ({window_size}d)',
                        line=dict(color='red', width=1.5, dash='dot'),
                    ))
                    for t_type, t_color, t_symbol in [
                        ('Buy',  'green',  'triangle-up'),
                        ('Sell', 'orange', 'triangle-down'),
                    ]:
                        subset = chart_data[chart_data['action'] == t_type]
                        if not subset.empty:
                            fig.add_trace(go.Scatter(
                                x=subset['date'], y=subset['close'],
                                mode='markers', name=t_type,
                                marker=dict(color=t_color, size=12, symbol=t_symbol,
                                            line=dict(width=1, color='black')),
                                hovertemplate=(
                                    "Date: %{x}<br>Price: %{y}<br>"
                                    f"Action: {t_type}<br>Qty: %{{customdata}}"
                                ),
                                customdata=subset['quantity'],
                            ))
                    fig.update_layout(
                        margin=dict(l=0, r=0, t=0, b=0),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        hovermode="x unified", template="plotly_white", height=450,
                    )
                    st.plotly_chart(fig, width='stretch')

                st.subheader("🔄 Update Prices")
                col1, col2 = st.columns([2, 1])
                with col1:
                    period_list = list(period_options.keys())
                    default_period_idx = 1
                    if ("mkt_last_ts_period" in st.session_state
                            and st.session_state.mkt_last_ts_period in period_list):
                        default_period_idx = period_list.index(st.session_state.mkt_last_ts_period)
                    selected_label_price = st.selectbox(
                        "Select download period:", period_list,
                        index=default_period_idx, key="mkt_price_period_select",
                    )
                    st.session_state.mkt_last_ts_period = selected_label_price
                    ts_period_price = period_options[selected_label_price]
                with col2:
                    # pd.notna guards against NULL → NaN coming from the DB;
                    # bool(NaN) is True in Python, which would be a false positive.
                    _yh        = selected_inv_sec.get('yahoo_ticker')
                    _tv        = selected_inv_sec.get('tv_symbol')
                    _has_yahoo = bool(pd.notna(_yh) and _yh)
                    _has_tv    = bool(pd.notna(_tv) and _tv)

                    if st.button("🚀 Download All from Yahoo", key="mkt_dl_all_yahoo", width="stretch"):
                        download_historical_prices_from_yahoo(ts_period_price)
                        _md_load_securities_with_price_stats.clear()
                        st.rerun()
                    if _has_yahoo:
                        if st.button(
                                f"🚀 Update {selected_inv_sec['securities_name']} from Yahoo",
                                key="mkt_dl_one_yahoo", width="stretch"):
                            download_historical_prices_from_yahoo(ts_period_price, inv_sec_id)
                            _md_load_securities_with_price_stats.clear()
                            st.rerun()
                    else:
                        st.caption(f"ℹ️ No Yahoo ticker defined for {selected_inv_sec['securities_name']}")

                    if st.button("🚀 Download All from TradingView", key="mkt_dl_all_tv", width="stretch"):
                        download_historical_prices_from_tradingview(ts_period_price)
                        _md_load_securities_with_price_stats.clear()
                        st.rerun()
                    if _has_tv:
                        if st.button(
                                f"🚀 Update {selected_inv_sec['securities_name']} from TradingView",
                                key="mkt_dl_one_tv", width="stretch"):
                            download_historical_prices_from_tradingview(ts_period_price, inv_sec_id)
                            _md_load_securities_with_price_stats.clear()
                            st.rerun()
                    else:
                        st.caption(f"ℹ️ No TradingView symbol defined for {selected_inv_sec['securities_name']}")

                    if st.button("🚀 Download Greek Bond Prices from Solidus", key="mkt_dl_solidus", width="stretch"):
                        download_bond_prices_from_solidus()
                        _md_load_securities_with_price_stats.clear()
                        st.rerun()

                # ── Import from file ──────────────────────────────────────────
                if st.session_state.get("_mkt_import_last_sec_id") != inv_sec_id:
                    st.session_state["_mkt_import_last_sec_id"] = inv_sec_id
                    st.session_state["_mkt_file_upload_counter"] = (
                        st.session_state.get("_mkt_file_upload_counter", 0) + 1)
                    st.session_state["mkt_import_sec_select"] = inv_sec_id

                with st.expander("📂 Import Prices from File", expanded=False):
                    st.markdown(
                        "Upload a tab-separated **`.txt` / `.csv` / `.tsv`** file with historical "
                        "prices. The file may contain metadata lines above the header — the importer "
                        "will find the `Date` header row automatically."
                    )
                    _import_sec_keys = list(sec_options.keys())
                    _import_default_idx = (
                        _import_sec_keys.index(inv_sec_id) if inv_sec_id in _import_sec_keys else 0)
                    import_sec_id = st.selectbox(
                        "Target Security:", _import_sec_keys,
                        format_func=lambda x: sec_options.get(x, str(x)),
                        index=_import_default_idx, key="mkt_import_sec_select",
                    )
                    _upload_key   = f"mkt_price_uploader_{st.session_state.get('_mkt_file_upload_counter', 0)}"
                    uploaded_file = st.file_uploader("Choose file:", type=["txt","csv","tsv"], key=_upload_key)
                    conflict_mode = st.radio(
                        "If a date already exists in the database:",
                        ["Skip (keep existing)", "Overwrite (replace existing)"],
                        horizontal=True, key="mkt_import_conflict_mode",
                    )
                    if uploaded_file is not None:
                        try:
                            import io as _io
                            raw   = uploaded_file.read().decode("utf-8", errors="replace")
                            lines = raw.splitlines()
                            header_idx = None
                            for _i, _line in enumerate(lines):
                                if _line.strip().lower().startswith("date"):
                                    header_idx = _i
                                    break
                            if header_idx is None:
                                st.error("❌ Could not find a 'Date' header row in the file.")
                            else:
                                data_text  = "\n".join(lines[header_idx:])
                                df_import  = pd.read_csv(
                                    _io.StringIO(data_text), sep="\t", dayfirst=True,
                                    parse_dates=["Date"])
                                df_import.columns = df_import.columns.str.strip().str.lower()
                                if "price" in df_import.columns and "close" not in df_import.columns:
                                    df_import.rename(columns={"price": "close"}, inplace=True)
                                df_import["close"] = pd.to_numeric(df_import["close"], errors="coerce")
                                df_import = df_import[
                                    df_import["close"].notna() & (df_import["close"] != 0)].copy()
                                for _col in ("high", "low", "volume"):
                                    if _col in df_import.columns:
                                        df_import[_col] = pd.to_numeric(df_import[_col], errors="coerce")
                                        df_import[_col] = df_import[_col].where(
                                            df_import[_col] != 0, other=None)
                                df_import["date"] = pd.to_datetime(df_import["date"]).dt.date
                                _keep = [c for c in ("date","close","high","low","volume")
                                         if c in df_import.columns]
                                df_import = df_import[_keep].dropna(
                                    subset=["date","close"]).reset_index(drop=True)

                                st.write(f"**Preview** — {len(df_import):,} rows parsed (showing first 20):")
                                st.dataframe(df_import.head(20), width='stretch')
                                copy_df_button(df_import, key="mkt_dl_import_preview")

                                if st.button("⬆️ Import into Database", key="mkt_do_import", type="primary"):
                                    from database.connection import get_connection as _get_conn
                                    _conn = _get_conn()
                                    _cur  = _conn.cursor()
                                    _inserted = _skipped = _overwritten = 0
                                    _has_high   = "high"   in df_import.columns
                                    _has_low    = "low"    in df_import.columns
                                    _has_volume = "volume" in df_import.columns

                                    def _db_val(v):
                                        try:
                                            import math
                                            if math.isnan(float(v)): return None
                                        except (TypeError, ValueError):
                                            pass
                                        return None if v is None else v

                                    for _, _row in df_import.iterrows():
                                        _vals = (
                                            import_sec_id, _row["date"],
                                            _db_val(_row["close"]),
                                            _db_val(_row["high"])   if _has_high   else None,
                                            _db_val(_row["low"])    if _has_low    else None,
                                            _db_val(_row["volume"]) if _has_volume else None,
                                        )
                                        if conflict_mode.startswith("Skip"):
                                            _cur.execute(
                                                "INSERT INTO Historical_Prices"
                                                " (Securities_Id, Date, Close, High, Low, Volume)"
                                                " VALUES (%s,%s,%s,%s,%s,%s)"
                                                " ON CONFLICT (Securities_Id, Date) DO NOTHING",
                                                _vals)
                                            if _cur.rowcount == 1: _inserted += 1
                                            else:                  _skipped  += 1
                                        else:
                                            _cur.execute(
                                                "INSERT INTO Historical_Prices"
                                                " (Securities_Id, Date, Close, High, Low, Volume)"
                                                " VALUES (%s,%s,%s,%s,%s,%s)"
                                                " ON CONFLICT (Securities_Id, Date) DO UPDATE"
                                                " SET Close=EXCLUDED.Close, High=EXCLUDED.High,"
                                                "     Low=EXCLUDED.Low, Volume=EXCLUDED.Volume",
                                                _vals)
                                            _overwritten += 1

                                    _conn.commit(); _cur.close(); _conn.close()
                                    _parts = []
                                    if conflict_mode.startswith("Skip"):
                                        _parts.append(f"{_inserted:,} inserted")
                                        if _skipped:
                                            _parts.append(f"{_skipped:,} skipped (already exist)")
                                    else:
                                        _parts.append(
                                            f"{_overwritten:,} rows written (inserted or overwritten)")
                                    st.success(f"✅ Import complete — {', '.join(_parts)}.")
                                    _md_load_securities_with_price_stats.clear()
                                    st.rerun()
                        except Exception as _exc:
                            st.error(f"❌ Error parsing file: {_exc}")

            # ─────────────────────────────────────────────────────────────────
            # SUB-TAB: Investment Transactions
            # ─────────────────────────────────────────────────────────────────
            with st_inv_txs:
                with get_db() as _conn_det:
                    df_inv_det = pd.read_sql("""
                        SELECT
                            a.accounts_name                        AS "Account",
                            i.date                                 AS "Date",
                            i.action                               AS "Action",
                            COALESCE(i.quantity, 0)                AS "Quantity",
                            COALESCE(i.price_per_share, 0)         AS "Price/Share",
                            COALESCE(i.commission, 0)              AS "Commission",
                            COALESCE(i.total_amount_acccur, 0)     AS "Total Amount",
                            i.description                          AS "Description"
                        FROM Investments i
                        JOIN Accounts a ON a.accounts_id = i.accounts_id
                        WHERE i.securities_id = %(sid)s
                        ORDER BY i.date DESC, i.investments_id DESC
                    """, _conn_det, params={"sid": inv_sec_id})

                    df_price = pd.read_sql("""
                        SELECT close AS current_price, date AS price_date
                        FROM Historical_Prices
                        WHERE securities_id = %(sid)s ORDER BY date DESC LIMIT 1
                    """, _conn_det, params={"sid": inv_sec_id})

                    df_hold = pd.read_sql("""
                        SELECT
                            a.accounts_name AS "Account",
                            SUM(CASE
                                WHEN i.action IN ('Buy','ShrIn','Reinvest','Vest','Grant','Exercise')
                                     THEN  COALESCE(i.quantity, 0)
                                WHEN i.action IN ('Sell','ShrOut','Expire')
                                     THEN -COALESCE(i.quantity, 0)
                                ELSE 0 END)                                           AS "Qty Held",
                            SUM(CASE
                                WHEN i.action IN ('Buy','ShrIn','Reinvest','Vest','Grant','Exercise')
                                     THEN  COALESCE(i.total_amount_acccur, 0)
                                WHEN i.action IN ('Sell','ShrOut','Expire')
                                     THEN -COALESCE(i.total_amount_acccur, 0)
                                ELSE 0 END)                                           AS "Cost Basis"
                        FROM Investments i
                        JOIN Accounts a ON a.accounts_id = i.accounts_id
                        WHERE i.securities_id = %(sid)s
                        GROUP BY a.accounts_id, a.accounts_name ORDER BY a.accounts_name
                    """, _conn_det, params={"sid": inv_sec_id})

                _cur_price  = (
                    float(df_price["current_price"].iloc[0])
                    if not df_price.empty and df_price["current_price"].iloc[0] is not None
                    else None)
                _price_date = str(df_price["price_date"].iloc[0]) if not df_price.empty else "N/A"
                _total_qty  = float(df_hold["Qty Held"].sum())   if not df_hold.empty else 0.0
                _total_cost = float(df_hold["Cost Basis"].sum()) if not df_hold.empty else 0.0

                _m1, _m2, _m3, _m4 = st.columns(4)
                with _m1: st.metric("Transactions",  f"{len(df_inv_det):,}")
                with _m2: st.metric("Total Qty Held", f"{_total_qty:,.4f}")
                with _m3: st.metric(f"Price ({_price_date})", f"{_cur_price:,.4f}" if _cur_price else "N/A")
                with _m4:
                    if _cur_price and _total_qty:
                        _cur_val    = _total_qty * _cur_price
                        _unrealised = _cur_val - _total_cost
                        st.metric("Est. Current Value", f"{_cur_val:,.2f}",
                                  delta=f"{_unrealised:+,.2f} P&L")
                    else:
                        st.metric("Est. Current Value", "N/A")

                if not df_hold.empty:
                    st.markdown("**Holdings by Account**")
                    _df_hold_disp = df_hold.copy()
                    if _cur_price:
                        _df_hold_disp["Cur. Value"]     = _df_hold_disp["Qty Held"] * _cur_price
                        _df_hold_disp["Unrealised P&L"] = _df_hold_disp["Cur. Value"] - _df_hold_disp["Cost Basis"]
                    st.dataframe(_df_hold_disp, width="stretch", hide_index=True,
                        column_config={
                            "Qty Held":       st.column_config.NumberColumn(format="%,.4f"),
                            "Cost Basis":     st.column_config.NumberColumn(format="%,.2f"),
                            "Cur. Value":     st.column_config.NumberColumn(format="%,.2f"),
                            "Unrealised P&L": st.column_config.NumberColumn(format="%,.2f"),
                        })
                    copy_df_button(_df_hold_disp, key=f"mkt_dl_holdings_{inv_sec_id}")

                st.markdown(f"**All Transactions ({len(df_inv_det):,})**")
                if not df_inv_det.empty:
                    st.dataframe(df_inv_det, width="stretch", hide_index=True,
                        column_config={
                            "Date":         st.column_config.DateColumn(format="DD/MM/YYYY"),
                            "Quantity":     st.column_config.NumberColumn(format="%,.4f"),
                            "Price/Share":  st.column_config.NumberColumn(format="%,.4f"),
                            "Commission":   st.column_config.NumberColumn(format="%,.4f"),
                            "Total Amount": st.column_config.NumberColumn(format="%,.2f"),
                        })
                    copy_df_button(df_inv_det, key=f"mkt_dl_inv_txns_{inv_sec_id}")
                else:
                    st.info("No investment transactions recorded for this security.")

            # ─────────────────────────────────────────────────────────────────
            # SUB-TAB: Price Anomalies (scoped to the selected security)
            # ─────────────────────────────────────────────────────────────────
            with st_anomalies:
                st.caption(
                    "Flags prices that changed by more than the chosen threshold vs the previous "
                    "or next trading day for **this security**. "
                    "The nearest buy/sell transaction is shown for context."
                )
                threshold = st.slider(
                    "Flag when move exceeds (%):",
                    min_value=10, max_value=1000, value=100, step=10,
                    key=f"mkt_pq_threshold_{inv_sec_id}",
                    help="100% = flag any price that is more than 2× or less than ½ of its neighbour",
                )

                with st.spinner("Scanning price history…"):
                    df_anomalies = get_price_anomalies(float(threshold), securities_ids=(inv_sec_id,))

                if df_anomalies.empty:
                    st.success(f"No prices flagged at the {threshold}% threshold for this security.")
                else:
                    st.warning(f"{len(df_anomalies):,} suspicious price record(s).")
                    df_anomalies = df_anomalies.copy()
                    df_anomalies['date']    = pd.to_datetime(df_anomalies['date']).dt.date
                    df_anomalies['tx_date'] = pd.to_datetime(df_anomalies['tx_date'], errors='coerce').dt.date
                    df_anomalies.insert(0, 'Delete', False)

                    edited_anom = st.data_editor(
                        df_anomalies,
                        column_config={
                            'Delete':        st.column_config.CheckboxColumn('🗑 Delete', default=False, pinned=True),
                            'securities_id': None,
                            'security_name': st.column_config.TextColumn('Security',         pinned=True),
                            'date':          st.column_config.DateColumn('Date'),
                            'price':         st.column_config.NumberColumn('Price',           format='%.4f'),
                            'prev_close':    st.column_config.NumberColumn('Prev Close',      format='%.4f'),
                            'next_close':    st.column_config.NumberColumn('Next Close',      format='%.4f'),
                            'pct_vs_prev':   st.column_config.NumberColumn('% vs Prev',      format='%+.1f %%'),
                            'pct_vs_next':   st.column_config.NumberColumn('% vs Next',      format='%+.1f %%'),
                            'tx_date':       st.column_config.DateColumn('Nearest Tx Date'),
                            'tx_action':     st.column_config.TextColumn('Tx Action'),
                            'tx_price':      st.column_config.NumberColumn('Tx Price',        format='%.4f'),
                            'days_diff':     st.column_config.NumberColumn('Days to Tx',      format='%d'),
                            'pct_vs_tx':     st.column_config.NumberColumn('% vs Tx',        format='%+.1f %%'),
                        },
                        disabled=[c for c in df_anomalies.columns if c != 'Delete'],
                        hide_index=True, width="stretch",
                        key=f"mkt_pq_editor_{inv_sec_id}",
                    )
                    copy_df_button(df_anomalies, key=f"mkt_dl_anomalies_{inv_sec_id}")

                    to_delete  = edited_anom[edited_anom['Delete']]
                    n_selected = len(to_delete)
                    n_visible  = len(df_anomalies)

                    col_del, col_del_all, col_info = st.columns([1, 1, 3])
                    with col_del:
                        del_btn = st.button(
                            f"🗑 Delete {n_selected} selected" if n_selected else "🗑 Delete selected",
                            type="primary" if n_selected else "secondary",
                            disabled=(n_selected == 0), width="stretch",
                            key=f"mkt_pq_del_btn_{inv_sec_id}",
                        )
                    with col_del_all:
                        del_all_btn = st.button(
                            f"🗑 Delete all {n_visible} listed",
                            type="primary", width="stretch",
                            key=f"mkt_pq_del_all_btn_{inv_sec_id}",
                        )
                    with col_info:
                        if n_selected:
                            st.info(f"{n_selected} row(s) checked — or use 'Delete all listed' to remove every visible row.")

                    _del_ck     = f"mkt_pq_del_confirm_{inv_sec_id}"
                    _del_all_ck = f"mkt_pq_del_all_confirm_{inv_sec_id}"

                    if del_btn and n_selected:
                        st.session_state[_del_ck] = True
                    if del_all_btn:
                        st.session_state[_del_all_ck] = True

                    if st.session_state.get(_del_ck):
                        st.warning(f"⚠️ Delete **{n_selected}** selected price record(s)? This cannot be undone.")
                        _cn, _cy, _ = st.columns([1, 1, 3])
                        with _cn:
                            if st.button("✖ Cancel", key=f"mkt_pq_del_cancel_{inv_sec_id}", width="stretch"):
                                st.session_state[_del_ck] = False
                                st.rerun()
                        with _cy:
                            if st.button("✔ Yes, delete", type="primary", key=f"mkt_pq_del_yes_{inv_sec_id}", width="stretch"):
                                delete_historical_prices(to_delete[['securities_id', 'date']].to_dict('records'))
                                get_price_anomalies.clear()
                                st.session_state[_del_ck] = False
                                st.success(f"Deleted {n_selected} price record(s).")
                                st.rerun()

                    if st.session_state.get(_del_all_ck):
                        st.warning(f"⚠️ Delete **all {n_visible}** listed price record(s)? This cannot be undone.")
                        _cn, _cy, _ = st.columns([1, 1, 3])
                        with _cn:
                            if st.button("✖ Cancel", key=f"mkt_pq_del_all_cancel_{inv_sec_id}", width="stretch"):
                                st.session_state[_del_all_ck] = False
                                st.rerun()
                        with _cy:
                            if st.button("✔ Yes, delete all", type="primary", key=f"mkt_pq_del_all_yes_{inv_sec_id}", width="stretch"):
                                delete_historical_prices(df_anomalies[['securities_id', 'date']].to_dict('records'))
                                get_price_anomalies.clear()
                                st.session_state[_del_all_ck] = False
                                st.success(f"Deleted {n_visible} price record(s).")
                                st.rerun()

            # ─────────────────────────────────────────────────────────────────
            # SUB-TAB: Dummy Prices (scoped to the selected security)
            # ─────────────────────────────────────────────────────────────────
            with st_dummy:
                st.caption(
                    "Finds Buy / Sell / Reinvest / ShrIn / ShrOut transactions for **this security** "
                    "whose Price Per Share or Quantity appears to be a placeholder (whole-number dummy) "
                    "while a real Historical Price exists for that date. "
                    "Updates Price → actual close price and recalculates Quantity = Total ÷ Price, "
                    "leaving Total Amount unchanged so P&L is preserved."
                )
                with st.spinner("Scanning investments…"):
                    df_dummy_all = get_investments_with_dummy_prices()

                # Filter to the selected security
                df_dummy = (
                    df_dummy_all[df_dummy_all['securities_id'] == inv_sec_id].copy()
                    if not df_dummy_all.empty else df_dummy_all.copy()
                )

                if df_dummy.empty:
                    st.success("No investments with dummy prices found for this security.")
                else:
                    st.info(
                        f"{len(df_dummy):,} transaction(s) with dummy prices across "
                        f"{df_dummy['account_name'].nunique()} account(s)."
                    )

                    # Position-closure sanity check
                    pos_check = df_dummy.copy()
                    pos_check['signed_new_qty'] = pos_check.apply(
                        lambda r: r['new_qty'] if r['action'] in ('Buy','Reinvest','ShrIn')
                                  else -r['new_qty'], axis=1)
                    net_pos  = (
                        pos_check
                        .groupby(['account_name','security_name'])['signed_new_qty']
                        .sum().reset_index()
                        .rename(columns={'signed_new_qty': 'net_qty'})
                    )
                    non_zero = net_pos[net_pos['net_qty'].abs() > 0.0001]
                    if not non_zero.empty:
                        msg = ("**Position closure warning** — after normalization the following "
                               "will have a non-zero net holding:\n")
                        for _, row in non_zero.iterrows():
                            msg += f"- {row['account_name']} / {row['security_name']}: net qty = {row['net_qty']:+.6f}\n"
                        st.warning(msg)

                    df_dummy_disp        = df_dummy.copy()
                    df_dummy_disp['date'] = pd.to_datetime(df_dummy_disp['date']).dt.date
                    st.dataframe(
                        df_dummy_disp,
                        column_config={
                            'investments_id': None, 'accounts_id': None, 'securities_id': None,
                            'account_name':   st.column_config.TextColumn('Account'),
                            'security_name':  st.column_config.TextColumn('Security'),
                            'date':           st.column_config.DateColumn('Date'),
                            'action':         st.column_config.TextColumn('Action'),
                            'total_amount':   st.column_config.NumberColumn('Total Amount',  format='%.4f'),
                            'current_qty':    st.column_config.NumberColumn('Current Qty',   format='%.6f'),
                            'current_price':  st.column_config.NumberColumn('Current Price', format='%.4f'),
                            'hist_price':     st.column_config.NumberColumn('Hist. Close',   format='%.4f',
                                              help='Historical close price on that date'),
                            'new_qty':        st.column_config.NumberColumn('New Qty',       format='%.6f',
                                              help='Buys: total/hist_price  •  Sells: proportional from buy qty'),
                            'new_price':      st.column_config.NumberColumn('New Price',      format='%.4f',
                                              help='Buys: hist close  •  Sells: effective realised price'),
                        },
                        hide_index=True, width="stretch",
                    )
                    copy_df_button(df_dummy_disp, key=f"mkt_dl_dummy_{inv_sec_id}")
                    st.caption(
                        "**Buys**: Price ← hist close, Qty ← Total ÷ hist close.  "
                        "**Sells**: Qty is distributed from the total buy quantity so the position "
                        "closes; Price is the effective realised price (Total ÷ Qty)."
                    )

                    _norm_ck = f"mkt_ni_norm_confirm_{inv_sec_id}"
                    col_norm, col_info = st.columns([1, 4])
                    with col_norm:
                        if st.button(
                                f"⚖ Normalize ({len(df_dummy):,})", type="primary",
                                width="stretch", key=f"mkt_ni_norm_btn_{inv_sec_id}"):
                            st.session_state[_norm_ck] = True
                    with col_info:
                        st.caption("Normalizes only the rows shown above (for this security).")

                    if st.session_state.get(_norm_ck):
                        st.warning(
                            f"⚠️ This will overwrite prices and quantities for "
                            f"**{len(df_dummy):,}** investment row(s). This cannot be undone."
                        )
                        _cn, _cy, _ = st.columns([1, 1, 3])
                        with _cn:
                            if st.button("✖ Cancel", key=f"mkt_ni_norm_cancel_{inv_sec_id}",
                                         width="stretch"):
                                st.session_state[_norm_ck] = False
                                st.rerun()
                        with _cy:
                            if st.button("✔ Yes, normalize", type="primary",
                                         key=f"mkt_ni_norm_yes_{inv_sec_id}", width="stretch"):
                                ids     = df_dummy['investments_id'].tolist()
                                updated = normalize_investment_prices(ids)
                                get_investments_with_dummy_prices.clear()
                                st.session_state[_norm_ck] = False
                                st.success(f"Updated {updated} investment row(s).")
                                st.rerun()

                    st.divider()
                    col_rh, col_rh_info = st.columns([1, 4])
                    with col_rh:
                        if st.button(
                                "🔄 Refresh Holdings", width="stretch",
                                key=f"mkt_ni_refresh_holdings_{inv_sec_id}"):
                            from database.crud import update_holdings as _update_holdings
                            _update_holdings()
                            st.success("Holdings recalculated.")
                    with col_rh_info:
                        st.caption(
                            "Recalculates the Holdings table from Investments data. "
                            "Run this after normalization to update portfolio quantities and P&L."
                        )

            # ─────────────────────────────────────────────────────────────────
            # SUB-TAB: Dividends
            # ─────────────────────────────────────────────────────────────────
            with st_divs:
                # Load dividend summary + history for the selected security
                with get_db() as _conn_div:
                    df_div_info = pd.read_sql("""
                        SELECT Dividend_Yield, Dividend_Rate, Five_Year_Avg_Yield,
                               Payout_Ratio, Ex_Dividend_Date, Dividend_Pay_Date,
                               Dividend_Frequency
                        FROM   Securities
                        WHERE  Securities_Id = %(sid)s
                    """, _conn_div, params={"sid": inv_sec_id})

                    df_div_hist = pd.read_sql("""
                        SELECT Ex_Date AS "Ex-Date", Amount AS "Amount"
                        FROM   Securities_Dividends
                        WHERE  Securities_Id = %(sid)s
                        ORDER  BY Ex_Date DESC
                    """, _conn_div, params={"sid": inv_sec_id})

                # ── Summary metric cards ──────────────────────────────────────
                _has_div_info = (
                    not df_div_info.empty
                    and not df_div_info.iloc[0].isnull().all()
                )
                if _has_div_info:
                    _di = df_div_info.iloc[0]

                    def _fmt_pct(v):
                        return f"{v:.2f}%" if pd.notna(v) else "—"
                    def _fmt_num(v):
                        return f"{v:,.4f}" if pd.notna(v) else "—"
                    def _fmt_date(v):
                        return str(v) if pd.notna(v) else "—"
                    def _fmt_str(v):
                        return str(v) if pd.notna(v) else "—"

                    _d1, _d2, _d3, _d4 = st.columns(4)
                    with _d1: st.metric("Dividend Yield",  _fmt_pct(_di.get("dividend_yield")))
                    with _d2: st.metric("Annual Rate",     _fmt_num(_di.get("dividend_rate")))
                    with _d3: st.metric("5Y Avg Yield",    _fmt_pct(_di.get("five_year_avg_yield")))
                    with _d4: st.metric("Payout Ratio",    _fmt_pct(_di.get("payout_ratio")))

                    _d5, _d6, _d7, _ = st.columns(4)
                    with _d5: st.metric("Ex-Dividend Date", _fmt_date(_di.get("ex_dividend_date")))
                    with _d6: st.metric("Payment Date",     _fmt_date(_di.get("dividend_pay_date")))
                    with _d7: st.metric("Frequency",        _fmt_str(_di.get("dividend_frequency")))
                else:
                    st.info(
                        "No dividend summary data stored for this security. "
                        "Use the download button below to fetch it from Yahoo Finance."
                    )

                # ── Historical dividend bar chart + table ─────────────────────
                if not df_div_hist.empty:
                    st.subheader("📅 Dividend History")

                    _df_chart = df_div_hist.copy()
                    _df_chart["Ex-Date"] = pd.to_datetime(_df_chart["Ex-Date"])
                    _df_chart["Year"]    = _df_chart["Ex-Date"].dt.year
                    _df_annual = (
                        _df_chart.groupby("Year")["Amount"]
                        .sum().reset_index().sort_values("Year")
                    )
                    fig_div = go.Figure(go.Bar(
                        x=_df_annual["Year"].astype(str),
                        y=_df_annual["Amount"],
                        marker_color="steelblue",
                        hovertemplate="Year: %{x}<br>Total: %{y:.4f}<extra></extra>",
                    ))
                    fig_div.update_layout(
                        xaxis_title="Year", yaxis_title="Total Dividend per Share",
                        margin=dict(l=0, r=0, t=0, b=0), height=280,
                        template="plotly_white",
                    )
                    st.plotly_chart(fig_div, width="stretch")

                    st.dataframe(
                        df_div_hist,
                        hide_index=True, width="stretch",
                        column_config={
                            "Ex-Date": st.column_config.DateColumn("Ex-Date", format="DD/MM/YYYY"),
                            "Amount":  st.column_config.NumberColumn("Amount",  format="%,.4f"),
                        },
                    )
                    copy_df_button(df_div_hist, key=f"mkt_dl_div_hist_{inv_sec_id}")
                else:
                    st.info("No historical dividend records found for this security.")

                # ── Targeted download button ──────────────────────────────────
                st.divider()
                _yh_ticker_div = selected_inv_sec.get("yahoo_ticker")
                _has_yahoo_div = bool(pd.notna(_yh_ticker_div) and _yh_ticker_div)
                if _has_yahoo_div:
                    if st.button(
                            "📅 Update dividend data for this security",
                            key=f"mkt_div_update_{inv_sec_id}", width="stretch"):
                        with st.spinner("Fetching dividend data from Yahoo…"):
                            download_securities_info_from_yahoo(target_sec_id=inv_sec_id)
                            download_dividend_history(target_sec_id=inv_sec_id)
                        _md_load_securities_list.clear()
                        st.rerun()
                else:
                    st.caption(
                        f"ℹ️ No Yahoo ticker defined for {selected_inv_sec['securities_name']} "
                        "— cannot fetch dividend data."
                    )
