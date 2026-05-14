import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from database.crud import save_changes_no_serial, save_changes_mid
from ui.components import copy_df_button
from data.downloaders import download_historical_fx, download_historical_prices_from_tradingview, download_historical_prices_from_yahoo, download_bond_prices_from_solidus

def render_market_data(conn):
    """Render the Market Data page."""
    st.title("Market Data")
    t1, t2 = st.tabs(["FX Rates", "Security Prices"])
    
    df_curr_list = pd.read_sql("SELECT Currencies_Id, Currencies_ShortName FROM Currencies", conn)
    curr_options = df_curr_list.set_index('currencies_id')['currencies_shortname'].to_dict()

    df_sec_list = pd.read_sql("SELECT Securities_Id, Securities_Name FROM Securities", conn)
    sec_options = df_sec_list.set_index('securities_id')['securities_name'].to_dict()

    period_options = {
        "1 Day": "1d", "5 Days": "5d", "1 Month": "1mo", "3 Months": "3mo", "6 Months": "6mo",
        "1 Year": "1y", "3 Years": "3y", "5 Years": "5y", "10 Years": "10y", "15 Years": "15y",
        "20 Years": "20y", "25 Years": "25y", "30 Years": "30y"
    }
    
    with t1:  # FX Rates
        df = pd.read_sql("SELECT * FROM Historical_FX ORDER BY Date DESC, Currencies_Id_1 ASC", conn)
        edited_hfx = st.data_editor(df, num_rows="dynamic", key="set_hfx", column_config={
            "currencies_id_1": st.column_config.SelectboxColumn("Base Currency", options=list(curr_options.keys()), format_func=lambda x: curr_options.get(x, "Unknown")),
            "currencies_id_2": st.column_config.SelectboxColumn("Target Currency", options=list(curr_options.keys()), format_func=lambda x: curr_options.get(x, "Unknown"))
        })
        save_changes_no_serial(df, edited_hfx, "Historical_FX", "date")
        copy_df_button(df, key="dl_mkt_fx")

        if not df.empty:
            st.subheader("📈 Exchange Rate Chart")
            df_plot = df.copy()
            df_plot['Pair'] = df_plot.apply(
                lambda row: f"{curr_options.get(row['currencies_id_1'], '??')}/{curr_options.get(row['currencies_id_2'], '??')}", 
                axis=1
            )
            available_pairs = df_plot['Pair'].unique()
            selected_pair = st.selectbox(
                "Select pair to display:", 
                available_pairs,
                key="fx_pair_select"
            )
            chart_data = df_plot[df_plot['Pair'] == selected_pair].sort_values('date')
            if not chart_data.empty:
                st.line_chart(
                    data=chart_data, 
                    x='date', 
                    y='fx_rate', 
                    x_label="Date",
                    y_label="Rate",
                    width='stretch')
        
        st.subheader("🔄 Update Exchange Rates")
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_label_fx = st.selectbox(
                "Select time period:", 
                list(period_options.keys()), 
                index=1,
                key="fx_period_select"
            )
            ts_period_fx = period_options[selected_label_fx]
        with col2:
            if st.button("🚀 Download Rates", key="download_fx_btn"):
                with st.spinner("Processing..."):
                    download_historical_fx(ts_period_fx)
                    st.balloons()
                    st.rerun()
    
    with t2:  # Security Prices
        df_inv_secs = pd.read_sql("""
            SELECT S.Securities_Id, S.Securities_Name, 
                (SELECT COUNT(HP.*) FROM Historical_Prices HP WHERE HP.Securities_Id = S.Securities_Id) NoOfRecords, 
                (SELECT COALESCE(MAX(HP.Date),'1900-01-01') FROM Historical_Prices HP WHERE HP.Securities_Id = S.Securities_Id) MaxDate 
            FROM Securities S ORDER BY S.Securities_Name ASC
        """, conn)
        
        if df_inv_secs.empty:
            st.warning("⚠️ No Securities found. Define a Security in Settings.")
        else:
            records = df_inv_secs.to_dict('records')
            
            # 1. Διατήρηση επιλογής Security
            default_sec_idx = 0
            if "selected_sec_id" in st.session_state:
                for i, rec in enumerate(records):
                    if rec['securities_id'] == st.session_state.selected_sec_id:
                        default_sec_idx = i
                        break

            selected_inv_sec = st.selectbox(
                "Select Security:", 
                records, 
                index=default_sec_idx,
                format_func=lambda x: f"{x['securities_name']} ({x['noofrecords']:,.0f}) ({x['maxdate']})",
                key="security_select"
            )
            st.session_state.selected_sec_id = selected_inv_sec['securities_id']
            inv_sec_id = selected_inv_sec['securities_id']
        
        df_hpr_tx = pd.read_sql(f"SELECT Securities_Id, Date, Close, High, Low, Volume, embedding FROM Historical_Prices WHERE Securities_Id = {inv_sec_id} ORDER BY Date DESC", conn)
        edited_hpr_tx = st.data_editor(
            df_hpr_tx, 
            num_rows="dynamic", 
            key=f"inv_hpr_editor_{inv_sec_id}", 
            width="stretch",
            column_config={
                'securities_id': None,
                'date': st.column_config.DateColumn('Date', format="DD/MM/YYYY"),
                'close': st.column_config.NumberColumn('Close', format="%,.8f"),
                'high': st.column_config.NumberColumn('High', format="%,.8f"),
                'low': st.column_config.NumberColumn('Low', format="%,.8f"),
                'volume': st.column_config.NumberColumn('Volume', format="%,.0f"),
                'embedding': None
            }
        )
        save_changes_mid(edited_hpr_tx, "Historical_Prices", id_cols=["securities_id", "date"], filter_col="securities_id", filter_val=inv_sec_id)
        copy_df_button(df_hpr_tx, key=f"dl_mkt_prices_{inv_sec_id}")

        if not df_hpr_tx.empty:
            st.subheader("📈 Security Pricing Chart")

            df_tx_marks = pd.read_sql(f"""
                SELECT Date as Transaction_Date, Action, Quantity 
                FROM Investments 
                WHERE Securities_Id = {inv_sec_id} 
                AND Action IN ('Buy', 'Sell')
            """, conn)
            df_tx_marks['transaction_date'] = pd.to_datetime(df_tx_marks['transaction_date'])

            # --- ΝΕΟ: Φίλτρα Γραφήματος ---
            c1, c2 = st.columns([1, 1])
            with c1:
                # 2. Επιλογή χρονικού εύρους γραφήματος
                chart_view_options = {
                    "1 Week": 7, 
                    "1 Month": 30, 
                    "3 Months": 90, 
                    "6 Months": 180, 
                    "YTD": "YTD", 
                    "1 Year": 365, 
                    "3 Years": 1095, 
                    "5 Years": 1825,
                    "All Time": None
                }
                selected_view = st.selectbox("Chart Range:", list(chart_view_options.keys()), index=8)
            
            with c2:
                # 3. Slider για Moving Average
                window_size = st.slider("Moving Average (MA) Days:", 2, 30, 5)

            # Προετοιμασία δεδομένων
            chart_data = df_hpr_tx.sort_values('date').copy()
            chart_data['date'] = pd.to_datetime(chart_data['date'])
            
            # Φιλτράρισμα βάσει επιλογής Chart Range
            if chart_view_options[selected_view] is not None:
                max_date = chart_data['date'].max()
                if chart_view_options[selected_view] == "YTD":
                    start_date = pd.Timestamp(year=max_date.year, month=1, day=1)
                else:
                    start_date = max_date - pd.Timedelta(days=chart_view_options[selected_view])
                chart_data = chart_data[chart_data['date'] >= start_date]

            chart_data = chart_data.merge(
                df_tx_marks, 
                left_on='date', 
                right_on='transaction_date', 
                how='left'
            )
            
            chart_data['Trade'] = chart_data.apply(
                lambda row: row['close'] if pd.notnull(row['action']) else None, 
                axis=1
            )            

            # Υπολογισμοί MA & Μετονομασίες
            chart_data['MA'] = chart_data['close'].rolling(window=window_size).mean()

            # 3. Δημιουργία Plotly Figure
            fig = go.Figure()

            # Γραμμή Τιμής
            fig.add_trace(go.Scatter(
                x=chart_data['date'], y=chart_data['close'],
                mode='lines', name='Price', line=dict(color='blue', width=2)
            ))

            # Γραμμή Κινητού Μέσου Όρου
            fig.add_trace(go.Scatter(
                x=chart_data['date'], y=chart_data['MA'],
                mode='lines', name=f'MA ({window_size}d)', line=dict(color='red', width=1.5, dash='dot')
            ))

            # Σημεία Συναλλαγών (Buy/Sell)
            
            for t_type, t_color, t_symbol in [('Buy', 'green', 'triangle-up'), ('Sell', 'orange', 'triangle-down')]:
                subset = chart_data[chart_data['action'] == t_type]
                if not subset.empty:
                    fig.add_trace(go.Scatter(
                        x=subset['date'], y=subset['close'],
                        mode='markers', name=t_type,
                        marker=dict(color=t_color, size=12, symbol=t_symbol, line=dict(width=1, color='black')),
                        hovertemplate="Date: %{x}<br>Price: %{y}<br>Action: " + t_type + "<br>Qty: %{customdata}",
                        customdata=subset['quantity'] # Προσθήκη ποσότητας στο hover
                   ))

            # Ρυθμίσεις εμφάνισης
            fig.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
                template="plotly_white",
                height=450
            )

            st.plotly_chart(fig, width='stretch')

        st.subheader("🔄 Update Prices")
        col1, col2 = st.columns([2, 1])
        with col1:
            # 4. Διατήρηση επιλογής Period για το Download
            period_list = list(period_options.keys())
            default_period_idx = 1 # '1mo'
            if "last_ts_period" in st.session_state:
                if st.session_state.last_ts_period in period_list:
                    default_period_idx = period_list.index(st.session_state.last_ts_period)

            selected_label_price = st.selectbox(
                "Select download period:", 
                period_list, 
                index=default_period_idx,
                key="price_period_select"
            )
            st.session_state.last_ts_period = selected_label_price
            ts_period_price = period_options[selected_label_price]
            
        with col2:
            if st.button("🚀 Download All from Yahoo", key="download_all_yahoo", width="stretch"):
                download_historical_prices_from_yahoo(ts_period_price)
                st.rerun()

            if st.button(f"🚀 Update {selected_inv_sec['securities_name']} from Yahoo", key="download_one_yahoo", width="stretch"):
                download_historical_prices_from_yahoo(ts_period_price, inv_sec_id)
                st.rerun()

            if st.button("🚀 Download All from TradingView", key="download_all_tradingview", width="stretch"):
                download_historical_prices_from_tradingview(ts_period_price)
                st.rerun()

            if st.button(f"🚀 Update {selected_inv_sec['securities_name']} from TradingView", key="download_one_tradingview", width="stretch"):
                download_historical_prices_from_tradingview(ts_period_price, inv_sec_id)
                st.rerun()

            if st.button("🚀 Download Bond Prices from Solidus", key="download_solidus", width="stretch"):
                download_bond_prices_from_solidus()
                st.rerun()

        # ── Import Prices from File ───────────────────────────────────────────
        # When the top security selector changes, reset the import selectbox and
        # clear the file uploader (cycling its key forces Streamlit to discard it).
        if st.session_state.get("_import_last_inv_sec_id") != inv_sec_id:
            st.session_state["_import_last_inv_sec_id"] = inv_sec_id
            st.session_state["_file_upload_counter"] = (
                st.session_state.get("_file_upload_counter", 0) + 1
            )
            st.session_state["import_sec_select"] = inv_sec_id

        with st.expander("📂 Import Prices from File", expanded=False):
            st.markdown(
                "Upload a tab-separated **`.txt` / `.csv` / `.tsv`** file with historical prices. "
                "The file may contain metadata lines above the header — the importer will find the "
                "`Date` header row automatically."
            )

            # Target security (defaults to the currently selected one)
            _import_sec_keys = list(sec_options.keys())
            _import_default_idx = _import_sec_keys.index(inv_sec_id) if inv_sec_id in _import_sec_keys else 0
            import_sec_id = st.selectbox(
                "Target Security:",
                _import_sec_keys,
                format_func=lambda x: sec_options.get(x, str(x)),
                index=_import_default_idx,
                key="import_sec_select",
            )

            _upload_key = f"price_file_uploader_{st.session_state.get('_file_upload_counter', 0)}"
            uploaded_file = st.file_uploader(
                "Choose file:",
                type=["txt", "csv", "tsv"],
                key=_upload_key,
            )

            conflict_mode = st.radio(
                "If a date already exists in the database:",
                ["Skip (keep existing)", "Overwrite (replace existing)"],
                horizontal=True,
                key="import_conflict_mode",
            )

            if uploaded_file is not None:
                try:
                    import io as _io

                    raw = uploaded_file.read().decode("utf-8", errors="replace")
                    lines = raw.splitlines()

                    # Find the header row — first line whose first token is "Date"
                    header_idx = None
                    for _i, _line in enumerate(lines):
                        if _line.strip().lower().startswith("date"):
                            header_idx = _i
                            break

                    if header_idx is None:
                        st.error("❌ Could not find a 'Date' header row in the file.")
                    else:
                        data_text = "\n".join(lines[header_idx:])
                        df_import = pd.read_csv(
                            _io.StringIO(data_text),
                            sep="\t",
                            dayfirst=True,
                            parse_dates=["Date"],
                        )

                        # Normalise column names to lowercase
                        df_import.columns = df_import.columns.str.strip().str.lower()

                        # Rename "price" → "close"
                        if "price" in df_import.columns and "close" not in df_import.columns:
                            df_import.rename(columns={"price": "close"}, inplace=True)

                        # Parse close as numeric, drop zero / NaN rows
                        df_import["close"] = pd.to_numeric(df_import["close"], errors="coerce")
                        df_import = df_import[
                            df_import["close"].notna() & (df_import["close"] != 0)
                        ].copy()

                        # Treat 0 as NULL for High / Low / Volume
                        for _col in ("high", "low", "volume"):
                            if _col in df_import.columns:
                                df_import[_col] = pd.to_numeric(df_import[_col], errors="coerce")
                                df_import[_col] = df_import[_col].where(df_import[_col] != 0, other=None)

                        df_import["date"] = pd.to_datetime(df_import["date"]).dt.date

                        # Keep only the columns we care about
                        _keep = [c for c in ("date", "close", "high", "low", "volume") if c in df_import.columns]
                        df_import = df_import[_keep].dropna(subset=["date", "close"]).reset_index(drop=True)

                        st.write(f"**Preview** — {len(df_import):,} rows parsed (showing first 20):")
                        st.dataframe(df_import.head(20), use_container_width=True)

                        if st.button("⬆️ Import into Database", key="do_import_prices", type="primary"):
                            from database.connection import get_connection as _get_conn
                            _conn = _get_conn()
                            _cur  = _conn.cursor()
                            _inserted = 0
                            _skipped  = 0
                            _overwritten = 0

                            _has_high   = "high"   in df_import.columns
                            _has_low    = "low"    in df_import.columns
                            _has_volume = "volume" in df_import.columns

                            def _db_val(v):
                                """Convert pandas NaN/NA to Python None so psycopg2 can adapt it."""
                                try:
                                    import math
                                    if math.isnan(float(v)):
                                        return None
                                except (TypeError, ValueError):
                                    pass
                                return None if v is None else v

                            for _, _row in df_import.iterrows():
                                _vals = (
                                    import_sec_id,
                                    _row["date"],
                                    _db_val(_row["close"]),
                                    _db_val(_row["high"])   if _has_high   else None,
                                    _db_val(_row["low"])    if _has_low    else None,
                                    _db_val(_row["volume"]) if _has_volume else None,
                                )

                                if conflict_mode.startswith("Skip"):
                                    _cur.execute(
                                        """INSERT INTO Historical_Prices
                                               (Securities_Id, Date, Close, High, Low, Volume)
                                           VALUES (%s, %s, %s, %s, %s, %s)
                                           ON CONFLICT (Securities_Id, Date) DO NOTHING""",
                                        _vals,
                                    )
                                    if _cur.rowcount == 1:
                                        _inserted += 1
                                    else:
                                        _skipped += 1
                                else:
                                    _cur.execute(
                                        """INSERT INTO Historical_Prices
                                               (Securities_Id, Date, Close, High, Low, Volume)
                                           VALUES (%s, %s, %s, %s, %s, %s)
                                           ON CONFLICT (Securities_Id, Date) DO UPDATE
                                               SET Close  = EXCLUDED.Close,
                                                   High   = EXCLUDED.High,
                                                   Low    = EXCLUDED.Low,
                                                   Volume = EXCLUDED.Volume""",
                                        _vals,
                                    )
                                    _overwritten += 1

                            _conn.commit()
                            _cur.close()
                            _conn.close()

                            _parts = []
                            if conflict_mode.startswith("Skip"):
                                _parts.append(f"{_inserted:,} inserted")
                                if _skipped:
                                    _parts.append(f"{_skipped:,} skipped (already exist)")
                            else:
                                _parts.append(f"{_overwritten:,} rows written (inserted or overwritten)")
                            st.success(f"✅ Import complete — {', '.join(_parts)}.")
                            st.rerun()

                except Exception as _exc:
                    st.error(f"❌ Error parsing file: {_exc}")