import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from database.crud import save_changes_no_serial, save_changes_mid
from data.downloaders import download_historical_fx, download_historical_prices_from_eodhd, download_historical_prices_from_yahoo, download_bond_prices_from_solidus

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

            if st.button("🚀 Download All from EODHD", key="download_all_eodhd", width="stretch"):
                download_historical_prices_from_eodhd(ts_period_price)
                st.rerun()

            if st.button(f"🚀 Update {selected_inv_sec['securities_name']} from EODHD", key="download_one_eodhd", width="stretch"):
                download_historical_prices_from_eodhd(ts_period_price, inv_sec_id)
                st.rerun()

            if st.button("🚀 Download Bond Prices from Solidus", key="download_solidus", width="stretch"):
                download_bond_prices_from_solidus()
                st.rerun
