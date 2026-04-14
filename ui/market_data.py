import streamlit as st
import pandas as pd
from database.crud import save_changes_no_serial, save_changes_mid
from data.downloaders import download_historical_fx, download_historical_prices_from_yahoo

def render_market_data(conn):
    """Render the Market Data page."""
    st.title("Market Data")
    t1, t2 = st.tabs(["FX Rates", "Security Prices"])
    
    df_curr_list = pd.read_sql("SELECT Currencies_Id, Currencies_ShortName FROM Currencies", conn)
    curr_options = df_curr_list.set_index('currencies_id')['currencies_shortname'].to_dict()
    
    period_options = {
        "1 Day": "1d", "5 Days": "5d", "1 Month": "1mo", "6 Months": "6mo",
        "1 Year": "1y", "5 Years": "5y", "10 Years": "10y", "15 Years": "15y",
        "20 Years": "20y", "25 Years": "25y", "30 Years": "30y", "All": "max"
    }
    
    with t1:  # FX Rates
        df = pd.read_sql("SELECT * FROM Historical_FX ORDER BY FX_Date DESC, Base_Currency_Id ASC", conn)
        edited_hfx = st.data_editor(df, num_rows="dynamic", key="set_hfx", column_config={
            "base_currency_id": st.column_config.SelectboxColumn("Base Currency", options=list(curr_options.keys()), format_func=lambda x: curr_options.get(x, "Unknown")),
            "target_currency_id": st.column_config.SelectboxColumn("Target Currency", options=list(curr_options.keys()), format_func=lambda x: curr_options.get(x, "Unknown"))
        })
        save_changes_no_serial(df, edited_hfx, "Historical_FX", "fx_date")
        
        if not df.empty:
            st.subheader("📈 Exchange Rate Chart")
            df_plot = df.copy()
            df_plot['Pair'] = df_plot.apply(
                lambda row: f"{curr_options.get(row['base_currency_id'], '??')}/{curr_options.get(row['target_currency_id'], '??')}", 
                axis=1
            )
            available_pairs = df_plot['Pair'].unique()
            selected_pair = st.selectbox(
                "Select pair to display:", 
                available_pairs,
                key="fx_pair_select"
            )
            chart_data = df_plot[df_plot['Pair'] == selected_pair].sort_values('fx_date')
            if not chart_data.empty:
                st.line_chart(data=chart_data, x='fx_date', y='fx_rate', use_container_width=True)
        
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
            SELECT S.Securities_Id, S.Security_Name, 
                (SELECT COUNT(HP.*) FROM Historical_Prices HP WHERE HP.Securities_Id = S.Securities_Id) NoOfRecords, 
                (SELECT COALESCE(MAX(HP.Price_Date),'1900-01-01') FROM Historical_Prices HP WHERE HP.Securities_Id = S.Securities_Id) MaxDate 
            FROM Securities S ORDER BY S.Security_Name ASC
        """, conn)
        
        if df_inv_secs.empty:
            st.warning("⚠️ No Securities found. Define a Security in Settings.")
        else:
            selected_inv_sec = st.selectbox(
                "Select Security:", 
                df_inv_secs.to_dict('records'), 
                format_func=lambda x: f"{x['security_name']} ({x['noofrecords']:,.0f}) ({x['maxdate']})",
                key="security_select"
            )
            inv_sec_id = selected_inv_sec['securities_id']
        
        df_hpr_tx = pd.read_sql(f"SELECT * FROM Historical_Prices WHERE Securities_Id = {inv_sec_id} ORDER BY Price_Date DESC", conn)
        edited_hpr_tx = st.data_editor(df_hpr_tx, num_rows="dynamic", key=f"inv_hpr_editor_{inv_sec_id}", width="stretch")
        save_changes_mid(edited_hpr_tx, "Historical_Prices", id_cols=["securities_id", "price_date"], filter_col="securities_id", filter_val=inv_sec_id)
        
        st.subheader("🔄 Update Prices")
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_label_price = st.selectbox(
                "Select time period:", 
                list(period_options.keys()), 
                index=1,
                key="price_period_select"
            )
            ts_period_price = period_options[selected_label_price]
        with col2:
            if st.button("🚀 Download Prices", key="download_price_btn"):
                with st.spinner("Processing..."):
                    download_historical_prices_from_yahoo(ts_period_price)
                    st.balloons()
                    st.rerun()