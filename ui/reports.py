import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from database.connection import get_db
from database.crud import update_holdings, save_nwr_account_selection
from database.queries import (
    get_category_hierarchy, get_hist_inv_positions_data,
    get_net_worth_report_data, get_all_accounts_for_nwr, get_nwr_account_selection,
    get_nwr_security_detail,
    get_pnl_report_data, get_income_expense_data, get_portfolio_signals,
    get_cash_flow_forecast, get_dividend_tracker_data, get_asset_allocation_data,
    get_fx_exposure_data, get_bond_schedule_data,
)
from data.downloaders import download_historical_fx, download_historical_prices_from_tradingview, download_historical_prices_from_yahoo, download_bond_prices_from_solidus, download_securities_info_from_yahoo
from ui.components import color_negative_red, color_value, custom_metric, get_color, copy_df_button
from datetime import datetime, timedelta

def render_reports():
    """Render the Reports page."""
    st.title("Reports")
    
    hist_sub_menu = st.sidebar.radio(
        "Select Report:",
        [
            "Net Worth Report",
            "Historical Investment Positions",
            "P&L Reports",
            "Securities & Portfolio Analysis",
            "Income & Expense",
            "Dividend Tracker",
            "Cash Flow Forecast",
            "Asset Allocation",
            "FX Exposure",
            "Bond Schedule",
        ],
        key="hist_sub_nav"
    )
    
    if hist_sub_menu == "Net Worth Report":
        render_net_worth_report()

    elif hist_sub_menu == "Historical Investment Positions":
        st.subheader("📈 Investments Position Progress (Monthly)")
        
        # ... (Start Date logic remains same) ...
        last_day_prev_month = pd.Timestamp.now().replace(day=1) - pd.Timedelta(days=1)
        
        min_inv_date = st.sidebar.date_input(
            "📅 Start Date",
            value=st.session_state.inv_date_val,
            min_value=datetime(1900, 1, 1),
            max_value=last_day_prev_month,
            key="inv_date"
        )
        st.session_state.inv_date_val = min_inv_date

        if st.sidebar.button("🔄 Refresh Positions"):
            get_hist_inv_positions_data.clear()
            st.cache_data.clear()
            st.rerun()

        df_raw = get_hist_inv_positions_data(min_inv_date)
        
        if df_raw.empty:
            st.warning("No data found for the selected period.")
            return

        # 1. Προετοιμασία δεδομένων για το Γράφημα (Pivot by Account)
        df_acc_daily = df_raw.groupby(['date', 'accounts_name'])['value_in_eur'].sum().reset_index()
        
        # Υπολογισμός Total για κάθε ημερομηνία
        df_total = df_raw.groupby('date')['value_in_eur'].sum().reset_index()
        df_total['accounts_name'] = 'TOTAL'
        
        # Ένωση για το γράφημα
        df_plot = pd.concat([df_acc_daily, df_total])
        df_pivot = df_plot.pivot(index='date', columns='accounts_name', values='value_in_eur').fillna(0).reset_index()

        tab_graph, tab_data, tab_details = st.tabs(["📊 Graph", "📋 Summary per Account", "🔍 Detail Analysis"])
        
        with tab_graph:
            fig = px.line(df_pivot, x="date", y=[c for c in df_pivot.columns if c != 'date'],
                         title="<b>Investment Value per Account</b>", template="plotly_dark")
            fig.for_each_trace(lambda t: t.update(line=dict(color="white", width=4) if t.name == "TOTAL" else dict(width=2)))
            st.plotly_chart(fig, width='stretch')

            # Drawdown chart
            st.markdown("#### 📉 Portfolio Drawdown")
            df_dd = df_total.copy().sort_values('date')
            df_dd['peak'] = df_dd['value_in_eur'].cummax()
            df_dd['drawdown_pct'] = (df_dd['value_in_eur'] - df_dd['peak']) / df_dd['peak'].replace(0, float('nan')) * 100
            max_dd = df_dd['drawdown_pct'].min()
            st.caption(f"Max drawdown over the selected period: **{max_dd:.2f}%**")
            fig_dd = px.area(
                df_dd, x='date', y='drawdown_pct',
                title="<b>Drawdown from Peak (%)</b>",
                template="plotly_dark",
                color_discrete_sequence=["#E74C3C"],
                labels={'drawdown_pct': 'Drawdown (%)', 'date': 'Date'}
            )
            fig_dd.update_layout(yaxis_tickformat='.1f', margin=dict(l=0, r=0, t=50, b=0))
            st.plotly_chart(fig_dd, width='stretch')
            

        with tab_data:
            # 1. Προετοιμασία δεδομένων
            df_summary = df_pivot.sort_values('date', ascending=False).copy()
            df_summary['date'] = pd.to_datetime(df_summary['date']).dt.strftime('%Y-%m-%d')
            
            # 2. Δυναμικός εντοπισμός των αριθμητικών στηλών (όλες εκτός από την 'date')
            numeric_cols = [col for col in df_summary.columns if col != 'date']

            # 3. Δημιουργία του configuration
            col_config = {
                # Προσθήκη του "Date" header
                "date": st.column_config.TextColumn("Date"),
            }

            # Προσθήκη των αριθμητικών στηλών στο υπάρχον dictionary
            for col in numeric_cols:
                col_config[col] = st.column_config.NumberColumn(
                    label=col,
                    format="%,.2f €"
                )

            df_summary = df_summary.set_index('date')
           
            # 4. Εμφάνιση του πίνακα με το config
            st.dataframe(
                df_summary,
                hide_index=False,
                width='stretch',
                column_config=col_config,
            )
            copy_df_button(df_summary, key="dl_rpt_hist_inv_summary")

        with tab_details:
            st.markdown("### 🔍 Drill-down per Security")
            
            # Φίλτρο ημερομηνίας για τις λεπτομέρειες
            available_dates = sorted(df_raw['date'].unique(), reverse=True)
            selected_date = st.selectbox("Select Snapshot Date:", available_dates)
            
            df_snapshot = df_raw[df_raw['date'] == selected_date].copy()
            
            st.dataframe(
                df_snapshot[['accounts_name', 'securities_name', 'qty_at_date', 'price_at_date', 'value_in_eur']],
                column_config={
                    "accounts_name": "Account",
                    "securities_name": "Security",
                    "qty_at_date": st.column_config.NumberColumn("Quantity", format="%,.8f"),
                    "price_at_date": st.column_config.NumberColumn("Price", format="%,.2f"),
                    "value_in_eur": st.column_config.NumberColumn("Value (€)", format="%,.2f €")
                },
                hide_index=True,
                width="stretch"
            )
            copy_df_button(df_snapshot, key="dl_rpt_hist_inv_detail")
    
    elif hist_sub_menu == "P&L Reports":
        tab_report, tab_movers, tab_savings = st.tabs(["📊 P&L Report", "🚀 Top Movers", "💰 Savings"])
        
        with tab_report:
            st.subheader("📈 Investments Profit & Loss")
         
            df_pnl = get_pnl_report_data()
            if df_pnl is not None:
                if df_pnl.empty:
                    st.warning("No P&L data found. Please refresh the report.")
                    return
            else:
                st.error("Error fetching P&L data. Please try refreshing.")
                return

            if st.sidebar.button("🔄 Recalculate P&L", key="recalculate_pnl_btn"):
                get_pnl_report_data.clear()
                with st.spinner("Running :green[update_holdings()]"):
                    update_holdings()
                st.cache_data.clear()
                st.rerun()

            if st.sidebar.button ("🔄 Refresh Market Data", key="refresh_market_data_btn"):
                get_pnl_report_data.clear()
                with st.spinner("Running :green[download_historical_fx('2d')]"):
                    download_historical_fx("2d")
                with st.spinner("Running :green[download_historical_prices_from_yahoo('2d')]"):
                    download_historical_prices_from_yahoo("2d")
                with st.spinner("Running :green[download_historical_prices_from_tradingview('2d')]"):
                    download_historical_prices_from_tradingview("2d")
                with st.spinner("Running :green[download_bond_prices_from_solidus()]"):
                    download_bond_prices_from_solidus()
                with st.spinner("Running :green[update_holdings()]"):
                    update_holdings()
                st.cache_data.clear()
                st.rerun()
            
            try:
                summary = {
                    "Total Current Value": df_pnl['current_value_eur'].sum(),
                    "Total Realized P&L": df_pnl['realized_pnl_eur'].fillna(0).sum(),
                    "Total Unrealized P&L": df_pnl['unrealized_pnl_eur'].fillna(0).sum(),
                    "Total Net All Time P&L": df_pnl['pnl_net_all_time_eur'].fillna(0).sum(),
                    "Total Daily P&L": df_pnl['pnl_dtd_eur'].sum(),
                    "Total Weekly P&L": df_pnl['pnl_wtd_eur'].sum(),
                    "Total Monthly P&L": df_pnl['pnl_mtd_eur'].sum(),
                    "Total Quarterly P&L": df_pnl['pnl_qtd_eur'].sum(),
                    "Total YTD P&L": df_pnl['pnl_ytd_eur'].sum(),
                    "Total Daily Market P&L": df_pnl['pnl_dtd_market_eur'].sum(),
                    "Total Daily FX P&L": df_pnl['pnl_dtd_fx_eur'].sum(),
                    "Total YTD Market P&L": df_pnl['pnl_ytd_market_eur'].sum(),
                    "Total YTD FX P&L": df_pnl['pnl_ytd_fx_eur'].sum(),
                    "Total YTD Realized P&L": df_pnl['realized_pnl_ytd_eur'].fillna(0).sum(),
                    "Total YTD Unrealized P&L": df_pnl['unrealized_pnl_ytd_eur'].fillna(0).sum()
                }

                row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)

                with row1_col1:
                    total_dtd_pnl = df_pnl['pnl_dtd_eur'].sum()
                    total_current_value = df_pnl['current_value_eur'].sum()
                    st.metric("Total Current Value (EUR)", f"{total_current_value:,.2f} €", delta=f"{total_dtd_pnl:,.2f} €")
 
                with row1_col2:
                    total_realized_pnl = df_pnl['realized_pnl_eur'].fillna(0).sum()
                    custom_metric(
                        label="Total Realized P&L", 
                        value=f"{total_realized_pnl:,.2f} €", 
                        pnl_value=total_realized_pnl
                    )

                with row1_col3:
                    total_unrealized_pnl = df_pnl['unrealized_pnl_eur'].fillna(0).sum()
                    custom_metric(
                        label="Total Unrealized P&L", 
                        value=f"{total_unrealized_pnl:,.2f} €", 
                        pnl_value=total_unrealized_pnl
                    )

                with row1_col4:
                    total_net_all_time_pnl = df_pnl['pnl_net_all_time_eur'].fillna(0).sum()
                    custom_metric(
                        label="Total Net All Time P&L", 
                        value=f"{total_net_all_time_pnl:,.2f} €", 
                        pnl_value=total_net_all_time_pnl
                    )

                row2_col1, row2_col2, row2_col3, row2_col4, row2_col5 = st.columns(5)

                with row2_col1:
                    total_dtd_pnl = df_pnl['pnl_dtd_eur'].sum()
                    custom_metric(
                        label="Total Daily P&L", 
                        value=f"{total_dtd_pnl:,.2f} €", 
                        pnl_value=total_dtd_pnl
                    )

                with row2_col2:
                    total_wtd_pnl = df_pnl['pnl_wtd_eur'].sum()
                    custom_metric(
                        label="Total Weekly P&L", 
                        value=f"{total_wtd_pnl:,.2f} €", 
                        pnl_value=total_wtd_pnl
                    )

                with row2_col3:
                    total_mtd_pnl = df_pnl['pnl_mtd_eur'].sum()
                    custom_metric(
                        label="Total Monthly P&L", 
                        value=f"{total_mtd_pnl:,.2f} €", 
                        pnl_value=total_mtd_pnl
                    )

                with row2_col4:                    
                    total_qtd_pnl = df_pnl['pnl_qtd_eur'].sum()
                    custom_metric(
                        label="Total Quarterly P&L",
                        value=f"{total_qtd_pnl:,.2f} €",
                        pnl_value=total_qtd_pnl
                    )

                with row2_col5:
                    total_ytd_pnl = df_pnl['pnl_ytd_eur'].sum()
                    custom_metric(
                        label="Total YTD P&L", 
                        value=f"{total_ytd_pnl:,.2f} €", 
                        pnl_value=total_ytd_pnl
                    )


            #    st.write("---") # Διαχωριστική γραμμή
                m_col1, m_col2, m_col3, m_col4, m_col5, m_col6 = st.columns(6)


                with m_col1:
                    # Χρήση f-string και πρόσβαση στο dictionary με []
                    st.markdown(f"""
                        <div style="line-height: 1.5;text-align: center;">
                            <p style="color: grey; font-size: 14px; margin: 0; font-family: sans-serif;">Daily Market / FX Split</p>
                            <p style="margin: 0; font-weight: bold;">
                                <span style="color: {get_color(summary['Total Daily Market P&L'])};">{summary['Total Daily Market P&L']:+,.2f} €</span> / 
                                <span style="color: {get_color(summary['Total Daily FX P&L'])};">{summary['Total Daily FX P&L']:+,.2f} €</span>
                            </p>
                        </div>
                    """, unsafe_allow_html=True)

                with m_col5:
                    st.markdown(f"""
                        <div style="line-height: 1.5;text-align: center;">
                            <p style="color: grey; font-size: 14px; margin: 0; font-family: sans-serif;">YTD Realized / Unrealized</p>
                            <p style="margin: 0; font-weight: bold;">
                                <span style="color: {get_color(summary['Total YTD Realized P&L'])};">{summary['Total YTD Realized P&L']:+,.2f} €</span> / 
                                <span style="color: {get_color(summary['Total YTD Unrealized P&L'])};">{summary['Total YTD Unrealized P&L']:+,.2f} €</span>
                            </p>
                        </div>
                    """, unsafe_allow_html=True)

                with m_col6:
                    st.markdown(f"""
                        <div style="line-height: 1.5;text-align: center;">
                            <p style="color: grey; font-size: 14px; margin: 0; font-family: sans-serif;">YTD Market / FX</p>
                            <p style="margin: 0; font-weight: bold;">
                                <span style="color: {get_color(summary['Total YTD Market P&L'])};">{summary['Total YTD Market P&L']:+,.2f} €</span> / 
                                <span style="color: {get_color(summary['Total YTD FX P&L'])};">{summary['Total YTD FX P&L']:+,.2f} €</span>
                            </p>
                        </div>
                    """, unsafe_allow_html=True)


                # 1. Καθαρισμός από τυχόν προϋπάρχοντα σύνολα της SQL για αποφυγή double-counting
                df_input = df_pnl[df_pnl['securities_name'].notnull()].copy()

                # 2. Υπολογισμός Κόστους μόνο για τις ανοιχτές θέσεις (Denominator για το YOC)
                # Το Current Value - Unrealized P&L μας δίνει το κεφάλαιο που είναι "κλειδωμένο" τώρα
                df_input['current_cost_basis_eur'] = df_input['current_value_eur'] - df_input['unrealized_pnl_eur']

                # 3. Υπολογισμός αναμενόμενου μερίσματος (Numerator για το YOC)
                df_input['annual_div_cash_eur'] = (df_input['dividend_yoc_pct'] / 100) * df_input['current_cost_basis_eur']

                # 4. Ομαδοποίηση
                agg_dict = {
                    'current_value_eur': 'sum',
                    'pnl_dtd_market_eur': 'sum',
                    'pnl_dtd_fx_eur': 'sum',
                    'pnl_dtd_eur': 'sum',
                    'pnl_wtd_eur': 'sum',
                    'pnl_mtd_eur': 'sum',
                    'pnl_qtd_eur': 'sum',
                    'pnl_ytd_market_eur': 'sum',
                    'pnl_ytd_fx_eur': 'sum',
                    'realized_pnl_ytd_eur': 'sum',
                    'unrealized_pnl_ytd_eur': 'sum',
                    'pnl_ytd_eur': 'sum',
                    'pnl_all_time_eur': 'sum',
                    'realized_pnl_eur': 'sum',
                    'unrealized_pnl_eur': 'sum',
                    'pnl_net_all_time_eur': 'sum',
                    'annual_div_cash_eur': 'sum', # Προσθέτουμε τα ευρώ των μερισμάτων
                    'current_cost_basis_eur': 'sum' # Προσθέτουμε το κόστος βάσης
                }

                # Προσθέστε δυναμικά όποιες άλλες στήλες P&L έχετε (DTD Market, κλπ)
                for col in df_input.columns:
                    if col.startswith('pnl_') and col not in agg_dict:
                        agg_dict[col] = 'sum'

                df_acc = df_input.groupby('accounts_name').agg(agg_dict)

                # 5. Υπολογισμός του σταθμισμένου Yield on Cost για τον λογαριασμό
                # (Συνολικά αναμενόμενα ευρώ μερισμάτων / Συνολικό τρέχον κόστος αγοράς) * 100
                df_acc['dividend_yoc_pct'] = (df_acc['annual_div_cash_eur'] / df_acc['current_cost_basis_eur'].replace(0, np.nan)) * 100
                df_acc['dividend_yoc_pct'] = df_acc['dividend_yoc_pct'].fillna(0)

                # 6. Καθαρισμός βοηθητικών στηλών πριν το display
            #    df_acc = df_acc.drop(columns=['annual_div_cash_eur', 'current_cost_basis_eur'])
                df_acc = df_acc.drop(columns=['pnl_all_time_eur','annual_div_cash_eur', 'current_cost_basis_eur'])

                st.divider() # Optional separation line for better visual effect

                # 1. Δημιουργία στηλών για τα φίλτρα και το Selectbox
                col1, col2, col3 = st.columns([1, 1, 2])

                with col1:
                    show_market_fx_split = st.checkbox("Show Market/FX Split", value=False)
                with col2:
                    show_realized_unrealized_split = st.checkbox("Show Realized/Unrealized Split", value=False)

                if not show_market_fx_split:
                    df_acc = df_acc.drop(columns=['pnl_dtd_market_eur','pnl_dtd_fx_eur', 'pnl_ytd_market_eur', 'pnl_ytd_fx_eur'])
                if not show_realized_unrealized_split:
                    df_acc = df_acc.drop(columns=['realized_pnl_ytd_eur', 'unrealized_pnl_ytd_eur', 'realized_pnl_eur', 'unrealized_pnl_eur'])

                df_acc = df_acc.rename(columns={
                    'current_value_eur': 'Current Value',
                    'pnl_dtd_market_eur': 'Daily Market P&L',
                    'pnl_dtd_fx_eur': 'Daily FX P&L',
                    'pnl_dtd_eur': 'Daily P&L',
                    'pnl_wtd_eur': 'Weekly P&L',
                    'pnl_mtd_eur': 'Monthly P&L',
                    'pnl_qtd_eur': 'Quarterly P&L',
                    'pnl_ytd_market_eur': 'YTD Market P&L',
                    'pnl_ytd_fx_eur': 'YTD FX P&L',
                    'realized_pnl_ytd_eur': 'YTD Realized P&L',
                    'unrealized_pnl_ytd_eur': 'YTD Unrealized P&L',
                    'pnl_ytd_eur': 'YTD P&L',
                #    'pnl_all_time_eur': 'Total P&L',
                    'realized_pnl_eur': 'Realized P&L',
                    'unrealized_pnl_eur': 'Unrealized P&L',
                    'pnl_net_all_time_eur': 'Total Net P&L',
                    'dividend_yoc_pct': 'Annual YOC %'
                })

                df_acc.index.name = "Account"
            #    st.dataframe(df_acc.style.map(color_negative_red).format("{:,.2f} €"), width="stretch")

                # Ορίζουμε τις στήλες που θέλουν σύμβολο € (όλες εκτός από το YOC)
                euro_cols = [col for col in df_acc.columns if col != 'Annual YOC %']

                st.dataframe(
                    df_acc.style
                    .map(color_negative_red)
                    .format({
                        **{col: "{:,.2f} €" for col in euro_cols}, # Όλα τα υπόλοιπα σε €
                        'Annual YOC %': "{:.4f}%"                  # Το YOC σε %
                    }),
                    width="stretch"
                )
                copy_df_button(df_acc, key="dl_rpt_pnl_account")



                # 1. Δημιουργία στηλών για τα φίλτρα και το Selectbox
                col1, col2, col3 = st.columns([1, 1, 2])

                with col1:
                    show_closed_accounts = st.checkbox("Show Closed Accounts", value=False)
                with col2:
                    show_closed_positions = st.checkbox("Show Closed Positions", value=False)

                # 2. Φιλτράρισμα Λογαριασμών (βάσει current_value_eur)
                if show_closed_accounts:
                    acc_options = df_pnl['accounts_name'].unique()
                else:
                    # Μόνο λογαριασμοί που έχουν τουλάχιστον μία εγγραφή με αξία != 0
                    active_accs = df_pnl.groupby('accounts_name')['current_value_eur'].sum()
                    acc_options = active_accs[active_accs != 0].index.tolist()

                with col3:
                    selected_acc = st.selectbox(
                        "Select Account for Details:", 
                        acc_options,
                        key="pnl_account_select"
                    )

                with get_db() as conn:
                    query_acc_id = f"SELECT Accounts_Id FROM Accounts WHERE Accounts_Name = '{selected_acc}'"
                    df_acc_id = pd.read_sql(query_acc_id, conn)

                    if not df_acc_id.empty:
                        account_id = df_acc_id.iloc[0]['accounts_id']
                        query_holdings = f"""
                            SELECT h.Securities_Id, s.Securities_Name, h.Quantity 
                            FROM Holdings h 
                            JOIN Securities s ON h.Securities_Id = s.Securities_Id 
                            WHERE h.Accounts_Id = {account_id}
                        """
                        df_holdings = pd.read_sql(query_holdings, conn)

                        query_prices = f"""
                            SELECT DISTINCT ON (h.Securities_Id) 
                                h.Securities_Id, 
                                s.Securities_Name,
                                h.Quantity,
                                (SELECT hp.Close 
                                FROM Historical_Prices hp 
                                WHERE hp.Securities_Id = h.Securities_Id 
                                AND hp.Date <= CURRENT_DATE 
                                ORDER BY hp.Date DESC LIMIT 1) AS Latest_Price
                            FROM Holdings h
                            JOIN Securities s ON h.Securities_Id = s.Securities_Id
                            WHERE h.Accounts_Id = {account_id} AND h.Quantity != 0
                        """
                        df_prices = pd.read_sql(query_prices, conn)
                    else:
                        df_acc_id = pd.DataFrame()
                        df_holdings = pd.DataFrame()
                        df_prices = pd.DataFrame()

                if not df_acc_id.empty:
                    account_id = df_acc_id.iloc[0]['accounts_id']
                    
                    df_details = df_pnl[df_pnl['accounts_name'] == selected_acc].copy()
                    
                    if not df_holdings.empty:
                        df_details = df_details.merge(
                            df_holdings[['securities_name', 'quantity']], 
                            on='securities_name', 
                            how='left'
                        )
                    else:
                        df_details['quantity'] = 0
                    
                    if not df_prices.empty:
                        df_details = df_details.merge(
                            df_prices[['securities_name', 'latest_price']], 
                            on='securities_name', 
                            how='left'
                        )
                    else:
                        df_details['latest_price'] = 0
                    
                    df_details['quantity'] = df_details['quantity'].fillna(0)
                    df_details['latest_price'] = df_details['latest_price'].fillna(0)

                    df_display = df_details[[
                        'securities_name', 'quantity', 'latest_price', 'current_value_eur', 
                        'pnl_dtd_market_eur', 'pnl_dtd_fx_eur',
                        'pnl_dtd_eur', 'pnl_wtd_eur', 'pnl_mtd_eur', 'pnl_qtd_eur',
                        'pnl_ytd_market_eur', 'pnl_ytd_fx_eur',
                        'realized_pnl_ytd_eur', 'unrealized_pnl_ytd_eur', 
                        'pnl_ytd_eur', 'realized_pnl_eur', 'unrealized_pnl_eur', 'pnl_net_all_time_eur', 'dividend_yoc_pct'
                    ]].rename(columns={
                        'securities_name': 'Security',
                        'quantity': 'Quantity',        
                        'latest_price': 'Latest Price',      
                        'current_value_eur': 'Value (€)',
                        'pnl_dtd_market_eur': 'Daily Market P&L',
                        'pnl_dtd_fx_eur': 'Daily FX P&L',
                        'pnl_dtd_eur': 'Daily P&L',
                        'pnl_wtd_eur': 'Weekly P&L',
                        'pnl_mtd_eur': 'Monthly P&L',
                        'pnl_qtd_eur': 'Quarterly P&L',
                        'pnl_ytd_market_eur': 'YTD Market P&L',
                        'pnl_ytd_fx_eur': 'YTD FX P&L',''
                        'realized_pnl_ytd_eur': 'YTD Realized P&L',
                        'unrealized_pnl_ytd_eur': 'YTD Unrealized P&L',
                        'pnl_ytd_eur': 'YTD P&L',
                    #    'pnl_all_time_eur': 'Total P&L',
                        'realized_pnl_eur': 'Realized P&L',
                        'unrealized_pnl_eur': 'Unrealized P&L',
                        'pnl_net_all_time_eur': 'Total Net P&L',
                        'dividend_yoc_pct': 'Annual YOC %'
                    })

                    # 3. Φιλτράρισμα Κλειστών Θέσεων στο τελικό Dataframe
                    if not show_closed_positions:
                        df_display = df_display[df_display['Value (€)'] != 0]


                    pnl_cols = ['Daily Market P&L', 'Daily FX P&L', 'Daily P&L', 'Weekly P&L', 'Monthly P&L', 'Quarterly P&L', 'YTD Market P&L', 'YTD FX P&L', 'YTD Realized P&L', 'YTD Unrealized P&L', 'YTD P&L', 'Realized P&L', 'Unrealized P&L', 'Total Net P&L', 'Annual YOC %']

                    # 1. Set 'Security' as the index so Streamlit treats it as the frozen lead column
                    df_to_show = df_display.set_index('Security')

                    if not show_market_fx_split:
                        df_to_show = df_to_show.drop(columns=['Daily Market P&L','Daily FX P&L', 'YTD Market P&L', 'YTD FX P&L'])
                    #    pnl_cols = ['Daily P&L', 'Weekly P&L', 'Monthly P&L', 'Quarterly P&L', 'YTD Realized P&L', 'YTD Unrealized P&L', 'YTD P&L', 'Realized P&L', 'Unrealized P&L', 'Total Net P&L', 'Annual YOC %']

                    if not show_realized_unrealized_split:
                        df_to_show = df_to_show.drop(columns=['YTD Realized P&L', 'YTD Unrealized P&L', 'Realized P&L', 'Unrealized P&L'])
                    #    pnl_cols = ['Daily Market P&L', 'Daily FX P&L', 'Daily P&L', 'Weekly P&L', 'Monthly P&L', 'Quarterly P&L', 'YTD Market P&L', 'YTD FX P&L', 'YTD P&L', 'Total Net P&L', 'Annual YOC %']

                    existing_pnl_cols = [col for col in pnl_cols if col in df_to_show.columns]

                    st.dataframe(
                    #    df_display.style
                        df_to_show.style
                    #    .map(color_negative_red, subset=pnl_cols)
                        .map(color_negative_red, subset=existing_pnl_cols)
                        .format({
                            # P&L columns
                    #        **{col: "{:,.2f} €" for col in pnl_cols},
                            **{col: "{:,.2f} €" for col in existing_pnl_cols},
                            # Value column
                            'Value (€)': "{:,.2f} €",
                            # Price and Quantity columns
                            'Latest Price': "{:,.2f}",
                            'Quantity': "{:,.8f}",
                            'Annual YOC %': "{:.4f}%"
                        }),
                        width="stretch",
                        hide_index=False
                    )
                    copy_df_button(df_to_show, key="dl_rpt_pnl_positions")

                else:
                    st.warning(f"No account found with name: {selected_acc}")
                    
            except Exception as e:
                st.error(f"Error: {e}")
                st.exception(e)

        with tab_movers:
            st.subheader("🔝 Investment Top Movers (Daily)")
            
            mover_col = st.radio(
                "Sort by:", 
                ["Daily P&L (€)", "Daily Change (%)"], 
                horizontal=True,
                key="mover_sort_radio"
            )
            df_pnl = get_pnl_report_data()
            df_pnl['daily_change_pct'] = (df_pnl['pnl_dtd_eur'] / (df_pnl['current_value_eur'] - df_pnl['pnl_dtd_eur'])) * 100
            
            df_movers = df_pnl[['securities_name', 'accounts_name', 'pnl_dtd_eur', 'daily_change_pct']].copy()
            df_movers.columns = ['Security', 'Account', 'Daily P&L (€)', 'Daily Change (%)']
            
            col_to_sort = 'Daily P&L (€)' if mover_col == "Daily P&L (€)" else 'Daily Change (%)'
            
            gainer_col, loser_col = st.columns(2)
            
            with gainer_col:
                st.success("📈 Top Gainers")
                top_gainers = df_movers.sort_values(by=col_to_sort, ascending=False).head(10)
                st.dataframe(top_gainers.style.format({
                    'Daily P&L (€)': "{:,.2f} €",
                    'Daily Change (%)': "{:,.2f}%"
                }), hide_index=True, width="stretch",
                column_config={
                    "securities_name": st.column_config.TextColumn("Security", width="small"),
                    "accounts_name": st.column_config.TextColumn("Account", width="small"),
                    "Daily P&L (€)": st.column_config.NumberColumn("P&L (€)", format="%.2f €", width="small"),
                    "Daily Change (%)": st.column_config.NumberColumn("Day %", format="%.2f%%", width="small"),
                }
                )
                copy_df_button(top_gainers, key="dl_rpt_pnl_gainers")

            with loser_col:
                st.error("📉 Top Losers")
                top_losers = df_movers.sort_values(by=col_to_sort, ascending=True).head(10)
                st.dataframe(top_losers.style.format({
                    'Daily P&L (€)': "{:,.2f} €",
                    'Daily Change (%)': "{:,.2f}%"
                }), hide_index=True, width="stretch",
                column_config={
                    "securities_name": st.column_config.TextColumn("Security", width="small"),
                    "accounts_name": st.column_config.TextColumn("Account", width="small"),
                    "Daily P&L (€)": st.column_config.NumberColumn("P&L (€)", format="%.2f €", width="small"),
                    "Daily Change (%)": st.column_config.NumberColumn("Day %", format="%.2f%%", width="small"),
                }
                )
                copy_df_button(top_losers, key="dl_rpt_pnl_losers")

        with tab_savings:
            st.subheader("💰 Savings Accounts — Yield over Cost & APY")
            st.caption(
                "**Principal** = non-interest cash inflows (deposits/transfers in, excluding interest). "
                "**Total Interest** = sum of splits categorised as 'Interest'. "
                "**Cumulative YoC** = Total Interest ÷ Principal × 100. "
                "**APY** = (1 + Total Interest / Principal) ^ (365 / holding days) − 1, "
                "i.e. the compound annualised rate implied by actual interest earned over the holding period."
            )

            with get_db() as conn:
                df_savings = pd.read_sql("""
                    WITH CategorizedSplits AS (
                        SELECT
                            t.Accounts_Id,
                            t.Transactions_Id,
                            t.Date,
                            t.Transfers_Id,
                            CASE WHEN t.Transfers_Id IS NOT NULL THEN t.Total_Amount
                                 ELSE s.Amount
                            END AS Amount,
                            cat.Categories_Type,
                            CASE WHEN t.Transfers_Id IS NOT NULL THEN 'Principal'
                                 WHEN cat.Categories_Type = 'Interest' THEN 'Interest'
                                 ELSE 'Principal'
                            END AS Kind
                        FROM Transactions t--, Accounts a
                        LEFT JOIN Splits s   ON s.Transactions_Id = t.Transactions_Id
                        LEFT JOIN Categories cat ON cat.Categories_Id = s.Categories_Id
						LEFT JOIN Accounts a ON a.Accounts_Id = t.Accounts_Id
						WHERE a.Accounts_Type = 'Savings'
                    ),
                    -- Latest FX rate per non-EUR account for converting to EUR
                    NonEURAccounts AS (
                        SELECT DISTINCT a.Accounts_Id, a.Currencies_Id
                        FROM Accounts a
                        WHERE a.Currencies_Id NOT IN (
                            SELECT Currencies_Id FROM Currencies WHERE Currencies_ShortName = 'EUR'
                        )
                    ),
                    Last_FXRates AS (
                        SELECT nea.Accounts_Id, hfx.FX_Rate
                        FROM Historical_FX hfx
                        JOIN NonEURAccounts nea ON nea.Currencies_Id = hfx.Currencies_Id_1
                        WHERE hfx.Currencies_Id_2 = (
                                SELECT Currencies_Id FROM Currencies WHERE Currencies_ShortName = 'EUR'
                              )
                          AND hfx.Date = (
                                SELECT MAX(h2.Date)
                                FROM Historical_FX h2
                                WHERE h2.Currencies_Id_1 = hfx.Currencies_Id_1
                                  AND h2.Currencies_Id_2 = hfx.Currencies_Id_2
                                  AND h2.Date <= CURRENT_DATE
                              )
                    ),
                    -- Overall account stats (total principal, total interest, date range)
                    AccountStats AS (
                        SELECT
                            cs.Accounts_Id,
                            MIN(cs.Date)  AS first_tx_date,
                            MAX(cs.Date)  AS last_tx_date,
                            MAX(CASE WHEN cs.Kind = 'Interest' THEN cs.Date END) AS last_interest_date,
                            SUM(CASE WHEN cs.Kind = 'Principal'
                                     THEN COALESCE(cs.Amount, 0) ELSE 0 END)     AS principal,
                            SUM(CASE WHEN cs.Kind = 'Principal'
                                     THEN COALESCE(cs.Amount, 0) * COALESCE(fx.FX_Rate, 1)
                                     ELSE 0 END)                                 AS principal_eur,
                            SUM(CASE WHEN cs.Kind = 'Interest'
                                     THEN COALESCE(cs.Amount, 0) ELSE 0 END)     AS total_interest,
                            SUM(CASE WHEN cs.Kind = 'Interest'
                                     THEN COALESCE(cs.Amount, 0) * COALESCE(fx.FX_Rate, 1)
                                     ELSE 0 END)                                 AS total_interest_eur
                        FROM CategorizedSplits cs
                        LEFT JOIN Last_FXRates fx ON fx.Accounts_Id = cs.Accounts_Id
                        GROUP BY cs.Accounts_Id
                    ),
                    -- The two most recent interest payment dates per account
                    InterestDates AS (
                        SELECT cs.Accounts_Id, cs.Date AS interest_date,
                               ROW_NUMBER() OVER (
                                   PARTITION BY cs.Accounts_Id
                                   ORDER BY cs.Date DESC
                               ) AS rn
                        FROM (
                            SELECT DISTINCT Accounts_Id, Date
                            FROM CategorizedSplits
                            WHERE Kind = 'Interest'
                        ) cs
                    ),
                    LastInterestDate  AS (SELECT Accounts_Id, interest_date AS last_interest_date
                                          FROM InterestDates WHERE rn = 1),
                    PriorInterestDate AS (SELECT Accounts_Id, interest_date AS prior_interest_date
                                          FROM InterestDates WHERE rn = 2),
                    -- Interest earned in the last interest period
                    -- (from prior interest date up to and including last interest date)
                    LastPeriodInterest AS (
                        SELECT cs.Accounts_Id,
                               SUM(cs.Amount) AS last_interest_sum,
                               SUM(cs.Amount * COALESCE(fx.FX_Rate, 1)) AS last_interest_sum_eur
                        FROM CategorizedSplits cs
                        JOIN LastInterestDate li ON li.Accounts_Id = cs.Accounts_Id
                        LEFT JOIN PriorInterestDate pi ON pi.Accounts_Id = cs.Accounts_Id
                        LEFT JOIN Last_FXRates fx ON fx.Accounts_Id = cs.Accounts_Id
                        WHERE cs.Kind = 'Interest'
                          AND cs.Date = li.last_interest_date
                        GROUP BY cs.Accounts_Id
                    ),
                    -- Running balance at the start of the last interest period.
                    -- = sum of all principal movements up to and including the prior interest date
                    -- (or up to first tx if no prior interest date exists).
                    PeriodStartBalance AS (
                        SELECT cs.Accounts_Id,
                               SUM(cs.Amount) AS period_start_balance,
                               SUM(cs.Amount * COALESCE(fx.FX_Rate, 1)) AS period_start_balance_eur
                        FROM CategorizedSplits cs
                        LEFT JOIN PriorInterestDate pi ON pi.Accounts_Id = cs.Accounts_Id
                        LEFT JOIN Last_FXRates fx ON fx.Accounts_Id = cs.Accounts_Id
						WHERE cs.Date <= pi.prior_interest_date										  
                        GROUP BY cs.Accounts_Id
                    ),
                    PeriodEndBalance AS (
                        SELECT cs.Accounts_Id,
                               SUM(cs.Amount) AS period_end_balance,
                               SUM(cs.Amount * COALESCE(fx.FX_Rate, 1)) AS period_end_balance_eur
                        FROM CategorizedSplits cs
                        LEFT JOIN LastInterestDate li ON li.Accounts_Id = cs.Accounts_Id
                        LEFT JOIN Last_FXRates fx ON fx.Accounts_Id = cs.Accounts_Id
						WHERE cs.Date < li.last_interest_date										  
                        GROUP BY cs.Accounts_Id
                    ),
                    -- 1. Παραγωγή όλων των ημερών μεταξύ Prior και Last Interest Date
                    PeriodDates AS (
                        SELECT 
                            pid.Accounts_Id,
                            pid.prior_interest_date + generate_series(0, (lid.last_interest_date - pid.prior_interest_date)-1)::int AS calendar_day
                        FROM PriorInterestDate pid
                        JOIN LastInterestDate lid ON pid.Accounts_Id = lid.Accounts_Id
                    ),
                    -- 2. Υπολογισμός τρέχοντος υπολοίπου για κάθε ημέρα της περιόδου
                    DailyBalances AS (
                        SELECT 
                            pd.Accounts_Id,
                            pd.calendar_day,
                            (SELECT SUM(cs.Amount) 
                             FROM CategorizedSplits cs 
                             WHERE cs.Accounts_Id = pd.Accounts_Id 
                               AND cs.Date <= pd.calendar_day) AS daily_balance
                        FROM PeriodDates pd
                    ),
                    -- 3. Υπολογισμός Μέσου Όρου
                    PeriodAverageBalance AS (
                        SELECT 
                            dbal.Accounts_Id,
                            AVG(dbal.daily_balance) AS avg_period_balance,
                            -- Μετατροπή σε EUR αν χρειάζεται (χρησιμοποιώντας το fx rate που ορίσατε πριν)
                            AVG(dbal.daily_balance) * COALESCE(fx.FX_Rate, 1) AS avg_period_balance_eur
						FROM DailyBalances dbal
                        LEFT JOIN Last_FXRates fx ON fx.Accounts_Id = dbal.Accounts_Id
                        GROUP BY dbal.Accounts_Id, COALESCE(fx.FX_Rate, 1)
                    )
					SELECT
                        a.Accounts_Id,
                        a.Accounts_Name,
                        a.Accounts_Type,
                        c.Currencies_ShortName          AS currency,
                        a.Accounts_Balance              AS current_balance,
                        ast.first_tx_date,
                        ast.last_tx_date,
                        ast.last_interest_date,
                        ast.principal,
                        ast.principal_eur,
                        ast.total_interest,
                        ast.total_interest_eur,
                        -- Last interest period fields
                        pid.prior_interest_date,
                        lpi.last_interest_sum,
                        lpi.last_interest_sum_eur,
                        psb.period_start_balance,
                        psb.period_start_balance_eur,
                        pse.period_end_balance,
                        pse.period_end_balance_eur,
                        pab.avg_period_balance,
                        pab.avg_period_balance_eur						
					FROM Accounts a
                    JOIN Currencies c       ON c.Currencies_Id  = a.Currencies_Id
                    LEFT JOIN AccountStats ast ON ast.Accounts_Id = a.Accounts_Id
                    LEFT JOIN PriorInterestDate pid ON pid.Accounts_Id = a.Accounts_Id
                    LEFT JOIN LastPeriodInterest lpi ON lpi.Accounts_Id = a.Accounts_Id
                    LEFT JOIN PeriodStartBalance psb ON psb.Accounts_Id = a.Accounts_Id
                    LEFT JOIN PeriodEndBalance pse ON pse.Accounts_Id = a.Accounts_Id
                    LEFT JOIN PeriodAverageBalance pab ON a.Accounts_Id = pab.Accounts_Id					
                    WHERE a.Accounts_Type = 'Savings'
                    ORDER BY a.Accounts_Name;
                """, conn)

            if df_savings.empty:
                st.info("No Savings accounts found.")
            else:
                df_savings.columns = [c.lower() for c in df_savings.columns]

                # Parse dates
                for _dc in ['first_tx_date', 'last_tx_date', 'last_interest_date',
                            'last_interest_date', 'prior_interest_date']:
                    df_savings[_dc] = pd.to_datetime(df_savings[_dc])

                # ── Overall period calculations ───────────────────────────────
                df_savings['holding_days_total'] = (
                    (df_savings['last_tx_date'] - df_savings['first_tx_date'])
                    .dt.days.clip(lower=1)
                )
                _principal = df_savings['principal'].replace(0, float('nan'))

                # Annualised interest over full holding period
                df_savings['annual_interest_cash'] = (
                    df_savings['total_interest'] / df_savings['holding_days_total'] * 365
                )
                df_savings['annual_yoc_pct'] = (
                    df_savings['annual_interest_cash'] / _principal * 100
                ).fillna(0)

                _r_total = df_savings['total_interest'] / _principal
                df_savings['apy_pct'] = (
                    ((1 + _r_total) ** (365 / df_savings['holding_days_total']) - 1) * 100
                ).fillna(0)

                # ── Last interest period calculations ─────────────────────────
                # Period = prior_interest_date → last_interest_date
                # If no prior interest date (first ever interest payment),
                # fall back to first_tx_date as period start.
                _period_start = df_savings['prior_interest_date'].fillna(
                    df_savings['first_tx_date']
                )
                df_savings['period_start_date'] = _period_start
                df_savings['holding_days_last'] = (
                    (df_savings['last_interest_date'] - _period_start)
                    .dt.days.clip(lower=1)
                )

                # Average principal during the period, calculated in SQL as the average of daily balances.:
                # (balance at period start + balance at period end) / 2
                # Period-start balance comes from SQL (PeriodStartBalance CTE).
                # Period-end balance ≈ period_start_balance + last_interest_sum
                # (interest is added to the account at period end).
                _apb = df_savings['avg_period_balance'].replace(0, float('nan'))
            #    _psb = df_savings['period_start_balance'].replace(0, float('nan'))
            #    _period_end_balance = _psb + df_savings['last_interest_sum'].fillna(0)
            #    df_savings['avg_principal_last'] = ((_psb + _period_end_balance) / 2)
                df_savings['avg_principal_last'] = _apb

                # Annualise last period interest over actual period length
                df_savings['annual_interest_cash_last'] = (
                    df_savings['last_interest_sum'] / df_savings['holding_days_last'] * 365
                )

                # Annual YOC (last period) = annualised interest / avg principal * 100
            #    _avg_p = df_savings['avg_principal_last'].replace(0, float('nan'))
                _avg_p = df_savings['avg_period_balance'].replace(0, float('nan'))
                df_savings['annual_yoc_pct_last'] = (
                    df_savings['annual_interest_cash_last'] / _avg_p * 100
                ).fillna(0)

                # APY (last period) = (1 + period_interest/avg_principal)^(365/period_days)-1
                _r_last = df_savings['last_interest_sum'] / _avg_p
                df_savings['apy_pct_last'] = (
                    ((1 + _r_last) ** (365 / df_savings['holding_days_last']) - 1) * 100
                ).fillna(0)


                # ── Summary metrics ───────────────────────────────────────────
                m1, m2, m3, m4, m5 = st.columns(5)
                with m1:
                    st.metric("Savings Accounts", len(df_savings))
                with m2:
                    st.metric("Total Principal",
                              f"{df_savings['principal_eur'].sum():,.2f} €")
                with m3:
                    st.metric("Total Interest Received",
                              f"{df_savings['total_interest_eur'].sum():,.2f} €")
                with m4:
                    _avg_yoc = df_savings['annual_yoc_pct'].replace(0, float('nan')).mean()
                    st.metric("Avg Annual YOC",
                              f"{_avg_yoc:.2f}%" if not pd.isna(_avg_yoc) else "N/A")
                with m5:
                    _avg_apy = df_savings['apy_pct'].replace(0, float('nan')).mean()
                    st.metric("Avg APY",
                              f"{_avg_apy:.2f}%" if not pd.isna(_avg_apy) else "N/A")

                st.write("---")

                # ── Bar chart: Annual YOC per account ─────────────────────────
                df_chart = df_savings[df_savings['annual_yoc_pct'] != 0].copy()
                if not df_chart.empty:
                    fig_yoc = px.bar(
                        df_chart.sort_values('annual_yoc_pct', ascending=True),
                        x='annual_yoc_pct',
                        y='accounts_name',
                        orientation='h',
                        color='annual_yoc_pct',
                        color_continuous_scale='RdYlGn',
                        labels={'annual_yoc_pct': 'Annual YOC (%)', 'accounts_name': 'Account'},
                        title='Annual Yield over Cost (%) per Savings Account',
                        template='plotly_dark',
                        text=df_chart['annual_yoc_pct'].apply(lambda x: f"{x:.2f}%"),
                    )
                    fig_yoc.update_traces(textposition='outside')
                    fig_yoc.update_layout(
                        coloraxis_showscale=False,
                        margin=dict(l=0, r=60, t=50, b=0),
                        yaxis_title=None
                    )
                    st.plotly_chart(fig_yoc, width="stretch")

                # ── Detail table ──────────────────────────────────────────────
                st.subheader("Detail")
                df_display = df_savings[[
                    'accounts_name', 'accounts_type', 'currency',
                    'principal', 'total_interest', 'annual_interest_cash',
                    'current_balance', 'annual_yoc_pct', 'apy_pct',
                    'holding_days_total', 'first_tx_date', 'last_tx_date'
                ]].copy()

                df_display['first_tx_date'] = df_display['first_tx_date'].dt.strftime('%Y-%m-%d')
                df_display['last_tx_date']  = df_display['last_tx_date'].dt.strftime('%Y-%m-%d')

                st.dataframe(
                    df_display.style.format({
                        'principal':             "{:,.2f}",
                        'total_interest':        "{:,.2f}",
                        'annual_interest_cash':  "{:,.2f}",
                        'current_balance':       "{:,.2f}",
                        'annual_yoc_pct':        "{:.4f}%",
                        'apy_pct':               "{:.4f}%",
                        'holding_days_total':    "{:,.0f}",
                    }),
                    hide_index=True,
                    width="stretch",
                    column_config={
                        'accounts_name':        'Account',
                        'accounts_type':        'Type',
                        'currency':             'Currency',
                        'principal':            st.column_config.NumberColumn('Principal', format="%,.2f"),
                        'total_interest':       st.column_config.NumberColumn('Total Interest', format="%,.2f"),
                        'annual_interest_cash': st.column_config.NumberColumn('Ann. Interest', format="%,.2f",
                                                    help="Total interest annualised over holding period"),
                        'current_balance':      st.column_config.NumberColumn('Current Balance', format="%,.2f"),
                        'annual_yoc_pct':       st.column_config.NumberColumn('Annual YOC %', format="%.4f%%",
                                                    help="Annualised interest / Principal × 100 — same method as P&L securities"),
                        'apy_pct':              st.column_config.NumberColumn('APY %', format="%.4f%%",
                                                    help="(1 + Interest/Principal)^(365/days) − 1"),
                        'holding_days_total':         st.column_config.NumberColumn('Days Held', format="%,d"),
                        'first_tx_date':        'First Transaction',
                        'last_tx_date':         'Last Transaction',
                    }
                )
                copy_df_button(df_display, key="dl_rpt_savings_detail")

            #    csv = df_display.to_csv(index=False)
            #    st.download_button(
            #        label="📥 Download as CSV",
            #        data=csv,
            #        file_name="savings_yield_over_cost.csv",
            #        mime="text/csv"
            #    )

                # ── Detail table: last interest period ───────────────────────────
                st.subheader("Detail for Last Interest Period")
                st.caption(
                    "Period = prior interest payment date → last interest payment date. "
                    "Annual YOC and APY are calculated using the **average principal** "
                    "during this period and the **actual number of days** in the period."
                )
                df_display2 = df_savings[[
                    'accounts_name', 'accounts_type', 'currency',
                    'avg_principal_last', 'last_interest_sum',
                    'annual_interest_cash_last',
                    'annual_yoc_pct_last', 'apy_pct_last',
                    'holding_days_last', 'period_start_date', 'last_interest_date'
                ]].copy()

                df_display2['period_start_date']   = df_display2['period_start_date'].dt.strftime('%Y-%m-%d')
                df_display2['last_interest_date'] = df_display2['last_interest_date'].dt.strftime('%Y-%m-%d')

                st.dataframe(
                    df_display2.style.format({
                        'avg_principal_last':        "{:,.2f}",
                        'last_interest_sum':         "{:,.2f}",
                        'annual_interest_cash_last': "{:,.2f}",
                        'annual_yoc_pct_last':       "{:.4f}%",
                        'apy_pct_last':              "{:.4f}%",
                        'holding_days_last':         "{:,.0f}",
                    }),
                    hide_index=True,
                    width="stretch",
                    column_config={
                        'accounts_name':            'Account',
                        'accounts_type':            'Type',
                        'currency':                 'Currency',
                        'avg_principal_last':       st.column_config.NumberColumn('Avg Principal',     format="%,.2f",
                                                        help="Average of balance at period start and end"),
                        'last_interest_sum':        st.column_config.NumberColumn('Period Interest',   format="%,.2f"),
                        'annual_interest_cash_last':st.column_config.NumberColumn('Ann. Interest',     format="%,.2f",
                                                        help="Period interest annualised over period days"),
                        'annual_yoc_pct_last':      st.column_config.NumberColumn('Annual YOC %',      format="%.4f%%",
                                                        help="Ann. Interest / Avg Principal × 100"),
                        'apy_pct_last':             st.column_config.NumberColumn('APY %',             format="%.4f%%",
                                                        help="(1 + Period Interest / Avg Principal)^(365/days) − 1"),
                        'holding_days_last':        st.column_config.NumberColumn('Period Days',       format="%,d"),
                        'period_start_date':        'Period Start',
                        'last_interest_date':       'Period End (Last Interest)',
                    }
                )
                copy_df_button(df_display2, key="dl_rpt_savings_detail2")


    elif hist_sub_menu == "Securities & Portfolio Analysis":
        # 1. Tabs
        tab_change, tab_volat, tab_inv_signals, tab_port_signals = st.tabs(["📈 Price Change %", "🌊 Volatility", "🎯 Investment Signals", "📢 Portfolio Action Signals"])

        # 2. Sidebar (Διασφάλιση σωστών ονομάτων στηλών)
        with get_db() as conn:
            db_accounts = pd.read_sql("SELECT accounts_id, accounts_name FROM accounts WHERE accounts_type IN ('Brokerage', 'Margin', 'Other Investment') AND is_active = TRUE ORDER BY accounts_name", conn)
        
        # Δημιουργούμε το dictionary χρησιμοποιώντας τα πεζά ονόματα που επιστρέφει η Postgres
        account_options = {"All Portfolio": None}
        for _, row in db_accounts.iterrows():
            # Χρησιμοποιούμε .lower() στα κλειδιά αν δεν είμαστε σίγουροι, 
            # ή απλώς τα πεζά που επιστρέφει η Pandas
            account_options[row['accounts_name']] = row['accounts_id']

        selected_acc_name = st.sidebar.selectbox("📂 Select Account:", list(account_options.keys()), key="global_acc_filter")
        selected_acc_id = account_options[selected_acc_name] # Αυτό θα είναι είτε ID (int) είτε None

        # Global Refresh
        if st.sidebar.button("🔄 Refresh All Market Data", key="refresh_all_btn"):
            st.cache_data.clear()
            st.rerun()

        # 3. Data Fetch - Περνάμε το selected_acc_id (που είναι ήδη None για το All)
        df_data = get_portfolio_signals(selected_acc_id)

    #    st.sidebar.write(f"DEBUG: Found {len(df_data)} securities")
    #    if "Berkshire Hathaway Inc" in df_data['securities_name'].values:
    #        st.sidebar.success("✅ Berkshire found in data")
    #    else:
    #        st.sidebar.error("❌ Berkshire NOT in data")

        with tab_change:
            st.subheader("📈 Securities Top Price Change %")

            # 1. Mapping των επιλογών του UI με τις στήλες της SQL
            period_map = {
                "Daily (%)": "daily_chg_pct",
                "Weekly (%)": "weekly_chg_pct",
                "Monthly (%)": "monthly_chg_pct",
                "Quarterly (%)": "quarterly_chg_pct",
                "Semi-Annual (%)": "semiannual_chg_pct",
                "Annual (%)": "annual_chg_pct",
                "Tri-Annual (%)": "triannual_chg_pct",
                "YTD (%)": "ytd_chg_pct"
            }

            mover_col = st.radio(
                "Select Period:", 
                list(period_map.keys()), 
                horizontal=True,
                key="mover_sort_radio"
            )

            # 2. Προετοιμασία του DataFrame για την επιλεγμένη περίοδο
            selected_sql_col = period_map[mover_col]
            
            # --- ΝΕΟ: Φιλτράρισμα βάσει price_today_date ---
            # Μετατροπή σε datetime αν δεν είναι ήδη
            df_filtered = df_data.copy()
            df_filtered['price_today_date'] = pd.to_datetime(df_filtered['price_today_date']).dt.date
            today = pd.Timestamp.now().date()

            if mover_col == "Daily (%)":
                # Μόνο αν η τιμή είναι σημερινή
                df_filtered = df_filtered[df_filtered['price_today_date'] == today]
            elif mover_col == "Weekly (%)":
                # Μόνο αν η τιμή είναι εντός των τελευταίων 7 ημερών
                week_ago = today - pd.Timedelta(days=7)
                df_filtered = df_filtered[df_filtered['price_today_date'] >= week_ago]
            else:
                # Για όλες τις άλλες περιόδους (Monthly, YTD κλπ), 
                # ίσως θέλετε ένα "λογικό" όριο, π.χ. εντός τελευταίου μήνα
                month_ago = today - pd.Timedelta(days=30)
                df_filtered = df_filtered[df_filtered['price_today_date'] >= month_ago]
            # ----------------------------------------------

            # Προετοιμασία του DataFrame για την εμφάνιση
            df_display = df_filtered[['securities_name', selected_sql_col]].copy()
            df_display.columns = ['Security', mover_col]
            df_display = df_display.dropna(subset=[mover_col])


            # Φιλτράρουμε και μετονομάζουμε δυναμικά
        #    df_display = df_data[['securities_name', selected_sql_col]].copy()
        #    df_display.columns = ['Security', mover_col]
            
            # Αφαίρεση εγγραφών με NaN (αν υπάρχουν) για σωστό sorting
        #    df_display = df_display.dropna(subset=[mover_col])

            # 3. Εμφάνιση Gainers / Losers
            gainer_col, loser_col = st.columns(2)
            
            with gainer_col:
                st.success(f"📈 Top Gainers ({mover_col})")
                top_gainers = df_display.sort_values(by=mover_col, ascending=False).head(10)
                st.dataframe(
                    top_gainers.style.format({mover_col: "{:,.2f}%"}),
                    hide_index=True,
                    width='stretch',
                    column_config={
                        "Security": st.column_config.TextColumn("Security"),
                        mover_col: st.column_config.NumberColumn(mover_col, format="%.2f%%"),
                    }
                )
                copy_df_button(top_gainers, key="dl_rpt_sec_gainers")

            with loser_col:
                st.error(f"📉 Top Losers ({mover_col})")
                top_losers = df_display.sort_values(by=mover_col, ascending=True).head(10)
                st.dataframe(
                    top_losers.style.format({mover_col: "{:,.2f}%"}),
                    hide_index=True,
                    width='stretch',
                    column_config={
                        "Security": st.column_config.TextColumn("Security"),
                        mover_col: st.column_config.NumberColumn(mover_col, format="%.2f%%"),
                    }
                )
                copy_df_button(top_losers, key="dl_rpt_sec_losers")

        with tab_volat:
            st.subheader("🌊 Securities Top Volatility")

            # 1. Mapping των επιλογών Volatility με τις στήλες της SQL
            vol_map = {
                "Monthly Vol (ann)": "vol_1m_ann",
                "Quarterly Vol (ann)": "vol_3m_ann",
                "Annual Vol (ann)": "vol_1y_ann",
                "YTD Vol (ann)": "vol_ytd_ann"
            }

            vol_period = st.radio(
                "Select Volatility Period:", 
                list(vol_map.keys()), 
                horizontal=True,
                key="vol_sort_radio"
            )

            # 2. Προετοιμασία του DataFrame
            selected_vol_col = vol_map[vol_period]
            df_vol_display = df_data[['securities_name', selected_vol_col]].copy()
            df_vol_display.columns = ['Security', vol_period]
            
            # Αφαίρεση μηδενικών ή NaN τιμών (συχνά σε τίτλους χωρίς κίνηση)
            df_vol_display = df_vol_display.dropna(subset=[vol_period])
            df_vol_display = df_vol_display[df_vol_display[vol_period] > 0]

            # 3. Εμφάνιση High / Low Volatility
            high_vol_col, low_vol_col = st.columns(2)
            
            with high_vol_col:
                st.warning(f"⚡ High Volatility ({vol_period})")
                top_high_vol = df_vol_display.sort_values(by=vol_period, ascending=False).head(10)
                st.dataframe(
                    top_high_vol.style.format({vol_period: "{:,.2f}%"}),
                    hide_index=True,
                    width='stretch',
                    column_config={
                        "Security": st.column_config.TextColumn("Security"),
                        vol_period: st.column_config.NumberColumn("Volatility %", format="%.2f%%"),
                    }
                )
                copy_df_button(top_high_vol, key="dl_rpt_high_vol")

            with low_vol_col:
                st.info(f"🛡️ Low Volatility ({vol_period})")
                top_low_vol = df_vol_display.sort_values(by=vol_period, ascending=True).head(10)
                st.dataframe(
                    top_low_vol.style.format({vol_period: "{:,.2f}%"}),
                    hide_index=True,
                    width='stretch',
                    column_config={
                        "Security": st.column_config.TextColumn("Security"),
                        vol_period: st.column_config.NumberColumn("Volatility %", format="%.2f%%"),
                    }
                )
                copy_df_button(top_low_vol, key="dl_rpt_low_vol")


        with tab_inv_signals:
            st.subheader("🎯 Risk-Reward Analysis")
            df_plot = df_data.copy()
              
            # Αντικατάσταση NaN με 0
            df_plot['quality_score'] = df_plot['quality_score'].fillna(0)
            
            # Προσαρμογή του μεγέθους: 
            # Προσθέτουμε μια σταθερή τιμή ή παίρνουμε το Max(0, score) 
            # για να μην έχουμε αρνητικά μεγέθη στις τελείες
            df_plot['marker_size'] = df_plot['quality_score'].apply(lambda x: max(x, 0) + 5)

            # 1. Scatter Chart με Plotly για καλύτερο interactivity
            fig = px.scatter(
                df_plot,
                x="vol_1y_ann",
                y="annual_chg_pct",
                size="marker_size",      # Χρησιμοποιούμε τη νέα διορθωμένη στήλη
                color="sharpe_ratio", 
                hover_name="securities_name",
                # Προσθέτουμε το πραγματικό score στο hover για να φαίνεται σωστά
                hover_data={"marker_size": False, "quality_score": True, "sharpe_ratio": ":.2f"},
                labels={
                    "vol_1y_ann": "Annual Volatility (%)",
                    "annual_chg_pct": "Annual Return (%)",
                    "sharpe_ratio": "Sharpe Ratio",
                    "quality_score": "Quality Score"
                },
                title="Risk vs. Reward Matrix",
                color_continuous_scale=px.colors.diverging.RdYlGn
            )

            # Προσθήκη γραμμών "σταυρού" για τα quadrants
            fig.add_hline(y=0, line_dash="dash", line_color="white")
            fig.add_vline(x=df_data["vol_1y_ann"].median(), line_dash="dash", line_color="gray")
            
            st.plotly_chart(fig, width='stretch')

            # 2. Top Efficiency Picks Table
            st.markdown("### 🏆 Top Efficiency Picks (High Sharpe Ratio)")
            top_picks = df_data.sort_values("sharpe_ratio", ascending=False).head(10)
            
            st.dataframe(
                top_picks[['securities_name', 'annual_chg_pct', 'vol_1y_ann', 'sharpe_ratio', 'quality_score']],
                column_config={
                    "securities_name": "Security",
                    "annual_chg_pct": st.column_config.NumberColumn("Return 1Y", format="%.2f%%"),
                    "vol_1y_ann": st.column_config.NumberColumn("Vol 1Y", format="%.2f%%"),
                    "sharpe_ratio": st.column_config.ProgressColumn("Sharpe Ratio", min_value=0, max_value=3, format="%.2f"),
                    "quality_score": "Quality Score"
                },
                hide_index=True,
                width='stretch'
            )
            copy_df_button(top_picks, key="dl_rpt_top_picks")

        with tab_port_signals:
            st.subheader("📢 Portfolio Action Signals")

            # 1. Προσθήκη του επιλογέα στο UI
            view_option = st.radio(
                "Filter View:",
                ["Show All", "Hide Neutral", "Show Only Open Positions"],
                horizontal=True,
                key="signal_view_filter"
            )

            # 2. Εφαρμογή της λογικής φιλτραρίσματος
            if view_option == "Show All":
                df_rec = df_data.copy()

            elif view_option == "Hide Neutral":
                # Εξαιρούμε μόνο όσα έχουν σήμα '⚪ NEUTRAL'
                # (Φροντίστε το string να ταιριάζει ακριβώς με αυτό της SQL)
                df_rec = df_data[df_data['recommendation_signal'] != '⚪ NEUTRAL'].copy()

            elif view_option == "Show Only Open Positions":
                # Δείχνουμε μόνο όσα έχουν Quantity > 0
                df_rec = df_data[df_data['current_value_eur'] > 0].copy()

         
            # Display με χρωματική κωδικοποίηση
            def color_rec(val):
                if 'SELL' in val: return 'color: red; font-weight: bold'
                if 'STRONG BUY' in val: return 'color: green; font-weight: bold'
                return ''

            st.dataframe(
                df_rec[['final_signal', 'recommendation_signal', 'wall_street_view', 'securities_name', 'current_value_eur', 'sharpe_ratio', 'quality_score', 'price_today', 'upside_pct', 'target_price']].style.map(color_rec, subset=['recommendation_signal', 'final_signal']),
                column_config={
                    "final_signal": st.column_config.TextColumn("Final Signal", help="Conviction signals appear when our math matches Analyst views"),
                    "recommendation_signal": "Math Signal",
                    "wall_street_view": st.column_config.TextColumn("Analyst View"),
                    "securities_name": st.column_config.TextColumn("Security", width="medium"),
                    "current_value_eur": st.column_config.NumberColumn("Value", format="%,.2f €", width="small"),
                    "sharpe_ratio": st.column_config.NumberColumn("Sharpe", format="%.2f"),
                    "quality_score": st.column_config.NumberColumn("Quality", format="%.2f"),
                    "price_today": st.column_config.NumberColumn("Current Price", format="%.2f"),
                    "upside_pct": st.column_config.NumberColumn(
                        "Upside %",
                        help="Analyst Target Price vs Current Price",
                        format="%.2f%%"
                    ),
                    "target_price": st.column_config.NumberColumn("Target Price", format="%.2f"),
                },
                hide_index=True,
                width='stretch'
            )
            copy_df_button(df_rec, key="dl_rpt_signals")

            # Update buttons
            # Custom CSS to center the "Update All" button row and add spacing
            st.markdown("""
                <style>
                div.stButton > button {
                    border-radius: 5px;
                    height: 3em;
                    font-weight: bold;
                }
                </style>
            """, unsafe_allow_html=True)

            st.subheader("Market Data Synchronization")

            # Create a 2x2 grid for specific updates
            grid = st.columns(2)

            # Define the buttons in a list to keep code DRY (Don't Repeat Yourself)
            tasks = [
                ("FX Rates from Yahoo", download_historical_fx),
                ("Security Prices from Yahoo", download_historical_prices_from_yahoo),
                ("Bond Prices from Solidus", download_bond_prices_from_solidus),
                ("Info from Yahoo", download_securities_info_from_yahoo)
            ]

            for i, (label, func) in enumerate(tasks):
                with grid[i % 2]:
                    if st.button(f"🔄 Update {label}", width='stretch'):
                        with st.spinner(f"Updating {label}..."):
                            func()
                            st.toast(f"{label} updated!") # Less intrusive than balloons for small tasks
                            st.rerun()  # Refresh the page to show updated data

            st.markdown("---")

            # Center the "Update All" button and make it prominent
            _, center_col, _ = st.columns([1, 2, 1])

            with center_col:
                # use 'primary' type to give it the brand color
                if st.button("🚀 Run Full Update", type="primary", width='stretch'):
                    with st.spinner("Processing full update..."):
                        download_historical_fx("3y")
                        download_historical_prices_from_yahoo("3y")
                        download_bond_prices_from_solidus()
                        download_securities_info_from_yahoo()
                        st.balloons()
                        st.success("All data up to date!")
                        st.rerun()



 
 
    elif hist_sub_menu == "Income & Expense":
        render_income_expense_reports()

    elif hist_sub_menu == "Dividend Tracker":
        render_dividend_tracker()

    elif hist_sub_menu == "Cash Flow Forecast":
        render_cash_flow_forecast()

    elif hist_sub_menu == "Asset Allocation":
        render_asset_allocation()

    elif hist_sub_menu == "FX Exposure":
        render_fx_exposure()

    elif hist_sub_menu == "Bond Schedule":
        render_bond_schedule()



def render_income_expense_reports():
    """Render Income and Expense Reports page"""
    st.subheader("📊 Income & Expense Analysis")
    
    # Get category hierarchy
    df_categories = get_category_hierarchy()
    
    if df_categories.empty:
        st.warning("No categories found in the database. Please add categories in Settings.")
        return
    
    income_cats = df_categories[df_categories['categories_type'] == 'Income']
    expense_cats = df_categories[df_categories['categories_type'] == 'Expense']
    tax_cats = df_categories[df_categories['categories_type'] == 'Tax']
    div_cats = df_categories[df_categories['categories_type'] == 'Dividend']
    int_cats = df_categories[df_categories['categories_type'] == 'Interest']
    
    # Create category options dict for selectbox
    cat_options = {}
    for _, row in df_categories.iterrows():
        cat_options[row['categories_id']] = f"{'  ' * row['level']}{row['full_path']}"
    
    # Date range selection
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        report_type = st.selectbox(
            "Report Type",
            ["Total Summary", "Income Analysis", "Expense Analysis", "Tax Analysis", "Dividend Analysis", "Interest Analysis"],
            key="ie_report_type"
        )
    
    with col2:
        period_type = st.selectbox(
            "Period Type",
            ["Monthly", "Quarterly", "Yearly"],
            key="ie_period_type"
        )
    
    with col3:
        # Default YTD
        today = datetime.now().date()
        start_of_year = datetime(today.year, 1, 1).date()
        
        start_date = st.date_input(
            "Start Date",
            value=start_of_year,
            min_value="1900-01-01",
            max_value=today,
            key="ie_start_date"
        )
    
    with col4:
        end_date = st.date_input(
            "End Date",
            value=today,
            key="ie_end_date"
        )
    
    # Category filter (only for detailed views)
    filter_category = None
    if report_type != "Total Summary":
        with st.expander("🔍 Filter by Category", expanded=False):
            # Δημιουργούμε ένα dictionary: {id: "Name"}
            if report_type == "Income Analysis":
                current_cats = income_cats
            elif report_type == "Expense Analysis":
                 current_cats = expense_cats
            elif report_type == "Tax Analysis":
                current_cats = tax_cats 
            elif report_type == "Dividend Analysis":
                current_cats = div_cats
            elif report_type == "Interest Analysis":
                current_cats = int_cats

                
            # Κατασκευή του mapping: ID -> Name
            cat_map = {row['categories_id']: cat_options.get(row['categories_id'], "Unknown") 
                    for _, row in current_cats.iterrows()}
            
            # Λίστα επιλογών: Μόνο τα IDs (συμπεριλαμβανομένου του "All" ως string)
            options = ["All"] + list(cat_map.keys())

            selected_cat = st.selectbox(
                "Select Category",
                options=options,
                format_func=lambda x: cat_map.get(x, "All Categories"),
                key="ie_category_filter"
            )
            
            if selected_cat != "All":
                filter_category = selected_cat
    
    # Load data
    if start_date and end_date:
        if start_date > end_date:
            st.error("Start date must be before end date")
            return

        # Ορισμός όλων των πιθανών τύπων από το ENUM σου
        all_account_types = [
            'Cash', 'Checking', 'Savings', 'Credit Card', 'Brokerage', 
            'Pension', 'Other Investment', 'Margin', 'Loan', 'Real Estate', 
            'Vehicle', 'Asset', 'Liability', 'Other'
        ]

        # Προεπιλεγμένοι τύποι (αυτοί που είχες στον κώδικα)
        default_cash = ['Cash', 'Checking', 'Savings', 'Credit Card', 'Loan', 'Real Estate', 'Vehicle', 'Asset', 'Liability', 'Other']

        # Προεπιλεγμένοι τύποι (αυτοί που είχες στον κώδικα)
        default_inv = ['Brokerage', 'Other Investment', 'Margin']

        col_a, col_b = st.columns(2)

        with col_a:      
            with st.expander("🏦 Cash Account Types", expanded=False):
                options_for_cash = [t for t in all_account_types if t not in st.session_state.get("ie_inv_account_types_filter", [])]
                
                selected_cash_account_types = st.multiselect(
                    "Select Cash Types:",
                    options=options_for_cash,
                #    default=[t for t in default_cash if t in options_for_cash],
                    default=default_cash,
                    key="ie_cash_account_types_filter"
                )

        with col_b:
            with st.expander("📈 Investment Account Types", expanded=False):
                # Αφαιρούμε από τις επιλογές όσα είναι ήδη επιλεγμένα στα Cash
                options_for_inv = [t for t in all_account_types if t not in selected_cash_account_types]
                
                selected_inv_account_types = st.multiselect(
                    "Select Investment Types:",
                    options=options_for_inv,
                #    default=[t for t in default_inv if t in options_for_inv],
                    default=default_inv,
                    key="ie_inv_account_types_filter"
                )

        # Function για το reset
        def reset_account_filters():
            st.session_state["ie_cash_account_types_filter"] = default_cash
            st.session_state["ie_inv_account_types_filter"] = default_inv

        # Τοποθέτηση του κουμπιού (π.χ. δίπλα από τους επιλογείς)
        st.button("🔄 Reset to Defaults", on_click=reset_account_filters)


        # Έλεγχος αν κάποια λίστα είναι άδεια για να αποφύγεις σφάλματα στην SQL
        if not selected_cash_account_types or not selected_inv_account_types:
            st.warning("Please select at least one account type.")
        else:
            with st.spinner("Loading data..."):
                df = get_income_expense_data(start_date, end_date, filter_category, selected_cash_account_types, selected_inv_account_types)
            
            if df.empty:
                st.warning("No transactions found for the selected period.")
                return

            if st.button("🔄 Refresh Data", key="refresh_ie_btn"):
                get_income_expense_data.clear()
                st.cache_data.clear()
                st.rerun()

            # Handle datetime conversion safely
            try:
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    if hasattr(df['date'].dt, 'tz') and df['date'].dt.tz is not None:
                        df['date'] = df['date'].dt.tz_localize(None)
                
                if 'month_date' in df.columns:
                    df['month_date'] = pd.to_datetime(df['month_date'], errors='coerce')
                    if hasattr(df['month_date'].dt, 'tz') and df['month_date'].dt.tz is not None:
                        df['month_date'] = df['month_date'].dt.tz_localize(None)
            except Exception as e:
                st.warning(f"Date conversion issue: {e}")
                if 'year' in df.columns and 'month' in df.columns:
                    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
                    df['month_date'] = df['date']
            
            # Filter by report type
            if report_type == "Income Analysis":
                df = df[df['categories_type'] == 'Income']
            elif report_type == "Expense Analysis":
                df = df[df['categories_type'] == 'Expense']
            elif report_type == "Tax Analysis":
                df = df[df['categories_type'] == 'Tax']
            elif report_type == "Dividend Analysis":
                df = df[df['categories_type'] == 'Dividend']
            elif report_type == "Interest Analysis":
                df = df[df['categories_type'] == 'Interest']
            
            if df.empty:
                st.warning(f"No {report_type} transactions found for the selected period.")
                return

            # Display summary metrics
            st.markdown("### 📈 Summary")
            
            # total_bank_income = df[df['source_type'] == 'Bank'][df['categories_type'] == 'Income']['split_amount'].sum() if 'Bank' in df['source_type'].values and 'Income' in df['categories_type'].values else 0
            # Πιο σωστή και αποδοτική σύνταξη με χρήση του .loc
            condition = (df['source_type'] == 'Bank') & (df['categories_type'] == 'Income')
            total_bank_income = df.loc[condition, 'split_amount'].sum()

            condition = (df['source_type'] == 'Bank') & (df['categories_type'] == 'Interest')
            total_bank_interest = df.loc[condition, 'split_amount'].sum()
            total_income_non_investment = total_bank_income + total_bank_interest

            condition = (df['source_type'] == 'Investment') & (df['categories_type'] == 'Income')
            total_investment_income = df.loc[condition, 'split_amount'].sum()

            condition = (df['source_type'] == 'Investment') & (df['categories_type'] == 'Dividend')
            total_investment_dividend = df.loc[condition, 'split_amount'].sum()

            condition = (df['source_type'] == 'Investment') & (df['categories_type'] == 'Interest')
            total_investment_interest = df.loc[condition, 'split_amount'].sum()
            total_income_investment = total_investment_income + total_investment_dividend + total_investment_interest
 
            overall_income = total_income_non_investment + total_income_investment

            condition = (df['source_type'] == 'Bank') & (df['categories_type'] == 'Expense')
            total_bank_expense = df.loc[condition, 'split_amount'].sum()

            condition = (df['categories_type'] == 'Tax')
            total_tax = df.loc[condition, 'split_amount'].sum()
            overall_expense_non_investment = total_bank_expense + total_tax

            condition = (df['source_type'] == 'Investment') & (df['categories_type'] == 'Expense')
            total_investment_expense = df.loc[condition, 'split_amount'].sum()
            overall_expense = overall_expense_non_investment + total_investment_expense

            net_savings = overall_income + overall_expense
            
            overall_col1, overall_col2, overall_col3, overall_col4 = st.columns(4)
            with overall_col1:
            #    st.metric("Overall Income", f"€ {overall_income:,.2f}") 
                custom_metric(
                    label="Overall Income", 
                    value=f"€ {overall_income:,.2f}", 
                    pnl_value=overall_income    
                )                
            with overall_col2:
            #    st.metric("Overall Expenses", f"€ {overall_expense:,.2f}")
                custom_metric(
                    label="Overall Expenses", 
                    value=f"€ {overall_expense:,.2f}", 
                    pnl_value=overall_expense    
                )                     
            with overall_col3:
                if report_type == "Total Summary":
                #    st.metric("Net Savings", f"€ {net_savings:,.2f}", 
                #            delta="Positive" if net_savings > 0 else "Negative",
                #            delta_color="normal" if net_savings > 0 else "inverse")
                    custom_metric(
                        label="Net Savings", 
                        value=f"€ {net_savings:,.2f}", 
                        pnl_value=net_savings    
                    )                        
            with overall_col4:
                if report_type == "Total Summary":
                    savings_rate = (net_savings / overall_income * 100) if overall_income > 0 else 0
                #    st.metric("Savings Rate", f"{savings_rate:.1f}%")
                    custom_metric(
                        label="Savings Rate", 
                        value=f"{round(savings_rate, 2):.2f}%", 
                        pnl_value=savings_rate    
                    )       

            m_col1, m_col2, m_col3, m_col4 = st.columns(4)

            with m_col1:
                # Χρήση f-string και πρόσβαση στο dictionary με []
                st.markdown(f"""
                    <div style="line-height: 1.5;text-align: center;">
                        <p style="color: grey; font-size: 16px; margin: 0; font-family: sans-serif;">Earned & Reimbursed / Investments</p>
                        <p style="margin: 0; font-weight: bold;">
                            <span style="color: {get_color(total_income_non_investment)};">€ {total_income_non_investment:+,.2f}</span> / 
                            <span style="color: {get_color(total_income_investment)};">€ {total_income_investment:+,.2f}</span>
                        </p>
                    </div>
                """, unsafe_allow_html=True)

            with m_col2:
                st.markdown(f"""
                    <div style="line-height: 1.5;text-align: center;">
                        <p style="color: grey; font-size: 16px; margin: 0; font-family: sans-serif;">Expenses / Taxes / Investments</p>
                        <p style="margin: 0; font-weight: bold;">
                            <span style="color: {get_color(total_bank_expense)};">€ {total_bank_expense:+,.2f}</span> / 
                            <span style="color: {get_color(total_tax)};">€ {total_tax:+,.2f}</span> / 
                            <span style="color: {get_color(total_investment_expense)};">€ {total_investment_expense:+,.2f}</span>
                        </p>
                    </div>
                """, unsafe_allow_html=True)

            with m_col3:
                bank_total = df[df['source_type'] == 'Bank']['split_amount'].sum()
                inv_total = df[df['source_type'] == 'Investment']['split_amount'].sum()

                st.markdown(f"""
                    <div style="line-height: 1.5;text-align: center;">
                        <p style="color: grey; font-size: 16px; margin: 0; font-family: sans-serif;">Savings by Cash / Investments</p>
                        <p style="margin: 0; font-weight: bold;">
                            <span style="color: {get_color(bank_total)};">€ {bank_total:+,.2f}</span> / 
                            <span style="color: {get_color(inv_total)};">€ {inv_total:+,.2f}</span> 
                        </p>
                    </div>
                """, unsafe_allow_html=True)

            st.divider()

            # Define aggregate_by_period inside the function
            def aggregate_by_period(df, period='monthly'):
                """Aggregate data by period (monthly, quarterly, yearly)"""
                if df.empty:
                    return pd.DataFrame(), []
                
                df_copy = df.copy()
                
                # Ensure we have a proper date column for grouping
                if 'month_date' in df_copy.columns and not df_copy['month_date'].isna().all():
                    if period == 'monthly':
                        df_copy['period'] = df_copy['month_date'].dt.strftime('%Y-%m')
                    elif period == 'quarterly':
                        df_copy['period'] = df_copy['month_date'].dt.year.astype(str) + '-Q' + df_copy['month_date'].dt.quarter.astype(str)
                    else:
                        df_copy['period'] = df_copy['month_date'].dt.year.astype(str)
                elif 'date' in df_copy.columns and not df_copy['date'].isna().all():
                    if period == 'monthly':
                        df_copy['period'] = df_copy['date'].dt.strftime('%Y-%m')
                    elif period == 'quarterly':
                        df_copy['period'] = df_copy['date'].dt.year.astype(str) + '-Q' + ((df_copy['date'].dt.month - 1) // 3 + 1).astype(str)
                    else:
                        df_copy['period'] = df_copy['date'].dt.year.astype(str)
                else:
                    if period == 'monthly':
                        df_copy['period'] = df_copy['year'].astype(str) + '-' + df_copy['month'].astype(str).str.zfill(2)
                    elif period == 'quarterly':
                        df_copy['period'] = df_copy['year'].astype(str) + '-Q' + ((df_copy['month'] - 1) // 3 + 1).astype(str)
                    else:
                        df_copy['period'] = df_copy['year'].astype(str)
                
                # Aggregate by category and period
                agg_df = df_copy.groupby(['category_full_path', 'categories_type', 'period'])['split_amount'].sum().reset_index()
                
                # Pivot for easier display
                pivot_df = agg_df.pivot_table(
                    index=['category_full_path', 'categories_type'],
                    columns='period',
                    values='split_amount',
                    fill_value=0
                ).reset_index()
                
                # Add total column
                period_cols = [col for col in pivot_df.columns if col not in ['category_full_path', 'categories_type']]
                if period_cols:
                    pivot_df['Total'] = pivot_df[period_cols].sum(axis=1)
                else:
                    pivot_df['Total'] = 0
                
                # Sort period columns chronologically
                try:
                    period_cols_sorted = sorted(period_cols)
                    pivot_df = pivot_df[['category_full_path', 'categories_type'] + period_cols_sorted + ['Total']]
                    return pivot_df, period_cols_sorted
                except:
                    return pivot_df, period_cols
        
            # Aggregate by period
            period_map = {'Monthly': 'monthly', 'Quarterly': 'quarterly', 'Yearly': 'yearly'}
            aggregated_df, period_columns = aggregate_by_period(df, period_map[period_type])
            
            if aggregated_df.empty:
                st.warning("No data available for the selected criteria.")
                return
            
            # Tabs for different views
            if period_columns:
                tab_chart, tab_table, tab_trend, tab_drilldown = st.tabs([
                    "📊 Chart", "📋 Detailed Table", "📈 Trend Analysis", "🔍 Drill Down"
                ])
                
                with tab_chart:
                    # 1. Slider για το Top N
                    top_n = st.slider("Show Top N Categories", 5, 20, 10, key="ie_top_n")
                    
                    # --- ENIAIO GROUPED BAR CHART ----
                    st.subheader("Income vs Expenses Comparison")
                    
                    # Προετοιμασία δεδομένων για το Bar Chart (Ομαδοποίηση ανά τύπο και περίοδο)
                    bar_data = aggregated_df.groupby('categories_type')[period_columns].sum().reset_index()
                    bar_melted = bar_data.melt(id_vars=['categories_type'], var_name='Period', value_name='Amount')
                    
                    # Μετατροπή σε απόλυτες τιμές για να είναι συγκρίσιμα
                    bar_melted['Amount'] = bar_melted['Amount'].abs()

                    # 1. Ταξινόμηση των δεδομένων ανά ημερομηνία για να είναι σωστός ο άξονας Χ
                    # Υποθέτουμε ότι το period_columns περιέχει ημερομηνίες ή strings που ταξινομούνται
                    bar_melted['Period'] = bar_melted['Period'].astype(str) 
                    bar_melted = bar_melted.sort_values(by='Period')

                    fig_bar = px.bar(
                        bar_melted,
                        x='Period',
                        y='Amount',
                        color='categories_type',
                        barmode='group',
                        # ΕΠΙΒΟΛΗ ΣΕΙΡΑΣ: Income πρώτα, μετά Expense
                        category_orders={"categories_type": ["Income", "Dividend", "Interest", "Expense", "Tax"]}, 
                        color_discrete_map={'Income': '#FFA500', 'Expense': '#808000'},
                        labels={'Amount': 'Amount (€)', 'categories_type': 'Type'},
                        height=450
                    )
                    
                    # 2. Ρύθμιση του άξονα Χ ώστε να δείχνει τις περιόδους στη σωστή σειρά
                    fig_bar.update_xaxes(type='category') # Αντιμετωπίζει τις ημερομηνίες ως ετικέτες
                    
                    fig_bar.update_layout(
                        xaxis_tickangle=-45, 
                        hovermode='x unified',
                        legend_traceorder="normal" # Διατηρεί τη σειρά και στο legend
                    )
                    st.plotly_chart(fig_bar, width='stretch')

                    st.markdown("---")

                    # --- SIDE-BY-SIDE PIE CHARTS ---
                    st.subheader("Distribution Analysis")
                    col_inc, col_exp = st.columns(2)

                    # Λογική για τα Pie Charts
                    for c_type, column in [("Income", col_inc), ("Expense", col_exp)]:
                        with column:
                            type_df = aggregated_df[aggregated_df['categories_type'] == c_type].copy()
                            
                            if not type_df.empty:
                                # 1. Μετατρέπουμε το Total σε απόλυτη τιμή ΠΡΙΝ βρούμε τα Top N
                                type_df['Total_Abs'] = type_df['Total'].abs()
                                
                                # 2. Χρησιμοποιούμε το Total_Abs για να βρούμε τις σημαντικότερες κατηγορίες
                                top_cats = type_df.nlargest(top_n, 'Total_Abs')['category_full_path'].tolist()
                                
                                type_df['Category'] = type_df['category_full_path'].apply(
                                    lambda x: x if x in top_cats else "Other"
                                )
                                
                                # 3. Ομαδοποίηση και υπολογισμός Pie χρησιμοποιώντας απόλυτες τιμές
                                pie_agg = type_df.groupby('Category')['Total_Abs'].sum().reset_index()

                                fig_pie = px.pie(
                                    pie_agg,
                                    values='Total_Abs',
                                    names='Category',
                                    title=f"{c_type} Breakdown",
                                    hole=0.4,
                                    height=400,
                                    # Προσθήκη χρωμάτων για να μην είναι όλα μπλε
                                    color_discrete_sequence=px.colors.qualitative.Pastel if c_type == "Income" else px.colors.qualitative.Safe
                                )
                                
                                # Βελτίωση εμφάνισης κειμένου
                                fig_pie.update_traces(
                                    textposition='inside', 
                                    textinfo='percent+label',
                                    insidetextorientation='radial' # Βοηθάει στην ανάγνωση αν είναι πολλά τα slices
                                )
                                fig_pie.update_layout(showlegend=False)
                                st.plotly_chart(fig_pie, width='stretch')
                            else:
                                st.info(f"No {c_type} data available.")
                
                with tab_table:
                    st.subheader(f"{report_type} - {period_type} Breakdown")
                    
                    display_df = aggregated_df.copy()

                    # Remove duplicate category column if it exists
                    if 'category_full_path' in display_df.columns:
                        # Make sure we don't have duplicate column names
                        display_df = display_df.loc[:, ~display_df.columns.duplicated()]
                        
                    if 'categories_type' in display_df.columns:
                        display_df = display_df.sort_values(by='categories_type', ascending=False)

                    format_dict = {col: "{:,.2f} €" for col in period_columns + ['Total']}
                    
                    # Remove categories_type if it's all the same (not needed for display)
                    #if 'categories_type' in display_df.columns and len(display_df['categories_type'].unique()) == 1:
                    #    display_df = display_df.drop(columns=['categories_type'])
                    
                    st.dataframe(
                        display_df.style.format(format_dict),
                        width='stretch',
                        hide_index=True,
                        column_config={
                            "category_full_path": "Category",
                            "categories_type": "Type"}
                    )
                    copy_df_button(display_df, key="dl_rpt_ie_breakdown")
                
                with tab_trend:
                    st.subheader("Monthly Trend Analysis")
                    
                    trend_data = df.copy()
                    if 'date' in trend_data.columns and not trend_data['date'].isna().all():
                        trend_data['month_year'] = trend_data['date'].dt.strftime('%b %Y')
                        trend_data['month_num'] = trend_data['date'].dt.year * 100 + trend_data['date'].dt.month
                        
                        monthly_trend = trend_data.groupby(['month_num', 'month_year', 'category_full_path'])['split_amount'].sum().reset_index()
                        
                        if not monthly_trend.empty:
                            top_trend_cats = monthly_trend.groupby('category_full_path')['split_amount'].sum().nlargest(8).index.tolist()
                            trend_filtered = monthly_trend[monthly_trend['category_full_path'].isin(top_trend_cats)]
                            
                            if not trend_filtered.empty:
                                fig_line = px.line(
                                    trend_filtered,
                                    x='month_year',
                                    y='split_amount',
                                    color='category_full_path',
                                    title=f"{report_type} Monthly Trend - Top 8 Categories",
                                    labels={'split_amount': 'Amount (€)', 'month_year': 'Month'},
                                    markers=True
                                )
                                fig_line.update_layout(xaxis_tickangle=-45, hovermode='x unified', height=500)
                                st.plotly_chart(fig_line, width='stretch')
                
                with tab_drilldown:
                    st.subheader("Detailed Transaction Drill Down")
                    
                    all_cats = sorted(df['category_full_path'].unique())
                    selected_drill_cat = st.selectbox(
                        "Select Category to Drill Down",
                        ["All Categories"] + all_cats,
                        key="ie_drill_category"
                    )
                    
                    if selected_drill_cat != "All Categories":
                        drill_df = df[df['category_full_path'] == selected_drill_cat].copy()
                    else:
                        drill_df = df.copy()
                    
                    if not drill_df.empty:
                    #    display_cols = ['date', 'description', 'payees_name', 'category_full_path', 'split_amount', 'accounts_name', 'source_type']
                        # In the drill-down tab, add original currency column
                        display_cols = ['date', 'description', 'payees_name', 'category_full_path', 
                                        'split_amount', 'split_amount_original', 'original_currency', 
                                        'accounts_name', 'source_type']
                        display_cols = [c for c in display_cols if c in drill_df.columns]
                        
                        display_drill_df = drill_df[display_cols].sort_values('date', ascending=False).copy()
                        if 'date' in display_drill_df.columns:
                            display_drill_df['date'] = display_drill_df['date'].dt.strftime('%Y-%m-%d')
                        
                        st.dataframe(
                            display_drill_df,
                            width='stretch',
                            hide_index=True,
                            column_config={
                                'date': 'Date',
                                'description': 'Description',
                                'payees_name': 'Payee',
                                'category_full_path': 'Category',
                                'split_amount': st.column_config.NumberColumn('Amount', format="%.2f €"),
                                'split_amount_original': st.column_config.NumberColumn('Original Amount', format="%.2f"),
                                'original_currency': 'Currency',
                                'accounts_name': 'Account'
                            }
                        )
                        copy_df_button(display_drill_df, key="dl_rpt_ie_drilldown")

                        if selected_drill_cat != "All Categories":
                            total = drill_df['split_amount'].sum()
                            st.info(f"**Total for {selected_drill_cat}:** € {total:,.2f}")
            else:
                st.info("No period data available for the selected criteria.")


# ======================================================
# DIVIDEND TRACKER
# ======================================================

def render_dividend_tracker():
    st.subheader("💸 Dividend & Interest Income Tracker")

    # ── Sidebar period controls ───────────────────────────────────────────────
    today = pd.Timestamp.now().date()

    period_opt = st.sidebar.radio(
        "Period:",
        ["1 Year", "2 Years", "3 Years", "5 Years", "All Time", "Custom"],
        index=0,
        key="div_period",
    )

    _period_days = {"1 Year": 365, "2 Years": 730, "3 Years": 1095, "5 Years": 1825}
    if period_opt == "Custom":
        start_date = st.sidebar.date_input("From", value=today - pd.Timedelta(days=365), key="div_from")
        end_date   = st.sidebar.date_input("To",   value=today,                          key="div_to")
    elif period_opt == "All Time":
        start_date = pd.Timestamp("1900-01-01").date()
        end_date   = today
    else:
        start_date = today - pd.Timedelta(days=_period_days[period_opt])
        end_date   = today

    period_label = (
        period_opt if period_opt in ("All Time", "Custom")
        else f"Last {period_opt}"
    )

    if st.sidebar.button("🔄 Refresh", key="div_refresh"):
        get_dividend_tracker_data.clear()
        st.rerun()

    df = get_dividend_tracker_data(str(start_date), str(end_date))

    if df.empty:
        st.info("No dividend or interest transactions found for the selected period.")
        return

    df['month'] = pd.to_datetime(df['month'])

    # Drop income events where the position was closed at that date (cost_basis = 0).
    # This excludes e.g. MiscExp charged a few days after a full sell, when no shares
    # are held and the expense cannot be attributed to any open position.
    df = df[df['cost_basis_eur'] > 0].copy()
    if df.empty:
        st.info("No income found for open positions in the selected period.")
        return

    # Cap the income span per security to (N×12 − 1) months.
    # For a 1-year window the oldest included event must be no more than 11 months
    # before the most recent one; for 2 years no more than 23 months, etc.
    # This prevents accidentally collecting N+1 payment cycles when a dividend
    # lands right at the edge of the calendar window.
    _period_months = {"1 Year": 12, "2 Years": 24, "3 Years": 36, "5 Years": 60}
    if period_opt in _period_months:
        _max_span_days = (_period_months[period_opt] - 1) * 365.25 / 12
    elif period_opt == "Custom":
        _custom_months = (end_date - start_date).days / (365.25 / 12)
        _max_span_days = max(_custom_months - 1, 0) * 365.25 / 12
    else:  # All Time — no cap
        _max_span_days = None

    if _max_span_days is not None:
        _last_per_sec = df.groupby('securities_name')['date'].transform('max')
        df = df[(_last_per_sec - df['date']).dt.days <= _max_span_days].copy()
        if df.empty:
            st.info("No income found for the selected period after span cap.")
            return

    # Monthly bar chart
    df_monthly = df.groupby('month')['income_eur'].sum().reset_index()
    fig_bar = px.bar(
        df_monthly, x='month', y='income_eur',
        title=f"<b>Monthly Dividend & Interest Income (€) — {period_label}</b>",
        labels={'income_eur': 'Income (€)', 'month': 'Month'},
        template='plotly_dark',
        color_discrete_sequence=['#2ECC71'],
    )
    fig_bar.update_layout(margin=dict(l=0, r=0, t=50, b=0))
    st.plotly_chart(fig_bar, width='stretch')

    # Income by security for the selected period
    st.markdown(f"#### Income by Security — {period_label}")

    # ── YoC: total income / cost × (365 / holding days) ──────────────────────
    # Annualise over the actual holding period of the position, not the calendar
    # period or the gap between dividends.
    #
    # cost_basis_eur and position_start_date come from the LATERAL FIFO for each
    # income row.  We use the LAST payment's values (sorted ascending by date) so
    # the cost and position-start reflect the most-recent portfolio state.
    #
    # holding_days = last_income_date − position_start_date
    # (position_start_date = date of the oldest FIFO lot still held at the time of
    #  the last income payment for this security)
    df_sorted = df.sort_values(['securities_name', 'date'])

    def _wtd_cost(g):
        """Income-weighted average FIFO cost across all payments in the period.
        Each payment carries the cost at its own date, so this correctly weights
        larger positions (higher cost) that generated more income."""
        abs_inc = g['income_eur'].abs()
        total_w = abs_inc.sum()
        if total_w == 0:
            return g['cost_basis_eur'].iloc[-1]
        return (g['cost_basis_eur'] * abs_inc).sum() / total_w

    df_t12 = (
        df_sorted.groupby(['securities_name', 'securities_type'])
        .apply(lambda g: pd.Series({
            'period_income_eur':   g['income_eur'].sum(),
            'cost_basis_eur':      _wtd_cost(g),
            'position_start_date': g['position_start_date'].min(),
            'last_income_date':    g['date'].max(),
        }))
        .reset_index()
        .sort_values('period_income_eur', ascending=False)
    )

    # Annualised YoC = (income / cost) × (365 / period_days).
    # Dividends are paid on a per-share periodic basis, so annualise over the
    # selected calendar window — NOT over inter-dividend gaps or buy-to-dividend spans.
    # Exception: "All Time" has no fixed window, so use the actual holding period
    # (position_start_date → last income date) as the denominator.
    _ann_days_map = {"1 Year": 365, "2 Years": 730, "3 Years": 1095, "5 Years": 1825}
    if period_opt == "All Time":
        ann_days = (
            (df_t12['last_income_date'] - df_t12['position_start_date'])
            .dt.days
            .clip(lower=1)
        )
    elif period_opt in _ann_days_map:
        ann_days = _ann_days_map[period_opt]
    else:  # Custom
        ann_days = max((end_date - start_date).days, 1)

    df_t12['yoc_pct'] = (
        df_t12['period_income_eur']
        / df_t12['cost_basis_eur'].replace(0, float('nan'))
        * 100 * 365
        / ann_days
    ).fillna(0)

    m1, m2, m3 = st.columns(3)
    m1.metric(f"Total ({period_label})", f"€ {df_t12['period_income_eur'].sum():,.2f}")
    m2.metric("Securities paying", str(len(df_t12)))
    _avg_yoc = df_t12[df_t12['yoc_pct'] > 0]['yoc_pct'].mean()
    m3.metric("Avg Ann. YOC", f"{_avg_yoc:.2f}%" if not pd.isna(_avg_yoc) else "N/A")

    st.dataframe(
        df_t12[['securities_name','securities_type','period_income_eur','cost_basis_eur','yoc_pct']].style.format({
            'period_income_eur': '{:,.2f} €',
            'cost_basis_eur':    '{:,.2f} €',
            'yoc_pct':           '{:.2f}%',
        }),
        hide_index=True, width='stretch',
        column_config={
            'securities_name':   'Security',
            'securities_type':   'Type',
            'period_income_eur': st.column_config.NumberColumn(f'Income ({period_label})', format='%,.2f €'),
            'cost_basis_eur':    st.column_config.NumberColumn('Cost Basis (€)', format='%,.2f €',
                                     help='Average FIFO cost basis across all income payments in the period (EUR). Expire/Reinvest on the same date as the income payment are excluded so closed positions show their true cost.'),
            'yoc_pct':           st.column_config.NumberColumn('Ann. YOC %', format='%.2f%%',
                                     help='Annualised yield on cost: (total period income / FIFO cost at last payment) × (365 / days from oldest held lot to last dividend). MiscExp deducted from income.'),
        }
    )
    copy_df_button(df_t12, key="dl_rpt_div_t12")

    # Full detail table
    with st.expander("Full transaction detail"):
        st.dataframe(
            df[['month', 'securities_name', 'accounts_name', 'action', 'income_eur']].style.format({'income_eur': '{:,.2f} €'}),
            hide_index=True, width='stretch',
            column_config={
                'month':          'Month',
                'securities_name':'Security',
                'accounts_name':  'Account',
                'action':         'Action',
                'income_eur':     st.column_config.NumberColumn('Income (€)', format='%,.2f €'),
            }
        )
        copy_df_button(df, key="dl_rpt_div_detail")


# ======================================================
# CASH FLOW FORECAST
# ======================================================

def render_cash_flow_forecast():
    st.subheader("🔮 Cash Flow Forecast")

    horizon = st.sidebar.radio("Forecast horizon:", ["30 days", "60 days", "90 days"], index=1, key="cf_horizon")
    days = int(horizon.split()[0])

    if st.sidebar.button("🔄 Refresh", key="cf_refresh"):
        get_cash_flow_forecast.clear()
        st.rerun()

    df_future, df_recurring = get_cash_flow_forecast()

    st.markdown("#### Explicitly Scheduled Future Transactions")
    if df_future.empty:
        st.info("No future-dated transactions found.")
    else:
        cutoff = pd.Timestamp.now() + pd.Timedelta(days=days)
        df_f = df_future[df_future['date'] <= cutoff].copy()
        if df_f.empty:
            st.info(f"No transactions scheduled within {days} days.")
        else:
            total_in  = df_f[df_f['amount_eur'] > 0]['amount_eur'].sum()
            total_out = df_f[df_f['amount_eur'] < 0]['amount_eur'].sum()
            c1, c2, c3 = st.columns(3)
            c1.metric("Expected In",  f"€ {total_in:,.2f}")
            c2.metric("Expected Out", f"€ {total_out:,.2f}")
            c3.metric("Net",          f"€ {(total_in + total_out):,.2f}")

            fig = px.bar(
                df_f.sort_values('date'), x='date', y='amount_eur',
                color=df_f['amount_eur'].apply(lambda x: 'Income' if x >= 0 else 'Expense'),
                color_discrete_map={'Income': '#2ECC71', 'Expense': '#E74C3C'},
                title=f"<b>Scheduled cash flows — next {days} days</b>",
                labels={'amount_eur': 'Amount (€)', 'date': 'Date'},
                hover_data=['payees_name', 'category'],
                template='plotly_dark',
            )
            fig.update_layout(margin=dict(l=0, r=0, t=50, b=0), showlegend=False)
            st.plotly_chart(fig, width='stretch')

            st.dataframe(
                df_f[['date', 'payees_name', 'accounts_name', 'category', 'amount_eur', 'currency']]
                .style.format({'amount_eur': '{:,.2f} €'}),
                hide_index=True, width='stretch',
            )
            copy_df_button(df_f, key="dl_rpt_cf_future")

    st.markdown("#### Detected Recurring Payments")
    st.caption("Payees with ≥ 2 transactions in the last 120 days, showing projected next occurrence.")
    if df_recurring.empty:
        st.info("No recurring patterns detected.")
    else:
        cutoff2 = pd.Timestamp.now() + pd.Timedelta(days=days)
        df_r = df_recurring[df_recurring['next_expected_date'] <= cutoff2].copy()
        if df_r.empty:
            st.info(f"No recurring payments expected within {days} days.")
        else:
            st.dataframe(
                df_r[['next_expected_date', 'payees_name', 'avg_amount_eur', 'avg_days_between', 'tx_count', 'currency']]
                .style.format({'avg_amount_eur': '{:,.2f} €', 'avg_days_between': '{:.0f} days'}),
                hide_index=True, width='stretch',
                column_config={
                    'next_expected_date': 'Next Expected',
                    'payees_name':        'Payee',
                    'avg_amount_eur':     st.column_config.NumberColumn('Avg Amount (€)', format='%,.2f €'),
                    'avg_days_between':   'Avg Frequency',
                    'tx_count':           'Occurrences',
                    'currency':           'Currency',
                }
            )
            copy_df_button(df_r, key="dl_rpt_cf_recurring")


# ======================================================
# ASSET ALLOCATION
# ======================================================

def render_asset_allocation():
    st.subheader("🥧 Asset Allocation vs. Target")

    if st.sidebar.button("🔄 Refresh", key="alloc_refresh"):
        get_asset_allocation_data.clear()
        st.rerun()

    df = get_asset_allocation_data()

    if df.empty:
        st.info("No holdings found.")
        return

    total_eur = df['value_eur'].sum()
    st.metric("Total Portfolio Value", f"€ {total_eur:,.2f}")

    col_pie, col_table = st.columns([1, 1])

    with col_pie:
        fig = px.pie(
            df, names='securities_type', values='value_eur',
            title="<b>Current Allocation</b>",
            template='plotly_dark',
            hole=0.4,
        )
        fig.update_traces(textinfo='percent+label')
        fig.update_layout(margin=dict(l=0, r=0, t=50, b=0), showlegend=False)
        st.plotly_chart(fig, width='stretch')

    with col_table:
        fig2 = px.bar(
            df, x='securities_type', y=['actual_pct', 'target_pct'],
            barmode='group',
            title="<b>Actual vs. Target (%)</b>",
            labels={'value': '%', 'securities_type': 'Type', 'variable': ''},
            template='plotly_dark',
            color_discrete_map={'actual_pct': '#457B9D', 'target_pct': '#F1A208'},
        )
        fig2.update_layout(margin=dict(l=0, r=0, t=50, b=0))
        st.plotly_chart(fig2, width='stretch')

    st.markdown("#### Rebalancing Delta")
    df_display = df.copy()
    df_display['delta_pct'] = df_display['actual_pct'] - df_display['target_pct']
    df_display['rebalance_eur'] = df_display['delta_pct'] / 100 * total_eur

    st.dataframe(
        df_display.style.format({
            'value_eur':   '{:,.2f} €',
            'actual_pct':  '{:.2f}%',
            'target_pct':  '{:.2f}%',
            'delta_pct':   '{:+.2f}%',
            'rebalance_eur': '{:+,.2f} €',
        }).map(color_negative_red, subset=['delta_pct', 'rebalance_eur']),
        hide_index=True, width='stretch',
        column_config={
            'securities_type': 'Asset Type',
            'value_eur':       st.column_config.NumberColumn('Value (€)',   format='%,.2f €'),
            'actual_pct':      st.column_config.NumberColumn('Actual %',    format='%.2f%%'),
            'target_pct':      st.column_config.NumberColumn('Target %',    format='%.2f%%'),
            'delta_pct':       st.column_config.NumberColumn('Delta %',     format='%+.2f%%'),
            'rebalance_eur':   st.column_config.NumberColumn('Rebalance €', format='%+,.2f €'),
        }
    )
    copy_df_button(df_display, key="dl_rpt_alloc")

    st.caption("Set target allocations via SQL: `UPDATE Allocation_Targets SET Target_Pct = 40 WHERE Securities_Type = 'ETF';`")


# ======================================================
# FX EXPOSURE
# ======================================================

def render_fx_exposure():
    st.subheader("🌍 FX Exposure Report")

    if st.sidebar.button("🔄 Refresh", key="fx_exp_refresh"):
        get_fx_exposure_data.clear()
        st.rerun()

    df = get_fx_exposure_data()

    if df.empty:
        st.info("No FX exposure data found.")
        return

    total_eur = df['eur_exposure'].sum()
    st.metric("Total Net Worth (EUR)", f"€ {total_eur:,.2f}")

    fig = px.bar(
        df, x='currency', y='eur_exposure',
        title="<b>Net Exposure by Currency (€)</b>",
        template='plotly_dark',
        color='eur_exposure',
        color_continuous_scale='RdYlGn',
        labels={'eur_exposure': 'Net Exposure (€)', 'currency': 'Currency'},
        text=df['eur_exposure'].apply(lambda x: f"€ {x:,.0f}"),
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=0, t=50, b=0))
    st.plotly_chart(fig, width='stretch')

    st.markdown("#### Sensitivity to ±5% FX Move")
    df_display = df.copy()
    df_display['impact_up_5pct']   =  df_display['sensitivity_5pct_eur']
    df_display['impact_down_5pct'] = -df_display['sensitivity_5pct_eur']

    st.dataframe(
        df_display[['currency', 'native_exposure', 'eur_exposure', 'impact_up_5pct', 'impact_down_5pct']]
        .style.format({
            'native_exposure':   '{:,.2f}',
            'eur_exposure':      '{:,.2f} €',
            'impact_up_5pct':    '{:+,.2f} €',
            'impact_down_5pct':  '{:+,.2f} €',
        }).map(color_negative_red, subset=['impact_down_5pct']),
        hide_index=True, width='stretch',
        column_config={
            'currency':          'Currency',
            'native_exposure':   st.column_config.NumberColumn('Native Exposure', format='%,.2f'),
            'eur_exposure':      st.column_config.NumberColumn('EUR Exposure',    format='%,.2f €'),
            'impact_up_5pct':    st.column_config.NumberColumn('+5% FX Impact',   format='%+,.2f €'),
            'impact_down_5pct':  st.column_config.NumberColumn('-5% FX Impact',   format='%+,.2f €'),
        }
    )
    copy_df_button(df_display, key="dl_rpt_fx_exposure")


# ======================================================
# BOND SCHEDULE
# ======================================================

def render_bond_schedule():
    st.subheader("📅 Bond Maturity & Coupon Schedule")

    if st.sidebar.button("🔄 Refresh", key="bond_refresh"):
        get_bond_schedule_data.clear()
        st.rerun()

    df = get_bond_schedule_data()

    if df.empty:
        st.info("No bond holdings found. Make sure Securities of type 'Bond' have Maturity_Date, Coupon_Rate, and Face_Value filled in.")
        return

    # Summary metrics
    total_face  = df['total_face_eur'].sum()
    total_ann   = df['annual_coupon_eur'].sum()
    next_12m    = df[df['days_to_maturity'] <= 365]['total_face_eur'].sum()

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Face Value (€)", f"€ {total_face:,.2f}")
    m2.metric("Annual Coupon Income (€)", f"€ {total_ann:,.2f}")
    m3.metric("Maturing in 12m (€)", f"€ {next_12m:,.2f}")

    # Maturity timeline
    df_mat = df.dropna(subset=['maturity_date']).copy()
    if not df_mat.empty:
        fig = px.bar(
            df_mat.sort_values('maturity_date'),
            x='maturity_date', y='total_face_eur',
            color='securities_name',
            title="<b>Bond Maturities Timeline</b>",
            labels={'total_face_eur': 'Face Value (€)', 'maturity_date': 'Maturity Date'},
            template='plotly_dark',
            text=df_mat['securities_name'],
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=50, b=0))
        st.plotly_chart(fig, width='stretch')

    st.markdown("#### Bond Holdings Detail")
    st.dataframe(
        df.style.format({
            'quantity':           '{:,.4f}',
            'face_value':         '{:,.2f}',
            'total_face_eur':     '{:,.2f} €',
            'coupon_rate':        '{:.4f}%',
            'next_coupon_eur':    '{:,.2f} €',
            'annual_coupon_eur':  '{:,.2f} €',
            'days_to_maturity':   '{:,.0f}',
        }, na_rep="—"),
        hide_index=True, width='stretch',
        column_config={
            'securities_name':   'Bond',
            'quantity':          st.column_config.NumberColumn('Quantity',       format='%,.4f'),
            'face_value':        st.column_config.NumberColumn('Face Value',     format='%,.2f'),
            'total_face_eur':    st.column_config.NumberColumn('Total Face (€)', format='%,.2f €'),
            'coupon_rate':       st.column_config.NumberColumn('Coupon %',       format='%.4f%%'),
            'coupon_frequency':  'Frequency',
            'next_coupon_eur':   st.column_config.NumberColumn('Next Coupon (€)',format='%,.2f €'),
            'annual_coupon_eur': st.column_config.NumberColumn('Annual Income (€)', format='%,.2f €'),
            'maturity_date':     'Maturity Date',
            'days_to_maturity':  st.column_config.NumberColumn('Days to Maturity', format='%,d'),
            'currency':          'Currency',
        }
    )
    copy_df_button(df, key="dl_rpt_bonds")


# ======================================================
# NET WORTH REPORT  (Quicken-style)
# ======================================================

def render_net_worth_report():
    st.subheader("📋 Net Worth Report")

    # ── Sidebar controls ──────────────────────────────────────────────────
    last_day_prev_month = pd.Timestamp.now().replace(day=1) - pd.Timedelta(days=1)
    start_date = st.sidebar.date_input(
        "📅 Start Date",
        value=st.session_state.nwr_date_val,
        min_value=datetime(1900, 1, 1),
        max_value=last_day_prev_month,
        key="nwr_date",
    )
    st.session_state.nwr_date_val = start_date
    interval  = st.sidebar.radio("Interval:", ["Year", "Quarter", "Month"], key="nwr_interval")
    show_zero = st.sidebar.checkbox("Show zero-balance accounts", value=False, key="nwr_show_zero")
    if st.sidebar.button("🔄 Refresh", key="nwr_refresh"):
        get_net_worth_report_data.clear()
        get_nwr_security_detail.clear()
        st.cache_data.clear()
        st.rerun()

    # ── Account selection (main area, full-width) ─────────────────────────
    df_accounts = get_all_accounts_for_nwr()
    all_ids     = df_accounts['accounts_id'].tolist()
    id_to_name  = dict(zip(df_accounts['accounts_id'], df_accounts['accounts_name']))
    id_to_type  = dict(zip(df_accounts['accounts_id'], df_accounts['accounts_type']))

    saved_ids = get_nwr_account_selection()
    init_sel  = set(saved_ids) if saved_ids is not None else set(all_ids)

    df_sel = df_accounts.copy()
    df_sel.insert(0, 'Include', df_sel['accounts_id'].isin(init_sel))

    with st.expander("⚙️ Account Selection", expanded=False):
        edited_df = st.data_editor(
            df_sel.rename(columns={'accounts_name': 'Account', 'accounts_type': 'Type'}),
            column_config={
                'Include':     st.column_config.CheckboxColumn('Include', default=True),
                'accounts_id': None,
            },
            hide_index=True,
            width="stretch",
            disabled=['Account', 'Type'],
            key="nwr_account_editor",
        )
        selected_ids = edited_df[edited_df['Include']]['accounts_id'].tolist()
        col_save, _ = st.columns([1, 4])
        if col_save.button("💾 Save Selection", key="nwr_save"):
            save_nwr_account_selection(selected_ids)
            st.success("Account selection saved!")

    # ── Data ──────────────────────────────────────────────────────────────
    account_ids_tuple = tuple(sorted(selected_ids)) if selected_ids else None
    with st.spinner("Loading net worth data…"):
        df = get_net_worth_report_data(start_date.isoformat(), interval, account_ids_tuple)

    if df.empty:
        st.info("No data found for the selected period.")
        return

    df['period_end'] = pd.to_datetime(df['period_end'])
    period_cols = sorted(df['period_end'].unique())

    def fmt_period(dt):
        dt = pd.Timestamp(dt)
        if interval == 'Year':    return dt.strftime('%Y')
        if interval == 'Quarter': return f"{dt.year} Q{(dt.month - 1) // 3 + 1}"
        return dt.strftime('%b %Y')

    period_labels = [fmt_period(p) for p in period_cols]
    label_map     = dict(zip(period_cols, period_labels))

    # ── Shared helpers ────────────────────────────────────────────────────
    def fmt_val(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        if abs(v) < 0.005:
            return '—'
        return f'€ {v:,.2f}'

    def color_neg(v):
        if isinstance(v, (int, float)) and not pd.isna(v) and v < -0.005:
            return 'color: #E74C3C'
        return ''

    # ── Tabs ──────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📋 Summary per Type", "🔍 Detail Analysis", "💰 Account Balances"])

    # ═══════════════════════════════════════════════════════════════════════
    with tab1:
        # ── Chart ─────────────────────────────────────────────────────────
        chart_rows = []
        for p in period_cols:
            p_df   = df[df['period_end'] == p]
            assets = p_df[p_df['section'] == 'Assets']['balance_eur'].sum()
            liabs  = p_df[p_df['section'] == 'Liabilities']['balance_eur'].sum()
            chart_rows.append({
                'Period':      label_map[p],
                'Assets':      max(assets, 0),
                'Liabilities': abs(min(liabs, 0)),
                'Net Worth':   assets + liabs,
            })
        df_chart = pd.DataFrame(chart_rows)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            name='Assets', x=df_chart['Period'], y=df_chart['Assets'],
            marker_color='#2ECC71',
        ))
        fig.add_trace(go.Bar(
            name='Liabilities', x=df_chart['Period'], y=df_chart['Liabilities'],
            marker_color='#E74C3C',
        ))
        fig.add_trace(go.Scatter(
            name='Net Worth', x=df_chart['Period'], y=df_chart['Net Worth'],
            mode='lines+markers',
            line=dict(color='white', width=2),
            marker=dict(color='#E74C3C', size=8),
        ))
        fig.update_layout(
            barmode='group', template='plotly_dark',
            title='<b>Net Worth — Assets vs Liabilities</b>',
            yaxis_tickformat=',.0f', hovermode='x unified',
            margin=dict(l=0, r=0, t=50, b=0),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        st.plotly_chart(fig, width='stretch')

        # ── Hierarchical table ─────────────────────────────────────────────
        ASSET_GROUPS     = ['Investments', 'Pension', 'Cash & Bank', 'Other Assets', 'Other']
        LIABILITY_GROUPS = ['Credit Cards', 'Loans', 'Other Liabilities', 'Other']
        SECTIONS = [('Assets', ASSET_GROUPS), ('Liabilities', LIABILITY_GROUPS)]

        display_rows   = []
        section_totals = {}

        def _row(label, row_type, vals_by_period):
            return {'Account': label, 'row_type': row_type,
                    **{label_map[p]: vals_by_period.get(p, 0) for p in period_cols}}

        def _empty_row():
            return {'Account': '', 'row_type': 'separator',
                    **{lbl: None for lbl in period_labels}}

        for section, group_list in SECTIONS:
            display_rows.append({'Account': section.upper(), 'row_type': 'section_header',
                                 **{lbl: None for lbl in period_labels}})
            sec_total = {p: 0.0 for p in period_cols}

            for group in group_list:
                grp_df = df[(df['section'] == section) & (df['group_name'] == group)]
                if grp_df.empty:
                    continue

                grp_pivot = grp_df.pivot_table(
                    index=['accounts_id', 'accounts_name'],
                    columns='period_end',
                    values='balance_eur',
                    fill_value=0,
                    aggfunc='sum',
                )

                if not show_zero:
                    row_max = grp_pivot.reindex(columns=period_cols, fill_value=0).abs().max(axis=1)
                    visible_pivot = grp_pivot[row_max >= 0.005]
                else:
                    visible_pivot = grp_pivot

                if visible_pivot.empty and not show_zero:
                    continue

                grp_total = {p: float(visible_pivot[p].sum()) if p in visible_pivot.columns else 0.0 for p in period_cols}

                for (acc_id, acc_name), row in visible_pivot.iterrows():
                    vals = {p: float(row.get(p, 0)) for p in period_cols}
                    display_rows.append(_row(f'    {acc_name}', 'account', vals))

                display_rows.append(_row(f'  TOTAL {group}', 'group_subtotal', grp_total))
                for p in period_cols:
                    sec_total[p] += grp_total[p]

            display_rows.append(_row(f'TOTAL {section.upper()}', 'section_total', sec_total))
            section_totals[section] = sec_total
            display_rows.append(_empty_row())

        nw_vals = {p: section_totals.get('Assets', {}).get(p, 0)
                      + section_totals.get('Liabilities', {}).get(p, 0)
                   for p in period_cols}
        display_rows.append(_row('NET WORTH', 'net_worth', nw_vals))

        df_display = pd.DataFrame(display_rows)

        def row_styler(row):
            rt = row.get('row_type', 'account')
            if rt == 'net_worth':
                s = 'font-weight: bold; border-top: 2px solid rgba(255,255,255,0.6)'
            elif rt == 'section_total':
                s = 'font-weight: bold; border-top: 1px solid rgba(255,255,255,0.4)'
            elif rt == 'group_subtotal':
                s = 'font-weight: bold'
            elif rt == 'section_header':
                s = 'color: rgba(200,200,200,0.8); font-weight: bold'
            else:
                s = ''
            return [s] * len(row)

        format_dict = {lbl: fmt_val for lbl in period_labels}

        styled = (
            df_display.style
            .apply(row_styler, axis=1)
            .format(format_dict, na_rep='')
            .map(color_neg, subset=period_labels)
            .hide(axis='index')
            .hide(['row_type'], axis='columns')
        )

        st.dataframe(
            styled, width='stretch', hide_index=True,
            column_config={'Account': st.column_config.TextColumn("Account", pinned=True)},
        )
        copy_df_button(df_display, key="dl_rpt_nwr_overview")

        # ── Investment account drilldown ───────────────────────────────────
        inv_types    = {'Brokerage', 'Margin'}
        inv_accounts = df_accounts[
            df_accounts['accounts_id'].isin(selected_ids) &
            df_accounts['accounts_type'].isin(inv_types)
        ]

        if not inv_accounts.empty:
            st.markdown("---")
            st.subheader("🔍 Investment Account Detail")

            detail_acc_id = st.selectbox(
                "Select account:",
                options=inv_accounts['accounts_id'].tolist(),
                format_func=lambda x: f"{id_to_name[x]} ({id_to_type[x]})",
                key="nwr_detail_acc",
            )

            with st.spinner("Loading security detail…"):
                df_detail = get_nwr_security_detail(
                    start_date.isoformat(), interval, int(detail_acc_id)
                )

            if df_detail.empty:
                st.info("No investment transactions found for this account.")
            else:
                df_detail['period_end'] = pd.to_datetime(df_detail['period_end'])

                detail_pivot = df_detail.pivot_table(
                    index='security_name',
                    columns='period_end',
                    values='value_eur',
                    fill_value=0,
                    aggfunc='sum',
                )

                if not show_zero:
                    row_max = detail_pivot.reindex(columns=period_cols, fill_value=0).abs().max(axis=1)
                    detail_pivot = detail_pivot[row_max >= 0.005]

                if detail_pivot.empty:
                    st.info("All securities have zero value in the selected period.")
                else:
                    detail_pivot.columns = [label_map.get(c, str(c)) for c in detail_pivot.columns]
                    present_labels = [lbl for lbl in period_labels if lbl in detail_pivot.columns]
                    detail_pivot = detail_pivot[present_labels].reset_index()
                    detail_pivot.rename(columns={'security_name': 'Security'}, inplace=True)

                    total_row = {'Security': 'TOTAL'}
                    for lbl in present_labels:
                        total_row[lbl] = detail_pivot[lbl].sum()
                    detail_pivot = pd.concat(
                        [detail_pivot, pd.DataFrame([total_row])], ignore_index=True
                    )

                    def style_total_row(row):
                        if row.get('Security') == 'TOTAL':
                            return ['font-weight: bold; border-top: 1px solid rgba(255,255,255,0.4)'] * len(row)
                        return [''] * len(row)

                    styled_detail = (
                        detail_pivot.style
                        .apply(style_total_row, axis=1)
                        .format({lbl: fmt_val for lbl in present_labels}, na_rep='')
                        .map(color_neg, subset=present_labels)
                        .hide(axis='index')
                    )

                    st.dataframe(
                        styled_detail, width='stretch', hide_index=True,
                        column_config={'Security': st.column_config.TextColumn("Security", pinned=True)},
                    )
                    copy_df_button(detail_pivot, key="dl_rpt_nwr_detail")

    # ═══════════════════════════════════════════════════════════════════════
    with tab2:
        GROUP_ORDER = ['Cash & Bank', 'Investments', 'Pension', 'Other Assets',
                       'Credit Cards', 'Loans', 'Other Liabilities']

        summary_rows = []
        for p in period_cols:
            p_df = df[df['period_end'] == p]
            row  = {'Period': label_map[p]}
            for grp in GROUP_ORDER:
                row[grp] = p_df[p_df['group_name'] == grp]['balance_eur'].sum()
            row['Total Assets']      = p_df[p_df['section'] == 'Assets']['balance_eur'].sum()
            row['Total Liabilities'] = p_df[p_df['section'] == 'Liabilities']['balance_eur'].sum()
            row['Net Worth']         = row['Total Assets'] + row['Total Liabilities']
            summary_rows.append(row)

        df_summary = pd.DataFrame(summary_rows[::-1])  # latest first

        num_cols = [c for c in df_summary.columns if c != 'Period']

        def style_summary_row(row):
            if row.get('Period') == df_summary['Period'].iloc[0]:
                return [''] * len(row)
            return [''] * len(row)

        def color_summary_neg(v):
            if isinstance(v, (int, float)) and not pd.isna(v) and v < -0.005:
                return 'color: #E74C3C'
            return ''

        styled_summary = (
            df_summary.style
            .format({c: fmt_val for c in num_cols}, na_rep='')
            .map(color_summary_neg, subset=num_cols)
            .hide(axis='index')
        )

        st.dataframe(
            styled_summary, width='stretch', hide_index=True,
            column_config={'Period': st.column_config.TextColumn('Period', pinned=True)},
        )
        copy_df_button(df_summary, key="dl_rpt_nwr_summary")

    # ═══════════════════════════════════════════════════════════════════════
    with tab3:
        selected_period = st.selectbox(
            "Select Period:",
            options=period_cols[::-1],
            format_func=fmt_period,
            key="nwr_snapshot_period",
        )

        p_df   = df[df['period_end'] == selected_period]
        assets = p_df[p_df['section'] == 'Assets']['balance_eur'].sum()
        liabs  = p_df[p_df['section'] == 'Liabilities']['balance_eur'].sum()
        nw     = assets + liabs

        grp_sums = p_df.groupby('group_name')['balance_eur'].sum()

        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Net Worth",       f"€ {nw:,.2f}")
        m2.metric("Cash & Bank",     f"€ {grp_sums.get('Cash & Bank', 0):,.2f}")
        m3.metric("Investments",     f"€ {grp_sums.get('Investments', 0):,.2f}")
        m4.metric("Pension",         f"€ {grp_sums.get('Pension', 0):,.2f}")
        m5.metric("Other Assets",    f"€ {grp_sums.get('Other Assets', 0):,.2f}")
        m6.metric("Liabilities",     f"€ {(liabs):,.2f}")

        df_breakdown = (
            grp_sums[grp_sums.abs() >= 0.005]
            .reset_index()
            .rename(columns={'group_name': 'Category', 'balance_eur': 'Value (€)'})
        )

        if not df_breakdown.empty and nw != 0:
            df_breakdown['% of Net Worth'] = (df_breakdown['Value (€)'] / nw * 100).round(2)

            col_pie, col_table = st.columns([1, 1])

            with col_pie:
                fig_pie = px.pie(
                    df_breakdown,
                    values='Value (€)',
                    names='Category',
                    title=f"<b>Net Worth Breakdown — {fmt_period(selected_period)}</b>",
                    template='plotly_dark',
                    hole=0.4,
                    color_discrete_sequence=[
                        '#457B9D', '#2ECC71', '#A8DADC', '#5D6D7E',
                        '#E74C3C', '#E67E22', '#FFD700',
                    ],
                )
                fig_pie.update_traces(textinfo='percent+label')
                fig_pie.update_layout(showlegend=False, margin=dict(l=0, r=0, t=50, b=0))
                st.plotly_chart(fig_pie, width='stretch')

            with col_table:
                st.dataframe(
                    df_breakdown.style.format({
                        'Value (€)':      fmt_val,
                        '% of Net Worth': '{:.2f}%',
                    }).map(color_neg, subset=['Value (€)']).hide(axis='index'),
                    hide_index=True,
                    width='stretch',
                    column_config={
                        'Category':       st.column_config.TextColumn('Category'),
                        'Value (€)':      st.column_config.NumberColumn('Value (€)',  format='€ %,.2f'),
                        '% of Net Worth': st.column_config.NumberColumn('% of NW',   format='%.2f%%'),
                    },
                )
                copy_df_button(df_breakdown, key="dl_rpt_nwr_breakdown")

    # ═══════════════════════════════════════════════════════════════════════
    with tab4:
        # ── Stacked bar chart by account group + Balance line ──────────────
        GROUP_COLORS = {
            'Cash & Bank':       '#FFD700',
            'Investments':       '#457B9D',
            'Pension':           '#A8DADC',
            'Other Assets':      '#9B59B6',
            'Other':             '#95A5A6',
            'Credit Cards':      '#E74C3C',
            'Loans':             '#E67E22',
            'Other Liabilities': '#C0392B',
        }
        GROUP_ORDER = ['Cash & Bank', 'Investments', 'Pension', 'Other Assets', 'Other',
                       'Credit Cards', 'Loans', 'Other Liabilities']

        fig_ab = go.Figure()
        for grp in GROUP_ORDER:
            if grp not in df['group_name'].values:
                continue
            y_vals = [
                df[(df['period_end'] == p) & (df['group_name'] == grp)]['balance_eur'].sum()
                for p in period_cols
            ]
            fig_ab.add_trace(go.Bar(
                name=grp,
                x=period_labels,
                y=y_vals,
                marker_color=GROUP_COLORS.get(grp, '#95A5A6'),
            ))

        nw_line = [df[df['period_end'] == p]['balance_eur'].sum() for p in period_cols]
        fig_ab.add_trace(go.Scatter(
            name='Balance',
            x=period_labels,
            y=nw_line,
            mode='lines+markers',
            line=dict(color='#FF69B4', width=2),
            marker=dict(color='#FF69B4', size=8),
        ))
        fig_ab.update_layout(
            barmode='stack', template='plotly_dark',
            title='<b>Account Balances</b>',
            yaxis_tickformat=',.0f', hovermode='x unified',
            margin=dict(l=0, r=0, t=50, b=0),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        st.plotly_chart(fig_ab, width='stretch')

        # ── Table — period columns labelled with explicit end dates ─────────
        def fmt_period_date(dt):
            return pd.Timestamp(dt).strftime('%d/%m/%Y')

        date_labels   = [fmt_period_date(p) for p in period_cols]
        date_label_map = dict(zip(period_cols, date_labels))

        ACCOUNT_TYPE_GROUPS = [
            ('Assets',      'Cash & Bank',       'Bank & Cash Accounts'),
            ('Assets',      'Investments',        'Investment Accounts'),
            ('Assets',      'Pension',            'Pension Accounts'),
            ('Assets',      'Other Assets',       'Asset Accounts'),
            ('Assets',      'Other',              'Other Accounts'),
            ('Liabilities', 'Credit Cards',       'Credit Card Accounts'),
            ('Liabilities', 'Loans',              'Loan Accounts'),
            ('Liabilities', 'Other Liabilities',  'Liability Accounts'),
        ]

        ab_rows    = []
        ending_vals = {p: 0.0 for p in period_cols}

        def _ab_row(label, row_type, vals_by_period):
            return {'Account': label, 'row_type': row_type,
                    **{date_label_map[p]: vals_by_period.get(p, 0) for p in period_cols}}

        def _ab_empty():
            return {'Account': '', 'row_type': 'separator',
                    **{lbl: None for lbl in date_labels}}

        for section, group_name, display_name in ACCOUNT_TYPE_GROUPS:
            grp_df = df[(df['section'] == section) & (df['group_name'] == group_name)]
            if grp_df.empty:
                continue

            ab_rows.append({'Account': display_name, 'row_type': 'group_header',
                            **{lbl: None for lbl in date_labels}})

            grp_pivot = grp_df.pivot_table(
                index=['accounts_id', 'accounts_name'],
                columns='period_end',
                values='balance_eur',
                fill_value=0,
                aggfunc='sum',
            )

            visible_pivot = grp_pivot
            if not show_zero:
                row_max = grp_pivot.reindex(columns=period_cols, fill_value=0).abs().max(axis=1)
                visible_pivot = grp_pivot[row_max >= 0.005]

            grp_total = {p: float(visible_pivot[p].sum()) if p in visible_pivot.columns else 0.0
                         for p in period_cols}

            for (acc_id, acc_name), row in visible_pivot.iterrows():
                vals = {p: float(row.get(p, 0)) for p in period_cols}
                ab_rows.append(_ab_row(f'    {acc_name}', 'account', vals))

            ab_rows.append(_ab_row(f'TOTAL {display_name}', 'group_total', grp_total))
            ab_rows.append(_ab_empty())

            for p in period_cols:
                ending_vals[p] += grp_total[p]

        ab_rows.append(_ab_row('Ending Balance', 'ending_balance', ending_vals))

        df_ab = pd.DataFrame(ab_rows)

        def ab_row_styler(row):
            rt = row.get('row_type', 'account')
            if rt == 'ending_balance':
                return ['font-weight: bold; border-top: 2px solid rgba(255,255,255,0.6)'] * len(row)
            elif rt == 'group_total':
                return ['font-weight: bold; border-top: 1px solid rgba(255,255,255,0.4)'] * len(row)
            elif rt == 'group_header':
                return ['font-weight: bold; color: rgba(220,220,220,0.9)'] * len(row)
            return [''] * len(row)

        styled_ab = (
            df_ab.style
            .apply(ab_row_styler, axis=1)
            .format({lbl: fmt_val for lbl in date_labels}, na_rep='')
            .map(color_neg, subset=date_labels)
            .hide(axis='index')
            .hide(['row_type'], axis='columns')
        )

        st.dataframe(
            styled_ab, width='stretch', hide_index=True,
            column_config={'Account': st.column_config.TextColumn("Account", pinned=True)},
        )
        copy_df_button(df_ab, key="dl_rpt_nwr_balances")

        # ── Investment account drilldown ───────────────────────────────────
        inv_types_ab    = {'Brokerage', 'Margin'}
        inv_accounts_ab = df_accounts[
            df_accounts['accounts_id'].isin(selected_ids) &
            df_accounts['accounts_type'].isin(inv_types_ab)
        ]

        if not inv_accounts_ab.empty:
            st.markdown("---")
            st.subheader("🔍 Investment Account Detail")

            detail_acc_id_ab = st.selectbox(
                "Select account:",
                options=inv_accounts_ab['accounts_id'].tolist(),
                format_func=lambda x: f"{id_to_name[x]} ({id_to_type[x]})",
                key="nwr_detail_acc_ab",
            )

            with st.spinner("Loading security detail…"):
                df_detail_ab = get_nwr_security_detail(
                    start_date.isoformat(), interval, int(detail_acc_id_ab)
                )

            if df_detail_ab.empty:
                st.info("No investment transactions found for this account.")
            else:
                df_detail_ab['period_end'] = pd.to_datetime(df_detail_ab['period_end'])

                detail_pivot_ab = df_detail_ab.pivot_table(
                    index='security_name',
                    columns='period_end',
                    values='value_eur',
                    fill_value=0,
                    aggfunc='sum',
                )

                if not show_zero:
                    row_max_ab = detail_pivot_ab.reindex(columns=period_cols, fill_value=0).abs().max(axis=1)
                    detail_pivot_ab = detail_pivot_ab[row_max_ab >= 0.005]

                if detail_pivot_ab.empty:
                    st.info("All securities have zero value in the selected period.")
                else:
                    detail_pivot_ab.columns = [label_map.get(c, str(c)) for c in detail_pivot_ab.columns]
                    present_labels_ab = [lbl for lbl in period_labels if lbl in detail_pivot_ab.columns]
                    detail_pivot_ab = detail_pivot_ab[present_labels_ab].reset_index()
                    detail_pivot_ab.rename(columns={'security_name': 'Security'}, inplace=True)

                    total_row_ab = {'Security': 'TOTAL'}
                    for lbl in present_labels_ab:
                        total_row_ab[lbl] = detail_pivot_ab[lbl].sum()
                    detail_pivot_ab = pd.concat(
                        [detail_pivot_ab, pd.DataFrame([total_row_ab])], ignore_index=True
                    )

                    def style_total_row_ab(row):
                        if row.get('Security') == 'TOTAL':
                            return ['font-weight: bold; border-top: 1px solid rgba(255,255,255,0.4)'] * len(row)
                        return [''] * len(row)

                    styled_detail_ab = (
                        detail_pivot_ab.style
                        .apply(style_total_row_ab, axis=1)
                        .format({lbl: fmt_val for lbl in present_labels_ab}, na_rep='')
                        .map(color_neg, subset=present_labels_ab)
                        .hide(axis='index')
                    )

                    st.dataframe(
                        styled_detail_ab, width='stretch', hide_index=True,
                        column_config={'Security': st.column_config.TextColumn("Security", pinned=True)},
                    )
                    copy_df_button(detail_pivot_ab, key="dl_rpt_nwr_detail_ab")