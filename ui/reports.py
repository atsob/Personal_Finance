import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from database.crud import update_holdings
from database.queries import get_category_hierarchy, get_hist_net_worth_data, get_hist_inv_positions_data, get_pnl_report_data, get_income_expense_data, get_portfolio_signals
from data.downloaders import download_historical_fx, download_historical_prices_from_yahoo, download_bond_prices_from_solidus, download_securities_info_from_yahoo
from ui.components import color_negative_red, color_value, custom_metric
from datetime import datetime, timedelta

def render_reports(conn):
    """Render the Reports page."""
    st.title("Reports")
    
    hist_sub_menu = st.sidebar.radio(
        "Select Report:",
        ["Historical Net Worth", "Historical Investment Positions", "P&L Reports", "Securities & Portfolio Analysis", "Income & Expense"],
        key="hist_sub_nav"
    )
    
    if hist_sub_menu == "Historical Net Worth":
        st.subheader("📈 Net Worth Progress (Monthly) to Date")
        
        last_day_prev_month = pd.Timestamp.now().replace(day=1) - pd.Timedelta(days=1)
        
        min_nwt_date = st.sidebar.date_input(
            "📅 Start Date", 
            value=st.session_state.nw_date_val,
            max_value=last_day_prev_month,
            key="nw_date"
        )
        st.session_state.nw_date_val = min_nwt_date
        
        df_hist = get_hist_net_worth_data(min_nwt_date)
        
        if st.sidebar.button("🔄 Refresh Net Worth"):
            get_hist_net_worth_data.clear()
            st.cache_data.clear()
            st.rerun()
        
        try:
            df_hist.columns = [c.lower() for c in df_hist.columns]
            df_hist['date'] = pd.to_datetime(df_hist['date'])
            df_hist = df_hist.sort_values('date')
            
            tab1, tab2 = st.tabs(["📊 Graph", "📋 Data"])
            
            with tab1:
                df_hist['net_change'] = df_hist['total_net_worth'].diff()
                max_gain_idx = df_hist['net_change'].idxmax()
                max_loss_idx = df_hist['net_change'].idxmin()
                
                fig = px.line(
                    df_hist, 
                    x="date", 
                    y=["total_cash", "total_invested", "total_pension", "total_assets"],
                    color_discrete_sequence=["#FFD700", "#457B9D", "#A8DADC", "#5D6D7E"],
                    template="plotly_dark"
                )
                fig.update_traces(line=dict(width=2))
                
                fig.add_trace(
                    go.Scatter(
                        x=df_hist["date"], 
                        y=df_hist["total_net_worth"],
                        name="<b>TOTAL NET WORTH</b>",
                        line=dict(color="white", width=5),
                        hovertemplate="<b>%{y:,.0f} €</b>"
                    )
                )
                
                fig.add_annotation(
                    x=df_hist.loc[max_gain_idx, 'date'],
                    y=df_hist.loc[max_gain_idx, 'total_net_worth'],
                    text="🚀 Max Gain",
                    showarrow=True,
                    arrowhead=2,
                    arrowcolor="#2ECC71",
                    ax=0, ay=-40,
                    font=dict(color="#2ECC71", size=12)
                )
                
                fig.add_annotation(
                    x=df_hist.loc[max_loss_idx, 'date'],
                    y=df_hist.loc[max_loss_idx, 'total_net_worth'],
                    text="🔻 Max Loss",
                    showarrow=True,
                    arrowhead=2,
                    arrowcolor="#E74C3C",
                    ax=0, ay=40,
                    font=dict(color="#E74C3C", size=12)
                )
                
                fig.update_layout(
                    yaxis_tickformat=',.0f', 
                    legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1),
                    title="<b>Net Worth & Changes Analysis</b>",
                    xaxis=dict(
                        range=[df_hist['date'].min(), df_hist['date'].max()],
                        type='date',
                        tickformat="%b %Y",
                        dtick="M12"
                    ),
                    hovermode="x unified",
                    margin=dict(l=0, r=0, t=100, b=0)
                )
                st.plotly_chart(fig, width="stretch")
            
            with tab2:
                st.dataframe(
                    df_hist.sort_values('date', ascending=False)
                    .style
                    .map(color_negative_red, subset=['net_change'])
                    .format({
                        "total_assets": "€ {:,.2f}",
                        "total_cash": "€ {:,.2f}",
                        "total_pension": "€ {:,.2f}",
                        "total_invested": "€ {:,.2f}",
                        "total_net_worth": "€ {:,.2f}",
                        "net_change": "€ {:,.2f}"
                    }),
                    width="stretch",
                    hide_index=True
                )
        except Exception as e:
            st.error(f"Error: {e}")
    
    elif hist_sub_menu == "Historical Investment Positions":
        st.subheader("📈 Investments Position Progress (Monthly)")
        
        # ... (Start Date logic remains same) ...
        last_day_prev_month = pd.Timestamp.now().replace(day=1) - pd.Timedelta(days=1)
        
        min_inv_date = st.sidebar.date_input(
            "📅 Start Date", 
            value=st.session_state.inv_date_val,
            max_value=last_day_prev_month,
            key="inv_date"
        )
        st.session_state.inv_date_val = min_inv_date


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
            # Highlight Total line
            fig.for_each_trace(lambda t: t.update(line=dict(color="white", width=4) if t.name == "TOTAL" else dict(width=2)))
            st.plotly_chart(fig, use_container_width=True)
            

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
           
            # 4. Εμφάνιση του πίνακα με το config
            st.dataframe(
                df_summary, 
                hide_index=True, 
                use_container_width=True,
                column_config=col_config
            )


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
                use_container_width=True
            )
    
    elif hist_sub_menu == "P&L Reports":
        tab_report, tab_movers = st.tabs(["📊 P&L Report", "🚀 Top Movers"])
        
        with tab_report:
            st.subheader("📈 Investments Profit & Loss")
            df_pnl = get_pnl_report_data()
            
            if st.sidebar.button("🔄 Refresh P&L", key="refresh_pnl_btn"):
                get_pnl_report_data.clear()
                with st.spinner("Running :green[download_historical_fx('1d')]"):
                    download_historical_fx("1d")
                with st.spinner("Running :green[download_historical_prices_from_yahoo('1d')]"):
                    download_historical_prices_from_yahoo("1d")
                with st.spinner("Running :green[download_bond_prices_from_solidus()]"):
                    download_bond_prices_from_solidus()
                with st.spinner("Running :green[update_holdings()]"):
                    update_holdings()
                st.cache_data.clear()
                st.rerun()
            
            try:
                row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)
                
                with row1_col1:
                    total_dtd_pnl = df_pnl['pnl_dtd_eur'].sum()
                    total_current_value = df_pnl['current_value_eur'].sum()
                    st.metric("Total Current Value (EUR)", f"{total_current_value:,.2f} €", delta=f"{total_dtd_pnl:,.2f} €")
 
                with row1_col2:
                    total_all_time_pnl = df_pnl['pnl_all_time_eur'].fillna(0).sum()
                    custom_metric(
                        label="Total All Time P&L", 
                        value=f"{total_all_time_pnl:,.2f} €", 
                        pnl_value=total_all_time_pnl
                    )

                with row1_col3:
                    total_net_all_time_pnl = df_pnl['pnl_net_all_time_eur'].fillna(0).sum()
                    custom_metric(
                        label="Total Net All Time P&L", 
                        value=f"{total_net_all_time_pnl:,.2f} €", 
                        pnl_value=total_net_all_time_pnl
                    )

                with row1_col4:
                    total_unrealized_pnl = df_pnl['unrealized_pnl_eur'].fillna(0).sum()
                #    st.metric("Total Unrealized P&L", f"{total_unrealized_pnl:,.2f} €", delta=f"{total_unrealized_pnl:,.2f} €")
                    custom_metric(
                        label="Total Unrealized P&L", 
                        value=f"{total_unrealized_pnl:,.2f} €", 
                        pnl_value=total_unrealized_pnl
                    )

                row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)

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
                    total_ytd_pnl = df_pnl['pnl_ytd_eur'].sum()
                #    st.metric("Total YTD P&L", f"{total_ytd_pnl:,.2f} €", delta=f"{total_ytd_pnl:,.2f} €")
                    custom_metric(
                        label="Total YTD P&L", 
                        value=f"{total_ytd_pnl:,.2f} €", 
                        pnl_value=total_ytd_pnl
                    )

                row3_col1, row3_col2, row3_col3, row3_col4 = st.columns(4)

                with row3_col1:
                    total_dtd_market_pnl = df_pnl['pnl_dtd_market_eur'].sum()
                    custom_metric(
                        label="Total Daily Market P&L", 
                        value=f"{total_dtd_market_pnl:,.2f} €", 
                        pnl_value=total_dtd_market_pnl
                    )

                with row3_col2:
                    total_dtd_fx_pnl = df_pnl['pnl_dtd_fx_eur'].sum()
                    custom_metric(
                        label="Total Daily FX P&L", 
                        value=f"{total_dtd_fx_pnl:,.2f} €", 
                        pnl_value=total_dtd_fx_pnl
                    )

                with row3_col3:
                    total_ytd_market_pnl = df_pnl['pnl_ytd_market_eur'].sum()
                    custom_metric(
                        label="Total YTD Market P&L", 
                        value=f"{total_ytd_market_pnl:,.2f} €", 
                        pnl_value=total_ytd_market_pnl
                    )

                with row3_col4:
                    total_ytd_fx_pnl = df_pnl['pnl_ytd_fx_eur'].sum()
                    custom_metric(
                        label="Total YTD FX P&L", 
                        value=f"{total_ytd_fx_pnl:,.2f} €", 
                        pnl_value=total_ytd_fx_pnl
                    )

                st.divider() # Optional separation line for better visual effect

                df_acc = df_pnl.groupby('accounts_name')[['current_value_eur', 'pnl_dtd_market_eur', 'pnl_dtd_fx_eur', 'pnl_dtd_eur', 'pnl_wtd_eur', 'pnl_mtd_eur', 'pnl_ytd_eur',  'pnl_all_time_eur', 'pnl_net_all_time_eur', 'unrealized_pnl_eur']].sum()
                df_acc = df_acc.rename(columns={
                    'current_value_eur': 'Current Value',
                    'pnl_dtd_market_eur': 'Daily Market P&L',
                    'pnl_dtd_fx_eur': 'Daily FX P&L',
                    'pnl_dtd_eur': 'Daily P&L',
                    'pnl_wtd_eur': 'Weekly P&L',
                    'pnl_mtd_eur': 'Monthly P&L',
                    'pnl_ytd_eur': 'YTD P&L',
                    'pnl_all_time_eur': 'Total P&L',
                    'pnl_net_all_time_eur': 'Total Net P&L',
                    'unrealized_pnl_eur': 'Unrealized P&L'            
                })
                df_acc.index.name = "Account"
                st.dataframe(df_acc.style.map(color_negative_red).format("{:,.2f} €"), width="stretch")
                
                selected_acc = st.selectbox(
                    "Select Account for Details:", 
                    df_pnl['accounts_name'].unique(),
                    key="pnl_account_select"
                )
                
                query_acc_id = f"SELECT Accounts_Id FROM Accounts WHERE Accounts_Name = '{selected_acc}'"
                df_acc_id = pd.read_sql(query_acc_id, conn)
                if not df_acc_id.empty:
                    account_id = df_acc_id.iloc[0]['accounts_id']
                    
                    query_holdings = f"""
                        SELECT h.Securities_Id, s.Securities_Name, h.Quantity
                        FROM Holdings h
                        JOIN Securities s ON h.Securities_Id = s.Securities_Id
                        WHERE h.Accounts_Id = {account_id} AND h.Quantity != 0
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
                        'pnl_dtd_eur', 'pnl_wtd_eur', 'pnl_mtd_eur', 'pnl_ytd_eur', 
                        'pnl_all_time_eur', 'pnl_net_all_time_eur', 'unrealized_pnl_eur'
                    ]].rename(columns={
                        'securities_name': 'Security',
                        'quantity': 'Quantity',        
                        'latest_price': 'Latest Price',      
                        'current_value_eur': 'Value (€)',
                        'pnl_dtd_eur': 'Daily P&L',
                        'pnl_wtd_eur': 'Weekly P&L',
                        'pnl_mtd_eur': 'Monthly P&L',
                        'pnl_ytd_eur': 'YTD P&L',
                        'pnl_all_time_eur': 'Total P&L',
                        'pnl_net_all_time_eur': 'Total Net P&L',
                        'unrealized_pnl_eur': 'Unrealized P&L'
                    })
                    
                    pnl_cols = ['Daily P&L', 'Weekly P&L', 'Monthly P&L', 'YTD P&L', 'Total P&L', 'Total Net P&L', 'Unrealized P&L']

                    st.dataframe(
                        df_display.style
                        .map(color_negative_red, subset=pnl_cols)
                        .format({
                            # P&L columns
                            **{col: "{:,.2f} €" for col in pnl_cols},
                            # Value column
                            'Value (€)': "{:,.2f} €",
                            # Price and Quantity columns
                            'Latest Price': "{:,.2f}",
                            'Quantity': "{:,.8f}"
                        }),
                        width="stretch",
                        hide_index=True
                    )

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
                }), hide_index=True, use_container_width=True,
                column_config={
                    "securities_name": st.column_config.TextColumn("Security", width="small"),
                    "accounts_name": st.column_config.TextColumn("Account", width="small"),
                    "Daily P&L (€)": st.column_config.NumberColumn("P&L (€)", format="%.2f €", width="small"),
                    "Daily Change (%)": st.column_config.NumberColumn("Day %", format="%.2f%%", width="small"),
                }                
                )
            
            with loser_col:
                st.error("📉 Top Losers")
                top_losers = df_movers.sort_values(by=col_to_sort, ascending=True).head(10)
                st.dataframe(top_losers.style.format({
                    'Daily P&L (€)': "{:,.2f} €",
                    'Daily Change (%)': "{:,.2f}%"
                }), hide_index=True, use_container_width=True,
                column_config={
                    "securities_name": st.column_config.TextColumn("Security", width="small"),
                    "accounts_name": st.column_config.TextColumn("Account", width="small"),
                    "Daily P&L (€)": st.column_config.NumberColumn("P&L (€)", format="%.2f €", width="small"),
                    "Daily Change (%)": st.column_config.NumberColumn("Day %", format="%.2f%%", width="small"),
                }                
                )

    elif hist_sub_menu == "Securities & Portfolio Analysis":
        # 1. Tabs
        tab_change, tab_volat, tab_inv_signals, tab_port_signals = st.tabs(["📈 Price Change %", "🌊 Volatility", "🎯 Investment Signals", "📢 Portfolio Action Signals"])

        # 2. Sidebar (Διασφάλιση σωστών ονομάτων στηλών)
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
            
            # Φιλτράρουμε και μετονομάζουμε δυναμικά
            df_display = df_data[['securities_name', selected_sql_col]].copy()
            df_display.columns = ['Security', mover_col]
            
            # Αφαίρεση εγγραφών με NaN (αν υπάρχουν) για σωστό sorting
            df_display = df_display.dropna(subset=[mover_col])

            # 3. Εμφάνιση Gainers / Losers
            gainer_col, loser_col = st.columns(2)
            
            with gainer_col:
                st.success(f"📈 Top Gainers ({mover_col})")
                top_gainers = df_display.sort_values(by=mover_col, ascending=False).head(10)
                st.dataframe(
                    top_gainers.style.format({mover_col: "{:,.2f}%"}),
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Security": st.column_config.TextColumn("Security"),
                        mover_col: st.column_config.NumberColumn(mover_col, format="%.2f%%"),
                    }                
                )

            with loser_col:
                st.error(f"📉 Top Losers ({mover_col})")
                top_losers = df_display.sort_values(by=mover_col, ascending=True).head(10)
                st.dataframe(
                    top_losers.style.format({mover_col: "{:,.2f}%"}),
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Security": st.column_config.TextColumn("Security"),
                        mover_col: st.column_config.NumberColumn(mover_col, format="%.2f%%"),
                    }                
                )

 
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
                    use_container_width=True,
                    column_config={
                        "Security": st.column_config.TextColumn("Security"),
                        vol_period: st.column_config.NumberColumn("Volatility %", format="%.2f%%"),
                    }                
                )

            with low_vol_col:
                st.info(f"🛡️ Low Volatility ({vol_period})")
                top_low_vol = df_vol_display.sort_values(by=vol_period, ascending=True).head(10)
                st.dataframe(
                    top_low_vol.style.format({vol_period: "{:,.2f}%"}),
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Security": st.column_config.TextColumn("Security"),
                        vol_period: st.column_config.NumberColumn("Volatility %", format="%.2f%%"),
                    }                
                )


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
            
            st.plotly_chart(fig, use_container_width=True)

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
                use_container_width=True
            )

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
                use_container_width=True
            )

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
                    if st.button(f"🔄 Update {label}", use_container_width=True):
                        with st.spinner(f"Updating {label}..."):
                            func()
                            st.toast(f"{label} updated!") # Less intrusive than balloons for small tasks
                            st.rerun()  # Refresh the page to show updated data

            st.markdown("---")

            # Center the "Update All" button and make it prominent
            _, center_col, _ = st.columns([1, 2, 1])

            with center_col:
                # use 'primary' type to give it the brand color
                if st.button("🚀 Run Full Update", type="primary", use_container_width=True):
                    with st.spinner("Processing full update..."):
                        download_historical_fx("3y")
                        download_historical_prices_from_yahoo("3y")
                        download_bond_prices_from_solidus()
                        download_securities_info_from_yahoo()
                        st.balloons()
                        st.success("All data up to date!")
                        st.rerun()



 
 
    elif hist_sub_menu == "Income & Expense":
        render_income_expense_reports(conn)



def render_income_expense_reports(conn):
    """Render Income and Expense Reports page"""
    st.subheader("📊 Income & Expense Analysis")
    
    # Get category hierarchy
    df_categories = get_category_hierarchy()
    
    if df_categories.empty:
        st.warning("No categories found in the database. Please add categories in Settings.")
        return
    
    income_cats = df_categories[df_categories['categories_type'] == 'Income']
    expense_cats = df_categories[df_categories['categories_type'] == 'Expense']
    
    # Create category options dict for selectbox
    cat_options = {}
    for _, row in df_categories.iterrows():
        cat_options[row['categories_id']] = f"{'  ' * row['level']}{row['full_path']}"
    
    # Date range selection
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        report_type = st.selectbox(
            "Report Type",
            ["Income & Expense Summary", "Income Analysis", "Expense Analysis"],
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
    if report_type != "Income & Expense Summary":
        with st.expander("🔍 Filter by Category", expanded=False):
            # Δημιουργούμε ένα dictionary: {id: "Name"}
            if report_type == "Income Analysis":
                current_cats = income_cats
            else:
                current_cats = expense_cats
                
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
            
            if df.empty:
                st.warning(f"No {report_type} transactions found for the selected period.")
                return

            # Display summary metrics
            st.markdown("### 📈 Summary")
            
            total_income = df[df['categories_type'] == 'Income']['split_amount'].sum() if 'Income' in df['categories_type'].values else 0
            total_expense = df[df['categories_type'] == 'Expense']['split_amount'].sum() if 'Expense' in df['categories_type'].values else 0
            net_savings = total_income - abs(total_expense)
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Income", f"€ {total_income:,.2f}")
            with col2:
                expense_delta = f"{abs(total_expense)/total_income*100:.1f}%" if total_income > 0 else None
                st.metric("Total Expenses", f"€ {abs(total_expense):,.2f}", delta=expense_delta)
            with col3:
                if report_type == "Income & Expense Summary":
                    st.metric("Net Savings", f"€ {net_savings:,.2f}", 
                            delta="Positive" if net_savings > 0 else "Negative",
                            delta_color="normal" if net_savings > 0 else "inverse")
            with col4:
                if report_type == "Income & Expense Summary":
                    savings_rate = (net_savings / total_income * 100) if total_income > 0 else 0
                    st.metric("Savings Rate", f"{savings_rate:.1f}%")

            st.divider()

            if 'source_type' in df.columns:
                st.markdown("### 📊 Savings by Source")
                col1, col2 = st.columns(2)
                
                with col1:
                    bank_total = df[df['source_type'] == 'Bank']['split_amount'].sum()
                    custom_metric(
                        label="🏦 Cash", 
                        value=f"€ {bank_total:,.2f}", 
                        pnl_value=bank_total    
                    )
                            
                with col2:
                    inv_total = df[df['source_type'] == 'Investment']['split_amount'].sum()
                    custom_metric(
                        label="📈 Investments",
                        value=f"€ {inv_total:,.2f}",
                        pnl_value=inv_total
                    )

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
                        category_orders={"categories_type": ["Income", "Expense"]}, 
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
                    st.plotly_chart(fig_bar, use_container_width=True)

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
                                st.plotly_chart(fig_pie, use_container_width=True)
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
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "category_full_path": "Category",
                            "categories_type": "Type"}
                    )
                    
                    # Download button
                    csv = display_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name=f"{report_type}_{start_date}_{end_date}.csv",
                        mime="text/csv"
                    )
                
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
                                st.plotly_chart(fig_line, use_container_width=True)
                
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
                            use_container_width=True,
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
                        
                        if selected_drill_cat != "All Categories":
                            total = drill_df['split_amount'].sum()
                            st.info(f"**Total for {selected_drill_cat}:** € {total:,.2f}")
            else:
                st.info("No period data available for the selected criteria.")