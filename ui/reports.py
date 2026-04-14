import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from database.crud import update_holdings
from database.queries import get_hist_net_worth_data, get_hist_inv_positions_data, get_pnl_report_data
from data.downloaders import download_historical_fx, download_historical_prices_from_yahoo
from ui.components import color_negative_red, custom_metric

def render_reports(conn):
    """Render the Reports page."""
    st.title("Reports")
    
    hist_sub_menu = st.sidebar.radio(
        "Select Report:",
        ["Historical Net Worth", "Historical Investment Positions", "P&L Reports"],
        key="hist_sub_nav"
    )
    
    if hist_sub_menu == "Historical Net Worth":
        st.subheader("📈 Net Worth Progress (Monthly)")
        
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
        
        last_day_prev_month = pd.Timestamp.now().replace(day=1) - pd.Timedelta(days=1)
        
        min_inv_date = st.sidebar.date_input(
            "📅 Start Date", 
            value=st.session_state.inv_date_val,
            max_value=last_day_prev_month,
            key="inv_date"
        )
        st.session_state.inv_date_val = min_inv_date
        
        df_inv = get_hist_inv_positions_data(min_inv_date)
        
        if st.sidebar.button("🔄 Refresh Positions"):
            get_hist_inv_positions_data.clear()
            st.cache_data.clear()
            st.rerun()
        
        try:
            df_inv.columns = [c.lower() for c in df_inv.columns]
            df_inv['date'] = pd.to_datetime(df_inv['date'])
            
            df_pivot = df_inv.pivot(
                index='date', 
                columns='accounts_name', 
                values='account_value'
            ).fillna(0).reset_index()
            
            tab_graph, tab_data = st.tabs(["📊 Graph", "📋 Data Table"])
            
            with tab_graph:
                fig = px.line(
                    df_pivot, 
                    x="date", 
                    y=[c for c in df_pivot.columns if c != 'date'],
                    title="<b>Investment Value per Account</b>",
                    labels={"value": "Value (€)", "date": "Date", "variable": "Account"},
                    template="plotly_dark"
                )
                fig.for_each_trace(lambda t: t.update(
                    line=dict(color="white", width=4) if t.name.upper() == "TOTAL" else dict(width=2)
                ))
                fig.update_layout(
                    hovermode="x unified",
                    yaxis_tickformat=',.0f',
                    xaxis=dict(range=[df_pivot['date'].min(), df_pivot['date'].max()], type='date')
                )
                st.plotly_chart(fig, width="stretch")
            
            with tab_data:
                df_display = df_pivot.copy().sort_values('date', ascending=False)
                numeric_cols = [c for c in df_display.columns if c != 'date']
                df_display['date'] = df_display['date'].dt.strftime('%Y-%m-%d')
                
                col_config = {
                    col: st.column_config.NumberColumn(col, format="%,.2f €", width="medium")
                    for col in numeric_cols
                }
                st.dataframe(df_display, width="stretch", hide_index=True, column_config=col_config)
        except Exception as e:
            st.error(f"Error: {e}")
    
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
                with st.spinner("Running :green[update_holdings()]"):
                    update_holdings()
                st.cache_data.clear()
                st.rerun()
            
            try:
                row1_col1, row1_col2, row1_col3 = st.columns(3)
                
                with row1_col1:
                    total_dtd_pnl = df_pnl['pnl_dtd_eur'].sum()
                    total_current_value = df_pnl['current_value_eur'].sum()
                    st.metric("Total Current Value (EUR)", f"{total_current_value:,.2f} €", delta=f"{total_dtd_pnl:,.2f} €")
                with row1_col2:
                    total_net_all_time_pnl = df_pnl['pnl_net_all_time_eur'].fillna(0).sum()
                #    st.metric("Total Net All Time P&L", f"{total_net_all_time_pnl:,.2f} €", delta=f"{total_net_all_time_pnl:,.2f} €")
                    custom_metric(
                        label="Total Net All Time P&L", 
                        value=f"{total_net_all_time_pnl:,.2f} €", 
                        pnl_value=total_net_all_time_pnl
                    )

                with row1_col3:
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
                #    st.metric("Total DTD P&L", f"{total_dtd_pnl:,.2f} €", delta=f"{total_dtd_pnl:,.2f} €")
                    custom_metric(
                        label="Total DTD P&L", 
                        value=f"{total_dtd_pnl:,.2f} €", 
                        pnl_value=total_dtd_pnl
                    )

                with row2_col2:
                    total_wtd_pnl = df_pnl['pnl_wtd_eur'].sum()
                #    st.metric("Total WTD P&L", f"{total_wtd_pnl:,.2f} €", delta=f"{total_wtd_pnl:,.2f} €")
                    custom_metric(
                        label="Total WTD P&L", 
                        value=f"{total_wtd_pnl:,.2f} €", 
                        pnl_value=total_wtd_pnl
                    )

                with row2_col3:
                    total_mtd_pnl = df_pnl['pnl_mtd_eur'].sum()
                #    st.metric("Total MTD P&L", f"{total_mtd_pnl:,.2f} €", delta=f"{total_mtd_pnl:,.2f} €")
                    custom_metric(
                        label="Total MTD P&L", 
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

                st.divider() # Optional separation line for better visual effect

                df_acc = df_pnl.groupby('accounts_name')[['current_value_eur', 'pnl_dtd_eur', 'pnl_wtd_eur', 'pnl_mtd_eur', 'pnl_ytd_eur', 'pnl_net_all_time_eur', 'unrealized_pnl_eur']].sum()
                df_acc = df_acc.rename(columns={
                    'current_value_eur': 'Current Value',
                    'pnl_dtd_eur': 'Daily P&L',
                    'pnl_wtd_eur': 'Weekly P&L',
                    'pnl_mtd_eur': 'Monthly P&L',
                    'pnl_ytd_eur': 'YTD P&L',
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
                        SELECT h.Securities_Id, s.Security_Name, h.Quantity
                        FROM Holdings h
                        JOIN Securities s ON h.Securities_Id = s.Securities_Id
                        WHERE h.Accounts_Id = {account_id} AND h.Quantity != 0
                    """
                    df_holdings = pd.read_sql(query_holdings, conn)
                    
                    query_prices = f"""
                        SELECT DISTINCT ON (h.Securities_Id) 
                            h.Securities_Id, 
                            s.Security_Name,
                            h.Quantity,
                            (SELECT hp.Price_Close 
                            FROM Historical_Prices hp 
                            WHERE hp.Securities_Id = h.Securities_Id 
                            AND hp.Price_Date <= CURRENT_DATE 
                            ORDER BY hp.Price_Date DESC LIMIT 1) AS Latest_Price
                        FROM Holdings h
                        JOIN Securities s ON h.Securities_Id = s.Securities_Id
                        WHERE h.Accounts_Id = {account_id} AND h.Quantity != 0
                    """
                    df_prices = pd.read_sql(query_prices, conn)
                    
                    df_details = df_pnl[df_pnl['accounts_name'] == selected_acc].copy()
                    
                    if not df_holdings.empty:
                        df_details = df_details.merge(
                            df_holdings[['security_name', 'quantity']], 
                            on='security_name', 
                            how='left'
                        )
                    else:
                        df_details['quantity'] = 0
                    
                    if not df_prices.empty:
                        df_details = df_details.merge(
                            df_prices[['security_name', 'latest_price']], 
                            on='security_name', 
                            how='left'
                        )
                    else:
                        df_details['latest_price'] = 0
                    
                    df_details['quantity'] = df_details['quantity'].fillna(0)
                    df_details['latest_price'] = df_details['latest_price'].fillna(0)
                    
                    df_display = df_details[[
                        'security_name', 'quantity', 'latest_price', 'current_value_eur', 
                        'pnl_dtd_eur', 'pnl_wtd_eur', 'pnl_mtd_eur', 'pnl_ytd_eur', 
                        'pnl_net_all_time_eur', 'unrealized_pnl_eur'
                    ]].rename(columns={
                        'security_name': 'Security',
                        'quantity': 'Quantity',        
                        'latest_price': 'Latest Price',      
                        'current_value_eur': 'Value (€)',
                        'pnl_dtd_eur': 'Daily P&L',
                        'pnl_wtd_eur': 'Weekly P&L',
                        'pnl_mtd_eur': 'Monthly P&L',
                        'pnl_ytd_eur': 'YTD P&L',
                        'pnl_net_all_time_eur': 'Total Net P&L',
                        'unrealized_pnl_eur': 'Unrealized P&L'
                    })
                    
                    pnl_cols = ['Daily P&L', 'Weekly P&L', 'Monthly P&L', 'YTD P&L', 'Total Net P&L', 'Unrealized P&L']

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
            
            df_movers = df_pnl[['security_name', 'accounts_name', 'pnl_dtd_eur', 'daily_change_pct']].copy()
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
                    "security_name": st.column_config.TextColumn("Security", width="small"),
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
                    "security_name": st.column_config.TextColumn("Security", width="small"),
                    "accounts_name": st.column_config.TextColumn("Account", width="small"),
                    "Daily P&L (€)": st.column_config.NumberColumn("P&L (€)", format="%.2f €", width="small"),
                    "Daily Change (%)": st.column_config.NumberColumn("Day %", format="%.2f%%", width="small"),
                }                
                )