import streamlit as st
import pandas as pd
from ui.components import format_qty_display, color_negative_red, style_qty_display
from database.crud import update_account_balances, update_holdings, update_investment_balances, update_pension_balances

def render_dashboard(conn):
    """Render the Dashboard page."""
    st.title("🏛 Net Worth")
    
    query_combined = """
        WITH Latest_FX AS (
            SELECT DISTINCT ON (Base_Currency_Id) Base_Currency_Id, FX_Rate 
            FROM Historical_FX 
            ORDER BY Base_Currency_Id, FX_Date DESC
        ),
        Latest_Prices AS (
            SELECT DISTINCT ON (Securities_Id) Securities_Id, Price_Close 
            FROM Historical_Prices 
            ORDER BY Securities_Id, Price_Date DESC
        )
        SELECT a.Accounts_Name as name, 'Assets' as type, c.Currencies_ShortName as curr, a.Account_Balance as qty,
               CASE WHEN c.Currencies_ShortName = 'EUR' THEN a.Account_Balance ELSE a.Account_Balance * COALESCE(fx.FX_Rate, 1) END as value_eur
        FROM Accounts a 
        LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_FX fx ON a.Currencies_Id = fx.Base_Currency_Id 
        WHERE a.Is_Active = TRUE AND a.Accounts_Type NOT IN ('Cash', 'Checking', 'Savings', 'Credit Card', 'Brokerage', 'Pension', 'Other Investment', 'Margin', 'Loan', 'Other')
        UNION ALL
        SELECT a.Accounts_Name as name, 'Cash' as type, c.Currencies_ShortName as curr, a.Account_Balance as qty,
               CASE WHEN c.Currencies_ShortName = 'EUR' THEN a.Account_Balance ELSE a.Account_Balance * COALESCE(fx.FX_Rate, 1) END as value_eur
        FROM Accounts a 
        LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_FX fx ON a.Currencies_Id = fx.Base_Currency_Id 
        WHERE a.Is_Active = TRUE AND a.Accounts_Type NOT IN ('Brokerage', 'Pension', 'Other Investment', 'Margin', 'Real Estate', 'Vehicle', 'Asset', 'Liability')
        UNION ALL
        SELECT a.Accounts_Name as name, 'Cash' as type, c.Currencies_ShortName as curr, a.Account_Balance as qty,
               CASE WHEN c.Currencies_ShortName = 'EUR' THEN a.Account_Balance ELSE a.Account_Balance * COALESCE(fx.FX_Rate, 1) END as value_eur
        FROM Accounts a 
        LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_FX fx ON a.Currencies_Id = fx.Base_Currency_Id 
        WHERE a.Is_Active = TRUE AND a.Accounts_Type IN ('Other Investment')
        UNION ALL
        SELECT a.Accounts_Name as name, 'Pension' as type, c.Currencies_ShortName as curr, a.Account_Balance as qty,
               CASE WHEN c.Currencies_ShortName = 'EUR' THEN a.Account_Balance ELSE a.Account_Balance * COALESCE(fx.FX_Rate, 1) END as value_eur
        FROM Accounts a 
        LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_FX fx ON a.Currencies_Id = fx.Base_Currency_Id 
        WHERE a.Is_Active = TRUE AND a.Accounts_Type IN ('Pension')
        UNION ALL
        SELECT 
            COALESCE(s.Security_Name, 'Unknown Security') as name, 
            'Investment' as type, 
            COALESCE(c.Currencies_ShortName, 'EUR') as curr, 
            h.Quantity as qty,
            CASE 
                WHEN COALESCE(c.Currencies_ShortName, 'EUR') = 'EUR' THEN h.Quantity * COALESCE(lp.Price_Close, 0) 
                ELSE (h.Quantity * COALESCE(lp.Price_Close, 0)) * COALESCE(fx.FX_Rate, 1) 
            END as value_eur
        FROM Holdings h 
        LEFT JOIN Securities s ON h.Securities_Id = s.Securities_Id 
        LEFT JOIN Currencies c ON s.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_Prices lp ON s.Securities_Id = lp.Securities_Id 
        LEFT JOIN Latest_FX fx ON c.Currencies_Id = fx.Base_Currency_Id
        WHERE h.Quantity <> 0
    """
    
    df_net = pd.read_sql(query_combined, conn)
    df_net.columns = [c.lower() for c in df_net.columns]
    df_net['type'] = df_net['type'].str.strip()
    df_net['qty_display'] = df_net.apply(format_qty_display, axis=1)
    
    # Display metrics
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Net Worth", f"€ {df_net['value_eur'].sum():,.2f}")
    m2.metric("Assets", f"€ {df_net[df_net['type']=='Assets']['value_eur'].sum():,.2f}")
    m3.metric("Cash", f"€ {df_net[df_net['type']=='Cash']['value_eur'].sum():,.2f}")
    m4.metric("Pension", f"€ {df_net[df_net['type']=='Pension']['value_eur'].sum():,.2f}")
    m5.metric("Investments", f"€ {df_net[df_net['type']=='Investment']['value_eur'].sum():,.2f}")
    
    # Apply Style and Formatting
    # We define the new order (qty_display before value_eur)
    new_order = ['name', 'type', 'curr', 'qty', 'qty_display', 'value_eur']
    df_net = df_net.reindex(columns=new_order)
    # We use .style for color and format simultaneously
    styled_df = df_net.style \
        .map(color_negative_red, subset=['value_eur', 'qty']) \
        .apply(lambda x: style_qty_display(df_net), subset=['qty_display'], axis=0) \
        .format({
            "qty": "{:,.2f}",
            "value_eur": "{:,.2f} €"
        }) \
        .hide(['qty'], axis=1)  # If this doesn't work, try: .hide(subset=['qty'], axis=1)

    # Display the Styled DataFrame
    # We define the order we want, skipping the 'qty' column
    st.dataframe(
        styled_df, 
        #use_container_width=True, 
        width="stretch", 
        hide_index=True,
        column_order=("name", "type", "curr", "qty_display", "value_eur"),
        column_config={
            "name": "Description",
            "type": "Category",
            "curr": "Currency",
            "qty_display": "Value / Quantity",
            "value_eur": "Value (€)"
        }
    )

    # Update buttons

    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        if st.button("🔄 Update Bank & Cash Accounts"):
            with st.spinner("Processing..."):
                update_account_balances()
                st.balloons()

    with row1_col2:
        if st.button("🔄 Update Investment Cash Accounts"):
            with st.spinner("Processing..."):
                update_investment_balances()
                st.balloons()

    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        if st.button("🔄 Update Pension Accounts"):
            with st.spinner("Processing..."):
                update_pension_balances()
                st.balloons()

    with row2_col2:
        if st.button("🔄 Update Security Holdings"):
            with st.spinner("Processing..."):
                update_holdings()
                st.balloons()

    row3_col1, row3_col2, row3_col3 = st.columns(3)

    with row3_col2:
        if st.button("🔄 Update All"):
            with st.spinner("Processing..."):
                update_account_balances()
                update_investment_balances()
                update_pension_balances()
                update_holdings()
                st.balloons()