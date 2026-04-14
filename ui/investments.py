import streamlit as st
import pandas as pd
from database.crud import save_changes, update_holdings

def render_investments(conn):
    """Render the Investments page."""
    st.title("🥧 Investment Portfolio & Transactions")
    
    df_inv_accs = pd.read_sql("""
        SELECT Accounts_Id, Accounts_Name, 
            (SELECT SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity WHEN Action IN ('Sell', 'ShrOut') THEN -Quantity ELSE 0 END) 
             FROM Investment_Transactions WHERE Investment_Transactions.Accounts_Id = Accounts.Accounts_Id) Account_Position, 
            (SELECT SUM(CASE WHEN Action IN ('Dividend', 'CashIn', 'IntInc') THEN Total_Amount WHEN Action IN ('CashOut') THEN -Total_Amount ELSE 0 END) 
             FROM Investment_Transactions WHERE Investment_Transactions.Accounts_Id = Accounts.Accounts_Id) Account_Amount 
        FROM Accounts WHERE Accounts_Type IN ('Brokerage', 'Pension', 'Other Investment', 'Margin')
    """, conn)
    
    if df_inv_accs.empty:
        st.warning("⚠️ No investment accounts found. Set an account as 'Brokerage' or 'Pension' in Settings.")
        return
    
    selected_inv_acc = st.selectbox(
        "Select Investment / Pension Account:", 
        df_inv_accs.to_dict('records'), 
        format_func=lambda x: f"{x['accounts_name']} ({x['account_position']:,.8f}) ({x['account_amount']:,.2f})",
        key="inv_account_select"
    )
    inv_acc_id = selected_inv_acc['accounts_id']
    
    df_sec_list = pd.read_sql("SELECT Securities_Id, Security_Name FROM Securities", conn)
    sec_options = df_sec_list.set_index('securities_id')['security_name'].to_dict()
    
    tab_reg, tab_hold = st.tabs(["📓 Investment Register", "📊 Current Holdings"])
    
    with tab_reg:
        st.subheader(f"Transaction History: {selected_inv_acc['accounts_name']}")
        df_inv_tx = pd.read_sql(f"SELECT * FROM Investment_Transactions WHERE Accounts_Id = {inv_acc_id} ORDER BY Date DESC", conn)
        
        edited_inv_tx = st.data_editor(
            df_inv_tx,
            num_rows="dynamic",
            key=f"inv_tx_editor_{inv_acc_id}",
            width="stretch",
            column_config={
                "inv_transactions_id": st.column_config.NumberColumn("ID", disabled=True),
                "securities_id": st.column_config.SelectboxColumn(
                    "Security",
                    options=list(sec_options.keys()),
                    format_func=lambda x: sec_options.get(x, "Unknown"),
                    required=True
                ),
                "action": st.column_config.SelectboxColumn(
                    "Action", 
                    options=['Buy', 'Sell', 'Dividend', 'Reinvest', 'Split', 'ShrIn', 'ShrOut', 'IntInc', 'CashIn', 'CashOut', 'Vest', 'Expire', 'Grant', 'Exercise', 'MiscExp', 'RtrnCap'],
                    required=True
                ),
                "quantity": st.column_config.NumberColumn("Shares", format="%.8f"),
                "price_per_share": st.column_config.NumberColumn("Price", format="%.4f"),
                "total_amount": st.column_config.NumberColumn("Total Cash", format="%.2f")
            }
        )
        save_changes(df_inv_tx, edited_inv_tx, "Investment_Transactions", "inv_transactions_id")
    
    with tab_hold:
        st.subheader(f"Current Holdings: {selected_inv_acc['accounts_name']}")
        df_h = pd.read_sql(f"SELECT Holdings_Id, Accounts_Id, Securities_Id, Quantity, Simple_Avg_Price, Fifo_Avg_Price FROM Holdings WHERE Accounts_Id = {inv_acc_id}", conn)
        edited_h = st.data_editor(
            df_h, 
            key=f"inv_h_editor_{inv_acc_id}",
            width="stretch",
            column_config={
                # Hiding the IDs by setting them to None
                "holdings_id": None,
                "accounts_id": None,                
                "securities_id": st.column_config.SelectboxColumn(
                    "Security",
                    options=list(sec_options.keys()),
                    format_func=lambda x: sec_options.get(x, "NO SECURITY")
                ),
                # Format numbers
                "quantity": st.column_config.NumberColumn("Quantity", format="%.8f"),
                "simple_avg_price": st.column_config.NumberColumn("Simple Avg", format="%.4f"),
                "fifo_avg_price": st.column_config.NumberColumn("FIFO Avg", format="%.4f")
            },
            hide_index=True,
        )

        save_changes(df_h, edited_h, "Holdings", "holdings_id")
    
    if st.button("🚀 Update Holdings"):
        with st.spinner("Processing..."):
            update_holdings()
            st.balloons()