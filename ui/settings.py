import streamlit as st
import pandas as pd
from database.crud import save_changes
from data.qif_importer import render_qif_importer
from database.backup import render_backup_restore
from database.backup import render_backup_restore_simple
from database.backup import render_backup_restore_quick


def render_settings(conn):
    """Render the Settings page."""
    st.title("System Settings")
    t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 = st.tabs(["Currencies", "Institutions", "Categories", "Securities", "Payees", "Accounts", "📁 QIF Importer", "💾 Backup & Restore", "💾 Backup & Restore Simple", "💾 Quick Backup & Restore"])
    
    df_curr_list = pd.read_sql("SELECT Currencies_Id, Currencies_ShortName FROM Currencies", conn)
    df_inst_list = pd.read_sql("SELECT FinancialInstitutions_Id, FinancialInstitutions_Name FROM FinancialInstitutions", conn)
    df_sec_list = pd.read_sql("SELECT Securities_Id, Security_Name FROM Securities", conn)
    
    curr_options = df_curr_list.set_index('currencies_id')['currencies_shortname'].to_dict()
    inst_options = df_inst_list.set_index('financialinstitutions_id')['financialinstitutions_name'].to_dict()
    
    with t1:
        df = pd.read_sql("SELECT * FROM Currencies ORDER BY Currencies_Id", conn)
        save_changes(df, st.data_editor(df, num_rows="dynamic", key="set_curr"), "Currencies", "currencies_id")
    
    with t2:
        df = pd.read_sql("SELECT * FROM FinancialInstitutions ORDER BY FinancialInstitutions_Id", conn)
        edited_inst = st.data_editor(
            df, 
            num_rows="dynamic", 
            key="set_inst",
            column_config={
                "financialinstitutions_type": st.column_config.SelectboxColumn("Institution Type", options=['Bank', 'Credit Union', 'Insurance', 'Pension Fund', 'Broker', 'Crypto Exchange', 'Internal', 'Other'])
            }
        )
        save_changes(df, edited_inst, "FinancialInstitutions", "financialinstitutions_id")
    
    with t3:
        df = pd.read_sql("SELECT * FROM Categories ORDER BY Categories_Id", conn)
        edited_cat = st.data_editor(df, num_rows="dynamic", key="set_cat", column_config={
            "category_type": st.column_config.SelectboxColumn("Type", options=['Income', 'Expense', 'Transfer', 'Investment_Buy', 'Investment_Sell', 'Dividend', 'Interest', 'Tax', 'Fee'])
        })
        save_changes(df, edited_cat, "Categories", "categories_id")
    
    with t4:
        df = pd.read_sql("SELECT * FROM Securities ORDER BY Security_Name", conn)
        edited_sec = st.data_editor(df, num_rows="dynamic", key="set_sec", column_config={
            "security_type": st.column_config.SelectboxColumn("Type", options=['Stock', 'ETF', 'Bond', 'CD', 'Emp. Stock Opt.', 'FX Spot', 'Market Index', 'Mutual Fund', 'Crypto', 'Option', 'Commodity', 'PF_Unit', 'Other']),
            "currencies_id": st.column_config.SelectboxColumn("Currency", options=list(curr_options.keys()), format_func=lambda x: curr_options.get(x, "Unknown"))
        })
        save_changes(df, edited_sec, "Securities", "securities_id")
    
    with t5:
        df = pd.read_sql("SELECT * FROM Payees ORDER BY Payees_Id", conn)
        save_changes(df, st.data_editor(df, num_rows="dynamic", key="set_pay"), "Payees", "payees_id")
    
    with t6:
        df = pd.read_sql("SELECT * FROM Accounts ORDER BY Accounts_Id", conn)
        edited_acc = st.data_editor(df, num_rows="dynamic", key="set_acc", column_config={
            "accounts_type": st.column_config.SelectboxColumn("Account Type", options=['Cash', 'Checking', 'Savings', 'Credit Card', 'Brokerage', 'Pension', 'Other Investment', 'Margin', 'Loan', 'Real Estate', 'Vehicle', 'Asset', 'Liability', 'Other']),
            "institution_id": st.column_config.SelectboxColumn("Institution", options=list(inst_options.keys()), format_func=lambda x: inst_options.get(x, "Unknown")),
            "currencies_id": st.column_config.SelectboxColumn("Currency", options=list(curr_options.keys()), format_func=lambda x: curr_options.get(x, "Unknown")),
            "is_active": st.column_config.CheckboxColumn("Active")
        })
        save_changes(df, edited_acc, "Accounts", "accounts_id")

    with t7:  # QIF Importer tab
        render_qif_importer()

    with t8:  # Backup & Restore
        render_backup_restore()

    with t9:  # Backup & Restore Simple
        render_backup_restore_simple()

    with t10:  # Backup & Restore Quick
        render_backup_restore_quick()
        