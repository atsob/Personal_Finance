import streamlit as st
import pandas as pd
from database.crud import save_changes
from database.crud import update_payee_default_category
from data.qif_importer import render_qif_importer
from database.backup import render_backup_restore
from database.backup import render_backup_restore_simple
from database.backup import render_backup_restore_quick
from data.downloaders import download_securities_info_from_yahoo


def render_settings(conn):
    """Render the Settings page."""
    st.title("System Settings")
    t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 = st.tabs(["Currencies", "Institutions", "Categories", "Securities", "Payees", "Accounts", "📁 QIF Importer", "💾 Backup & Restore", "💾 Backup & Restore Simple", "💾 Quick Backup & Restore"])
    
    df_curr_list = pd.read_sql("SELECT Currencies_Id, Currencies_ShortName FROM Currencies ORDER BY Currencies_ShortName ASC", conn)
    df_inst_list = pd.read_sql("SELECT Institutions_Id, Institutions_Name FROM Institutions ORDER BY Institutions_Name ASC", conn)
    df_sec_list = pd.read_sql("SELECT Securities_Id, Securities_Name FROM Securities ORDER BY Securities_Name ASC", conn)

    # Category hierarchy
    query_cat_hierarchy = """
    WITH RECURSIVE CategoryHierarchy AS (
        SELECT Categories_Id, Categories_Name::TEXT as Full_Path
        FROM Categories 
        WHERE Categories_Id_Parent IS NULL
        UNION ALL
        SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name
        FROM Categories c
        JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
    )
    SELECT Categories_Id, Full_Path FROM CategoryHierarchy ORDER BY Full_Path;
    """
    df_cat_list = pd.read_sql(query_cat_hierarchy, conn)

    df_moodys_list = pd.read_sql("SELECT Moodys FROM Credit_Ratings_LT ORDER BY Credit_Ratings_LT_Id ASC ", conn)
    df_s_p_list = pd.read_sql("SELECT S_P FROM Credit_Ratings_LT ORDER BY Credit_Ratings_LT_Id ASC ", conn)
    df_fitch_list = pd.read_sql("SELECT Fitch FROM Credit_Ratings_LT ORDER BY Credit_Ratings_LT_Id ASC ", conn)
    
    
    curr_options = df_curr_list.set_index('currencies_id')['currencies_shortname'].to_dict()
    inst_options = df_inst_list.set_index('institutions_id')['institutions_name'].to_dict()
    sec_options = df_sec_list.set_index('securities_id')['securities_name'].to_dict()
    cat_options = df_cat_list.set_index('categories_id')['full_path'].to_dict()

    # Μετατροπή των στηλών σε πεζά για να συμβαδίζουν με τον υπόλοιπο κώδικα
#    df_moodys_list.columns = df_moodys_list.columns.str.lower()
#    df_s_p_list.columns = df_s_p_list.columns.str.lower()
#    df_fitch_list.columns = df_fitch_list.columns.str.lower()

#    moodys_options = df_moodys_list.set_index('moodys')['moodys'].to_dict()
#    s_p_options = df_s_p_list.set_index('s_p')['s_p'].to_dict()
#    fitch_options = df_fitch_list.set_index('fitch')['fitch'].to_dict()
    
    moodys_options = dict(zip(df_moodys_list['moodys'], df_moodys_list['moodys']))
    s_p_options = dict(zip(df_s_p_list['s_p'], df_s_p_list['s_p']))
    fitch_options = dict(zip(df_fitch_list['fitch'], df_fitch_list['fitch']))

    
    with t1:
        df = pd.read_sql("SELECT * FROM Currencies ORDER BY Currencies_ShortName ASC", conn)
        edited_curr = st.data_editor(df, 
            num_rows="dynamic", 
            key="set_curr",
            column_config={
            #    "currencies_id": st.column_config.NumberColumn(
            #        "Currency ID", 
            #        width="small",
            #        disabled=True
            #    ),
                "currencies_id": None,
                "currencies_shortname": st.column_config.TextColumn(
                    "Currency ISO Code",
                    width="small"
                ),
                "currencies_name": st.column_config.TextColumn(
                    "Currency Name",
                    width="large"
                ),
                "embedding": None
            }
        )
        if not edited_curr.equals(df):
                save_changes(df, edited_curr, "Currencies", "currencies_id")
    
    with t2:
        df = pd.read_sql("SELECT Institutions_Id, Institutions_Name, Institutions_Type, BIC_Code, Moodys, S_P, Fitch, Contact, Phone, Email, Website, Notes, embedding FROM Institutions ORDER BY Institutions_Name ASC", conn)
        edited_inst = st.data_editor(
            df, 
            num_rows="dynamic", 
            key="set_inst",
            column_config={
            #    "institutions_id": st.column_config.NumberColumn(
            #        "Institution ID", 
            #        width="small",
            #        disabled=True
            #    ),
                "institutions_id": None,
                "institutions_name": st.column_config.TextColumn(
                    "Institution Name",
                    width="medium"
                ),
                "institutions_type": st.column_config.SelectboxColumn(
                    "Institution Type", options=['Bank', 'Credit Union', 'Insurance', 'Pension Fund', 'Broker', 'Crypto Exchange', 'Internal', 'Other']
                ),
                "bic_code": st.column_config.TextColumn(
                    "BIC Code",
                    width="small"
                ),
                "moodys": st.column_config.SelectboxColumn(
                    "Moody's", options=list(moodys_options.keys()), format_func=lambda x: moodys_options.get(x, "Unknown"),
                    width="small"
                ),
                "s_p": st.column_config.SelectboxColumn(
                    "S&P", options=list(s_p_options.keys()), format_func=lambda x: s_p_options.get(x, "Unknown"),
                    width="small"
                ),
                "fitch": st.column_config.SelectboxColumn(
                    "Fitch", options=list(fitch_options.keys()), format_func=lambda x: fitch_options.get(x, "Unknown"),
                    width="small"
                ),
                "contact": st.column_config.TextColumn(
                    "Contact Info",
                    width="small"
                ),
                "phone": st.column_config.TextColumn(
                    "Phone",
                    width="small"
                ),
                "email": st.column_config.TextColumn(
                    "Email Address",
                    width="medium"
                ),
                "website": st.column_config.TextColumn(
                    "Website",
                    width="small"
                ),
                "embedding": None
            }
        )
        if not edited_inst.equals(df):
                save_changes(df, edited_inst, "Institutions", "institutions_id")
    
    with t3:

        query_cat_hierarchy = """
        WITH RECURSIVE CategoryHierarchy AS (
            SELECT Categories_Id, Categories_Name::TEXT as Full_Path
            FROM Categories 
            WHERE Categories_Id_Parent IS NULL
            UNION ALL
            SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name
            FROM Categories c
            JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
        )
        SELECT Categories_Id, Full_Path FROM CategoryHierarchy ORDER BY Full_Path;
        """
        df_cat_list = pd.read_sql(query_cat_hierarchy, conn)
        cat_options = df_cat_list.set_index('categories_id')['full_path'].to_dict()

        df = pd.read_sql("SELECT * FROM Categories ORDER BY Categories_Id", conn)
        edited_cat = st.data_editor(
            df, 
            num_rows="dynamic", 
            key="set_cat", 
            column_config={
                "categories_id": None, # Hiding the categories_id since it's not very useful to edit and clutters the UI
                "categories_name": st.column_config.TextColumn(
                    "Category Name",
                    width="medium"
                ),
                "categories_type": st.column_config.SelectboxColumn("Type", options=['Income', 'Expense', 'Transfer', 'Investment_Buy', 'Investment_Sell', 'Dividend', 'Interest', 'Tax', 'Fee']),
                "categories_id_parent": st.column_config.SelectboxColumn(
                    "Parent Category", options=list(cat_options.keys()), 
                    format_func=lambda x: cat_options.get(x, "Unknown"), width="large"
                ),
                "embedding": None
            }
        )
        if not edited_cat.equals(df):
            save_changes(df, edited_cat, "Categories", "categories_id")
    
    with t4:
        df = pd.read_sql("SELECT Securities_Id, Ticker, Securities_Name, Securities_Type, Currencies_Id, Sector, Industry, Analyst_Rating, Analyst_Target_Price, Is_Active, Yahoo_Ticker, embedding FROM Securities ORDER BY Securities_Name", conn)
        edited_sec = st.data_editor(
             df, 
             num_rows="dynamic", 
             key="set_sec", 
             column_config={
                "securities_id": None, # Hiding the securities_id since it's not very useful to edit and clutters the UI
                "ticker": st.column_config.TextColumn(
                    "Ticker Symbol",
                    width="medium"
                ),
                "securities_name": st.column_config.TextColumn(
                    "Security Name",
                    width="medium"
                ),
                "securities_type": st.column_config.SelectboxColumn(
                    "Type", 
                    options=['Stock', 'ETF', 'Bond', 'CD', 'Emp. Stock Opt.', 'FX Spot', 'Market Index', 'Mutual Fund', 'Crypto', 'Option', 'Commodity', 'PF_Unit', 'Other'],
                    width="small"
                ),
                "currencies_id": st.column_config.SelectboxColumn(
                    "Currency", options=list(curr_options.keys()), format_func=lambda x: curr_options.get(x, "Unknown"),
                    width="small"
                ),
                "sector": st.column_config.TextColumn(
                    "Sector",
                    width="small"
                ),
                "industry": st.column_config.TextColumn(
                    "Industry",
                    width="small"
                ),
                "analyst_rating": st.column_config.TextColumn(
                    "Rating",
                    width="small"
                ),
                "analyst_target_price": st.column_config.NumberColumn(
                    "Target Price",
                    width="auto",
                    format="%,.2f"                
                ),
                "is_active": st.column_config.CheckboxColumn(
                    "Is Active",
                    width="small"
                ),
                "yahoo_ticker": st.column_config.TextColumn(
                    "Yahoo Ticker",
                    width="small"
                ),
                "embedding": None
            }
        )
        if not edited_sec.equals(df):
            save_changes(df, edited_sec, "Securities", "securities_id")
        
        if st.button("🚀 Update Securities Information from Yahoo", key="download_sec_info", width="stretch"):
            download_securities_info_from_yahoo()
            st.rerun
    
    with t5:
        df = pd.read_sql("SELECT * FROM Payees ORDER BY Payees_Name ASC", conn)
        edited_payee = st.data_editor(
            df, 
            num_rows="dynamic", 
            key="set_pay", 
            column_config={
                "payees_id": None, # Hiding the payees_id since it's not very useful to edit and clutters the UI
                "payees_name": st.column_config.TextColumn(
                     "Payee Name", 
                     width="medium"
                ),
                "categories_id_default": st.column_config.SelectboxColumn(
                    "Default Category", options=list(cat_options.keys()), format_func=lambda x: cat_options.get(x, "Unknown"),
                    width="medium"
                ),                
                "notes": st.column_config.TextColumn(
                    "Notes",
                    width="medium"
                ),
                "embedding": None
            }
        )
        if not edited_payee.equals(df):
             save_changes(df, edited_payee, "Payees", "payees_id")

        if st.button("🔄 Update Default Category based on usage, in case not defined"):
            with st.spinner("Processing..."):
                update_payee_default_category()
                st.success("Updated successfully!")
                st.balloons()
                st.rerun() 
    
    with t6:
        df = pd.read_sql("SELECT * FROM Accounts ORDER BY Accounts_Name ASC", conn)

        column_order = [
            "accounts_id", 
            "accounts_name", 
            "accounts_type", 
            "currencies_id", 
            "institutions_id", 
            "is_active",
            "iban", 
            "credit_limit", 
            "accounts_balance",
            "embedding"
        ]
        df = df[column_order]

        # Creation of a new column with Status icons
        def get_status_icon(q):
            if q == 0:
                return "🔵" # Blue for zero
            elif q < 0:
                return "🔴" # Red for negative (short)
            return "🟢"     # Green for positive

        # Addition of the Status column at the beginning of the DataFrame
        df.insert(0, "Balance", df['accounts_balance'].apply(get_status_icon))

        edited_acc = st.data_editor(
            df, 
            num_rows="dynamic", 
            key="set_acc", 
            width="content",
            column_config={
                "Balance": st.column_config.TextColumn("Status", width="small", disabled=True), # Locked column for status icons
                "accounts_id": None, # Hiding the accounts_id since it's not very useful to edit and clutters the UI
                "accounts_name": st.column_config.TextColumn(
                    "Account Name",
                    width="auto"
                ),
                "accounts_type": st.column_config.SelectboxColumn(
                    "Type", 
                    options=['Cash', 'Checking', 'Savings', 'Credit Card', 'Brokerage', 'Pension', 'Other Investment', 'Margin', 'Loan', 'Real Estate', 'Vehicle', 'Asset', 'Liability', 'Other'],
                    width="auto"
                ),
                "currencies_id": st.column_config.SelectboxColumn(
                    "Currency", options=list(curr_options.keys()), format_func=lambda x: curr_options.get(x, "Unknown"),
                    width="auto"
                ),
                "institutions_id": st.column_config.SelectboxColumn(
                    "Institution", options=list(inst_options.keys()), format_func=lambda x: inst_options.get(x, "Unknown"),
                    width="medium"
                ),
                "is_active": st.column_config.CheckboxColumn(
                    "Active",
                    width="auto"
                ),
                "iban": st.column_config.TextColumn(
                    "IBAN",
                    width="snall"
                ),
                "credit_limit": st.column_config.NumberColumn(
                    "Limit",
                    width="auto",
                    format="%,.2f"
                ),
                "accounts_balance": st.column_config.NumberColumn(
                    "Balance",
                    width="auto",
                    format="%,.2f"
                ),
                "embedding": None
            }
        )
    #    if not edited_acc.equals(df):
    #         save_changes(df, edited_acc, "Accounts", "accounts_id")

        if not edited_acc.equals(df):
            save_df = edited_acc.drop(columns=["Balance"])
            save_changes(df.drop(columns=["Balance"]), save_df, "Accounts", "accounts_id")

    with t7:  # QIF Importer tab
        render_qif_importer()

    with t8:  # Backup & Restore
        render_backup_restore()

    with t9:  # Backup & Restore Simple
        render_backup_restore_simple()

    with t10:  # Backup & Restore Quick
        render_backup_restore_quick()
        