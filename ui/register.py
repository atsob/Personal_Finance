import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import calendar
from database.crud import save_changes, update_account_balances

def get_or_create_payee_id(conn, payee_name, default_categories_id=None):
    if not payee_name:
        return None
    payee_name = payee_name.strip()
    if not payee_name:
        return None

    cur = conn.cursor()
    cur.execute("SELECT payees_id FROM Payees WHERE Payees_Name = %s", (payee_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    if default_categories_id is not None:
        cur.execute("INSERT INTO Payees (Payees_Name, Default_Categories_Id) VALUES (%s, %s) RETURNING Payees_Id", (payee_name, default_categories_id))
    else:
        cur.execute("INSERT INTO Payees (Payees_Name) VALUES (%s) RETURNING Payees_Id", (payee_name,))
    payee_id = cur.fetchone()[0]
    conn.commit()
    return payee_id


def get_latest_fx_rate(conn, base_currency_id, target_currency_id, as_of_date=None):
    """Get the latest FX rate from base to target currency as of the given date."""
    base_currency_id = int(base_currency_id)
    target_currency_id = int(target_currency_id)
    if base_currency_id == target_currency_id:
        return 1.0

    if as_of_date is not None:
        if isinstance(as_of_date, datetime):
            as_of_date = as_of_date.date()
        elif isinstance(as_of_date, pd.Timestamp):
            as_of_date = as_of_date.date()
        elif isinstance(as_of_date, str):
            as_of_date = datetime.fromisoformat(as_of_date).date()

    cur = conn.cursor()
    if as_of_date:
        cur.execute("""
            SELECT fx_rate FROM Historical_FX 
            WHERE base_currency_id = %s AND target_currency_id = %s AND fx_date <= %s
            ORDER BY fx_date DESC LIMIT 1
        """, (base_currency_id, target_currency_id, as_of_date))
    else:
        cur.execute("""
            SELECT fx_rate FROM Historical_FX 
            WHERE base_currency_id = %s AND target_currency_id = %s 
            ORDER BY fx_date DESC LIMIT 1
        """, (base_currency_id, target_currency_id))
    row = cur.fetchone()
    if row:
        return float(row[0])
    
    # Try reverse rate
    if as_of_date:
        cur.execute("""
            SELECT 1.0 / fx_rate FROM Historical_FX 
            WHERE base_currency_id = %s AND target_currency_id = %s AND fx_date <= %s
            ORDER BY fx_date DESC LIMIT 1
        """, (target_currency_id, base_currency_id, as_of_date))
    else:
        cur.execute("""
            SELECT 1.0 / fx_rate FROM Historical_FX 
            WHERE base_currency_id = %s AND target_currency_id = %s 
            ORDER BY fx_date DESC LIMIT 1
        """, (target_currency_id, base_currency_id))
    row = cur.fetchone()
    if row:
        return float(row[0])
    
    return 1.0  # Default if no rate found


def add_months(start_date, months):
    month = start_date.month - 1 + months
    year = start_date.year + month // 12
    month = month % 12 + 1
    day = min(start_date.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def advance_date(base_date, periodicity):
    if periodicity == "Weekly":
        return base_date + timedelta(weeks=1)
    if periodicity == "Monthly":
        return add_months(base_date, 1)
    if periodicity == "Bi-Monthly":
        return add_months(base_date, 2)
    if periodicity == "Quarterly":
        return add_months(base_date, 3)
    if periodicity == "Semi-Annually":
        return add_months(base_date, 6)
    if periodicity == "Annually":
        return add_months(base_date, 12)
    return base_date


def build_recurrence_schedule(start_date, next_date, periodicity, installments=None, end_date=None):
    dates = [start_date]
    if not periodicity:
        return dates

    next_date = next_date or advance_date(start_date, periodicity)
    if next_date <= start_date:
        next_date = advance_date(start_date, periodicity)

    current = next_date
    while True:
        if installments is not None:
            if len(dates) >= installments:
                break
        elif end_date is not None:
            if current > end_date:
                break
        else:
            break

        dates.append(current)
        current = advance_date(current, periodicity)

    return dates


def insert_bank_transaction(cur, accounts_id, tx_date, payees_id, description, total_amount, cleared=True, target_account_id=None, target_amount=None, transfer_id=None):
    if transfer_id is not None:
        cur.execute(
            """
            INSERT INTO Bank_Transactions (Accounts_Id, Date, Payees_Id, Description, Total_Amount, Cleared, Target_Account_Id, Target_Amount, Transfer_Id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING Bank_Transactions_Id
            """,
            (accounts_id, tx_date, payees_id, description, total_amount, cleared, target_account_id, target_amount, transfer_id)
        )
    else:
        cur.execute(
            """
            INSERT INTO Bank_Transactions (Accounts_Id, Date, Payees_Id, Description, Total_Amount, Cleared, Target_Account_Id, Target_Amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING Bank_Transactions_Id
            """,
            (accounts_id, tx_date, payees_id, description, total_amount, cleared, target_account_id, target_amount)
        )
    return cur.fetchone()[0]


def insert_transaction_splits(cur, bank_transaction_id, splits):
    for split in splits:
        cur.execute(
            """
            INSERT INTO Bank_Transaction_Splits (Bank_Transactions_Id, Categories_Id, Amount, Memo)
            VALUES (%s, %s, %s, %s)
            """,
            (bank_transaction_id, split.get('categories_id'), split['amount'], split.get('memo'))
        )


def reset_transaction_form_state():
    if 'reset_counter' not in st.session_state:
        st.session_state.reset_counter = 0
    st.session_state.reset_counter += 1


def render_register(conn):
    """Render the Register page."""
    st.title("📝 Account Transactions Register")
    
    # Initialize counters
    st.session_state.reset_counter = st.session_state.get('reset_counter', 0)
    st.session_state.balance_update_counter = st.session_state.get('balance_update_counter', 0)
    
    # Load data with caching
    if 'df_accs' not in st.session_state:
        st.session_state.df_accs = pd.read_sql("SELECT * FROM Accounts WHERE Is_Active = True", conn)
    df_accs = st.session_state.df_accs
    
    if 'df_payees' not in st.session_state:
        st.session_state.df_payees = pd.read_sql("SELECT Payees_Id, Payees_Name, Default_Categories_Id FROM Payees", conn)
    df_payees = st.session_state.df_payees
    
    if 'df_payee_list' not in st.session_state:
        st.session_state.df_payee_list = pd.read_sql("SELECT Payees_Id, Payees_Name FROM Payees", conn)
    df_payee_list = st.session_state.df_payee_list
    
    # Category hierarchy
    if 'df_cat_list' not in st.session_state:
        query_cat_hierarchy = """
        WITH RECURSIVE CategoryHierarchy AS (
            SELECT Categories_Id, Categories_Name::TEXT as Full_Path
            FROM Categories 
            WHERE Parent_Category_Id IS NULL
            UNION ALL
            SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name
            FROM Categories c
            JOIN CategoryHierarchy ch ON c.Parent_Category_Id = ch.Categories_Id
        )
        SELECT Categories_Id, Full_Path FROM CategoryHierarchy ORDER BY Full_Path;
        """
        st.session_state.df_cat_list = pd.read_sql(query_cat_hierarchy, conn)
    df_cat_list = st.session_state.df_cat_list
    
    acc_options = {
        row['accounts_id']: f"{row['accounts_name']} ({row['account_balance']:,.2f})" 
        for _, row in df_accs.iterrows()
    }
    acc_ids_list = list(acc_options.keys())
    payee_options = df_payee_list.set_index('payees_id')['payees_name'].to_dict()
    payee_names = df_payee_list['payees_name'].tolist()
    cat_options = df_cat_list.set_index('categories_id')['full_path'].to_dict()
    
    # Account selection
    if 'account_id_internal' not in st.session_state or st.session_state.account_id_internal is None:
        st.session_state.account_id_internal = acc_ids_list[0] if acc_ids_list else None
    
    # Determine the index for the selectbox based on current account in session state
    current_account = st.session_state.account_id_internal
    default_index = acc_ids_list.index(current_account) if current_account in acc_ids_list else 0
    
    acc_id = st.selectbox(
        "Select Account:", 
        options=acc_ids_list,
        format_func=lambda x: acc_options.get(x, "Unknown"),
        index=default_index,
        key=f"account_id_internal_{st.session_state.balance_update_counter}"
    )
    
    # Update session state with the selected account
    st.session_state.account_id_internal = acc_id
    
    acc_type = df_accs.loc[df_accs['accounts_id'] == acc_id, 'accounts_type'].values[0]
    
    # Main transactions df
    if 'register_df' not in st.session_state or st.session_state.get('register_acc_id') != acc_id:
        df = pd.read_sql(f"SELECT * FROM Bank_Transactions WHERE Accounts_Id = {acc_id} ORDER BY Date DESC", conn)
        st.session_state.register_df = df
        st.session_state.register_acc_id = acc_id
    else:
        df = st.session_state.register_df
    
    if acc_type not in ['Brokerage', 'Pension']:
        t_view, t_new = st.tabs(["👁️ View Register", "➕ New Transaction / Transfer"])

        with t_new:
            t_tx, t_transfer = st.tabs(["Transaction", "Money Transfer"])

            with t_tx:
                st.info("Create a bank/cash transaction and optional category splits.")

                recurring = st.checkbox("Recurring transaction", key=f"tx_recurring_{st.session_state.reset_counter}")

                transaction_mode = st.radio(
                    "Transaction mode",
                    ["Single Category", "Split Transaction"],
                    horizontal=True,
                    key=f"tx_transaction_mode_{st.session_state.reset_counter}"
                )

                with st.form("tx_form_with_splits"):
                    c1, c2 = st.columns(2)
                    date = c1.date_input("Date", datetime.now().date(), key=f"tx_date_{st.session_state.reset_counter}")

                    if payee_names:
                        selected_payee = c2.selectbox(
                            "Payee",
                            ["(new payee)"] + payee_names,
                            index=0,
                            help="Select an existing payee or choose New to type a payee name.",
                            key=f"tx_payee_select_{st.session_state.reset_counter}"
                        )
                        if selected_payee == "(new payee)":
                            payee_name = c2.text_input(
                                "New Payee Name",
                                value="",
                                help="Enter a new payee name.",
                                key=f"tx_payee_text_{st.session_state.reset_counter}"
                            )
                        else:
                            payee_name = selected_payee
                    else:
                        payee_name = c2.text_input(
                            "Payee",
                            value="",
                            help="Enter a payee name. Missing payees are created automatically.",
                            key=f"tx_payee_text_{st.session_state.reset_counter}"
                        )

                    # Determine default category for existing payee
                    default_cat = None
                    if payee_name and payee_name in payee_names:
                        payee_row = df_payees[df_payees['payees_name'] == payee_name]
                        if not payee_row.empty:
                            default_cat = payee_row['default_categories_id'].values[0]

                    total_amount = st.number_input("Total Amount", value=0.0, format="%.2f", key=f"tx_total_amount_{st.session_state.reset_counter}")
                    desc = st.text_input("Description", key=f"tx_description_{st.session_state.reset_counter}")

                    if total_amount > 0:
                        st.success("Income transaction")
                    elif total_amount < 0:
                        st.error("Expense transaction")
                    else:
                        st.info("Zero amount transaction. Enter a non-zero amount to save.")

                    st.write("---")
                    transaction_category = None
                    split_rows = []
                    if transaction_mode == "Single Category":
                        cat_list = [None] + list(cat_options.keys())
                        default_index = 0
                        if default_cat and default_cat in cat_list:
                            default_index = cat_list.index(default_cat)
                        transaction_category = st.selectbox(
                            "Category",
                            cat_list,
                            format_func=lambda x: cat_options.get(x, "Select a category") if x else "Select a category",
                            index=default_index,
                            key=f"tx_transaction_category_{st.session_state.reset_counter}"
                        )
                    else:
                        st.info("Add one row per split. The sum of split amounts must equal the Total Amount.")
                        df_new_splits = pd.DataFrame([{'categories_id': None, 'amount': 0.0, 'memo': ''}])
                        new_splits_data = st.data_editor(
                            df_new_splits,
                            num_rows="dynamic",
                            hide_index=True,
                            use_container_width=True,
                            key=f"new_splits_editor_{st.session_state.reset_counter}",
                            column_config={
                                "categories_id": st.column_config.SelectboxColumn(
                                    "Category",
                                    options=list(cat_options.keys()),
                                    format_func=lambda x: cat_options.get(x, "Unknown")
                                ),
                                "amount": st.column_config.NumberColumn("Amount"),
                                "memo": st.column_config.TextColumn("Memo")
                            }
                        )
                        new_splits_data = new_splits_data.dropna(subset=['categories_id', 'amount'], how='all').reset_index(drop=True)
                        split_rows = new_splits_data.to_dict('records')

                    if recurring:
                        next_occurrence = None
                        recurrence_periodicity = None
                        recurrence_installments = None
                        recurrence_end_date = None
                        with st.expander("Recurring transaction options", expanded=True):
                            r1, r2 = st.columns(2)
                            next_occurrence = r1.date_input("Next occurrence date", date + timedelta(days=30), key=f"tx_next_occurrence_{st.session_state.reset_counter}")
                            recurrence_periodicity = r2.selectbox(
                                "Periodicity",
                                ["Weekly", "Monthly", "Bi-Monthly", "Quarterly", "Semi-Annually", "Annually"],
                                key=f"tx_recurrence_periodicity_{st.session_state.reset_counter}"
                            )

                            recurrence_end_type = st.radio(
                                "End after",
                                ["Number of installments", "End date"],
                                horizontal=True,
                                key=f"tx_recurrence_end_type_{st.session_state.reset_counter}"
                            )

                            if recurrence_end_type == "Number of installments":
                                recurrence_installments = st.number_input(
                                    "Installments",
                                    min_value=2,
                                    value=2,
                                    step=1,
                                    help="Total occurrences including the current transaction.",
                                    key=f"tx_recurrence_installments_{st.session_state.reset_counter}"
                                )
                            else:
                                recurrence_end_date = st.date_input("End date", date + timedelta(days=365), key=f"tx_recurrence_end_date_{st.session_state.reset_counter}")
                    else:
                        next_occurrence = None
                        recurrence_periodicity = None
                        recurrence_installments = None
                        recurrence_end_date = None
                    
                    if st.form_submit_button("🔥 Save Transaction & Splits"):
                        try:
                            val_total_amount = float(total_amount)
                            if val_total_amount == 0:
                                st.error("The total amount must be non-zero.")
                            elif transaction_mode == "Single Category" and not transaction_category:
                                st.error("Please select a category for the single-category transaction.")
                            elif transaction_mode == "Split Transaction" and not split_rows:
                                st.error("Please add at least one split for a split transaction.")
                            else:
                                if transaction_mode == "Split Transaction":
                                    split_amounts = [pd.to_numeric(row['amount'], errors='coerce') for row in split_rows]
                                    if any(pd.isna(x) for x in split_amounts):
                                        st.error("Please enter valid numeric amounts for all split rows.")
                                        raise ValueError("Invalid split amount")

                                    if abs(sum(float(x) for x in split_amounts) - val_total_amount) > 0.01:
                                        st.error(f"Split total ({sum(float(x) for x in split_amounts):,.2f}) doesn't match Total Amount ({val_total_amount:,.2f})")
                                        raise ValueError("Split total mismatch")
                                
                                dates = build_recurrence_schedule(
                                    date,
                                    next_occurrence,
                                    recurrence_periodicity,
                                    installments=recurrence_installments,
                                    end_date=recurrence_end_date
                                )

                                payee_id = get_or_create_payee_id(conn, payee_name.strip()) if payee_name else None

                                # Set default category for new payee if single category transaction
                                if payee_id and transaction_mode == "Single Category" and payee_name and payee_name not in payee_names:
                                    cur = conn.cursor()
                                    cur.execute("UPDATE Payees SET Default_Categories_Id = %s WHERE Payees_Id = %s", (transaction_category, payee_id))
                                    conn.commit()
                                cur = conn.cursor()

                                for idx, tx_date in enumerate(dates):
                                    tx_desc = desc or ""
                                    if len(dates) > 1:
                                        tx_desc = f"{tx_desc} ({idx + 1}/{len(dates)})"

                                    tx_id = insert_bank_transaction(
                                        cur,
                                        acc_id,
                                        tx_date,
                                        payee_id,
                                        tx_desc,
                                        val_total_amount
                                    )

                                    if transaction_mode == "Single Category":
                                        insert_transaction_splits(
                                            cur,
                                            tx_id,
                                            [{
                                                'categories_id': transaction_category,
                                                'amount': val_total_amount,
                                                'memo': None
                                            }]
                                        )
                                    else:
                                        for row in split_rows:
                                            row_amount = float(pd.to_numeric(row['amount'], errors='coerce') or 0)
                                            insert_transaction_splits(
                                                cur,
                                                tx_id,
                                                [{
                                                    'categories_id': row['categories_id'],
                                                    'amount': row_amount,
                                                    'memo': row.get('memo')
                                                }]
                                            )

                                conn.commit()
                                update_account_balances(st.session_state["account_id_internal"])
                                st.session_state.balance_update_counter += 1
                                st.success("Transaction and splits saved!")
                                reset_transaction_form_state()
                                st.rerun()
                        except ValueError:
                            pass
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Error saving transaction: {e}")

            with t_transfer:
                st.info("Create a money transfer and mirror it on the target account.")

                recurring_transfer = st.checkbox("Recurring transfer", key=f"transfer_recurring_{st.session_state.reset_counter}")

                with st.form("tx_form_transfer"):
                    date = st.date_input("Date", datetime.now().date(), key=f"transfer_date_{st.session_state.reset_counter}")
                    
                    direction = st.radio(
                        "Transfer direction",
                        ["Transfer out", "Transfer in"],
                        horizontal=True,
                        key=f"transfer_direction_{st.session_state.reset_counter}"
                    )

                    target_accounts = [aid for aid in acc_ids_list if aid != acc_id]
                    if not target_accounts:
                        st.warning("No other active accounts are available for transfers.")
                        target_account_id = None
                        source_curr = None
                        target_curr = None
                        fx_rate = 1.0
                    else:
                        target_account_id = st.selectbox(
                            "Target Account",
                            target_accounts,
                            format_func=lambda x: acc_options.get(x, "Unknown"),
                            key=f"transfer_target_account_{st.session_state.reset_counter}"
                        )
                        source_curr = int(df_accs.loc[df_accs['accounts_id'] == acc_id, 'currencies_id'].values[0])
                        target_curr = int(df_accs.loc[df_accs['accounts_id'] == target_account_id, 'currencies_id'].values[0])
                        fx_rate = float(get_latest_fx_rate(conn, source_curr, target_curr, date))

                    t_amount = st.number_input("Transfer Amount", value=0.0, format="%.2f", key=f"transfer_amount_{st.session_state.reset_counter}")
                    default_target_amount = abs(t_amount) * fx_rate if t_amount != 0 else 0.0
                    t_target_amount = st.number_input(
                        "Target Amount",
                        value=default_target_amount,
                        format="%.2f",
                        help=f"Auto-calculated based on FX rate {fx_rate:.4f}. Edit if needed.",
                        key=f"transfer_target_amount_{st.session_state.reset_counter}"
                    )

                    # Payee selection for transfers
                    default_payee_index = 0
                    if "Transfer Money" in payee_names:
                        default_payee_index = payee_names.index("Transfer Money") + 1  # +1 for "(new payee)"

                    selected_transfer_payee = st.selectbox(
                        "Payee",
                        ["(new payee)"] + payee_names,
                        index=default_payee_index,
                        help="Select an existing payee or choose New to type a payee name.",
                        key=f"transfer_payee_select_{st.session_state.reset_counter}"
                    )
                    if selected_transfer_payee == "(new payee)":
                        transfer_payee_name = st.text_input(
                            "New Payee Name",
                            value="Transfer Money" if default_payee_index == 0 else "",
                            help="Enter a new payee name.",
                            key=f"transfer_payee_text_{st.session_state.reset_counter}"
                        )
                    else:
                        transfer_payee_name = selected_transfer_payee

                    transfer_desc = st.text_input("Description", key=f"transfer_description_{st.session_state.reset_counter}")

                    if recurring_transfer:
                        next_transfer_date = None
                        transfer_periodicity = None
                        transfer_installments = None
                        transfer_end_date = None
                        with st.expander("Recurring transfer options", expanded=True):
                            r1, r2 = st.columns(2)
                            next_transfer_date = r1.date_input("Next occurrence date", date + timedelta(days=30), key=f"transfer_next_date_{st.session_state.reset_counter}")
                            transfer_periodicity = r2.selectbox(
                                "Periodicity",
                                ["Weekly", "Monthly", "Bi-Monthly", "Quarterly", "Semi-Annually", "Annually"],
                                key=f"transfer_periodicity_{st.session_state.reset_counter}"
                            )

                            transfer_end_type = st.radio(
                                "End after",
                                ["Number of installments", "End date"],
                                horizontal=True,
                                key=f"transfer_end_type_{st.session_state.reset_counter}"
                            )

                            if transfer_end_type == "Number of installments":
                                transfer_installments = st.number_input(
                                    "Installments",
                                    min_value=2,
                                    value=2,
                                    step=1,
                                    help="Total occurrences including the current transfer.",
                                    key=f"transfer_installments_{st.session_state.reset_counter}"
                                )
                            else:
                                transfer_end_date = st.date_input("End date", date + timedelta(days=365), key=f"transfer_end_date_{st.session_state.reset_counter}")
                    else:
                        next_transfer_date = None
                        transfer_periodicity = None
                        transfer_installments = None
                        transfer_end_date = None

                    if st.form_submit_button("🔥 Save Transfer"):
                        try:
                            if t_amount <= 0:
                                st.error("Transfer amount must be greater than zero.")
                            elif not target_account_id:
                                st.error("Please select a target account for the transfer.")
                            else:
                                payee_id = get_or_create_payee_id(conn, transfer_payee_name.strip()) if transfer_payee_name else None
                                transfer_amount = abs(t_amount)
                                transfer_target_amount = abs(t_target_amount) if t_target_amount else transfer_amount
                                dates = build_recurrence_schedule(
                                    date,
                                    next_transfer_date,
                                    transfer_periodicity,
                                    installments=transfer_installments,
                                    end_date=transfer_end_date
                                )

                                cur = conn.cursor()
                                for idx, tx_date in enumerate(dates):
                                    # Generate a new Transfer_Id for this transfer pair
                                    cur.execute("SELECT nextval('transfer_id_seq')")
                                    transfer_id = cur.fetchone()[0]
                                    
                                    transfer_label = transfer_desc or "Transfer"
                                    if len(dates) > 1:
                                        transfer_label = f"{transfer_label} ({idx + 1}/{len(dates)})"

                                    if direction == "Transfer out":
                                        source_account = acc_id
                                        destination_account = target_account_id
                                        source_amount = -transfer_amount
                                        destination_amount = transfer_target_amount
                                    else:
                                        source_account = target_account_id
                                        destination_account = acc_id
                                        source_amount = -transfer_target_amount
                                        destination_amount = transfer_amount

                                    source_tx_id = insert_bank_transaction(
                                        cur,
                                        source_account,
                                        tx_date,
                                        payee_id,
                                        transfer_label,
                                        source_amount,
                                        target_account_id=destination_account,
                                        target_amount=abs(destination_amount),
                                        transfer_id=transfer_id
                                    )
                                    insert_transaction_splits(
                                        cur,
                                        source_tx_id,
                                        [{
                                            'categories_id': None,
                                            'amount': abs(source_amount),
                                            'memo': 'Transfer'
                                        }]
                                    )

                                    destination_tx_id = insert_bank_transaction(
                                        cur,
                                        destination_account,
                                        tx_date,
                                        payee_id,
                                        transfer_label,
                                        destination_amount,
                                        target_account_id=source_account,
                                        target_amount=abs(source_amount),
                                        transfer_id=transfer_id
                                    )
                                    insert_transaction_splits(
                                        cur,
                                        destination_tx_id,
                                        [{
                                            'categories_id': None,
                                            'amount': abs(destination_amount),
                                            'memo': 'Transfer'
                                        }]
                                    )

                                conn.commit()
                                update_account_balances(acc_id)
                                update_account_balances(target_account_id)
                                st.session_state.balance_update_counter += 1
                                st.success("Transfer saved and mirrored successfully!")
                                reset_transaction_form_state()
                                st.rerun()
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Error saving transfer: {e}")

        with t_view:
            query_reg = f"SELECT * FROM Bank_Transactions WHERE Accounts_Id = {acc_id} ORDER BY Date DESC"
            df = pd.read_sql(query_reg, conn)
            
            unique_key = f"set_reg_{acc_id}"
            edited_reg = st.data_editor(
                df, 
                num_rows="dynamic", 
                key=unique_key, 
                width="stretch", 
                column_config={
                    "accounts_id": st.column_config.SelectboxColumn("Account", options=list(acc_options.keys()), format_func=lambda x: acc_options.get(x, "Unknown")),
                    "payees_id": st.column_config.SelectboxColumn("Payee", options=list(payee_options.keys()), format_func=lambda x: payee_options.get(x, "Unknown")),
                    "target_account_id": st.column_config.SelectboxColumn("Target Account", options=list(acc_options.keys()), format_func=lambda x: acc_options.get(x, "Unknown"))
                }
            )
            
            if not edited_reg.equals(df):
                save_changes(df, edited_reg, "Bank_Transactions", "bank_transactions_id", current_acc_id=acc_id, conn=conn)
                # Update cache after save
                st.session_state.register_df = pd.read_sql(f"SELECT * FROM Bank_Transactions WHERE Accounts_Id = {acc_id} ORDER BY Date DESC", conn)
                st.session_state.df_accs = pd.read_sql("SELECT * FROM Accounts WHERE Is_Active = True", conn)
                st.rerun()
            
            # Splits section
            st.write("---")
            st.subheader("🔍 Split Analysis")
            
            available_ids = df['bank_transactions_id'].tolist()
            default_ix = 0
            if st.session_state.current_tx_id in available_ids:
                default_ix = available_ids.index(st.session_state.current_tx_id) + 1
            
            selected_tx_id = st.selectbox("Select Transaction ID for Splits:", [None] + available_ids, index=default_ix)
            
            if st.button("View Splits", key=f"view_splits_{acc_id}_{selected_tx_id}"):
                st.session_state.show_splits_pane = True
                st.session_state.current_tx_id = selected_tx_id
                st.rerun()
            
            if st.session_state.show_splits_pane and st.session_state.current_tx_id:
                if selected_tx_id != st.session_state.current_tx_id:
                    st.session_state.show_splits_pane = False
                    st.session_state.current_tx_id = None
                    st.rerun()
                
                st.write("---")
                st.write(f"### 📑 Edit Splits for ID: {st.session_state.current_tx_id}")
                
                df_splits = pd.read_sql("SELECT * FROM Bank_Transaction_Splits WHERE Bank_Transactions_Id = %s", 
                                        conn, params=(int(st.session_state.current_tx_id),))
                
                edited_splits = st.data_editor(
                    df_splits,
                    num_rows="dynamic",
                    key=f"splits_ed_{st.session_state.current_tx_id}",
                    width="stretch",
                    column_config={
                        "categories_id": st.column_config.SelectboxColumn(
                            "Category", 
                            options=list(cat_options.keys()), 
                            format_func=lambda x: cat_options.get(x, "Unknown"),
                            width="large"
                        ),
                        "bank_transactions_id": None
                    }
                )
                
                if st.button("💾 Save Splits Changes", key=f"save_{st.session_state.current_tx_id}"):
                    from database.crud import save_changes as save_changes_generic
                    save_changes_generic(df_splits, edited_splits, "Bank_Transaction_Splits", "split_id")
                    st.rerun()
    else:
        df_inv = pd.read_sql(f"SELECT * FROM Investment_Transactions WHERE Accounts_Id = {acc_id} ORDER BY Date DESC", conn)
        save_changes(df_inv, st.data_editor(df_inv, width="stretch", key="inv_reg"), "Investment_Transactions", "inv_transactions_id")