import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import calendar
from database.connection import get_db
from database.crud import save_changes, execute_db_save, update_accounts_balances, update_holdings, update_investment_balances, update_pension_balances

def _render_transaction_table(acc_id, payee_options, acc_options, cat_options, tab_key):
    """Render a filtered transaction register for one account.

    Filters reduce the dataset client-side — all matching rows are loaded into
    memory (no DB pagination). This keeps inline editing simple: edits are
    committed with an explicit Save button so typing in a cell does not
    trigger a rerun.

    tab_key must be unique per call ('bank' / 'cash') to avoid widget key
    collisions when both tabs exist on the same page.
    """
    from datetime import date, timedelta

    _sk = f"reg_{acc_id}_{tab_key}"  # session-state key prefix

    # ── Filter bar ────────────────────────────────────────────────────────────
    today = date.today()
    default_from = today - timedelta(days=30)

    f1, f2, f3, f4 = st.columns([3, 1.5, 1.5, 1.5])

    with f1:
        _period = st.radio(
            "Period",
            options=["30d", "3mo", "6mo", "1y", "All", "Custom"],
            index=0,
            horizontal=True,
            key=f"{_sk}_period",
            label_visibility="collapsed"
        )

    _period_map = {
        "30d": today - timedelta(days=30),
        "3mo": today - timedelta(days=90),
        "6mo": today - timedelta(days=180),
        "1y":  today - timedelta(days=365),
        "All": date(1900, 1, 1),
    }

    if _period == "Custom":
        with f2:
            _from_date = st.date_input("From", value=default_from, key=f"{_sk}_from")
        with f3:
            _to_date = st.date_input("To", value=today, key=f"{_sk}_to")
        _show_future = False  # custom range already explicit
    else:
        _from_date = _period_map.get(_period, default_from)
        _to_date = today

    with f4:
        _status = st.selectbox(
            "Status",
            options=["All", "Cleared", "Uncleared"],
            key=f"{_sk}_status",
            label_visibility="collapsed"
        )

    # Show future transactions checkbox (not shown for Custom — range is explicit)
    if _period != "Custom":
        _show_future = st.checkbox(
            "Include future transactions",
            value=True,
            key=f"{_sk}_future"
        )
        if _show_future:
            _to_date = date(2099, 12, 31)

    # ── Query — all matching rows, no LIMIT/OFFSET ────────────────────────────
    _params = [acc_id, _from_date, _to_date]
    _where = "WHERE Accounts_Id = %s AND Date BETWEEN %s AND %s"
    if _status == "Cleared":
        _where += " AND Cleared = TRUE"
    elif _status == "Uncleared":
        _where += " AND Cleared = FALSE"

    with get_db() as conn:
        df = pd.read_sql(
            f"SELECT * FROM Transactions {_where} ORDER BY Date DESC",
            conn, params=_params
        )

    # ── Stat cards ────────────────────────────────────────────────────────────
    s1, s2 = st.columns(2)
    with s1:
        st.metric("Transactions", f"{len(df):,}")
    with s2:
        st.metric("Period total", f"{float(df['total_amount'].sum()):,.2f}" if not df.empty else "0.00")

    if len(df) > 500:
        st.warning(f"⚠️ {len(df):,} transactions — consider narrowing the date range for better performance.")

    # ── Data editor ───────────────────────────────────────────────────────────
    df.insert(0, "_selected", False)

    # Stable editor key: changes only when filter changes, not on every rerun.
    _filter_sig = (_period, _status, acc_id,
                   str(_from_date) if _period == "Custom" else "",
                   str(_to_date)   if _period == "Custom" else "")
    unique_key = f"set_reg_{acc_id}_{tab_key}_{hash(str(_filter_sig)) % 10**8}"
    _orig_key  = f"{unique_key}_orig"

    # Store the original df the first time this filter combination is seen.
    # This is the baseline for change detection — NOT the re-fetched df,
    # which would always equal edited_reg after a rerun and hide pending edits.
    if _orig_key not in st.session_state:
        st.session_state[_orig_key] = df.copy()
    df_original = st.session_state[_orig_key]

    col_config = {
        "_selected": st.column_config.CheckboxColumn(
            "☑", width="small",
            help="Tick to select for moving to another account"
        ),
        "transactions_id": st.column_config.NumberColumn(
            "ID", width="small", disabled=True
        ),
        "accounts_id": None,
        "date": st.column_config.DateColumn(
            "Date", width="small", format="DD/MM/YYYY"
        ),
        "payees_id": st.column_config.SelectboxColumn(
            "Payee", width="medium",
            options=list(payee_options.keys()),
            format_func=lambda x: payee_options.get(x, "Unknown")
        ),
        "description": st.column_config.TextColumn("Description"),
        "total_amount": st.column_config.NumberColumn(
            "Amount", width="small", format="%,.2f"
        ),
        "cleared": st.column_config.CheckboxColumn("Cleared", width="small"),
        "accounts_id_target": st.column_config.SelectboxColumn(
            "Target Account",
            options=list(acc_options.keys()),
            format_func=lambda x: acc_options.get(x, "Unknown")
        ),
        "total_amount_target": st.column_config.NumberColumn(
            "Target Amount", width="small", format="%,.2f"
        ),
        "transfers_id": None,
        "embedding": None,
    }

    edited_reg = st.data_editor(
        df_original,   # always render from the stored original so edits persist
        num_rows="dynamic",
        key=unique_key,
        use_container_width=True,
        column_config=col_config,
    )

    # ── Change detection & Save ───────────────────────────────────────────────
    _orig_for_cmp   = df_original.drop(columns=["_selected"])
    edited_for_save = edited_reg.drop(columns=["_selected"], errors="ignore")

    # Align dtypes before comparing — pd.read_sql and st.data_editor can produce
    # different dtypes for the same values (e.g. int64 vs object for nullable cols),
    # which makes .equals() return False even when no values changed.
    try:
        edited_aligned = edited_for_save.astype(_orig_for_cmp.dtypes.to_dict())
    except Exception:
        edited_aligned = edited_for_save
    _has_changes = not edited_aligned.equals(_orig_for_cmp)

    if _has_changes:
        if st.button("💾 Save Changes", key=f"save_reg_{acc_id}_{tab_key}", type="primary"):
            # Clear caches BEFORE calling execute_db_save — it calls st.rerun()
            # internally on success, so any code after it never executes.
            # Clear ALL period _orig_key entries for this account+tab so switching
            # periods after a save always shows fresh data, not a stale snapshot.
            _prefix = f"set_reg_{acc_id}_{tab_key}_"
            for _k in list(st.session_state.keys()):
                if _k.startswith(_prefix) and _k.endswith("_orig"):
                    st.session_state.pop(_k, None)
            for _k in ["df_accs", "register_df"]:
                st.session_state.pop(_k, None)
            # Call execute_db_save directly — save_changes() renders its own
            # st.button internally which conflicts with ours.
            with get_db() as _conn_save:
                execute_db_save(
                    _orig_for_cmp, edited_for_save,
                    "Transactions", "transactions_id",
                    current_acc_id=acc_id, conn=_conn_save
                )

    # ── Move transactions ─────────────────────────────────────────────────────
    st.write("---")
    st.subheader("🔀 Move Transactions to Another Account")
    st.caption("Tick ☑ on rows above, pick a target account, then click Move.")

    _selected_ids = edited_reg.loc[
        edited_reg.get("_selected", False) == True, "transactions_id"
    ].dropna().astype(int).tolist()

    _move_targets = {k: v for k, v in acc_options.items() if k != acc_id}
    _m1, _m2 = st.columns([3, 1])
    with _m1:
        _move_target_id = st.selectbox(
            "Move to account",
            options=list(_move_targets.keys()),
            format_func=lambda x: _move_targets[x],
            key=f"move_target_{acc_id}_{tab_key}"
        )
    with _m2:
        st.write("")
        st.write("")
        _move_btn = st.button(
            f"▶️ Move {len(_selected_ids)} transaction(s)",
            key=f"move_btn_{acc_id}_{tab_key}",
            type="primary",
            disabled=len(_selected_ids) == 0
        )

    if _move_btn and _selected_ids:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE Transactions SET Accounts_Id = %s WHERE Transactions_Id = ANY(%s)",
                (_move_target_id, _selected_ids)
            )
        update_accounts_balances(acc_id)
        update_accounts_balances(_move_target_id)
        st.success(
            f"✅ Moved {len(_selected_ids)} transaction(s) to "
            f"**{_move_targets[_move_target_id]}**."
        )
        for _k in ["df_accs", "register_df"]:
            st.session_state.pop(_k, None)
        st.rerun()

    # ── Splits section ────────────────────────────────────────────────────────
    st.write("---")
    st.subheader("🔍 Split Analysis")

    available_ids = df["transactions_id"].tolist()
    _splits_key_sel  = f"tx_selector_{acc_id}_{tab_key}"
    _splits_key_show = f"show_splits_{acc_id}_{tab_key}"
    _splits_key_cur  = f"current_tx_{acc_id}_{tab_key}"

    # If exactly one row is ticked, auto-select it for splits.
    # We must set the selectbox widget's own session_state key (not a proxy key)
    # so Streamlit picks up the value when rendering the selectbox.
    if len(_selected_ids) == 1:
        _auto_id = _selected_ids[0]
        if _auto_id in available_ids and st.session_state.get(_splits_key_cur) != _auto_id:
            st.session_state[_splits_key_cur]  = _auto_id
            st.session_state[_splits_key_show] = True
            # Override the selectbox widget state directly so index= is respected
            st.session_state[_splits_key_sel]  = _auto_id

    # Derive the selectbox index from session state (works even after auto-select)
    _cur_split_id = st.session_state.get(_splits_key_cur)
    default_ix = (available_ids.index(_cur_split_id) + 1
                  if _cur_split_id in available_ids else 0)

    selected_tx_id = st.selectbox(
        "Select Transaction ID for Splits:",
        [None] + available_ids,
        index=default_ix,
        key=_splits_key_sel
    )

    if selected_tx_id != st.session_state.get(_splits_key_cur):
        st.session_state[_splits_key_cur] = selected_tx_id
        st.session_state[_splits_key_show] = selected_tx_id is not None

    if st.session_state.get(_splits_key_show) and st.session_state.get(_splits_key_cur):
        tx_id = int(st.session_state[_splits_key_cur])
        st.write("---")
        st.write(f"### 📑 Edit Splits for ID: {tx_id}")

        with get_db() as conn:
            df_splits = pd.read_sql(
                "SELECT * FROM Splits WHERE Transactions_Id = %s",
                conn, params=(tx_id,)
            )

        editor_key = f"splits_ed_{tx_id}_{tab_key}"
        edited_splits = st.data_editor(
            df_splits,
            num_rows="dynamic",
            key=editor_key,
            use_container_width=True,
            column_config={
                "splits_id": st.column_config.NumberColumn("Split ID", disabled=True),
                "transactions_id": None,
                "categories_id": st.column_config.SelectboxColumn(
                    "Category", options=list(cat_options.keys()),
                    format_func=lambda x: cat_options.get(x, "Unknown"), width="large"
                ),
                "amount": st.column_config.NumberColumn("Amount", format="%,.2f", width="medium"),
                "memo": st.column_config.TextColumn("Memo", width="large"),
                "embedding": None,
            }
        )

        if not edited_splits.equals(df_splits):
            df_to_save = edited_splits.copy()
            df_to_save["transactions_id"] = tx_id
            with get_db() as _conn_sp:
                execute_db_save(
                    df_splits, df_to_save, "Splits", "splits_id", conn=_conn_sp
                )
            # execute_db_save shows success toast and calls st.rerun() internally

        total_split = float(edited_splits["amount"].sum())
        with get_db() as _conn_chk:
            _cur_chk = _conn_chk.cursor()
            _cur_chk.execute(
                "SELECT total_amount FROM Transactions WHERE Transactions_Id = %s", (tx_id,)
            )
            res = _cur_chk.fetchone()
        if res:
            expected_total = float(res[0])
            if abs(total_split - expected_total) > 0.01:
                st.warning(
                    f"⚠️ Sum of splits ({total_split:,.2f}) ≠ "
                    f"Transaction Total ({expected_total:,.2f})"
                )
            else:
                st.info(f"✅ Splits balance correctly ({total_split:,.2f})")


def get_or_create_payee_id(cur, payee_name, categories_id_default=None):
    """Lookup or insert a payee. Accepts a cursor; commit is handled by the caller."""
    if not payee_name:
        return None
    payee_name = payee_name.strip()
    if not payee_name:
        return None

    cur.execute("SELECT payees_id FROM Payees WHERE Payees_Name = %s", (payee_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    if categories_id_default is not None:
        cur.execute("INSERT INTO Payees (Payees_Name, Categories_Id_Default) VALUES (%s, %s) RETURNING Payees_Id", (payee_name, categories_id_default))
    else:
        cur.execute("INSERT INTO Payees (Payees_Name) VALUES (%s) RETURNING Payees_Id", (payee_name,))
    return cur.fetchone()[0]


def get_latest_fx_rate(cur, currencies_id_1, currencies_id_2, as_of_date=None):
    """Get the latest FX rate from base to target currency as of the given date."""
    currencies_id_1 = int(currencies_id_1)
    currencies_id_2 = int(currencies_id_2)
    if currencies_id_1 == currencies_id_2:
        return 1.0

    if as_of_date is not None:
        if isinstance(as_of_date, datetime):
            as_of_date = as_of_date.date()
        elif isinstance(as_of_date, pd.Timestamp):
            as_of_date = as_of_date.date()
        elif isinstance(as_of_date, str):
            as_of_date = datetime.fromisoformat(as_of_date).date()

    if as_of_date:
        cur.execute("""
            SELECT fx_rate FROM Historical_FX 
            WHERE currencies_id_1 = %s AND currencies_id_2 = %s AND date <= %s
            ORDER BY date DESC LIMIT 1
        """, (currencies_id_1, currencies_id_2, as_of_date))
    else:
        cur.execute("""
            SELECT fx_rate FROM Historical_FX 
            WHERE currencies_id_1 = %s AND currencies_id_2 = %s 
            ORDER BY date DESC LIMIT 1
        """, (currencies_id_1, currencies_id_2))
    row = cur.fetchone()
    if row:
        return float(row[0])
    
    # Try reverse rate
    if as_of_date:
        cur.execute("""
            SELECT 1.0 / fx_rate FROM Historical_FX 
            WHERE currencies_id_1 = %s AND currencies_id_2 = %s AND date <= %s
            ORDER BY date DESC LIMIT 1
        """, (currencies_id_2, currencies_id_1, as_of_date))
    else:
        cur.execute("""
            SELECT 1.0 / fx_rate FROM Historical_FX 
            WHERE currencies_id_1 = %s AND currencies_id_2 = %s 
            ORDER BY date DESC LIMIT 1
        """, (currencies_id_2, currencies_id_1))
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


def insert_bank_transaction(cur, accounts_id, tx_date, payees_id, description, total_amount, cleared=True, accounts_id_target=None, total_amount_target=None, transfers_id=None):
    if transfers_id is not None:
        cur.execute(
            """
            INSERT INTO Transactions (Accounts_Id, Date, Payees_Id, Description, Total_Amount, Cleared, Accounts_Id_Target, Total_Amount_Target, Transfers_Id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING Transactions_Id
            """,
            (accounts_id, tx_date, payees_id, description, total_amount, cleared, accounts_id_target, total_amount_target, transfers_id)
        )
    else:
        cur.execute(
            """
            INSERT INTO Transactions (Accounts_Id, Date, Payees_Id, Description, Total_Amount, Cleared, Accounts_Id_Target, Total_Amount_Target)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING Transactions_Id
            """,
            (accounts_id, tx_date, payees_id, description, total_amount, cleared, accounts_id_target, total_amount_target)
        )
    return cur.fetchone()[0]


def insert_transaction_splits(cur, bank_transaction_id, splits):
    for split in splits:
        cur.execute(
            """
            INSERT INTO Splits (Transactions_Id, Categories_Id, Amount, Memo)
            VALUES (%s, %s, %s, %s)
            """,
            (bank_transaction_id, split.get('categories_id'), split['amount'], split.get('memo'))
        )


def reset_transaction_form_state():
    if 'reset_counter' not in st.session_state:
        st.session_state.reset_counter = 0
    st.session_state.reset_counter += 1


def render_register():
    """Render the Register page."""
    st.title("📝 Account Transactions Register")
    
    # Initialize counters
    st.session_state.reset_counter = st.session_state.get('reset_counter', 0)
    st.session_state.balance_update_counter = st.session_state.get('balance_update_counter', 0)
    
    # Load data with caching
 #   if 'df_accs' not in st.session_state:
 #       st.session_state.df_accs = pd.read_sql("SELECT * FROM Accounts WHERE Is_Active = True ORDER BY Accounts_Name ASC", conn)
 #   df_accs = st.session_state.df_accs


    # 1. Add the Checkbox
    show_inactive = st.checkbox("Display Inactive Accounts", value=False)

    # 2. Check if the preference has changed to clear the cache
    if "last_show_inactive" not in st.session_state:
        st.session_state.last_show_inactive = show_inactive

    if st.session_state.last_show_inactive != show_inactive:
        if 'df_accs' in st.session_state:
            del st.session_state.df_accs
        st.session_state.last_show_inactive = show_inactive
        st.rerun()

    # 3. Load data with dynamic Query — use get_db() for cache misses only
    needs_db = (
        'df_accs' not in st.session_state
        or 'df_payees' not in st.session_state
        or 'df_payee_list' not in st.session_state
        or 'df_securities' not in st.session_state
        or 'df_cat_list' not in st.session_state
    )
    if needs_db:
        with get_db() as conn:
            if 'df_accs' not in st.session_state:
                if show_inactive:
                    query = "SELECT * FROM Accounts ORDER BY Accounts_Name ASC"
                else:
                    query = "SELECT * FROM Accounts WHERE Is_Active = True ORDER BY Accounts_Name ASC"
                st.session_state.df_accs = pd.read_sql(query, conn)

            if 'df_payees' not in st.session_state:
                st.session_state.df_payees = pd.read_sql(
                    "SELECT Payees_Id, Payees_Name, Categories_Id_Default FROM Payees", conn)

            if 'df_payee_list' not in st.session_state:
                st.session_state.df_payee_list = pd.read_sql(
                    "SELECT Payees_Id, Payees_Name FROM Payees", conn)

            if 'df_securities' not in st.session_state:
                st.session_state.df_securities = pd.read_sql(
                    "SELECT Securities_Id, Securities_Name FROM Securities", conn)

            if 'df_cat_list' not in st.session_state:
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
                st.session_state.df_cat_list = pd.read_sql(query_cat_hierarchy, conn)

    df_accs      = st.session_state.df_accs
    df_payees    = st.session_state.df_payees
    df_payee_list = st.session_state.df_payee_list
    df_securities = st.session_state.df_securities
    df_cat_list  = st.session_state.df_cat_list
    
    acc_options = {
        row['accounts_id']: f"{row['accounts_name']} ({row['accounts_balance']:,.2f})" 
        for _, row in df_accs.iterrows()
    }
    acc_ids_list = list(acc_options.keys())
    payee_options = df_payee_list.set_index('payees_id')['payees_name'].to_dict()
    payee_names = df_payee_list['payees_name'].tolist()
    cat_options = df_cat_list.set_index('categories_id')['full_path'].to_dict()
    sec_options = df_securities.set_index('securities_id')['securities_name'].to_dict()
    
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
    acc_balance = df_accs.loc[df_accs['accounts_id'] == acc_id, 'accounts_balance'].values[0]

    # Main transactions df
    if 'register_df' not in st.session_state or st.session_state.get('register_acc_id') != acc_id:
        with get_db() as conn:
            df = pd.read_sql(f"SELECT * FROM Transactions WHERE Accounts_Id = {acc_id} ORDER BY Date DESC", conn)
        st.session_state.register_df = df
        st.session_state.register_acc_id = acc_id
    else:
        df = st.session_state.register_df
    
    if acc_type not in ['Brokerage', 'Pension', 'Other Investment', 'Margin']:  #'Brokerage', 'Pension', 'Other Investment', 'Margin', 'Real Estate', 'Vehicle', 'Asset', 'Liability'
        
        st.subheader(f"Outstanding Balance: {acc_balance:,.2f}")

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

                # ── Payee selector OUTSIDE the form so selecting a payee
                # triggers an immediate rerun and the category list rebuilds.
                _payee_sk = f"tx_payee_select_{st.session_state.reset_counter}"
                _payee_txt_sk = f"tx_payee_text_{st.session_state.reset_counter}"
                if payee_names:
                    selected_payee = st.selectbox(
                        "Payee",
                        ["(new payee)"] + payee_names,
                        index=0,
                        help="Select an existing payee or choose New to type a payee name.",
                        key=_payee_sk
                    )
                    if selected_payee == "(new payee)":
                        payee_name = st.text_input(
                            "New Payee Name", value="",
                            help="Enter a new payee name.",
                            key=_payee_txt_sk
                        )
                    else:
                        payee_name = selected_payee
                else:
                    payee_name = st.text_input(
                        "Payee", value="",
                        help="Enter a payee name. Missing payees are created automatically.",
                        key=_payee_txt_sk
                    )

                # Resolve ranked category list as soon as payee is known
                default_cat = None
                payee_top_cats = []
                if payee_name and payee_name in payee_names:
                    payee_row = df_payees[df_payees['payees_name'] == payee_name]
                    if not payee_row.empty:
                        default_cat = payee_row['categories_id_default'].values[0]
                        _payee_id_val = int(payee_row['payees_id'].values[0])
                        _pcat_cache_key = f"payee_top_cats_{_payee_id_val}"
                        if _pcat_cache_key not in st.session_state:
                            with get_db() as _conn_cats:
                                _df_top = pd.read_sql("""
                                    SELECT s.Categories_Id, COUNT(*) AS cnt
                                    FROM Splits s
                                    JOIN Transactions t
                                      ON t.Transactions_Id = s.Transactions_Id
                                    WHERE t.Payees_Id = %s
                                      AND s.Categories_Id IS NOT NULL
                                    GROUP BY s.Categories_Id
                                    ORDER BY cnt DESC
                                """, _conn_cats, params=(_payee_id_val,))
                            st.session_state[_pcat_cache_key] = (
                                _df_top['categories_id'].tolist()
                            )
                        payee_top_cats = st.session_state[_pcat_cache_key]

                with st.form("tx_form_with_splits"):
                    c1, c2 = st.columns(2)
                    date = c1.date_input("Date", datetime.now().date(), key=f"tx_date_{st.session_state.reset_counter}", format="DD/MM/YYYY")
                    # Show the resolved payee name as read-only info inside the form
                    c2.text_input("Payee (selected above)", value=payee_name or "", disabled=True)

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
                        # Top-used categories for this payee first (★), then the rest
                        _all_cat_ids = list(cat_options.keys())
                        _remaining   = [c for c in _all_cat_ids if c not in payee_top_cats]
                        _ordered     = payee_top_cats + _remaining
                        cat_list     = [None] + _ordered

                        def _cat_label(x):
                            if x is None:
                                return "Select a category"
                            label = cat_options.get(x, "Unknown")
                            return f"★ {label}" if x in payee_top_cats else label

                        # Default: explicit payee default first, else most-used
                        default_index = 0
                        if default_cat and default_cat in cat_list:
                            default_index = cat_list.index(default_cat)
                        elif payee_top_cats:
                            default_index = 1  # first item after None is the top cat

                        transaction_category = st.selectbox(
                            "Category",
                            cat_list,
                            format_func=_cat_label,
                            index=default_index,
                            key=f"tx_transaction_category_{st.session_state.reset_counter}"
                        )
                    else:
                        st.info("Add one row per split. The sum of split amounts must equal the Total Amount.")
                        # Reorder category options: top-used for this payee first,
                        # then remaining. SelectboxColumn doesn't support format_func
                        # so we build a display-label dict and pass ordered label strings.
                        _all_cat_ids   = list(cat_options.keys())
                        _remaining_ids = [c for c in _all_cat_ids if c not in payee_top_cats]
                        _split_ordered_ids = payee_top_cats + _remaining_ids

                        # Map id → display label (★ for top-used)
                        _split_cat_labels = {
                            c: (f"★ {cat_options[c]}" if c in payee_top_cats else cat_options[c])
                            for c in _split_ordered_ids
                            if c in cat_options
                        }

                        df_new_splits = pd.DataFrame([{'categories_id': None, 'amount': 0.0, 'memo': ''}])
                        new_splits_data = st.data_editor(
                            df_new_splits,
                            num_rows="dynamic",
                            hide_index=True,
                            width='stretch',
                            key=f"new_splits_editor_{st.session_state.reset_counter}",
                            column_config={
                                "categories_id": st.column_config.SelectboxColumn(
                                    "Category",
                                    options=_split_ordered_ids,
                                    format_func=lambda x: _split_cat_labels.get(x, cat_options.get(x, "Unknown"))
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
                            next_occurrence = r1.date_input("Next occurrence date", date + timedelta(days=30), key=f"tx_next_occurrence_{st.session_state.reset_counter}", format="DD/MM/YYYY")
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
                                recurrence_end_date = st.date_input("End date", date + timedelta(days=365), key=f"tx_recurrence_end_date_{st.session_state.reset_counter}", format="DD/MM/YYYY")
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

                                with get_db() as conn:
                                    cur = conn.cursor()
                                    payee_id = get_or_create_payee_id(cur, payee_name.strip()) if payee_name else None

                                    # Set default category for new payee if single category transaction
                                    if payee_id and transaction_mode == "Single Category" and payee_name and payee_name not in payee_names:
                                        cur.execute("UPDATE Payees SET Categories_Id_Default = %s WHERE Payees_Id = %s", (transaction_category, payee_id))

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
                                # get_db().__exit__ commits here
                                update_accounts_balances(st.session_state["account_id_internal"])
                                st.session_state.balance_update_counter += 1
                                # Clear balance-dependent caches so the rerun
                                # picks up the updated account balances from the DB.
                                for _k in ['df_accs', 'register_df']:
                                    st.session_state.pop(_k, None)
                                # Also clear all register _orig_key snapshots for
                                # this account so every period reloads fresh data.
                                _acc_prefix_bank = f"set_reg_{acc_id}_bank_"
                                _acc_prefix_cash = f"set_reg_{acc_id}_cash_"
                                for _k in list(st.session_state.keys()):
                                    if (_k.startswith(_acc_prefix_bank) or
                                            _k.startswith(_acc_prefix_cash)) and                                             _k.endswith("_orig"):
                                        st.session_state.pop(_k, None)
                                st.success("Transaction and splits saved!")
                                reset_transaction_form_state()
                                st.rerun()
                        except ValueError:
                            pass
                        except Exception as e:
                            st.error(f"Error saving transaction: {e}")

            with t_transfer:
                st.info("Create a money transfer and mirror it on the target account.")

                recurring_transfer = st.checkbox("Recurring transfer", key=f"transfer_recurring_{st.session_state.reset_counter}")

                with st.form("tx_form_transfer"):
                    date = st.date_input("Date", datetime.now().date(), key=f"transfer_date_{st.session_state.reset_counter}", format="DD/MM/YYYY")
                    
                    direction = st.radio(
                        "Transfer direction",
                        ["Transfer out", "Transfer in"],
                        horizontal=True,
                        key=f"transfer_direction_{st.session_state.reset_counter}"
                    )

                    target_accounts = [aid for aid in acc_ids_list if aid != acc_id]
                    if not target_accounts:
                        st.warning("No other active accounts are available for transfers.")
                        accounts_id_target = None
                        source_curr = None
                        target_curr = None
                        fx_rate = 1.0
                    else:
                        accounts_id_target = st.selectbox(
                            "Target Account",
                            target_accounts,
                            format_func=lambda x: acc_options.get(x, "Unknown"),
                            key=f"transfer_target_account_{st.session_state.reset_counter}"
                        )
                        source_curr = int(df_accs.loc[df_accs['accounts_id'] == acc_id, 'currencies_id'].values[0])
                        target_curr = int(df_accs.loc[df_accs['accounts_id'] == accounts_id_target, 'currencies_id'].values[0])
                        with get_db() as _conn_fx:
                            _cur_fx = _conn_fx.cursor()
                            fx_rate = float(get_latest_fx_rate(_cur_fx, source_curr, target_curr, date))

                    t_amount = st.number_input("Transfer Amount", value=0.0, format="%.2f", key=f"transfer_amount_{st.session_state.reset_counter}")
                    default_total_amount_target = abs(t_amount) * fx_rate if t_amount != 0 else 0.0
                    t_total_amount_target = st.number_input(
                        "Target Amount",
                        value=default_total_amount_target,
                        format="%.2f",
                        help=f"Auto-calculated based on FX rate {fx_rate:.4f}. Edit if needed.",
                        key=f"transfer_total_amount_target_{st.session_state.reset_counter}"
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
                            next_transfer_date = r1.date_input("Next occurrence date", date + timedelta(days=30), key=f"transfer_next_date_{st.session_state.reset_counter}", format="DD/MM/YYYY")
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
                                transfer_end_date = st.date_input("End date", date + timedelta(days=365), key=f"transfer_end_date_{st.session_state.reset_counter}", format="DD/MM/YYYY")
                    else:
                        next_transfer_date = None
                        transfer_periodicity = None
                        transfer_installments = None
                        transfer_end_date = None

                    if st.form_submit_button("🔥 Save Transfer"):
                        try:
                            if t_amount <= 0:
                                st.error("Transfer amount must be greater than zero.")
                            elif not accounts_id_target:
                                st.error("Please select a target account for the transfer.")
                            else:
                                transfer_amount = abs(t_amount)
                                transfer_total_amount_target = abs(t_total_amount_target) if t_total_amount_target else transfer_amount
                                dates = build_recurrence_schedule(
                                    date,
                                    next_transfer_date,
                                    transfer_periodicity,
                                    installments=transfer_installments,
                                    end_date=transfer_end_date
                                )

                                with get_db() as conn:
                                    cur = conn.cursor()
                                    payee_id = get_or_create_payee_id(cur, transfer_payee_name.strip()) if transfer_payee_name else None

                                    for idx, tx_date in enumerate(dates):
                                        cur.execute("SELECT nextval('transfers_id_seq')")
                                        transfers_id = cur.fetchone()[0]

                                        transfer_label = transfer_desc or "Transfer"
                                        if len(dates) > 1:
                                            transfer_label = f"{transfer_label} ({idx + 1}/{len(dates)})"

                                        if direction == "Transfer out":
                                            source_account = acc_id
                                            destination_account = accounts_id_target
                                            source_amount = -transfer_amount
                                            destination_amount = transfer_total_amount_target
                                        else:
                                            source_account = accounts_id_target
                                            destination_account = acc_id
                                            source_amount = -transfer_total_amount_target
                                            destination_amount = transfer_amount

                                        source_tx_id = insert_bank_transaction(
                                            cur,
                                            source_account,
                                            tx_date,
                                            payee_id,
                                            transfer_label,
                                            source_amount,
                                            accounts_id_target=destination_account,
                                            total_amount_target=abs(destination_amount),
                                            transfers_id=transfers_id
                                        )
                                        insert_transaction_splits(
                                            cur,
                                            source_tx_id,
                                            [{
                                                'categories_id': None,
                                                'amount': source_amount,
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
                                            accounts_id_target=source_account,
                                            total_amount_target=abs(source_amount),
                                            transfers_id=transfers_id
                                        )
                                        insert_transaction_splits(
                                            cur,
                                            destination_tx_id,
                                            [{
                                                'categories_id': None,
                                                'amount': destination_amount,
                                                'memo': 'Transfer'
                                            }]
                                        )
                                # get_db().__exit__ commits here
                                update_accounts_balances(acc_id)
                                update_accounts_balances(accounts_id_target)
                                st.session_state.balance_update_counter += 1
                                # Clear balance-dependent caches so the rerun
                                # picks up the updated account balances from the DB.
                                for _k in ['df_accs', 'register_df']:
                                    st.session_state.pop(_k, None)
                                # Also clear all register _orig_key snapshots for
                                # this account so every period reloads fresh data.
                                _acc_prefix_bank = f"set_reg_{acc_id}_bank_"
                                _acc_prefix_cash = f"set_reg_{acc_id}_cash_"
                                for _k in list(st.session_state.keys()):
                                    if (_k.startswith(_acc_prefix_bank) or
                                            _k.startswith(_acc_prefix_cash)) and                                             _k.endswith("_orig"):
                                        st.session_state.pop(_k, None)
                                st.success("Transfer saved and mirrored successfully!")
                                reset_transaction_form_state()
                                st.rerun()
                        except Exception as e:
                            st.error(f"Error saving transfer: {e}")

        with t_view:
            _render_transaction_table(acc_id, payee_options, acc_options, cat_options, "bank")
    else:
        cash_view, tab_reg, tab_view_hold, tab_edit_hold = st.tabs(["👁️ Cash Transaction Register", "📓 Investment Register", "📊 Current Holdings", "✏️ Edit Holdings"])
        with cash_view:
            _render_transaction_table(acc_id, payee_options, acc_options, cat_options, "cash")

        with tab_reg:

            with get_db() as conn:
                df_inv = pd.read_sql(f"SELECT * FROM Investments WHERE Accounts_Id = {acc_id} ORDER BY Date DESC", conn)

            column_order = [
                "investments_id", 
                "accounts_id",        
                "date",               
                "securities_id",      
                "action", 
                "quantity", 
                "price_per_share", 
                "commission", 
                "total_amount", 
                "description",
                "embedding"
            ]
            df_inv = df_inv[[col for col in column_order if col in df_inv.columns]]

            edited_df = st.data_editor(
                df_inv, 
                num_rows="dynamic", 
                key="inv_reg",
                width="stretch", 
                column_config={
                    "investments_id": st.column_config.NumberColumn(
                        "Transaction ID", 
                        disabled=True
                    ),                
                #    "accounts_id": st.column_config.SelectboxColumn(
                #        "Account", 
                #        options=list(acc_options.keys()), 
                #        format_func=lambda x: acc_options.get(x, "Unknown"),
                #        disabled=True
                #    ),               
                    "accounts_id": None,  # Hiding the duplicate accounts_id column
                #    "securities_id": st.column_config.NumberColumn("Security ID"),
                    "date": st.column_config.DateColumn(
                        "Date", 
                        format="DD/MM/YYYY"
                    ),
                    "securities_id": st.column_config.SelectboxColumn(
                        "Security", 
                        options=list(sec_options.keys()), 
                        format_func=lambda x: sec_options.get(x, "Unknown"),
                        width="large"
                    ),                
                #    "action": st.column_config.TextColumn("Action"),
                    "action": st.column_config.SelectboxColumn(
                        "Action", 
                        options=['Buy', 'Sell', 'Dividend', 'Reinvest', 'Split', 'ShrIn', 'ShrOut', 'IntInc', 'CashIn', 'CashOut', 'Vest', 'Expire', 'Grant', 'Exercise', 'MiscExp', 'RtrnCap'],
                        required=True
                    ),                
                    "quantity": st.column_config.NumberColumn(
                        "Quantity", 
                        format="%,.8f",  # More decimals for cryptos
                    ),
                    "price_per_share": st.column_config.NumberColumn(
                        "Price", 
                        format="%,.4f", # Διαχωριστικό χιλιάδων και σύμβολο νομίσματος
                    ),
                    "commission": st.column_config.NumberColumn(
                        "Commission", 
                        format="%,.4f"
                    ),
                    "total_amount": st.column_config.NumberColumn(
                        "Total Amount", 
                        format="%,.4f"
                    ),
                    "description": st.column_config.TextColumn(
                        "Memo",
                        width="large"
                    ),
                    "embedding": None # Hiding the column embedding since it's huge and not needed for editing
                },
            #    hide_index=True # Optional: hides the numbering 0, 1, 2... at the left, but then not able to select a record to delete
            )

            if not edited_df.equals(df_inv):
                # 1. Indentidy new records (the ones not in the initial df_inv)
                # Usually new lines have NaN in ID or not included in the index of df_inv
                new_rows_mask = ~edited_df['investments_id'].isin(df_inv['investments_id'])

                # 2. We fill in accounts_id only for the new rows
                edited_df.loc[new_rows_mask, 'accounts_id'] = acc_id

                # 3. Ορισμός συνάρτησης υπολογισμού
                def calculate_total(row):
                    qty = float(row.get('quantity') or 0)
                    price = float(row.get('price_per_share') or 0)
                    comm = float(row.get('commission') or 0)
                    action = str(row.get('action')).strip()

                    if action == 'Buy':
                        return (qty * price) + comm
                    elif action == 'Sell':
                        return (qty * price) - comm
                    return row.get('total_amount')

                # 4. Εφαρμογή του υπολογισμού ΜΟΝΟ στις νέες γραμμές (new_rows_mask)
                if new_rows_mask.any():
                    edited_df.loc[new_rows_mask, 'total_amount'] = edited_df[new_rows_mask].apply(calculate_total, axis=1)

                # 5. Αποθήκευση και Rerun
                save_changes(df_inv, edited_df, "Investments", "investments_id")
                st.rerun()


        with tab_view_hold:
            with get_db() as conn:
                df_h = pd.read_sql(f"SELECT Holdings_Id, Accounts_Id, Securities_Id, Quantity, Simple_Avg_Price, Fifo_Avg_Price FROM Holdings WHERE Accounts_Id = {acc_id}", conn)
            
            # 1. Function to colorize the lines
            def highlight_rows(row):
                color = ''
                if row['quantity'] == 0:
                    color = 'color: blue;'
                elif row['quantity'] < 0:
                    color = 'color: red;'
                return [color] * len(row)

            # 2. Εφαρμογή του στυλ
            styled_df = df_h.style.apply(highlight_rows, axis=1).format({
                "quantity": "{:.8f}",
                "simple_avg_price": "{:.4f}",
                "fifo_avg_price": "{:.4f}"
            })

            # 3. Display the date (Caution: st.dataframe is not editable)
            st.dataframe(
                styled_df,
                width='stretch',
                hide_index=True,
                column_config={
                    "holdings_id": None,
                    "accounts_id": None,                
                    "securities_id": st.column_config.SelectboxColumn(
                        "Security",
                        options=list(sec_options.keys()),
                        format_func=lambda x: sec_options.get(x, "NO SECURITY"),
                        width="large"
                    ),
                    "quantity": st.column_config.NumberColumn("Quantity", format="%,.8f"),
                    "simple_avg_price": st.column_config.NumberColumn("Simple Avg Price", format="%,.4f"),
                    "fifo_avg_price": st.column_config.NumberColumn("FIFO Avg Price", format="%,.4f")
                }
            )

        with tab_edit_hold:
         #   st.subheader(f"Current Holdings: {selected_inv_acc['accounts_name']}")
            with get_db() as conn:
                df_h = pd.read_sql(f"SELECT Holdings_Id, Accounts_Id, Securities_Id, Quantity, Simple_Avg_Price, Fifo_Avg_Price FROM Holdings WHERE Accounts_Id = {acc_id}", conn)

            # Creation of a new column with Status icons
            def get_status_icon(q):
                if q == 0:
                    return "🔵" # Blue for zero
                elif q < 0:
                    return "🔴" # Red for negative (short)
                return "🟢"     # Green for positive

            # Addition of the Status column at the beginning of the DataFrame
            df_h.insert(0, "Status", df_h['quantity'].apply(get_status_icon))
            
            edited_h = st.data_editor(
                df_h, 
            #    key=f"inv_h_editor_{inv_acc_id}",
                key="inv_holdings",
                width="stretch",
                column_config={
                    "Status": st.column_config.TextColumn("Status", width="small", disabled=True), # Locked column for status icons
                    # Hiding the IDs by setting them to None
                    "holdings_id": None,
                    "accounts_id": None,                
                    "securities_id": st.column_config.SelectboxColumn(
                        "Security",
                        options=list(sec_options.keys()),
                        format_func=lambda x: sec_options.get(x, "NO SECURITY"),
                        width="large"
                    ),
                    # Format numbers
                    "quantity": st.column_config.NumberColumn("Quantity", format="%,.8f"),
                    "simple_avg_price": st.column_config.NumberColumn("Simple Avg Price", format="%,.4f"),
                    "fifo_avg_price": st.column_config.NumberColumn("FIFO Avg Price", format="%,.4f")
                },
                hide_index=True
            )

        #    save_changes(df_h, edited_h, "Holdings", "holdings_id")

            if not edited_h.equals(df_h):
                save_df = edited_h.drop(columns=["Status"])
                save_changes(df_h.drop(columns=["Status"]), save_df, "Holdings", "holdings_id")

        
        if st.button("🚀 Update Holdings"):
            with st.spinner("Processing..."):
                update_holdings()
                st.balloons()