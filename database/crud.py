import math
import pandas as pd
import streamlit as st
from psycopg2.extras import execute_values
from database.connection import get_connection

def _safe_val(val):
    """Convert a DataFrame cell value to a Python-native type safe for psycopg2."""
    if pd.isna(val):
        return None
    if hasattr(val, 'item'):
        val = val.item()
    # Treat sentinel strings as NULL so they never reach the database.
    if isinstance(val, str) and val.strip().lower() in ('none', 'n/a', 'na', ''):
        return None
    return val


def execute_db_save(df_original, df_edited, table_name, id_col, current_acc_id=None, conn=None):
    """Save changes from data editor to database."""
    if conn is None:
        conn = get_connection()
    cur = conn.cursor()
    try:
        original_ids = set(df_original[id_col].dropna().unique())
        edited_ids = set(df_edited[id_col].dropna().unique())
        ids_to_delete = [int(x) for x in (original_ids - edited_ids) if pd.notna(x)]

        # ── DELETE cascades ───────────────────────────────────────────────────
        if ids_to_delete and table_name == "Transactions":
            for deleted_id in ids_to_delete:
                deleted_row = df_original[df_original[id_col] == deleted_id].iloc[0]
                transfers_id = deleted_row.get('transfers_id')
                if pd.notna(transfers_id) and transfers_id:
                    cur.execute("""
                        SELECT transactions_id FROM Transactions
                        WHERE transfers_id = %s AND transactions_id != %s
                    """, (int(transfers_id), int(deleted_id)))
                    mirrored = cur.fetchone()
                    if mirrored:
                        cur.execute("DELETE FROM Splits WHERE transactions_id = %s", (mirrored[0],))
                        cur.execute("DELETE FROM Transactions WHERE transactions_id = %s", (mirrored[0],))
            cur.execute("DELETE FROM Splits WHERE transactions_id IN %s", (tuple(ids_to_delete),))
            cur.execute(f"DELETE FROM {table_name} WHERE {id_col} IN %s", (tuple(ids_to_delete),))

        elif ids_to_delete and table_name == "Investments":
            for deleted_id in ids_to_delete:
                deleted_row = df_original[df_original[id_col] == deleted_id].iloc[0]
                linked_tx_id = deleted_row.get('transactions_id')
                if pd.notna(linked_tx_id) and linked_tx_id:
                    # Must NULL-out the FK on the Investments row BEFORE deleting the
                    # Transactions row — otherwise the FK constraint fires.
                    cur.execute(
                        "UPDATE Investments SET Transactions_Id = NULL WHERE Investments_Id = %s",
                        (int(deleted_id),)
                    )
                    cur.execute("DELETE FROM Splits WHERE transactions_id = %s", (int(linked_tx_id),))
                    cur.execute("DELETE FROM Transactions WHERE transactions_id = %s", (int(linked_tx_id),))
            cur.execute(f"DELETE FROM {table_name} WHERE {id_col} IN %s", (tuple(ids_to_delete),))

        elif ids_to_delete:
            if table_name == "Transactions":
                cur.execute("DELETE FROM Splits WHERE transactions_id IN %s", (tuple(ids_to_delete),))
            cur.execute(f"DELETE FROM {table_name} WHERE {id_col} IN %s", (tuple(ids_to_delete),))

        df_new = df_edited[df_edited[id_col].isna() | df_edited[id_col].isnull()].copy()
        df_updates = df_edited[df_edited[id_col].notna()].copy()
        _inv_acc_ids_to_refresh: set = set()   # populated by the Investments cascade below

        # ── UPDATE rows ───────────────────────────────────────────────────────
        if not df_updates.empty:
            cols = [c for c in df_updates.columns.tolist() if c != id_col]
            for _, row in df_updates.iterrows():
                set_clause = ", ".join([f"{c} = %s" for c in cols])
                sql_upd = f"UPDATE {table_name} SET {set_clause} WHERE {id_col} = %s"
                vals = [_safe_val(row[c]) for c in cols]
                vals.append(int(row[id_col]))
                cur.execute(sql_upd, tuple(vals))

            # ── UPDATE cascades ────────────────────────────────────────────────
            if table_name == "Investments":
                # Sync date and total_amount to every linked cash transaction.
                # No change-detection guard — Decimal/float comparisons across
                # psycopg2 and the data editor are unreliable.  Unconditional
                # UPDATE by PK is cheap and safe.
                #
                # Convention (mirrors the investment creation logic):
                #   Investments.total_amount        → in investment-account currency
                #   Transactions.total_amount       → in cash-account currency
                #                                     (= inv total when same currency,
                #                                      = inv total × FX rate otherwise)
                #   Transactions.total_amount_target → abs(inv total) in investment
                #                                      account currency (always)
                _inv_acc_ids_to_refresh = set()
                for _, row in df_updates.iterrows():
                    linked_tx_id = _safe_val(row.get('transactions_id'))
                    if not linked_tx_id:
                        continue
                    inv_acc_id = _safe_val(row.get('accounts_id'))
                    new_date   = _safe_val(row.get('date'))
                    new_total  = _safe_val(row.get('total_amount'))
                    action     = str(row.get('action', '') or '')
                    cash_out   = action in {'Buy', 'MiscExp'}

                    if new_total is None:
                        continue   # nothing to sync if amount is unknown

                    abs_inv_total = abs(float(new_total))

                    # ── Resolve currencies ────────────────────────────────────
                    cur.execute("""
                        SELECT inv_acc.currencies_id, cash_acc.currencies_id,
                               tx.accounts_id
                        FROM   Transactions tx
                        JOIN   Accounts cash_acc ON cash_acc.accounts_id = tx.accounts_id
                        JOIN   Accounts inv_acc  ON inv_acc.accounts_id  = %s
                        WHERE  tx.transactions_id = %s
                    """, (inv_acc_id, int(linked_tx_id)))
                    curr_row = cur.fetchone()
                    if not curr_row:
                        continue
                    inv_curr_id, cash_curr_id, cash_acc_id = curr_row

                    # ── FX conversion (only when currencies differ) ───────────
                    if inv_curr_id != cash_curr_id:
                        # Try direct rate: inv_curr → cash_curr
                        cur.execute("""
                            SELECT fx_rate FROM Historical_FX
                            WHERE  currencies_id_1 = %s
                              AND  currencies_id_2 = %s
                              AND  date <= COALESCE(%s, CURRENT_DATE)
                            ORDER  BY date DESC LIMIT 1
                        """, (inv_curr_id, cash_curr_id, new_date))
                        fx_row = cur.fetchone()
                        if fx_row:
                            fx_rate = float(fx_row[0])
                        else:
                            # Try reverse rate
                            cur.execute("""
                                SELECT 1.0 / fx_rate FROM Historical_FX
                                WHERE  currencies_id_1 = %s
                                  AND  currencies_id_2 = %s
                                  AND  date <= COALESCE(%s, CURRENT_DATE)
                                ORDER  BY date DESC LIMIT 1
                            """, (cash_curr_id, inv_curr_id, new_date))
                            fx_row = cur.fetchone()
                            fx_rate = float(fx_row[0]) if fx_row else 1.0
                        cash_amount = abs_inv_total * fx_rate
                    else:
                        cash_amount = abs_inv_total

                    signed_cash = -cash_amount if cash_out else cash_amount

                    cur.execute(
                        """UPDATE Transactions
                              SET date                = COALESCE(%s, date),
                                  total_amount        = %s,
                                  total_amount_target = %s
                           WHERE transactions_id = %s
                           RETURNING accounts_id""",
                        (new_date, signed_cash, abs_inv_total, int(linked_tx_id)),
                    )
                    result = cur.fetchone()
                    if result:
                        _inv_acc_ids_to_refresh.add(result[0])

            elif table_name == "Transactions":
                # Sync changes to the mirrored transfer transaction
                for _, row in df_updates.iterrows():
                    transfers_id = _safe_val(row.get('transfers_id'))
                    if not transfers_id:
                        continue
                    tx_id = int(row[id_col])
                    orig_rows = df_original[df_original[id_col] == tx_id]
                    if orig_rows.empty:
                        continue
                    orig = orig_rows.iloc[0]

                    new_date         = _safe_val(row.get('date'))
                    orig_date        = _safe_val(orig.get('date'))
                    new_acc_id       = _safe_val(row.get('accounts_id'))
                    orig_acc_id      = _safe_val(orig.get('accounts_id'))
                    new_total        = _safe_val(row.get('total_amount'))
                    orig_total       = _safe_val(orig.get('total_amount'))
                    new_total_target = _safe_val(row.get('total_amount_target'))
                    orig_total_target= _safe_val(orig.get('total_amount_target'))

                    if new_date != orig_date:
                        cur.execute(
                            """UPDATE Transactions SET date = %s
                               WHERE transfers_id = %s AND transactions_id != %s""",
                            (new_date, int(transfers_id), tx_id),
                        )
                    # When this transaction's account changes, update the mirror's
                    # Accounts_Id_Target so it still points to the correct account
                    if new_acc_id and new_acc_id != orig_acc_id:
                        cur.execute(
                            """UPDATE Transactions SET accounts_id_target = %s
                               WHERE transfers_id = %s AND transactions_id != %s""",
                            (int(new_acc_id), int(transfers_id), tx_id),
                        )
                    # Sync amount changes to the mirror.
                    # Convention (from transfer creation):
                    #   source.total_amount        = signed outflow (-) or inflow (+)
                    #   source.total_amount_target = abs amount arriving at the mirror account
                    #   mirror.total_amount        = opposite-signed value of source.total_amount_target
                    #   mirror.total_amount_target = abs(source.total_amount)
                    if new_total != orig_total or new_total_target != orig_total_target:
                        mirror_total = None
                        mirror_total_target = None
                        if new_total_target is not None and new_total is not None:
                            # Mirror receives the target amount with the opposite sign
                            mirror_total = math.copysign(abs(float(new_total_target)), -float(new_total))
                            mirror_total_target = abs(float(new_total))
                        elif new_total is not None:
                            mirror_total_target = abs(float(new_total))
                        elif new_total_target is not None and orig_total is not None:
                            mirror_total = math.copysign(abs(float(new_total_target)), -float(orig_total))
                        cur.execute(
                            """UPDATE Transactions
                                  SET total_amount        = COALESCE(%s, total_amount),
                                      total_amount_target = COALESCE(%s, total_amount_target)
                               WHERE transfers_id = %s AND transactions_id != %s""",
                            (mirror_total, mirror_total_target, int(transfers_id), tx_id),
                        )

        # ── INSERT new rows ───────────────────────────────────────────────────
        if not df_new.empty:
            # Ensure the sequence is at least at max(existing id) to avoid drift
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table_name}', '{id_col}'), "
                f"COALESCE(MAX({id_col}), 0)) FROM {table_name}"
            )
            cols_new = [c for c in df_new.columns.tolist() if c != id_col]
            for _, row in df_new.iterrows():
                placeholders = ", ".join(["%s"] * len(cols_new))
                sql_ins = f"INSERT INTO {table_name} ({', '.join(cols_new)}) VALUES ({placeholders})"
                vals = [_safe_val(row[c]) for c in cols_new]
                cur.execute(sql_ins, tuple(vals))

        conn.commit()

        if table_name == "Transactions" and current_acc_id:
            update_accounts_balances(current_acc_id)
            st.session_state.balance_update_counter = st.session_state.get('balance_update_counter', 0) + 1

        if table_name == "Investments" and _inv_acc_ids_to_refresh:
            # Recalculate balances for all cash accounts whose linked transactions
            # were just updated so the account summary stays in sync.
            for _aid in _inv_acc_ids_to_refresh:
                update_accounts_balances(_aid)
            # Clear the transaction-register session-state cache for every affected
            # cash account so the next render re-fetches fresh data from the DB.
            # Cache keys follow the pattern: set_reg_{acc_id}_{tab_key}_{hash}_orig
            _cash_prefix_set = {f"set_reg_{_aid}_" for _aid in _inv_acc_ids_to_refresh}
            for _k in list(st.session_state.keys()):
                if any(_k.startswith(pfx) for pfx in _cash_prefix_set) and _k.endswith("_orig"):
                    st.session_state.pop(_k, None)

        st.success(f"Saved: {len(df_updates)} updates, {len(df_new)} new, {len(ids_to_delete)} deletions")
        st.rerun()

    except Exception as e:
        conn.rollback()
        st.error(f"Error: {e}")
        st.exception(e)

def save_changes(df_original, df_edited, table_name, id_col, current_acc_id=None, conn=None):
    """Save changes from data editor to database."""
    if st.button(f"💾 Save {table_name}"):
        # execute_db_save handles its own st.success / st.error / st.rerun internally.
        execute_db_save(df_original, df_edited, table_name, id_col, current_acc_id, conn)

def save_changes_no_serial(df_original, df_edited, table_name, id_col):
    """Save changes for tables without serial ID."""
    if st.button(f"💾 Save {table_name}"):
        conn = get_connection()
        cur = conn.cursor()
        try:
            if table_name == "Historical_FX":
                def get_keys(df):
                    return set(df.apply(lambda r: f"{int(r['currencies_id_1'])}|{int(r['currencies_id_2'])}|{r['date']}", axis=1))
                
                original_keys = get_keys(df_original)
                edited_keys = get_keys(df_edited)
                keys_to_delete = original_keys - edited_keys
                
                for key in keys_to_delete:
                    b_id, t_id, f_date = key.split('|')
                    cur.execute(f"""
                        DELETE FROM {table_name} 
                        WHERE currencies_id_1 = %s AND currencies_id_2 = %s AND date = %s
                    """, (int(b_id), int(t_id), f_date))
            
            cols = df_edited.columns.tolist()
            data_tuples = [tuple(None if pd.isna(v) else v for v in row) for row in df_edited.values]
            
            if table_name == "Historical_FX":
                conflict_target = "currencies_id_1, currencies_id_2, date"
                update_cols = ["fx_rate"]
            else:
                conflict_target = id_col
                update_cols = [c for c in cols if c != id_col]
            
            update_stmt = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s ON CONFLICT ({conflict_target}) DO UPDATE SET {update_stmt}"
            
            execute_values(cur, sql, data_tuples)
            conn.commit()
            st.success("Changes saved!")
            st.rerun()
            
        except Exception as e:
            conn.rollback()
            st.error(f"Error: {e}")
        finally:
            conn.close()

def save_changes_mid(df_edited, table_name, id_cols, filter_col=None, filter_val=None):
    """Save changes for tables with composite keys."""
    if st.button(f"💾 Save {table_name}"):
        conn = get_connection()
        cur = conn.cursor()
        try:
            if filter_col and filter_val:
                current_dates = df_edited['date'].dropna().tolist()
                if current_dates:
                    cur.execute(f"DELETE FROM {table_name} WHERE {filter_col} = %s AND date NOT IN %s",
                                (filter_val, tuple(current_dates)))
                else:
                    cur.execute(f"DELETE FROM {table_name} WHERE {filter_col} = %s", (filter_val,))
            
            for _, row in df_edited.iterrows():
                if filter_col and filter_val:
                    row[filter_col] = filter_val
                
                cols = row.index.tolist()
                vals = [None if pd.isna(v) else v for v in row.values]
                placeholders = ", ".join(["%s"] * len(cols))
                update_cols = [c for c in cols if c not in id_cols]
                update_stmt = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                conflict_target = ", ".join(id_cols)
                
                sql = f"""
                    INSERT INTO {table_name} ({', '.join(cols)}) 
                    VALUES ({placeholders}) 
                    ON CONFLICT ({conflict_target}) 
                    DO UPDATE SET {update_stmt}
                """
                cur.execute(sql, vals)
            
            conn.commit()
            st.success("Changes saved!")
            st.rerun()
        except Exception as e:
            conn.rollback()
            st.error(f"Error: {e}")
        finally:
            conn.close()

def update_accounts_balances(target_acc_id=None):
    """Update account balances based on transactions."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        if target_acc_id:
            sql = """
                UPDATE Accounts a
                SET Accounts_Balance = COALESCE((
                    SELECT SUM(Total_Amount) 
                    FROM Transactions t 
                    WHERE t.Accounts_Id = a.Accounts_Id
                ), 0)
                WHERE a.Accounts_Id = %s;
            """
            cur.execute(sql, (int(target_acc_id),))
        else:
            sql = """
                UPDATE Accounts a
                SET Accounts_Balance = COALESCE((
                    SELECT SUM(Total_Amount) 
                    FROM Transactions t 
                    WHERE t.Accounts_Id = a.Accounts_Id
                ), 0)
                WHERE a.Accounts_Type NOT IN ('Pension', 'Brokerage', 'Other Investment', 'Margin');
            """
            cur.execute(sql)
        conn.commit()
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()

def update_pension_balances():
    """Update pension account balances."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE Accounts a
            SET Accounts_Balance = COALESCE((
                SELECT  
                    SUM(CASE WHEN Action IN ('CashIn', 'IntInc') THEN Total_Amount 
                             WHEN Action IN ('CashOut') THEN -Total_Amount 
                             ELSE 0 END)
                FROM Investments t 
                WHERE t.Accounts_Id = a.Accounts_Id
            ), 0)
            WHERE a.Accounts_Type IN ('Pension');
        """)
        conn.commit()
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()

def update_investment_balances():
    """Update investment account balances."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE Accounts a
            SET Accounts_Balance = COALESCE((
                SELECT  
                    SUM(CASE WHEN Action IN ('Dividend', 'CashIn', 'IntInc', 'Sell') THEN Total_Amount 
                             WHEN Action IN ('CashOut', 'Buy') THEN -Total_Amount 
                             ELSE 0 END)
                FROM Investments t 
                WHERE t.Accounts_Id = a.Accounts_Id
            ), 0) +  COALESCE((
                    SELECT SUM(Total_Amount) 
                    FROM Transactions t 
                    WHERE t.Accounts_Id = a.Accounts_Id
                ), 0)
            WHERE a.Accounts_Type IN ('Brokerage', 'Other Investment', 'Margin');
        """)
        conn.commit()
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()

def update_holdings():
    """Update holdings based on investment transactions."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM Holdings
            WHERE NOT EXISTS (
                SELECT 1
                FROM Investments i
                WHERE i.Accounts_Id  = Holdings.Accounts_Id
                  AND i.Securities_Id = Holdings.Securities_Id
                  AND i.Securities_Id IS NOT NULL
            );

            WITH TransactionFlow AS (
                SELECT 
                    Accounts_Id, 
                    Securities_Id,
                    Date,
                    Investments_Id,
                    Action,
                    Quantity,
                    Price_Per_Share,
                    -- 1. Historical Average (Simple Average) of all purchases up to today
                    AVG(Price_Per_Share) FILTER (WHERE Action IN ('Buy', 'Reinvest', 'ShrIn')) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id) as simple_avg_cost,
                    -- Cumulative Purchases & Sales
                    SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity ELSE 0 END) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id ORDER BY Date, Investments_Id) as running_buys,
                    SUM(CASE WHEN Action IN ('Sell', 'ShrOut') THEN Quantity ELSE 0 END) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id ORDER BY Date, Investments_Id) as running_sells,
                    -- Total Amounts
                    SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity ELSE 0 END) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id) as total_buys,
                    SUM(CASE WHEN Action IN ('Sell', 'ShrOut') THEN Quantity ELSE 0 END) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id) as total_sells
                FROM Investments
                WHERE Action IN ('Buy', 'Reinvest', 'ShrIn', 'Sell', 'ShrOut')
            ),
            FIFO_Positions AS (
                SELECT 
                    Accounts_Id, 
                    Securities_Id,
                    simple_avg_cost, -- Transfer of the price to the next level
                    CASE 
                        WHEN total_buys >= total_sells THEN 
                            CASE 
                                WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN
                                    CASE 
                                        WHEN running_buys <= total_sells THEN 0
                                        WHEN running_buys - Quantity < total_sells THEN running_buys - total_sells
                                        ELSE Quantity 
                                    END
                                ELSE 0 
                            END
                        ELSE -- SHORT CASE
                            CASE 
                                WHEN Action IN ('Sell', 'ShrOut') THEN
                                    CASE 
                                        WHEN running_sells <= total_buys THEN 0
                                        WHEN running_sells - Quantity < total_buys THEN -(running_sells - total_buys)
                                        ELSE -Quantity 
                                    END
                                ELSE 0 
                            END
                    END as remaining_qty,
                    Price_Per_Share
                FROM TransactionFlow
            )
            INSERT INTO Holdings (Accounts_Id, Securities_Id, Quantity, Simple_Avg_Price, Fifo_Avg_Price)
            SELECT 
                Accounts_Id, 
                Securities_Id, 
                SUM(remaining_qty) as Current_Quantity,
                -- 2. Simple Average Price (historical average of all purchases) - This is the same as simple_avg_cost but we take the max to get the final value for the holding
                MAX(simple_avg_cost) as Simple_Avg_Price,
                -- 3. FIFO Average Price (only for open positions)
                CASE 
                    WHEN ABS(SUM(remaining_qty)) > 0 
                    THEN SUM(ABS(remaining_qty) * Price_Per_Share) / SUM(ABS(remaining_qty)) 
                    ELSE 0 
                END as FIFO_Avg_Price
            FROM FIFO_Positions
            GROUP BY Accounts_Id, Securities_Id
        --    HAVING SUM(remaining_qty) <> 0        -- Excluding closed positions has impact on the Total P&L calculation, so we keep them with zero quantity
            ON CONFLICT (Accounts_Id, Securities_Id) 
            DO UPDATE SET 
                Quantity = EXCLUDED.Quantity,
                Simple_Avg_Price = EXCLUDED.Simple_Avg_Price, 
                Fifo_Avg_Price = EXCLUDED.Fifo_Avg_Price, -- Update and the new column
                Last_Update = CURRENT_TIMESTAMP;

            -- Remove zero-quantity Holdings rows that have no remaining investments
            -- (e.g. after all transactions for a security have been moved or deleted).
            -- Holdings with Quantity=0 that still have Investments rows are kept
            -- intentionally for closed-position P&L history.
            DELETE FROM Holdings
            WHERE ABS(Quantity) = 0
              AND NOT EXISTS (
                SELECT 1
                FROM Investments i
                WHERE i.Accounts_Id   = Holdings.Accounts_Id
                  AND i.Securities_Id  = Holdings.Securities_Id
                  AND i.Securities_Id IS NOT NULL
              );
        """)
        conn.commit()
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()
        

def update_payee_default_category():
    """Update payee default category based on usage and when not defined."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE payees p
            SET categories_id_default = top_cat.Categories_Id
            FROM (
                SELECT DISTINCT ON (t.Payees_Id)
                    t.Payees_Id, 
                    s.Categories_Id
                FROM transactions t
                JOIN splits s ON t.transactions_id = s.transactions_id 
                WHERE s.Categories_Id IS NOT NULL 
                GROUP BY t.Payees_Id, s.Categories_Id 
                ORDER BY t.Payees_Id, COUNT(*) DESC
            ) AS top_cat
            WHERE p.payees_id = top_cat.Payees_Id
              AND p.categories_id_default IS NULL;
        """)
        conn.commit()
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()

def update_db_stats():
    """Update database statistics."""
    conn = get_connection()
    try:
        # Χρήση επιπέδου απομόνωσης που επιτρέπει το ANALYZE αν χρειαστεί
        old_isolation_level = conn.isolation_level
        conn.set_isolation_level(0) # autocommit mode
        
        with conn.cursor() as cursor:
            cursor.execute("ANALYZE;")
        
        conn.set_isolation_level(old_isolation_level)
        print("Database statistics updated successfully.")
    except Exception as e:
        print(f"Error updating stats: {e}")

# Καλέστε το στο τέλος του import process:
# update_db_stats()


def delete_historical_prices(rows: list):
    """Delete specific Historical_Prices rows by (securities_id, date) pairs."""
    if not rows:
        return 0
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(
            "DELETE FROM Historical_Prices WHERE Securities_Id = %s AND Date = %s",
            [(int(r['securities_id']), r['date']) for r in rows],
        )
        deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        cur.close()
        conn.close()


def insert_prices_from_transactions(rows: list) -> int:
    """Insert Historical_Prices rows derived from investment transaction prices.

    Each element of *rows* must have 'securities_id', 'date', 'price'.
    Existing rows are left untouched (ON CONFLICT DO NOTHING).
    Returns the number of rows actually inserted.
    """
    if not rows:
        return 0
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO Historical_Prices (Securities_Id, Date, Close)
            VALUES (%s, %s, %s)
            ON CONFLICT (Securities_Id, Date) DO NOTHING
            """,
            [(int(r['securities_id']), r['date'], float(r['price'])) for r in rows],
        )
        inserted = cur.rowcount
        conn.commit()
        return inserted
    finally:
        cur.close()
        conn.close()


def normalize_investment_prices(investments_ids: list) -> int:
    """Normalize Quantity and Price_Per_Share for investment transactions using Historical_Prices.

    Two-phase update so positions close correctly:

    Phase 1 — Buy / Reinvest / ShrIn:
        Price_Per_Share = Historical_Prices.Close on that date
        Quantity        = Total_Amount / Close

    Phase 2 — Sell / ShrOut:
        Quantity is distributed proportionally from the total normalised buy quantity
        for the same (account, security), so sum(sell_qty) = sum(buy_qty) and the
        position closes.  Price_Per_Share is back-computed as Total_Amount / Quantity
        (effective realised price, which may differ from the hist close).

    Total_Amount is never modified, so P&L and account balances are preserved.
    Returns the total number of rows updated (buys + sells).
    """
    if not investments_ids:
        return 0
    conn = get_connection()
    cur = conn.cursor()
    try:
        # ── Phase 1: normalise buy-side rows ─────────────────────────────────
        cur.execute(
            """
            UPDATE Investments i
               SET Price_Per_Share = hp.Close,
                   Quantity        = ROUND((i.Total_Amount / NULLIF(hp.Close, 0))::numeric, 6)
              FROM Historical_Prices hp
             WHERE hp.Securities_Id = i.Securities_Id
               AND hp.Date          = i.Date
               AND i.Action IN ('Buy', 'Reinvest', 'ShrIn')
               AND i.Investments_Id = ANY(%s)
            """,
            (investments_ids,),
        )
        buy_updated = cur.rowcount

        # ── Phase 2: normalise sell-side rows ─────────────────────────────────
        # Sell qty is distributed proportionally from the total normalised buy qty
        # for the same (account, security), so sum(sell_qty) = sum(buy_qty) and the
        # position closes correctly.
        #
        # IMPORTANT: use ABS(Total_Amount) throughout so that sells whose
        # Total_Amount is negative (e.g. a losing CFD trade) still produce a
        # positive quantity.  Price is back-computed as ABS(Total_Amount) / Quantity
        # so that Quantity × Price_Per_Share = ABS(Total_Amount).
        cur.execute(
            """
            WITH buy_totals AS (
                -- Sum of already-normalised buy quantities for each (account, security)
                -- that has at least one sell being normalised now.
                SELECT i.Accounts_Id, i.Securities_Id,
                       SUM(i.Quantity) AS total_buy_qty
                FROM Investments i
                WHERE i.Action IN ('Buy', 'Reinvest', 'ShrIn')
                  AND EXISTS (
                      SELECT 1 FROM Investments s2
                      WHERE s2.Investments_Id = ANY(%s)
                        AND s2.Action IN ('Sell', 'ShrOut')
                        AND s2.Accounts_Id   = i.Accounts_Id
                        AND s2.Securities_Id = i.Securities_Id
                  )
                GROUP BY i.Accounts_Id, i.Securities_Id
            ),
            sell_totals AS (
                -- Use ABS so mixed-sign Total_Amounts (losing trades) don't cancel
                -- each other out or invert the proportional weight.
                SELECT Accounts_Id, Securities_Id,
                       SUM(ABS(Total_Amount)) AS total_sell_amt_abs
                FROM Investments
                WHERE Action IN ('Sell', 'ShrOut')
                  AND Investments_Id = ANY(%s)
                GROUP BY Accounts_Id, Securities_Id
            )
            UPDATE Investments i
               SET Quantity        = ROUND(
                       (bt.total_buy_qty
                        * (ABS(i.Total_Amount) / NULLIF(st.total_sell_amt_abs, 0)))::numeric,
                       6),
                   Price_Per_Share = ROUND(
                       (ABS(i.Total_Amount)
                        / NULLIF(bt.total_buy_qty
                                 * (ABS(i.Total_Amount) / NULLIF(st.total_sell_amt_abs, 0)),
                                 0))::numeric,
                       4)
              FROM buy_totals bt
              JOIN sell_totals st
                   ON  st.Accounts_Id   = bt.Accounts_Id
                   AND st.Securities_Id = bt.Securities_Id
             WHERE i.Accounts_Id   = bt.Accounts_Id
               AND i.Securities_Id = bt.Securities_Id
               AND i.Action IN ('Sell', 'ShrOut')
               AND i.Investments_Id = ANY(%s)
            """,
            (investments_ids, investments_ids, investments_ids),
        )
        sell_updated = cur.rowcount

        conn.commit()

        # Refresh Holdings so the portfolio view is immediately consistent.
        update_holdings()

        return buy_updated + sell_updated
    finally:
        cur.close()
        conn.close()


def save_nwr_account_selection(account_ids: list, settings_key: str = 'nwr_account_ids'):
    """Persist an account selection to app_settings under *settings_key*."""
    import json
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)
        """)
        cur.execute("""
            INSERT INTO app_settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (settings_key, json.dumps(account_ids)))
        conn.commit()
    finally:
        cur.close()
        conn.close()