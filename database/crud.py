import pandas as pd
import streamlit as st
from psycopg2.extras import execute_values
from database.connection import get_connection

def save_changes(df_original, df_edited, table_name, id_col, current_acc_id=None, conn=None):
    """Save changes from data editor to database."""
    if st.button(f"💾 Save {table_name}"):
        if conn is None:
            conn = get_connection()
        cur = conn.cursor()
        try:
            original_ids = set(df_original[id_col].dropna().unique())
            edited_ids = set(df_edited[id_col].dropna().unique())
            ids_to_delete = [int(x) for x in (original_ids - edited_ids) if pd.notna(x)]
            
            # If deleting Bank_Transactions, also handle mirrored transfers
            if ids_to_delete and table_name == "Bank_Transactions":
                for deleted_id in ids_to_delete:
                    deleted_row = df_original[df_original[id_col] == deleted_id].iloc[0]
                    if pd.notna(deleted_row.get('transfer_id')) and deleted_row['transfer_id']:
                        # It's a transfer, find and delete the mirrored transaction using Transfer_Id
                        transfer_id = deleted_row['transfer_id']
                        # Find the other transaction in this transfer pair
                        cur.execute("""
                            SELECT bank_transactions_id FROM Bank_Transactions 
                            WHERE transfer_id = %s AND bank_transactions_id != %s
                        """, (int(transfer_id), int(deleted_id)))
                        mirrored_row = cur.fetchone()
                        if mirrored_row:
                            mirrored_id = mirrored_row[0]
                            cur.execute("DELETE FROM Bank_Transaction_Splits WHERE bank_transactions_id = %s", (mirrored_id,))
                            cur.execute("DELETE FROM Bank_Transactions WHERE bank_transactions_id = %s", (mirrored_id,))
                
                if ids_to_delete:
                    cur.execute("DELETE FROM Bank_Transaction_Splits WHERE bank_transactions_id IN %s", (tuple(ids_to_delete),))
                    cur.execute(f"DELETE FROM {table_name} WHERE {id_col} IN %s", (tuple(ids_to_delete),))
            elif ids_to_delete:
                if table_name == "Bank_Transactions":
                    cur.execute("DELETE FROM Bank_Transaction_Splits WHERE bank_transactions_id IN %s", (tuple(ids_to_delete),))
                cur.execute(f"DELETE FROM {table_name} WHERE {id_col} IN %s", (tuple(ids_to_delete),))
            
            df_new = df_edited[df_edited[id_col].isna() | df_edited[id_col].isnull()].copy()
            df_updates = df_edited[df_edited[id_col].notna()].copy()
            
            if not df_updates.empty:
                cols = [c for c in df_updates.columns.tolist() if c != id_col]
                for _, row in df_updates.iterrows():
                    set_clause = ", ".join([f"{c} = %s" for c in cols])
                    sql_upd = f"UPDATE {table_name} SET {set_clause} WHERE {id_col} = %s"

                #    vals = [None if pd.isna(row[c]) else row[c] for c in cols]

                    # Instead of vals = [None if pd.isna(row[c]) else row[c] for c in cols]
                    vals = []
                    for c in cols:
                        val = row[c]
                        if pd.isna(val):
                            vals.append(None)
                        elif hasattr(val, 'item'): # Μετατρέπει numpy types σε python types
                            vals.append(val.item())
                        else:
                            vals.append(val)


                    vals.append(int(row[id_col]))
                    cur.execute(sql_upd, tuple(vals))
            
            if not df_new.empty:
                cols_new = [c for c in df_new.columns.tolist() if c != id_col]
                for _, row in df_new.iterrows():
                    placeholders = ", ".join(["%s"] * len(cols_new))
                    sql_ins = f"INSERT INTO {table_name} ({', '.join(cols_new)}) VALUES ({placeholders})"

                #    vals = [None if pd.isna(row[c]) else row[c] for c in cols_new]

                    # Instead of: vals = [None if pd.isna(row[c]) else row[c] for c in cols_new]
                    vals = []
                    for c in cols_new:
                        val = row[c]
                        if pd.isna(val):
                            vals.append(None)
                        elif hasattr(val, 'item'):
                            vals.append(val.item())
                        else:
                            vals.append(val)


                    cur.execute(sql_ins, tuple(vals))
            
            conn.commit()
            
            if table_name == "Bank_Transactions" and current_acc_id:
                update_account_balances(current_acc_id)
                st.session_state.balance_update_counter = st.session_state.get('balance_update_counter', 0) + 1
            
            st.success(f"Saved: {len(df_updates)} updates, {len(df_new)} new, {len(ids_to_delete)} deletions")
            st.rerun()
            
        except Exception as e:
            conn.rollback()
            st.error(f"Error: {e}")
            st.exception(e)

def save_changes_no_serial(df_original, df_edited, table_name, id_col):
    """Save changes for tables without serial ID."""
    if st.button(f"💾 Save {table_name}"):
        conn = get_connection()
        cur = conn.cursor()
        try:
            if table_name == "Historical_FX":
                def get_keys(df):
                    return set(df.apply(lambda r: f"{int(r['base_currency_id'])}|{int(r['target_currency_id'])}|{r['fx_date']}", axis=1))
                
                original_keys = get_keys(df_original)
                edited_keys = get_keys(df_edited)
                keys_to_delete = original_keys - edited_keys
                
                for key in keys_to_delete:
                    b_id, t_id, f_date = key.split('|')
                    cur.execute(f"""
                        DELETE FROM {table_name} 
                        WHERE base_currency_id = %s AND target_currency_id = %s AND fx_date = %s
                    """, (int(b_id), int(t_id), f_date))
            
            cols = df_edited.columns.tolist()
            data_tuples = [tuple(None if pd.isna(v) else v for v in row) for row in df_edited.values]
            
            if table_name == "Historical_FX":
                conflict_target = "base_currency_id, target_currency_id, fx_date"
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
                current_dates = df_edited['price_date'].dropna().tolist()
                if current_dates:
                    cur.execute(f"DELETE FROM {table_name} WHERE {filter_col} = %s AND price_date NOT IN %s",
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

def update_account_balances(target_acc_id=None):
    """Update account balances based on transactions."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        if target_acc_id:
            sql = """
                UPDATE Accounts a
                SET Account_Balance = COALESCE((
                    SELECT SUM(Total_Amount) 
                    FROM Bank_Transactions t 
                    WHERE t.Accounts_Id = a.Accounts_Id
                ), 0)
                WHERE a.Accounts_Id = %s;
            """
            cur.execute(sql, (int(target_acc_id),))
        else:
            sql = """
                UPDATE Accounts a
                SET Account_Balance = COALESCE((
                    SELECT SUM(Total_Amount) 
                    FROM Bank_Transactions t 
                    WHERE t.Accounts_Id = a.Accounts_Id
                ), 0)
                WHERE a.Accounts_Type NOT IN ('Pension');
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
            SET Account_Balance = COALESCE((
                SELECT  
                    SUM(CASE WHEN Action IN ('CashIn', 'IntInc') THEN Total_Amount 
                             WHEN Action IN ('CashOut') THEN -Total_Amount 
                             ELSE 0 END)
                FROM Investment_Transactions t 
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
            SET Account_Balance = COALESCE((
                SELECT  
                    SUM(CASE WHEN Action IN ('Dividend', 'CashIn', 'IntInc') THEN Total_Amount 
                             WHEN Action IN ('CashOut') THEN -Total_Amount 
                             ELSE 0 END)
                FROM Investment_Transactions t 
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
            WITH TransactionFlow AS (
                SELECT 
                    Accounts_Id, 
                    Securities_Id,
                    Date,
                    Inv_Transactions_Id,
                    Action,
                    Quantity,
                    Price_Per_Share,
                    -- 1. Historical Average (Simple Average) of all purchases up to today
                    AVG(Price_Per_Share) FILTER (WHERE Action IN ('Buy', 'Reinvest', 'ShrIn')) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id) as simple_avg_cost,
                    -- Cumulative Purchases & Sales
                    SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity ELSE 0 END) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id ORDER BY Date, Inv_Transactions_Id) as running_buys,
                    SUM(CASE WHEN Action IN ('Sell', 'ShrOut') THEN Quantity ELSE 0 END) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id ORDER BY Date, Inv_Transactions_Id) as running_sells,
                    -- Total Amounts
                    SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity ELSE 0 END) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id) as total_buys,
                    SUM(CASE WHEN Action IN ('Sell', 'ShrOut') THEN Quantity ELSE 0 END) 
                        OVER (PARTITION BY Accounts_Id, Securities_Id) as total_sells
                FROM Investment_Transactions
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
        """)
        conn.commit()
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()