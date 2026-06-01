"""Register API endpoints: transaction list per account."""
from fastapi import APIRouter, Query
from typing import Optional
import pandas as pd
from database.connection import get_db

router = APIRouter()


def _df_to_list(df: pd.DataFrame) -> list:
    df = df.copy()
    for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
        df[col] = df[col].astype(str)
    return df.where(pd.notnull(df), None).to_dict(orient="records")


@router.get("/transactions")
def get_transactions(
    account_id: int = Query(...),
    from_date: str = Query("2020-01-01"),
    to_date: str = Query("2099-12-31"),
    status: Optional[str] = Query(None),   # 'cleared' | 'uncleared' | None
    search: Optional[str] = Query(None),
    limit: int = Query(200),
    offset: int = Query(0),
):
    """Paginated transaction list with running balance for one account."""
    status_clause = ""
    if status == "cleared":
        status_clause = "AND t.Cleared = TRUE"
    elif status == "uncleared":
        status_clause = "AND t.Cleared = FALSE"

    search_clause = ""
    search_param = None
    if search:
        search_clause = "AND (t.Description ILIKE %(search)s OR p.Payees_Name ILIKE %(search)s)"
        search_param = f"%{search}%"

    query = f"""
        WITH all_txns AS (
            SELECT Transactions_Id,
                   SUM(Total_Amount) OVER (
                       ORDER BY Date ASC, Transactions_Id ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS cumulative_total,
                   SUM(Total_Amount) OVER () AS grand_total
            FROM Transactions
            WHERE Accounts_Id = %(account_id)s
        ),
        account_info AS (
            SELECT Accounts_Balance FROM Accounts WHERE Accounts_Id = %(account_id)s
        )
        SELECT
            t.Transactions_Id AS id,
            t.Date::text AS date,
            t.Description AS description,
            t.Total_Amount AS amount,
            t.Cleared AS cleared,
            t.Memo AS memo,
            p.Payees_Name AS payee,
            cat.Full_Path AS category,
            ta.Accounts_Name AS target_account,
            (ai.Accounts_Balance - at2.grand_total + at2.cumulative_total) AS running_balance
        FROM Transactions t
        JOIN all_txns at2 ON at2.Transactions_Id = t.Transactions_Id
        CROSS JOIN account_info ai
        LEFT JOIN Payees p ON t.Payees_Id = p.Payees_Id
        LEFT JOIN (
            WITH RECURSIVE ch AS (
                SELECT Categories_Id, Categories_Name::TEXT AS Full_Path, Categories_Id_Parent
                FROM Categories WHERE Categories_Id_Parent IS NULL
                UNION ALL
                SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name, c.Categories_Id_Parent
                FROM Categories c JOIN ch ON c.Categories_Id_Parent = ch.Categories_Id
            )
            SELECT * FROM ch
        ) cat ON t.Categories_Id = cat.Categories_Id
        LEFT JOIN Accounts ta ON t.Transfer_Account_Id = ta.Accounts_Id
        WHERE t.Accounts_Id = %(account_id)s
          AND t.Date BETWEEN %(from_date)s AND %(to_date)s
          {status_clause}
          {search_clause}
        ORDER BY t.Date DESC, t.Transactions_Id DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    params = {
        "account_id": account_id,
        "from_date": from_date,
        "to_date": to_date,
        "limit": limit,
        "offset": offset,
    }
    if search_param:
        params["search"] = search_param

    with get_db() as conn:
        df = pd.read_sql(query, conn, params=params)

    # Total count for pagination
    count_query = f"""
        SELECT COUNT(*) AS total
        FROM Transactions t
        LEFT JOIN Payees p ON t.Payees_Id = p.Payees_Id
        WHERE t.Accounts_Id = %(account_id)s
          AND t.Date BETWEEN %(from_date)s AND %(to_date)s
          {status_clause}
          {search_clause}
    """
    count_params = {k: v for k, v in params.items() if k in ("account_id", "from_date", "to_date")}
    if search_param:
        count_params["search"] = search_param

    with get_db() as conn:
        total = pd.read_sql(count_query, conn, params=count_params).iloc[0]["total"]

    return {"total": int(total), "transactions": _df_to_list(df)}
