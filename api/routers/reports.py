"""Reports API endpoints: income/expense, P&L, savings rate."""
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


@router.get("/income-expense")
def get_income_expense(
    start_date: str = Query("2024-01-01"),
    end_date: str = Query("2099-12-31"),
):
    """Monthly income vs expense totals, EUR-converted."""
    query = """
    WITH RECURSIVE CategoryHierarchy AS (
        SELECT Categories_Id, Categories_Name::TEXT AS Full_Path,
               Categories_Type::TEXT AS Categories_Type, Categories_Id_Parent, 0 AS Level
        FROM Categories WHERE Categories_Id_Parent IS NULL
        UNION ALL
        SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name,
               c.Categories_Type::TEXT, c.Categories_Id_Parent, ch.Level + 1
        FROM Categories c JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
    ),
    tx_with_cat AS (
        SELECT
            date_trunc('month', t.Date)::date AS month,
            COALESCE(s.Amount, t.Total_Amount) AS amount,
            COALESCE(cat.Categories_Type, 'Uncategorized') AS cat_type,
            SPLIT_PART(COALESCE(cat.Full_Path, 'Uncategorized'), ' : ', 1) AS top_category,
            COALESCE(cat.Full_Path, 'Uncategorized') AS full_category
        FROM Transactions t
        LEFT JOIN Splits s ON s.Transactions_Id = t.Transactions_Id
        LEFT JOIN CategoryHierarchy cat ON COALESCE(s.Categories_Id, t.Categories_Id) = cat.Categories_Id
        JOIN Accounts a ON t.Accounts_Id = a.Accounts_Id
        WHERE t.Date BETWEEN %(start_date)s AND %(end_date)s
          AND a.Accounts_Type IN ('Cash','Checking','Savings','Credit Card','Loan','Other')
          AND t.Transfer_Account_Id IS NULL
    )
    SELECT month,
           SUM(CASE WHEN cat_type = 'Income' THEN amount ELSE 0 END) AS income,
           SUM(CASE WHEN cat_type = 'Expense' THEN ABS(amount) ELSE 0 END) AS expense
    FROM tx_with_cat
    GROUP BY month
    ORDER BY month ASC
    """
    with get_db() as conn:
        df = pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})
    return _df_to_list(df)


@router.get("/top-categories")
def get_top_categories(
    start_date: str = Query("2024-01-01"),
    end_date: str = Query("2099-12-31"),
    cat_type: str = Query("Expense"),
    top_n: int = Query(10),
):
    """Top N income or expense categories for the period."""
    query = """
    WITH RECURSIVE CategoryHierarchy AS (
        SELECT Categories_Id, Categories_Name::TEXT AS Full_Path,
               Categories_Type::TEXT AS Categories_Type, Categories_Id_Parent
        FROM Categories WHERE Categories_Id_Parent IS NULL
        UNION ALL
        SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name,
               c.Categories_Type::TEXT, c.Categories_Id_Parent
        FROM Categories c JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
    )
    SELECT
        SPLIT_PART(COALESCE(cat.Full_Path,'Uncategorized'),' : ',1) AS category,
        SUM(ABS(COALESCE(s.Amount, t.Total_Amount))) AS total
    FROM Transactions t
    LEFT JOIN Splits s ON s.Transactions_Id = t.Transactions_Id
    LEFT JOIN CategoryHierarchy cat ON COALESCE(s.Categories_Id, t.Categories_Id) = cat.Categories_Id
    JOIN Accounts a ON t.Accounts_Id = a.Accounts_Id
    WHERE t.Date BETWEEN %(start_date)s AND %(end_date)s
      AND a.Accounts_Type IN ('Cash','Checking','Savings','Credit Card','Loan','Other')
      AND t.Transfer_Account_Id IS NULL
      AND COALESCE(cat.Categories_Type,'Uncategorized') = %(cat_type)s
    GROUP BY 1
    ORDER BY total DESC
    LIMIT %(top_n)s
    """
    with get_db() as conn:
        df = pd.read_sql(query, conn, params={
            "start_date": start_date,
            "end_date": end_date,
            "cat_type": cat_type,
            "top_n": top_n,
        })
    return _df_to_list(df)


@router.get("/savings-rate")
def get_savings_rate(months: int = Query(12)):
    """Monthly savings rate for the last N months."""
    query = """
    WITH RECURSIVE CategoryHierarchy AS (
        SELECT Categories_Id, Categories_Name::TEXT AS Full_Path,
               Categories_Type::TEXT AS Categories_Type, Categories_Id_Parent
        FROM Categories WHERE Categories_Id_Parent IS NULL
        UNION ALL
        SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name,
               c.Categories_Type::TEXT, c.Categories_Id_Parent
        FROM Categories c JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
    ),
    monthly AS (
        SELECT
            date_trunc('month', t.Date)::date AS month,
            SUM(CASE WHEN cat.Categories_Type='Income' THEN COALESCE(s.Amount,t.Total_Amount) ELSE 0 END) AS income,
            SUM(CASE WHEN cat.Categories_Type='Expense' THEN ABS(COALESCE(s.Amount,t.Total_Amount)) ELSE 0 END) AS expense
        FROM Transactions t
        LEFT JOIN Splits s ON s.Transactions_Id = t.Transactions_Id
        LEFT JOIN CategoryHierarchy cat ON COALESCE(s.Categories_Id,t.Categories_Id)=cat.Categories_Id
        JOIN Accounts a ON t.Accounts_Id=a.Accounts_Id
        WHERE a.Accounts_Type IN ('Cash','Checking','Savings','Credit Card','Loan','Other')
          AND t.Transfer_Account_Id IS NULL
          AND t.Date >= (CURRENT_DATE - (%(months)s || ' months')::interval)
        GROUP BY 1
    )
    SELECT month,
           income,
           expense,
           CASE WHEN income > 0 THEN ROUND(((income - expense) / income * 100)::numeric, 1) ELSE 0 END AS savings_rate
    FROM monthly
    ORDER BY month ASC
    """
    with get_db() as conn:
        df = pd.read_sql(query, conn, params={"months": months})
    return _df_to_list(df)


@router.get("/portfolio-summary")
def get_portfolio_summary():
    """Current holdings with value in EUR grouped by account."""
    query = """
    SELECT
        a.Accounts_Name AS account,
        a.Accounts_Type AS account_type,
        s.Securities_Name AS security,
        s.Securities_Ticker AS ticker,
        h.Quantity AS quantity,
        COALESCE(
            (SELECT Close FROM Historical_Prices WHERE Securities_Id=h.Securities_Id ORDER BY Date DESC LIMIT 1),
            0
        ) AS last_price,
        COALESCE(
            (SELECT FX_Rate FROM Historical_FX
             WHERE Currencies_Id_1=s.Currencies_Id
             ORDER BY Date DESC LIMIT 1),
            1
        ) AS fx_rate,
        h.Quantity * COALESCE(
            (SELECT Close FROM Historical_Prices WHERE Securities_Id=h.Securities_Id ORDER BY Date DESC LIMIT 1),
            0
        ) * COALESCE(
            (SELECT FX_Rate FROM Historical_FX WHERE Currencies_Id_1=s.Currencies_Id ORDER BY Date DESC LIMIT 1),
            1
        ) AS value_eur,
        c.Currencies_ShortName AS currency
    FROM Holdings h
    JOIN Securities s ON h.Securities_Id = s.Securities_Id
    JOIN Accounts a ON h.Accounts_Id = a.Accounts_Id
    JOIN Currencies c ON s.Currencies_Id = c.Currencies_Id
    WHERE h.Quantity != 0
    ORDER BY value_eur DESC
    """
    with get_db() as conn:
        df = pd.read_sql(query, conn)
    return _df_to_list(df)
