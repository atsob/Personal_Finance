"""Dashboard API endpoints: net worth, accounts, summaries."""
from fastapi import APIRouter, Query
from typing import Optional
import pandas as pd
from database.connection import get_db

router = APIRouter()


def _df_to_list(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-serialisable list of dicts."""
    df = df.copy()
    for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
        df[col] = df[col].astype(str)
    for col in df.columns:
        if hasattr(df[col], "dt"):
            df[col] = df[col].astype(str)
    return df.where(pd.notnull(df), None).to_dict(orient="records")


@router.get("/net-worth")
def get_net_worth(start_date: str = Query("2020-01-01")):
    """Historical monthly net worth (cash, invested, pension, assets)."""
    query = f"""
    WITH RECURSIVE
    months AS (
        SELECT (date_trunc('month', %(sd)s::date) + INTERVAL '1 month' - INTERVAL '1 day')::date AS d
        UNION ALL
        SELECT (date_trunc('month', d + INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::date
        FROM months WHERE d < date_trunc('month', CURRENT_DATE)
    ),
    dates AS (
        SELECT d FROM months WHERE d <= CURRENT_DATE
        UNION SELECT CURRENT_DATE::date
    ),
    historical_assets AS (
        SELECT dt.d AS date, a.Accounts_Id, a.Currencies_Id,
            a.Accounts_Balance - COALESCE((
                SELECT SUM(Total_Amount) FROM Transactions
                WHERE Accounts_Id = a.Accounts_Id AND Date > dt.d
            ), 0) AS balance_at_date
        FROM dates dt CROSS JOIN Accounts a
        WHERE a.Accounts_Type IN ('Real Estate', 'Vehicle', 'Asset', 'Liability')
    ),
    historical_cash AS (
        SELECT dt.d AS date, a.Accounts_Id, a.Currencies_Id,
            a.Accounts_Balance - COALESCE((
                SELECT SUM(Total_Amount) FROM Transactions
                WHERE Accounts_Id = a.Accounts_Id AND Date > dt.d
            ), 0) AS balance_at_date
        FROM dates dt CROSS JOIN Accounts a
        WHERE a.Accounts_Type NOT IN ('Brokerage','Pension','Other Investment','Margin','Real Estate','Vehicle','Asset','Liability')
        UNION ALL
        SELECT dt.d AS date, a.Accounts_Id, a.Currencies_Id,
            a.Accounts_Balance - COALESCE((
                SELECT SUM(Total_Amount) FROM Transactions
                WHERE Accounts_Id = a.Accounts_Id AND Date > dt.d
            ), 0) AS balance_at_date
        FROM dates dt CROSS JOIN Accounts a
        WHERE a.Accounts_Type IN ('Other Investment')
    ),
    historical_pension AS (
        SELECT dt.d AS date, a.Accounts_Id, a.Currencies_Id,
            a.Accounts_Balance - COALESCE((
                SELECT SUM(CASE WHEN Action IN ('CashIn','IntInc') THEN Total_Amount_AccCur
                               WHEN Action IN ('CashOut') THEN -Total_Amount_AccCur
                               ELSE 0 END)
                FROM Investments WHERE Accounts_Id = a.Accounts_Id AND Date > dt.d
            ), 0) AS balance_at_date
        FROM dates dt CROSS JOIN Accounts a
        WHERE a.Accounts_Type IN ('Pension')
    ),
    historical_inv AS (
        SELECT dt.d AS date, h.Securities_Id,
            h.Quantity - COALESCE((
                SELECT SUM(CASE WHEN Action='Buy' THEN Quantity WHEN Action='Sell' THEN -Quantity ELSE 0 END)
                FROM Investments WHERE Securities_Id = h.Securities_Id AND Date > dt.d
            ), 0) AS qty_at_date
        FROM dates dt CROSS JOIN Holdings h
    ),
    daily_fx AS (
        SELECT dt.d AS date, c.Currencies_Id,
            (SELECT FX_Rate FROM Historical_FX WHERE Date <= dt.d AND Currencies_Id_1 = c.Currencies_Id ORDER BY Date DESC LIMIT 1) AS fx_rate
        FROM dates dt CROSS JOIN Currencies c
    ),
    daily_prices AS (
        SELECT dt.d AS date, s.Securities_Id,
            (SELECT Close FROM Historical_Prices WHERE Date <= dt.d AND Securities_Id = s.Securities_Id ORDER BY Date DESC LIMIT 1) AS close
        FROM dates dt CROSS JOIN Securities s
    ),
    final_calculation AS (
        SELECT dt.d AS date,
            (SELECT SUM(CASE WHEN cur.Currencies_ShortName='EUR' THEN ha.balance_at_date
                             ELSE ha.balance_at_date * COALESCE(dfx.fx_rate,1) END)
             FROM historical_assets ha
             JOIN Currencies cur ON ha.Currencies_Id = cur.Currencies_Id
             LEFT JOIN daily_fx dfx ON ha.date=dfx.date AND ha.Currencies_Id=dfx.Currencies_Id
             WHERE ha.date=dt.d) AS total_assets,
            (SELECT SUM(CASE WHEN cur.Currencies_ShortName='EUR' THEN hc.balance_at_date
                             ELSE hc.balance_at_date * COALESCE(dfx.fx_rate,1) END)
             FROM historical_cash hc
             JOIN Currencies cur ON hc.Currencies_Id = cur.Currencies_Id
             LEFT JOIN daily_fx dfx ON hc.date=dfx.date AND hc.Currencies_Id=dfx.Currencies_Id
             WHERE hc.date=dt.d) AS total_cash,
            (SELECT SUM(CASE WHEN cur.Currencies_ShortName='EUR' THEN hp.balance_at_date
                             ELSE hp.balance_at_date * COALESCE(dfx.fx_rate,1) END)
             FROM historical_pension hp
             JOIN Currencies cur ON hp.Currencies_Id = cur.Currencies_Id
             LEFT JOIN daily_fx dfx ON hp.date=dfx.date AND hp.Currencies_Id=dfx.Currencies_Id
             WHERE hp.date=dt.d) AS total_pension,
            (SELECT SUM(hi.qty_at_date * COALESCE(dp.close,0) *
                CASE WHEN cs.Currencies_ShortName='EUR' THEN 1 ELSE COALESCE(dfx_inv.fx_rate,1) END)
             FROM historical_inv hi
             JOIN Securities s ON hi.Securities_Id=s.Securities_Id
             JOIN Currencies cs ON s.Currencies_Id=cs.Currencies_Id
             LEFT JOIN daily_prices dp ON hi.date=dp.date AND hi.Securities_Id=dp.Securities_Id
             LEFT JOIN daily_fx dfx_inv ON hi.date=dfx_inv.date AND s.Currencies_Id=dfx_inv.Currencies_Id
             WHERE hi.date=dt.d) AS total_invested
        FROM dates dt
    )
    SELECT date,
           COALESCE(total_assets,0) AS total_assets,
           COALESCE(total_cash,0) AS total_cash,
           COALESCE(total_pension,0) AS total_pension,
           COALESCE(total_invested,0) AS total_invested,
           (COALESCE(total_assets,0)+COALESCE(total_cash,0)+COALESCE(total_pension,0)+COALESCE(total_invested,0)) AS total_net_worth
    FROM final_calculation
    ORDER BY date ASC
    """
    with get_db() as conn:
        df = pd.read_sql(query, conn, params={"sd": start_date})
    return _df_to_list(df)


@router.get("/accounts")
def get_accounts():
    """All accounts with current balance, type, and currency."""
    with get_db() as conn:
        df = pd.read_sql("""
            SELECT a.Accounts_Id AS id,
                   a.Accounts_Name AS name,
                   a.Accounts_Type AS type,
                   a.Accounts_Balance AS balance,
                   a.Is_Active AS is_active,
                   c.Currencies_ShortName AS currency,
                   i.Institutions_Name AS institution
            FROM Accounts a
            JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id
            LEFT JOIN Institutions i ON a.Institutions_Id = i.Institutions_Id
            ORDER BY a.Accounts_Type, a.Accounts_Name
        """, conn)
    return _df_to_list(df)


@router.get("/monthly-summaries")
def get_monthly_summaries(limit: int = Query(12)):
    """Latest AI-generated monthly financial summaries."""
    with get_db() as conn:
        df = pd.read_sql("""
            SELECT month_start, summary_text
            FROM ai_monthly_summaries
            ORDER BY month_start DESC
            LIMIT %(limit)s
        """, conn, params={"limit": limit})
    return _df_to_list(df)
