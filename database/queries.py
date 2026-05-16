import pandas as pd
import streamlit as st
from database.connection import get_connection

from datetime import datetime, timedelta

@st.cache_data(ttl=3600)
def get_category_hierarchy():
    """Get category hierarchy with full paths"""
    conn = get_connection()

    query = """
    WITH RECURSIVE CategoryHierarchy AS (
        SELECT 
            Categories_Id, 
            Categories_Name::TEXT as Full_Path,
            Categories_Name::TEXT as Name,
            Categories_Type,
            Categories_Id_Parent,
            0 as Level
        FROM Categories 
        WHERE Categories_Id_Parent IS NULL
        
        UNION ALL
        
        SELECT 
            c.Categories_Id, 
            ch.Full_Path || ' : ' || c.Categories_Name as Full_Path,
            c.Categories_Name as Name,
            c.Categories_Type,
            c.Categories_Id_Parent,
            ch.Level + 1 as Level
        FROM Categories c
        JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
    )
    SELECT * FROM CategoryHierarchy ORDER BY Full_Path
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=3600)
def get_hist_net_worth_data(start_date):
    """Get historical net worth data."""
    conn = get_connection()
    
    query = f"""
    WITH RECURSIVE 
    months AS (
        SELECT (date_trunc('month', '{start_date}'::date) + INTERVAL '1 month' - INTERVAL '1 day')::date as d
        UNION ALL
        SELECT (date_trunc('month', d + INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::date 
        FROM months 
        WHERE d < date_trunc('month', CURRENT_DATE)
    ),
    dates AS (
        SELECT d FROM months WHERE d <= CURRENT_DATE
        UNION
        SELECT CURRENT_DATE::date
    ),
    historical_assets AS (
        SELECT 
            dt.d as date,
            a.Accounts_Id,
            a.Currencies_Id,
            a.Accounts_Balance - COALESCE((
                SELECT SUM(Total_Amount) 
                FROM Transactions 
                WHERE Accounts_Id = a.Accounts_Id 
                AND Date > dt.d
            ), 0) as balance_at_date
        FROM dates dt
        CROSS JOIN Accounts a
        WHERE a.Accounts_Type IN ('Real Estate', 'Vehicle', 'Asset', 'Liability')
    ),
    historical_cash AS (
        SELECT 
            dt.d as date,
            a.Accounts_Id,
            a.Currencies_Id,
            a.Accounts_Balance - COALESCE((
                SELECT SUM(Total_Amount) 
                FROM Transactions 
                WHERE Accounts_Id = a.Accounts_Id 
                AND Date > dt.d
            ), 0) as balance_at_date
        FROM dates dt
        CROSS JOIN Accounts a
        WHERE a.Accounts_Type NOT IN ('Brokerage', 'Pension', 'Other Investment', 'Margin', 'Real Estate', 'Vehicle', 'Asset', 'Liability')
        UNION ALL
        SELECT 
            dt.d as date,
            a.Accounts_Id,
            a.Currencies_Id,
            a.Accounts_Balance - COALESCE((
                SELECT SUM(Total_Amount) 
                FROM Transactions 
                WHERE Accounts_Id = a.Accounts_Id 
                AND Date > dt.d
            ), 0) as balance_at_date
        FROM dates dt
        CROSS JOIN Accounts a
        WHERE a.Accounts_Type IN ('Other Investment')        
    ),
    historical_pension AS (
        SELECT 
            dt.d as date,
            a.Accounts_Id,
            a.Currencies_Id,
            a.Accounts_Balance - COALESCE((
                SELECT  
                    SUM(CASE WHEN Action IN ('CashIn', 'IntInc') THEN Total_Amount 
                             WHEN Action IN ('CashOut') THEN -Total_Amount 
                             ELSE 0 END)
                FROM Investments
                WHERE Accounts_Id = a.Accounts_Id
                AND Date > dt.d
            ), 0) as balance_at_date
        FROM dates dt
        CROSS JOIN Accounts a
        WHERE a.Accounts_Type IN ('Pension')
    ),
    historical_inv AS (
        SELECT 
            dt.d as date,
            h.Securities_Id,
            h.Quantity - COALESCE((
                SELECT SUM(CASE WHEN Action = 'Buy' THEN Quantity WHEN Action = 'Sell' THEN -Quantity ELSE 0 END)
                FROM Investments 
                WHERE Securities_Id = h.Securities_Id 
                AND Date > dt.d
            ), 0) as qty_at_date
        FROM dates dt
        CROSS JOIN Holdings h
    ),
    daily_fx AS (
        SELECT dt.d as date, c.Currencies_Id,
            (SELECT FX_Rate FROM Historical_FX WHERE Date <= dt.d AND Currencies_Id_1 = c.Currencies_Id ORDER BY Date DESC LIMIT 1) as fx_rate
        FROM dates dt CROSS JOIN Currencies c
    ),
    daily_prices AS (
        SELECT dt.d as date, s.Securities_Id,
            (SELECT Close FROM Historical_Prices WHERE Date <= dt.d AND Securities_Id = s.Securities_Id ORDER BY Date DESC LIMIT 1) as close
        FROM dates dt CROSS JOIN Securities s
    ),
    final_calculation AS (
        SELECT 
            dt.d as date,
            (SELECT SUM(CASE 
                WHEN cur.Currencies_ShortName = 'EUR' THEN ha.balance_at_date 
                ELSE ha.balance_at_date * COALESCE(dfx.fx_rate, 1) 
             END)
             FROM historical_assets ha
             JOIN Currencies cur ON ha.Currencies_Id = cur.Currencies_Id
             LEFT JOIN daily_fx dfx ON ha.date = dfx.date AND ha.Currencies_Id = dfx.Currencies_Id
             WHERE ha.date = dt.d) as total_assets,
            (SELECT SUM(CASE 
                WHEN cur.Currencies_ShortName = 'EUR' THEN hc.balance_at_date 
                ELSE hc.balance_at_date * COALESCE(dfx.fx_rate, 1) 
             END)
             FROM historical_cash hc
             JOIN Currencies cur ON hc.Currencies_Id = cur.Currencies_Id
             LEFT JOIN daily_fx dfx ON hc.date = dfx.date AND hc.Currencies_Id = dfx.Currencies_Id
             WHERE hc.date = dt.d) as total_cash,
            (SELECT SUM(CASE 
                WHEN cur.Currencies_ShortName = 'EUR' THEN hp.balance_at_date 
                ELSE hp.balance_at_date * COALESCE(dfx.fx_rate, 1) 
             END)
             FROM historical_pension hp
             JOIN Currencies cur ON hp.Currencies_Id = cur.Currencies_Id
             LEFT JOIN daily_fx dfx ON hp.date = dfx.date AND hp.Currencies_Id = dfx.Currencies_Id
             WHERE hp.date = dt.d) as total_pension,
            (SELECT SUM(hi.qty_at_date * COALESCE(dp.close, 0) * 
                CASE WHEN cs.Currencies_ShortName = 'EUR' THEN 1 ELSE COALESCE(dfx_inv.fx_rate, 1) END
             )
             FROM historical_inv hi
             JOIN Securities s ON hi.Securities_Id = s.Securities_Id
             JOIN Currencies cs ON s.Currencies_Id = cs.Currencies_Id
             LEFT JOIN daily_prices dp ON hi.date = dp.date AND hi.Securities_Id = dp.Securities_Id
             LEFT JOIN daily_fx dfx_inv ON hi.date = dfx_inv.date AND s.Currencies_Id = dfx_inv.Currencies_Id
             WHERE hi.date = dt.d) as total_invested
        FROM dates dt
    )
    SELECT 
        date,
        COALESCE(total_assets, 0) as total_assets,
        COALESCE(total_cash, 0) as total_cash,
        COALESCE(total_pension, 0) as total_pension,
        COALESCE(total_invested, 0) as total_invested,
        (COALESCE(total_assets, 0) + COALESCE(total_cash, 0) + COALESCE(total_pension, 0) + COALESCE(total_invested, 0)) as total_net_worth
    FROM final_calculation
    ORDER BY date ASC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def get_hist_inv_positions_data(start_date):
    conn = get_connection()
    query = f"""
    WITH RECURSIVE 
    months AS (
        SELECT (date_trunc('month', '{start_date}'::date) + INTERVAL '1 month' - INTERVAL '1 day')::date as d
        UNION ALL
        SELECT (date_trunc('month', d + INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::date 
        FROM months 
        WHERE d < date_trunc('month', CURRENT_DATE)
    ),
    dates AS (
        SELECT d FROM months WHERE d <= CURRENT_DATE
        UNION
        SELECT CURRENT_DATE::date
    ),
    historical_qty AS (
        SELECT 
            dt.d as date,
            h.Securities_Id,
            h.Accounts_Id,
            h.Quantity - COALESCE((
                SELECT SUM(CASE 
                    WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity 
                    WHEN Action IN ('Sell', 'ShrOut') THEN -Quantity 
                    ELSE 0 END)
                FROM Investments 
                WHERE Securities_Id = h.Securities_Id AND Accounts_Id = h.Accounts_Id
                AND Date > dt.d
            ), 0) as qty_at_date
        FROM dates dt
        CROSS JOIN Holdings h
    ),
    daily_fx AS (
        SELECT dt.d as date, c.Currencies_Id,
            (SELECT FX_Rate FROM Historical_FX WHERE Date <= dt.d AND Currencies_Id_1 = c.Currencies_Id ORDER BY Date DESC LIMIT 1) as fx_rate
        FROM dates dt CROSS JOIN Currencies c
    ),
    daily_prices AS (
        SELECT dt.d as date, s.Securities_Id,
            (SELECT Close FROM Historical_Prices WHERE Date <= dt.d AND Securities_Id = s.Securities_Id ORDER BY Date DESC LIMIT 1) as close
        FROM dates dt CROSS JOIN Securities s
    )
    SELECT 
        hq.date,
        a.Accounts_Name,
        s.Securities_Name,
        hq.qty_at_date,
        COALESCE(dp.close, 0) as price_at_date,
        (hq.qty_at_date * COALESCE(dp.close, 0) * 
            CASE WHEN cur_s.Currencies_ShortName = 'EUR' THEN 1 ELSE COALESCE(dfx.fx_rate, 1) END
        ) as value_in_eur
    FROM historical_qty hq
    JOIN Accounts a ON hq.Accounts_Id = a.Accounts_Id
    JOIN Securities s ON hq.Securities_Id = s.Securities_Id
    JOIN Currencies cur_s ON s.Currencies_Id = cur_s.Currencies_Id
    LEFT JOIN daily_prices dp ON hq.date = dp.date AND hq.Securities_Id = dp.Securities_Id
    LEFT JOIN daily_fx dfx ON hq.date = dfx.date AND s.Currencies_Id = dfx.Currencies_Id
    WHERE hq.qty_at_date != 0
    ORDER BY hq.date ASC, a.Accounts_Name ASC, value_in_eur DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=3600)
def get_net_worth_report_data(start_date: str, interval: str = 'Year', account_ids: tuple = None):
    """Per-account historical balances at each period-end for the Quicken-style Net Worth report."""
    conn = get_connection()

    trunc_map = {'Year': 'year', 'Quarter': 'quarter', 'Month': 'month'}
    intv_map  = {'Year': '1 year', 'Quarter': '3 months', 'Month': '1 month'}
    trunc_unit  = trunc_map.get(interval, 'year')
    pg_interval = intv_map.get(interval, '1 year')

    # Build account filter clause (account_ids are internal DB integers — safe to embed)
    if account_ids:
        ids_sql = ', '.join(str(int(i)) for i in account_ids)
        acc_filter = f'AND a.Accounts_Id IN ({ids_sql})'
        inv_acc_filter = f'AND Accounts_Id IN ({ids_sql})'
    else:
        acc_filter = ''
        inv_acc_filter = ''

    query = f"""
    WITH
    period_dates AS (
        SELECT (gs - INTERVAL '1 day')::date AS period_end
        FROM generate_series(
            date_trunc('{trunc_unit}', '{start_date}'::date) + '{pg_interval}'::interval,
            date_trunc('{trunc_unit}', CURRENT_DATE),
            '{pg_interval}'::interval
        ) gs
        UNION
        SELECT CURRENT_DATE::date
        ORDER BY 1
    ),
    daily_fx AS (
        SELECT p.period_end, cur.Currencies_Id,
            (SELECT FX_Rate FROM Historical_FX
             WHERE Currencies_Id_1 = cur.Currencies_Id AND Date <= p.period_end
             ORDER BY Date DESC LIMIT 1) AS fx_rate
        FROM period_dates p
        CROSS JOIN Currencies cur
        WHERE cur.Currencies_ShortName != 'EUR'
    ),
    -- Cash, savings, credit cards, loans, assets, liabilities, other-investment
    cash_like AS (
        SELECT
            p.period_end,
            a.Accounts_Id,
            a.Accounts_Name,
            a.Accounts_Type,
            CASE
                WHEN a.Accounts_Type IN ('Real Estate', 'Vehicle', 'Asset')
                -- Physical assets cannot go negative (backwards reconstruction artefact)
                THEN GREATEST(0, a.Accounts_Balance - COALESCE((
                    SELECT SUM(Total_Amount) FROM Transactions
                    WHERE Accounts_Id = a.Accounts_Id AND Date > p.period_end
                ), 0))
                ELSE (a.Accounts_Balance - COALESCE((
                    SELECT SUM(Total_Amount) FROM Transactions
                    WHERE Accounts_Id = a.Accounts_Id AND Date > p.period_end
                ), 0))
            END * COALESCE(
                (SELECT fx_rate FROM daily_fx
                 WHERE period_end = p.period_end AND Currencies_Id = a.Currencies_Id),
                1
            ) AS balance_eur
        FROM period_dates p
        CROSS JOIN Accounts a
        WHERE a.Accounts_Type NOT IN ('Brokerage', 'Margin', 'Pension')
          {acc_filter}
    ),
    -- Brokerage / Margin: forward cumulative qty per security × price × fx
    -- Uses Investments table directly so fully-sold securities appear in history
    investment_universe AS (
        SELECT DISTINCT Securities_Id, Accounts_Id
        FROM Investments
        WHERE Action IN ('Buy', 'Reinvest', 'ShrIn', 'Sell', 'ShrOut')
          {inv_acc_filter}
    ),
    investment_holdings AS (
        SELECT
            p.period_end,
            a.Accounts_Id,
            a.Accounts_Name,
            a.Accounts_Type,
            SUM(
                GREATEST(COALESCE((
                    SELECT SUM(CASE
                        WHEN Action IN ('Buy','Reinvest','ShrIn') THEN  Quantity
                        WHEN Action IN ('Sell','ShrOut')          THEN -Quantity
                        ELSE 0 END)
                    FROM Investments i2
                    WHERE i2.Securities_Id = i.Securities_Id
                      AND i2.Accounts_Id   = i.Accounts_Id
                      AND i2.Date          <= p.period_end
                ), 0), 0) *
                COALESCE((
                    SELECT Close FROM Historical_Prices
                    WHERE Securities_Id = i.Securities_Id AND Date <= p.period_end
                    ORDER BY Date DESC LIMIT 1
                ), 0) *
                COALESCE(
                    (SELECT fx_rate FROM daily_fx
                     WHERE period_end = p.period_end AND Currencies_Id = s.Currencies_Id),
                    1
                )
            ) AS balance_eur
        FROM period_dates p
        CROSS JOIN investment_universe i
        JOIN Accounts   a ON i.Accounts_Id   = a.Accounts_Id
        JOIN Securities s ON i.Securities_Id = s.Securities_Id
        WHERE a.Accounts_Type IN ('Brokerage', 'Margin')
        GROUP BY p.period_end, a.Accounts_Id, a.Accounts_Name, a.Accounts_Type
    ),
    -- Pension: backwards from current balance via CashIn/CashOut
    pension_like AS (
        SELECT
            p.period_end,
            a.Accounts_Id,
            a.Accounts_Name,
            a.Accounts_Type,
            GREATEST(0, a.Accounts_Balance - COALESCE((
                SELECT SUM(CASE
                    WHEN Action IN ('CashIn', 'IntInc') THEN  Total_Amount
                    WHEN Action IN ('CashOut')          THEN -Total_Amount
                    ELSE 0 END)
                FROM Investments
                WHERE Accounts_Id = a.Accounts_Id AND Date > p.period_end
            ), 0)) * COALESCE(
                (SELECT fx_rate FROM daily_fx
                 WHERE period_end = p.period_end AND Currencies_Id = a.Currencies_Id),
                1
            ) AS balance_eur
        FROM period_dates p
        CROSS JOIN Accounts a
        WHERE a.Accounts_Type = 'Pension'
          {acc_filter}
    )
    SELECT
        period_end,
        Accounts_Id   AS accounts_id,
        Accounts_Name AS accounts_name,
        Accounts_Type AS accounts_type,
        CASE Accounts_Type
            WHEN 'Credit Card' THEN 'Liabilities'
            WHEN 'Loan'        THEN 'Liabilities'
            WHEN 'Liability'   THEN 'Liabilities'
            ELSE 'Assets'
        END AS section,
        CASE Accounts_Type
            WHEN 'Brokerage'        THEN 'Investments'
            WHEN 'Margin'           THEN 'Investments'
            WHEN 'Pension'          THEN 'Pension'
            WHEN 'Cash'             THEN 'Cash & Bank'
            WHEN 'Checking'         THEN 'Cash & Bank'
            WHEN 'Savings'          THEN 'Cash & Bank'
            WHEN 'Other Investment' THEN 'Cash & Bank'
            WHEN 'Other'            THEN 'Cash & Bank'
            WHEN 'Real Estate'      THEN 'Other Assets'
            WHEN 'Vehicle'          THEN 'Other Assets'
            WHEN 'Asset'            THEN 'Other Assets'
            WHEN 'Credit Card'      THEN 'Credit Cards'
            WHEN 'Loan'             THEN 'Loans'
            WHEN 'Liability'        THEN 'Other Liabilities'
            ELSE 'Other'
        END AS group_name,
        COALESCE(balance_eur, 0) AS balance_eur
    FROM (
        SELECT period_end, Accounts_Id, Accounts_Name, Accounts_Type, balance_eur FROM cash_like
        UNION ALL
        SELECT period_end, Accounts_Id, Accounts_Name, Accounts_Type, balance_eur FROM investment_holdings
        UNION ALL
        SELECT period_end, Accounts_Id, Accounts_Name, Accounts_Type, balance_eur FROM pension_like
    ) all_accounts
    ORDER BY period_end, section DESC, group_name, Accounts_Name
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_all_accounts_for_nwr():
    """All accounts available for Net Worth Report selection, ordered by type then name."""
    conn = get_connection()
    df = pd.read_sql("""
        SELECT Accounts_Id AS accounts_id, Accounts_Name AS accounts_name,
               Accounts_Type AS accounts_type
        FROM Accounts
        ORDER BY Accounts_Type, Accounts_Name
    """, conn)
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_price_anomalies(threshold_pct: float = 100.0, securities_ids: tuple = None):
    """
    Detect Historical_Prices rows that are outliers via two independent signals:
      1. Day-to-day spike: price ratio vs previous OR next trading day >= threshold.
      2. Transaction divergence: price deviates from the nearest buy/sell within 90 days
         by >= threshold — catches whole blocks of systematically wrong prices (e.g.
         pre-split prices in wrong units) that look internally consistent.
    A LATERAL join fetches the nearest transaction once per price row efficiently.
    """
    conn = get_connection()

    sec_filter = (
        f"AND hp.Securities_Id IN ({', '.join(str(int(i)) for i in securities_ids)})"
        if securities_ids else ""
    )
    ratio = 1.0 + threshold_pct / 100.0

    query = f"""
    WITH price_neighbors AS (
        SELECT
            hp.Securities_Id,
            s.Securities_Name AS security_name,
            hp.Date,
            hp.Close,
            LAG(hp.Close)  OVER (PARTITION BY hp.Securities_Id ORDER BY hp.Date) AS prev_close,
            LEAD(hp.Close) OVER (PARTITION BY hp.Securities_Id ORDER BY hp.Date) AS next_close
        FROM Historical_Prices hp
        JOIN Securities s ON s.Securities_Id = hp.Securities_Id
        WHERE hp.Close > 0
          {sec_filter}
    ),
    enriched AS (
        SELECT
            pn.*,
            CASE WHEN pn.prev_close > 0
                 THEN ROUND(((pn.Close / pn.prev_close) - 1) * 100, 1) END AS pct_vs_prev,
            CASE WHEN pn.next_close > 0
                 THEN ROUND(((pn.Close / pn.next_close) - 1) * 100, 1) END AS pct_vs_next,
            tx.tx_date,
            tx.tx_action,
            tx.tx_price,
            tx.days_diff
        FROM price_neighbors pn
        LEFT JOIN LATERAL (
            SELECT
                i.Date  AS tx_date,
                i.Action AS tx_action,
                ROUND((i.Total_Amount / NULLIF(i.Quantity, 0))::numeric, 4) AS tx_price,
                ABS(pn.Date - i.Date) AS days_diff
            FROM Investments i
            WHERE i.Securities_Id = pn.Securities_Id
              AND i.Action IN ('Buy','Sell','Reinvest','ShrIn','ShrOut')
              AND i.Quantity > 0
              AND ABS(pn.Date - i.Date) <= 90
            ORDER BY ABS(pn.Date - i.Date)
            LIMIT 1
        ) tx ON TRUE
    )
    SELECT
        Securities_Id AS securities_id,
        security_name,
        Date          AS date,
        Close         AS price,
        prev_close,
        next_close,
        pct_vs_prev,
        pct_vs_next,
        tx_date,
        tx_action,
        tx_price,
        days_diff,
        CASE WHEN tx_price > 0
             THEN ROUND(((Close / tx_price) - 1) * 100, 1)
        END AS pct_vs_tx
    FROM enriched
    WHERE
        -- Signal 1: day-to-day spike
        (prev_close > 0 AND (Close / prev_close >= {ratio} OR prev_close / Close >= {ratio}))
        OR
        (next_close > 0 AND (Close / next_close >= {ratio} OR next_close / Close >= {ratio}))
        -- Signal 2: systematic offset vs nearest transaction
        OR
        (tx_price > 0 AND (Close / tx_price >= {ratio} OR tx_price / Close >= {ratio}))
    ORDER BY security_name, date
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def get_missing_tx_prices() -> pd.DataFrame:
    """Return investment transactions whose Price_Per_Share is not yet in Historical_Prices.

    When multiple transactions for the same security exist on the same date the
    average price is used.  Only actions that carry a meaningful per-share price
    are included (Buy, Sell, Reinvest, ShrIn, ShrOut).
    """
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            i.Securities_Id                      AS securities_id,
            s.Securities_Name                    AS security_name,
            i.Date                               AS date,
            ROUND(AVG(i.Price_Per_Share)::numeric, 4) AS price,
            string_agg(DISTINCT i.Action::text, ', ' ORDER BY i.Action::text) AS actions,
            COUNT(*)                             AS tx_count
        FROM Investments i
        JOIN Securities s ON s.Securities_Id = i.Securities_Id
        WHERE i.Price_Per_Share > 0
          AND i.Action IN ('Buy','Sell','Reinvest','ShrIn','ShrOut')
          AND NOT EXISTS (
              SELECT 1 FROM Historical_Prices hp
              WHERE hp.Securities_Id = i.Securities_Id
                AND hp.Date          = i.Date
          )
        GROUP BY i.Securities_Id, s.Securities_Name, i.Date
        ORDER BY s.Securities_Name, i.Date
    """, conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def get_investments_with_dummy_prices() -> pd.DataFrame:
    """Return Investments rows where Quantity or Price_Per_Share looks like a placeholder.

    Detection: Price_Per_Share is a whole number (no decimal component) AND either
    a Historical_Price exists (buy side) or there are corresponding buys to match against
    (sell side).

    Preview columns:
      • Buys  — new_qty = Total_Amount / hist_price  (phase-1 formula)
      • Sells — new_qty = total_buy_qty × (sell_total / all_sell_total)  (phase-2 formula,
                so positions close correctly)

    Total_Amount is never touched.
    """
    conn = get_connection()
    df = pd.read_sql("""
        WITH
        -- Identify candidate rows (whole-number price → dummy placeholder)
        candidates AS (
            SELECT i.Investments_Id, i.Accounts_Id, i.Securities_Id,
                   i.Date, i.Action::text AS action,
                   i.Total_Amount, i.Quantity, i.Price_Per_Share
            FROM Investments i
            WHERE i.Action IN ('Buy','Sell','Reinvest','ShrIn','ShrOut')
              AND i.Price_Per_Share > 0
              AND i.Total_Amount    > 0
              AND (
                  i.Price_Per_Share = FLOOR(i.Price_Per_Share)
                  OR
                  (i.Quantity = FLOOR(i.Quantity)
                   AND EXISTS (
                       SELECT 1 FROM Historical_Prices hp2
                       WHERE hp2.Securities_Id = i.Securities_Id
                         AND hp2.Date          = i.Date
                         AND ABS(i.Price_Per_Share - hp2.Close) > 0.001
                   ))
              )
        ),
        -- Normalised buy totals per (account, security) — used to pin sell quantities
        buy_totals AS (
            SELECT c.Accounts_Id, c.Securities_Id,
                   SUM(ROUND((c.Total_Amount / NULLIF(hp.Close, 0))::numeric, 6)) AS total_buy_qty
            FROM candidates c
            JOIN Historical_Prices hp
                 ON hp.Securities_Id = c.Securities_Id AND hp.Date = c.Date
            WHERE c.action IN ('Buy','Reinvest','ShrIn')
            GROUP BY c.Accounts_Id, c.Securities_Id
        ),
        -- Use ABS so losing trades (negative Total_Amount) don't invert weights
        sell_totals AS (
            SELECT Accounts_Id, Securities_Id,
                   SUM(ABS(Total_Amount)) AS total_sell_amt_abs
            FROM candidates
            WHERE action IN ('Sell','ShrOut')
            GROUP BY Accounts_Id, Securities_Id
        )
        SELECT
            c.Investments_Id                        AS investments_id,
            c.Accounts_Id                           AS accounts_id,
            a.Accounts_Name                         AS account_name,
            c.Securities_Id                         AS securities_id,
            s.Securities_Name                       AS security_name,
            c.Date                                  AS date,
            c.action,
            c.Total_Amount                          AS total_amount,
            c.Quantity                              AS current_qty,
            c.Price_Per_Share                       AS current_price,
            hp.Close                                AS hist_price,
            CASE
                WHEN c.action IN ('Buy','Reinvest','ShrIn') THEN
                    ROUND((c.Total_Amount / NULLIF(hp.Close, 0))::numeric, 6)
                ELSE
                    -- sell: proportional share of total normalised buy qty;
                    -- ABS(Total_Amount) so losing trades (negative) stay positive
                    ROUND((bt.total_buy_qty
                           * (ABS(c.Total_Amount) / NULLIF(st.total_sell_amt_abs, 0)))::numeric, 6)
            END                                     AS new_qty,
            CASE
                WHEN c.action IN ('Buy','Reinvest','ShrIn') THEN hp.Close
                ELSE
                    ROUND((ABS(c.Total_Amount)
                           / NULLIF(bt.total_buy_qty
                                    * (ABS(c.Total_Amount) / NULLIF(st.total_sell_amt_abs, 0)), 0))::numeric, 4)
            END                                     AS new_price
        FROM candidates c
        JOIN Securities s ON s.Securities_Id = c.Securities_Id
        JOIN Accounts   a ON a.Accounts_Id   = c.Accounts_Id
        LEFT JOIN Historical_Prices hp
             ON hp.Securities_Id = c.Securities_Id AND hp.Date = c.Date
        LEFT JOIN buy_totals  bt ON bt.Accounts_Id  = c.Accounts_Id
                                AND bt.Securities_Id = c.Securities_Id
        LEFT JOIN sell_totals st ON st.Accounts_Id  = c.Accounts_Id
                                AND st.Securities_Id = c.Securities_Id
        -- Buys require a hist price; sells require a buy total to distribute against
        WHERE (c.action IN ('Buy','Reinvest','ShrIn') AND hp.Close IS NOT NULL)
           OR (c.action IN ('Sell','ShrOut')           AND bt.total_buy_qty IS NOT NULL)
        ORDER BY s.Securities_Name, c.Date
    """, conn)
    conn.close()
    return df


def get_all_securities_for_filter():
    """All securities for use in filter dropdowns."""
    conn = get_connection()
    df = pd.read_sql(
        "SELECT Securities_Id AS securities_id, Securities_Name AS securities_name "
        "FROM Securities ORDER BY Securities_Name",
        conn,
    )
    conn.close()
    return df


def get_nwr_account_selection(settings_key: str = 'nwr_account_ids'):
    """Load saved account selection from app_settings. Returns list of ints or None."""
    conn = get_connection()
    cur = conn.cursor()
    import json
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)
        """)
        conn.commit()
        cur.execute("SELECT value FROM app_settings WHERE key = %s", (settings_key,))
        row = cur.fetchone()
        return json.loads(row[0]) if row else None
    except Exception:
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()


@st.cache_data(ttl=3600)
def get_nwr_security_detail(start_date: str, interval: str, account_id: int):
    """Security-level historical values for a single investment account (for NWR drilldown)."""
    conn = get_connection()

    trunc_map = {'Year': 'year', 'Quarter': 'quarter', 'Month': 'month'}
    intv_map  = {'Year': '1 year', 'Quarter': '3 months', 'Month': '1 month'}
    trunc_unit  = trunc_map.get(interval, 'year')
    pg_interval = intv_map.get(interval, '1 year')
    acc_id = int(account_id)

    query = f"""
    WITH
    period_dates AS (
        SELECT (gs - INTERVAL '1 day')::date AS period_end
        FROM generate_series(
            date_trunc('{trunc_unit}', '{start_date}'::date) + '{pg_interval}'::interval,
            date_trunc('{trunc_unit}', CURRENT_DATE),
            '{pg_interval}'::interval
        ) gs
        UNION SELECT CURRENT_DATE::date
        ORDER BY 1
    ),
    daily_fx AS (
        SELECT p.period_end, cur.Currencies_Id,
            (SELECT FX_Rate FROM Historical_FX
             WHERE Currencies_Id_1 = cur.Currencies_Id AND Date <= p.period_end
             ORDER BY Date DESC LIMIT 1) AS fx_rate
        FROM period_dates p
        CROSS JOIN Currencies cur
        WHERE cur.Currencies_ShortName != 'EUR'
    ),
    sec_universe AS (
        SELECT DISTINCT Securities_Id
        FROM Investments
        WHERE Accounts_Id = {acc_id}
          AND Action IN ('Buy','Reinvest','ShrIn','Sell','ShrOut')
    )
    SELECT
        p.period_end,
        s.Securities_Name  AS security_name,
        c.Currencies_ShortName AS currency,
        GREATEST(COALESCE((
            SELECT SUM(CASE
                WHEN Action IN ('Buy','Reinvest','ShrIn') THEN  Quantity
                WHEN Action IN ('Sell','ShrOut')          THEN -Quantity
                ELSE 0 END)
            FROM Investments i2
            WHERE i2.Securities_Id = u.Securities_Id
              AND i2.Accounts_Id   = {acc_id}
              AND i2.Date          <= p.period_end
        ), 0), 0) AS qty_at_date,
        COALESCE((
            SELECT Close FROM Historical_Prices
            WHERE Securities_Id = u.Securities_Id AND Date <= p.period_end
            ORDER BY Date DESC LIMIT 1
        ), 0) AS price_at_date,
        GREATEST(COALESCE((
            SELECT SUM(CASE
                WHEN Action IN ('Buy','Reinvest','ShrIn') THEN  Quantity
                WHEN Action IN ('Sell','ShrOut')          THEN -Quantity
                ELSE 0 END)
            FROM Investments i2
            WHERE i2.Securities_Id = u.Securities_Id
              AND i2.Accounts_Id   = {acc_id}
              AND i2.Date          <= p.period_end
        ), 0), 0) *
        COALESCE((
            SELECT Close FROM Historical_Prices
            WHERE Securities_Id = u.Securities_Id AND Date <= p.period_end
            ORDER BY Date DESC LIMIT 1
        ), 0) *
        COALESCE(
            (SELECT fx_rate FROM daily_fx
             WHERE period_end = p.period_end AND Currencies_Id = s.Currencies_Id),
            1
        ) AS value_eur
    FROM period_dates p
    CROSS JOIN sec_universe u
    JOIN Securities s ON u.Securities_Id = s.Securities_Id
    JOIN Currencies c ON s.Currencies_Id = c.Currencies_Id
    ORDER BY p.period_end, value_eur DESC, s.Securities_Name
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=3600)
def get_portfolio_signals(selected_acc_id=None): # Προσθήκη '=' εδώ
    """Get signals for my investment portfolio."""
    conn = get_connection()
    
    # Χρησιμοποιούμε απλό τριπλό string (όχι f-string) για ασφάλεια με το pd.read_sql
    query = """
        WITH base_data AS (
            SELECT Securities_Id, Date, Close,
                   (Close / LAG(Close) OVER (PARTITION BY Securities_Id ORDER BY Date) - 1) as daily_ret
            FROM Historical_Prices
            WHERE Date >= (CURRENT_DATE - INTERVAL '62 months')
        ),
        ranked_prices AS (
            SELECT Securities_Id, Date, Close as price_today,
                   LAG(Close, 1) OVER (PARTITION BY Securities_Id ORDER BY Date) as price_1d,
                   LAG(Close, 5) OVER (PARTITION BY Securities_Id ORDER BY Date) as price_1w,
                   LAG(Close, 21) OVER (PARTITION BY Securities_Id ORDER BY Date) as price_1m,
                   LAG(Close, 63) OVER (PARTITION BY Securities_Id ORDER BY Date) as price_3m,
                   LAG(Close, 126) OVER (PARTITION BY Securities_Id ORDER BY Date) as price_6m,
                   LAG(Close, 252) OVER (PARTITION BY Securities_Id ORDER BY Date) as price_1y,
                   LAG(Close, 756) OVER (PARTITION BY Securities_Id ORDER BY Date) as price_3y,
                   ROW_NUMBER() OVER (PARTITION BY Securities_Id ORDER BY Date DESC) as rev_rank
            FROM base_data
        ),
        ytd_prices AS (
            SELECT DISTINCT ON (Securities_Id) Securities_Id, Close as price_ytd_start
            FROM Historical_Prices
            WHERE Date < date_trunc('year', CURRENT_DATE)
            ORDER BY Securities_Id, Date DESC
        ),
        latest_only AS (
            SELECT rp.*, yp.price_ytd_start 
            FROM ranked_prices rp
            LEFT JOIN ytd_prices yp ON rp.Securities_Id = yp.Securities_Id
            WHERE rp.rev_rank = 1
        ),
        performance_data AS (
            SELECT 
                lo.Securities_Id, Sec.Securities_Name, lo.price_today,
                ROUND(((lo.price_today / NULLIF(lo.price_1d, 0)) - 1) * 100, 2) as daily_chg_pct,
                ROUND(((lo.price_today / NULLIF(lo.price_1w, 0)) - 1) * 100, 2) as weekly_chg_pct,
                ROUND(((lo.price_today / NULLIF(lo.price_1m, 0)) - 1) * 100, 2) as monthly_chg_pct,
                ROUND(((lo.price_today / NULLIF(lo.price_3m, 0)) - 1) * 100, 2) as quarterly_chg_pct,
                ROUND(((lo.price_today / NULLIF(lo.price_6m, 0)) - 1) * 100, 2) as semiannual_chg_pct,
                ROUND(((lo.price_today / NULLIF(lo.price_1y, 0)) - 1) * 100, 2) as annual_chg_pct,
                ROUND(((lo.price_today / NULLIF(lo.price_3y, 0)) - 1) * 100, 2) as triannual_chg_pct,
                ROUND(((lo.price_today / NULLIF(lo.price_ytd_start, 0)) - 1) * 100, 2) as ytd_chg_pct,
                (SELECT ROUND((STDDEV(daily_ret) * SQRT(252) * 100)::numeric, 2) FROM base_data bd WHERE bd.Securities_Id = lo.Securities_Id AND bd.Date > (lo.Date - INTERVAL '1 month')) as vol_1m_ann,
                (SELECT ROUND((STDDEV(daily_ret) * SQRT(252) * 100)::numeric, 2) FROM base_data bd WHERE bd.Securities_Id = lo.Securities_Id AND bd.Date > (lo.Date - INTERVAL '3 months')) as vol_3m_ann,
                (SELECT ROUND((STDDEV(daily_ret) * SQRT(252) * 100)::numeric, 2) FROM base_data bd WHERE bd.Securities_Id = lo.Securities_Id AND bd.Date > (lo.Date - INTERVAL '12 months')) as vol_1y_ann,
                (SELECT ROUND((STDDEV(daily_ret) * SQRT(252) * 100)::numeric, 2) FROM base_data bd WHERE bd.Securities_Id = lo.Securities_Id AND bd.Date >= date_trunc('year', CURRENT_DATE)) as vol_ytd_ann
            FROM latest_only lo
            JOIN Securities Sec ON lo.Securities_Id = Sec.Securities_Id
            AND Sec.Is_Active
            AND lo.Date > (CURRENT_DATE - INTERVAL '15 days')
        ),
        rfr AS (
            SELECT COALESCE(annual_chg_pct, 2.36) as value 
            FROM performance_data 
            WHERE Securities_Name LIKE 'Hellenic T-Bill 52W%%'
            LIMIT 1
        ),
        investment_signals AS (
            SELECT *,
                ROUND(((monthly_chg_pct * 0.5) + (quarterly_chg_pct * 0.3) + (annual_chg_pct * 0.2))::numeric, 2) as quality_score,
                ROUND(((annual_chg_pct - (SELECT value FROM rfr)) / NULLIF(vol_1y_ann, 0))::numeric, 2) as sharpe_ratio
            FROM performance_data
            WHERE vol_1y_ann > 0 
        ),
        portfolio_status AS (
            SELECT sig.*,
                SUM(COALESCE(h.Quantity, 0)) as current_qty,
                (SUM(COALESCE(h.Quantity, 0)) * sig.price_today) as market_value_base_curr
            FROM investment_signals sig
            LEFT JOIN Holdings h ON sig.Securities_Id = h.Securities_Id
                AND (%s IS NULL OR h.Accounts_Id = %s) 
			GROUP BY sig.Securities_Id, sig.Securities_Name, sig.price_today, sig.daily_chg_pct, sig.weekly_chg_pct, 
                     sig.monthly_chg_pct, sig.quarterly_chg_pct, sig.semiannual_chg_pct, sig.annual_chg_pct, 
                     sig.triannual_chg_pct, sig.ytd_chg_pct, sig.vol_1m_ann, sig.vol_3m_ann, sig.vol_1y_ann, 
                     sig.vol_ytd_ann, sig.quality_score, sig.sharpe_ratio
        ),
        recommendations AS (
            SELECT 
                sig.*, 
                sig.market_value_base_curr * COALESCE((SELECT FX_Rate FROM Historical_FX WHERE Currencies_Id_1 = sec.Currencies_Id AND Date <= CURRENT_DATE ORDER BY Date DESC LIMIT 1), 1) as current_value_eur,
                sec.Analyst_Rating as wall_street_view,
                sec.Analyst_Target_Price as target_price,
                ROUND((((sec.Analyst_Target_Price / NULLIF(sig.price_today, 0)) - 1) * 100)::numeric, 2) as upside_pct,
                CASE 
                    WHEN current_qty > 0 AND (sharpe_ratio < 0 OR quality_score < -5) THEN '🔴 SELL / REDUCE'
                    WHEN sharpe_ratio > 1.2 AND quality_score > 10 THEN '🟢 STRONG BUY'
                    WHEN sharpe_ratio > 0.7 OR quality_score > 8 THEN '🟢 BUY'
                    WHEN sharpe_ratio > 0.3 OR quality_score > 0 THEN '🟡 HOLD'
                    WHEN current_qty = 0 AND sharpe_ratio > 0.5 THEN '👀 WATCHLIST'
                    ELSE '⚪ NEUTRAL'
                END as recommendation_signal
            FROM portfolio_status sig
            JOIN Securities sec ON sig.Securities_Id = sec.Securities_Id
        )
        SELECT *,
            CASE 
                WHEN recommendation_signal LIKE '🟢%%' AND wall_street_view IN ('buy', 'strong_buy') AND upside_pct > 20 THEN '🔥 HIGH CONVICTION BUY'
                WHEN recommendation_signal LIKE '🟢%%' AND wall_street_view = 'strong_buy' THEN '💎 STRONG CONVICTION'
                WHEN recommendation_signal LIKE '🟢%%' AND wall_street_view = 'buy' THEN '💎 CONVICTION BUY'
                WHEN recommendation_signal LIKE '🟢%%' AND wall_street_view IN ('sell', 'underperform') THEN '🔍 CONTRARIAN BUY'
                WHEN recommendation_signal LIKE '🔴%%' AND wall_street_view IN ('buy', 'strong_buy') THEN '🔍 CONTRARIAN SELL'
                WHEN recommendation_signal LIKE '🟢%%' AND wall_street_view = 'hold' THEN '🚀 MOMENTUM BUY'
                WHEN recommendation_signal LIKE '🔴%%' AND wall_street_view = 'hold' THEN '📉 MOMENTUM SELL'
                WHEN recommendation_signal LIKE '🟢%%' AND (wall_street_view IS NULL OR wall_street_view = 'none') THEN '⚙️ ALGO BUY'
                WHEN recommendation_signal LIKE '🔴%%' AND (wall_street_view IS NULL OR wall_street_view = 'none') THEN '⚙️ ALGO SELL'
                WHEN recommendation_signal LIKE '🔴%%' AND wall_street_view IN ('sell', 'underperform') THEN '⚠️ CONVICTION SELL'
                ELSE recommendation_signal 
            END as final_signal,
			(SELECT Date FROM Historical_Prices WHERE Securities_Id = recommendations.Securities_Id AND Date <= CURRENT_DATE ORDER BY Date DESC LIMIT 1) as price_today_date
        FROM recommendations
        ORDER BY sharpe_ratio DESC;
    """
    
    # Το pandas.read_sql χειρίζεται σωστά τις παραμέτρους για την αποφυγή SQL Injection
    df = pd.read_sql(query, conn, params=(selected_acc_id, selected_acc_id))
    cur = conn.cursor()
    cur.close()
    conn.close()
    return df



@st.cache_data(ttl=3600)
def get_pnl_report_data(start_date: str = '1900-01-01', end_date: str = None):
    """Get P&L report data.

    Args:
        start_date: Beginning of the 'all-time' window (ISO date string, default '1900-01-01').
        end_date:   Reference date treated as 'today' (ISO date string, default: today).
    """
    from datetime import date as _date
    if end_date is None:
        end_date = _date.today().isoformat()

    conn = get_connection()

    query = f"""
    WITH DateRange AS (
        SELECT '{start_date}'::date AS start_date, '{end_date}'::date AS end_date
    ),
        periods AS (
            SELECT 
                (date_trunc('day', end_date) - INTERVAL '1 day')::date as dtd_start,
                (date_trunc('week', end_date) - INTERVAL '1 day')::date as wtd_start,
                (date_trunc('month', end_date) - INTERVAL '1 day')::date as mtd_start,
                (date_trunc('quarter', end_date) - INTERVAL '1 day')::date as qtd_start,
                (date_trunc('year', end_date) - INTERVAL '1 day')::date as ytd_start,
            --    '1900-01-01'::date as all_time_start,
                start_date as all_time_start,
            --    CURRENT_DATE::date as today
                end_date as today
            FROM DateRange
        ),
        -- Βρίσκουμε κάθε συνδυασμό Λογαριασμού/Τίτλου που υπήρξε ποτέ
        historical_entities AS (
            SELECT Accounts_Id, Securities_Id FROM Holdings
            UNION
            SELECT Accounts_Id, Securities_Id FROM Investments
        ),
        historical_holdings AS (
            SELECT
                p.*,
                he.Accounts_Id, he.Securities_Id,
                COALESCE(inv.qty_today, 0) as qty_today,
                COALESCE(inv.qty_dtd,   0) as qty_dtd,
                COALESCE(inv.qty_wtd,   0) as qty_wtd,
                COALESCE(inv.qty_mtd,   0) as qty_mtd,
                COALESCE(inv.qty_qtd,   0) as qty_qtd,
                COALESCE(inv.qty_ytd,   0) as qty_ytd
            FROM periods p
            CROSS JOIN historical_entities he
            -- Single scan per (Account, Security) instead of 6 separate correlated subqueries
            LEFT JOIN LATERAL (
                SELECT
                    SUM(CASE WHEN Action IN ('Buy','Reinvest','ShrIn') THEN Quantity
                             WHEN Action IN ('Sell','ShrOut') THEN -Quantity ELSE 0 END)
                        FILTER (WHERE Date <= p.today)     AS qty_today,
                    SUM(CASE WHEN Action IN ('Buy','Reinvest','ShrIn') THEN Quantity
                             WHEN Action IN ('Sell','ShrOut') THEN -Quantity ELSE 0 END)
                        FILTER (WHERE Date <= p.dtd_start) AS qty_dtd,
                    SUM(CASE WHEN Action IN ('Buy','Reinvest','ShrIn') THEN Quantity
                             WHEN Action IN ('Sell','ShrOut') THEN -Quantity ELSE 0 END)
                        FILTER (WHERE Date <= p.wtd_start) AS qty_wtd,
                    SUM(CASE WHEN Action IN ('Buy','Reinvest','ShrIn') THEN Quantity
                             WHEN Action IN ('Sell','ShrOut') THEN -Quantity ELSE 0 END)
                        FILTER (WHERE Date <= p.mtd_start) AS qty_mtd,
                    SUM(CASE WHEN Action IN ('Buy','Reinvest','ShrIn') THEN Quantity
                             WHEN Action IN ('Sell','ShrOut') THEN -Quantity ELSE 0 END)
                        FILTER (WHERE Date <= p.qtd_start) AS qty_qtd,
                    SUM(CASE WHEN Action IN ('Buy','Reinvest','ShrIn') THEN Quantity
                             WHEN Action IN ('Sell','ShrOut') THEN -Quantity ELSE 0 END)
                        FILTER (WHERE Date <= p.ytd_start) AS qty_ytd
                FROM Investments
                WHERE Accounts_Id = he.Accounts_Id AND Securities_Id = he.Securities_Id
            ) inv ON true
        ),
        prices_fx AS (
            SELECT
                hh.*,
                hp_today.Close  AS price_today,
                hp_dtd.Close    AS price_dtd,
                hp_wtd.Close    AS price_wtd,
                hp_mtd.Close    AS price_mtd,
                hp_qtd.Close    AS price_qtd,
                hp_ytd.Close    AS price_ytd,
                fx_today.FX_Rate AS fx_today,
                fx_dtd.FX_Rate   AS fx_dtd,
                fx_wtd.FX_Rate   AS fx_wtd,
                fx_mtd.FX_Rate   AS fx_mtd,
                fx_qtd.FX_Rate   AS fx_qtd,
                fx_ytd.FX_Rate   AS fx_ytd,
                s.Securities_Name, a.Accounts_Name, s.Currencies_Id AS sec_curr_id
            FROM historical_holdings hh
            JOIN Securities s ON hh.Securities_Id = s.Securities_Id
            JOIN Accounts   a ON hh.Accounts_Id   = a.Accounts_Id
            -- One scan per security to find latest price date for each period cutoff
            LEFT JOIN LATERAL (
                SELECT
                    MAX(Date) FILTER (WHERE Date <= hh.today)      AS d_today,
                    MAX(Date) FILTER (WHERE Date <= hh.dtd_start)  AS d_dtd,
                    MAX(Date) FILTER (WHERE Date <= hh.wtd_start)  AS d_wtd,
                    MAX(Date) FILTER (WHERE Date <= hh.mtd_start)  AS d_mtd,
                    MAX(Date) FILTER (WHERE Date <= hh.qtd_start)  AS d_qtd,
                    MAX(Date) FILTER (WHERE Date <= hh.ytd_start)  AS d_ytd
                FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id
            ) pd ON true
            LEFT JOIN Historical_Prices hp_today ON hp_today.Securities_Id = hh.Securities_Id AND hp_today.Date = pd.d_today
            LEFT JOIN Historical_Prices hp_dtd   ON hp_dtd.Securities_Id   = hh.Securities_Id AND hp_dtd.Date   = pd.d_dtd
            LEFT JOIN Historical_Prices hp_wtd   ON hp_wtd.Securities_Id   = hh.Securities_Id AND hp_wtd.Date   = pd.d_wtd
            LEFT JOIN Historical_Prices hp_mtd   ON hp_mtd.Securities_Id   = hh.Securities_Id AND hp_mtd.Date   = pd.d_mtd
            LEFT JOIN Historical_Prices hp_qtd   ON hp_qtd.Securities_Id   = hh.Securities_Id AND hp_qtd.Date   = pd.d_qtd
            LEFT JOIN Historical_Prices hp_ytd   ON hp_ytd.Securities_Id   = hh.Securities_Id AND hp_ytd.Date   = pd.d_ytd
            -- One scan per currency to find latest FX date for each period cutoff
            LEFT JOIN LATERAL (
                SELECT
                    MAX(Date) FILTER (WHERE Date <= hh.today)      AS d_today,
                    MAX(Date) FILTER (WHERE Date <= hh.dtd_start)  AS d_dtd,
                    MAX(Date) FILTER (WHERE Date <= hh.wtd_start)  AS d_wtd,
                    MAX(Date) FILTER (WHERE Date <= hh.mtd_start)  AS d_mtd,
                    MAX(Date) FILTER (WHERE Date <= hh.qtd_start)  AS d_qtd,
                    MAX(Date) FILTER (WHERE Date <= hh.ytd_start)  AS d_ytd
                FROM Historical_FX WHERE Currencies_Id_1 = s.Currencies_Id
            ) fxd ON true
            LEFT JOIN Historical_FX fx_today ON fx_today.Currencies_Id_1 = s.Currencies_Id AND fx_today.Date = fxd.d_today
            LEFT JOIN Historical_FX fx_dtd   ON fx_dtd.Currencies_Id_1   = s.Currencies_Id AND fx_dtd.Date   = fxd.d_dtd
            LEFT JOIN Historical_FX fx_wtd   ON fx_wtd.Currencies_Id_1   = s.Currencies_Id AND fx_wtd.Date   = fxd.d_wtd
            LEFT JOIN Historical_FX fx_mtd   ON fx_mtd.Currencies_Id_1   = s.Currencies_Id AND fx_mtd.Date   = fxd.d_mtd
            LEFT JOIN Historical_FX fx_qtd   ON fx_qtd.Currencies_Id_1   = s.Currencies_Id AND fx_qtd.Date   = fxd.d_qtd
            LEFT JOIN Historical_FX fx_ytd   ON fx_ytd.Currencies_Id_1   = s.Currencies_Id AND fx_ytd.Date   = fxd.d_ytd
        ),
        cash_flows AS (
            SELECT
                i.Accounts_Id, i.Securities_Id,
                -- DTD CF
                SUM(CASE WHEN i.Date > (SELECT dtd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          ELSE 0 END) ELSE 0 END) AS cf_dtd,
                -- DTD CF EUR
                SUM(CASE WHEN i.Date > (SELECT dtd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          ELSE 0 END) ELSE 0 END) AS cf_dtd_eur,
                -- WTD CF
                SUM(CASE WHEN i.Date > (SELECT wtd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          ELSE 0 END) ELSE 0 END) AS cf_wtd,
                -- WTD CF EUR
                SUM(CASE WHEN i.Date > (SELECT wtd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          ELSE 0 END) ELSE 0 END) AS cf_wtd_eur,                      
                -- MTD CF
                SUM(CASE WHEN i.Date > (SELECT mtd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          ELSE 0 END) ELSE 0 END) AS cf_mtd,
                -- MTD CF EUR
                SUM(CASE WHEN i.Date > (SELECT mtd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          ELSE 0 END) ELSE 0 END) AS cf_mtd_eur,
                -- QTD CF
                SUM(CASE WHEN i.Date > (SELECT qtd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          ELSE 0 END) ELSE 0 END) AS cf_qtd,
                -- QTD CF EUR
                SUM(CASE WHEN i.Date > (SELECT qtd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          ELSE 0 END) ELSE 0 END) AS cf_qtd_eur,
                -- YTD CF
                SUM(CASE WHEN i.Date > (SELECT ytd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                          ELSE 0 END) ELSE 0 END) AS cf_ytd,
                    -- YTD CF EUR
                SUM(CASE WHEN i.Date > (SELECT ytd_start FROM periods) THEN
                    (CASE WHEN i.Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                          ELSE 0 END) ELSE 0 END) AS cf_ytd_eur,
                -- net_invested_ytd_eur: JOIN replaces per-row correlated subqueries
                SUM(CASE WHEN i.Date > (SELECT ytd_start FROM periods) THEN
                    CASE WHEN i.Action IN ('Buy', 'CashOut', 'MiscExp')
                            THEN i.Total_Amount * COALESCE(hfx.FX_Rate, 1)
                         WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'CashIn', 'RtrnCap')
                            THEN -i.Total_Amount * COALESCE(hfx.FX_Rate, 1)
                         ELSE 0 END
                ELSE 0 END) AS net_invested_ytd_eur,
                -- Συνολικό CF (για Realized P&L)
                SUM(CASE WHEN i.Action IN ('Buy', 'MiscExp', 'Reinvest', 'Exercise', 'ShrIn') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                         WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'RtrnCap', 'ShrOut') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                         ELSE 0 END) AS cf_all_time,
                -- Συνολικό CF (για Realized P&L)
                SUM(CASE WHEN i.Action IN ('Buy', 'MiscExp', 'Reinvest', 'Exercise', 'ShrIn') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                         WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'RtrnCap', 'ShrOut') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share) * COALESCE(hfx.FX_Rate, 1)
                         ELSE 0 END) AS cf_all_time_eur,
                -- net_invested_all_time_eur: JOIN replaces per-row correlated subqueries
                SUM(CASE WHEN i.Action IN ('Buy', 'CashOut', 'MiscExp')
                            THEN i.Total_Amount * COALESCE(hfx.FX_Rate, 1)
                         WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'CashIn', 'RtrnCap')
                            THEN -i.Total_Amount * COALESCE(hfx.FX_Rate, 1)
                         ELSE 0 END) AS net_invested_all_time_eur,
                -- gross_invested_all_time_eur: total cost of all buys (correct denominator for % return)
                -- Using net_invested as denominator is wrong for closed/profitable positions because
                -- proceeds from sells make it negative, flipping the sign of the percentage.
                SUM(CASE WHEN i.Action IN ('Buy', 'CashOut', 'MiscExp')
                            THEN i.Total_Amount * COALESCE(hfx.FX_Rate, 1)
                         ELSE 0 END) AS gross_invested_all_time_eur
            FROM Investments i
            JOIN Accounts a ON i.Accounts_Id = a.Accounts_Id
            LEFT JOIN Historical_FX hfx
                   ON hfx.Currencies_Id_1 = a.Currencies_Id
                  AND hfx.Date = i.Date
            GROUP BY i.Accounts_Id, i.Securities_Id
        ),
        dividend_yoc AS (
            SELECT
                i.Securities_Id, i.Accounts_Id,
                SUM(
                    CASE
                        WHEN i.Action = 'Dividend' THEN i.Total_Amount
                        WHEN i.Action IN ('Reinvest', 'ShrIn') THEN
                            i.Quantity * COALESCE(
                                NULLIF(i.Price_Per_Share, 0),
                                (SELECT hp.Close FROM Historical_Prices hp
                                 WHERE hp.Securities_Id = i.Securities_Id
                                   AND hp.Date <= i.Date
                                 ORDER BY hp.Date DESC LIMIT 1)
                            )
                        ELSE 0
                    END
                ) AS annual_income
            FROM Investments i
            WHERE i.Action IN ('Dividend', 'Reinvest', 'ShrIn')
              AND i.Date >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY i.Securities_Id, i.Accounts_Id
        ),
        -- Account-level capital inflows — used as the ROI denominator.
        -- Priority 1: explicit CashIn (Action='CashIn', Securities_Id IS NULL) recorded
        --             directly in the Investments table (e.g. pension contributions, or
        --             deposits recorded before a Buy).
        account_direct_flows AS (
            SELECT
                i.Accounts_Id,
                SUM(CASE WHEN i.Action = 'CashIn'
                         THEN i.Total_Amount * COALESCE(hfx.FX_Rate, 1) ELSE 0 END) AS direct_cashin_eur
            FROM Investments i
            JOIN Accounts a ON i.Accounts_Id = a.Accounts_Id
            LEFT JOIN Historical_FX hfx
                   ON hfx.Currencies_Id_1 = a.Currencies_Id
                  AND hfx.Date = i.Date
            WHERE i.Securities_Id IS NULL   -- CashIn/CashOut never have a security
            GROUP BY i.Accounts_Id
        ),
        -- Priority 2: explicit cash transfers recorded in the LINKED cash account
        --             (Transactions.Accounts_Id_Target = investment_account).
        --             Buy/Sell-linked transactions use transactions_id and have
        --             Accounts_Id_Target = NULL, so they are NOT included here.
        account_linked_flows AS (
            SELECT
                a.Accounts_Id AS inv_acc_id,
                SUM(-t.Total_Amount * COALESCE(fxl.FX_Rate, 1)) AS linked_cashin_eur
            FROM Accounts a
            INNER JOIN Accounts al ON al.Accounts_Id = a.Accounts_Id_Linked
            INNER JOIN Transactions t
                    ON t.Accounts_Id       = al.Accounts_Id
                   AND t.Accounts_Id_Target = a.Accounts_Id
                   AND t.Total_Amount < 0   -- negative = cash leaving the linked account (deposit into investment)
            LEFT JOIN Historical_FX fxl
                   ON fxl.Currencies_Id_1 = al.Currencies_Id
                  AND fxl.Date = t.Date
            GROUP BY a.Accounts_Id
        )
        SELECT
            pf.Accounts_Name, pf.Securities_Name,

            (pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) as current_value_eur,
            
            -- DTD Analysis
            -- 1. P&L λόγω μεταβολής Τιμής (Market Effect YTD)
            ((pf.qty_today * pf.price_today) - (pf.qty_dtd * pf.price_dtd) - COALESCE(cf.cf_dtd, 0)) * COALESCE(pf.fx_today, 1) as pnl_dtd_market_eur,
            -- 2. P&L λόγω μεταβολής Ισοτιμίας (FX Effect YTD)
            (pf.qty_dtd * pf.price_dtd) * (COALESCE(pf.fx_today, 1) - COALESCE(pf.fx_dtd, 1)) as pnl_dtd_fx_eur,
            -- 3. Total P&L DTD		
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_dtd * pf.price_dtd * COALESCE(pf.fx_dtd, 1)) - COALESCE(cf.cf_dtd_eur, 0)) as pnl_dtd_eur,
            -- DTD P&L %
            CASE WHEN (pf.qty_dtd * pf.price_dtd * COALESCE(pf.fx_dtd, 1)) = 0 THEN 0
                 ELSE (((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_dtd * pf.price_dtd * COALESCE(pf.fx_dtd, 1)) - COALESCE(cf.cf_dtd_eur, 0)) / (pf.qty_dtd * pf.price_dtd * COALESCE(pf.fx_dtd, 1))) * 100
            END as pnl_dtd_percent,

            -- WTD/MTD/QTD Analysis
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_wtd * pf.price_wtd * COALESCE(pf.fx_wtd, 1)) - COALESCE(cf.cf_wtd_eur, 0)) as pnl_wtd_eur,
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_mtd * pf.price_mtd * COALESCE(pf.fx_mtd, 1)) - COALESCE(cf.cf_mtd_eur, 0)) as pnl_mtd_eur,
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_qtd * pf.price_qtd * COALESCE(pf.fx_qtd, 1)) - COALESCE(cf.cf_qtd_eur, 0)) as pnl_qtd_eur,
            
            -- YTD Analysis
            -- 1. P&L λόγω μεταβολής Τιμής (Market Effect YTD)
			(CASE WHEN pf.qty_today = 0 THEN COALESCE((pf.qty_today * pf.price_today), 0) - COALESCE((pf.qty_ytd * pf.price_ytd),0) -- this is required for positions closed within the year and by today
				 ELSE COALESCE((pf.qty_today * pf.price_today), 0) - COALESCE((pf.qty_ytd * pf.price_ytd),0) -- this is required for outstanding positions
			END   - COALESCE(cf.cf_ytd_eur, 0) --/ COALESCE(pf.fx_today, 1) -- this is required for positions closed within the year and by today
			) * COALESCE(pf.fx_today, 1) as pnl_ytd_market_eur,
            -- 2. P&L λόγω μεταβολής Ισοτιμίας (FX Effect YTD)
			CASE WHEN pf.qty_today = 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)),0) -- this is required for positions closed within the year and by today
				 ELSE COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)),0) -- this is required for outstanding positions
			END   - COALESCE(cf.cf_ytd_eur, 0) --* COALESCE(pf.fx_today, 1) -- this is required for positions closed within the year and by today
			-
			(CASE WHEN pf.qty_today = 0 THEN COALESCE((pf.qty_today * pf.price_today), 0) - COALESCE((pf.qty_ytd * pf.price_ytd),0) -- this is required for positions closed within the year and by today
				 ELSE COALESCE((pf.qty_today * pf.price_today), 0) - COALESCE((pf.qty_ytd * pf.price_ytd),0) -- this is required for outstanding positions
			END   - COALESCE(cf.cf_ytd_eur, 0) --/ COALESCE(pf.fx_today, 1) -- this is required for positions closed within the year and by today
			) * COALESCE(pf.fx_today, 1)			
			as pnl_ytd_fx_eur,
			
            -- Total YTD P&L
			CASE WHEN pf.qty_today = 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)),0) -- this is required for positions closed within the year and by today
				 ELSE COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)),0) -- this is required for outstanding positions
			END   - COALESCE(cf.cf_ytd_eur, 0) -- this is required for positions closed within the year and by today
			as pnl_ytd_eur,	 
            -- Total YTD P&L %
            CASE WHEN COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)), 0) = 0 THEN 0
                 ELSE (((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)),0) - COALESCE(cf.cf_ytd_eur, 0)) / COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)), 1)) * 100
            END as pnl_ytd_percent,

            -- YTD UNREALIZED P&L
			CASE WHEN pf.qty_today <> 0 AND pf.qty_ytd = 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE(cf.cf_ytd_eur, 0)  
				 WHEN pf.qty_today <> 0 AND pf.qty_ytd <> 0 AND pf.qty_today >= pf.qty_ytd AND COALESCE(cf.net_invested_ytd_eur, 0) >= 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)), 0) - COALESCE(cf.net_invested_ytd_eur, 0)
				 WHEN pf.qty_today <> 0 AND pf.qty_ytd <> 0 AND pf.qty_today >= pf.qty_ytd AND COALESCE(cf.net_invested_ytd_eur, 0) < 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)), 0) 
			 ELSE 0
			END	AS unrealized_pnl_ytd_eur,

            -- YTD REALIZED P&L
			CASE WHEN pf.qty_today = 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)),0) -- this is required for positions closed within the year and by today
				 ELSE COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)),0) -- this is required for outstanding positions
			END   - COALESCE(cf.cf_ytd_eur, 0)  -- this is required for positions closed within the year and by today
			-
			CASE WHEN pf.qty_today <> 0 AND pf.qty_ytd = 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE(cf.cf_ytd, 0) * COALESCE(pf.fx_today, 1) 
				 WHEN pf.qty_today <> 0 AND pf.qty_ytd <> 0 AND pf.qty_today >= pf.qty_ytd AND COALESCE(cf.net_invested_ytd_eur, 0) >= 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)), 0) - COALESCE(cf.net_invested_ytd_eur, 0)
				 WHEN pf.qty_today <> 0 AND pf.qty_ytd <> 0 AND pf.qty_today >= pf.qty_ytd AND COALESCE(cf.net_invested_ytd_eur, 0) < 0 THEN COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE((pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)), 0) 
			 ELSE 0
			END realized_pnl_ytd_eur,

            -- All Time P&L - ERROR - Use the NET instead
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - COALESCE(cf.cf_all_time_eur, 0)) as pnl_all_time_eur,

            -- Συνολικό P&L από την αρχή (Total Net Economic Gain)
            COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)),0) - COALESCE(cf.net_invested_all_time_eur, 0) as pnl_net_all_time_eur,
            -- Total Net Economic Gain %
            -- Denominator is gross_invested (total cost of buys), NOT net_invested.
            -- net_invested goes negative for profitable closed positions (proceeds > cost),
            -- which would flip the sign.  gross_invested is always >= 0.
            CASE WHEN COALESCE(cf.gross_invested_all_time_eur, 0) = 0 THEN 0
                 ELSE (COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)),0) - COALESCE(cf.net_invested_all_time_eur, 0))
                      / cf.gross_invested_all_time_eur * 100
            END as pnl_net_all_time_percent,
            COALESCE(cf.gross_invested_all_time_eur, 0) as gross_invested_all_time_eur,
            -- Capital-inflow columns for account-level ROI denominator (same value on every
            -- row for an account; Python picks it up with groupby 'first').
            COALESCE(adf.direct_cashin_eur, 0) AS direct_cashin_eur,
            COALESCE(alf.linked_cashin_eur, 0) AS linked_cashin_eur,

            -- Unrealized P&L (FIFO based)
            h.Quantity * (pf.price_today - h.Fifo_Avg_Price) * COALESCE(pf.fx_today, 1) AS unrealized_pnl_eur,

            -- Realized P&L (Total - Unrealized)
            COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)), 0) - COALESCE(cf.net_invested_all_time_eur, 0)
            - COALESCE(h.Quantity * (pf.price_today - h.Fifo_Avg_Price) * COALESCE(pf.fx_today, 1), 0) AS realized_pnl_eur,

            -- Annual YOC %
            ROUND(
                dy.annual_income / NULLIF(h.Quantity * h.Fifo_Avg_Price, 0) * 100,
                8
            ) AS dividend_yoc_pct

        FROM prices_fx pf
        LEFT JOIN cash_flows cf
               ON pf.Accounts_Id = cf.Accounts_Id AND pf.Securities_Id = cf.Securities_Id
        LEFT JOIN Holdings h
               ON h.Accounts_Id = pf.Accounts_Id AND h.Securities_Id = pf.Securities_Id
        LEFT JOIN dividend_yoc dy
               ON dy.Accounts_Id = pf.Accounts_Id AND dy.Securities_Id = pf.Securities_Id
        LEFT JOIN account_direct_flows adf ON adf.Accounts_Id    = pf.Accounts_Id
        LEFT JOIN account_linked_flows alf ON alf.inv_acc_id = pf.Accounts_Id
        WHERE (pf.qty_today != 0 OR cf.cf_all_time IS NOT NULL) -- Εξασφαλίζει ότι βλέπουμε και κλειστές θέσεις με ιστορικό
    --  AND pf.Accounts_Id = 89
        ORDER BY pf.Accounts_Name, pf.Securities_Name;
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=3600)
def get_income_expense_data(start_date, end_date, category_id=None, cash_account_types=None, inv_account_types=None):
    """Get income and expense data for a period, optionally filtered by category.
    Includes both bank transactions, investment transactions (dividends, interest, etc.),
    and realized P&L from securities sales calculated using FIFO method with FX conversion.
    All amounts are converted to EUR using historical FX rates.
    """
    conn = get_connection()

    # Μετατρέπουμε τη λίστα σε tuple για την SQL (π.χ. ('Brokerage', 'Pension'))
    if not cash_account_types:
        cash_account_types = ['Cash', 'Checking', 'Savings', 'Credit Card', 'Loan', 'Real Estate', 'Vehicle', 'Asset', 'Liability', 'Other']
    
    cash_account_types_tuple = tuple(cash_account_types)

    if not inv_account_types:
        inv_account_types = ['Brokerage', 'Other Investment', 'Margin']
    
    inv_account_types_tuple = tuple(inv_account_types)


    # First, create the CategoryHierarchy CTE
    category_hierarchy_cte = """
    WITH RECURSIVE CategoryHierarchy AS (
        SELECT 
            Categories_Id, 
            Categories_Name::TEXT as Full_Path,
            Categories_Name::TEXT as Name,
            Categories_Type::TEXT as Categories_Type,
            Categories_Id_Parent,
            0 as Level
        FROM Categories 
        WHERE Categories_Id_Parent IS NULL
        
        UNION ALL
        
        SELECT 
            c.Categories_Id, 
            ch.Full_Path || ' : ' || c.Categories_Name as Full_Path,
            c.Categories_Name as Name,
            c.Categories_Type::TEXT as Categories_Type,
            c.Categories_Id_Parent,
            ch.Level + 1 as Level
        FROM Categories c
        JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
    )
    """
    
    # Build the complete query with UNION of Bank Transactions, Investment Transactions, and Realized P&L
    base_query = f"""
    {category_hierarchy_cte},
    DateRange AS (
        SELECT %s::date as start_date, %s::date as end_date
    ),
    -- Get FX rates for currency conversion
    FX_Rates AS (
        SELECT DISTINCT ON (currencies_id_1)
            hf.currencies_id_1,
            hf.currencies_id_2,
            hf.fx_rate,
            hf.date,
            c.currencies_shortname as base_currency
        FROM Historical_FX hf
        JOIN Currencies c ON hf.currencies_id_1 = c.currencies_id
        WHERE hf.currencies_id_2 = (SELECT currencies_id FROM Currencies WHERE currencies_shortname = 'EUR')
          AND hf.date <= (SELECT end_date FROM DateRange)
        ORDER BY currencies_id_1, hf.date DESC
    ),
    -- Default FX rates for each currency
    DefaultFX_Rates AS (
        SELECT DISTINCT ON (c.currencies_id)
            c.currencies_id as currencies_id_1,
            c.currencies_shortname as base_currency,
            COALESCE(hf.fx_rate, 1) as fx_rate
        FROM Currencies c
        LEFT JOIN Historical_FX hf ON c.currencies_id = hf.currencies_id_1 
            AND hf.currencies_id_2 = (SELECT currencies_id FROM Currencies WHERE currencies_shortname = 'EUR')
            AND hf.date <= (SELECT end_date FROM DateRange)
        WHERE c.currencies_shortname != 'EUR'
        ORDER BY c.currencies_id, hf.date DESC
    ),
    -- Calculate Realized P&L using FIFO method
    FIFO_Inventory AS (
        SELECT 
            t.Accounts_Id,
            t.Securities_Id,
            t.Date,
            t.Investments_Id,
            t.Action,
            t.Quantity,
            t.Price_Per_Share,
            t.Total_Amount,
            t.Commission,
            -- Running total of shares bought
            SUM(CASE WHEN t.Action IN ('Buy', 'Reinvest', 'ShrIn') THEN t.Quantity ELSE 0 END) 
                OVER (PARTITION BY t.Accounts_Id, t.Securities_Id ORDER BY t.Date, t.Investments_Id) as running_bought,
            -- Running total of shares sold
            SUM(CASE WHEN t.Action IN ('Sell', 'ShrOut') THEN t.Quantity ELSE 0 END) 
                OVER (PARTITION BY t.Accounts_Id, t.Securities_Id ORDER BY t.Date, t.Investments_Id) as running_sold,
            -- Total bought for FIFO calculation
            SUM(CASE WHEN t.Action IN ('Buy', 'Reinvest', 'ShrIn') THEN t.Quantity ELSE 0 END) 
                OVER (PARTITION BY t.Accounts_Id, t.Securities_Id) as total_bought,
            -- Get the security currency
            s.Currencies_Id as security_currency_id,
            cur_s.Currencies_ShortName as security_currency
        FROM Investments t
        JOIN Securities s ON t.Securities_Id = s.Securities_Id
        JOIN Currencies cur_s ON s.Currencies_Id = cur_s.Currencies_Id
        WHERE t.Action IN ('Buy', 'Reinvest', 'ShrIn', 'Sell', 'ShrOut')
          AND t.Date <= (SELECT end_date FROM DateRange)
    ),
    -- Step 1: Materialise cumulative_bought_before for each sale row.
    -- This intermediate CTE is necessary because PostgreSQL does not allow a
    -- SELECT-list alias (cumulative_bought_before) to be referenced by another
    -- expression in the same SELECT list (the purchase_lots subquery).
    FIFO_Sales AS (
        SELECT 
            f.Accounts_Id,
            f.Securities_Id,
            f.Date as sale_date,
            f.Investments_Id as sale_id,
            f.Quantity as sold_quantity,
            f.Price_Per_Share as sale_price,
            f.Total_Amount as sale_amount,
            f.Commission as sale_commission,
            f.security_currency_id,
            f.security_currency,
            -- How many shares have already been consumed by prior sales (FIFO offset).
            -- We sum prior SELL quantities, NOT prior buy quantities.
            -- Using prior buys was the bug: it pushed the sale window past the buy lots entirely.
            (SELECT COALESCE(SUM(f2.Quantity), 0) 
             FROM FIFO_Inventory f2 
             WHERE f2.Accounts_Id = f.Accounts_Id 
               AND f2.Securities_Id = f.Securities_Id 
               AND f2.Action IN ('Sell', 'ShrOut')
               AND (f2.Date < f.Date OR (f2.Date = f.Date AND f2.Investments_Id < f.Investments_Id))
            ) as cumulative_bought_before
        FROM FIFO_Inventory f
        WHERE f.Action IN ('Sell', 'ShrOut')
          AND f.Date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
    ),
    -- Step 2: Now that cumulative_bought_before is a real column we can reference
    -- it freely inside the purchase_lots correlated subquery.
    FIFO_CostBasis AS (
        SELECT 
            fs.Accounts_Id,
            fs.Securities_Id,
            fs.sale_date,
            fs.sale_id,
            fs.sold_quantity,
            fs.sale_price,
            fs.sale_amount,
            fs.sale_commission,
            fs.security_currency_id,
            fs.security_currency,
            (
                SELECT json_agg(json_build_object(
                    'purchase_date', purchase_date,
                    'quantity', quantity,
                    'price', price,
                    'cost', cost
                ))
                FROM (
                    SELECT 
                        p.Date as purchase_date,
                        -- Correct FIFO overlap formula:
                        --   lot covers  [lot_start, lot_end)  where lot_start = running_bought - Quantity
                        --   sale covers [sale_start, sale_end) where sale_start = cumulative_bought_before
                        --   allocated qty = LEAST(lot_end, sale_end) - GREATEST(lot_start, sale_start)
                        GREATEST(0,
                            LEAST(p.running_bought,
                                  fs.cumulative_bought_before + fs.sold_quantity)
                            - GREATEST(p.running_bought - p.Quantity,
                                       fs.cumulative_bought_before)
                        ) as quantity,
                        p.Price_Per_Share as price,
                        p.Price_Per_Share * GREATEST(0,
                            LEAST(p.running_bought,
                                  fs.cumulative_bought_before + fs.sold_quantity)
                            - GREATEST(p.running_bought - p.Quantity,
                                       fs.cumulative_bought_before)
                        ) as cost
                    FROM FIFO_Inventory p
                    WHERE p.Accounts_Id = fs.Accounts_Id 
                      AND p.Securities_Id = fs.Securities_Id
                      AND p.Action IN ('Buy', 'Reinvest', 'ShrIn')
                      AND p.Date <= fs.sale_date
                      -- Only lots that overlap with the sale range
                      AND p.running_bought > fs.cumulative_bought_before
                      AND p.running_bought - p.Quantity < fs.cumulative_bought_before + fs.sold_quantity
                    ORDER BY p.Date, p.Investments_Id
                ) lots
                WHERE quantity > 0
            ) as purchase_lots
        FROM FIFO_Sales fs
    ),
    -- Calculate the realized P&L for each sale
    RealizedPNL AS (
        SELECT 
            fc.sale_date as date,
            DATE_TRUNC('month', fc.sale_date)::timestamp without time zone as month_date,
            EXTRACT(YEAR FROM fc.sale_date) as year,
            EXTRACT(MONTH FROM fc.sale_date) as month,
            fc.Accounts_Id,
            fc.Securities_Id,
            -- Calculate cost basis in original currency
            (SELECT COALESCE(SUM((lot->>'cost')::numeric), 0) FROM json_array_elements(fc.purchase_lots) as lot) as cost_basis_original,
            fc.sale_amount as sale_proceeds_original,
            fc.sale_commission as commission_original,
            -- Calculate realized P&L in original currency
            (fc.sale_amount - COALESCE((SELECT COALESCE(SUM((lot->>'cost')::numeric), 0) FROM json_array_elements(fc.purchase_lots) as lot), 0) - fc.sale_commission) as realized_pnl_original,
            -- Get account currency for FX conversion
            a.Currencies_Id as account_currency_id,
            curr_acc.Currencies_ShortName as account_currency,
            fc.security_currency,
            -- Description for the transaction
            (SELECT Securities_Name FROM Securities WHERE Securities_Id = fc.Securities_Id) as description,
            -- Map to category
            (SELECT Categories_Id FROM Categories WHERE Categories_Name = '_RealizedGain' LIMIT 1) as gain_category_id,
            (SELECT Categories_Id FROM Categories WHERE Categories_Name = '_RealizedLoss' LIMIT 1) as loss_category_id
        FROM FIFO_CostBasis fc
        JOIN Accounts a ON fc.Accounts_Id = a.Accounts_Id
        JOIN Currencies curr_acc ON a.Currencies_Id = curr_acc.Currencies_Id
        WHERE fc.sold_quantity > 0
    ),
    -- Convert realized P&L to EUR
    RealizedPNL_EUR AS (
        SELECT 
            rp.date,
            rp.month_date,
            rp.year,
            rp.month,
            -- Convert realized P&L to EUR using FX rate on sale date
            CASE 
                WHEN rp.account_currency = 'EUR' THEN rp.realized_pnl_original
                ELSE rp.realized_pnl_original * COALESCE(
                    (SELECT fx_rate FROM FX_Rates 
                     WHERE currencies_id_1 = rp.account_currency_id 
                       AND date <= rp.date 
                     ORDER BY date DESC LIMIT 1),
                    (SELECT fx_rate FROM DefaultFX_Rates WHERE currencies_id_1 = rp.account_currency_id),
                    1
                )
            END as split_amount_eur,
            rp.realized_pnl_original as split_amount_original,
            rp.account_currency as original_currency,
            -- Use gain or loss category based on P&L sign
            CASE 
                WHEN rp.realized_pnl_original > 0 THEN rp.gain_category_id
                ELSE rp.loss_category_id
            END as categories_id,
            rp.description,
            rp.Accounts_id,
            a.accounts_name,
            a.accounts_type,
            NULL as payees_name,
            'Realized P&L' as source_type
        FROM RealizedPNL rp
        JOIN Accounts a ON rp.Accounts_Id = a.Accounts_Id
    ),
    BankTransactionData AS (
        SELECT 
            t.date::timestamp without time zone as date,
            DATE_TRUNC('month', t.date)::timestamp without time zone as month_date,
            EXTRACT(YEAR FROM t.date) as year,
            EXTRACT(MONTH FROM t.date) as month,
            -- Convert amount to EUR using the FX rate as of the transaction date
            CASE 
                WHEN curr.currencies_shortname = 'EUR' THEN s.amount
                ELSE s.amount * COALESCE(
                    (SELECT fx_rate FROM FX_Rates 
                     WHERE currencies_id_1 = curr.currencies_id 
                       AND date <= t.date 
                     ORDER BY date DESC LIMIT 1),
                    (SELECT fx_rate FROM DefaultFX_Rates WHERE currencies_id_1 = curr.currencies_id),
                    1
                )
            END as split_amount_eur,
            s.amount as split_amount_original,
            curr.currencies_shortname as original_currency,
            s.categories_id,
            t.description,
            t.accounts_id,
            a.accounts_name,
            a.accounts_type,
            p.payees_name,
            'Bank' as source_type
        FROM Transactions t
        JOIN Splits s ON t.transactions_id = s.transactions_id
        JOIN CategoryHierarchy ch ON s.categories_id = ch.Categories_Id
        JOIN Accounts a ON t.accounts_id = a.accounts_id
        JOIN Currencies curr ON a.currencies_id = curr.currencies_id
        LEFT JOIN Payees p ON t.payees_id = p.payees_id
        WHERE t.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
    --      AND a.accounts_type NOT IN ('Brokerage', 'Pension', 'Other Investment', 'Margin')
          AND a.accounts_type IN %s
          AND s.amount != 0
    ),
    InvestmentTransactionData AS (
        SELECT 
            t.date::timestamp without time zone as date,
            DATE_TRUNC('month', t.date)::timestamp without time zone as month_date,
            EXTRACT(YEAR FROM t.date) as year,
            EXTRACT(MONTH FROM t.date) as month,
            -- Convert amount to EUR using the FX rate as of the transaction date
            CASE 
                WHEN curr.currencies_shortname = 'EUR' THEN 
                    CASE 
                        WHEN t.action IN ('Dividend', 'IntInc') THEN t.total_amount
                        WHEN t.action IN ('MiscExp') THEN -ABS(t.total_amount)
                        ELSE 0
                    END
                ELSE 
                    CASE 
                        WHEN t.action IN ('Dividend', 'IntInc') THEN t.total_amount * COALESCE(
                            (SELECT fx_rate FROM FX_Rates 
                             WHERE currencies_id_1 = curr.currencies_id 
                               AND date <= t.date 
                             ORDER BY date DESC LIMIT 1),
                            (SELECT fx_rate FROM DefaultFX_Rates WHERE currencies_id_1 = curr.currencies_id),
                            1
                        )
                        WHEN t.action IN ('MiscExp') THEN -ABS(t.total_amount) * COALESCE(
                            (SELECT fx_rate FROM FX_Rates 
                             WHERE currencies_id_1 = curr.currencies_id 
                               AND date <= t.date 
                             ORDER BY date DESC LIMIT 1),
                            (SELECT fx_rate FROM DefaultFX_Rates WHERE currencies_id_1 = curr.currencies_id),
                            1
                        )
                        ELSE 0
                    END
            END as split_amount_eur,
            CASE 
                WHEN t.action IN ('Dividend', 'IntInc') THEN t.total_amount
                WHEN t.action IN ('MiscExp') THEN -ABS(t.total_amount)
                ELSE 0
            END as split_amount_original,
            curr.currencies_shortname as original_currency,
            -- Map investment actions to appropriate categories
            CASE
                WHEN t.action = 'Dividend' THEN (SELECT Categories_Id FROM Categories WHERE Categories_Name = '_DivInc' LIMIT 1)
                WHEN t.action = 'IntInc' THEN (SELECT Categories_Id FROM Categories WHERE Categories_Name = '_IntInc' LIMIT 1)
                WHEN t.action = 'MiscExp' THEN (SELECT Categories_Id FROM Categories WHERE Categories_Name = '_MiscExp' LIMIT 1)
                ELSE NULL
            END as categories_id,
            COALESCE((SELECT s.Securities_Name FROM Securities s WHERE s.Securities_Id = t.Securities_id), t.description) as description,
            t.accounts_id,
            a.accounts_name,
            a.accounts_type,
            NULL as payees_name,
            'Investment' as source_type
        FROM Investments t
        JOIN Accounts a ON t.accounts_id = a.accounts_id
        JOIN Currencies curr ON a.currencies_id = curr.currencies_id
        WHERE t.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
    --      AND a.accounts_type IN ('Brokerage', 'Pension', 'Other Investment', 'Margin')
          AND a.accounts_type IN %s
          AND t.action IN ('Dividend', 'IntInc', 'MiscExp')
          AND t.total_amount != 0
    ),
    TransactionData AS (
        SELECT * FROM BankTransactionData
        UNION ALL
        SELECT * FROM InvestmentTransactionData
        UNION ALL
        SELECT * FROM RealizedPNL_EUR
    )
    SELECT 
        td.date,
        td.month_date,
        td.year,
        td.month,
        td.split_amount_eur as split_amount,
        td.split_amount_original,
        td.original_currency,
        td.categories_id,
        td.description,
        td.accounts_id,
        td.accounts_name,
        td.accounts_type,
        td.payees_name,
        CASE WHEN td.source_type = 'Realized P&L' THEN 'Investment' ELSE td.source_type END as source_type,
        COALESCE(c.Full_Path, 
            CASE 
                WHEN td.source_type = 'Realized P&L' AND td.split_amount_eur > 0 THEN '_RlzdGain' --'Investment: Realized Gains'
                WHEN td.source_type = 'Realized P&L' AND td.split_amount_eur < 0 THEN '_RlzdGain' --'Investment: Realized Losses'
                ELSE 'Uncategorized'
            END
        ) as category_full_path,
        COALESCE(c.Name,
            CASE 
                WHEN td.source_type = 'Realized P&L' AND td.split_amount_eur > 0 THEN '_RlzdGain' --'Realized Gains'
                WHEN td.source_type = 'Realized P&L' AND td.split_amount_eur < 0 THEN '_RlzdGain' --'Realized Losses'
                ELSE 'Uncategorized'
            END
        ) as category_name,
        COALESCE(c.Categories_Type, 
            CASE 
            --    WHEN td.split_amount_eur > 0 THEN 'Income'::text 
                WHEN td.split_amount_eur != 0 THEN 'Income'::text 
                ELSE 'Expense'::text 
            END
        ) as Categories_Type,
        COALESCE(c.Level, 0) as category_level
    FROM TransactionData td
    LEFT JOIN CategoryHierarchy c ON td.categories_id = c.Categories_Id
    """
    
    params = [start_date, end_date, cash_account_types_tuple, inv_account_types_tuple]
    
    # Add category filter if specified
    if category_id:
        cursor = conn.cursor()
        cursor.execute("""
            WITH RECURSIVE CategoryHierarchy AS (
                SELECT Categories_Id, Categories_Name::TEXT as Full_Path
                FROM Categories WHERE Categories_Id = %s
                UNION ALL
                SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name
                FROM Categories c
                JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
            )
            SELECT Full_Path FROM CategoryHierarchy WHERE Categories_Id = %s
        """, (category_id, category_id))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            category_path = result[0]
            base_query += " AND c.Full_Path LIKE %s || '%%'"
            params.append(category_path)
    
    base_query += " ORDER BY td.date DESC, c.Full_Path"
    
    df = pd.read_sql(base_query, conn, params=params)
    conn.close()
    
    # Convert datetime columns to naive datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if hasattr(df['date'].dt, 'tz') and df['date'].dt.tz is not None:
            df['date'] = df['date'].dt.tz_localize(None)
    
    if 'month_date' in df.columns:
        df['month_date'] = pd.to_datetime(df['month_date'], errors='coerce')
        if hasattr(df['month_date'].dt, 'tz') and df['month_date'].dt.tz is not None:
            df['month_date'] = df['month_date'].dt.tz_localize(None)

    return df


# ======================================================
# CASH FLOW FORECAST
# ======================================================

@st.cache_data(ttl=300)
def get_cash_flow_forecast(months_back: int = 3):
    """
    Returns two DataFrames:
      - df_future   : explicitly scheduled future transactions (Date > today)
      - df_recurring: payees present in EVERY one of the last `months_back`
                      complete calendar months, with average amount and interval.
    All amounts in EUR.
    `months_back` is treated as a trusted integer (sidebar slider, range 2-6).
    """
    conn = get_connection()
    mb   = int(months_back)   # guard against accidental float

    # 1. Explicitly entered future transactions
    df_future = pd.read_sql("""
        WITH LatestFX AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
            FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
        )
        SELECT
            t.Date,
            p.Payees_Name,
            a.Accounts_Name,
            c.Currencies_ShortName AS currency,
            s.Amount,
            CASE WHEN c.Currencies_ShortName = 'EUR' THEN s.Amount
                 ELSE s.Amount * COALESCE(fx.FX_Rate, 1) END AS amount_eur,
            cat.Categories_Name AS category
        FROM Transactions t
        JOIN Accounts a ON t.Accounts_Id = a.Accounts_Id
        JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id
        LEFT JOIN Payees p ON t.Payees_Id = p.Payees_Id
        LEFT JOIN Splits s ON t.Transactions_Id = s.Transactions_Id
        LEFT JOIN Categories cat ON s.Categories_Id = cat.Categories_Id
        LEFT JOIN LatestFX fx ON fx.Currencies_Id_1 = c.Currencies_Id
        WHERE t.Date > CURRENT_DATE
          AND t.Transfers_Id IS NULL       -- exclude internal transfers
        ORDER BY t.Date ASC
    """, conn)

    # 2. Recurring detection — payees present in ALL last mb complete months.
    #    One row per (payee, transaction_date) aggregating all splits so that
    #    the LAG-based interval reflects actual payment cadence.
    df_recurring = pd.read_sql(f"""
        WITH
        -- One row per (payee, category, date): amount = sum of splits for that
        -- category on that single transaction date.
        recent AS (
            SELECT
                t.Payees_Id,
                p.Payees_Name,
                s.Categories_Id,
                cat.Categories_Name,
                DATE_TRUNC('month', t.Date)::date   AS month_start,
                t.Date,
                SUM(s.Amount)                        AS amount,
                a.Currencies_Id
            FROM Transactions t
            JOIN  Accounts a  ON a.Accounts_Id  = t.Accounts_Id
            LEFT JOIN Payees p ON p.Payees_Id   = t.Payees_Id
            LEFT JOIN Splits s ON s.Transactions_Id = t.Transactions_Id
            LEFT JOIN Categories cat ON cat.Categories_Id = s.Categories_Id
            WHERE t.Date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{mb} months'
              AND t.Date <  DATE_TRUNC('month', CURRENT_DATE)
              AND t.Payees_Id IS NOT NULL
              AND t.Transfers_Id IS NULL
            GROUP BY t.Payees_Id, p.Payees_Name, s.Categories_Id, cat.Categories_Name,
                     t.Date, DATE_TRUNC('month', t.Date)::date, a.Currencies_Id
        ),
        -- Keep only (payee, category) pairs present in EVERY one of the mb months
        qualified AS (
            SELECT Payees_Id, Categories_Id, Currencies_Id
            FROM   recent
            GROUP  BY Payees_Id, Categories_Id, Currencies_Id
            HAVING COUNT(DISTINCT month_start) = {mb}
        ),
        -- Average transactions per month — drives interval strategy selection
        tx_freq AS (
            SELECT r.Payees_Id, r.Categories_Id, r.Currencies_Id,
                   COUNT(*)::float / {mb} AS avg_tx_per_month
            FROM recent r
            JOIN qualified q
                 ON  q.Payees_Id     = r.Payees_Id
                 AND q.Categories_Id = r.Categories_Id
                 AND q.Currencies_Id = r.Currencies_Id
            GROUP BY r.Payees_Id, r.Categories_Id, r.Currencies_Id
        ),
        -- Strategy A — transaction-level LAG.
        -- Correct for high-frequency payees (e.g. 3-4 supermarket visits/month).
        tx_lag AS (
            SELECT r.Payees_Id, r.Categories_Id, r.Currencies_Id,
                   (r.Date - LAG(r.Date) OVER (
                       PARTITION BY r.Payees_Id, r.Categories_Id, r.Currencies_Id
                       ORDER BY r.Date
                   ))::float AS days_since_prev
            FROM recent r
            JOIN qualified q
                 ON  q.Payees_Id     = r.Payees_Id
                 AND q.Categories_Id = r.Categories_Id
                 AND q.Currencies_Id = r.Currencies_Id
        ),
        interval_tx AS (
            SELECT Payees_Id, Categories_Id, Currencies_Id,
                   COALESCE(AVG(days_since_prev), 30) AS avg_interval
            FROM   tx_lag
            GROUP  BY Payees_Id, Categories_Id, Currencies_Id
        ),
        -- Strategy B — monthly-repr LAG.
        -- Collapse to the earliest date per month so sporadic bonus entries
        -- (e.g. Christmas bonus on top of regular salary) don't create false
        -- short intra-month intervals.
        monthly_repr AS (
            SELECT r.Payees_Id, r.Payees_Name, r.Categories_Id, r.Categories_Name,
                   r.month_start, r.Currencies_Id,
                   MIN(r.Date) AS repr_date
            FROM recent r
            JOIN qualified q
                 ON  q.Payees_Id     = r.Payees_Id
                 AND q.Categories_Id = r.Categories_Id
                 AND q.Currencies_Id = r.Currencies_Id
            GROUP BY r.Payees_Id, r.Payees_Name, r.Categories_Id, r.Categories_Name,
                     r.month_start, r.Currencies_Id
        ),
        monthly_lag AS (
            SELECT *,
                   (repr_date - LAG(repr_date) OVER (
                       PARTITION BY Payees_Id, Categories_Id, Currencies_Id
                       ORDER BY month_start
                   ))::float AS days_since_prev
            FROM monthly_repr
        ),
        interval_monthly AS (
            SELECT Payees_Id, Categories_Id, Currencies_Id,
                   COALESCE(AVG(days_since_prev), 30) AS avg_interval
            FROM   monthly_lag
            GROUP  BY Payees_Id, Categories_Id, Currencies_Id
        ),
        -- Median single-transaction amount — robust against bonus outliers
        amount_stats AS (
            SELECT r.Payees_Id, r.Categories_Id, r.Currencies_Id,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.amount) AS median_amount,
                   STDDEV(r.amount)                                        AS std_amount,
                   MAX(r.Date)                                             AS last_date
            FROM recent r
            JOIN qualified q
                 ON  q.Payees_Id     = r.Payees_Id
                 AND q.Categories_Id = r.Categories_Id
                 AND q.Currencies_Id = r.Currencies_Id
            GROUP BY r.Payees_Id, r.Categories_Id, r.Currencies_Id
        ),
        -- Distinct names (one row per key from monthly_repr)
        names AS (
            SELECT DISTINCT ON (Payees_Id, Categories_Id, Currencies_Id)
                   Payees_Id, Payees_Name, Categories_Id, Categories_Name, Currencies_Id
            FROM   monthly_repr
        ),
        stats AS (
            SELECT
                n.Payees_Id,
                n.Payees_Name,
                n.Categories_Id,
                n.Categories_Name,
                am.median_amount                            AS avg_amount,
                am.std_amount,
                -- Use transaction-level intervals when avg > 1.5 tx/month
                -- (genuine high-frequency payees like supermarkets); use
                -- month-level intervals for low-frequency payees to suppress
                -- bonus-entry noise.
                CASE WHEN tf.avg_tx_per_month > 1.5
                     THEN it.avg_interval
                     ELSE im.avg_interval
                END                                         AS avg_days_between,
                am.last_date,
                n.Currencies_Id
            FROM names n
            JOIN amount_stats  am ON am.Payees_Id = n.Payees_Id AND am.Categories_Id = n.Categories_Id AND am.Currencies_Id = n.Currencies_Id
            JOIN tx_freq       tf ON tf.Payees_Id = n.Payees_Id AND tf.Categories_Id = n.Categories_Id AND tf.Currencies_Id = n.Currencies_Id
            JOIN interval_tx   it ON it.Payees_Id = n.Payees_Id AND it.Categories_Id = n.Categories_Id AND it.Currencies_Id = n.Currencies_Id
            JOIN interval_monthly im ON im.Payees_Id = n.Payees_Id AND im.Categories_Id = n.Categories_Id AND im.Currencies_Id = n.Currencies_Id
        ),
        fx AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
            FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
        )
        SELECT
            s.Payees_Name,
            s.Categories_Name                       AS category,
            ROUND(s.avg_amount::numeric,       2)   AS avg_amount,
            ROUND(s.std_amount::numeric,       2)   AS std_amount,
            ROUND(s.avg_days_between::numeric, 0)   AS avg_days_between,
            s.last_date,
            (s.last_date + ROUND(s.avg_days_between)::int)::date AS next_expected_date,
            c.Currencies_ShortName                  AS currency,
            ROUND((s.avg_amount * COALESCE(fx.FX_Rate, 1))::numeric, 2) AS avg_amount_eur
        FROM   stats s
        JOIN   Currencies c ON c.Currencies_Id    = s.Currencies_Id
        LEFT   JOIN fx      ON fx.Currencies_Id_1 = s.Currencies_Id
        ORDER  BY next_expected_date ASC
    """, conn)

    conn.close()
    if not df_future.empty:
        df_future['date'] = pd.to_datetime(df_future['date'])
    if not df_recurring.empty:
        df_recurring['last_date']          = pd.to_datetime(df_recurring['last_date'])
        df_recurring['next_expected_date'] = pd.to_datetime(df_recurring['next_expected_date'])
    return df_future, df_recurring


# ======================================================
# DIVIDEND INCOME TRACKER
# ======================================================

@st.cache_data(ttl=3600)
def get_dividend_tracker_data(start_date: str, end_date: str):
    """Monthly dividend and interest income and FIFO YOC per security for the given date range."""
    conn = get_connection()

    df = pd.read_sql("""
        WITH fx AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
            FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
        ),
        -- One row per actual income date (not truncated to month) so the LATERAL
        -- cutoffs use the exact date, avoiding the Expire-same-day bug.
        income AS (
            SELECT
                i.Date,
                DATE_TRUNC('month', i.Date)::date                        AS month,
                i.Securities_Id,
                s.Securities_Name,
                s.Securities_Type,
                a.Accounts_Name,
                a.Currencies_Id,
                SUM(
                    CASE WHEN i.Action = 'MiscExp'
                    THEN -i.Total_Amount * COALESCE(fx.FX_Rate, 1)
                    ELSE  i.Total_Amount * COALESCE(fx.FX_Rate, 1)
                    END
                )                                                          AS income_eur,
                i.Action
            FROM Investments i
            JOIN Securities s ON i.Securities_Id = s.Securities_Id
            JOIN Accounts   a ON i.Accounts_Id   = a.Accounts_Id
            LEFT JOIN fx      ON fx.Currencies_Id_1 = a.Currencies_Id
            WHERE i.Action IN ('Dividend','IntInc','Reinvest','RtrnCap','MiscExp')
              AND i.Date BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY i.Date, i.Securities_Id, s.Securities_Name, s.Securities_Type,
                     a.Accounts_Name, a.Currencies_Id, i.Action
        )
        SELECT
            i.Date                                                        AS date,
            i.month,
            i.Securities_Name,
            i.Securities_Type,
            i.Accounts_Name,
            i.Action,
            ROUND(i.income_eur::numeric, 2)                               AS income_eur,
            ROUND(COALESCE(fc.cost_eur, 0)::numeric, 2)                   AS cost_basis_eur,
            -- date of the oldest FIFO lot still held at this income payment's date
            -- (used only for "All Time" annualisation)
            fc.position_start_date
        FROM income i
        -- FIFO cost of the position held on the exact day of each income payment.
        -- Buys: on or before income date. Sells/Expire: strictly before (so same-day
        -- expiry on a bond maturity date does not zero out the cost basis).
        -- Also returns position_start_date = oldest remaining lot date, used for YoC.
        CROSS JOIN LATERAL (
            WITH buys AS (
                SELECT
                    b.Date                                                                       AS buy_date,
                    b.Quantity                                                                   AS buy_qty,
                    ABS(b.Total_Amount) * COALESCE(fx2.FX_Rate, 1) / NULLIF(b.Quantity, 0)     AS cost_per_unit_eur,
                    SUM(b.Quantity) OVER (ORDER BY b.Date, b.Investments_Id)                    AS running_buy_qty
                FROM Investments b
                JOIN  Accounts a2 ON b.Accounts_Id      = a2.Accounts_Id
                LEFT JOIN fx fx2  ON fx2.Currencies_Id_1 = a2.Currencies_Id
                WHERE b.Securities_Id = i.Securities_Id
                  AND (
                      b.Action IN ('Buy','ShrIn','Vest')
                      OR (b.Action = 'Reinvest' AND i.Securities_Type NOT IN ('CD','Bond'))
                  )
                  AND b.Date    <= i.Date
                  AND b.Quantity > 0
            ),
            sells AS (
                SELECT COALESCE(SUM(s.Quantity), 0) AS total_sell_qty
                FROM Investments s
                WHERE s.Securities_Id = i.Securities_Id
                  AND s.Action IN ('Sell','ShrOut','Expire')
                  AND s.Date     < i.Date
            ),
            fifo AS (
                SELECT
                    b.buy_date,
                    GREATEST(0.0, LEAST(b.buy_qty, b.running_buy_qty - s.total_sell_qty))                         AS remaining_qty,
                    GREATEST(0.0, LEAST(b.buy_qty, b.running_buy_qty - s.total_sell_qty)) * b.cost_per_unit_eur   AS lot_cost
                FROM buys b CROSS JOIN sells s
            )
            SELECT
                COALESCE(SUM(lot_cost), 0)                              AS cost_eur,
                MIN(CASE WHEN remaining_qty > 0 THEN buy_date END)      AS position_start_date
            FROM fifo
        ) AS fc
        ORDER BY i.Date DESC, i.income_eur DESC
    """, conn, params={'start_date': start_date, 'end_date': end_date})

    conn.close()
    if not df.empty:
        df['month']               = pd.to_datetime(df['month'])
        df['date']                = pd.to_datetime(df['date'])
        df['position_start_date'] = pd.to_datetime(df['position_start_date'])
    return df


# ======================================================
# ASSET ALLOCATION VS TARGET
# ======================================================

def get_allocation_targets():
    """Return all rows from Allocation_Targets as a DataFrame."""
    conn = get_connection()
    df = pd.read_sql(
        "SELECT Securities_Type, Target_Pct FROM Allocation_Targets ORDER BY Securities_Type",
        conn,
    )
    conn.close()
    return df


def save_allocation_targets(targets: dict):
    """Upsert {securities_type: target_pct} into Allocation_Targets."""
    conn = get_connection()
    cur = conn.cursor()
    for sec_type, pct in targets.items():
        cur.execute(
            """
            INSERT INTO Allocation_Targets (Securities_Type, Target_Pct)
            VALUES (%s, %s)
            ON CONFLICT (Securities_Type)
            DO UPDATE SET Target_Pct = EXCLUDED.Target_Pct
            """,
            (sec_type, float(pct)),
        )
    conn.commit()
    cur.close()
    conn.close()


@st.cache_data(ttl=300)
def get_sector_allocation_data():
    """Current allocation by Sector and Industry in EUR."""
    conn = get_connection()
    df = pd.read_sql("""
        WITH fx AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
            FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
        ),
        prices AS (
            SELECT DISTINCT ON (Securities_Id) Securities_Id, Close
            FROM Historical_Prices ORDER BY Securities_Id, Date DESC
        ),
        holdings_value AS (
            SELECT
                COALESCE(NULLIF(TRIM(s.Sector),   ''), 'Other / Unknown') AS sector,
                COALESCE(NULLIF(TRIM(s.Industry), ''), 'Other / Unknown') AS industry,
                s.Securities_Type::text                                    AS securities_type,
                SUM(h.Quantity * COALESCE(p.Close, 0)
                               * COALESCE(fx.FX_Rate, 1))                 AS value_eur
            FROM Holdings h
            JOIN  Securities s ON h.Securities_Id = s.Securities_Id
            JOIN  Accounts   a ON h.Accounts_Id   = a.Accounts_Id
            LEFT JOIN prices p ON p.Securities_Id  = h.Securities_Id
            LEFT JOIN fx       ON fx.Currencies_Id_1 = s.Currencies_Id
            WHERE h.Quantity > 0
            GROUP BY
                COALESCE(NULLIF(TRIM(s.Sector),   ''), 'Other / Unknown'),
                COALESCE(NULLIF(TRIM(s.Industry), ''), 'Other / Unknown'),
                s.Securities_Type::text
        ),
        total AS (SELECT SUM(value_eur) AS grand_total FROM holdings_value)
        SELECT
            hv.sector,
            hv.industry,
            hv.securities_type,
            ROUND(hv.value_eur::numeric, 2)                                        AS value_eur,
            ROUND((hv.value_eur / NULLIF(t.grand_total, 0) * 100)::numeric, 2)    AS actual_pct
        FROM holdings_value hv
        CROSS JOIN total t
        ORDER BY hv.value_eur DESC
    """, conn)
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_asset_allocation_data():
    """Current allocation by Securities_Type in EUR vs. targets from Allocation_Targets."""
    conn = get_connection()

    df = pd.read_sql("""
        WITH fx AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
            FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
        ),
        prices AS (
            SELECT DISTINCT ON (Securities_Id) Securities_Id, Close
            FROM Historical_Prices ORDER BY Securities_Id, Date DESC
        ),
        holdings_value AS (
            SELECT
                s.Securities_Type::text AS securities_type,
                SUM(h.Quantity * COALESCE(p.Close, 0) * COALESCE(fx.FX_Rate, 1)) AS value_eur
            FROM Holdings h
            JOIN Securities s ON h.Securities_Id = s.Securities_Id
            JOIN Accounts   a ON h.Accounts_Id   = a.Accounts_Id
            LEFT JOIN prices p  ON p.Securities_Id = h.Securities_Id
            LEFT JOIN fx        ON fx.Currencies_Id_1 = s.Currencies_Id
            WHERE h.Quantity > 0
            GROUP BY s.Securities_Type
        ),
        total AS (SELECT SUM(value_eur) AS grand_total FROM holdings_value)
        SELECT
            hv.securities_type,
            ROUND(hv.value_eur::numeric, 2)                              AS value_eur,
            ROUND((hv.value_eur / NULLIF(t.grand_total, 0) * 100)::numeric, 2) AS actual_pct,
            COALESCE(at.Target_Pct, 0)                                   AS target_pct,
            ROUND((hv.value_eur / NULLIF(t.grand_total, 0) * 100 - COALESCE(at.Target_Pct, 0))::numeric, 2) AS delta_pct
        FROM holdings_value hv
        CROSS JOIN total t
        LEFT JOIN Allocation_Targets at ON at.Securities_Type = hv.securities_type
        ORDER BY value_eur DESC
    """, conn)

    conn.close()
    return df


# ======================================================
# FX EXPOSURE
# ======================================================

@st.cache_data(ttl=300)
def get_fx_exposure_data():
    """Net exposure per currency in EUR and sensitivity to a ±5 % FX move."""
    conn = get_connection()

    df = pd.read_sql("""
        WITH fx AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
            FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
        ),
        prices AS (
            SELECT DISTINCT ON (Securities_Id) Securities_Id, Close
            FROM Historical_Prices ORDER BY Securities_Id, Date DESC
        ),
        cash_exp AS (
            -- Mirrors dashboard logic: all account balances except Brokerage/Margin
            -- (whose value is captured entirely through Holdings)
            SELECT
                a.Currencies_Id,
                SUM(a.Accounts_Balance) AS balance
            FROM Accounts a
            WHERE a.Is_Active = TRUE
              AND a.Accounts_Type NOT IN ('Brokerage', 'Margin')
            GROUP BY a.Currencies_Id
        ),
        inv_exp AS (
            SELECT
                s.Currencies_Id,
                SUM(h.Quantity * COALESCE(p.Close, 0)) AS value_native
            FROM Holdings h
            JOIN Securities s ON h.Securities_Id = s.Securities_Id
            LEFT JOIN prices p ON p.Securities_Id = h.Securities_Id
            WHERE h.Quantity > 0
            GROUP BY s.Currencies_Id
        ),
        combined AS (
            SELECT Currencies_Id, SUM(balance) AS native_exposure FROM cash_exp GROUP BY Currencies_Id
            UNION ALL
            SELECT Currencies_Id, SUM(value_native) FROM inv_exp GROUP BY Currencies_Id
        ),
        aggregated AS (
            SELECT Currencies_Id, SUM(native_exposure) AS native_exposure FROM combined GROUP BY Currencies_Id
        )
        SELECT
            c.Currencies_ShortName                                          AS currency,
            ROUND(a.native_exposure::numeric, 2)                            AS native_exposure,
            ROUND((a.native_exposure * COALESCE(fx.FX_Rate, 1))::numeric, 2) AS eur_exposure,
            ROUND((a.native_exposure * COALESCE(fx.FX_Rate, 1) * 0.05)::numeric, 2) AS sensitivity_5pct_eur
        FROM aggregated a
        JOIN Currencies c ON c.Currencies_Id = a.Currencies_Id
        LEFT JOIN fx      ON fx.Currencies_Id_1 = a.Currencies_Id
        ORDER BY ABS(a.native_exposure * COALESCE(fx.FX_Rate, 1)) DESC
    """, conn)

    conn.close()
    return df


# ======================================================
# BOND SCHEDULE
# ======================================================

@st.cache_data(ttl=3600)
def get_bond_schedule_data():
    """Upcoming maturities and coupon cash flows for bond holdings."""
    conn = get_connection()

    df = pd.read_sql("""
        WITH fx AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
            FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
        ),
        bond_holdings AS (
            SELECT
                h.Securities_Id,
                s.Securities_Name,
                s.Securities_Type,
                h.Quantity,
                s.Maturity_Date,
                s.Coupon_Rate,
                s.Face_Value,
                s.Coupon_Frequency,
                s.Currencies_Id,
                c.Currencies_ShortName AS currency
            FROM Holdings h
            JOIN Securities s ON h.Securities_Id = s.Securities_Id
            JOIN Currencies c ON s.Currencies_Id = c.Currencies_Id
            WHERE h.Quantity > 0
              AND s.Securities_Type IN ('Bond')
        )
        SELECT
            bh.Securities_Name,
            bh.Quantity,
            bh.Face_Value,
            ROUND((bh.Quantity * COALESCE(bh.Face_Value, 0))::numeric, 2)   AS total_face_eur_native,
            ROUND((bh.Quantity * COALESCE(bh.Face_Value, 0) * COALESCE(fx.FX_Rate, 1))::numeric, 2) AS total_face_eur,
            bh.Coupon_Rate,
            bh.Coupon_Frequency,
            ROUND((bh.Quantity * COALESCE(bh.Face_Value, 0) * COALESCE(bh.Coupon_Rate, 0) / 100 *
                CASE bh.Coupon_Frequency
                    WHEN 'At Maturity'  THEN 0
                    WHEN 'Semi-Annual'  THEN 0.5
                    WHEN 'Quarterly'    THEN 0.25
                    WHEN 'Monthly'      THEN 1.0/12
                    ELSE 1.0
                END * COALESCE(fx.FX_Rate, 1))::numeric, 2) AS next_coupon_eur,
            ROUND((bh.Quantity * COALESCE(bh.Face_Value, 0) * COALESCE(bh.Coupon_Rate, 0) / 100 *
                CASE bh.Coupon_Frequency
                    WHEN 'At Maturity'  THEN 0
                    ELSE 1.0
                END * COALESCE(fx.FX_Rate, 1))::numeric, 2) AS annual_coupon_eur,
            bh.Maturity_Date,
            (bh.Maturity_Date - CURRENT_DATE)            AS days_to_maturity,
            bh.currency
        FROM bond_holdings bh
        LEFT JOIN fx ON fx.Currencies_Id_1 = bh.Currencies_Id
        ORDER BY bh.Maturity_Date ASC NULLS LAST
    """, conn)

    conn.close()
    if not df.empty and 'maturity_date' in df.columns:
        df['maturity_date'] = pd.to_datetime(df['maturity_date'])
    return df


# ======================================================
# ANOMALY DETECTION
# ======================================================

@st.cache_data(ttl=300)
def get_transaction_anomalies(z_threshold: float = 2.5, lookback_days: int = 365):
    """
    Returns recent transactions whose split amount is >= z_threshold standard
    deviations from the mean for that payee+category combination.
    Amounts in EUR.
    """
    conn = get_connection()

    df = pd.read_sql("""
        WITH fx AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
            FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
        ),
        splits_eur AS (
            SELECT
                t.Transactions_Id,
                t.Date,
                p.Payees_Name,
                cat.Categories_Name AS category,
                a.Accounts_Name,
                s.Amount,
                ROUND((s.Amount * COALESCE(fx.FX_Rate, 1))::numeric, 2) AS amount_eur
            FROM Transactions t
            JOIN Accounts a  ON t.Accounts_Id = a.Accounts_Id
            JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id
            LEFT JOIN Payees p   ON t.Payees_Id = p.Payees_Id
            LEFT JOIN Splits s   ON s.Transactions_Id = t.Transactions_Id
            LEFT JOIN Categories cat ON s.Categories_Id = cat.Categories_Id
            LEFT JOIN fx ON fx.Currencies_Id_1 = a.Currencies_Id
            WHERE t.Date >= CURRENT_DATE - %(lookback)s * INTERVAL '1 day'
              AND p.Payees_Name IS NOT NULL
              AND s.Amount IS NOT NULL
              AND t.Transfers_Id IS NULL -- Exclude internal transfers
        ),
        stats AS (
            SELECT
                Payees_Name,
                category,
                AVG(amount_eur)    AS mean_eur,
                STDDEV(amount_eur) AS std_eur,
                COUNT(*)           AS sample_size
            FROM splits_eur
            GROUP BY Payees_Name, category
            HAVING COUNT(*) >= 3
        )
        SELECT
            se.Date,
            se.Payees_Name,
            se.category,
            se.Accounts_Name,
            se.amount_eur,
            st.mean_eur,
            st.std_eur,
            ROUND(((se.amount_eur - st.mean_eur) / NULLIF(st.std_eur, 0))::numeric, 2) AS z_score
        FROM splits_eur se
        JOIN stats st
          ON st.Payees_Name = se.Payees_Name
         AND (st.category = se.category OR (st.category IS NULL AND se.category IS NULL))
        WHERE ABS((se.amount_eur - st.mean_eur) / NULLIF(st.std_eur, 0)) >= %(z)s
          AND se.Date >= CURRENT_DATE - 30
        ORDER BY ABS((se.amount_eur - st.mean_eur) / NULLIF(st.std_eur, 0)) DESC
        LIMIT 20
    """, conn, params={"lookback": lookback_days, "z": z_threshold})

    conn.close()
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
    return df


# ======================================================
# AI WEEKLY SUMMARIES
# ======================================================

@st.cache_data(ttl=3600)
def get_weekly_summaries(limit: int = 12) -> "pd.DataFrame":
    """Return the most recent AI weekly summaries, newest first."""
    conn = get_connection()
    df = pd.read_sql("""
        SELECT Week_Start, Generated_At, Summary_Text
        FROM AI_Weekly_Summaries
        ORDER BY Week_Start DESC
        LIMIT %s
    """, conn, params=(limit,))
    conn.close()
    if not df.empty:
        df['week_start']    = pd.to_datetime(df['week_start'])
        df['generated_at']  = pd.to_datetime(df['generated_at'])
    return df

@st.cache_data(ttl=3600)
def get_monthly_summaries(limit: int = 12) -> "pd.DataFrame":
    """Return the most recent AI monthly summaries, newest first."""
    conn = get_connection()
    df = pd.read_sql("""
        SELECT Month_Start, Generated_At, Summary_Text
        FROM AI_Monthly_Summaries
        ORDER BY Month_Start DESC
        LIMIT %s
    """, conn, params=(limit,))
    conn.close()
    if not df.empty:
        df['month_start']    = pd.to_datetime(df['month_start'])
        df['generated_at']  = pd.to_datetime(df['generated_at'])
    return df