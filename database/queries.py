import pandas as pd
import streamlit as st
from database.connection import get_connection

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
            a.Account_Balance - COALESCE((
                SELECT SUM(Total_Amount) 
                FROM Bank_Transactions 
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
            a.Account_Balance - COALESCE((
                SELECT SUM(Total_Amount) 
                FROM Bank_Transactions 
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
            a.Account_Balance - COALESCE((
                SELECT SUM(Total_Amount) 
                FROM Bank_Transactions 
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
            a.Account_Balance - COALESCE((
                SELECT  
                    SUM(CASE WHEN Action IN ('CashIn', 'IntInc') THEN Total_Amount 
                             WHEN Action IN ('CashOut') THEN -Total_Amount 
                             ELSE 0 END)
                FROM Investment_Transactions
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
                FROM Investment_Transactions 
                WHERE Securities_Id = h.Securities_Id 
                AND Date > dt.d
            ), 0) as qty_at_date
        FROM dates dt
        CROSS JOIN Holdings h
    ),
    daily_fx AS (
        SELECT dt.d as date, c.Currencies_Id,
            (SELECT FX_Rate FROM Historical_FX WHERE FX_Date <= dt.d AND Base_Currency_Id = c.Currencies_Id ORDER BY FX_Date DESC LIMIT 1) as fx_rate
        FROM dates dt CROSS JOIN Currencies c
    ),
    daily_prices AS (
        SELECT dt.d as date, s.Securities_Id,
            (SELECT Price_Close FROM Historical_Prices WHERE Price_Date <= dt.d AND Securities_Id = s.Securities_Id ORDER BY Price_Date DESC LIMIT 1) as price_close
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
            (SELECT SUM(hi.qty_at_date * COALESCE(dp.price_close, 0) * 
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
    """Get historical investment positions data."""
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
                SELECT SUM(CASE WHEN Action = 'Buy' THEN Quantity WHEN Action = 'Sell' THEN -Quantity ELSE 0 END)
                FROM Investment_Transactions 
                WHERE Securities_Id = h.Securities_Id AND Accounts_Id = h.Accounts_Id
                AND Date > dt.d
            ), 0) as qty_at_date
        FROM dates dt
        CROSS JOIN Holdings h
    ),
    daily_fx AS (
        SELECT dt.d as date, c.Currencies_Id,
            (SELECT FX_Rate FROM Historical_FX WHERE FX_Date <= dt.d AND Base_Currency_Id = c.Currencies_Id ORDER BY FX_Date DESC LIMIT 1) as fx_rate
        FROM dates dt CROSS JOIN Currencies c
    ),
    daily_prices AS (
        SELECT dt.d as date, s.Securities_Id,
            (SELECT Price_Close FROM Historical_Prices WHERE Price_Date <= dt.d AND Securities_Id = s.Securities_Id ORDER BY Price_Date DESC LIMIT 1) as price_close
        FROM dates dt CROSS JOIN Securities s
    )
    SELECT 
        hq.date,
        COALESCE(a.Accounts_Name, 'Total') as Accounts_Name,
        SUM(hq.qty_at_date * COALESCE(dp.price_close, 0) * 
            CASE WHEN cur_s.Currencies_ShortName = 'EUR' THEN 1 ELSE COALESCE(dfx.fx_rate, 1) END
        ) as account_value
    FROM historical_qty hq
    JOIN Accounts a ON hq.Accounts_Id = a.Accounts_Id
    JOIN Securities s ON hq.Securities_Id = s.Securities_Id
    JOIN Currencies cur_s ON s.Currencies_Id = cur_s.Currencies_Id
    LEFT JOIN daily_prices dp ON hq.date = dp.date AND hq.Securities_Id = dp.Securities_Id
    LEFT JOIN daily_fx dfx ON hq.date = dfx.date AND s.Currencies_Id = dfx.Currencies_Id
    GROUP BY hq.date, ROLLUP(a.Accounts_Name)
    HAVING SUM(hq.qty_at_date) > 0 
    ORDER BY hq.date ASC, (a.Accounts_Name IS NULL) ASC, a.Accounts_Name ASC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def get_pnl_report_data():
    """Get P&L report data."""
    conn = get_connection()
    
    query = """
    WITH RECURSIVE 
    periods AS (
        SELECT 
            (date_trunc('day', CURRENT_DATE) - INTERVAL '1 day')::date as dtd_start,
            (date_trunc('week', CURRENT_DATE) - INTERVAL '1 day')::date as wtd_start,
            (date_trunc('month', CURRENT_DATE) - INTERVAL '1 day')::date as mtd_start,
            (date_trunc('year', CURRENT_DATE) - INTERVAL '1 day')::date as ytd_start,
            '1900-01-01'::date as all_time_start,
            CURRENT_DATE::date as today
    ),
    historical_holdings AS (
        SELECT 
            p.today, p.dtd_start, p.wtd_start, p.mtd_start, p.ytd_start, p.all_time_start,
            h.Accounts_Id, h.Securities_Id,
            h.Quantity as qty_today,
            h.Quantity - COALESCE((SELECT SUM(CASE WHEN Action = 'Buy' THEN Quantity WHEN Action = 'Sell' THEN -Quantity ELSE 0 END) FROM Investment_Transactions WHERE Securities_Id = h.Securities_Id AND Accounts_Id = h.Accounts_Id AND Date > p.dtd_start), 0) as qty_dtd,
            h.Quantity - COALESCE((SELECT SUM(CASE WHEN Action = 'Buy' THEN Quantity WHEN Action = 'Sell' THEN -Quantity ELSE 0 END) FROM Investment_Transactions WHERE Securities_Id = h.Securities_Id AND Accounts_Id = h.Accounts_Id AND Date > p.wtd_start), 0) as qty_wtd,
            h.Quantity - COALESCE((SELECT SUM(CASE WHEN Action = 'Buy' THEN Quantity WHEN Action = 'Sell' THEN -Quantity ELSE 0 END) FROM Investment_Transactions WHERE Securities_Id = h.Securities_Id AND Accounts_Id = h.Accounts_Id AND Date > p.mtd_start), 0) as qty_mtd,
            h.Quantity - COALESCE((SELECT SUM(CASE WHEN Action = 'Buy' THEN Quantity WHEN Action = 'Sell' THEN -Quantity ELSE 0 END) FROM Investment_Transactions WHERE Securities_Id = h.Securities_Id AND Accounts_Id = h.Accounts_Id AND Date > p.ytd_start), 0) as qty_ytd
        FROM periods p
        CROSS JOIN Holdings h
    ),
    prices_fx AS (
        SELECT 
            hh.*,
            (SELECT Price_Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Price_Date <= hh.today ORDER BY Price_Date DESC LIMIT 1) as price_today,
            (SELECT Price_Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Price_Date <= hh.dtd_start ORDER BY Price_Date DESC LIMIT 1) as price_dtd,
            (SELECT Price_Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Price_Date <= hh.wtd_start ORDER BY Price_Date DESC LIMIT 1) as price_wtd,
            (SELECT Price_Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Price_Date <= hh.mtd_start ORDER BY Price_Date DESC LIMIT 1) as price_mtd,
            (SELECT Price_Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Price_Date <= hh.ytd_start ORDER BY Price_Date DESC LIMIT 1) as price_ytd,
            (SELECT FX_Rate FROM Historical_FX WHERE Base_Currency_Id = s.Currencies_Id AND FX_Date <= hh.today ORDER BY FX_Date DESC LIMIT 1) as fx_today,
            (SELECT FX_Rate FROM Historical_FX WHERE Base_Currency_Id = s.Currencies_Id AND FX_Date <= hh.dtd_start ORDER BY FX_Date DESC LIMIT 1) as fx_dtd,
            (SELECT FX_Rate FROM Historical_FX WHERE Base_Currency_Id = s.Currencies_Id AND FX_Date <= hh.wtd_start ORDER BY FX_Date DESC LIMIT 1) as fx_wtd,
            (SELECT FX_Rate FROM Historical_FX WHERE Base_Currency_Id = s.Currencies_Id AND FX_Date <= hh.mtd_start ORDER BY FX_Date DESC LIMIT 1) as fx_mtd,
            (SELECT FX_Rate FROM Historical_FX WHERE Base_Currency_Id = s.Currencies_Id AND FX_Date <= hh.ytd_start ORDER BY FX_Date DESC LIMIT 1) as fx_ytd,
            s.Security_Name, a.Accounts_Name
        FROM historical_holdings hh
        JOIN Securities s ON hh.Securities_Id = s.Securities_Id
        JOIN Accounts a ON hh.Accounts_Id = a.Accounts_Id
    ),
    cash_flows AS (
        SELECT 
            Accounts_Id, Securities_Id,
            SUM(CASE WHEN Date > (SELECT dtd_start FROM periods) THEN 
                (CASE 
                    WHEN Action IN ('Buy', 'MiscExp') THEN Total_Amount 
                    WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -Total_Amount 
                    ELSE 0 END) 
                ELSE 0 END) as cf_dtd,
            SUM(CASE WHEN Date > (SELECT wtd_start FROM periods) THEN 
                (CASE 
                    WHEN Action IN ('Buy', 'MiscExp') THEN Total_Amount 
                    WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -Total_Amount 
                    ELSE 0 END) 
                ELSE 0 END) as cf_wtd,
            SUM(CASE WHEN Date > (SELECT mtd_start FROM periods) THEN 
                (CASE 
                    WHEN Action IN ('Buy', 'MiscExp') THEN Total_Amount 
                    WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -Total_Amount 
                    ELSE 0 END) 
                ELSE 0 END) as cf_mtd,
            SUM(CASE WHEN Date > (SELECT ytd_start FROM periods) THEN 
                (CASE 
                    WHEN Action IN ('Buy', 'MiscExp') THEN Total_Amount 
                    WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -Total_Amount 
                    ELSE 0 END) 
                ELSE 0 END) as cf_ytd,
            SUM(CASE 
                WHEN Action IN ('Buy', 'MiscExp') THEN Total_Amount 
                WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -Total_Amount 
                ELSE 0 END) as cf_all_time,
            SUM(CASE 
                WHEN Action IN ('Buy', 'CashOut', 'MiscExp') THEN Total_Amount 
                WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'CashIn', 'RtrnCap') THEN -Total_Amount 
                ELSE 0 END) as net_invested_all_time                
        FROM Investment_Transactions
        GROUP BY Accounts_Id, Securities_Id
    )
    SELECT 
        pf.Accounts_Name, pf.Security_Name,
        (pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) as current_value_eur,
        (pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_dtd * pf.price_dtd * COALESCE(pf.fx_dtd, 1)) - COALESCE(cf.cf_dtd, 0) as pnl_dtd_eur,
        (pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_wtd * pf.price_wtd * COALESCE(pf.fx_wtd, 1)) - COALESCE(cf.cf_wtd, 0) as pnl_wtd_eur,
        (pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_mtd * pf.price_mtd * COALESCE(pf.fx_mtd, 1)) - COALESCE(cf.cf_mtd, 0) as pnl_mtd_eur,
        (pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)) - COALESCE(cf.cf_ytd, 0) as pnl_ytd_eur,
        (pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - COALESCE(cf.cf_all_time, 0) as pnl_all_time_eur,
        (pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - COALESCE(cf.net_invested_all_time, 0) as pnl_net_all_time_eur,
        (SELECT Quantity * (pf.price_today - Fifo_Avg_Price) * COALESCE(pf.fx_today, 1) FROM Holdings WHERE Accounts_Id = pf.Accounts_Id AND Securities_Id = pf.Securities_Id) as unrealized_pnl_eur      
    FROM prices_fx pf
    LEFT JOIN cash_flows cf ON pf.Accounts_Id = cf.Accounts_Id AND pf.Securities_Id = cf.Securities_Id
    ORDER BY pf.Accounts_Name, pf.Security_Name;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df