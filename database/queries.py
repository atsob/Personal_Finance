import pandas as pd
import streamlit as st
from database.connection import get_connection

from datetime import datetime, timedelta

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
            END as final_signal            
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
                -- Υπολογισμός ποσότητας σήμερα (qty_today)
                COALESCE((SELECT SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity WHEN Action IN ('Sell', 'ShrOut') THEN -Quantity ELSE 0 END) 
                        FROM Investments WHERE Securities_Id = he.Securities_Id AND Accounts_Id = he.Accounts_Id AND Date <= p.today), 0) as qty_today,
                -- Υπολογισμός ποσότητας DTD
                COALESCE((SELECT SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity WHEN Action IN ('Sell', 'ShrOut') THEN -Quantity ELSE 0 END) 
                        FROM Investments WHERE Securities_Id = he.Securities_Id AND Accounts_Id = he.Accounts_Id AND Date <= p.dtd_start), 0) as qty_dtd,
                -- Υπολογισμός ποσότητας WTD
                COALESCE((SELECT SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity WHEN Action IN ('Sell', 'ShrOut') THEN -Quantity ELSE 0 END) 
                        FROM Investments WHERE Securities_Id = he.Securities_Id AND Accounts_Id = he.Accounts_Id AND Date <= p.wtd_start), 0) as qty_wtd,
                -- Υπολογισμός ποσότητας MTD
                COALESCE((SELECT SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity WHEN Action IN ('Sell', 'ShrOut') THEN -Quantity ELSE 0 END) 
                        FROM Investments WHERE Securities_Id = he.Securities_Id AND Accounts_Id = he.Accounts_Id AND Date <= p.mtd_start), 0) as qty_mtd,
                -- Υπολογισμός ποσότητας YTD
                COALESCE((SELECT SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity WHEN Action IN ('Sell', 'ShrOut') THEN -Quantity ELSE 0 END) 
                        FROM Investments WHERE Securities_Id = he.Securities_Id AND Accounts_Id = he.Accounts_Id AND Date <= p.ytd_start), 0) as qty_ytd
            FROM periods p
            CROSS JOIN historical_entities he
        ),
        prices_fx AS (
            SELECT 
                hh.*,
                (SELECT Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Date <= hh.today ORDER BY Date DESC LIMIT 1) as price_today,
                (SELECT Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Date <= hh.dtd_start ORDER BY Date DESC LIMIT 1) as price_dtd,
                (SELECT Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Date <= hh.wtd_start ORDER BY Date DESC LIMIT 1) as price_wtd,
                (SELECT Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Date <= hh.mtd_start ORDER BY Date DESC LIMIT 1) as price_mtd,
                (SELECT Close FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id AND Date <= hh.ytd_start ORDER BY Date DESC LIMIT 1) as price_ytd,
                (SELECT FX_Rate FROM Historical_FX WHERE Currencies_Id_1 = s.Currencies_Id AND Date <= hh.today ORDER BY Date DESC LIMIT 1) as fx_today,
                (SELECT FX_Rate FROM Historical_FX WHERE Currencies_Id_1 = s.Currencies_Id AND Date <= hh.dtd_start ORDER BY Date DESC LIMIT 1) as fx_dtd,
                (SELECT FX_Rate FROM Historical_FX WHERE Currencies_Id_1 = s.Currencies_Id AND Date <= hh.wtd_start ORDER BY Date DESC LIMIT 1) as fx_wtd,
                (SELECT FX_Rate FROM Historical_FX WHERE Currencies_Id_1 = s.Currencies_Id AND Date <= hh.mtd_start ORDER BY Date DESC LIMIT 1) as fx_mtd,
                (SELECT FX_Rate FROM Historical_FX WHERE Currencies_Id_1 = s.Currencies_Id AND Date <= hh.ytd_start ORDER BY Date DESC LIMIT 1) as fx_ytd,
                s.Securities_Name, a.Accounts_Name, s.Currencies_Id as sec_curr_id
            FROM historical_holdings hh
            JOIN Securities s ON hh.Securities_Id = s.Securities_Id
            JOIN Accounts a ON hh.Accounts_Id = a.Accounts_Id
        ),
        cash_flows AS (
            SELECT 
                Accounts_Id, Securities_Id,
                -- DTD CF
                SUM(CASE WHEN Date > (SELECT dtd_start FROM periods) THEN 
                    (CASE WHEN Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share) 
                        WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share) 
                        ELSE 0 END) ELSE 0 END) as cf_dtd,
                -- WTD CF
                SUM(CASE WHEN Date > (SELECT wtd_start FROM periods) THEN 
                    (CASE WHEN Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share) 
                        WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share) 
                        ELSE 0 END) ELSE 0 END) as cf_wtd,
                -- MTD CF
                SUM(CASE WHEN Date > (SELECT mtd_start FROM periods) THEN 
                    (CASE WHEN Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share) 
                        WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share) 
                        ELSE 0 END) ELSE 0 END) as cf_mtd,
                -- YTD CF
                SUM(CASE WHEN Date > (SELECT ytd_start FROM periods) THEN 
                    (CASE WHEN Action IN ('Buy', 'MiscExp') THEN COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share) 
                        WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'Reinvest', 'RtrnCap') THEN -COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share) 
                        ELSE 0 END) ELSE 0 END) as cf_ytd,
                -- Συνολικό CF (για Realized P&L)
                SUM(CASE WHEN Action IN ('Buy', 'MiscExp', 'Reinvest', 'Exercise', 'ShrIn') THEN COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share)
                        WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'RtrnCap', 'ShrOut') THEN -COALESCE(NULLIF(Total_Amount, 0), Quantity * Price_Per_Share)
                        ELSE 0 END) as cf_all_time,

     --           SUM(CASE 
     --               WHEN Action IN ('Buy', 'CashOut', 'MiscExp') THEN Total_Amount 
     --               WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'CashIn', 'RtrnCap') THEN -Total_Amount 
     --               ELSE 0 END) as net_invested_all_time    
				SUM(CASE 
                    WHEN Action IN ('Buy', 'CashOut', 'MiscExp') THEN Total_Amount * COALESCE((SELECT FX_Rate FROM Historical_FX WHERE Date = Investments.Date AND Currencies_Id_1 = (SELECT Currencies_Id FROM Accounts WHERE Accounts_Id = Investments.Accounts_Id)), 1)
                    WHEN Action IN ('Sell', 'Dividend', 'IntInc', 'CashIn', 'RtrnCap') THEN -Total_Amount * COALESCE((SELECT FX_Rate FROM Historical_FX WHERE Date = Investments.Date AND Currencies_Id_1 = (SELECT Currencies_Id FROM Accounts WHERE Accounts_Id = Investments.Accounts_Id)), 1)
                    ELSE 0 END) as net_invested_all_time      					 
            FROM Investments
            GROUP BY Accounts_Id, Securities_Id
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
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_dtd * pf.price_dtd * COALESCE(pf.fx_dtd, 1)) - COALESCE(cf.cf_dtd, 0)) as pnl_dtd_eur,
            
            -- WTD/MTD Analysis
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_wtd * pf.price_wtd * COALESCE(pf.fx_wtd, 1)) - COALESCE(cf.cf_wtd, 0)) as pnl_wtd_eur,
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_mtd * pf.price_mtd * COALESCE(pf.fx_mtd, 1)) - COALESCE(cf.cf_mtd, 0)) as pnl_mtd_eur,
            
            -- YTD Analysis
            -- 1. P&L λόγω μεταβολής Τιμής (Market Effect YTD)
            ((pf.qty_today * pf.price_today) - (pf.qty_ytd * pf.price_ytd) - (COALESCE(cf.cf_ytd, 0) / NULLIF(COALESCE(pf.fx_today, 1), 0))) * COALESCE(pf.fx_today, 1) as pnl_ytd_market_eur,
            -- 2. P&L λόγω μεταβολής Ισοτιμίας (FX Effect YTD)
            (pf.qty_ytd * pf.price_ytd) * (COALESCE(pf.fx_today, 1) - COALESCE(pf.fx_ytd, 1)) as pnl_ytd_fx_eur,
            -- Total YTD P&L
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - (pf.qty_ytd * pf.price_ytd * COALESCE(pf.fx_ytd, 1)) - COALESCE(cf.cf_ytd, 0)) as pnl_ytd_eur,
            
            -- All Time P&L
            ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - COALESCE(cf.cf_all_time, 0)) as pnl_all_time_eur,

            -- Συνολικό P&L από την αρχή (Total Net Economic Gain)
            COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)),0) - COALESCE(cf.net_invested_all_time, 0) as pnl_net_all_time_eur,
            
            -- Unrealized P&L (FIFO based)
            (SELECT Quantity * (pf.price_today - Fifo_Avg_Price) * COALESCE(pf.fx_today, 1) 
            FROM Holdings WHERE Accounts_Id = pf.Accounts_Id AND Securities_Id = pf.Securities_Id) as unrealized_pnl_eur,
            
            -- Realized P&L (Total - Unrealized)
    --     ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - COALESCE(cf.cf_all_time, 0)) - 
            COALESCE((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)),0) - COALESCE(cf.net_invested_all_time, 0) - 
            COALESCE((SELECT Quantity * (pf.price_today - Fifo_Avg_Price) * COALESCE(pf.fx_today, 1) FROM Holdings WHERE Accounts_Id = pf.Accounts_Id AND Securities_Id = pf.Securities_Id), 0) as realized_pnl_eur,
/*
            -- Annual YOC %
            ROUND(((SELECT ABS(SUM(CASE WHEN i.Action = 'Dividend' THEN i.Total_Amount WHEN i.Action = 'Reinvest' THEN (i.Quantity * i.Price_Per_Share) ELSE 0 END)) 
                    FROM Investments i WHERE i.Securities_Id = pf.Securities_Id AND i.Accounts_Id = pf.Accounts_Id AND i.Action IN ('Dividend', 'Reinvest') AND i.Date >= CURRENT_DATE - INTERVAL '1 year') / 
                    NULLIF((SELECT Quantity * Fifo_Avg_Price FROM Holdings WHERE Accounts_Id = pf.Accounts_Id AND Securities_Id = pf.Securities_Id), 0))::numeric * 100, 2) as dividend_yoc_pct
*/
            -- Annual YOC % - Λεπτομερής Υπολογισμός με Διευκρινίσεις                   
            ROUND(
                (
                    SELECT SUM(
                        CASE 
                            -- Αν έχουμε Total_Amount (κλασικό Dividend), το παίρνουμε απευθείας
                            WHEN i.Action = 'Dividend' THEN i.Total_Amount 
                            -- Αν είναι Reinvest ή ShrIn, υπολογίζουμε την αξία βάσει Historical_Prices
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
                    )
                    FROM Investments i 
                    WHERE i.Securities_Id = pf.Securities_Id 
                    AND i.Accounts_Id = pf.Accounts_Id 
                    AND i.Action IN ('Dividend', 'Reinvest', 'ShrIn') 
                    AND i.Date >= CURRENT_DATE - INTERVAL '1 year'
                ) / 
                NULLIF(
                    (SELECT Quantity * Fifo_Avg_Price FROM Holdings 
                    WHERE Accounts_Id = pf.Accounts_Id 
                    AND Securities_Id = pf.Securities_Id), 0
                ) * 100, 8
            ) as dividend_yoc_pct
          
        FROM prices_fx pf
        LEFT JOIN cash_flows cf ON pf.Accounts_Id = cf.Accounts_Id AND pf.Securities_Id = cf.Securities_Id
        WHERE (pf.qty_today != 0 OR cf.cf_all_time IS NOT NULL) -- Εξασφαλίζει ότι βλέπουμε και κλειστές θέσεις με ιστορικό
	--	AND pf.Accounts_Id = 112
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
 