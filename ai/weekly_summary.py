"""
Weekly AI Financial Summary
===========================
Run this script (or schedule it via cron / Task Scheduler) once a week.
It queries key financial metrics, asks the LLM to produce a plain-English
summary, and saves the result to the database table `AI_Weekly_Summaries`.

Usage:
    python -m ai.weekly_summary

Schedule (Linux cron — every Monday at 07:00):
    0 7 * * 1 cd /path/to/app && python -m ai.weekly_summary >> logs/weekly_summary.log 2>&1

Schedule (Windows Task Scheduler):
    Program : python
    Arguments: -m ai.weekly_summary
    Start in : C:\\path\\to\\app
"""

import logging
import textwrap
import warnings
from datetime import date, timedelta

# Suppress Streamlit cache warnings that fire when @st.cache_data decorated
# functions are imported outside a running Streamlit server.
warnings.filterwarnings("ignore", message="No runtime found", module="streamlit")
# Suppress pandas warning about non-SQLAlchemy DBAPI2 connections.
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

import pandas as pd

from config.settings import ENV_CONFIG
from database.connection import get_connection
from ai.llm import init_llm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _query(conn, sql: str, params=None) -> pd.DataFrame:
    """Execute *sql* via psycopg2 cursor and return a DataFrame — no SQLAlchemy needed."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ──────────────────────────────────────────────────────────────────────────────
# SCHEMA  (run once to create the summary table)
# ──────────────────────────────────────────────────────────────────────────────
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS AI_Weekly_Summaries (
    Summary_Id   SERIAL PRIMARY KEY,
    Week_Start   DATE NOT NULL,
    Generated_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Summary_Text TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_weekly_summaries_week
    ON AI_Weekly_Summaries(Week_Start);
"""


def ensure_summary_table(conn):
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# DATA GATHERING
# ──────────────────────────────────────────────────────────────────────────────

def _gather_context(conn) -> str:
    """Pull key metrics from the DB and format them as a compact text block."""
    week_ago = (date.today() - timedelta(days=7)).isoformat()

    blocks = []

    # 1. Net worth snapshot
    try:
        df = _query(conn, """
            WITH fx AS (
                SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate
                FROM Historical_FX ORDER BY Currencies_Id_1, Date DESC
            ),
            prices AS (
                SELECT DISTINCT ON (Securities_Id) Securities_Id, Close
                FROM Historical_Prices ORDER BY Securities_Id, Date DESC
            ),
            account_totals AS (
                SELECT
                    SUM(CASE WHEN a.Accounts_Type NOT IN ('Brokerage','Pension','Other Investment','Margin',
                                                          'Real Estate','Vehicle','Asset','Liability')
                        THEN a.Accounts_Balance * COALESCE(fx.FX_Rate,1) ELSE 0 END) AS cash_eur,
                    SUM(CASE WHEN a.Accounts_Type IN ('Pension')
                        THEN a.Accounts_Balance * COALESCE(fx.FX_Rate,1) ELSE 0 END) AS pension_eur,
                    SUM(CASE WHEN a.Accounts_Type IN ('Real Estate','Vehicle','Asset','Liability')
                        THEN a.Accounts_Balance * COALESCE(fx.FX_Rate,1) ELSE 0 END) AS assets_eur
                FROM Accounts a
                LEFT JOIN fx ON fx.Currencies_Id_1 = a.Currencies_Id
                WHERE a.Is_Active = TRUE
            ),
            investment_totals AS (
                SELECT
                    SUM(h.Quantity * COALESCE(p.Close,0) * COALESCE(fx.FX_Rate,1)) AS investments_eur
                FROM Holdings h
                JOIN Securities s ON h.Securities_Id = s.Securities_Id
                LEFT JOIN prices p ON p.Securities_Id = h.Securities_Id
                LEFT JOIN fx      ON fx.Currencies_Id_1 = s.Currencies_Id
                WHERE h.Quantity > 0
            )
            SELECT
                at.cash_eur,
                at.pension_eur,
                at.assets_eur,
                it.investments_eur
            FROM account_totals at
            CROSS JOIN investment_totals it
        """, conn)
        if not df.empty:
            r = df.iloc[0]
            total = sum(float(v or 0) for v in r)
            blocks.append(
                f"NET WORTH SNAPSHOT:\n"
                f"  Cash: €{float(r.cash_eur or 0):,.0f}  "
                f"Investments: €{float(r.investments_eur or 0):,.0f}  "
                f"Pension: €{float(r.pension_eur or 0):,.0f}  "
                f"Assets: €{float(r.assets_eur or 0):,.0f}  "
                f"TOTAL: €{total:,.0f}"
            )
    except Exception as e:
        blocks.append(f"NET WORTH: unavailable ({e})")

    # 2. Weekly income & expenses
    try:
        df = _query(conn, """
            SELECT
                SUM(CASE WHEN c.Categories_Type = 'Income' THEN s.Amount ELSE 0 END) AS income,
                SUM(CASE WHEN c.Categories_Type = 'Expense' THEN s.Amount ELSE 0 END) AS expense,
                SUM(CASE WHEN c.Categories_Type = 'Tax' THEN s.Amount ELSE 0 END) AS tax,
                SUM(CASE WHEN c.Categories_Type = 'Interest' THEN s.Amount ELSE 0 END) AS interest,
                SUM(CASE WHEN c.Categories_Type NOT IN ('Income', 'Expense', 'Tax', 'Interest') THEN t.Total_Amount ELSE 0 END) AS other
            FROM Splits s
            JOIN Categories c ON c.Categories_Id = s.Categories_Id
			JOIN Transactions t ON t.Transactions_Id = s.Transactions_Id
            WHERE t.Date >= %s
            AND t.Date <= CURRENT_DATE	
        """, (week_ago,))
        if not df.empty:
            r = df.iloc[0]
            blocks.append(
                f"WEEKLY CASH FLOWS (last 7 days):\n"
                f"  Income: €{float(r.income or 0):,.2f}  "
                f"Expenses: €{float(r.expense or 0):,.2f}  "
                f"Tax: €{float(r.tax or 0):,.2f}  "
                f"Interest: €{float(r.interest or 0):,.2f}  "
                f"Other: €{float(r.other or 0):,.2f}  "
                f"Net: €{float((r.income or 0) + (r.expense or 0) + (r.tax or 0) + (r.interest or 0) + (r.other or 0)):,.2f}"
            )
    except Exception as e:
        blocks.append(f"WEEKLY CASH FLOWS: unavailable ({e})")

    # 3. Investment P&L this week
    try:
        df = _query(conn, """
            WITH RECURSIVE 
                periods AS (
                    SELECT 
                        %s::date as wtd_start,
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
                        COALESCE(inv.qty_today, 0) as qty_today,
                        COALESCE(inv.qty_wtd,   0) as qty_wtd
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
                                FILTER (WHERE Date <= p.wtd_start) AS qty_wtd
                        FROM Investments
                        WHERE Accounts_Id = he.Accounts_Id AND Securities_Id = he.Securities_Id
                    ) inv ON true
                ),
                prices_fx AS (
                    SELECT
                        hh.*,
                        hp_today.Close  AS price_today,
                        hp_wtd.Close    AS price_wtd,
                        fx_today.FX_Rate AS fx_today,
                        fx_wtd.FX_Rate   AS fx_wtd,
                        s.Securities_Name, a.Accounts_Name, s.Currencies_Id AS sec_curr_id
                    FROM historical_holdings hh
                    JOIN Securities s ON hh.Securities_Id = s.Securities_Id
                    JOIN Accounts   a ON hh.Accounts_Id   = a.Accounts_Id
                    -- One scan per security to find latest price date for each period cutoff
                    LEFT JOIN LATERAL (
                        SELECT
                            MAX(Date) FILTER (WHERE Date <= hh.today)      AS d_today,
                            MAX(Date) FILTER (WHERE Date <= hh.wtd_start)  AS d_wtd
                        FROM Historical_Prices WHERE Securities_Id = hh.Securities_Id
                    ) pd ON true
                    LEFT JOIN Historical_Prices hp_today ON hp_today.Securities_Id = hh.Securities_Id AND hp_today.Date = pd.d_today
                    LEFT JOIN Historical_Prices hp_wtd   ON hp_wtd.Securities_Id   = hh.Securities_Id AND hp_wtd.Date   = pd.d_wtd
                    -- One scan per currency to find latest FX date for each period cutoff
                    LEFT JOIN LATERAL (
                        SELECT
                            MAX(Date) FILTER (WHERE Date <= hh.today)      AS d_today,
                            MAX(Date) FILTER (WHERE Date <= hh.wtd_start)  AS d_wtd
                        FROM Historical_FX WHERE Currencies_Id_1 = s.Currencies_Id
                    ) fxd ON true
                    LEFT JOIN Historical_FX fx_today ON fx_today.Currencies_Id_1 = s.Currencies_Id AND fx_today.Date = fxd.d_today
                    LEFT JOIN Historical_FX fx_wtd   ON fx_wtd.Currencies_Id_1   = s.Currencies_Id AND fx_wtd.Date   = fxd.d_wtd
                ),
                cash_flows AS (
                    SELECT
                        i.Accounts_Id, i.Securities_Id,
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
                        -- Συνολικό CF (για Realized P&L)
                        SUM(CASE WHEN i.Action IN ('Buy', 'MiscExp', 'Reinvest', 'Exercise', 'ShrIn') THEN COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                                WHEN i.Action IN ('Sell', 'Dividend', 'IntInc', 'RtrnCap', 'ShrOut') THEN -COALESCE(NULLIF(i.Total_Amount, 0), i.Quantity * i.Price_Per_Share)
                                ELSE 0 END) AS cf_all_time
                    FROM Investments i
                    JOIN Accounts a ON i.Accounts_Id = a.Accounts_Id
                    LEFT JOIN Historical_FX hfx
                        ON hfx.Currencies_Id_1 = a.Currencies_Id
                        AND hfx.Date = i.Date
                    GROUP BY i.Accounts_Id, i.Securities_Id
                )
                SELECT
                    SUM(
                        ((pf.qty_today * pf.price_today * COALESCE(pf.fx_today, 1)) - 
                        (pf.qty_wtd * pf.price_wtd * COALESCE(pf.fx_wtd, 1)) - 
                        COALESCE(cf.cf_wtd_eur, 0))
                    ) as pnl_wtd_eur
                FROM prices_fx pf
                LEFT JOIN cash_flows cf
                    ON pf.Accounts_Id = cf.Accounts_Id AND pf.Securities_Id = cf.Securities_Id
                LEFT JOIN Holdings h
                    ON h.Accounts_Id = pf.Accounts_Id AND h.Securities_Id = pf.Securities_Id
                WHERE (pf.qty_today != 0 OR cf.cf_all_time IS NOT NULL)
        """, (week_ago,))        
        if not df.empty:
        #    pnl = float(df.iloc[0]['weekly_pnl_eur'] or 0)
            pnl = float(df.iloc[0]['pnl_wtd_eur'] or 0)
            blocks.append(f"INVESTMENT P&L (week): €{pnl:+,.2f}")
    except Exception as e:
        blocks.append(f"INVESTMENT P&L: unavailable ({e})")

    # 4. Top 5 transactions this week (largest absolute amounts)
    try:
        df = _query(conn, """
            SELECT t.Date, p.Payees_Name, ABS(t.Total_Amount) AS abs_amount, t.Total_Amount
            FROM Transactions t
            LEFT JOIN Payees p ON t.Payees_Id = p.Payees_Id
            WHERE t.Date >= %s
            AND	t.Transfers_Id IS NULL  -- Exclude internal transfers to avoid cluttering the list with large but non-impactful movements
            ORDER BY ABS(t.Total_Amount) DESC
            LIMIT 5
        """, (week_ago,))
        if not df.empty:
            rows = "\n".join(
                f"  {r.date}  {r.payees_name or 'N/A':30s}  €{float(r.total_amount):+,.2f}"
                for _, r in df.iterrows()
            )
            blocks.append(f"TOP 5 TRANSACTIONS THIS WEEK:\n{rows}")
    except Exception as e:
        blocks.append(f"TOP TRANSACTIONS: unavailable ({e})")

    return "\n\n".join(blocks)


# ──────────────────────────────────────────────────────────────────────────────
# LLM SUMMARY GENERATION
# ──────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = textwrap.dedent("""\
    You are a personal finance assistant producing a concise weekly summary
    for the account holder. Use plain, friendly language. Highlight any
    noteworthy items (unusually large expenses, good investment week, etc.).
    Keep the summary under 250 words. Do not invent numbers — only use those
    provided in the CONTEXT block below. Do not invent duplicate transactions - only summarize the top 5 largest ones provided.
""")


def generate_summary(llm, context: str) -> str:
    prompt = f"{SYSTEM_PROMPT}\n\nCONTEXT:\n{context}\n\nPlease write the weekly summary:"
    try:
        response = llm.invoke(prompt)
        if hasattr(response, "content"):
            return response.content.strip()
        return str(response).strip()
    except Exception as e:
        return f"[LLM error: {e}]\n\nRaw context:\n{context}"


# ──────────────────────────────────────────────────────────────────────────────
# PERSISTENCE
# ──────────────────────────────────────────────────────────────────────────────

def save_summary(conn, week_start: date, summary_text: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO AI_Weekly_Summaries (Week_Start, Summary_Text)
            VALUES (%s, %s)
            ON CONFLICT (Week_Start)
            DO UPDATE SET Summary_Text = EXCLUDED.Summary_Text,
                          Generated_At = CURRENT_TIMESTAMP
        """, (week_start, summary_text))
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

def run():
    logging.info("Starting weekly AI financial summary generation...")

    conn = get_connection()
    try:
        ensure_summary_table(conn)

        week_start = date.today() - timedelta(days=date.today().weekday())  # Monday
        logging.info(f"Week start: {week_start}")

        context = _gather_context(conn)
        logging.info("Context gathered.")

        llm = init_llm()
        summary = generate_summary(llm, context)
        logging.info("Summary generated.")

        save_summary(conn, week_start, summary)
        logging.info("Summary saved to DB.")

        print(f"\n{'='*60}")
        print(f"WEEKLY SUMMARY  ({week_start})")
        print('='*60)
        print(summary)
        print('='*60)

    finally:
        conn.close()


if __name__ == "__main__":
    run()
