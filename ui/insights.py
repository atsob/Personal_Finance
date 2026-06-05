"""Actionable spending insight cards for the Dashboard.

Queries the database for financial patterns and renders styled cards
that surface things the user should actually act on:
  • Category overspending vs. last month
  • Unusually large recent transaction
  • Low cash balance warning
  • Positive savings rate trend
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from database.connection import get_connection
from ui.components import insight_card


@st.cache_data(ttl=300, show_spinner=False)
def _get_category_overspending() -> pd.DataFrame:
    """Find categories where last month's spending is ≥ 20 % more than the prior month."""
    conn = get_connection()
    try:
        df = pd.read_sql("""
            WITH last_month AS (
                SELECT
                    COALESCE(c.Categories_Name, 'Uncategorized') AS category,
                    ABS(SUM(t.Total_Amount))                     AS spent
                FROM Transactions t
                LEFT JOIN Splits     s ON s.Transactions_Id = t.Transactions_Id
                LEFT JOIN Categories c ON c.Categories_Id   = s.Categories_Id
                WHERE t.Date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                  AND t.Date <  DATE_TRUNC('month', CURRENT_DATE)
                  AND t.Transfers_Id IS NULL
                  AND t.Is_Draft     = FALSE
                  AND t.Total_Amount < 0
                GROUP BY 1
            ),
            prev_month AS (
                SELECT
                    COALESCE(c.Categories_Name, 'Uncategorized') AS category,
                    ABS(SUM(t.Total_Amount))                     AS spent
                FROM Transactions t
                LEFT JOIN Splits     s ON s.Transactions_Id = t.Transactions_Id
                LEFT JOIN Categories c ON c.Categories_Id   = s.Categories_Id
                WHERE t.Date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months')
                  AND t.Date <  DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                  AND t.Transfers_Id IS NULL
                  AND t.Is_Draft     = FALSE
                  AND t.Total_Amount < 0
                GROUP BY 1
            )
            SELECT
                l.category,
                ROUND(l.spent::numeric, 2)                       AS this_month,
                ROUND(COALESCE(p.spent, 0)::numeric, 2)          AS last_month,
                ROUND((l.spent - COALESCE(p.spent, 0))::numeric, 2) AS increase,
                CASE WHEN COALESCE(p.spent, 0) > 0
                     THEN ROUND(((l.spent - p.spent) / p.spent * 100)::numeric, 1)
                     ELSE NULL END                               AS pct_change
            FROM last_month l
            LEFT JOIN prev_month p ON p.category = l.category
            WHERE l.spent        > 30
              AND l.spent        > COALESCE(p.spent, 0) * 1.20
            ORDER BY increase DESC
            LIMIT 3
        """, conn)
    finally:
        conn.close()
    return df


@st.cache_data(ttl=300, show_spinner=False)
def _get_low_balance_accounts() -> pd.DataFrame:
    """Return active bank/cash accounts with balance below €300."""
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT
                a.Accounts_Name                 AS name,
                ROUND(a.Accounts_Balance::numeric, 2) AS balance,
                c.Currencies_ShortName          AS currency
            FROM Accounts a
            JOIN Currencies c ON c.Currencies_Id = a.Currencies_Id
            WHERE a.Is_Active      = TRUE
              AND a.Accounts_Type IN ('Checking', 'Savings', 'Cash')
              AND a.Accounts_Balance < 300
              AND a.Accounts_Balance > -50000   -- exclude intentional overdrafts / credit lines
            ORDER BY a.Accounts_Balance ASC
        """, conn)
    finally:
        conn.close()
    return df


@st.cache_data(ttl=300, show_spinner=False)
def _get_savings_rate_trend() -> pd.DataFrame:
    """Return savings rate for the last 3 complete months (bank accounts only)."""
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT
                DATE_TRUNC('month', t.Date)::date           AS month,
                SUM(CASE WHEN t.Total_Amount > 0
                         THEN t.Total_Amount ELSE 0 END)    AS income,
                ABS(SUM(CASE WHEN t.Total_Amount < 0
                             THEN t.Total_Amount ELSE 0 END)) AS expenses
            FROM Transactions t
            JOIN Accounts a ON a.Accounts_Id = t.Accounts_Id
            WHERE a.Accounts_Type IN ('Checking', 'Savings', 'Cash')
              AND t.Transfers_Id  IS NULL
              AND t.Is_Draft      = FALSE
              AND t.Date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
              AND t.Date <  DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1
            ORDER BY 1
        """, conn)
    finally:
        conn.close()
    if not df.empty and df['income'].sum() > 0:
        df['savings_rate'] = (
            (df['income'] - df['expenses']) / df['income'] * 100
        ).round(1)
    else:
        df['savings_rate'] = 0.0
    return df


@st.cache_data(ttl=300, show_spinner=False)
def _get_recent_large_transactions() -> pd.DataFrame:
    """Return unusually large transactions in the last 14 days (z-score ≥ 2.5)."""
    conn = get_connection()
    try:
        df = pd.read_sql("""
            WITH stats AS (
                SELECT
                    COALESCE(p.Payees_Name, 'Unknown') AS payee,
                    AVG(ABS(t.Total_Amount))            AS mean_amt,
                    STDDEV(ABS(t.Total_Amount))         AS std_amt
                FROM Transactions t
                LEFT JOIN Payees p ON p.Payees_Id = t.Payees_Id
                WHERE t.Date >= CURRENT_DATE - INTERVAL '90 days'
                  AND t.Transfers_Id IS NULL
                  AND t.Total_Amount <> 0
                GROUP BY 1
                HAVING COUNT(*) >= 3
            )
            SELECT
                t.Date                                  AS date,
                COALESCE(p.Payees_Name, t.Description, '—') AS payee,
                ROUND(t.Total_Amount::numeric, 2)       AS amount,
                ROUND(s.mean_amt::numeric, 2)           AS typical,
                ROUND(
                    ABS(ABS(t.Total_Amount) - s.mean_amt) / NULLIF(s.std_amt, 0), 1
                )                                       AS z_score
            FROM Transactions t
            LEFT JOIN Payees  p ON p.Payees_Id = t.Payees_Id
            JOIN      stats   s ON s.payee = COALESCE(p.Payees_Name, 'Unknown')
            WHERE t.Date >= CURRENT_DATE - INTERVAL '14 days'
              AND t.Is_Draft = FALSE
              AND ABS(ABS(t.Total_Amount) - s.mean_amt) / NULLIF(s.std_amt, 0) >= 2.5
            ORDER BY ABS(t.Total_Amount) DESC
            LIMIT 3
        """, conn)
    finally:
        conn.close()
    return df


@st.fragment
def render_spending_insights() -> None:
    """Render actionable spending insight cards. Uses st.fragment for isolated reruns."""

    cards: list[dict] = []

    # ── 1. Category overspending ───────────────────────────────────────────
    try:
        df_over = _get_category_overspending()
        for _, row in df_over.iterrows():
            pct  = row['pct_change']
            inc  = row['increase']
            pct_str = f"{pct:+.0f}%" if pct is not None and not pd.isna(pct) else "new category"
            cards.append({
                "icon":    "💸",
                "title":   f"Overspending · {row['category']}",
                "message": (
                    f"Last month you spent **€{row['this_month']:,.0f}** on {row['category']} — "
                    f"**{pct_str}** more than the prior month (↑ €{inc:,.0f}). "
                    "Consider reviewing this category."
                ),
                "level":   "warning" if (pct or 0) < 50 else "danger",
            })
    except Exception:
        pass

    # ── 2. Low cash balance ────────────────────────────────────────────────
    try:
        df_low = _get_low_balance_accounts()
        for _, row in df_low.iterrows():
            sym = {"EUR": "€", "USD": "$", "GBP": "£"}.get(row['currency'], row['currency'])
            cards.append({
                "icon":    "⚠️",
                "title":   f"Low balance · {row['name']}",
                "message": (
                    f"**{row['name']}** has only **{sym}{row['balance']:,.2f}** remaining. "
                    "Consider a top-up before upcoming bills."
                ),
                "level":   "danger" if row['balance'] < 100 else "warning",
            })
    except Exception:
        pass

    # ── 3. Recent unusually large transactions ─────────────────────────────
    try:
        df_large = _get_recent_large_transactions()
        for _, row in df_large.iterrows():
            cards.append({
                "icon":    "🔍",
                "title":   f"Unusual transaction · {row['payee']}",
                "message": (
                    f"**€{abs(row['amount']):,.2f}** on {row['date']} with {row['payee']} — "
                    f"{row['z_score']}× above the usual amount for this payee. "
                    "Was this expected?"
                ),
                "level":   "info",
            })
    except Exception:
        pass

    # ── 4. Savings rate trend ──────────────────────────────────────────────
    try:
        df_sr = _get_savings_rate_trend()
        if len(df_sr) >= 2:
            latest = float(df_sr.iloc[-1]["savings_rate"])
            prev   = float(df_sr.iloc[-2]["savings_rate"])
            delta  = latest - prev
            if latest >= 25 and delta >= 5:
                cards.append({
                    "icon":    "🎯",
                    "title":   "Savings rate improving",
                    "message": (
                        f"Your savings rate last month was **{latest:.1f}%** — "
                        f"up {delta:+.1f} pp vs. the prior month. Great work!"
                    ),
                    "level":   "success",
                })
            elif latest < 10:
                cards.append({
                    "icon":    "📉",
                    "title":   "Low savings rate",
                    "message": (
                        f"Your savings rate last month was only **{latest:.1f}%**. "
                        "Review your expenses to find areas to cut back."
                    ),
                    "level":   "warning",
                })
    except Exception:
        pass

    if not cards:
        return   # nothing to show — don't render the section at all

    with st.expander(
        f"💡 **{len(cards)} financial insight{'s' if len(cards) != 1 else ''}** — click to review",
        expanded=False,
    ):
        for c in cards:
            insight_card(c["icon"], c["title"], c["message"], c["level"])
        st.caption("Insights refresh every 5 minutes · Based on your last 90 days of transactions")
