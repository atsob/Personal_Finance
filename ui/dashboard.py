import streamlit as st
import pandas as pd
from ui.components import format_qty_display, color_negative_red, style_qty_display
from database.queries import get_hist_net_worth_data, get_transaction_anomalies, get_weekly_summaries
from database.crud import update_accounts_balances, update_holdings, update_investment_balances, update_pension_balances
from datetime import datetime
from ai.weekly_summary import run as run_weekly_summary
from ai.monthly_summary import run as run_monthly_summary

def render_ai_monthly_summaries_ui(conn):
#    st.header("Monthly AI Financial Summary")

    # 1. Βρίσκουμε την 1η του προηγούμενου μήνα
    # Αν σήμερα είναι 8 Μαΐου, το last_month_start θα γίνει 2026-04-01
    last_month_start = (pd.Timestamp.now() - pd.offsets.MonthEnd(1)).replace(day=1)

    # 2. Δημιουργούμε τη λίστα ξεκινώντας από εκεί και πηγαίνοντας προς τα πίσω
    month_options = [
        (last_month_start - pd.offsets.DateOffset(months=i)).strftime('%Y-%m-01') 
        for i in range(24)
    ]

    # Selectbox για επιλογή μήνα
    selected_month = st.selectbox(
        "Select Month", 
        options=month_options,
        format_func=lambda x: pd.to_datetime(x).strftime('%B %Y'),
        index=0 # Προεπιλογή ο πιο πρόσφατος (Απρίλιος)
    )

    # 2. Έλεγχος αν υπάρχει ήδη summary στη βάση
    # Υποθέτουμε ότι ο πίνακας έχει στήλες 'month' και 'summary_text'
    existing_data = pd.read_sql(
        "SELECT summary_text FROM ai_monthly_summaries WHERE month_start = %s", 
        conn, 
        params=(selected_month,)
    )
#    conn.close()

    # 3. Εμφάνιση αποτελέσματος ή κουμπί εκτέλεσης
    if not existing_data.empty:
        st.success(f"Summary found for {pd.to_datetime(selected_month).strftime('%B %Y')}:")
        st.markdown(existing_data.iloc[0]['summary_text'])
        
        # Προαιρετικά: Κουμπί για επανεκτέλεση (Regenerate)
        if st.button("Regenerate Summary"):
            with st.spinner("Generating new summary..."):
                run_monthly_summary(target_month=selected_month)
                st.rerun()
    else:
        st.warning("No summary found for this month.")
        if st.button("Generate AI Summary Now"):
            with st.spinner("Analyzing financial data and generating summary..."):
                try:
                    run_monthly_summary(target_month=selected_month)
                    st.success("Summary generated successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")


def render_dashboard(conn):
    """Render the Dashboard page."""
#    st.title("🏛 Net Worth (incl. future registered transactions)")
    st.markdown(
        f"<h1>🏛 Net Worth <span style='font-size: 0.6em; opacity: 0.7;'>(incl. future registered transactions)</span></h1>", 
        unsafe_allow_html=True
    )

    query_combined = """
        WITH Latest_FX AS (
            SELECT DISTINCT ON (Currencies_Id_1) Currencies_Id_1, FX_Rate 
            FROM Historical_FX 
            ORDER BY Currencies_Id_1, Date DESC
        ),
        Latest_Prices AS (
            SELECT DISTINCT ON (Securities_Id) Securities_Id, Close 
            FROM Historical_Prices 
            ORDER BY Securities_Id, Date DESC
        )
            -- ASSETS
        SELECT a.Accounts_Name as name, 'Assets' as type, c.Currencies_ShortName as curr, a.Accounts_Balance as qty,
               CASE WHEN c.Currencies_ShortName = 'EUR' THEN a.Accounts_Balance ELSE a.Accounts_Balance * COALESCE(fx.FX_Rate, 1) END as value_eur
        FROM Accounts a 
        LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_FX fx ON a.Currencies_Id = fx.Currencies_Id_1 
        WHERE a.Is_Active = TRUE AND a.Accounts_Type NOT IN ('Cash', 'Checking', 'Savings', 'Credit Card', 'Brokerage', 'Pension', 'Other Investment', 'Margin', 'Loan', 'Other')
        UNION ALL
            -- CASH FROM NON-INVESTMENT ACCOUNTS
        SELECT a.Accounts_Name as name, 'Cash' as type, c.Currencies_ShortName as curr, a.Accounts_Balance as qty,
               CASE WHEN c.Currencies_ShortName = 'EUR' THEN a.Accounts_Balance ELSE a.Accounts_Balance * COALESCE(fx.FX_Rate, 1) END as value_eur
        FROM Accounts a 
        LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_FX fx ON a.Currencies_Id = fx.Currencies_Id_1 
        WHERE a.Is_Active = TRUE AND a.Accounts_Type NOT IN ('Brokerage', 'Pension', 'Other Investment', 'Margin', 'Real Estate', 'Vehicle', 'Asset', 'Liability')
        UNION ALL
            -- CASH FROM OTHER INVESTMENT ACCOUNTS
        SELECT a.Accounts_Name as name, 'Cash' as type, c.Currencies_ShortName as curr, a.Accounts_Balance as qty,
               CASE WHEN c.Currencies_ShortName = 'EUR' THEN a.Accounts_Balance ELSE a.Accounts_Balance * COALESCE(fx.FX_Rate, 1) END as value_eur
        FROM Accounts a 
        LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_FX fx ON a.Currencies_Id = fx.Currencies_Id_1 
        WHERE a.Is_Active = TRUE AND a.Accounts_Type IN ('Other Investment')
        UNION ALL
            -- PENSION ACCOUNTS
        SELECT a.Accounts_Name as name, 'Pension' as type, c.Currencies_ShortName as curr, a.Accounts_Balance as qty,
               CASE WHEN c.Currencies_ShortName = 'EUR' THEN a.Accounts_Balance ELSE a.Accounts_Balance * COALESCE(fx.FX_Rate, 1) END as value_eur
        FROM Accounts a 
        LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id 
        LEFT JOIN Latest_FX fx ON a.Currencies_Id = fx.Currencies_Id_1 
        WHERE a.Is_Active = TRUE AND a.Accounts_Type IN ('Pension')
        UNION ALL
            -- INVESTMENTS
        SELECT 
            s.Securities_Name as name, 
            'Investment' as type, 
            c.Currencies_ShortName as curr, 
            SUM(h.Quantity) as qty,
            SUM(CASE 
                WHEN c.Currencies_ShortName = 'EUR' THEN h.Quantity * COALESCE(lp.Close, 0) 
                ELSE (h.Quantity * COALESCE(lp.Close, 0)) * COALESCE(fx.FX_Rate, 1) 
            END) as value_eur
        FROM Holdings h
        JOIN Securities s ON s.Securities_Id = h.Securities_Id
        JOIN Currencies c ON c.Currencies_Id = s.Currencies_Id
        -- Χρησιμοποιούμε INNER JOIN για τις τιμές ώστε να μην φέρνει holdings χωρίς τιμή
        JOIN Latest_Prices lp ON lp.Securities_Id = h.Securities_Id
        -- To LEFT JOIN με την Latest_FX είναι πλέον ασφαλές λόγω του DISTINCT ON
        LEFT JOIN Latest_FX fx ON s.Currencies_Id = fx.Currencies_Id_1
        WHERE h.Quantity <> 0
        GROUP BY name, type, curr
        ORDER BY type ASC, value_eur DESC
    """
    
    df_net = pd.read_sql(query_combined, conn)
    df_net.columns = [c.lower() for c in df_net.columns]
    df_net['type'] = df_net['type'].str.strip()
    df_net['qty_display'] = df_net.apply(format_qty_display, axis=1)
    
    # Display metrics
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Net Worth", f"€ {df_net['value_eur'].sum():,.2f}")
    m2.metric("Assets", f"€ {df_net[df_net['type']=='Assets']['value_eur'].sum():,.2f}")
    m3.metric("Cash", f"€ {df_net[df_net['type']=='Cash']['value_eur'].sum():,.2f}")
    m4.metric("Pension", f"€ {df_net[df_net['type']=='Pension']['value_eur'].sum():,.2f}")
    m5.metric("Investments", f"€ {df_net[df_net['type']=='Investment']['value_eur'].sum():,.2f}")
    
    # Apply Style and Formatting
    # We define the new order (qty_display before value_eur)
    new_order = ['name', 'type', 'curr', 'qty', 'qty_display', 'value_eur']
    df_net = df_net.reindex(columns=new_order)
    # We use .style for color and format simultaneously
    styled_df = df_net.style \
        .map(color_negative_red, subset=['value_eur', 'qty']) \
        .apply(lambda x: style_qty_display(df_net), subset=['qty_display'], axis=0) \
        .format({
            "qty": "{:,.2f}",
            "value_eur": "{:,.2f} €"
        }) \
        .hide(['qty'], axis=1)  # If this doesn't work, try: .hide(subset=['qty'], axis=1)

    # Display the Styled DataFrame
    # We define the order we want, skipping the 'qty' column
    st.dataframe(
        styled_df, 
        width="stretch", 
        hide_index=True,
        column_order=("name", "type", "curr", "qty_display", "value_eur"),
        column_config={
            "name": "Description",
            "type": "Category",
            "curr": "Currency",
            "qty_display": "Value / Quantity",
            "value_eur": "Value (€)"
        }
    )

    # Update buttons
    # Custom CSS to center the "Update All" button row and add spacing
    st.markdown("""
        <style>
        div.stButton > button {
            border-radius: 5px;
            height: 3em;
            font-weight: bold;
        }
        </style>
    """, unsafe_allow_html=True)

    st.subheader("Balances Synchronization")

    # Create a 2x2 grid for specific updates
    grid = st.columns(2)

    # Define the buttons in a list to keep code DRY (Don't Repeat Yourself)
    tasks = [
        ("Bank & Cash Accounts", update_accounts_balances),
        ("Investment Cash Accounts", update_investment_balances),
        ("Pension Accounts", update_pension_balances),
        ("Security Holdings", update_holdings)
    ]

    for i, (label, func) in enumerate(tasks):
        with grid[i % 2]:
            if st.button(f"🔄 Update {label}", width='stretch'):
                with st.spinner(f"Updating {label}..."):
                    func()
                    st.toast(f"{label} updated!") # Less intrusive than balloons for small tasks
                    st.rerun()  # Refresh the page to show updated data

    st.markdown("---")

    # Center the "Update All" button and make it prominent
    _, center_col, _ = st.columns([1, 2, 1])

    with center_col:
        # use 'primary' type to give it the brand color
        if st.button("🚀 Run Full Update", type="primary", width='stretch'):
            with st.spinner("Processing full update..."):
                update_accounts_balances()
                update_investment_balances()
                update_pension_balances()
                update_holdings()
                st.balloons()
                st.success("All balances up to date!")
                st.rerun()

    # ------------------------------------------------------------------
    # AI WEEKLY SUMMARY
    # ------------------------------------------------------------------
    st.markdown("---")
    st.subheader("🧠 AI Weekly Summary")

    try:
        df_summaries = get_weekly_summaries()
        if df_summaries.empty:
            st.info("No weekly summary available yet. The scheduler generates one every Monday at 07:00, or run `python -m ai.weekly_summary` manually.")
        else:
            latest = df_summaries.iloc[0]
            week_label = latest['week_start'].strftime("Week of %B %d, %Y")
            generated  = latest['generated_at'].strftime("%Y-%m-%d %H:%M")
            st.caption(f"{week_label} · generated {generated}")
            st.markdown(latest['summary_text'])

            if len(df_summaries) > 1:
                with st.expander("📚 Previous summaries"):
                    for _, row in df_summaries.iloc[1:].iterrows():
                        st.markdown(
                            f"**{row['week_start'].strftime('Week of %B %d, %Y')}** "
                            f"<span style='opacity:.6;font-size:.85em'>(generated {row['generated_at'].strftime('%Y-%m-%d')})</span>",
                            unsafe_allow_html=True,
                        )
                        st.markdown(row['summary_text'])
                        st.divider()
    except Exception as e:
        st.info(f"Weekly summary unavailable: {e}")

    # ------------------------------------------------------------------
    # AI MONTHLY SUMMARIES
    # ------------------------------------------------------------------
    st.markdown("---")
    st.subheader("🧠 AI Monthly Summaries")
    render_ai_monthly_summaries_ui(conn)

    # ------------------------------------------------------------------
    # ANOMALY DETECTION
    # ------------------------------------------------------------------
    st.markdown("---")
    st.subheader("🚨 Unusual Transactions (Last 30 Days)")
    st.caption("Transactions whose amount is ≥ 2.5 standard deviations from the typical amount for that payee & category.")

    try:
        df_anomalies = get_transaction_anomalies(2.5, 30)
        if df_anomalies.empty:
            st.success("No unusual transactions detected in the last 30 days.")
        else:
            st.warning(f"{len(df_anomalies)} unusual transaction(s) detected.")
            df_anomalies['date'] = pd.to_datetime(df_anomalies['date']).dt.strftime('%Y-%m-%d')
            st.dataframe(
                df_anomalies.style.format({
                    'amount_eur': '{:,.2f} €',
                    'mean_eur':   '{:,.2f} €',
                    'std_eur':    '{:,.2f} €',
                    'z_score':    '{:+.2f}',
                }).map(
                    lambda v: 'color: #E74C3C; font-weight: bold' if isinstance(v, float) and abs(v) >= 3 else '',
                    subset=['z_score']
                ),
                hide_index=True, width='stretch',
                column_config={
                    'date':         'Date',
                    'payees_name':  'Payee',
                    'category':     'Category',
                    'accounts_name':'Account',
                    'amount_eur':   st.column_config.NumberColumn('Amount (€)',   format='%,.2f €'),
                    'mean_eur':     st.column_config.NumberColumn('Typical (€)',  format='%,.2f €'),
                    'std_eur':      st.column_config.NumberColumn('Std Dev (€)',  format='%,.2f €'),
                    'z_score':      st.column_config.NumberColumn('Z-Score',      format='%+.2f',
                                        help="How many standard deviations from the mean for this payee/category"),
                }
            )
    except Exception as e:
        st.info(f"Anomaly detection unavailable: {e}")
