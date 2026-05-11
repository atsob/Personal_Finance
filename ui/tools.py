import re
import streamlit as st
import pandas as pd
from data.qif_importer import render_qif_importer
from data.transfer_issues import render_transfer_issues
from data.capitalcom_importer import render_capitalcom_importer
from database.backup import render_backup_restore
from database.backup import render_backup_restore_simple
from database.backup import render_backup_restore_quick
from database.connection import get_connection
from database.queries import get_price_anomalies, get_missing_tx_prices, get_investments_with_dummy_prices
from database.crud import delete_historical_prices, insert_prices_from_transactions, normalize_investment_prices


_WRITE_PATTERN = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|MERGE|GRANT|REVOKE|CALL|DO)\b",
    re.IGNORECASE,
)


def _render_sql_interface():
    st.subheader("🛢 SQL Query Interface")
    st.caption("Read-only: only SELECT and WITH…SELECT queries are allowed.")

    default_sql = "SELECT table_name\nFROM information_schema.tables\nWHERE table_schema = 'public'\nORDER BY table_name;"

    sql = st.text_area(
        "SQL Query",
        value=st.session_state.get("sql_query", default_sql),
        height=200,
        key="sql_query",
        label_visibility="collapsed",
        placeholder="SELECT …",
    )

    col_run, col_clear, col_export = st.columns([1, 1, 1])

    with col_run:
        run = st.button("▶ Run Query", type="primary", width="stretch")
    with col_clear:
        if st.button("✖ Clear", width="stretch"):
            st.session_state["sql_query"] = default_sql
            st.session_state.pop("sql_result", None)
            st.session_state.pop("sql_error", None)
            st.rerun()
    with col_export:
        export_placeholder = st.empty()

    if run:
        query = sql.strip()
        if not query:
            st.warning("Please enter a SQL query.")
        elif _WRITE_PATTERN.match(query):
            st.error("Write statements (INSERT, UPDATE, DELETE, DROP, …) are not allowed.")
        else:
            try:
                conn = get_connection()
                df = pd.read_sql(query, conn)
                conn.close()
                st.session_state["sql_result"] = df
                st.session_state.pop("sql_error", None)
            except Exception as e:
                st.session_state["sql_error"] = str(e)
                st.session_state.pop("sql_result", None)

    if "sql_error" in st.session_state:
        st.error(st.session_state["sql_error"])

    if "sql_result" in st.session_state:
        df = st.session_state["sql_result"]
        rows, cols = df.shape
        st.caption(f"{rows:,} row(s) · {cols} column(s)")
        st.dataframe(df, width="stretch", hide_index=True)
        with col_export:
            csv = df.to_csv(index=False).encode("utf-8")
            export_placeholder.download_button(
                "⬇ Export CSV",
                data=csv,
                file_name="query_result.csv",
                mime="text/csv",
                width="stretch",
            )


def _render_price_quality():
    st.subheader("🔍 Price Data Quality")
    st.caption(
        "Flags prices that changed by more than the chosen threshold vs the previous or next "
        "trading day for the same security. The nearest buy/sell transaction is shown for context."
    )

    # ── Threshold + Refresh ───────────────────────────────────────────────
    col_thresh, col_btn = st.columns([5, 1])
    with col_thresh:
        threshold = st.slider(
            "Flag when move exceeds (%):",
            min_value=10, max_value=1000, value=100, step=10,
            help="100 % = flag any price that is more than 2× or less than ½ of its neighbour, "
                 "or that deviates from the nearest transaction by the same factor",
        )
    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Refresh", key="pq_refresh", width="stretch"):
            get_price_anomalies.clear()
            st.rerun()

    # ── Fetch all anomalies at this threshold ─────────────────────────────
    with st.spinner("Scanning price history…"):
        df_all = get_price_anomalies(float(threshold))

    if df_all.empty:
        st.success(f"No prices flagged at the {threshold} % threshold.")
        return

    # ── Security filter — only securities that actually have issues ────────
    sec_counts = df_all.groupby('security_name').size().to_dict()
    sec_names  = sorted(sec_counts.keys())
    selected_secs = st.multiselect(
        "Filter by security (showing only securities with issues):",
        options=sec_names,
        default=[],
        format_func=lambda x: f"{x}  ({sec_counts[x]} suspicious price{'s' if sec_counts[x] != 1 else ''})",
        key="pq_sec_filter",
    )
    df = df_all[df_all['security_name'].isin(selected_secs)] if selected_secs else df_all

    st.warning(f"{len(df):,} suspicious price record(s) — {df['security_name'].nunique()} security/ies.")

    # ── Editable table with Delete checkbox ───────────────────────────────
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['tx_date'] = pd.to_datetime(df['tx_date'], errors='coerce').dt.date
    df.insert(0, 'Delete', False)

    edited = st.data_editor(
        df,
        column_config={
            'Delete':        st.column_config.CheckboxColumn('🗑 Delete', default=False, pinned=True),
            'securities_id': None,
            'security_name': st.column_config.TextColumn('Security', pinned=True),
            'date':          st.column_config.DateColumn('Date'),
            'price':         st.column_config.NumberColumn('Price', format='%.4f'),
            'prev_close':    st.column_config.NumberColumn('Prev Close', format='%.4f'),
            'next_close':    st.column_config.NumberColumn('Next Close', format='%.4f'),
            'pct_vs_prev':   st.column_config.NumberColumn('% vs Prev', format='%+.1f %%'),
            'pct_vs_next':   st.column_config.NumberColumn('% vs Next', format='%+.1f %%'),
            'tx_date':       st.column_config.DateColumn('Nearest Tx Date'),
            'tx_action':     st.column_config.TextColumn('Tx Action'),
            'tx_price':      st.column_config.NumberColumn('Tx Price', format='%.4f'),
            'days_diff':     st.column_config.NumberColumn('Days to Tx', format='%d'),
            'pct_vs_tx':     st.column_config.NumberColumn('% vs Tx', format='%+.1f %%'),
        },
        disabled=[c for c in df.columns if c != 'Delete'],
        hide_index=True,
        width="stretch",
        key="pq_editor",
    )

    to_delete = edited[edited['Delete']]
    n_selected = len(to_delete)
    n_visible  = len(df)

    col_del, col_del_all, col_info = st.columns([1, 1, 3])

    with col_del:
        del_btn = st.button(
            f"🗑 Delete {n_selected} selected" if n_selected else "🗑 Delete selected",
            type="primary" if n_selected else "secondary",
            disabled=(n_selected == 0),
            width="stretch",
            key="pq_delete_btn",
        )

    with col_del_all:
        del_all_btn = st.button(
            f"🗑 Delete all {n_visible} listed",
            type="primary",
            width="stretch",
            key="pq_delete_all_btn",
        )

    with col_info:
        if n_selected:
            st.info(f"{n_selected} row(s) checked — or use 'Delete all listed' to remove every visible row.")

    if del_btn and n_selected:
        rows = to_delete[['securities_id', 'date']].to_dict('records')
        deleted = delete_historical_prices(rows)
        get_price_anomalies.clear()
        st.success(f"Deleted {deleted} price record(s).")
        st.rerun()

    if del_all_btn:
        rows = df[['securities_id', 'date']].to_dict('records')
        deleted = delete_historical_prices(rows)
        get_price_anomalies.clear()
        st.success(f"Deleted {deleted} price record(s).")
        st.rerun()


def _render_fill_missing_prices():
    st.subheader("📥 Fill Missing Prices from Transactions")
    st.caption(
        "Finds investment transactions (Buy / Sell / Reinvest / ShrIn / ShrOut) "
        "whose date has no entry in Historical Prices and uses the transaction's "
        "Price Per Share (averaged when multiple transactions exist on the same date) "
        "to fill the gap."
    )

    col_refresh, col_spacer = st.columns([1, 5])
    with col_refresh:
        if st.button("🔄 Refresh", key="fmp_refresh", use_container_width=True):
            get_missing_tx_prices.clear()
            st.rerun()

    with st.spinner("Scanning for gaps…"):
        df = get_missing_tx_prices()

    if df.empty:
        st.success("No missing prices found — every transaction date already has a Historical Price entry.")
        return

    # Security filter
    sec_counts = df.groupby('security_name').size().to_dict()
    sec_names  = sorted(sec_counts.keys())
    selected_secs = st.multiselect(
        "Filter by security (leave empty = all):",
        options=sec_names,
        default=[],
        format_func=lambda x: f"{x}  ({sec_counts[x]} missing date{'s' if sec_counts[x] != 1 else ''})",
        key="fmp_sec_filter",
    )
    df_view = df[df['security_name'].isin(selected_secs)] if selected_secs else df

    st.info(
        f"{len(df_view):,} missing price record(s) across "
        f"{df_view['security_name'].nunique()} security/ies."
    )

    df_display = df_view.copy()
    df_display['date'] = pd.to_datetime(df_display['date']).dt.date
    st.dataframe(
        df_display,
        column_config={
            'securities_id':  None,
            'security_name':  st.column_config.TextColumn('Security'),
            'date':           st.column_config.DateColumn('Date'),
            'price':          st.column_config.NumberColumn('Avg Tx Price', format='%.4f'),
            'actions':        st.column_config.TextColumn('Actions'),
            'tx_count':       st.column_config.NumberColumn('Tx Count', format='%d'),
        },
        hide_index=True,
        use_container_width=True,
    )

    col_ins, col_ins_all, col_info = st.columns([1, 1, 3])

    with col_ins:
        if st.button(
            f"📥 Insert for filtered ({len(df_view):,})" if selected_secs else "📥 Insert all",
            type="primary",
            use_container_width=True,
            key="fmp_insert_btn",
        ):
            rows = df_view[['securities_id', 'date', 'price']].to_dict('records')
            inserted = insert_prices_from_transactions(rows)
            get_missing_tx_prices.clear()
            get_price_anomalies.clear()
            st.success(f"Inserted {inserted} price record(s).")
            st.rerun()

    with col_ins_all:
        if selected_secs and st.button(
            f"📥 Insert all ({len(df):,})",
            type="secondary",
            use_container_width=True,
            key="fmp_insert_all_btn",
        ):
            rows = df[['securities_id', 'date', 'price']].to_dict('records')
            inserted = insert_prices_from_transactions(rows)
            get_missing_tx_prices.clear()
            get_price_anomalies.clear()
            st.success(f"Inserted {inserted} price record(s).")
            st.rerun()

    with col_info:
        if selected_secs:
            st.caption("First button inserts only the filtered rows; second inserts all gaps regardless of filter.")


def _render_normalize_investments():
    st.subheader("⚖ Normalize Investment Prices")
    st.caption(
        "Finds Buy / Sell / Reinvest / ShrIn / ShrOut transactions whose "
        "Price Per Share or Quantity appears to be a placeholder (whole-number dummy) "
        "while a real Historical Price exists for that date. "
        "Updates Price Per Share → actual close price and recalculates Quantity = "
        "Total Amount ÷ Price, leaving Total Amount unchanged so P&L is preserved."
    )

    col_refresh, _ = st.columns([1, 5])
    with col_refresh:
        if st.button("🔄 Refresh", key="ni_refresh", use_container_width=True):
            get_investments_with_dummy_prices.clear()
            st.rerun()

    with st.spinner("Scanning investments…"):
        df = get_investments_with_dummy_prices()

    if df.empty:
        st.success("No investments with dummy prices found — all transactions already use actual prices.")
        return

    # ── Account filter (first level) ─────────────────────────────────────────
    acc_counts = df.groupby('account_name').size().to_dict()
    acc_names = sorted(acc_counts.keys())
    selected_accs = st.multiselect(
        "Filter by account (leave empty = all):",
        options=acc_names,
        default=[],
        format_func=lambda x: f"{x}  ({acc_counts[x]} row{'s' if acc_counts[x] != 1 else ''})",
        key="ni_acc_filter",
    )
    df_acc = df[df['account_name'].isin(selected_accs)] if selected_accs else df

    # ── Security filter (second level, scoped to selected accounts) ──────────
    sec_counts = df_acc.groupby('security_name').size().to_dict()
    sec_names = sorted(sec_counts.keys())
    selected_secs = st.multiselect(
        "Filter by security (leave empty = all):",
        options=sec_names,
        default=[],
        format_func=lambda x: f"{x}  ({sec_counts[x]} row{'s' if sec_counts[x] != 1 else ''})",
        key="ni_sec_filter",
    )
    df_view = df_acc[df_acc['security_name'].isin(selected_secs)] if selected_secs else df_acc

    is_filtered = bool(selected_accs or selected_secs)
    st.info(
        f"{len(df_view):,} transaction(s) with dummy prices across "
        f"{df_view['account_name'].nunique()} account(s) · "
        f"{df_view['security_name'].nunique()} security/ies."
    )

    # ── Position-closure sanity check ────────────────────────────────────────
    # Compute expected net qty per (account, security) after normalization.
    # Buys contribute +new_qty, sells contribute -new_qty.
    pos_check = df_view.copy()
    pos_check['signed_new_qty'] = pos_check.apply(
        lambda r: r['new_qty'] if r['action'] in ('Buy', 'Reinvest', 'ShrIn') else -r['new_qty'],
        axis=1,
    )
    net_pos = (
        pos_check.groupby(['account_name', 'security_name'])['signed_new_qty']
        .sum()
        .reset_index()
        .rename(columns={'signed_new_qty': 'net_qty'})
    )
    non_zero = net_pos[net_pos['net_qty'].abs() > 0.0001]
    if not non_zero.empty:
        msg = "**Position closure warning** — after normalization the following will have a non-zero net holding:\n"
        for _, row in non_zero.iterrows():
            msg += f"- {row['account_name']} / {row['security_name']}: net qty = {row['net_qty']:+.6f}\n"
        st.warning(msg)

    df_display = df_view.copy()
    df_display['date'] = pd.to_datetime(df_display['date']).dt.date
    st.dataframe(
        df_display,
        column_config={
            'investments_id':  None,
            'accounts_id':     None,
            'securities_id':   None,
            'account_name':    st.column_config.TextColumn('Account'),
            'security_name':   st.column_config.TextColumn('Security'),
            'date':            st.column_config.DateColumn('Date'),
            'action':          st.column_config.TextColumn('Action'),
            'total_amount':    st.column_config.NumberColumn('Total Amount', format='%.4f'),
            'current_qty':     st.column_config.NumberColumn('Current Qty', format='%.6f'),
            'current_price':   st.column_config.NumberColumn('Current Price', format='%.4f'),
            'hist_price':      st.column_config.NumberColumn('Hist. Close', format='%.4f',
                                   help='Historical close price on that date'),
            'new_qty':         st.column_config.NumberColumn('New Qty', format='%.6f',
                                   help='Buys: total/hist_price  •  Sells: proportional from buy qty'),
            'new_price':       st.column_config.NumberColumn('New Price', format='%.4f',
                                   help='Buys: hist close  •  Sells: effective realised price (total/qty)'),
        },
        hide_index=True,
        use_container_width=True,
    )
    st.caption(
        "**Buys**: Price ← hist close, Qty ← Total ÷ hist close.  "
        "**Sells**: Qty is distributed from the total buy quantity so the position closes; "
        "Price is the effective realised price (Total ÷ Qty), not the hist close."
    )

    col_norm, col_norm_all, col_info = st.columns([1, 1, 3])

    with col_norm:
        label = (
            f"⚖ Normalize filtered ({len(df_view):,})" if is_filtered else "⚖ Normalize all"
        )
        if st.button(label, type="primary", use_container_width=True, key="ni_norm_btn"):
            ids = df_view['investments_id'].tolist()
            updated = normalize_investment_prices(ids)
            get_investments_with_dummy_prices.clear()
            st.success(f"Updated {updated} investment row(s).")
            st.rerun()

    with col_norm_all:
        if is_filtered and st.button(
            f"⚖ Normalize all ({len(df):,})",
            type="secondary",
            use_container_width=True,
            key="ni_norm_all_btn",
        ):
            ids = df['investments_id'].tolist()
            updated = normalize_investment_prices(ids)
            get_investments_with_dummy_prices.clear()
            st.success(f"Updated {updated} investment row(s).")
            st.rerun()

    with col_info:
        if is_filtered:
            st.caption("First button normalizes only filtered rows; second normalizes all flagged rows.")

    # Import here to avoid circular dependency at module level
    from database.crud import update_holdings as _update_holdings
    st.divider()
    col_rh, col_rh_info = st.columns([1, 4])
    with col_rh:
        if st.button("🔄 Refresh Holdings", use_container_width=True, key="ni_refresh_holdings"):
            _update_holdings()
            st.success("Holdings recalculated.")
    with col_rh_info:
        st.caption("Recalculates the Holdings table from Investments data. Run this after normalization to update portfolio quantities and P&L.")


def render_tools(conn):
    """Render the Tools page."""
    st.title("System Tools")
    t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 = st.tabs([
        "📁 QIF Importer",
        "📝 Transfer Issues",
        "💾 Backup & Restore",
        "💾 Backup & Restore Simple",
        "💾 Quick Backup & Restore",
        "🛢 SQL Interface",
        "🔍 Price Quality",
        "📥 Fill Missing Prices",
        "⚖ Normalize Investments",
        "📈 Capital.com Importer",
    ])

    with t1:
        render_qif_importer()

    with t2:
        render_transfer_issues()

    with t3:
        render_backup_restore()

    with t4:
        render_backup_restore_simple()

    with t5:
        render_backup_restore_quick()

    with t6:
        _render_sql_interface()

    with t7:
        _render_price_quality()

    with t8:
        _render_fill_missing_prices()

    with t9:
        _render_normalize_investments()

    with t10:
        render_capitalcom_importer()
