import re
import streamlit as st
import pandas as pd
from data.qif_importer import render_qif_importer
from data.transfer_issues import render_transfer_issues
from database.backup import render_backup_restore
from database.backup import render_backup_restore_simple
from database.backup import render_backup_restore_quick
from database.connection import get_connection
from database.queries import get_price_anomalies
from database.crud import delete_historical_prices


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


def render_tools(conn):
    """Render the Tools page."""
    st.title("System Tools")
    t1, t2, t3, t4, t5, t6, t7 = st.tabs([
        "📁 QIF Importer",
        "📝 Transfer Issues",
        "💾 Backup & Restore",
        "💾 Backup & Restore Simple",
        "💾 Quick Backup & Restore",
        "🛢 SQL Interface",
        "🔍 Price Quality",
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
        