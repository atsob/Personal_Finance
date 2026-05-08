import re
import streamlit as st
import pandas as pd
from data.qif_importer import render_qif_importer
from data.transfer_issues import render_transfer_issues
from database.backup import render_backup_restore
from database.backup import render_backup_restore_simple
from database.backup import render_backup_restore_quick
from database.connection import get_connection


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
        run = st.button("▶ Run Query", type="primary", use_container_width=True)
    with col_clear:
        if st.button("✖ Clear", use_container_width=True):
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
        st.dataframe(df, use_container_width=True, hide_index=True)
        with col_export:
            csv = df.to_csv(index=False).encode("utf-8")
            export_placeholder.download_button(
                "⬇ Export CSV",
                data=csv,
                file_name="query_result.csv",
                mime="text/csv",
                use_container_width=True,
            )


def render_tools(conn):
    """Render the Tools page."""
    st.title("System Tools")
    t1, t2, t3, t4, t5, t6 = st.tabs([
        "📁 QIF Importer",
        "📝 Transfer Issues",
        "💾 Backup & Restore",
        "💾 Backup & Restore Simple",
        "💾 Quick Backup & Restore",
        "🛢 SQL Interface",
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
        