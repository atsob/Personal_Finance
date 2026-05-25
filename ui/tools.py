import re
import streamlit as st
import pandas as pd
from database.backup import render_backup_restore
from database.connection import get_connection
from database.queries import get_price_anomalies, get_missing_tx_prices, get_investments_with_dummy_prices
from database.crud import delete_historical_prices, insert_prices_from_transactions, normalize_investment_prices
from ui.components import copy_df_button


# DDL operations that are always blocked (schema-destructive)
_DDL_PATTERN = re.compile(
    r"^\s*(DROP|TRUNCATE|ALTER|CREATE|REPLACE|MERGE|GRANT|REVOKE|CALL|DO)\b",
    re.IGNORECASE,
)

# DML operations that are allowed (data modifications, committed immediately)
_DML_PATTERN = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE)\b",
    re.IGNORECASE,
)


def _run_maintenance(sql: str, label: str):
    """Execute a maintenance command (VACUUM / REINDEX / ANALYZE) with autocommit."""
    conn = get_connection()
    try:
        conn.autocommit = True          # required — these cannot run inside a transaction
        with conn.cursor() as cur:
            cur.execute(sql)
        st.success(f"✅ {label} completed successfully.")
    except Exception as e:
        st.error(f"❌ {label} failed: {e}")
    finally:
        conn.close()


def _render_db_maintenance():
    st.subheader("🔧 Database Maintenance")
    st.caption(
        "VACUUM reclaims storage from dead tuples. "
        "ANALYZE updates planner statistics. "
        "REINDEX rebuilds indexes to remove bloat. "
        "All operations run with autocommit — no transaction wrapper needed."
    )

    # ── Table health overview ──────────────────────────────────────────────────
    st.markdown("#### 📊 Table Health")
    conn = get_connection()
    df_health = pd.read_sql("""
        SELECT
            relname                                         AS table_name,
            pg_size_pretty(pg_total_relation_size(relid))  AS total_size,
            pg_size_pretty(pg_relation_size(relid))        AS table_size,
            pg_size_pretty(pg_total_relation_size(relid)
                         - pg_relation_size(relid))        AS index_size,
            n_live_tup                                     AS live_rows,
            n_dead_tup                                     AS dead_rows,
            CASE WHEN n_live_tup > 0
                 THEN ROUND(n_dead_tup::numeric / n_live_tup * 100, 1)
                 ELSE 0 END                                AS dead_pct,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        ORDER BY n_dead_tup DESC, pg_total_relation_size(relid) DESC
    """, conn)
    conn.close()

    if not df_health.empty:
        for col in ['last_vacuum', 'last_autovacuum', 'last_analyze', 'last_autoanalyze']:
            df_health[col] = pd.to_datetime(df_health[col]).dt.strftime('%Y-%m-%d %H:%M').fillna('—')

        bloat_tables = df_health[df_health['dead_pct'] > 10]
        if not bloat_tables.empty:
            st.warning(
                f"⚠️ {len(bloat_tables)} table(s) have >10 % dead rows — consider running VACUUM ANALYZE."
            )

        st.dataframe(
            df_health,
            hide_index=True,
            width='stretch',
            column_config={
                'table_name':      st.column_config.TextColumn('Table'),
                'total_size':      'Total Size',
                'table_size':      'Table Size',
                'index_size':      'Index Size',
                'live_rows':       st.column_config.NumberColumn('Live Rows',  format='%,d'),
                'dead_rows':       st.column_config.NumberColumn('Dead Rows',  format='%,d'),
                'dead_pct':        st.column_config.NumberColumn('Dead %',     format='%.1f%%'),
                'last_vacuum':     'Last Vacuum',
                'last_autovacuum': 'Last Auto-Vacuum',
                'last_analyze':    'Last Analyze',
                'last_autoanalyze':'Last Auto-Analyze',
            },
        )
        copy_df_button(df_health, key="dl_tools_health")

    # ── Index health ───────────────────────────────────────────────────────────
    with st.expander("🗂 Index Usage"):
        conn = get_connection()
        df_idx = pd.read_sql("""
            SELECT
                relname                                         AS table_name,
                indexrelname                                    AS index_name,
                pg_size_pretty(pg_relation_size(indexrelid))   AS index_size,
                idx_scan                                        AS scans,
                idx_tup_read                                    AS tuples_read,
                idx_tup_fetch                                   AS tuples_fetched
            FROM pg_stat_user_indexes
            ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
        """, conn)
        conn.close()
        st.caption("Indexes sorted by scan count ascending — low-scan large indexes may be candidates for review.")
        st.dataframe(
            df_idx, hide_index=True, width='stretch',
            column_config={
                'table_name':     'Table',
                'index_name':     'Index',
                'index_size':     'Size',
                'scans':          st.column_config.NumberColumn('Scans',           format='%,d'),
                'tuples_read':    st.column_config.NumberColumn('Tuples Read',     format='%,d'),
                'tuples_fetched': st.column_config.NumberColumn('Tuples Fetched',  format='%,d'),
            },
        )
        copy_df_button(df_idx, key="dl_tools_idx")

    st.divider()

    # ── Database-wide operations ───────────────────────────────────────────────
    st.markdown("#### ⚡ Database-Wide Operations")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**ANALYZE**")
        st.caption("Updates planner statistics for all tables. Fast, safe, no locking.")
        if st.button("▶ Run ANALYZE", key="maint_analyze", width='stretch'):
            _run_maintenance("ANALYZE;", "ANALYZE")
            st.rerun()

    with col2:
        st.markdown("**VACUUM ANALYZE**")
        st.caption("Reclaims dead rows and updates statistics. Safe to run on a live database.")
        if st.button("▶ Run VACUUM ANALYZE", key="maint_vacuum_analyze", width='stretch'):
            _run_maintenance("VACUUM ANALYZE;", "VACUUM ANALYZE")
            st.rerun()

    with col3:
        st.markdown("**REINDEX DATABASE**")
        st.caption("Rebuilds all indexes. May take minutes on large databases.")
        if st.button("▶ Run REINDEX DATABASE", key="maint_reindex_db", width='stretch'):
            from config.settings import ENV_CONFIG
            _run_maintenance(
                f'REINDEX DATABASE "{ENV_CONFIG["db_name"]}";',
                "REINDEX DATABASE"
            )
            st.rerun()

    st.divider()

    # ── Per-table operations ───────────────────────────────────────────────────
    st.markdown("#### 🎯 Per-Table Operations")

    if not df_health.empty:
        table_names = df_health['table_name'].tolist()
        sel_col, op_col, run_col = st.columns([3, 2, 1])

        with sel_col:
            sel_table = st.selectbox(
                "Table", table_names, key="maint_table_sel",
                label_visibility="collapsed",
            )
        with op_col:
            sel_op = st.selectbox(
                "Operation",
                ["VACUUM ANALYZE", "VACUUM", "ANALYZE", "REINDEX TABLE", "VACUUM FULL"],
                key="maint_op_sel",
                label_visibility="collapsed",
            )
        with run_col:
            run_btn = st.button("▶ Run", key="maint_run_table", type="primary", width='stretch')

        if sel_op == "VACUUM FULL":
            st.warning(
                "⚠️ **VACUUM FULL** rewrites the entire table and holds an exclusive lock "
                "for the duration — no reads or writes are possible during this time. "
                "Only use it when dead-row bloat is severe and downtime is acceptable."
            )

        if run_btn:
            if sel_op == "REINDEX TABLE":
                sql = f'REINDEX TABLE "{sel_table}";'
            else:
                sql = f'{sel_op} "{sel_table}";'
            _run_maintenance(sql, f"{sel_op} on {sel_table}")
            st.rerun()
    else:
        st.info("No table statistics available.")

    st.divider()

    # ── Referential integrity check ────────────────────────────────────────────
    st.markdown("#### 🔗 Referential Integrity Check")
    st.caption(
        "Scans every foreign-key constraint in the public schema and counts orphaned rows "
        "(child rows whose referenced parent row no longer exists)."
    )

    ri_col, _ = st.columns([1, 4])
    with ri_col:
        run_ri = st.button("🔍 Run Integrity Check", key="maint_ri_check", width='stretch')

    if run_ri:
        conn = get_connection()
        try:
            # Discover all FK constraints (single-column FKs — covers all standard cases)
            df_fks = pd.read_sql("""
                SELECT
                    tc.constraint_name,
                    tc.table_name        AS child_table,
                    kcu.column_name      AS child_col,
                    ccu.table_name       AS parent_table,
                    ccu.column_name      AS parent_col
                FROM information_schema.table_constraints        tc
                JOIN information_schema.key_column_usage         kcu
                    ON  kcu.constraint_name = tc.constraint_name
                    AND kcu.table_schema    = tc.table_schema
                JOIN information_schema.constraint_column_usage  ccu
                    ON  ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema    = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema    = 'public'
                ORDER BY tc.table_name, tc.constraint_name
            """, conn)

            results = []
            cur = conn.cursor()
            for _, fk in df_fks.iterrows():
                child_t  = fk['child_table']
                child_c  = fk['child_col']
                parent_t = fk['parent_table']
                parent_c = fk['parent_col']
                try:
                    cur.execute(f"""
                        SELECT COUNT(*) FROM "{child_t}" c
                        WHERE c."{child_c}" IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM "{parent_t}" p
                              WHERE p."{parent_c}" = c."{child_c}"
                          )
                    """)
                    orphan_count = cur.fetchone()[0]
                except Exception as e:
                    orphan_count = f"error: {e}"

                results.append({
                    'constraint':   fk['constraint_name'],
                    'child_table':  child_t,
                    'child_col':    child_c,
                    'parent_table': parent_t,
                    'parent_col':   parent_c,
                    'orphaned_rows': orphan_count,
                })
            cur.close()
        finally:
            conn.close()

        df_ri = pd.DataFrame(results)

        # Separate issues from clean constraints
        df_issues = df_ri[df_ri['orphaned_rows'].apply(
            lambda x: isinstance(x, int) and x > 0
        )]
        df_errors = df_ri[df_ri['orphaned_rows'].apply(
            lambda x: isinstance(x, str)
        )]
        df_clean  = df_ri[df_ri['orphaned_rows'].apply(
            lambda x: isinstance(x, int) and x == 0
        )]

        if df_issues.empty and df_errors.empty:
            st.success(
                f"✅ All {len(df_clean)} foreign-key constraints are satisfied — no orphaned rows found."
            )
        else:
            if not df_issues.empty:
                st.error(
                    f"❌ {len(df_issues)} constraint(s) have orphaned rows:"
                )
                st.dataframe(
                    df_issues,
                    hide_index=True,
                    width='stretch',
                    column_config={
                        'constraint':    'Constraint',
                        'child_table':   'Child Table',
                        'child_col':     'Child Column',
                        'parent_table':  'Parent Table',
                        'parent_col':    'Parent Column',
                        'orphaned_rows': st.column_config.NumberColumn(
                            'Orphaned Rows', format='%,d'
                        ),
                    },
                )
                copy_df_button(df_issues, key="dl_tools_ri_issues")

            if not df_errors.empty:
                st.warning(f"⚠️ {len(df_errors)} constraint(s) could not be checked (see below):")
                st.dataframe(df_errors, hide_index=True, width='stretch')
                copy_df_button(df_errors, key="dl_tools_ri_errors")

        if not df_clean.empty:
            with st.expander(f"✅ {len(df_clean)} clean constraint(s)"):
                st.dataframe(
                    df_clean,
                    hide_index=True,
                    width='stretch',
                    column_config={
                        'constraint':    'Constraint',
                        'child_table':   'Child Table',
                        'child_col':     'Child Column',
                        'parent_table':  'Parent Table',
                        'parent_col':    'Parent Column',
                        'orphaned_rows': 'Orphaned Rows',
                    },
                )
                copy_df_button(df_clean, key="dl_tools_ri_clean")


def _render_sql_interface():
    st.subheader("🛢 SQL Query Interface")
    st.caption("SELECT, INSERT, UPDATE and DELETE are allowed. DROP, TRUNCATE, ALTER, CREATE and other DDL statements are blocked.")

    default_sql = "SELECT table_name\nFROM information_schema.tables\nWHERE table_schema = 'public'\nORDER BY table_name;"

    sql = st.text_area(
        "SQL Query",
        value=st.session_state.get("sql_query", default_sql),
        height=200,
        key="sql_query",
        label_visibility="collapsed",
        placeholder="SELECT … / INSERT … / UPDATE … / DELETE …",
    )

    col_run, col_clear, col_export = st.columns([1, 1, 1])

    with col_run:
        run = st.button("▶ Run Query", type="primary", width="stretch")
    with col_clear:
        if st.button("✖ Clear", width="stretch"):
            st.session_state["sql_query"] = default_sql
            st.session_state.pop("sql_result", None)
            st.session_state.pop("sql_error", None)
            st.session_state.pop("sql_dml_info", None)
            st.rerun()
    with col_export:
        export_placeholder = st.empty()

    if run:
        query = sql.strip()
        if not query:
            st.warning("Please enter a SQL query.")
        elif _DDL_PATTERN.match(query):
            st.error("DDL statements (DROP, TRUNCATE, ALTER, CREATE, …) are not allowed.")
        elif _DML_PATTERN.match(query):
            # ── DML: execute with commit, report rows affected ────────────────
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute(query)
                rows_affected = cur.rowcount
                conn.commit()
                cur.close()
                conn.close()
                st.session_state["sql_dml_info"] = f"✅ Query executed successfully — {rows_affected} row(s) affected."
                st.session_state.pop("sql_result", None)
                st.session_state.pop("sql_error", None)
            except Exception as e:
                st.session_state["sql_error"] = str(e)
                st.session_state.pop("sql_result", None)
                st.session_state.pop("sql_dml_info", None)
        else:
            # ── SELECT / WITH: return result set ──────────────────────────────
            try:
                conn = get_connection()
                df = pd.read_sql(query, conn)
                conn.close()
                st.session_state["sql_result"] = df
                st.session_state.pop("sql_error", None)
                st.session_state.pop("sql_dml_info", None)
            except Exception as e:
                st.session_state["sql_error"] = str(e)
                st.session_state.pop("sql_result", None)
                st.session_state.pop("sql_dml_info", None)

    if "sql_error" in st.session_state:
        st.error(st.session_state["sql_error"])

    if "sql_dml_info" in st.session_state:
        st.success(st.session_state["sql_dml_info"])

    if "sql_result" in st.session_state:
        df = st.session_state["sql_result"]
        rows, cols = df.shape
        st.caption(f"{rows:,} row(s) · {cols} column(s)")
        st.dataframe(df, width="stretch", hide_index=True)
        copy_df_button(df, key="dl_tools_sql")
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

    # ── Threshold ─────────────────────────────────────────────────────────
    threshold = st.slider(
        "Flag when move exceeds (%):",
        min_value=10, max_value=1000, value=100, step=10,
        help="100 % = flag any price that is more than 2× or less than ½ of its neighbour, "
             "or that deviates from the nearest transaction by the same factor",
    )

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
    copy_df_button(df, key="dl_tools_price_quality")

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
        st.session_state['pq_del_confirm'] = True
    if del_all_btn:
        st.session_state['pq_del_all_confirm'] = True

    if st.session_state.get('pq_del_confirm'):
        st.warning(f"⚠️ Delete **{n_selected}** selected price record(s)? This cannot be undone.")
        _cn, _cy, _ = st.columns([1, 1, 3])
        with _cn:
            if st.button("✖ Cancel", key="pq_del_cancel", width="stretch"):
                st.session_state['pq_del_confirm'] = False
                st.rerun()
        with _cy:
            if st.button("✔ Yes, delete", type="primary", key="pq_del_yes", width="stretch"):
                rows = to_delete[['securities_id', 'date']].to_dict('records')
                deleted = delete_historical_prices(rows)
                get_price_anomalies.clear()
                st.session_state['pq_del_confirm'] = False
                st.success(f"Deleted {deleted} price record(s).")
                st.rerun()

    if st.session_state.get('pq_del_all_confirm'):
        st.warning(f"⚠️ Delete **all {n_visible}** listed price record(s)? This cannot be undone.")
        _cn, _cy, _ = st.columns([1, 1, 3])
        with _cn:
            if st.button("✖ Cancel", key="pq_del_all_cancel", width="stretch"):
                st.session_state['pq_del_all_confirm'] = False
                st.rerun()
        with _cy:
            if st.button("✔ Yes, delete all", type="primary", key="pq_del_all_yes", width="stretch"):
                rows = df[['securities_id', 'date']].to_dict('records')
                deleted = delete_historical_prices(rows)
                get_price_anomalies.clear()
                st.session_state['pq_del_all_confirm'] = False
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
        width="stretch",
    )
    copy_df_button(df_display, key="dl_tools_missing_prices")

    col_ins, col_ins_all, col_info = st.columns([1, 1, 3])

    with col_ins:
        if st.button(
            f"📥 Insert for filtered ({len(df_view):,})" if selected_secs else "📥 Insert all",
            type="primary",
            width="stretch",
            key="fmp_insert_btn",
        ):
            st.session_state['fmp_insert_confirm'] = True

    with col_ins_all:
        if selected_secs and st.button(
            f"📥 Insert all ({len(df):,})",
            type="secondary",
            width="stretch",
            key="fmp_insert_all_btn",
        ):
            st.session_state['fmp_insert_all_confirm'] = True

    with col_info:
        if selected_secs:
            st.caption("First button inserts only the filtered rows; second inserts all gaps regardless of filter.")

    if st.session_state.get('fmp_insert_confirm'):
        _n = len(df_view)
        st.warning(f"⚠️ Insert **{_n:,}** missing price record(s) from transaction data? Existing prices will not be overwritten.")
        _cn, _cy, _ = st.columns([1, 1, 3])
        with _cn:
            if st.button("✖ Cancel", key="fmp_insert_cancel", width="stretch"):
                st.session_state['fmp_insert_confirm'] = False
                st.rerun()
        with _cy:
            if st.button("✔ Yes, insert", type="primary", key="fmp_insert_yes", width="stretch"):
                rows = df_view[['securities_id', 'date', 'price']].to_dict('records')
                inserted = insert_prices_from_transactions(rows)
                get_missing_tx_prices.clear()
                get_price_anomalies.clear()
                st.session_state['fmp_insert_confirm'] = False
                st.success(f"Inserted {inserted} price record(s).")
                st.rerun()

    if st.session_state.get('fmp_insert_all_confirm'):
        _n = len(df)
        st.warning(f"⚠️ Insert **all {_n:,}** missing price records from transaction data? Existing prices will not be overwritten.")
        _cn, _cy, _ = st.columns([1, 1, 3])
        with _cn:
            if st.button("✖ Cancel", key="fmp_insert_all_cancel", width="stretch"):
                st.session_state['fmp_insert_all_confirm'] = False
                st.rerun()
        with _cy:
            if st.button("✔ Yes, insert all", type="primary", key="fmp_insert_all_yes", width="stretch"):
                rows = df[['securities_id', 'date', 'price']].to_dict('records')
                inserted = insert_prices_from_transactions(rows)
                get_missing_tx_prices.clear()
                get_price_anomalies.clear()
                st.session_state['fmp_insert_all_confirm'] = False
                st.success(f"Inserted {inserted} price record(s).")
                st.rerun()


def _render_normalize_investments():
    st.subheader("⚖ Normalize Investment Prices")
    st.caption(
        "Finds Buy / Sell / Reinvest / ShrIn / ShrOut transactions whose "
        "Price Per Share or Quantity appears to be a placeholder (whole-number dummy) "
        "while a real Historical Price exists for that date. "
        "Updates Price Per Share → actual close price and recalculates Quantity = "
        "Total Amount ÷ Price, leaving Total Amount unchanged so P&L is preserved."
    )

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
        width="stretch",
    )
    copy_df_button(df_display, key="dl_tools_norm_prices")
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
        if st.button(label, type="primary", width="stretch", key="ni_norm_btn"):
            st.session_state['ni_norm_confirm'] = True

    with col_norm_all:
        if is_filtered and st.button(
            f"⚖ Normalize all ({len(df):,})",
            type="secondary",
            width="stretch",
            key="ni_norm_all_btn",
        ):
            st.session_state['ni_norm_all_confirm'] = True

    with col_info:
        if is_filtered:
            st.caption("First button normalizes only filtered rows; second normalizes all flagged rows.")

    if st.session_state.get('ni_norm_confirm'):
        _n = len(df_view)
        st.warning(f"⚠️ This will overwrite prices and quantities for **{_n:,}** investment row(s). This cannot be undone.")
        _cn, _cy, _ = st.columns([1, 1, 3])
        with _cn:
            if st.button("✖ Cancel", key="ni_norm_cancel", width="stretch"):
                st.session_state['ni_norm_confirm'] = False
                st.rerun()
        with _cy:
            if st.button("✔ Yes, normalize", type="primary", key="ni_norm_yes", width="stretch"):
                ids = df_view['investments_id'].tolist()
                updated = normalize_investment_prices(ids)
                get_investments_with_dummy_prices.clear()
                st.session_state['ni_norm_confirm'] = False
                st.success(f"Updated {updated} investment row(s).")
                st.rerun()

    if st.session_state.get('ni_norm_all_confirm'):
        _n = len(df)
        st.warning(f"⚠️ This will overwrite prices and quantities for **{_n:,}** investment row(s) (all flagged). This cannot be undone.")
        _cn, _cy, _ = st.columns([1, 1, 3])
        with _cn:
            if st.button("✖ Cancel", key="ni_norm_all_cancel", width="stretch"):
                st.session_state['ni_norm_all_confirm'] = False
                st.rerun()
        with _cy:
            if st.button("✔ Yes, normalize all", type="primary", key="ni_norm_all_yes", width="stretch"):
                ids = df['investments_id'].tolist()
                updated = normalize_investment_prices(ids)
                get_investments_with_dummy_prices.clear()
                st.session_state['ni_norm_all_confirm'] = False
                st.success(f"Updated {updated} investment row(s).")
                st.rerun()

    # Import here to avoid circular dependency at module level
    from database.crud import update_holdings as _update_holdings
    st.divider()
    col_rh, col_rh_info = st.columns([1, 4])
    with col_rh:
        if st.button("🔄 Refresh Holdings", width="stretch", key="ni_refresh_holdings"):
            _update_holdings()
            st.success("Holdings recalculated.")
    with col_rh_info:
        st.caption("Recalculates the Holdings table from Investments data. Run this after normalization to update portfolio quantities and P&L.")


_CATEGORIES = {
    "💾 Database": [
        "💾 Backup & Restore",
        "🔧 DB Maintenance",
        "🛢 SQL Interface",
    ],
    "📊 Market Data & Prices": [
        "📥 Fill Missing Prices",
        "🔍 Price Quality",
        "⚖ Normalize Investments",
    ],
}

_TOOL_RENDERERS = {
    "💾 Backup & Restore":       render_backup_restore,
    "🛢 SQL Interface":          _render_sql_interface,
    "🔧 DB Maintenance":         _render_db_maintenance,
    "📥 Fill Missing Prices":    _render_fill_missing_prices,
    "🔍 Price Quality":          _render_price_quality,
    "⚖ Normalize Investments":  _render_normalize_investments,
}


def render_tools(conn):
    """Render the Tools page. Only the selected tool is rendered to keep it fast."""
    st.title("Tools")

    category = st.radio(
        "category",
        list(_CATEGORIES.keys()),
        horizontal=True,
        label_visibility="collapsed",
        key="tools_category",
    )
    st.divider()

    tools_in_category = _CATEGORIES[category]

    # Categories with few tools get tabs; larger categories use a selectbox.
    if len(tools_in_category) <= 3:
        tabs = st.tabs(tools_in_category)
        for tab, tool in zip(tabs, tools_in_category):
            with tab:
                _TOOL_RENDERERS[tool]()
    else:
        tool = st.selectbox(
            "Tool",
            tools_in_category,
            label_visibility="collapsed",
            key="tools_tool",
        )
        st.divider()
        _TOOL_RENDERERS[tool]()
