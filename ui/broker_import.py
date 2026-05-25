"""Broker API Importers UI.

Entry points
------------
  render_brokerage_section() — called from ui/importers.py (Brokerage tab)
  render_revolut_import()    — called from ui/bank_import.py (Revolut Personal tab)
"""

from __future__ import annotations

import traceback

import pandas as pd
import streamlit as st

from database.connection import get_connection


# ---------------------------------------------------------------------------
# Brand logo helper (used by all importer sections)
# ---------------------------------------------------------------------------

_LOGO_HTML = """
<div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
  <img src="{url}" style="height:36px;border-radius:6px;object-fit:contain;"
       onerror="this.style.display='none'">
  <span style="font-size:1.25rem;font-weight:600;">{name}</span>
</div>
"""


def _brand_header(url: str, name: str) -> None:
    """Render a brand logo + name header, falling back gracefully if the image is unavailable."""
    st.markdown(_LOGO_HTML.format(url=url, name=name), unsafe_allow_html=True)
    st.divider()


# ---------------------------------------------------------------------------
# Shared DB helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=120)
def _load_accounts() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(
        """SELECT a.Accounts_Id   AS accounts_id,
                  a.Accounts_Name AS accounts_name,
                  a.Accounts_Type AS accounts_type,
                  c.Currencies_ShortName AS currency
           FROM   Accounts a
           LEFT JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id
           ORDER  BY a.Accounts_Name""",
        conn,
    )


def _account_selectbox(label: str, key: str,
                       type_filter: list[str] | None = None) -> tuple[int | None, str]:
    """Render an account selectbox; return (accounts_id, accounts_name)."""
    df = _load_accounts()
    if type_filter:
        df = df[df["accounts_type"].isin(type_filter)]
    if df.empty:
        st.warning("No accounts found. Please create one in Static Data first.")
        return None, ""
    options = df["accounts_name"].tolist()
    sel = st.selectbox(label, options, key=key)
    row = df[df["accounts_name"] == sel].iloc[0]
    return int(row["accounts_id"]), sel


def _import_summary(counts: dict) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Investments imported", counts.get("investments", 0))
    c2.metric("Investments skipped",  counts.get("investments_skip", 0))
    c3.metric("Transactions imported", counts.get("transactions", 0))
    c4.metric("Transactions skipped",  counts.get("transactions_skip", 0))


@st.cache_data(ttl=120)
def _load_all_securities() -> pd.DataFrame:
    """Return all Securities rows for use in mapping selectboxes."""
    conn = get_connection()
    try:
        return pd.read_sql(
            """SELECT s.Securities_Id   AS securities_id,
                      s.Ticker          AS ticker,
                      s.Securities_Name AS name,
                      s.Securities_Type AS type
               FROM   Securities s
               ORDER  BY s.Securities_Name""",
            conn,
        )
    finally:
        conn.close()


def _preview_records(inv_records: list, tx_records: list,
                     inv_df_fn, tx_df_fn) -> None:
    """Show expandable previews of investments and transactions."""
    tab_inv, tab_tx = st.tabs([
        f"📈 Investments ({len(inv_records)})",
        f"💳 Cash Transactions ({len(tx_records)})",
    ])
    with tab_inv:
        if inv_records:
            df = inv_df_fn(inv_records)
            st.dataframe(df, hide_index=True, use_container_width=True,
                         column_config={
                             "date":           "Date",
                             "action":         "Action",
                             "symbol":         "Symbol",
                             "name":           "Name",
                             "quantity":       st.column_config.NumberColumn("Qty",     format="%.4f"),
                             "price":          st.column_config.NumberColumn("Price",   format="%.4f"),
                             "total_eur":      st.column_config.NumberColumn("Total (€)", format="%.2f"),
                             "currency":       "Ccy",
                             "commission":     st.column_config.NumberColumn("Commission", format="%.4f"),
                             "asset_category": "Asset",
                             "desc":           "Dedup Key",
                         })
        else:
            st.info("No investment records in this file.")
    with tab_tx:
        if tx_records:
            df = tx_df_fn(tx_records)
            st.dataframe(df, hide_index=True, use_container_width=True,
                         column_config={
                             "date":        "Date",
                             "description": "Description",
                             "amount":      st.column_config.NumberColumn("Amount", format="%.2f"),
                             "currency":    "Ccy",
                         })
        else:
            st.info("No cash transaction records in this file.")


# ===========================================================================
# Interactive Brokers — preview helper with reconciliation status
# ===========================================================================

def _ib_preview_with_status(
    inv_records: list,
    tx_records: list,
    existing_inv: set,
    existing_tx: set,
    fuzzy_inv: set,
    fuzzy_tx: set,
    sec_matches: dict,
    ignored_descs: set | None = None,
) -> None:
    """Show IB preview tables annotated with Status and Security Match columns.

    Status values:
      ✅ Exists          — description key already in DB (exact dedup match)
      ⚠️ Likely duplicate — same date/action/qty found in DB under a different key
      ⏭️ Ignored         — user-marked as permanently ignored
      🆕 New             — no match found
    """
    ignored_descs = ignored_descs or set()
    tab_inv, tab_tx = st.tabs([
        f"📈 Investments ({len(inv_records)})",
        f"💳 Cash Transactions ({len(tx_records)})",
    ])

    with tab_inv:
        if inv_records:
            rows = []
            for r in inv_records:
                if r["desc"] in existing_inv:
                    status = "✅ Exists"
                elif r["desc"] in fuzzy_inv:
                    status = "⚠️ Likely duplicate"
                elif r["desc"] in ignored_descs:
                    status = "⏭️ Ignored"
                else:
                    status = "🆕 New"
                isin_or_name = r.get("isin") or r.get("name", "")
                match_info   = sec_matches.get(isin_or_name, (None, "new"))
                match_type   = match_info[1]
                if match_type.startswith("mapped:"):
                    sec_label = f"🗺️ {match_type[7:]}"
                elif match_type.startswith("isin:"):
                    sec_label = f"🔗 {match_type[5:]}"   # matched security name
                elif match_type == "name":
                    sec_label = "🔗 Name match"
                else:
                    sec_label = "🆕 New security"
                rows.append({**r, "status": status, "security_match": sec_label})

            df_inv = pd.DataFrame(rows)
            cols = ["status", "date", "action", "symbol", "name",
                    "quantity", "price", "total_eur", "currency",
                    "commission", "asset_category", "security_match", "desc"]
            df_inv = df_inv[[c for c in cols if c in df_inv.columns]]
            st.dataframe(
                df_inv,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "status":         "Status",
                    "date":           "Date",
                    "action":         "Action",
                    "symbol":         "Symbol",
                    "name":           "Name",
                    "quantity":       st.column_config.NumberColumn("Qty",          format="%.4f"),
                    "price":          st.column_config.NumberColumn("Price",        format="%.4f"),
                    "total_eur":      st.column_config.NumberColumn("Total (€)",    format="%.2f"),
                    "currency":       "Ccy",
                    "commission":     st.column_config.NumberColumn("Commission",   format="%.4f"),
                    "asset_category": "Asset",
                    "security_match": "Security Match",
                    "desc":           "Dedup Key",
                },
            )
        else:
            st.info("No investment records in this statement.")

    with tab_tx:
        if tx_records:
            rows = []
            for r in tx_records:
                if r["desc"] in existing_tx:
                    status = "✅ Exists"
                elif r["desc"] in fuzzy_tx:
                    status = "⚠️ Likely duplicate"
                elif r["desc"] in ignored_descs:
                    status = "⏭️ Ignored"
                else:
                    status = "🆕 New"
                rows.append({**r, "status": status})
            df_tx = pd.DataFrame(rows)
            cols = ["status", "date", "description", "amount", "currency"]
            df_tx = df_tx[[c for c in cols if c in df_tx.columns]]
            st.dataframe(
                df_tx,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "status":      "Status",
                    "date":        "Date",
                    "description": "Description",
                    "amount":      st.column_config.NumberColumn("Amount", format="%.2f"),
                    "currency":    "Ccy",
                },
            )
        else:
            st.info("No cash transaction records in this statement.")


# ===========================================================================
# Revolut Trading — preview helper with reconciliation status
# ===========================================================================

def _revt_preview_with_status(
    inv_records: list,
    tx_records: list,
    existing_inv: set,
    existing_tx: set,
    fuzzy_inv: set,
    fuzzy_tx: set,
    sec_matches: dict,
    ignored_descs: set | None = None,
) -> None:
    """Show Revolut Trading preview tables annotated with Status and Security Match.

    Status values:
      ✅ Exists          — description key already in DB
      ⚠️ Likely duplicate — same date/action/qty found under a different key
      ⏭️ Ignored         — user-marked as permanently ignored
      🆕 New             — no match found
    """
    ignored_descs = ignored_descs or set()
    tab_inv, tab_tx = st.tabs([
        f"📈 Investments ({len(inv_records)})",
        f"💳 Cash Transactions ({len(tx_records)})",
    ])

    with tab_inv:
        if inv_records:
            rows = []
            for r in inv_records:
                if r["desc"] in existing_inv:
                    status = "✅ Exists"
                elif r["desc"] in fuzzy_inv:
                    status = "⚠️ Likely duplicate"
                elif r["desc"] in ignored_descs:
                    status = "⏭️ Ignored"
                else:
                    status = "🆕 New"
                key        = (r.get("symbol") or r.get("name", "")).strip()
                match_info = sec_matches.get(key, (None, "new"))
                match_type = match_info[1]
                if match_type.startswith("mapped:"):
                    sec_label = f"🗺️ {match_type[7:]}"
                elif match_type == "ticker":
                    sec_label = "🔗 Ticker match"
                elif match_type == "name":
                    sec_label = "🔗 Name match"
                else:
                    sec_label = "🆕 New security"
                rows.append({**r, "status": status, "security_match": sec_label})

            df_inv = pd.DataFrame(rows)
            cols = ["status", "date", "action", "symbol", "name",
                    "quantity", "price", "total_eur", "currency",
                    "asset_category", "security_match", "desc"]
            df_inv = df_inv[[c for c in cols if c in df_inv.columns]]
            st.dataframe(
                df_inv,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "status":         "Status",
                    "date":           "Date",
                    "action":         "Action",
                    "symbol":         "Symbol",
                    "name":           "Name",
                    "quantity":       st.column_config.NumberColumn("Qty",        format="%.4f"),
                    "price":          st.column_config.NumberColumn("Price",      format="%.4f"),
                    "total_eur":      st.column_config.NumberColumn("Total (€)",  format="%.2f"),
                    "currency":       "Ccy",
                    "asset_category": "Asset",
                    "security_match": "Security Match",
                    "desc":           "Dedup Key",
                },
            )
        else:
            st.info("No investment records in this file.")

    with tab_tx:
        if tx_records:
            rows = []
            for r in tx_records:
                if r["desc"] in existing_tx:
                    status = "✅ Exists"
                elif r["desc"] in fuzzy_tx:
                    status = "⚠️ Likely duplicate"
                elif r["desc"] in ignored_descs:
                    status = "⏭️ Ignored"
                else:
                    status = "🆕 New"
                rows.append({**r, "status": status})
            df_tx = pd.DataFrame(rows)
            cols = ["status", "date", "description", "amount", "currency"]
            df_tx = df_tx[[c for c in cols if c in df_tx.columns]]
            st.dataframe(
                df_tx,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "status":      "Status",
                    "date":        "Date",
                    "description": "Description",
                    "amount":      st.column_config.NumberColumn("Amount", format="%.2f"),
                    "currency":    "Ccy",
                },
            )
        else:
            st.info("No cash transaction records in this file.")


# ===========================================================================
# Security Mapping UI  (Revolut Trading)
# ===========================================================================

def _revt_security_mapping_ui(sec_matches: dict) -> None:
    """Expander UI for mapping unmapped Revolut Trading tickers to DB securities.

    Shows only tickers whose match_type is 'new' (i.e. not found by ticker,
    name, or saved mapping).  On save the mapping is persisted to the DB and
    the session-state sec_matches dict is updated in-place so the preview
    refreshes without a full re-parse.
    """
    unmapped = {sym: info for sym, info in sec_matches.items() if info[1] == "new"}
    if not unmapped:
        return

    with st.expander(
        f"🗺️ Security Mappings — {len(unmapped)} unmapped ticker(s) — click to configure",
        expanded=True,
    ):
        st.caption(
            "These tickers were not found in your Securities database by ticker or name. "
            "Select the matching security for each one below, then click **💾 Save Mappings**. "
            "Saved mappings are permanent and will be used for all future imports."
        )

        all_secs = _load_all_securities()
        if all_secs.empty:
            st.warning("No securities found in the database. Create them in Static Data first.")
            return

        sec_options = ["(create new — will be added on import)"] + all_secs["name"].tolist()
        pending_mappings: dict[str, int] = {}  # ticker → securities_id

        for ticker in unmapped:
            c1, c2 = st.columns([1, 3])
            with c1:
                st.markdown(f"**{ticker}**")
            with c2:
                chosen = st.selectbox(
                    f"Map {ticker} to",
                    sec_options,
                    key=f"revt_map_{ticker}",
                    label_visibility="collapsed",
                )
                if not chosen.startswith("(create new"):
                    sec_row = all_secs[all_secs["name"] == chosen]
                    if not sec_row.empty:
                        pending_mappings[ticker] = int(sec_row.iloc[0]["securities_id"])

        if pending_mappings:
            if st.button("💾 Save Mappings", key="revt_save_mappings", type="primary"):
                from database.queries import save_security_mappings
                try:
                    save_security_mappings("Revolut Trading", pending_mappings)
                    # Update sec_matches in session state immediately (avoid full re-parse)
                    _updated = dict(st.session_state.get("revt_sec_matches", {}))
                    for sym, sec_id in pending_mappings.items():
                        sec_row = all_secs[all_secs["securities_id"] == sec_id]
                        sec_name = sec_row.iloc[0]["name"] if not sec_row.empty else sym
                        _updated[sym] = (sec_id, f"mapped:{sec_name}")
                    st.session_state["revt_sec_matches"] = _updated
                    _load_all_securities.clear()
                    st.success(f"✅ Saved {len(pending_mappings)} mapping(s). Re-parse to see updated Security Match column.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed to save mappings: {exc}")
        else:
            st.info("Select at least one mapping above and click Save.")


# ===========================================================================
# Security Mapping UI  (Interactive Brokers)
# ===========================================================================

def _ib_security_mapping_ui(sec_matches: dict) -> None:
    """Expander UI for mapping unmapped IB securities to DB securities.

    Shows only entries whose match_type is 'new' (not resolved by ISIN, name,
    or a previously saved mapping).  The mapping key is the ISIN when the
    record has one; otherwise the security name.  On save the mapping is
    persisted to import_security_mappings and sec_matches in session state is
    updated immediately so the preview reflects the change without a re-fetch.
    """
    unmapped = {key: info for key, info in sec_matches.items() if info[1] == "new"}
    if not unmapped:
        return

    with st.expander(
        f"🗺️ Security Mappings — {len(unmapped)} unmapped security(s) — click to configure",
        expanded=True,
    ):
        st.caption(
            "These securities could not be matched in your database by ISIN or name. "
            "Select the matching DB security for each one, then click **💾 Save Mappings**. "
            "The key used is the **ISIN** when available, otherwise the security name. "
            "Saved mappings are permanent and take priority on all future IB imports."
        )

        all_secs = _load_all_securities()
        if all_secs.empty:
            st.warning("No securities found in the database. Create them in Static Data first.")
            return

        sec_options = ["(create new — will be added on import)"] + all_secs["name"].tolist()
        pending_mappings: dict[str, int] = {}  # key (isin/name) → securities_id

        for key in unmapped:
            c1, c2 = st.columns([2, 3])
            with c1:
                st.markdown(f"**{key}**")
            with c2:
                widget_key = f"ib_map_{key.replace(' ', '_').replace('/', '_')}"
                chosen = st.selectbox(
                    f"Map {key}",
                    sec_options,
                    key=widget_key,
                    label_visibility="collapsed",
                )
                if not chosen.startswith("(create new"):
                    sec_row = all_secs[all_secs["name"] == chosen]
                    if not sec_row.empty:
                        pending_mappings[key] = int(sec_row.iloc[0]["securities_id"])

        if pending_mappings:
            if st.button("💾 Save Mappings", key="ib_save_mappings", type="primary"):
                from database.queries import save_security_mappings
                try:
                    save_security_mappings("Interactive Brokers", pending_mappings)
                    # Update sec_matches in session state — no re-fetch needed
                    _updated = dict(st.session_state.get("ib_sec_matches", {}))
                    for map_key, sec_id in pending_mappings.items():
                        sec_row = all_secs[all_secs["securities_id"] == sec_id]
                        sec_name = sec_row.iloc[0]["name"] if not sec_row.empty else map_key
                        _updated[map_key] = (sec_id, f"mapped:{sec_name}")
                    st.session_state["ib_sec_matches"] = _updated
                    st.success(f"✅ Saved {len(pending_mappings)} mapping(s).")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed to save mappings: {exc}")
        else:
            st.info("Select at least one mapping above and click Save.")


# ===========================================================================
# Ignore-record Manager UI  (shared between IB and Revolut Trading)
# ===========================================================================

def _ignore_manager_ui(
    source: str,
    inv_records: list,
    tx_records: list,
    existing_inv: set,
    existing_tx: set,
    fuzzy_inv: set,
    fuzzy_tx: set,
    ignored_descs: set,
    session_key: str,
) -> None:
    """Expander UI for permanently marking records as 'ignored'.

    Shows only records that are genuinely new (not existing, fuzzy, or already
    ignored), so the user can decide to never be asked about them again.
    Also lets the user un-ignore previously saved keys.

    Parameters
    ----------
    source      : DB source name, e.g. 'Interactive Brokers' or 'Revolut Trading'
    session_key : 'ib' or 'revt' — prefix for session-state keys
    """
    # Records that are genuinely new and not already ignored
    new_inv = [r for r in inv_records
               if r["desc"] not in existing_inv
               and r["desc"] not in fuzzy_inv
               and r["desc"] not in ignored_descs]
    new_tx  = [r for r in tx_records
               if r["desc"] not in existing_tx
               and r["desc"] not in fuzzy_tx
               and r["desc"] not in ignored_descs]

    total_new     = len(new_inv) + len(new_tx)
    total_ignored = len(ignored_descs)

    with st.expander(
        f"⏭️ Ignore Records — {total_ignored} saved · {total_new} new available to ignore",
        expanded=False,
    ):
        # ── Mark new records as ignored ───────────────────────────────────────
        if total_new:
            st.markdown("**Tick the records you want to permanently ignore:**")
            rows: list[dict] = []
            for r in new_inv:
                rows.append({
                    "desc":     r["desc"],
                    "type":     "📈 Investment",
                    "record":   (
                        f"{r.get('action', '')} {r.get('symbol', '')} "
                        f"on {r.get('date', '')}"
                    ).strip(),
                    "ignore":   False,
                })
            for r in new_tx:
                rows.append({
                    "desc":   r["desc"],
                    "type":   "💳 Transaction",
                    "record": (
                        f"{r.get('description', r.get('desc', ''))} "
                        f"· {r.get('amount', '')} on {r.get('date', '')}"
                    ).strip(),
                    "ignore": False,
                })

            df_new = pd.DataFrame(rows)
            edited = st.data_editor(
                df_new[["type", "record", "ignore"]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "type":   st.column_config.TextColumn("Type",   disabled=True),
                    "record": st.column_config.TextColumn("Record", disabled=True),
                    "ignore": st.column_config.CheckboxColumn("Ignore?"),
                },
                key=f"{session_key}_ignore_editor",
            )

            # Map edited "ignore" column back to desc keys
            to_ignore = [
                df_new.iloc[i]["desc"]
                for i, sel in enumerate(edited["ignore"].tolist())
                if sel
            ]

            if to_ignore:
                if st.button(
                    f"⏭️ Mark {len(to_ignore)} record(s) as ignored",
                    key=f"{session_key}_mark_ignored",
                    type="primary",
                ):
                    from database.queries import save_ignored_records
                    save_ignored_records(source, to_ignore)
                    _new_set = set(st.session_state.get(f"{session_key}_ignored", set()))
                    _new_set.update(to_ignore)
                    st.session_state[f"{session_key}_ignored"] = _new_set
                    st.success(f"✅ {len(to_ignore)} record(s) marked as ignored and will be skipped in future imports.")
                    st.rerun()
            else:
                st.info("Check at least one box above, then click the button to save.")
        else:
            st.info("No new records available to add to the ignore list.")

        # ── Un-ignore previously saved keys ──────────────────────────────────
        if total_ignored:
            st.divider()
            st.markdown(f"**Currently ignored** — {total_ignored} record(s):")
            ig_sorted = sorted(ignored_descs)
            to_unignore = st.multiselect(
                "Select key(s) to remove from the ignore list:",
                ig_sorted,
                key=f"{session_key}_unignore_select",
            )
            if to_unignore:
                if st.button(
                    f"✅ Un-ignore {len(to_unignore)} record(s)",
                    key=f"{session_key}_unignore_btn",
                ):
                    from database.queries import remove_ignored_records
                    remove_ignored_records(source, to_unignore)
                    _new_set = set(st.session_state.get(f"{session_key}_ignored", set()))
                    _new_set -= set(to_unignore)
                    st.session_state[f"{session_key}_ignored"] = _new_set
                    st.success(f"✅ Removed {len(to_unignore)} key(s) from the ignore list.")
                    st.rerun()


# ===========================================================================
# Interactive Brokers — Flex Web Service
# ===========================================================================

def render_ib_import() -> None:
    st.markdown(
        "Import trades, dividends, interest and cash transfers directly from "
        "Interactive Brokers using the **Flex Web Service**."
    )

    with st.expander("ℹ️ One-time IB setup (click to expand)", expanded=False):
        st.markdown("""
1. Log in to [IB Client Portal](https://www.interactivebrokers.com) →
   **Reports → Flex Queries**.
2. Click **+** → *Activity Flex Query*.
3. Under **Sections**, enable:
   - **Trades** (buySell, assetCategory, symbol, description, currency,
     fxRateToBase, quantity, tradePrice, netCash, tradeDate, ibOrderID, isin)
   - **Cash Transactions** (type, currency, fxRateToBase, amount,
     description, symbol, dateTime, isin)
4. Date Format: `yyyyMMdd`  ·  Time Format: `HHmmss`  ·  Separator: `;`
5. **Save** — note the numeric **Query ID** shown next to the query name.
6. ⚠️ **Critical** — still on the same query: click **"Edit"** →
   scroll to the bottom → tick **"Allow Web Service Access"** → Save.
   *Without this flag the API always returns error 1019.*
7. Go to **Reports → Flex Web Service** → create / copy your **Token**.
""")

    st.divider()

    # ── Pre-populate credentials from saved settings (first render only) ─────
    from database.queries import get_app_setting, save_app_setting
    if "ib_token" not in st.session_state:
        _saved_tok = get_app_setting("ib_flex_token") or ""
        st.session_state["ib_token"] = _saved_tok
        if _saved_tok:
            st.session_state.setdefault("ib_remember", True)
    if "ib_query_id" not in st.session_state:
        st.session_state["ib_query_id"] = get_app_setting("ib_flex_query_id") or ""

    # ── Source: API vs paste ─────────────────────────────────────────────────
    st.markdown("### 📡 Statement Source")
    xml_source_mode = st.radio(
        "source_mode",
        ["🌐 Fetch via API (Token + Query ID)", "📋 Paste XML from IB portal"],
        horizontal=True,
        label_visibility="collapsed",
        key="ib_source_mode",
        help=(
            "Use 'Paste XML' if the API keeps returning error 1019 — "
            "download the statement manually from IB and paste it here."
        ),
    )

    raw_xml  = ""
    token    = ""
    query_id = ""
    remember = False

    if xml_source_mode.startswith("🌐"):
        col1, col2 = st.columns(2)
        with col1:
            token    = st.text_input(
                "Flex Token", type="password", key="ib_token",
                help="Long alphanumeric string from IB → Reports → Flex Web Service.",
            )
        with col2:
            query_id = st.text_input(
                "Query ID", key="ib_query_id",
                help="Numeric ID shown next to your Activity Flex Query.",
            )
        remember = st.checkbox(
            "💾 Remember credentials",
            key="ib_remember",
            help="Save Token and Query ID to the database so they are pre-filled on your next visit.",
        )
    else:
        st.caption(
            "Run your Flex Query on the IB website → choose **XML** output format → "
            "copy the entire XML text and paste it below."
        )
        raw_xml = st.text_area(
            "Flex XML",
            height=160,
            key="ib_raw_xml",
            placeholder="<FlexQueryResponse queryName=...>...</FlexQueryResponse>",
        )

    st.divider()

    # ── Account mapping ──────────────────────────────────────────────────────
    st.markdown("### 🏦 Account Mapping")
    acc_id, acc_name = _account_selectbox(
        "Import into account",
        key="ib_account",
        type_filter=["Brokerage", "Margin", "Other Investment", "Pension"],
    )

    st.divider()

    # ── Date filter ──────────────────────────────────────────────────────────
    st.markdown("### 📅 Date Filter")
    st.caption(
        "IB's Flex API does not accept date parameters — the date range is "
        "configured inside the Flex Query on IB's website.  Use these filters "
        "to narrow the import to a specific period after the statement is fetched."
    )
    df_col1, df_col2 = st.columns(2)
    with df_col1:
        ib_filter_from = st.date_input(
            "Import from",
            value=None,
            key="ib_filter_from",
            help="Only import records on or after this date.  Leave blank for no lower limit.",
        )
    with df_col2:
        ib_filter_to = st.date_input(
            "Import to",
            value=None,
            key="ib_filter_to",
            help="Only import records on or before this date.  Leave blank for no upper limit.",
        )

    st.divider()

    # ── Options ──────────────────────────────────────────────────────────────
    st.markdown("### ⚙️ Options")
    replace_mode = st.checkbox(
        "Replace mode — delete all existing IB records for this account before importing",
        value=False, key="ib_replace",
        help="Use this to do a clean re-import.  All Investment and Transaction rows "
             "whose Description starts with 'IB|' will be deleted first.",
    )

    st.divider()

    # ── Fetch / Parse ────────────────────────────────────────────────────────
    _is_paste_mode = xml_source_mode.startswith("📋")
    _btn_label     = "🔍 Parse XML" if _is_paste_mode else "📡 Fetch & Preview"
    fetch_btn = st.button(_btn_label, key="ib_fetch",
                          type="primary", disabled=acc_id is None)

    if fetch_btn or st.session_state.get("ib_parsed"):
        from data.ib_flex_connector import (
            fetch_flex_xml, parse_flex_xml,
            check_existing_records, check_fuzzy_duplicates,
            preview_security_matches,
        )

        # ── Fetch (or use pasted XML) ────────────────────────────────────────
        if fetch_btn:
            xml_source = raw_xml.strip() if raw_xml and raw_xml.strip() else None

            if not xml_source:
                if _is_paste_mode:
                    st.error("Please paste the Flex XML above before parsing.")
                    return
                if not token.strip() or not query_id.strip():
                    st.error("Please enter both Token and Query ID.")
                    return

                # ── Save credentials at click time (before fetch) ─────────────
                # Do this now so credentials are persisted even if the fetch fails.
                if remember:
                    try:
                        save_app_setting("ib_flex_token",    token.strip())
                        save_app_setting("ib_flex_query_id", query_id.strip())
                    except Exception:
                        pass  # non-fatal

                status_box = st.empty()
                try:
                    def _cb(msg):
                        status_box.info(f"⏳ {msg}")

                    with st.spinner("Contacting Interactive Brokers…"):
                        xml_source = fetch_flex_xml(
                            token.strip(), query_id.strip(), progress_cb=_cb
                        )
                    status_box.empty()
                except Exception as exc:
                    err_str = str(exc)
                    st.error(f"Failed to fetch statement: {exc}")
                    if "could not be generated" in err_str.lower() or "1019" in err_str:
                        st.warning(
                            "💡 **Error 1019 — statement could not be generated.**\n\n"
                            "Switch to **📋 Paste XML** mode above: run the Flex Query "
                            "manually on IB's website (choose XML output format), "
                            "then paste the result here. No API token needed."
                        )
                    return

            try:
                inv_records, tx_records, meta = parse_flex_xml(xml_source)
            except Exception as exc:
                st.error(f"Failed to parse Flex XML: {exc}")
                st.code(traceback.format_exc())
                return

            # ── Reconciliation & securities matching (done once at fetch) ─────
            existing_inv, existing_tx = check_existing_records(
                inv_records, tx_records, acc_id
            )
            fuzzy_inv, fuzzy_tx = check_fuzzy_duplicates(
                inv_records, tx_records, acc_id
            )
            # Fuzzy set should only flag records NOT already caught by exact match
            fuzzy_inv -= existing_inv
            fuzzy_tx  -= existing_tx
            sec_matches = preview_security_matches(inv_records)

            from database.queries import get_ignored_records as _get_ignored
            ignored_descs = _get_ignored("Interactive Brokers")

            st.session_state["ib_inv_records"]  = inv_records
            st.session_state["ib_tx_records"]   = tx_records
            st.session_state["ib_meta"]         = meta
            st.session_state["ib_existing_inv"] = existing_inv
            st.session_state["ib_existing_tx"]  = existing_tx
            st.session_state["ib_fuzzy_inv"]    = fuzzy_inv
            st.session_state["ib_fuzzy_tx"]     = fuzzy_tx
            st.session_state["ib_sec_matches"]  = sec_matches
            st.session_state["ib_ignored"]      = ignored_descs
            st.session_state["ib_parsed"]       = True

        inv_records   = st.session_state.get("ib_inv_records",  [])
        tx_records    = st.session_state.get("ib_tx_records",   [])
        meta          = st.session_state.get("ib_meta",         {})
        existing_inv  = st.session_state.get("ib_existing_inv", set())
        existing_tx   = st.session_state.get("ib_existing_tx",  set())
        fuzzy_inv     = st.session_state.get("ib_fuzzy_inv",    set())
        fuzzy_tx      = st.session_state.get("ib_fuzzy_tx",     set())
        sec_matches   = st.session_state.get("ib_sec_matches",  {})
        ignored_descs = st.session_state.get("ib_ignored",      set())

        # ── Statement summary ─────────────────────────────────────────────────
        if meta:
            m1, m2, m3 = st.columns(3)
            m1.metric("IB Account",     meta.get("account_id", "—"))
            m2.metric("Statement from", str(meta.get("from_date", "—")))
            m3.metric("Statement to",   str(meta.get("to_date", "—")))

        # ── Apply date filter ────────────────────────────────────────────────
        def _in_range(r: dict) -> bool:
            d = r.get("date")
            if d is None:
                return True
            if isinstance(d, str):
                try:
                    from datetime import date as _dt
                    d = _dt.fromisoformat(d)
                except ValueError:
                    return True
            if ib_filter_from and d < ib_filter_from:
                return False
            if ib_filter_to and d > ib_filter_to:
                return False
            return True

        inv_total   = len(inv_records)
        tx_total    = len(tx_records)
        inv_records = [r for r in inv_records if _in_range(r)]
        tx_records  = [r for r in tx_records  if _in_range(r)]

        if (ib_filter_from or ib_filter_to) and (inv_total + tx_total) > 0:
            hidden = (inv_total - len(inv_records)) + (tx_total - len(tx_records))
            if hidden:
                _range_parts = []
                if ib_filter_from:
                    _range_parts.append(f"from {ib_filter_from}")
                if ib_filter_to:
                    _range_parts.append(f"to {ib_filter_to}")
                st.info(
                    f"📅 Date filter active ({', '.join(_range_parts)}) — "
                    f"**{hidden}** record(s) outside the range excluded."
                )

        if not inv_records and not tx_records:
            st.warning("No importable records found in this statement.")
            with st.expander("🔍 Diagnostics — why is the statement empty?"):
                trade_els = meta.get("diag_trade_els", "?")
                cash_els  = meta.get("diag_cash_els",  "?")
                sections  = meta.get("diag_sections",  [])
                st.markdown(f"""
**Raw elements found in the Flex XML**

| Section | Elements found |
|---------|---------------|
| `<Trade>` (raw, before filtering) | **{trade_els}** |
| `<CashTransaction>` (raw, before filtering) | **{cash_els}** |

**XML sections present in this statement:**
{", ".join(f"`{s}`" for s in sections) if sections else "_(none detected)_"}

**Common reasons for an empty import:**

1. **Date filter too narrow** — Check the *Import from / Import to* fields
   above.  If the Flex Query covers a wide range but the filter cuts it down
   to a period with no activity, all records will be excluded.

2. **No activity on this date range** — The Flex Query covers a period
   with no trades or cash transactions (e.g. a weekend or holiday).
   → Try a wider date range in your IB Flex Query settings.

3. **Sections not enabled in your Flex Query** — Both **Trades** and
   **Cash Transactions** must be enabled under *Sections* when creating
   the Flex Query.  Check *Reports → Flex Queries → Edit*.

4. **`buySell` attribute value not recognised** — If you see trades above
   but they're not imported, the `buySell` field may contain an unexpected
   value.  Use *"Paste XML"* mode (above) and share the raw XML
   so the parser can be updated.

5. **Today's date not yet settled** — IB typically makes intraday data
   available in Flex statements after market close.  Try again tomorrow or
   choose *"Last N days"* in the Flex Query date settings.
""")
            return

        # ── Reconciliation summary ─────────────────────────────────────────────
        truly_new_inv   = [r for r in inv_records
                           if r["desc"] not in existing_inv
                           and r["desc"] not in fuzzy_inv
                           and r["desc"] not in ignored_descs]
        truly_new_tx    = [r for r in tx_records
                           if r["desc"] not in existing_tx
                           and r["desc"] not in fuzzy_tx
                           and r["desc"] not in ignored_descs]
        fuzzy_only_inv  = [r for r in inv_records if r["desc"] in fuzzy_inv]
        fuzzy_only_tx   = [r for r in tx_records  if r["desc"] in fuzzy_tx]
        exist_inv_count = sum(1 for r in inv_records if r["desc"] in existing_inv)
        exist_tx_count  = sum(1 for r in tx_records  if r["desc"] in existing_tx)
        ignored_inv_count = sum(1 for r in inv_records if r["desc"] in ignored_descs)
        ignored_tx_count  = sum(1 for r in tx_records  if r["desc"] in ignored_descs)

        _skip_inv = exist_inv_count + len(fuzzy_only_inv)
        _skip_tx  = exist_tx_count  + len(fuzzy_only_tx)

        if not truly_new_inv and not truly_new_tx and not replace_mode:
            st.info(
                f"✅ Nothing genuinely new — "
                f"**{exist_inv_count}** inv exact + **{len(fuzzy_only_inv)}** likely-dup, "
                f"**{exist_tx_count}** tx exact + **{len(fuzzy_only_tx)}** likely-dup."
            )
        else:
            st.success(
                f"Found **{len(truly_new_inv)}** new investment record(s) and "
                f"**{len(truly_new_tx)}** new transaction(s) to import."
            )
        if fuzzy_only_inv or fuzzy_only_tx:
            st.warning(
                f"⚠️ **{len(fuzzy_only_inv)}** investment(s) and "
                f"**{len(fuzzy_only_tx)}** transaction(s) match existing records by "
                "date/action/amount but have a different description key — "
                "likely already entered manually or via another importer. "
                "They are marked **⚠️ Likely duplicate** in the preview and will be skipped."
            )
        if ignored_inv_count or ignored_tx_count:
            st.info(
                f"⏭️ **{ignored_inv_count}** investment(s) and "
                f"**{ignored_tx_count}** transaction(s) are on the ignore list and will be skipped. "
                "Use the **Ignore Records** panel below to manage them."
            )

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("🆕 New investments",   len(truly_new_inv))
        r2.metric("🔄 Skip investments",  _skip_inv + ignored_inv_count,
                  help="✅ Exact key match + ⚠️ Likely duplicates + ⏭️ Ignored")
        r3.metric("🆕 New transactions",  len(truly_new_tx))
        r4.metric("🔄 Skip transactions", _skip_tx + ignored_tx_count,
                  help="✅ Exact key match + ⚠️ Likely duplicates + ⏭️ Ignored")

        # ── Preview ──────────────────────────────────────────────────────────
        st.markdown("### 👁️ Preview")
        _ib_preview_with_status(
            inv_records, tx_records,
            existing_inv, existing_tx,
            fuzzy_inv, fuzzy_tx,
            sec_matches,
            ignored_descs=ignored_descs,
        )

        # ── Security Mappings ─────────────────────────────────────────────────
        _ib_security_mapping_ui(sec_matches)

        # ── Ignore manager ────────────────────────────────────────────────────
        _ignore_manager_ui(
            "Interactive Brokers",
            inv_records, tx_records,
            existing_inv, existing_tx,
            fuzzy_inv, fuzzy_tx,
            ignored_descs,
            session_key="ib",
        )

        # ── Import ───────────────────────────────────────────────────────────
        st.divider()
        st.markdown("### 💾 Import")
        st.caption(f"Target account: **{acc_name}** (ID {acc_id})")

        _icol1, _icol2 = st.columns(2)
        _import_inv = _icol1.checkbox("📈 Import investments",       value=True, key="ib_import_inv")
        _import_tx  = _icol2.checkbox("💳 Import cash transactions", value=True, key="ib_import_tx")

        _base_inv  = truly_new_inv if not replace_mode else inv_records
        _base_tx   = truly_new_tx  if not replace_mode else tx_records
        _imp_inv   = _base_inv if _import_inv else []
        _imp_tx    = _base_tx  if _import_tx  else []
        _new_total = len(_imp_inv) + len(_imp_tx)

        if _new_total == 0 and not replace_mode:
            st.info(
                "No records to import.  "
                "Enable **Replace mode** above if you want to force a clean re-import."
            )
        else:
            _btn_suffix = (
                f" ({_new_total} record{'s' if _new_total != 1 else ''})"
                if not replace_mode else " (replace mode)"
            )
            if st.button(f"✅ Confirm Import{_btn_suffix}", key="ib_confirm", type="primary"):
                from data.ib_flex_connector import run_import
                prog = st.progress(0.0, text="Importing…")
                try:
                    counts = run_import(
                        _imp_inv, _imp_tx, acc_id,
                        replace_mode=replace_mode,
                        progress_cb=lambda p: prog.progress(p, text="Importing…"),
                    )
                    prog.empty()
                    st.success("✅ Import complete!")
                    _import_summary(counts)
                    # Clear parsed state so next visit starts fresh
                    for k in ("ib_parsed", "ib_inv_records", "ib_tx_records", "ib_meta",
                              "ib_existing_inv", "ib_existing_tx",
                              "ib_fuzzy_inv", "ib_fuzzy_tx", "ib_sec_matches", "ib_ignored"):
                        st.session_state.pop(k, None)
                    _load_accounts.clear()
                    st.cache_data.clear()
                except Exception as exc:
                    prog.empty()
                    st.error(f"Import failed: {exc}")
                    st.code(traceback.format_exc())


# ===========================================================================
# Revolut Trading — brokerage CSV export
# ===========================================================================

def render_revolut_brokerage_import() -> None:
    st.markdown(
        "Import trades, dividends and cash events from a **Revolut Trading** "
        "(stocks/ETF brokerage) account using the CSV export from the Revolut app."
    )

    with st.expander("ℹ️ How to export from Revolut Trading (click to expand)", expanded=False):
        st.markdown("""
1. Open the **Revolut app** → tap **Stocks** (or **Investing**).
2. Tap the **clock / History** icon (top-right).
3. Tap the **↓ Download** / **Export** button.
4. Choose format **CSV** and a date range → tap **Download**.
5. Upload the downloaded `.csv` file below.

> **CSV columns:** Date · Ticker · Type · Quantity · Price per share ·
> Total amount · Currency · FX Rate
>
> **Supported types:** BUY · SELL · DIVIDEND · CUSTODY FEE ·
> CASH TOP-UP · CASH WITHDRAWAL
""")

    st.divider()

    # ── File upload ──────────────────────────────────────────────────────────
    st.markdown("### 📂 Statement File")
    uploaded = st.file_uploader(
        "Upload Revolut Trading CSV export",
        type=["csv"],
        key="revt_csv_upload",
        help="Export from Revolut → Stocks → History → Export CSV",
    )

    st.divider()

    # ── Account mapping ──────────────────────────────────────────────────────
    st.markdown("### 🏦 Account Mapping")
    acc_id, acc_name = _account_selectbox(
        "Import into account",
        key="revt_account",
        type_filter=["Brokerage", "Margin", "Other Investment", "Pension"],
    )

    st.divider()

    # ── Options ──────────────────────────────────────────────────────────────
    st.markdown("### ⚙️ Options")
    replace_mode = st.checkbox(
        "Replace mode — delete all existing Revolut Trading records for this account before importing",
        value=False, key="revt_replace",
        help="All Investment and Transaction rows whose Description starts with "
             "'REVT|' will be deleted first.",
    )

    st.divider()

    if uploaded is None:
        st.info("Upload a Revolut Trading CSV file above to continue.")
        return

    # ── Parse ────────────────────────────────────────────────────────────────
    parse_btn = st.button("🔍 Parse & Preview", key="revt_parse", type="primary",
                          disabled=acc_id is None)

    if parse_btn or st.session_state.get("revt_parsed"):
        from data.revolut_importer import (
            parse_revolut_trading_csv, build_trading_records,
            check_existing_records as _revt_check,
            check_fuzzy_duplicates as _revt_fuzzy,
            preview_security_matches as _revt_sec_matches,
        )

        if parse_btn:
            try:
                df_raw = parse_revolut_trading_csv(uploaded.read())
            except Exception as exc:
                st.error(f"Failed to parse CSV: {exc}")
                return

            try:
                inv_records, tx_records = build_trading_records(df_raw)
            except Exception as exc:
                st.error(f"Failed to build records: {exc}")
                st.code(traceback.format_exc())
                return

            # ── Reconciliation & securities matching (done once at parse) ──
            existing_inv, existing_tx = _revt_check(inv_records, tx_records, acc_id)
            fuzzy_inv, fuzzy_tx       = _revt_fuzzy(inv_records, tx_records, acc_id)
            fuzzy_inv -= existing_inv
            fuzzy_tx  -= existing_tx
            sec_matches = _revt_sec_matches(inv_records)

            from database.queries import get_ignored_records as _get_ignored_revt
            ignored_descs = _get_ignored_revt("Revolut Trading")

            st.session_state["revt_inv_records"]  = inv_records
            st.session_state["revt_tx_records"]   = tx_records
            st.session_state["revt_df_raw"]       = df_raw
            st.session_state["revt_existing_inv"] = existing_inv
            st.session_state["revt_existing_tx"]  = existing_tx
            st.session_state["revt_fuzzy_inv"]    = fuzzy_inv
            st.session_state["revt_fuzzy_tx"]     = fuzzy_tx
            st.session_state["revt_sec_matches"]  = sec_matches
            st.session_state["revt_ignored"]      = ignored_descs
            st.session_state["revt_parsed"]       = True

        inv_records   = st.session_state.get("revt_inv_records",  [])
        tx_records    = st.session_state.get("revt_tx_records",   [])
        df_raw        = st.session_state.get("revt_df_raw",       pd.DataFrame())
        existing_inv  = st.session_state.get("revt_existing_inv", set())
        existing_tx   = st.session_state.get("revt_existing_tx",  set())
        fuzzy_inv     = st.session_state.get("revt_fuzzy_inv",    set())
        fuzzy_tx      = st.session_state.get("revt_fuzzy_tx",     set())
        sec_matches   = st.session_state.get("revt_sec_matches",  {})
        ignored_descs = st.session_state.get("revt_ignored",      set())

        # ── File summary ──────────────────────────────────────────────────
        if not df_raw.empty:
            s1, s2, s3 = st.columns(3)
            s1.metric("Rows in file",    len(df_raw))
            s2.metric("Date range from", str(df_raw["date"].min()))
            s3.metric("Date range to",   str(df_raw["date"].max()))

        if not inv_records and not tx_records:
            st.warning("No importable records found in this file.")
            return

        # ── Transaction type breakdown ─────────────────────────────────────
        if not df_raw.empty:
            with st.expander("📊 Transaction type breakdown"):
                type_counts = (
                    df_raw.groupby("raw_type")["total_amount"]
                    .agg(count="count", total="sum")
                    .reset_index()
                    .sort_values("count", ascending=False)
                )
                st.dataframe(
                    type_counts,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "raw_type": "Type",
                        "count":    st.column_config.NumberColumn("# Rows",    format="%d"),
                        "total":    st.column_config.NumberColumn("Net Amount", format="%.2f"),
                    },
                )

        # ── Reconciliation summary ─────────────────────────────────────────
        truly_new_inv   = [r for r in inv_records
                           if r["desc"] not in existing_inv
                           and r["desc"] not in fuzzy_inv
                           and r["desc"] not in ignored_descs]
        truly_new_tx    = [r for r in tx_records
                           if r["desc"] not in existing_tx
                           and r["desc"] not in fuzzy_tx
                           and r["desc"] not in ignored_descs]
        fuzzy_only_inv  = [r for r in inv_records if r["desc"] in fuzzy_inv]
        fuzzy_only_tx   = [r for r in tx_records  if r["desc"] in fuzzy_tx]
        exist_inv_count = sum(1 for r in inv_records if r["desc"] in existing_inv)
        exist_tx_count  = sum(1 for r in tx_records  if r["desc"] in existing_tx)
        ignored_inv_count = sum(1 for r in inv_records if r["desc"] in ignored_descs)
        ignored_tx_count  = sum(1 for r in tx_records  if r["desc"] in ignored_descs)

        _skip_inv = exist_inv_count + len(fuzzy_only_inv)
        _skip_tx  = exist_tx_count  + len(fuzzy_only_tx)

        if not truly_new_inv and not truly_new_tx and not replace_mode:
            st.info(
                f"✅ Nothing genuinely new — "
                f"**{exist_inv_count}** inv exact + **{len(fuzzy_only_inv)}** likely-dup, "
                f"**{exist_tx_count}** tx exact + **{len(fuzzy_only_tx)}** likely-dup."
            )
        else:
            st.success(
                f"Found **{len(truly_new_inv)}** new investment record(s) and "
                f"**{len(truly_new_tx)}** new transaction(s) to import."
            )
        if fuzzy_only_inv or fuzzy_only_tx:
            st.warning(
                f"⚠️ **{len(fuzzy_only_inv)}** investment(s) and "
                f"**{len(fuzzy_only_tx)}** transaction(s) match existing records by "
                "date/action/amount but have a different description key — "
                "likely already entered manually or via another importer. "
                "They are marked **⚠️ Likely duplicate** in the preview and will be skipped."
            )
        if ignored_inv_count or ignored_tx_count:
            st.info(
                f"⏭️ **{ignored_inv_count}** investment(s) and "
                f"**{ignored_tx_count}** transaction(s) are on the ignore list and will be skipped. "
                "Use the **Ignore Records** panel below to manage them."
            )

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("🆕 New investments",   len(truly_new_inv))
        r2.metric("🔄 Skip investments",  _skip_inv + ignored_inv_count,
                  help="✅ Exact key match + ⚠️ Likely duplicates + ⏭️ Ignored")
        r3.metric("🆕 New transactions",  len(truly_new_tx))
        r4.metric("🔄 Skip transactions", _skip_tx + ignored_tx_count,
                  help="✅ Exact key match + ⚠️ Likely duplicates + ⏭️ Ignored")

        # ── Preview ──────────────────────────────────────────────────────────
        st.markdown("### 👁️ Preview")
        _revt_preview_with_status(
            inv_records, tx_records,
            existing_inv, existing_tx,
            fuzzy_inv, fuzzy_tx,
            sec_matches,
            ignored_descs=ignored_descs,
        )

        # ── Security Mappings ─────────────────────────────────────────────────
        _revt_security_mapping_ui(sec_matches)

        # ── Ignore manager ────────────────────────────────────────────────────
        _ignore_manager_ui(
            "Revolut Trading",
            inv_records, tx_records,
            existing_inv, existing_tx,
            fuzzy_inv, fuzzy_tx,
            ignored_descs,
            session_key="revt",
        )

        # ── Import ───────────────────────────────────────────────────────────
        st.divider()
        st.markdown("### 💾 Import")
        st.caption(f"Target account: **{acc_name}** (ID {acc_id})")

        _rcol1, _rcol2 = st.columns(2)
        _import_inv = _rcol1.checkbox("📈 Import investments",       value=True, key="revt_import_inv")
        _import_tx  = _rcol2.checkbox("💳 Import cash transactions", value=True, key="revt_import_tx")

        _base_inv  = truly_new_inv if not replace_mode else inv_records
        _base_tx   = truly_new_tx  if not replace_mode else tx_records
        _imp_inv   = _base_inv if _import_inv else []
        _imp_tx    = _base_tx  if _import_tx  else []
        _new_total = len(_imp_inv) + len(_imp_tx)

        if _new_total == 0 and not replace_mode:
            st.info(
                "No records to import.  "
                "Enable **Replace mode** above if you want to force a clean re-import."
            )
        else:
            _btn_suffix = (
                f" ({_new_total} record{'s' if _new_total != 1 else ''})"
                if not replace_mode else " (replace mode)"
            )
            if st.button(f"✅ Confirm Import{_btn_suffix}", key="revt_confirm", type="primary"):
                from data.revolut_importer import run_trading_import
                prog = st.progress(0.0, text="Importing…")
                try:
                    counts = run_trading_import(
                        _imp_inv, _imp_tx, acc_id,
                        replace_mode=replace_mode,
                        progress_cb=lambda p: prog.progress(p, text="Importing…"),
                    )
                    prog.empty()
                    st.success("✅ Import complete!")
                    _import_summary(counts)
                    for k in ("revt_parsed", "revt_inv_records", "revt_tx_records",
                              "revt_df_raw", "revt_existing_inv", "revt_existing_tx",
                              "revt_fuzzy_inv", "revt_fuzzy_tx", "revt_sec_matches",
                              "revt_ignored"):
                        st.session_state.pop(k, None)
                    _load_accounts.clear()
                    st.cache_data.clear()
                except Exception as exc:
                    prog.empty()
                    st.error(f"Import failed: {exc}")
                    st.code(traceback.format_exc())


# ===========================================================================
# Brokerage section — top-level container (called from ui/importers.py)
# ===========================================================================

def render_brokerage_section() -> None:
    """Render brokerage importers as tabs (IB · Revolut Trading · Capital.com · FxPro)."""
    tab_ib, tab_revt, tab_capital, tab_fxpro = st.tabs([
        "📊 Interactive Brokers",
        "💚 Revolut Trading",
        "📈 Capital.com",
        "📈 FxPro",
    ])

    with tab_ib:
        _brand_header("https://logo.clearbit.com/interactivebrokers.com",
                      "Interactive Brokers")
        render_ib_import()

    with tab_revt:
        _brand_header("https://logo.clearbit.com/revolut.com",
                      "Revolut Trading")
        render_revolut_brokerage_import()

    with tab_capital:
        _brand_header("https://logo.clearbit.com/capital.com",
                      "Capital.com")
        from data.capitalcom_importer import render_capitalcom_importer
        render_capitalcom_importer()

    with tab_fxpro:
        _brand_header("https://logo.clearbit.com/fxpro.com",
                      "FxPro")
        from data.fxpro_importer import render_fxpro_importer
        render_fxpro_importer()



# ===========================================================================
# Revolut Personal — CSV export
# ===========================================================================

def render_revolut_import() -> None:
    _brand_header("https://logo.clearbit.com/revolut.com", "Revolut Personal")
    st.markdown(
        "Import transactions from a **Revolut Personal** account using the "
        "CSV statement export from the Revolut app."
    )

    with st.expander("ℹ️ How to export from Revolut (click to expand)", expanded=False):
        st.markdown("""
1. Open the **Revolut app** → tap your account.
2. Tap the **↓** (statement) icon in the top-right corner.
3. Choose a date range → select format **CSV** → tap **Download**.
4. Upload the downloaded `.csv` file below.

> **Note:** Revolut Personal has no public API, so the CSV export is the
> only programmatic option.  Upload one statement file at a time; repeat
> for each month or date range you want to import.
""")

    st.divider()

    # ── File upload ──────────────────────────────────────────────────────────
    st.markdown("### 📂 Statement File")
    uploaded = st.file_uploader(
        "Upload Revolut CSV export",
        type=["csv"],
        key="rev_csv_upload",
        help="Export from Revolut app → Account → ↓ → CSV",
    )

    st.divider()

    # ── Account mapping ──────────────────────────────────────────────────────
    st.markdown("### 🏦 Account Mapping")
    acc_id, acc_name = _account_selectbox(
        "Import into account",
        key="rev_account",
        type_filter=["Cash", "Checking", "Savings", "Other"],
    )

    st.divider()

    # ── Options ──────────────────────────────────────────────────────────────
    st.markdown("### ⚙️ Options")
    replace_mode = st.checkbox(
        "Replace mode — delete all existing Revolut records for this account before importing",
        value=False, key="rev_replace",
        help="All Investment and Transaction rows whose Description starts with "
             "'REV|' will be deleted first.",
    )

    st.divider()

    if uploaded is None:
        st.info("Upload a Revolut CSV file above to continue.")
        return

    # ── Parse ────────────────────────────────────────────────────────────────
    parse_btn = st.button("🔍 Parse & Preview", key="rev_parse", type="primary",
                          disabled=acc_id is None)

    if parse_btn or st.session_state.get("rev_parsed"):
        from data.revolut_importer import (
            parse_revolut_csv, build_records,
            investments_preview_df, transactions_preview_df,
        )

        if parse_btn:
            try:
                df_raw = parse_revolut_csv(uploaded.read())
            except Exception as exc:
                st.error(f"Failed to parse CSV: {exc}")
                return

            try:
                inv_records, tx_records = build_records(df_raw)
            except Exception as exc:
                st.error(f"Failed to build records: {exc}")
                st.code(traceback.format_exc())
                return

            st.session_state["rev_inv_records"] = inv_records
            st.session_state["rev_tx_records"]  = tx_records
            st.session_state["rev_df_raw"]       = df_raw
            st.session_state["rev_parsed"]        = True

        inv_records = st.session_state.get("rev_inv_records", [])
        tx_records  = st.session_state.get("rev_tx_records",  [])
        df_raw      = st.session_state.get("rev_df_raw",      pd.DataFrame())

        # ── Summary ──────────────────────────────────────────────────────────
        if not df_raw.empty:
            s1, s2, s3 = st.columns(3)
            s1.metric("Rows in file",        len(df_raw))
            s2.metric("Date range from",     str(df_raw["date"].min()))
            s3.metric("Date range to",       str(df_raw["date"].max()))

        if not inv_records and not tx_records:
            st.warning("No importable records found in this file.")
            return

        st.success(
            f"Parsed **{len(inv_records)}** investment records "
            f"and **{len(tx_records)}** transactions."
        )

        # ── Transaction type breakdown ────────────────────────────────────────
        if not df_raw.empty:
            with st.expander("📊 Transaction type breakdown"):
                type_counts = (
                    df_raw.groupby("type")["amount"]
                    .agg(count="count", total="sum")
                    .reset_index()
                    .sort_values("count", ascending=False)
                )
                st.dataframe(
                    type_counts,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "type":  "Type",
                        "count": st.column_config.NumberColumn("# Rows", format="%d"),
                        "total": st.column_config.NumberColumn("Net Amount", format="%.2f"),
                    },
                )

        # ── Preview ──────────────────────────────────────────────────────────
        st.markdown("### 👁️ Preview")
        _preview_records(inv_records, tx_records,
                         investments_preview_df, transactions_preview_df)

        # ── Import ───────────────────────────────────────────────────────────
        st.divider()
        st.markdown("### 💾 Import")
        st.caption(f"Target account: **{acc_name}** (ID {acc_id})")

        if st.button("✅ Confirm Import", key="rev_confirm", type="primary"):
            from data.revolut_importer import run_import
            prog = st.progress(0.0, text="Importing…")
            try:
                counts = run_import(
                    inv_records, tx_records, acc_id,
                    replace_mode=replace_mode,
                    progress_cb=lambda p: prog.progress(p, text="Importing…"),
                )
                prog.empty()
                st.success("✅ Import complete!")
                _import_summary(counts)
                for k in ("rev_parsed", "rev_inv_records", "rev_tx_records", "rev_df_raw"):
                    st.session_state.pop(k, None)
                _load_accounts.clear()
                st.cache_data.clear()
            except Exception as exc:
                prog.empty()
                st.error(f"Import failed: {exc}")
                st.code(traceback.format_exc())
