import logging
import streamlit as st
import yfinance as yf
import pdfplumber
import requests
import io
import re
import time
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from database.connection import get_connection
from ai.llm import get_custom_session
from datetime import datetime, timedelta
from decimal import Decimal
from config.settings import ENV_CONFIG
from psycopg2.extras import execute_batch



def download_historical_fx(tsperiod=None, currencies_id=None):
    """Download historical FX rates from Yahoo Finance.

    Parameters
    ----------
    tsperiod : str, optional
        Yahoo Finance period string (e.g. "1mo", "1y"). Defaults to "1m".
    currencies_id : int, optional
        When provided, only download rates for this single Currencies_Id.
        When omitted (None), download rates for every non-EUR currency.
    """
    conn = get_connection()
    cur = conn.cursor()
    custom_session = get_custom_session()

    if not tsperiod:
        tsperiod="1m"

    try:
        cur.execute("SELECT Currencies_Id FROM Currencies WHERE Currencies_ShortName = 'EUR'")
        target_id = cur.fetchone()[0]

        if currencies_id is not None:
            cur.execute(
                "SELECT Currencies_Id, Currencies_ShortName FROM Currencies "
                "WHERE Currencies_Id = %s AND Currencies_ShortName != 'EUR'",
                (int(currencies_id),),
            )
        else:
            cur.execute(
                "SELECT Currencies_Id, Currencies_ShortName FROM Currencies "
                "WHERE Currencies_ShortName != 'EUR'"
            )
        currencies = cur.fetchall()
        
        for base_id, symbol in currencies:
            logging.info(f"Downloading historical data for {symbol}...")
            ticker_symbol = f"EUR{symbol}=X"
            ticker = yf.Ticker(ticker_symbol, session=custom_session)
            hist = ticker.history(period=tsperiod)
            
            if hist.empty:
                logging.warning(f"No data found for {ticker_symbol}")
                continue
            
            for date, row in hist.iterrows():
                rate_to_eur = float(1 / row['Close'])
                formatted_date = date.strftime('%Y-%m-%d')
                
                cur.execute("""
                    INSERT INTO Historical_FX (Currencies_Id_1, Currencies_Id_2, Date, FX_Rate)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (Currencies_Id_1, Currencies_Id_2, Date)
                    DO UPDATE SET FX_Rate = EXCLUDED.FX_Rate
                """, (base_id, target_id, formatted_date, rate_to_eur))
            
            conn.commit()
            logging.info(f"Completed import for {symbol}")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
        logging.error(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def download_securities_info_from_yahoo(target_sec_id=None):
    """Download securities information from Yahoo Finance.

    Fetches sector, industry, analyst rating and target price for all
    securities that have a Yahoo_Ticker defined.  Requests are made in
    parallel (up to MAX_WORKERS concurrent threads) to minimise wall-clock
    time; DB writes are batched into a single executemany + commit.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    MAX_WORKERS = 5   # conservative — avoids Yahoo rate-limiting

    conn = get_connection()
    cur  = conn.cursor()
    custom_session = get_custom_session()

    def _fetch(sec_id, sec_name, symbol):
        """Fetch Yahoo info for one ticker; returns a result tuple."""
        try:
            info = yf.Ticker(symbol, session=custom_session).info
            sector   = info.get('sector')   or None
            industry = info.get('industry') or None
            _raw     = info.get('recommendationKey')
            rating   = (
                None
                if (not _raw or str(_raw).strip().lower() in ('none', 'n/a', ''))
                else str(_raw).strip().lower()
            )
            target_price = info.get('targetMeanPrice') or None
            return (sec_id, sec_name, symbol, sector, industry, rating, target_price, None)
        except Exception as exc:
            return (sec_id, sec_name, symbol, None, None, None, None, str(exc))

    try:
        base_query = """
            SELECT Securities_Id, Securities_Name, Yahoo_Ticker
            FROM   Securities
            WHERE  Yahoo_Ticker IS NOT NULL
              AND  Yahoo_Ticker != ''
              AND  Securities_Name NOT LIKE 'Hellenic T-Bill%'
        """
        if target_sec_id:
            base_query += f" AND Securities_Id = {int(target_sec_id)}"
        base_query += " ORDER BY Securities_Name ASC"

        cur.execute(base_query)
        securities = cur.fetchall()

        if not securities:
            logging.warning("No matching securities found with a valid Yahoo Ticker.")
            return

        total = len(securities)
        print(f"Fetching Yahoo info for {total} securities (up to {MAX_WORKERS} in parallel)…")
        logging.info(f"Fetching Yahoo info for {total} securities…")

        # ── Parallel fetch ────────────────────────────────────────────────────
        results = []
        futures = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for sec_id, sec_name, symbol in securities:
                f = pool.submit(_fetch, sec_id, sec_name, symbol)
                futures[f] = sec_name

            for f in as_completed(futures):
                results.append(f.result())

        # ── Batch DB update ───────────────────────────────────────────────────
        updates = []
        for sec_id, sec_name, symbol, sector, industry, rating, target_price, err in results:
            if err:
                print(f"  ⚠️ Error fetching {sec_name} ({symbol}): {err}")
                logging.warning(f"Yahoo info error for {sec_name} ({symbol}): {err}")
                continue
            if not sector or not industry:
                print(f"  ⚠️ Limited data for {sec_name} ({symbol}) — skipping")
                logging.warning(f"Limited Yahoo data for {sec_name} ({symbol})")
                continue
            print(f"  ✔ {sec_name}: sector={sector}, industry={industry}, "
                  f"rating={rating}, target={target_price}")
            logging.info(f"Yahoo info {sec_name}: sector={sector}, industry={industry}, "
                         f"rating={rating}, target={target_price}")
            updates.append((sector, industry, rating, target_price, sec_id))

        if updates:
            cur.executemany("""
                UPDATE Securities
                SET    Sector               = %s,
                       Industry             = %s,
                       Analyst_Rating       = COALESCE(%s, Analyst_Rating),
                       Analyst_Target_Price = COALESCE(%s, Analyst_Target_Price)
                WHERE  Securities_Id = %s
            """, updates)
            conn.commit()

        print(f"Yahoo info update complete — {len(updates)}/{total} securities updated.")
        logging.info(f"Yahoo info update complete — {len(updates)}/{total} updated.")

    except Exception as e:
        st.error(f"❌ Error: {e}")
        logging.error(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def download_historical_prices_from_yahoo(tsperiod=None, target_sec_id=None):
    """Download historical security prices from Yahoo Finance.

    Requests are made in parallel (up to MAX_WORKERS concurrent threads) to
    minimise wall-clock time.  All rows are collected in memory first, then
    written to the DB in a single execute_batch + commit so the connection is
    never held open across slow network calls.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    MAX_WORKERS = 5   # conservative — avoids Yahoo rate-limiting

    conn = get_connection()
    cur  = conn.cursor()
    custom_session = get_custom_session()

    if not tsperiod:
        tsperiod = "1m"

    def _fetch(sec_id, sec_name, symbol):
        """Fetch OHLCV history for one ticker; returns (sec_id, sec_name, symbol, rows, error)."""
        try:
            hist = yf.Ticker(symbol, session=custom_session).history(period=tsperiod)
            if hist is None or hist.empty:
                return sec_id, sec_name, symbol, [], None
            rows = []
            for date, row in hist.iterrows():
                if 'Close' not in row or pd.isna(row['Close']):
                    continue
                rows.append((
                    int(sec_id),
                    date.strftime('%Y-%m-%d'),
                    float(row['Close']),
                    float(row['High'])   if 'High'   in row and not pd.isna(row['High'])   else None,
                    float(row['Low'])    if 'Low'    in row and not pd.isna(row['Low'])    else None,
                    float(row['Volume']) if 'Volume' in row and not pd.isna(row['Volume']) else 0,
                ))
            return sec_id, sec_name, symbol, rows, None
        except Exception as exc:
            return sec_id, sec_name, symbol, [], str(exc)

    try:
        base_query = """
            SELECT Securities_Id, Securities_Name, Yahoo_Ticker
            FROM   Securities
            WHERE  Yahoo_Ticker IS NOT NULL
              AND  Yahoo_Ticker != ''
              AND  Securities_Name NOT LIKE 'Hellenic T-Bill%'
        """
        if target_sec_id:
            base_query += f" AND Securities_Id = {target_sec_id}"
        base_query += " ORDER BY Securities_Name ASC"

        cur.execute(base_query)
        securities = cur.fetchall()

        if not securities:
            logging.warning("No matching securities found with a valid Yahoo Ticker.")
            return

        total = len(securities)
        logging.info(f"Fetching Yahoo prices for {total} securities (up to {MAX_WORKERS} in parallel)…")
        print(f"Fetching Yahoo prices for {total} securities (up to {MAX_WORKERS} in parallel)…")

        # ── Parallel fetch ────────────────────────────────────────────────────
        all_rows = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_fetch, sid, sname, sym): sname
                       for sid, sname, sym in securities}
            for f in as_completed(futures):
                sec_id, sec_name, symbol, rows, err = f.result()
                if err:
                    logging.warning(f"Price fetch error for {sec_name} ({symbol}): {err}")
                    print(f"  ⚠️ Error fetching {sec_name} ({symbol}): {err}")
                elif not rows:
                    logging.warning(f"No data for {sec_name} ({symbol})")
                    print(f"  ⚠️ No data for {sec_name} ({symbol})")
                else:
                    all_rows.extend(rows)
                    logging.info(f"  ✔ {sec_name}: {len(rows)} rows")
                    print(f"  ✔ {sec_name}: {len(rows)} rows")

        # ── Single batch upsert ───────────────────────────────────────────────
        if all_rows:
            execute_batch(cur, """
                INSERT INTO Historical_Prices (Securities_Id, Date, Close, High, Low, Volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (Securities_Id, Date)
                DO UPDATE SET
                    Close  = EXCLUDED.Close,
                    High   = EXCLUDED.High,
                    Low    = EXCLUDED.Low,
                    Volume = EXCLUDED.Volume
            """, all_rows, page_size=500)
            conn.commit()

        logging.info(
            f"Yahoo price download complete — {len(all_rows)} rows upserted "
            f"for {total} securities."
        )
        print(
            f"Yahoo price download complete — {len(all_rows)} rows upserted "
            f"for {total} securities."
        )

    except Exception as e:
        conn.rollback()
        st.error(f"❌ Error: {e}")
        logging.error(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

    _refresh_materialized_views_async()


# ======================================================
# MATERIALIZED VIEW REFRESH
# ======================================================

def refresh_materialized_views():
    """Refresh mv_latest_prices and mv_latest_fx after price/FX downloads."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_mv_prices_fx();")
        conn.commit()
        logging.info("Materialized views refreshed.")
    except Exception as e:
        logging.warning(f"MV refresh skipped (views may not exist yet): {e}")
    finally:
        conn.close()


def _refresh_materialized_views_async():
    """Fire-and-forget wrapper — runs refresh_materialized_views() in a
    background daemon thread so the calling download function can return
    immediately without blocking the Streamlit UI."""
    import threading
    threading.Thread(target=refresh_materialized_views, daemon=True).start()


# ======================================================
# DATE RANGE HELPER
# ======================================================

def get_smart_date_range(time_input="1mo"):
    """
    Converts Yahoo Finance / EODHD style period strings to a date range.
        15d  = 15 days
        1w   = 1 week
        1mo  = 1 month   (also accepts legacy '1m')
        3mo  = 3 months  (also accepts legacy '3m')
        1y   = 1 year
    Returns:
        from_date, to_date (YYYY-MM-DD)
    """
 
    to_date_obj = datetime.today()
 
    if not time_input:
        time_input = "1mo"
 
    s = str(time_input).lower().strip()
 
    # Normalise legacy short-month format: '1m' → '1mo', '3m' → '3mo'
    # (but don't touch 'min' or other non-month uses of 'm')
    s = re.sub(r'^(\d+)m$', r'mo', s)
 
    # Accept: 15d, 1w, 1mo, 3mo, 1y
    match = re.match(r"^(\d+)(d|w|mo|y)$", s)
 
    if not match:
        logging.warning(f"Invalid period format {time_input!r}, defaulting to 1mo")
        match = re.match(r"^(\d+)(d|w|mo|y)$", "1mo")
 
    value = int(match.group(1))
    unit = match.group(2)
 
    multiplier = {
        "d":  1,
        "w":  7,
        "mo": 30,
        "y":  365
    }
 
    days = value * multiplier[unit]
 
    from_date_obj = to_date_obj - timedelta(days=days)
 
    return (
        from_date_obj.strftime("%Y-%m-%d"),
        to_date_obj.strftime("%Y-%m-%d")
    )
 

# ======================================================
# TRADINGVIEW DOWNLOAD
# ======================================================

def _period_to_n_bars(tsperiod: str) -> int:
    s = str(tsperiod).lower().strip()
    s = re.sub(r"^(\d+)m$", r"\1mo", s)
    match = re.match(r"^(\d+)(d|w|mo|y)$", s)
    if not match:
        return 40
    value = int(match.group(1))
    unit = match.group(2)
    multiplier = {"d": 1, "w": 5, "mo": 22, "y": 250}
    return value * multiplier[unit] + 10


class _PersistentTvDatafeed(TvDatafeed):
    """TvDatafeed variant that keeps the WebSocket alive across get_hist() calls.

    The standard TvDatafeed.get_hist() calls __create_connection() on every
    invocation, paying a full TCP/TLS handshake per security.  This subclass
    opens the connection once via _connect() and reuses it, generating a fresh
    chart_session per symbol.

    Issues fixed vs the naive persistent approach:
    - Quote session removed: quote_create_session / quote_add_symbols enable
      streaming tick updates that pile up in the socket buffer after each
      request returns, causing the next call to read stale data and the socket
      to appear "already closed".
    - Socket drain: after series_completed we consume any residual frames
      (du, acks, etc.) with a 150 ms window so the next request starts clean.
    - Chart-session filtering: even after the drain, late-arriving du frames
      from a previous session can slip in at the start of the next recv loop.
      We filter raw_data to lines that belong to this chart_session before
      parsing, eliminating data bleed entirely.
    - Reconnect stagger: when a reconnect is needed mid-batch the shared
      connect_lock (passed in at construction time) is acquired so no two
      threads hammer TradingView simultaneously and trigger HTTP 429.
    """

    import re as _re
    # Matches any chart-session token (cs_xxxxxxxxxxxxxxxx) that is NOT ours.
    # Used to filter stale frames out of raw_data before parsing.
    _CS_RE = _re.compile(r'\bcs_[a-zA-Z0-9]+\b')

    # TradingView frames that signal the end of a series request (success or error).
    # Breaking on error frames avoids waiting for the 5 s socket timeout when
    # TradingView has no data for a symbol but keeps the connection alive.
    _TERMINAL_FRAMES = ("series_completed", "series_error", "symbol_error", "critical_error")

    # How long (seconds) to wait for each ws.recv() call.
    # Kept below the library default (5 s) so "no-data" stalls are shorter.
    _WS_TIMEOUT = 2

    def __init__(self, *args, connect_lock=None, **kwargs):
        """Initialise TvDatafeed with an optional shared connect_lock.

        connect_lock : threading.Lock (or RLock), optional
            When provided, both initial _connect() calls (via _get_tv()) and
            reconnect _connect() calls (via get_hist()) acquire this lock so
            that no more than one thread opens a new WebSocket at a time.
            This prevents HTTP 429 errors when multiple workers try to
            (re)connect simultaneously.
        """
        super().__init__(*args, **kwargs)
        self._connect_lock = connect_lock

    def _connect(self):
        """Open the WebSocket and authenticate.  No quote session — not needed for OHLCV.

        Does NOT acquire _connect_lock — the caller is responsible for holding
        the lock before calling this method (see _get_tv() and _reconnect()).
        """
        self._TvDatafeed__create_connection()
        try:
            self.ws.settimeout(self._WS_TIMEOUT)
        except Exception:
            pass
        self._TvDatafeed__send_message("set_auth_token", [self.token])

    def _reconnect(self):
        """Reconnect, acquiring the shared connect_lock if one was provided.

        Called from get_hist() when a mid-batch WS error is detected.
        Serialises reconnects across all worker threads so TradingView does
        not see a burst of simultaneous new connections (HTTP 429).
        """
        if self._connect_lock is not None:
            with self._connect_lock:
                time.sleep(0.5)   # same stagger as initial connection
                self._connect()
        else:
            self._connect()

    def get_hist(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False,
    ) -> pd.DataFrame:
        symbol_full = self._TvDatafeed__format_symbol(symbol, exchange, fut_contract)
        interval_val = interval.value
        session_str  = '"regular"' if not extended_session else '"extended"'

        for attempt in range(2):
            try:
                return self._request_hist(symbol_full, interval_val, n_bars, session_str)
            except Exception as exc:
                if attempt == 0:
                    logging.warning(
                        f"TV WS error on {symbol_full} (attempt 1): {exc} — reconnecting…"
                    )
                    try:
                        self._reconnect()   # uses connect_lock + stagger
                    except Exception as re_exc:
                        raise RuntimeError(
                            f"TV reconnect failed for {symbol_full}: {re_exc}"
                        ) from re_exc
                else:
                    raise

    def _request_hist(
        self, symbol_full: str, interval_val: str, n_bars: int, session_str: str
    ) -> pd.DataFrame:
        """Send a history request on the already-open WS and return a DataFrame."""
        chart_session = self._TvDatafeed__generate_chart_session()
        send = self._TvDatafeed__send_message   # alias for brevity

        send("chart_create_session", [chart_session, ""])
        send("resolve_symbol", [
            chart_session, "symbol_1",
            f'={{"symbol":"{symbol_full}","adjustment":"splits",'
            f'"session":{session_str}}}',
        ])
        send("create_series",   [chart_session, "s1", "s1", "symbol_1",
                                  interval_val, n_bars])
        send("switch_timezone", [chart_session, "exchange"])

        raw_data = ""
        while True:
            try:
                result = self.ws.recv()
                raw_data += result + "\n"
            except Exception:
                break

            if any(frame in result for frame in self._TERMINAL_FRAMES):
                if "series_completed" in result:
                    # Delete the chart session so TradingView stops sending
                    # real-time updates (du frames) for it.  Without this,
                    # stale du frames from old sessions bleed into the next
                    # security's raw_data and corrupt its price history.
                    try:
                        send("chart_delete_session", [chart_session])
                    except Exception:
                        pass
                    # Drain the delete-ack and any other residual frames so
                    # the next get_hist() call starts with a clean buffer.
                    # Simple blind drain is safe here because the session is
                    # already deleted — TV won't send any more data frames.
                    try:
                        self.ws.settimeout(0.15)
                        while True:
                            try:
                                self.ws.recv()
                            except Exception:
                                break
                    finally:
                        try:
                            self.ws.settimeout(self._WS_TIMEOUT)
                        except Exception:
                            pass
                break

        # ── Chart-session filtering ───────────────────────────────────────────
        # Despite chart_delete_session + the drain window, a late-arriving du
        # frame from a *previous* session can still slip through and appear at
        # the top of raw_data for this security (its chart_session token is
        # different from ours).  Strip any line that contains a foreign
        # cs_xxxxxxxxxxxxxxxx token to eliminate bleed unconditionally.
        filtered_lines = [
            line for line in raw_data.split('\n')
            if chart_session in line                  # belongs to our session
            or not self._CS_RE.search(line)           # general protocol frame (no cs_ at all)
        ]
        raw_data = '\n'.join(filtered_lines)

        return self._TvDatafeed__create_df(raw_data, symbol_full)


def _tv_recommend_to_rating(value):
    """Convert TradingView Recommend.All (-1 … +1) to a rating label.

    NOTE: Recommend.All is a TECHNICAL indicator composite (MAs, RSI, MACD…),
    NOT broker analyst consensus.  It is NOT written to Analyst_Rating — kept
    here in case a future 'Technical_Rating' column is added.

    Returns None (→ SQL NULL) when the value is absent, non-numeric, or a
    sentinel string ('none', 'n/a', 'nan') so we never write the literal
    string "none" into the database.
    """
    if value is None:
        return None
    # Guard against sentinel strings
    if isinstance(value, str) and value.strip().lower() in ('none', 'n/a', 'nan', ''):
        return None
    try:
        import math
        v = float(value)
        if math.isnan(v):
            return None
        if v >= 0.5:    return 'strong_buy'
        elif v >= 0.1:  return 'buy'
        elif v > -0.1:  return 'hold'
        elif v > -0.5:  return 'underperform'
        else:           return 'sell'
    except (ValueError, TypeError):
        return None


def download_securities_info_from_tradingview(target_sec_id=None, overwrite=False):
    """Fetch Sector, Industry, Analyst Rating and Target Price from TradingView Screener.

    Acts as a fallback for securities that Yahoo Finance does not cover (e.g. ATHEX
    stocks).  By default only fills NULL columns; pass overwrite=True to refresh all.

    Securities must have TV_Symbol and TV_Exchange populated.
    The Recommend.All field (-1 … +1) is mapped to the same rating strings used by
    the Yahoo Finance downloader (strong_buy / buy / hold / underperform / sell).
    """
    try:
        from tradingview_screener import Query, Column
    except ImportError:
        print("tradingview-screener not installed. Run: pip install tradingview-screener")
        return

    conn = get_connection()
    cur  = conn.cursor()

    try:
        sql = """
            SELECT Securities_Id, Securities_Name, TV_Symbol, TV_Exchange
            FROM   Securities
            WHERE  TV_Symbol   IS NOT NULL AND TV_Symbol   != ''
              AND  TV_Exchange IS NOT NULL AND TV_Exchange != ''
        """
        params = []

        if not overwrite:
            # Analyst_Rating is intentionally excluded: TradingView's screener API
            # only exposes technical ratings (Recommend.All), not broker consensus.
            # Analyst_Rating is populated exclusively by Yahoo Finance.
            sql += " AND (Sector IS NULL OR Industry IS NULL OR Analyst_Target_Price IS NULL)"

        if target_sec_id:
            sql += " AND Securities_Id = %s"
            params.append(int(target_sec_id))

        sql += " ORDER BY Securities_Name"
        cur.execute(sql, params)
        rows = cur.fetchall()

        if not rows:
            print("No securities require TradingView info update.")
            logging.info("No securities require TradingView info update.")
            return

        print(f"Fetching TradingView screener data for {len(rows)} securities...")
        logging.info(f"Fetching TradingView screener data for {len(rows)} securities...")

        # Build lookup: "TV_EXCHANGE:TV_SYMBOL" (upper) → (sec_id, sec_name)
        # Using full ticker ensures we match the exact security, not a same-named
        # symbol on a different exchange (e.g. ATHEX:GD ≠ NYSE:GD).
        sec_map = {
            f"{r[3].upper()}:{r[2].upper()}": (r[0], r[1])
            for r in rows
        }
        full_tickers = list(sec_map.keys())

        BATCH = 50
        updated = 0

        for i in range(0, len(full_tickers), BATCH):
            batch = full_tickers[i : i + BATCH]
            try:
                _count, df = (
                    Query()
                    .select('name', 'sector', 'industry', 'price_target_average')
                    .set_tickers(*batch)
                    .get_scanner_data()
                )
            except Exception as e:
                print(f"  Screener query error for batch {i//BATCH + 1}: {e}")
                logging.warning(f"TV screener batch error: {e}")
                continue

            for _, row in df.iterrows():
                # 'ticker' column is always returned as EXCHANGE:SYMBOL
                full_ticker = str(row.get('ticker', '')).upper()
                if full_ticker not in sec_map:
                    continue

                sec_id, sec_name = sec_map[full_ticker]
                sector       = row.get('sector')              or None
                industry     = row.get('industry')            or None
                target_price = row.get('price_target_average') or None

                print(f"  {sec_name}: sector={sector}, industry={industry}, "
                      f"target={target_price}")
                logging.info(f"TV screener {sec_name}: sector={sector}, "
                             f"industry={industry}, target={target_price}")

                if overwrite:
                    cur.execute("""
                        UPDATE Securities
                        SET    Sector               = COALESCE(%s, Sector),
                               Industry             = COALESCE(%s, Industry),
                               Analyst_Target_Price = COALESCE(%s, Analyst_Target_Price)
                        WHERE  Securities_Id = %s
                    """, (sector, industry, target_price, sec_id))
                else:
                    # Only fill genuinely empty columns
                    cur.execute("""
                        UPDATE Securities
                        SET    Sector               = COALESCE(Sector, %s),
                               Industry             = COALESCE(Industry, %s),
                               Analyst_Target_Price = COALESCE(Analyst_Target_Price, %s)
                        WHERE  Securities_Id = %s
                    """, (sector, industry, target_price, sec_id))

                updated += 1

            conn.commit()

        print(f"TradingView screener update complete — {updated} securities updated.")
        logging.info(f"TradingView screener update complete — {updated} updated.")

    except Exception as e:
        print(f"Error in download_securities_info_from_tradingview: {e}")
        logging.error(f"TV screener error: {e}")
    finally:
        cur.close()
        conn.close()


def download_historical_prices_from_tradingview(tsperiod="1m", target_sec_id=None):
    """Download and upsert historical daily prices from TradingView into DB.

    Securities must have TV_Symbol and TV_Exchange populated.

    Each worker thread owns its own TvDatafeed() WebSocket instance so calls
    run concurrently without sharing mutable state.  All rows are collected in
    memory first, then written to the DB in a single execute_batch + commit.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    MAX_WORKERS = 5   # each worker holds one persistent WS connection

    if not tsperiod:
        tsperiod = "1m"
    tsperiod = str(tsperiod).lower().strip()
    n_bars = _period_to_n_bars(tsperiod)

    logging.info(f"Starting TradingView download: period={tsperiod}, n_bars={n_bars}, "
                 f"target_sec_id={target_sec_id}")
    print(f"Starting TradingView download: period={tsperiod}, n_bars={n_bars}, "
          f"target_sec_id={target_sec_id}")

    # Thread-local _PersistentTvDatafeed instances.
    # Each worker thread opens ONE WebSocket on first use (_connect()) and
    # reuses it for every subsequent get_hist() call, eliminating the
    # per-security TCP/TLS handshake that made the sequential version slow.
    #
    # We also track every instance in _tv_instances so we can close their
    # WebSocket connections explicitly *before* the ThreadPoolExecutor exits.
    # Without this, pool.shutdown(wait=True) triggers thread-local GC cleanup
    # which calls WebSocket.__del__() → WebSocket.close() on each thread — a
    # blocking TCP close-handshake that serialises across all 5 workers and
    # adds several seconds after the last security is logged.
    _tv_local     = threading.local()
    _tv_instances: list = []
    _tv_lock      = threading.Lock()
    _connect_lock = threading.Lock()   # serialise initial WS connections

    def _get_tv():
        if not hasattr(_tv_local, 'tv'):
            # Hold the lock while connecting so all MAX_WORKERS threads don't
            # hammer TradingView simultaneously — that triggers HTTP 429.
            with _connect_lock:
                time.sleep(0.5)        # stagger: give TV time between connections
                # Pass connect_lock so that mid-batch reconnects (in get_hist)
                # also serialise through the same lock and avoid HTTP 429.
                tv = _PersistentTvDatafeed(connect_lock=_connect_lock)
                tv._connect()          # lock is already held — no re-entry needed
                _tv_local.tv = tv
                with _tv_lock:
                    _tv_instances.append(tv)
        return _tv_local.tv

    def _fetch(sec_id, sec_name, tv_symbol, tv_exchange):
        """Fetch OHLCV history for one security; returns (sec_id, sec_name, symbol, rows, error)."""
        try:
            df = _get_tv().get_hist(
                symbol=tv_symbol,
                exchange=tv_exchange,
                interval=Interval.in_daily,
                n_bars=n_bars,
            )
            if df is None or df.empty:
                return sec_id, sec_name, tv_symbol, [], None
            rows = []
            for date, row in df.iterrows():
                rows.append((
                    int(sec_id),
                    pd.Timestamp(date).strftime("%Y-%m-%d"),
                    float(row["close"]),
                    None if pd.isna(row["high"])   else float(row["high"]),
                    None if pd.isna(row["low"])    else float(row["low"]),
                    0    if pd.isna(row["volume"]) else int(row["volume"]),
                ))
            return sec_id, sec_name, tv_symbol, rows, None
        except Exception as exc:
            import traceback
            return sec_id, sec_name, tv_symbol, [], f"{exc}\n{traceback.format_exc()}"

    conn = get_connection()
    cur  = conn.cursor()

    try:
        query = """
            SELECT Securities_Id, Securities_Name, TV_Symbol, TV_Exchange
            FROM   Securities
            WHERE  TV_Symbol   IS NOT NULL AND TV_Symbol   != ''
              AND  TV_Exchange IS NOT NULL AND TV_Exchange != ''
        """
        params = []
        if target_sec_id:
            query += " AND Securities_Id = %s"
            params.append(int(target_sec_id))
        query += " ORDER BY Securities_Name"

        cur.execute(query, params)
        securities = cur.fetchall()

        if not securities:
            logging.warning("No securities with TV_Symbol/TV_Exchange found.")
            print("No securities with TV_Symbol/TV_Exchange found.")
            return

        total = len(securities)
        logging.info(f"Fetching {n_bars} bars for {total} securities "
                     f"(up to {MAX_WORKERS} in parallel)…")
        print(f"Fetching {n_bars} bars for {total} securities "
              f"(up to {MAX_WORKERS} in parallel)…")

        # ── Parallel fetch ────────────────────────────────────────────────────
        all_rows = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(_fetch, sid, sname, sym, exch): sname
                for sid, sname, sym, exch in securities
            }
            for f in as_completed(futures):
                sec_id, sec_name, tv_symbol, rows, err = f.result()
                if err:
                    logging.error(f"Failed {tv_symbol}: {err}")
                    print(f"  ⚠️ Failed {tv_symbol}: {err.splitlines()[0]}")
                elif not rows:
                    logging.warning(f"No data for {tv_symbol}")
                    print(f"  ⚠️ No data for {tv_symbol}")
                else:
                    all_rows.extend(rows)
                    logging.info(f"  ✔ {sec_name} ({tv_symbol}): {len(rows)} rows")
                    print(f"  ✔ {sec_name} ({tv_symbol}): {len(rows)} rows")

            # Close all WebSocket connections explicitly before the executor
            # exits.  If we leave this to thread-local GC (which happens
            # inside pool.shutdown(wait=True)), WebSocket.__del__() blocks on
            # the TCP close-handshake for each of the MAX_WORKERS threads —
            # serially — adding several seconds after the last security logs.
            for _tv_inst in _tv_instances:
                try:
                    if _tv_inst.ws:
                        _tv_inst.ws.close()
                        _tv_inst.ws = None
                except Exception:
                    pass

        # ── Single batch upsert ───────────────────────────────────────────────
        if all_rows:
            execute_batch(cur, """
                INSERT INTO Historical_Prices
                    (Securities_Id, Date, Close, High, Low, Volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (Securities_Id, Date)
                DO UPDATE SET
                    Close  = EXCLUDED.Close,
                    High   = EXCLUDED.High,
                    Low    = EXCLUDED.Low,
                    Volume = EXCLUDED.Volume
            """, all_rows, page_size=500)
            conn.commit()

        logging.info(
            f"TradingView price download complete — {len(all_rows)} rows upserted "
            f"for {total} securities."
        )
        print(
            f"TradingView price download complete — {len(all_rows)} rows upserted "
            f"for {total} securities."
        )

    except Exception as e:
        conn.rollback()
        logging.error(f"Global error in TradingView download: {e}")
        print(f"Global error: {e}")
        try:
            st.error(f"❌ Error: {e}")
        except Exception:
            pass

    finally:
        cur.close()
        conn.close()

    _refresh_materialized_views_async()


def download_bond_prices_from_solidus():
    pdf_url = "https://www.solidus.gr/AppFol/appDetails/RadControls/fol1/Bonds/SOLIDUS_BOND_LIST.pdf"
    
    response = requests.get(pdf_url)
    if response.status_code != 200:
        print("Failed to receive the file.")
        return

    bond_prices = {}
    pdf_date = None

    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        # 1. Extract Price Date from the 1st page
        first_page_text = pdf.pages[0].extract_text()
        # Search for patern DD/MM/YYYY (π.χ. 23/4/2026)
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', first_page_text)
        if date_match:
            pdf_date = datetime.strptime(date_match.group(1), '%d/%m/%Y').date()
            print(f"PDF Date: {pdf_date}")
        else:
            pdf_date = datetime.now().date()
            print("Date was not found, using today date.")

#    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            # Regex εξήγηση:
            # (GR[A-Z0-0]{10}) -> Το ISIN (ξεκινά με GR και έχει 10 χαρακτήρες)
            # .*? -> Οτιδήποτε ενδιάμεσα (περιγραφή, ημερομηνίες)
            # (\d+,\d{2,4}) -> Το Bid (νούμερο με κόμμα)
            # \s+ -> Κενό
            # (\d+,\d{2,4}) -> Το Ask (νούμερο με κόμμα)
            pattern = r'(GR[A-Z0-9]{10}).*?(\d+,\d{2,4})\s+(\d+,\d{2,4})'
            
            matches = re.findall(pattern, text)
            
            for match in matches:
                isin = match[0]
                try:
                    # Μετατροπή από "98,50" σε float 98.50
                    bid = float(match[1].replace(',', '.'))
                    ask = float(match[2].replace(',', '.'))
                    
                    mid_price = (bid + ask) / 2
                    bond_prices[isin] = mid_price
                    print(f"Found: {isin} | Bid: {bid} | Ask: {ask} | Mid: {mid_price}")
                except ValueError:
                    continue

    if not bond_prices:
        print("No Data found via Text Extraction.")
    else:
        # 3. Update the database
        try:
            conn = get_connection()
            cur = conn.cursor()
            custom_session = get_custom_session() # DO I NEED THIS?
        
            updated_count = 0

            for isin, mid_price in bond_prices.items():
                cur.execute("""
                    SELECT Securities_Id FROM Securities 
                    WHERE (Yahoo_Ticker = %s OR Ticker = %s) AND Is_Active = TRUE
                """, (isin, isin))
                
                res = cur.fetchone()
                if res:
                    s_id = res[0]
                    # Use PDF Date instead of datetime.now()
                    cur.execute("""
                        INSERT INTO Historical_Prices (Securities_Id, Date, Close)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (Securities_Id, Date) DO UPDATE SET Close = EXCLUDED.Close
                    """, (s_id, pdf_date, mid_price))
                    updated_count += 1

            conn.commit()
            print(f"Successful update of {updated_count} bonds for date {pdf_date}.")

        except Exception as e:
            print(f"❌ Error: {e}")
            st.error(f"❌ Error: {e}")
            logging.error(f"Error: {e}")
        finally:
            cur.close()
            conn.close()

