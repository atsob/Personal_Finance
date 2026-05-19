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
    """Download historical security prices from Yahoo Finance."""
    conn = get_connection()
    cur = conn.cursor()
    custom_session = get_custom_session()
    
    try:

        # Dynamic Query definition
        base_query = """
            SELECT Securities_Id, Securities_Name, Yahoo_Ticker 
            FROM Securities 
            WHERE Yahoo_Ticker IS NOT NULL 
            AND Yahoo_Ticker != '' 
            AND Securities_Name NOT LIKE 'Hellenic T-Bill%'
        """
        params = []

        if not tsperiod:
            tsperiod="1m"
        # If a specific Security ID has been defined, it is added in the filter
        if target_sec_id:
        #    base_query += " AND Securities_Id = %s"
        #    params.append(int(target_sec_id)) 
            base_query += f" AND Securities_Id = {target_sec_id}"


        base_query += " ORDER BY Securities_Name ASC"
        
    #    print(base_query)

        #cur.execute(base_query, params)
        cur.execute(base_query)

        securities = cur.fetchall()

        if not securities:
            logging.warning("No matching securities found with a valid Yahoo Ticker.")
            return
                
        for sec_id, sec_name, symbol in securities:
            logging.info(f"Downloading historical data for {sec_name}...")
            ticker = yf.Ticker(symbol, session=custom_session)
            hist = ticker.history(period=tsperiod)
            
            if hist is None or hist.empty:
                logging.warning(f"No data found for {sec_name} ({symbol})")
                continue
            
            for date, row in hist.iterrows():
                if 'Close' not in row or pd.isna(row['Close']):
                    continue
                
                rate = float(row['Close'])
                high = float(row['High']) if 'High' in row and not pd.isna(row['High']) else None
                low = float(row['Low']) if 'Low' in row and not pd.isna(row['Low']) else None
                volume = float(row['Volume']) if 'Volume' in row and not pd.isna(row['Volume']) else 0
                formatted_date = date.strftime('%Y-%m-%d')
                
                cur.execute("""
                    INSERT INTO Historical_Prices (Securities_Id, Date, Close, High, Low, Volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Securities_Id, Date)
                    DO UPDATE SET Close = EXCLUDED.Close, High = EXCLUDED.High, Low = EXCLUDED.Low, Volume = EXCLUDED.Volume
                """, (sec_id, formatted_date, rate, high, low, volume))
            
            conn.commit()
            logging.info(f"Completed import for {symbol}")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
        logging.error(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

    refresh_materialized_views()


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
    """
    Download and upsert historical daily prices from TradingView into DB.
    Securities must have TV_Symbol and TV_Exchange populated.
    """
    logging.info(f"Starting TradingView download with period={tsperiod} and target_sec_id={target_sec_id}")
    print(f"Starting TradingView download with period={tsperiod} and target_sec_id={target_sec_id}")

    if not tsperiod:
        tsperiod = "1m"
    tsperiod = str(tsperiod).lower().strip()

    n_bars = _period_to_n_bars(tsperiod)

    conn = get_connection()
    cur = conn.cursor()

    try:

        query = """
            SELECT Securities_Id, Securities_Name, TV_Symbol, TV_Exchange
            FROM Securities
            WHERE TV_Symbol IS NOT NULL
              AND TV_Symbol != ''
              AND TV_Exchange IS NOT NULL
              AND TV_Exchange != ''
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

        logging.info(f"Fetching {n_bars} bars for {len(securities)} securities")
        print(f"Fetching {n_bars} bars for {len(securities)} securities")

        tv = TvDatafeed()

        sql = """
            INSERT INTO Historical_Prices
                (Securities_Id, Date, Close, High, Low, Volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (Securities_Id, Date)
            DO UPDATE SET
                Close  = EXCLUDED.Close,
                High   = EXCLUDED.High,
                Low    = EXCLUDED.Low,
                Volume = EXCLUDED.Volume
        """

        for sec_id, sec_name, tv_symbol, tv_exchange in securities:

            try:
                logging.info(f"Downloading {sec_name} ({tv_symbol}:{tv_exchange})")
                print(f"Downloading {sec_name} ({tv_symbol}:{tv_exchange})")

                df = tv.get_hist(
                    symbol=tv_symbol,
                    exchange=tv_exchange,
                    interval=Interval.in_daily,
                    n_bars=n_bars
                )

                if df is None or df.empty:
                    logging.warning(f"No data for {tv_symbol}:{tv_exchange}")
                    print(f"No data for {tv_symbol}:{tv_exchange}")
                    continue

                logging.info(f"{tv_symbol}: {len(df)} rows downloaded")
                print(f"{tv_symbol}: {len(df)} rows downloaded")

                rows_to_insert = []

                for date, row in df.iterrows():
                    rows_to_insert.append((
                        int(sec_id),
                        pd.Timestamp(date).strftime("%Y-%m-%d"),
                        float(row["close"]),
                        None if pd.isna(row["high"]) else float(row["high"]),
                        None if pd.isna(row["low"]) else float(row["low"]),
                        0 if pd.isna(row["volume"]) else int(row["volume"])
                    ))

                if not rows_to_insert:
                    continue

                execute_batch(cur, sql, rows_to_insert, page_size=500)
                conn.commit()

                logging.info(f"Completed {tv_symbol} ({len(rows_to_insert)} rows)")
                print(f"Completed {tv_symbol} ({len(rows_to_insert)} rows)")

            except Exception as sec_error:

                conn.rollback()

                import traceback
                logging.error(f"Failed {tv_symbol}: {sec_error}\n{traceback.format_exc()}")
                print(f"Failed {tv_symbol}: {sec_error}\n{traceback.format_exc()}")

        logging.info("All TradingView imports completed.")
        print("All TradingView imports completed.")

    except Exception as e:

        conn.rollback()
        logging.error(f"Global error: {e}")
        print(f"Global error: {e}")

        try:
            st.error(f"❌ Error: {e}")
        except:
            pass

    finally:
        cur.close()
        conn.close()

    refresh_materialized_views()


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

