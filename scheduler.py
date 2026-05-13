"""
Background scheduler — runs as a separate Docker service (see docker-compose.yml).

Jobs
────
• Weekly summary    : every Monday at 07:00 local time.
                      Also fires once at startup if this week has no summary yet.
• Market data       : every MARKET_REFRESH_INTERVAL_MINUTES during business days
                      (Mon–Fri) between MARKET_HOURS_START and MARKET_HOURS_END.
                      Downloads the latest prices (1-day period) for all securities
                      and the latest FX rates.
"""

import logging
import time
import warnings
from datetime import date, datetime, timedelta

warnings.filterwarnings("ignore", message="No runtime found", module="streamlit")
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

from ai.weekly_summary import run as run_weekly_summary
from ai.monthly_summary import run as run_monthly_summary
from data.downloaders import (
    download_historical_prices_from_yahoo,
    download_historical_prices_from_tradingview,
    download_bond_prices_from_solidus,
    download_historical_fx,
    download_securities_info_from_yahoo,
)
from database.connection import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [scheduler] %(message)s",
)

# ── Market-data refresh config ────────────────────────────────────────────────
MARKET_REFRESH_INTERVAL_MINUTES = 5   # how often to refresh prices & FX
MARKET_HOURS_START = 7                 # inclusive  (local time, 24-h), earlier for FX markets and Cryptos considering global nature and after-hours trading
MARKET_HOURS_END   = 23                # exclusive, considering late trading and after-hours for US markets

# Tick interval — the scheduler wakes up this often to check all jobs.
# Keep it at 60 s so Monday-07:00 is never missed by more than a minute.
TICK_SECONDS = 60


# ── Helpers ───────────────────────────────────────────────────────────────────

def _current_week_start() -> date:
    today = date.today()
    return today - timedelta(days=today.weekday())


def _summary_exists_for_current_week() -> bool:
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM AI_Weekly_Summaries WHERE Week_Start = %s",
                (_current_week_start(),),
            )
            exists = cur.fetchone() is not None
        conn.close()
        return exists
    except Exception:
        return False


def _is_market_open(now: datetime) -> bool:
    """True on Mon–Fri between MARKET_HOURS_START and MARKET_HOURS_END."""
    return now.weekday() < 5 and MARKET_HOURS_START <= now.hour < MARKET_HOURS_END


# ── Jobs ──────────────────────────────────────────────────────────────────────

def _weekly_summary_job():
    logging.info("Running weekly summary job…")
    try:
        run_weekly_summary()
        logging.info("Weekly summary completed.")
    except Exception as e:
        logging.error(f"Weekly summary failed: {e}", exc_info=True)


def _market_data_job():
    logging.info("Running market data refresh…")
    try:
        download_historical_prices_from_yahoo(tsperiod="1d")
        logging.info("Security prices refreshed.")
    except Exception as e:
        logging.error(f"Price refresh failed: {e}", exc_info=True)

    try:
        download_historical_prices_from_tradingview(tsperiod="1d")
        logging.info("TradingView prices refreshed.")
    except Exception as e:
        logging.error(f"TradingView price refresh failed: {e}", exc_info=True)

    try:
        download_bond_prices_from_solidus()
        logging.info("Bond prices refreshed.")
    except Exception as e:
        logging.error(f"Bond price refresh failed: {e}", exc_info=True)

    try:
        download_historical_fx(tsperiod="3d")   # 3 d to catch weekend gaps on Monday
        logging.info("FX rates refreshed.")
    except Exception as e:
        logging.error(f"FX refresh failed: {e}", exc_info=True)


def _securities_info_job():
    """Download securities metadata (name, sector, description, …) once per day."""
    logging.info("Running securities info refresh…")
    try:
        download_securities_info_from_yahoo()
        logging.info("Securities info refreshed.")
    except Exception as e:
        logging.error(f"Securities info refresh failed: {e}", exc_info=True)


# ── Main loop ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.info("Scheduler starting.")

    # Run weekly summary immediately if this week's entry is missing
    if not _summary_exists_for_current_week():
        logging.info("No summary for current week — running now.")
        _weekly_summary_job()
    else:
        logging.info("Current week's summary already exists — skipping startup run.")

    # Run market data once at startup if the market is currently open
    _last_market_refresh: datetime = datetime.min
    now = datetime.now()
    if _is_market_open(now):
        logging.info("Market is open — running initial market data refresh.")
        _market_data_job()
        _last_market_refresh = now
    else:
        logging.info(
            f"Market closed at startup (weekday={now.weekday()}, hour={now.hour}) — "
            "skipping initial market data refresh."
        )

    _last_weekly_summary_date: date = date.today() if datetime.now().weekday() == 0 and datetime.now().hour >= 7 else date.min

    # Securities info: run once at startup every day
    _last_securities_info_date: date = date.min
    logging.info("Running initial securities info refresh.")
    _securities_info_job()
    _last_securities_info_date = date.today()

    # Tick loop — wakes every TICK_SECONDS and evaluates each job
    while True:
        time.sleep(TICK_SECONDS)
        now = datetime.now()

        # ── Market data: every N minutes during market hours ──────────────────
        minutes_since_refresh = (now - _last_market_refresh).total_seconds() / 60
        if _is_market_open(now) and minutes_since_refresh >= MARKET_REFRESH_INTERVAL_MINUTES:
            _market_data_job()
            _last_market_refresh = now

        # ── Securities info: once per calendar day (including weekends) ─────────
        if _last_securities_info_date != date.today():
            _securities_info_job()
            _last_securities_info_date = date.today()

        # ── Weekly summary: Monday at 07:00 (fire once per Monday) ───────────
        if (
            now.weekday() == 0          # Monday
            and now.hour == 7
            and now.minute < 5          # within the first 5 minutes of 07:00
            and _last_weekly_summary_date != date.today()
        ):
            _weekly_summary_job()
            _last_weekly_summary_date = date.today()
