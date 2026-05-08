"""
Background scheduler — runs as a separate Docker service (see docker-compose.yml).
Fires ai.weekly_summary every Monday at 07:00 local time.
Also runs once at startup if the current week has no summary yet.
"""

import logging
import time
import warnings
from datetime import date, datetime, timedelta

warnings.filterwarnings("ignore", message="No runtime found", module="streamlit")
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")


from ai.weekly_summary import run as run_weekly_summary
from ai.monthly_summary import run as run_monthly_summary
from database.connection import get_connection


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [scheduler] %(message)s",
)


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


def _next_monday_at_07() -> datetime:
    """Return the next Monday 07:00 that is strictly in the future."""
    now = datetime.now()
    days_ahead = (7 - now.weekday()) % 7  # 0 means today is Monday
    if days_ahead == 0 and now.hour >= 7:
        days_ahead = 7  # already past 07:00 on Monday — wait for next week
    target = (now + timedelta(days=days_ahead)).replace(
        hour=7, minute=0, second=0, microsecond=0
    )
    return target


def _job():
    logging.info("Running weekly summary job...")
    try:
        run_weekly_summary()
        logging.info("Weekly summary completed.")
    except Exception as e:
        logging.error(f"Weekly summary failed: {e}", exc_info=True)


if __name__ == "__main__":
    logging.info("Scheduler starting.")

    # Run immediately on startup if this week's summary is missing
    if not _summary_exists_for_current_week():
        logging.info("No summary for current week — running now.")
        _job()
    else:
        logging.info("Current week's summary already exists — skipping startup run.")

    # Loop: sleep until next Monday 07:00, run, repeat
    while True:
        next_run = _next_monday_at_07()
        sleep_seconds = max((next_run - datetime.now()).total_seconds(), 0)
        logging.info(
            f"Next run scheduled for {next_run.strftime('%Y-%m-%d %H:%M')} "
            f"(in {sleep_seconds / 3600:.1f} h)"
        )
        time.sleep(sleep_seconds)
        _job()
