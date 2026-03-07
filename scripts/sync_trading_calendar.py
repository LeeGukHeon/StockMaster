# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ingestion.calendar_sync import sync_trading_calendar
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync the KR trading calendar into DuckDB.")
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = sync_trading_calendar(
        settings,
        start_date=start_date,
        end_date=end_date,
    )
    logger.info(
        "Trading calendar sync completed.",
        extra={
            "run_id_value": result.run_id,
            "row_count": result.row_count,
            "trading_day_count": result.trading_day_count,
        },
    )
    print(
        "Trading calendar sync completed. "
        f"run_id={result.run_id} rows={result.row_count} "
        f"trading_days={result.trading_day_count} "
        f"range={result.min_date}..{result.max_date}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
