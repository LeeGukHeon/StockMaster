# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.evaluation.outcomes import materialize_selection_outcomes
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill frozen selection outcomes over a date range."
    )
    parser.add_argument("--start-selection-date", required=True, type=_parse_date)
    parser.add_argument("--end-selection-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--limit-symbols", type=int)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)

    result = materialize_selection_outcomes(
        settings,
        start_selection_date=args.start_selection_date,
        end_selection_date=args.end_selection_date,
        horizons=args.horizons,
        limit_symbols=args.limit_symbols,
    )
    logger.info(
        "Selection outcome backfill completed.",
        extra={
            "run_id_value": result.run_id,
            "row_count": result.row_count,
            "matured_row_count": result.matured_row_count,
        },
    )
    print(
        f"Selection outcome backfill completed. run_id={result.run_id} rows={result.row_count} "
        f"matured={result.matured_row_count} pending={result.pending_row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
