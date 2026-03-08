# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.data import backfill_intraday_candidate_bars
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill candidate-only intraday 1m bars.")
    parser.add_argument("--session-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = backfill_intraday_candidate_bars(
        settings,
        session_date=args.session_date,
        horizons=args.horizons,
        ranking_version="selection_engine_v2",
        dry_run=args.dry_run,
    )
    logger.info("Intraday bar backfill completed.", extra={"run_id_value": result.run_id})
    print(
        f"Intraday bar backfill completed. session_date={args.session_date.isoformat()} "
        f"run_id={result.run_id} rows={result.row_count} "
        f"missing_symbols={result.missing_symbol_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
