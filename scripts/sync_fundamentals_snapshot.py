# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.pipelines._helpers import iter_dates
from app.pipelines.fundamentals_snapshot import sync_fundamentals_snapshot
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [token.strip().zfill(6) for token in value.split(",") if token.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize DART fundamentals snapshots into fact_fundamentals_snapshot."
    )
    parser.add_argument("--as-of-date", type=_parse_date)
    parser.add_argument("--start", type=_parse_date)
    parser.add_argument("--end", type=_parse_date)
    parser.add_argument("--symbols")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if bool(args.as_of_date) == bool(args.start or args.end):
        raise SystemExit("Specify either --as-of-date or --start/--end.")
    if (args.start is None) != (args.end is None):
        raise SystemExit("Both --start and --end are required together.")

    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    symbols = _parse_symbols(args.symbols)
    dates = [args.as_of_date] if args.as_of_date else list(iter_dates(args.start, args.end))

    results = []
    for as_of_date in dates:
        result = sync_fundamentals_snapshot(
            settings,
            as_of_date=as_of_date,
            symbols=symbols,
            limit_symbols=args.limit_symbols,
            force=args.force,
            dry_run=args.dry_run,
        )
        logger.info(
            "Fundamentals snapshot completed.",
            extra={
                "run_id_value": result.run_id,
                "as_of_date": as_of_date.isoformat(),
                "row_count": result.row_count,
            },
        )
        results.append(result)
        print(
            f"Fundamentals snapshot completed. as_of_date={as_of_date.isoformat()} "
            f"run_id={result.run_id} rows={result.row_count} "
            f"failed={result.failed_symbol_count}"
        )
    return 0 if results else 1


if __name__ == "__main__":
    raise SystemExit(main())
