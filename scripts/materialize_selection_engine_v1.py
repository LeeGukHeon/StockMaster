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
from app.selection.engine_v1 import materialize_selection_engine_v1
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [token.strip().zfill(6) for token in value.split(",") if token.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize selection engine v1 rows.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--symbols")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)

    result = materialize_selection_engine_v1(
        settings,
        as_of_date=args.as_of_date,
        horizons=args.horizons,
        symbols=_parse_symbols(args.symbols),
        limit_symbols=args.limit_symbols,
        market=args.market,
        force=args.force,
        dry_run=args.dry_run,
    )
    logger.info(
        "Selection engine v1 completed.",
        extra={
            "run_id_value": result.run_id,
            "as_of_date": args.as_of_date.isoformat(),
            "row_count": result.row_count,
        },
    )
    print(
        f"Selection engine v1 completed. as_of_date={args.as_of_date.isoformat()} "
        f"run_id={result.run_id} rows={result.row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
