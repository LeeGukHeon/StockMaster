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
from app.pipelines.investor_flow import sync_investor_flow
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [token.strip().zfill(6) for token in value.split(",") if token.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill investor flow for a date range.")
    parser.add_argument("--start", required=True, type=_parse_date)
    parser.add_argument("--end", required=True, type=_parse_date)
    parser.add_argument("--symbols")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    parser.add_argument("--force", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)

    total_rows = 0
    total_failures = 0
    for trading_date in iter_dates(args.start, args.end):
        result = sync_investor_flow(
            settings,
            trading_date=trading_date,
            symbols=_parse_symbols(args.symbols),
            limit_symbols=args.limit_symbols,
            market=args.market,
            force=args.force,
        )
        total_rows += result.row_count
        total_failures += result.failed_symbol_count

    logger.info(
        "Investor flow backfill completed.",
        extra={
            "start_date": args.start.isoformat(),
            "end_date": args.end.isoformat(),
            "row_count": total_rows,
            "failed_symbol_count": total_failures,
        },
    )
    print(
        f"Investor flow backfill completed. range={args.start.isoformat()}..{args.end.isoformat()} "
        f"rows={total_rows} failed={total_failures}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
