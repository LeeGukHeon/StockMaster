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
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument(
        "--persist-raw",
        action="store_true",
        help="Persist per-symbol raw investor flow payloads during backfill.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)

    total_rows = 0
    total_failures = 0
    skipped_dates: list[str] = []
    for trading_date in iter_dates(args.start, args.end):
        try:
            result = sync_investor_flow(
                settings,
                trading_date=trading_date,
                symbols=_parse_symbols(args.symbols),
                limit_symbols=args.limit_symbols,
                market=args.market,
                force=args.force,
                max_workers=args.max_workers,
                persist_raw_artifacts=args.persist_raw,
            )
            total_rows += result.row_count
            total_failures += result.failed_symbol_count
        except RuntimeError as exc:
            # Some historical dates return HTTP 200 but no usable rows for any symbol.
            # Treat those dates as backfill skips instead of aborting the full range.
            if "No investor flow rows were loaded" not in str(exc):
                raise
            skipped_dates.append(trading_date.isoformat())
            logger.warning(
                "Investor flow backfill skipped empty date.",
                extra={
                    "trading_date": trading_date.isoformat(),
                    "reason": str(exc),
                },
            )

    logger.info(
        "Investor flow backfill completed.",
        extra={
            "start_date": args.start.isoformat(),
            "end_date": args.end.isoformat(),
            "row_count": total_rows,
            "failed_symbol_count": total_failures,
            "skipped_dates": skipped_dates,
        },
    )
    print(
        f"Investor flow backfill completed. range={args.start.isoformat()}..{args.end.isoformat()} "
        f"rows={total_rows} failed={total_failures} skipped_dates={len(skipped_dates)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
