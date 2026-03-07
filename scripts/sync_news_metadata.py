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
from app.pipelines.news_metadata import sync_news_metadata
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [token.strip().zfill(6) for token in value.split(",") if token.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Naver news metadata into fact_news_item.")
    parser.add_argument("--date", type=_parse_date)
    parser.add_argument("--start", type=_parse_date)
    parser.add_argument("--end", type=_parse_date)
    parser.add_argument("--symbols")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument(
        "--mode",
        required=True,
        choices=["market_only", "market_and_focus", "symbol_list"],
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--query-pack", default="default")
    parser.add_argument("--max-items-per-query", type=int, default=50)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if bool(args.date) == bool(args.start or args.end):
        raise SystemExit("Specify either --date or --start/--end.")
    if (args.start is None) != (args.end is None):
        raise SystemExit("Both --start and --end are required together.")

    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    symbols = _parse_symbols(args.symbols)
    dates = [args.date] if args.date else list(iter_dates(args.start, args.end))

    results = []
    for signal_date in dates:
        result = sync_news_metadata(
            settings,
            signal_date=signal_date,
            mode=args.mode,
            symbols=symbols,
            limit_symbols=args.limit_symbols,
            force=args.force,
            dry_run=args.dry_run,
            query_pack=args.query_pack,
            max_items_per_query=args.max_items_per_query,
        )
        logger.info(
            "News metadata sync completed.",
            extra={
                "run_id_value": result.run_id,
                "signal_date": signal_date.isoformat(),
                "row_count": result.deduped_row_count,
            },
        )
        results.append(result)
        print(
            f"News metadata sync completed. signal_date={signal_date.isoformat()} "
            f"run_id={result.run_id} rows={result.deduped_row_count} "
            f"queries={result.query_count}"
        )
    return 0 if results else 1


if __name__ == "__main__":
    raise SystemExit(main())
