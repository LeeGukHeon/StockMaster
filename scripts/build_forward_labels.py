# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.labels.forward_returns import build_forward_labels
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [token.strip().zfill(6) for token in value.split(",") if token.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build next-open forward return labels.")
    parser.add_argument("--start", required=True, type=_parse_date)
    parser.add_argument("--end", required=True, type=_parse_date)
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

    result = build_forward_labels(
        settings,
        start_date=args.start,
        end_date=args.end,
        horizons=args.horizons,
        symbols=_parse_symbols(args.symbols),
        limit_symbols=args.limit_symbols,
        market=args.market,
        force=args.force,
        dry_run=args.dry_run,
    )
    logger.info(
        "Forward label build completed.",
        extra={
            "run_id_value": result.run_id,
            "start_date": args.start.isoformat(),
            "end_date": args.end.isoformat(),
            "row_count": result.row_count,
        },
    )
    print(
        f"Forward labels completed. start={args.start.isoformat()} end={args.end.isoformat()} "
        f"run_id={result.run_id} rows={result.row_count} available={result.available_row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
