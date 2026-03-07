# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.features.feature_store import build_feature_store
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [token.strip().zfill(6) for token in value.split(",") if token.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build fact_feature_snapshot and feature matrix.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
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

    result = build_feature_store(
        settings,
        as_of_date=args.as_of_date,
        symbols=_parse_symbols(args.symbols),
        limit_symbols=args.limit_symbols,
        market=args.market,
        force=args.force,
        dry_run=args.dry_run,
    )
    logger.info(
        "Feature store build completed.",
        extra={
            "run_id_value": result.run_id,
            "as_of_date": args.as_of_date.isoformat(),
            "feature_row_count": result.feature_row_count,
            "feature_version": result.feature_version,
        },
    )
    print(
        f"Feature store completed. as_of_date={args.as_of_date.isoformat()} "
        f"run_id={result.run_id} symbols={result.symbol_count} "
        f"feature_rows={result.feature_row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
