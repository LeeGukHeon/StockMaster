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
from app.ml.training import backfill_alpha_oof_predictions
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill alpha OOF predictions.")
    parser.add_argument("--start-train-end-date", required=True, type=_parse_date)
    parser.add_argument("--end-train-end-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--min-train-days", type=int, default=120)
    parser.add_argument("--validation-days", type=int, default=20)
    parser.add_argument("--limit-models", type=int)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = backfill_alpha_oof_predictions(
        settings,
        start_train_end_date=args.start_train_end_date,
        end_train_end_date=args.end_train_end_date,
        horizons=args.horizons,
        min_train_days=args.min_train_days,
        validation_days=args.validation_days,
        limit_models=args.limit_models,
        limit_symbols=args.limit_symbols,
        market=args.market,
    )
    logger.info(
        "Alpha OOF backfill completed.",
        extra={"run_id_value": result.run_id, "run_count": result.run_count},
    )
    print(
        f"Alpha OOF backfill completed. range={args.start_train_end_date.isoformat()}.."
        f"{args.end_train_end_date.isoformat()} run_id={result.run_id} runs={result.run_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
