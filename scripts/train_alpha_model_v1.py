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
from app.ml.training import train_alpha_model_v1
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train alpha model v1.")
    parser.add_argument("--train-end-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--min-train-days", type=int, default=120)
    parser.add_argument("--validation-days", type=int, default=20)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = train_alpha_model_v1(
        settings,
        train_end_date=args.train_end_date,
        horizons=args.horizons,
        min_train_days=args.min_train_days,
        validation_days=args.validation_days,
        limit_symbols=args.limit_symbols,
        market=args.market,
    )
    logger.info(
        "Alpha model v1 training completed.",
        extra={"run_id_value": result.run_id, "training_run_count": result.training_run_count},
    )
    print(
        f"Alpha model v1 training completed. train_end_date={args.train_end_date.isoformat()} "
        f"run_id={result.run_id} training_runs={result.training_run_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
