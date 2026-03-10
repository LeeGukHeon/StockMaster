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
from app.ml.constants import (
    MCS_ALPHA,
    MCS_BLOCK_LENGTH,
    MCS_BOOTSTRAP_REPS,
    PROMOTION_LOOKBACK_SELECTION_DATES,
)
from app.ml.promotion import run_alpha_auto_promotion
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run alpha auto-promotion.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument(
        "--lookback-selection-dates",
        type=int,
        default=PROMOTION_LOOKBACK_SELECTION_DATES,
    )
    parser.add_argument("--mcs-alpha", type=float, default=MCS_ALPHA)
    parser.add_argument("--bootstrap-reps", type=int, default=MCS_BOOTSTRAP_REPS)
    parser.add_argument("--block-length", type=int, default=MCS_BLOCK_LENGTH)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = run_alpha_auto_promotion(
        settings,
        as_of_date=args.as_of_date,
        horizons=args.horizons,
        lookback_selection_dates=args.lookback_selection_dates,
        mcs_alpha=args.mcs_alpha,
        bootstrap_reps=args.bootstrap_reps,
        block_length=args.block_length,
    )
    logger.info("Alpha auto-promotion completed.", extra={"run_id_value": result.run_id})
    print(
        f"Alpha auto-promotion completed. as_of_date={args.as_of_date.isoformat()} "
        f"run_id={result.run_id} rows={result.row_count} "
        f"promoted_horizons={result.promoted_horizon_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
