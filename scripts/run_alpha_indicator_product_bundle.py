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
from app.ml.indicator_product import run_alpha_indicator_product_bundle
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the alpha indicator-product bundle.")
    parser.add_argument("--train-end-date", required=True, type=_parse_date)
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--shadow-start-selection-date", type=_parse_date)
    parser.add_argument("--shadow-end-selection-date", type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 5])
    parser.add_argument(
        "--model-spec-ids",
        nargs="+",
        default=["alpha_lead_d1_v1", "alpha_swing_d5_v1"],
    )
    parser.add_argument("--min-train-days", type=int, default=120)
    parser.add_argument("--validation-days", type=int, default=20)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    parser.add_argument("--rolling-windows", nargs="+", type=int, default=[20, 60])
    parser.add_argument("--freeze-horizons", nargs="+", type=int, default=[1])
    parser.add_argument("--backfill-shadow-history", action="store_true")
    parser.add_argument(
        "--skip-completed-shadow-dates",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-skip-completed-shadow-dates",
        dest="skip_completed_shadow_dates",
        action="store_false",
    )
    parser.add_argument("--keep-shadow-training-artifacts", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = run_alpha_indicator_product_bundle(
        settings,
        train_end_date=args.train_end_date,
        as_of_date=args.as_of_date,
        shadow_start_selection_date=args.shadow_start_selection_date or args.as_of_date,
        shadow_end_selection_date=args.shadow_end_selection_date or args.as_of_date,
        horizons=list(args.horizons),
        model_spec_ids=list(args.model_spec_ids),
        min_train_days=int(args.min_train_days),
        validation_days=int(args.validation_days),
        limit_symbols=args.limit_symbols,
        market=args.market,
        rolling_windows=list(args.rolling_windows),
        freeze_horizons=list(args.freeze_horizons),
        backfill_shadow_history=bool(args.backfill_shadow_history),
        skip_completed_shadow_dates=bool(args.skip_completed_shadow_dates),
        keep_shadow_training_artifacts=bool(args.keep_shadow_training_artifacts),
    )
    logger.info(
        "Alpha indicator product bundle completed.",
        extra={
            "train_end_date": result.train_end_date.isoformat(),
            "as_of_date": result.as_of_date.isoformat(),
            "training_run_count": result.training_run_count,
            "gap_scorecard_row_count": result.gap_scorecard_row_count,
        },
    )
    print(result.notes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
