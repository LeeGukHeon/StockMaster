# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.ml.indicator_product import inspect_alpha_indicator_product_readiness
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect alpha indicator-product readiness.")
    parser.add_argument("--train-end-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 5])
    parser.add_argument(
        "--model-spec-ids",
        nargs="+",
        default=["alpha_swing_d5_v2", "alpha_swing_d5_v1"],
    )
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = inspect_alpha_indicator_product_readiness(
        settings,
        train_end_date=args.train_end_date,
        horizons=list(args.horizons),
        model_spec_ids=list(args.model_spec_ids),
        limit_symbols=args.limit_symbols,
        market=args.market,
    )
    payload = {
        "train_end_date": result.train_end_date.isoformat(),
        "latest_market_date": (
            None if result.latest_market_date is None else result.latest_market_date.isoformat()
        ),
        "missing_snapshot_dates": [value.isoformat() for value in result.missing_snapshot_dates],
        "label_max_as_of_by_horizon": {
            str(key): (None if value is None else value.isoformat())
            for key, value in result.label_max_as_of_by_horizon.items()
        },
        "available_label_rows_by_horizon": result.available_label_rows_by_horizon,
        "specs": [
            {
                "model_spec_id": row.model_spec_id,
                "supported_horizons": row.supported_horizons,
                "runnable_horizons": row.runnable_horizons,
                "blocked_horizons": row.blocked_horizons,
                "blockers": row.blockers,
            }
            for row in result.specs
        ],
        "notes": result.notes,
    }
    logger.info(
        "Alpha indicator readiness inspected.",
        extra={"train_end_date": args.train_end_date.isoformat()},
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
