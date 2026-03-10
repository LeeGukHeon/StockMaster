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
from app.ml.active import freeze_alpha_active_model
from app.ml.constants import MODEL_SPEC_ID
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze active alpha model.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--source", default="latest_training")
    parser.add_argument("--note")
    parser.add_argument("--horizons", nargs="+", type=int)
    parser.add_argument("--model-spec-id", default=MODEL_SPEC_ID)
    parser.add_argument("--train-end-date", type=_parse_date)
    parser.add_argument("--promotion-type", default="MANUAL_FREEZE")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = freeze_alpha_active_model(
        settings,
        as_of_date=args.as_of_date,
        source=args.source,
        note=args.note,
        horizons=args.horizons,
        model_spec_id=args.model_spec_id,
        train_end_date=args.train_end_date,
        promotion_type=args.promotion_type,
    )
    logger.info("Alpha active model freeze completed.", extra={"run_id_value": result.run_id})
    print(
        f"Alpha active model freeze completed. as_of_date={args.as_of_date.isoformat()} "
        f"run_id={result.run_id} rows={result.row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
