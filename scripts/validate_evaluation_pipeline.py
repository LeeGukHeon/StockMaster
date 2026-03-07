# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.evaluation.validation import validate_evaluation_pipeline
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate the evaluation pipeline outputs.")
    parser.add_argument("--start-selection-date", required=True, type=_parse_date)
    parser.add_argument("--end-selection-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)

    result = validate_evaluation_pipeline(
        settings,
        start_selection_date=args.start_selection_date,
        end_selection_date=args.end_selection_date,
        horizons=args.horizons,
    )
    logger.info(
        "Evaluation pipeline validation completed.",
        extra={"run_id_value": result.run_id, "check_count": result.row_count},
    )
    print(
        f"Evaluation pipeline validation completed. run_id={result.run_id} "
        f"checks={result.row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
