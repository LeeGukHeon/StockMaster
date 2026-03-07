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
from app.selection.calibration import calibrate_proxy_prediction_bands
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calibrate proxy prediction bands.")
    parser.add_argument("--start", required=True, type=_parse_date)
    parser.add_argument("--end", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)

    result = calibrate_proxy_prediction_bands(
        settings,
        start_date=args.start,
        end_date=args.end,
        horizons=args.horizons,
    )
    logger.info(
        "Proxy prediction calibration completed.",
        extra={
            "run_id_value": result.run_id,
            "as_of_date": result.as_of_date.isoformat(),
            "prediction_row_count": result.row_count,
            "calibration_row_count": result.calibration_row_count,
        },
    )
    print(
        f"Proxy prediction calibration completed. selection_date={result.as_of_date.isoformat()} "
        f"run_id={result.run_id} predictions={result.row_count} "
        f"calibration_rows={result.calibration_row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
