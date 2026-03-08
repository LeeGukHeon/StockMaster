# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.policy import run_intraday_policy_calibration
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run intraday policy calibration.")
    parser.add_argument("--start-session-date", required=True, type=_parse_date)
    parser.add_argument("--end-session-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--checkpoints", nargs="+", required=True)
    parser.add_argument("--objective-version", required=True)
    parser.add_argument("--split-version", required=True)
    parser.add_argument("--search-space-version", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = run_intraday_policy_calibration(
        settings,
        start_session_date=args.start_session_date,
        end_session_date=args.end_session_date,
        horizons=args.horizons,
        checkpoints=args.checkpoints,
        objective_version=args.objective_version,
        split_version=args.split_version,
        search_space_version=args.search_space_version,
    )
    logger.info("Intraday policy calibration completed.", extra={"run_id_value": result.run_id})
    print(
        "Intraday policy calibration completed. "
        f"run_id={result.run_id} experiments={result.experiment_row_count} "
        f"evaluations={result.evaluation_row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
