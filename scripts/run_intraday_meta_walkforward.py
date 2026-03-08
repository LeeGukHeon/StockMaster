# ruff: noqa: E402
# ruff: noqa: E501

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.meta_training import run_intraday_meta_walkforward
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run intraday meta-model walk-forward.")
    parser.add_argument("--start-session-date", required=True, type=_parse_date)
    parser.add_argument("--end-session-date", required=True, type=_parse_date)
    parser.add_argument("--mode", required=True, choices=["anchored", "rolling"])
    parser.add_argument("--train-sessions", required=True, type=int)
    parser.add_argument("--validation-sessions", required=True, type=int)
    parser.add_argument("--test-sessions", required=True, type=int)
    parser.add_argument("--step-sessions", required=True, type=int)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = run_intraday_meta_walkforward(
        settings,
        start_session_date=args.start_session_date,
        end_session_date=args.end_session_date,
        mode=args.mode,
        train_sessions=args.train_sessions,
        validation_sessions=args.validation_sessions,
        test_sessions=args.test_sessions,
        step_sessions=args.step_sessions,
        horizons=args.horizons,
    )
    logger.info("Intraday meta walk-forward completed.", extra={"run_id_value": result.run_id})
    print(
        f"Intraday meta walk-forward completed. end_session_date={args.end_session_date.isoformat()} "
        f"run_id={result.run_id} splits={result.split_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
