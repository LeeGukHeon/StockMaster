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

from app.intraday.meta_training import train_intraday_meta_models
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train intraday meta-models.")
    parser.add_argument("--train-end-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--start-session-date", type=_parse_date)
    parser.add_argument("--validation-sessions", type=int, default=10)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = train_intraday_meta_models(
        settings,
        train_end_date=args.train_end_date,
        horizons=args.horizons,
        start_session_date=args.start_session_date,
        validation_sessions=args.validation_sessions,
    )
    logger.info("Intraday meta-model training completed.", extra={"run_id_value": result.run_id})
    print(
        f"Intraday meta-model training completed. train_end_date={args.train_end_date.isoformat()} "
        f"run_id={result.run_id} training_runs={result.training_run_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
