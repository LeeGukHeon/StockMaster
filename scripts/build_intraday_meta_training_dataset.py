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

from app.intraday.meta_dataset import build_intraday_meta_training_dataset
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build intraday meta-model training dataset.")
    parser.add_argument("--start-session-date", required=True, type=_parse_date)
    parser.add_argument("--end-session-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = build_intraday_meta_training_dataset(
        settings,
        start_session_date=args.start_session_date,
        end_session_date=args.end_session_date,
        horizons=args.horizons,
    )
    logger.info("Intraday meta dataset build completed.", extra={"run_id_value": result.run_id})
    print(
        f"Intraday meta dataset build completed. end_session_date={args.end_session_date.isoformat()} "
        f"run_id={result.run_id} rows={result.row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
