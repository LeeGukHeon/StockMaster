# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.strategy import materialize_intraday_decision_outcomes
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize intraday decision outcomes.")
    parser.add_argument("--start-session-date", required=True, type=_parse_date)
    parser.add_argument("--end-session-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--cutoff", default="11:00")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = materialize_intraday_decision_outcomes(
        settings,
        start_session_date=args.start_session_date,
        end_session_date=args.end_session_date,
        horizons=args.horizons,
        cutoff=args.cutoff,
    )
    logger.info("Intraday decision outcomes completed.", extra={"run_id_value": result.run_id})
    print(
        f"Intraday decision outcomes completed. range={args.start_session_date.isoformat()}.."
        f"{args.end_session_date.isoformat()} run_id={result.run_id} "
        f"rows={result.row_count} matured={result.matured_row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
