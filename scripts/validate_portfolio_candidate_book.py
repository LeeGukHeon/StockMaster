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

from app.logging import configure_logging, get_logger
from app.portfolio.candidate_book import validate_portfolio_candidate_book
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate portfolio candidate book.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--execution-modes", nargs="+")
    args = parser.parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    result = validate_portfolio_candidate_book(
        settings,
        as_of_date=args.as_of_date,
        execution_modes=args.execution_modes,
    )
    get_logger(__name__).info("Portfolio candidate validation completed.", extra={"run_id_value": result.run_id})
    print(f"Portfolio candidate validation completed. run_id={result.run_id} checks={result.check_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
