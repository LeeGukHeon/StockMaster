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
from app.portfolio.allocation import materialize_portfolio_position_snapshots
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize portfolio position snapshots.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--execution-modes", nargs="+")
    parser.add_argument("--policy-config-path")
    args = parser.parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    result = materialize_portfolio_position_snapshots(
        settings,
        as_of_date=args.as_of_date,
        execution_modes=args.execution_modes,
        policy_config_path=args.policy_config_path,
    )
    get_logger(__name__).info("Portfolio position snapshots completed.", extra={"run_id_value": result.run_id})
    print(f"Portfolio position snapshots completed. run_id={result.run_id} rows={result.row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
