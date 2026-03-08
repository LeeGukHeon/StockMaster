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
from app.portfolio.policies import freeze_active_portfolio_policy
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze active portfolio policy.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--policy-config-path", required=True)
    parser.add_argument("--promotion-type", default="MANUAL_FREEZE")
    parser.add_argument("--note")
    args = parser.parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    result = freeze_active_portfolio_policy(
        settings,
        as_of_date=args.as_of_date,
        policy_config_path=args.policy_config_path,
        promotion_type=args.promotion_type,
        note=args.note,
    )
    get_logger(__name__).info("Portfolio policy freeze completed.", extra={"run_id_value": result.run_id})
    print(f"Portfolio policy freeze completed. run_id={result.run_id} rows={result.row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
