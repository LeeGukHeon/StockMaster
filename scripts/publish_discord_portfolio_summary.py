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
from app.portfolio.report import publish_discord_portfolio_summary
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish Discord portfolio summary.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    result = publish_discord_portfolio_summary(settings, as_of_date=args.as_of_date, dry_run=args.dry_run)
    get_logger(__name__).info("Portfolio summary publish completed.", extra={"run_id_value": result.run_id})
    print(f"Portfolio summary publish completed. published={result.published} dry_run={result.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
