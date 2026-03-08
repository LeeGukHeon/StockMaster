# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.policy_report import publish_discord_intraday_policy_summary
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish intraday policy summary.")
    parser.add_argument("--as-of-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = publish_discord_intraday_policy_summary(
        settings,
        as_of_date=args.as_of_date,
        horizons=args.horizons,
        dry_run=args.dry_run,
    )
    logger.info("Intraday policy summary publish completed.", extra={"run_id_value": result.run_id})
    print(
        "Intraday policy summary publish completed. "
        f"run_id={result.run_id} published={result.published}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
