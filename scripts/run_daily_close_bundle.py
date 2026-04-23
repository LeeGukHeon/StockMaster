# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ops.bundles import run_daily_close_bundle
from scripts._ops_cli import load_cli_settings, log_and_print, parse_date


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the daily close scheduler bundle.")
    parser.add_argument("--as-of-date", type=parse_date)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-discord", action="store_true")
    parser.add_argument(
        "--skip-active-d5-swing",
        action="store_true",
        help="Do not freeze H5 active alpha to alpha_swing_d5_v2 during daily close.",
    )
    parser.add_argument("--policy-config-path")
    args = parser.parse_args()
    settings = load_cli_settings()
    result = run_daily_close_bundle(
        settings,
        as_of_date=args.as_of_date,
        dry_run=args.dry_run,
        force=args.force,
        publish_discord=not args.skip_discord,
        active_d5_swing=not args.skip_active_d5_swing,
        policy_config_path=args.policy_config_path,
    )
    log_and_print(f"Daily close bundle completed. run_id={result.run_id} status={result.status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
