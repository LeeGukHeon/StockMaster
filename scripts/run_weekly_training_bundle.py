# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ops.bundles import run_weekly_training_bundle
from scripts._ops_cli import load_cli_settings, log_and_print, parse_date


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the weekly training candidate bundle.")
    parser.add_argument("--as-of-date", type=parse_date)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--policy-config-path")
    args = parser.parse_args()
    settings = load_cli_settings()
    result = run_weekly_training_bundle(
        settings,
        as_of_date=args.as_of_date,
        dry_run=args.dry_run,
        force=args.force,
        policy_config_path=args.policy_config_path,
    )
    log_and_print(
        f"Weekly training candidate bundle completed. run_id={result.run_id} status={result.status}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
