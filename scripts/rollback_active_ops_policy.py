# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ops.policy import rollback_active_ops_policy
from scripts._ops_cli import load_cli_settings, log_and_print, parse_datetime


def main() -> int:
    parser = argparse.ArgumentParser(description="Rollback active ops policy.")
    parser.add_argument("--as-of-at", required=True, type=parse_datetime)
    parser.add_argument("--note")
    args = parser.parse_args()
    settings = load_cli_settings()
    result = rollback_active_ops_policy(
        settings,
        as_of_at=args.as_of_at,
        note=args.note,
    )
    log_and_print(
        "Active ops policy rollback completed. "
        f"run_id={result.run_id} policy={result.policy_id}:{result.policy_version}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
