# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.release.snapshot import build_latest_app_snapshot
from scripts._ops_cli import load_cli_settings, log_and_print, parse_date, run_standalone_job


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the latest app snapshot.")
    parser.add_argument("--as-of-date", type=parse_date)
    args = parser.parse_args()
    settings = load_cli_settings()

    def _runner(connection, job):
        return job.run_step(
            "build_latest_app_snapshot",
            build_latest_app_snapshot,
            settings,
            connection=connection,
            as_of_date=args.as_of_date,
            job_run_id=job.run_id,
        )

    result = run_standalone_job(
        settings,
        job_name="build_latest_app_snapshot",
        as_of_date=args.as_of_date,
        dry_run=False,
        policy_config_path=None,
        runner=_runner,
    )
    log_and_print(f"Latest app snapshot built. run_id={result.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
