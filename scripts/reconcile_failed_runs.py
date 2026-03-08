# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
from app.ops.maintenance import reconcile_failed_runs
from scripts._ops_cli import load_cli_settings, log_and_print, parse_date, run_standalone_job


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile failed ops runs into recovery queue.")
    parser.add_argument("--as-of-date", type=parse_date)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    settings = load_cli_settings()
    target_date = args.as_of_date or today_local(settings.app.timezone)

    def _runner(connection, job):
        return job.run_step(
            "reconcile_failed_runs",
            reconcile_failed_runs,
            settings,
            connection=connection,
            job_run_id=job.run_id,
            limit=args.limit,
        )

    result = run_standalone_job(
        settings,
        job_name="reconcile_failed_runs",
        as_of_date=target_date,
        dry_run=args.dry_run,
        policy_config_path=None,
        runner=_runner,
    )
    log_and_print(f"Failed runs reconciled. run_id={result.run_id} queued={result.row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
