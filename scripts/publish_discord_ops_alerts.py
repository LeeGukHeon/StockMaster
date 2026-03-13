# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
from app.ops.report import publish_discord_ops_alerts
from scripts._ops_cli import load_cli_settings, log_and_print, parse_date, run_standalone_job


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish ops alert summary to Discord.")
    parser.add_argument("--as-of-date", type=parse_date)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    settings = load_cli_settings()
    target_date = args.as_of_date or today_local(settings.app.timezone)

    def _runner(connection, job):
        return job.run_step(
            "publish_discord_ops_alerts",
            publish_discord_ops_alerts,
            settings,
            connection=connection,
            as_of_date=target_date,
            job_run_id=job.run_id,
            dry_run=args.dry_run,
            critical=True,
        )

    result = run_standalone_job(
        settings,
        job_name="publish_discord_ops_alerts",
        as_of_date=target_date,
        dry_run=args.dry_run,
        policy_config_path=None,
        runner=_runner,
    )
    log_and_print(
        "Ops alert publishing completed. "
        f"run_id={result.run_id} artifacts={len(result.artifact_paths)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
