# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.release.snapshot import build_report_index
from scripts._ops_cli import load_cli_settings, log_and_print, run_standalone_job


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the latest report index.")
    parser.parse_args()
    settings = load_cli_settings()

    def _runner(connection, job):
        return job.run_step(
            "build_report_index",
            build_report_index,
            settings,
            connection=connection,
            job_run_id=job.run_id,
        )

    result = run_standalone_job(
        settings,
        job_name="build_report_index",
        as_of_date=None,
        dry_run=False,
        policy_config_path=None,
        runner=_runner,
    )
    log_and_print(f"Report index built. run_id={result.run_id} rows={result.row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
