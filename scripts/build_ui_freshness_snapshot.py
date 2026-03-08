# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.release.snapshot import build_ui_freshness_snapshot
from scripts._ops_cli import load_cli_settings, log_and_print, run_standalone_job


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the UI freshness snapshot.")
    parser.parse_args()
    settings = load_cli_settings()

    def _runner(connection, job):
        return job.run_step(
            "build_ui_freshness_snapshot",
            build_ui_freshness_snapshot,
            settings,
            connection=connection,
            job_run_id=job.run_id,
        )

    result = run_standalone_job(
        settings,
        job_name="build_ui_freshness_snapshot",
        as_of_date=None,
        dry_run=False,
        policy_config_path=None,
        runner=_runner,
    )
    log_and_print(f"UI freshness snapshot built. run_id={result.run_id} rows={result.row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
