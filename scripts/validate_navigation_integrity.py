# ruff: noqa: E402, E501

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.release.validation import validate_navigation_integrity
from scripts._ops_cli import load_cli_settings, log_and_print, run_standalone_job


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Streamlit navigation integrity.")
    parser.parse_args()
    settings = load_cli_settings()

    def _runner(connection, job):
        return job.run_step(
            "validate_navigation_integrity",
            validate_navigation_integrity,
            settings,
            connection=connection,
            critical=False,
        )

    try:
        result = run_standalone_job(
            settings,
            job_name="validate_navigation_integrity",
            as_of_date=None,
            dry_run=False,
            policy_config_path=None,
            runner=_runner,
        )
        log_and_print(
            f"Navigation integrity validated. run_id={result.run_id} checks={result.check_count} warnings={result.warning_count}"
        )
    except (duckdb.ConnectionException, duckdb.IOException) as exc:
        message = str(exc).lower()
        if (
            "conflicting lock" not in message
            and "file is being used by another process" not in message
            and "다른 프로세스" not in message
        ):
            raise
        result = validate_navigation_integrity(
            settings,
            connection=None,
            persist_results=False,
        )
        log_and_print(
            "Navigation integrity validated in read-only fallback mode due to an active DuckDB writer lock. "
            f"checks={result.check_count} warnings={result.warning_count}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
