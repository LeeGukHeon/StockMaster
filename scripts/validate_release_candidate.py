# ruff: noqa: E402, E501

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.release.validation import validate_release_candidate
from scripts._ops_cli import load_cli_settings, log_and_print, parse_date, run_standalone_job


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the T013 release candidate checklist.")
    parser.add_argument("--as-of-date", type=parse_date)
    args = parser.parse_args()
    settings = load_cli_settings()

    def _runner(connection, job):
        return job.run_step(
            "validate_release_candidate",
            validate_release_candidate,
            settings,
            connection=connection,
            as_of_date=args.as_of_date,
            critical=False,
        )

    result = run_standalone_job(
        settings,
        job_name="validate_release_candidate",
        as_of_date=args.as_of_date,
        dry_run=False,
        policy_config_path=None,
        runner=_runner,
    )
    log_and_print(
        f"Release candidate validated. run_id={result.run_id} checks={result.check_count} warnings={result.warning_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
