# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.audit.checks import run_contract_checks
from scripts._audit_cli import load_cli_settings, log_and_print, suite_to_job_result
from scripts._ops_cli import run_standalone_job


def main() -> int:
    settings = load_cli_settings()

    def _runner(connection, job):
        suite = run_contract_checks(settings, connection=connection)
        log_and_print(
            f"DB contracts validated. PASS={suite.pass_count} WARN={suite.warn_count} FAIL={suite.fail_count}",
        )
        return suite_to_job_result(
            suite,
            job_name="validate_db_contracts",
            run_id=job.run_id,
            notes_prefix="DB contract validation completed.",
        )

    result = run_standalone_job(
        settings,
        job_name="validate_db_contracts",
        as_of_date=None,
        dry_run=False,
        policy_config_path=None,
        runner=_runner,
    )
    return 0 if result.status != "FAILED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
