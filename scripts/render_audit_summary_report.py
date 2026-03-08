# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.audit.checks import run_full_audit
from app.audit.reporting import write_audit_artifacts, write_audit_docs
from scripts._audit_cli import generated_at, load_cli_settings, log_and_print, suite_to_job_result
from scripts._ops_cli import run_standalone_job


def main() -> int:
    settings = load_cli_settings()

    def _runner(connection, job):
        audit_generated_at = generated_at(settings)
        suite = run_full_audit(settings, connection=connection, snapshot_ts=audit_generated_at)
        doc_paths = write_audit_docs(settings, audit_result=suite, generated_at=audit_generated_at)
        artifact_paths = write_audit_artifacts(
            settings,
            audit_result=suite,
            generated_at=audit_generated_at,
        )
        log_and_print(
            f"Audit summary report rendered. PASS={suite.pass_count} WARN={suite.warn_count} FAIL={suite.fail_count}",
        )
        return suite_to_job_result(
            suite,
            job_name="render_audit_summary_report",
            run_id=job.run_id,
            notes_prefix="Audit summary report rendered.",
            artifact_paths=[str(path) for path in doc_paths + artifact_paths],
        )

    result = run_standalone_job(
        settings,
        job_name="render_audit_summary_report",
        as_of_date=None,
        dry_run=False,
        policy_config_path=None,
        runner=_runner,
    )
    return 0 if result.status != "FAILED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
