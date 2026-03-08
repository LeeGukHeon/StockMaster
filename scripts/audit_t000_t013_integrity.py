# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.audit.checks import run_full_audit
from app.audit.reporting import write_audit_artifacts, write_audit_docs
from app.release.snapshot import (
    build_latest_app_snapshot,
    build_report_index,
    build_ui_freshness_snapshot,
)
from app.release.validation import validate_release_candidate
from scripts._audit_cli import generated_at, load_cli_settings, log_and_print, suite_to_job_result
from scripts._ops_cli import run_standalone_job


def main() -> int:
    settings = load_cli_settings()

    def _runner(connection, job):
        job.run_step(
            "build_report_index",
            lambda: build_report_index(settings, connection=connection, job_run_id=job.run_id),
            notes="Refresh canonical report index.",
        )
        job.run_step(
            "build_ui_freshness_snapshot",
            lambda: build_ui_freshness_snapshot(
                settings, connection=connection, job_run_id=job.run_id
            ),
            notes="Refresh UI data freshness snapshot.",
        )
        job.run_step(
            "build_latest_app_snapshot",
            lambda: build_latest_app_snapshot(
                settings, connection=connection, job_run_id=job.run_id
            ),
            notes="Refresh Home/Today latest snapshot.",
        )
        job.run_step(
            "validate_release_candidate",
            lambda: validate_release_candidate(settings, connection=connection),
            notes="Refresh release candidate checks before audit.",
        )
        suite = job.run_step(
            "run_full_audit",
            lambda: run_full_audit(
                settings, connection=connection, snapshot_ts=generated_at(settings)
            ),
            notes="Run contract/latest/artifact/ticket coverage audit.",
        )
        audit_generated_at = generated_at(settings)
        doc_paths = job.run_step(
            "write_audit_docs",
            lambda: write_audit_docs(settings, audit_result=suite, generated_at=audit_generated_at),
            notes="Write audit docs to docs/.",
        )
        artifact_paths = job.run_step(
            "write_audit_artifacts",
            lambda: write_audit_artifacts(
                settings, audit_result=suite, generated_at=audit_generated_at
            ),
            notes="Write audit summary artifact bundle.",
        )
        job.extend_artifacts([str(path) for path in doc_paths + artifact_paths])
        log_and_print(
            f"T000-T013 audit completed. PASS={suite.pass_count} WARN={suite.warn_count} FAIL={suite.fail_count}",
        )
        return suite_to_job_result(
            suite,
            job_name="audit_t000_t013_integrity",
            run_id=job.run_id,
            notes_prefix="T000-T013 audit completed.",
            artifact_paths=[str(path) for path in doc_paths + artifact_paths],
        )

    result = run_standalone_job(
        settings,
        job_name="audit_t000_t013_integrity",
        as_of_date=None,
        dry_run=False,
        policy_config_path=None,
        runner=_runner,
    )
    return 0 if result.status != "FAILED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
