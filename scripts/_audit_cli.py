# ruff: noqa: E402

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.audit.checks import AuditSuiteResult
from app.common.time import now_local
from app.ops.common import OpsJobResult
from scripts import _ops_cli

load_cli_settings = _ops_cli.load_cli_settings
log_and_print = _ops_cli.log_and_print
run_standalone_job = _ops_cli.run_standalone_job


def suite_to_job_result(
    suite: AuditSuiteResult,
    *,
    job_name: str,
    run_id: str,
    notes_prefix: str,
    artifact_paths: list[str] | None = None,
) -> OpsJobResult:
    return OpsJobResult(
        run_id=run_id,
        job_name=job_name,
        status=suite.job_status,
        notes=(
            f"{notes_prefix} "
            f"PASS={suite.pass_count} WARN={suite.warn_count} FAIL={suite.fail_count}"
        ),
        artifact_paths=artifact_paths or [],
        row_count=len(suite.results),
    )


def generated_at(settings) -> datetime:
    return now_local(settings.app.timezone)
