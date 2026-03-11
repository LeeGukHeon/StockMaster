# ruff: noqa: E402

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import now_local
from app.ops.common import JobStatus, LockConflictError
from app.ops.scheduler import get_scheduled_job, read_scheduler_state, write_scheduler_state
from app.ops.serial import SerialLockConflictError, acquire_serial_lock, release_serial_lock
from scripts._ops_cli import load_cli_settings, log_and_print

SUCCESSFUL_TERMINAL_STATUSES = {
    JobStatus.SUCCESS,
    JobStatus.PARTIAL_SUCCESS,
    JobStatus.DEGRADED_SUCCESS,
}


@dataclass(slots=True)
class SchedulerBundleResult:
    job_key: str
    status: str
    notes: str
    run_ids: list[str] = field(default_factory=list)
    artifact_paths: list[str] = field(default_factory=list)
    as_of_date: date | None = None
    row_count: int | None = None
    details: dict[str, Any] = field(default_factory=dict)


class SchedulerSkip(RuntimeError):
    def __init__(self, status: str, notes: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(notes)
        self.status = status
        self.notes = notes
        self.details = details or {}


def bundle_result(
    *,
    job_key: str,
    status: str,
    notes: str,
    run_ids: list[str] | None = None,
    artifact_paths: list[str] | None = None,
    as_of_date: date | None = None,
    row_count: int | None = None,
    details: dict[str, Any] | None = None,
) -> SchedulerBundleResult:
    return SchedulerBundleResult(
        job_key=job_key,
        status=status,
        notes=notes,
        run_ids=list(run_ids or []),
        artifact_paths=list(artifact_paths or []),
        as_of_date=as_of_date,
        row_count=row_count,
        details=dict(details or {}),
    )


def _state_matches_identity(state: dict[str, Any], identity: dict[str, Any]) -> bool:
    state_identity = state.get("identity")
    return isinstance(state_identity, dict) and state_identity == identity


def _write_bundle_state(
    *,
    settings,
    job_key: str,
    status: str,
    notes: str,
    identity: dict[str, Any],
    run_ids: list[str] | None = None,
    artifact_paths: list[str] | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    timestamp = now_local(settings.app.timezone)
    write_scheduler_state(
        settings,
        job_key,
        {
            "job_key": job_key,
            "status": str(status),
            "notes": notes,
            "identity": identity,
            "run_ids": list(run_ids or []),
            "artifact_paths": list(artifact_paths or []),
            "details": dict(details or {}),
            "finished_at": timestamp.isoformat(),
            "run_id": (run_ids or [None])[0],
        },
    )


def scheduler_exit_code(status: str) -> int:
    normalized = str(status).upper()
    return 1 if normalized in {JobStatus.FAILED, JobStatus.BLOCKED} else 0


def run_scheduled_bundle(
    *,
    job_key: str,
    runner: Callable[[Any], SchedulerBundleResult],
    identity: dict[str, Any],
    force: bool = False,
    stale_lock_minutes: int = 180,
) -> int:
    settings = load_cli_settings()
    job = get_scheduled_job(job_key)
    state = read_scheduler_state(settings, job_key)

    if (
        not force
        and _state_matches_identity(state, identity)
        and str(state.get("status", "")).upper() in SUCCESSFUL_TERMINAL_STATUSES
    ):
        result = bundle_result(
            job_key=job_key,
            status=JobStatus.SKIPPED_ALREADY_DONE,
            notes="Already completed for the same scheduler identity.",
            run_ids=list(state.get("run_ids") or []),
            artifact_paths=list(state.get("artifact_paths") or []),
            as_of_date=identity.get("as_of_date"),
            details={"previous_state": state},
        )
        _write_bundle_state(
            settings=settings,
            job_key=job_key,
            status=result.status,
            notes=result.notes,
            identity=identity,
            run_ids=result.run_ids,
            artifact_paths=result.artifact_paths,
            details=result.details,
        )
        log_and_print(f"{job.label}: {result.status} {result.notes}")
        return scheduler_exit_code(result.status)

    try:
        handle = acquire_serial_lock(
            settings,
            lock_key=job.serial_scope,
            owner_run_id=None,
            job_name=job_key,
            stale_after_minutes=stale_lock_minutes,
            details={"identity": identity},
        )
    except SerialLockConflictError as exc:
        result = bundle_result(
            job_key=job_key,
            status=JobStatus.SKIPPED_LOCKED,
            notes=f"Serial lock occupied: {exc.lock_key}",
            as_of_date=identity.get("as_of_date"),
            details={"lock_details": exc.details, "identity": identity},
        )
        _write_bundle_state(
            settings=settings,
            job_key=job_key,
            status=result.status,
            notes=result.notes,
            identity=identity,
            run_ids=result.run_ids,
            artifact_paths=result.artifact_paths,
            details=result.details,
        )
        log_and_print(f"{job.label}: {result.status} {result.notes}")
        return scheduler_exit_code(result.status)

    try:
        try:
            result = runner(settings)
        except SchedulerSkip as exc:
            result = bundle_result(
                job_key=job_key,
                status=exc.status,
                notes=exc.notes,
                as_of_date=identity.get("as_of_date"),
                details=exc.details,
            )
        except LockConflictError as exc:
            result = bundle_result(
                job_key=job_key,
                status=JobStatus.SKIPPED_LOCKED,
                notes=f"Active lock occupied: {exc.lock_name}",
                as_of_date=identity.get("as_of_date"),
                details={"lock_name": exc.lock_name, "owner_run_id": exc.owner_run_id},
            )
        except Exception as exc:
            result = bundle_result(
                job_key=job_key,
                status=JobStatus.FAILED,
                notes=str(exc),
                as_of_date=identity.get("as_of_date"),
                details={"identity": identity},
            )
            _write_bundle_state(
                settings=settings,
                job_key=job_key,
                status=result.status,
                notes=result.notes,
                identity=identity,
                run_ids=result.run_ids,
                artifact_paths=result.artifact_paths,
                details=result.details,
            )
            raise
    finally:
        release_serial_lock(handle)

    _write_bundle_state(
        settings=settings,
        job_key=job_key,
        status=result.status,
        notes=result.notes,
        identity=identity,
        run_ids=result.run_ids,
        artifact_paths=result.artifact_paths,
        details=result.details,
    )
    log_and_print(f"{job.label}: {result.status} {result.notes}")
    return scheduler_exit_code(result.status)
