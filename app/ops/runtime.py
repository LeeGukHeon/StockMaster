from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Callable

import duckdb

from app.common.disk import DiskWatermark, measure_disk_usage
from app.common.run_context import activate_run_context
from app.common.time import utc_now
from app.ops.common import (
    JobBlockedError,
    JobStatus,
    LockConflictError,
    OpsJobResult,
    TriggerType,
    manifest_status,
)
from app.ops.locks import LockManager
from app.ops.policy import OpsPolicyResolver, ResolvedOpsPolicy
from app.ops.repository import (
    record_job_run_finish,
    record_job_run_start,
    record_step_run_finish,
    record_step_run_start,
)
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, connect_duckdb
from app.storage.manifests import record_run_finish, record_run_start


def _cleanup_orphaned_job_runs(
    connection: duckdb.DuckDBPyConnection,
    *,
    job_name: str,
    stale_after_minutes: int,
) -> None:
    cutoff = utc_now() - timedelta(minutes=max(5, stale_after_minutes))
    rows = connection.execute(
        """
        SELECT run_id, notes, details_json, artifact_count
        FROM fact_job_run
        WHERE job_name = ?
          AND status = 'RUNNING'
          AND finished_at IS NULL
          AND started_at < ?
          AND run_id NOT IN (
              SELECT owner_run_id
              FROM fact_active_lock
              WHERE released_at IS NULL
                AND owner_run_id IS NOT NULL
          )
        """,
        [job_name, cutoff],
    ).fetchall()
    for run_id, notes, details_json, artifact_count in rows:
        now = utc_now()
        connection.execute(
            """
            UPDATE fact_job_step_run
            SET finished_at = ?,
                status = 'FAILED',
                error_message = COALESCE(error_message, ?)
            WHERE job_run_id = ?
              AND status = 'RUNNING'
            """,
            [now, "Marked failed after stale running state cleanup.", run_id],
        )
        current_details = json.loads(details_json) if details_json else {}
        current_details["cleanup_recovered"] = True
        current_details["cleanup_recovered_at"] = now.isoformat()
        merged_notes = " | ".join(
            part for part in [notes, "Cleared as stale before new run start."] if part
        )
        step_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM fact_job_step_run WHERE job_run_id = ?",
                [run_id],
            ).fetchone()[0]
        )
        failed_step_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM fact_job_step_run WHERE job_run_id = ? AND status = 'FAILED'",
                [run_id],
            ).fetchone()[0]
        )
        record_job_run_finish(
            connection,
            run_id=str(run_id),
            finished_at=now,
            status=JobStatus.FAILED,
            step_count=step_count,
            failed_step_count=failed_step_count,
            artifact_count=int(artifact_count or 0),
            notes=merged_notes,
            error_message="Marked failed after stale running state cleanup.",
            details=current_details,
        )
        record_run_finish(
            connection,
            run_id=str(run_id),
            finished_at=now,
            status=manifest_status(JobStatus.FAILED),
            output_artifacts=[],
            notes=merged_notes,
            error_message="Marked failed after stale running state cleanup.",
        )


@dataclass(slots=True)
class StepRunContext:
    connection: duckdb.DuckDBPyConnection
    job_run_id: str
    step_name: str
    step_order: int
    critical_flag: bool
    notes: str | None = None
    step_run_id: str = field(init=False)
    started_at: object = field(init=False)

    def __enter__(self) -> "StepRunContext":
        self.step_run_id = f"{self.job_run_id}:{self.step_order:02d}:{self.step_name}"
        self.started_at = utc_now()
        record_step_run_start(
            self.connection,
            step_run_id=self.step_run_id,
            job_run_id=self.job_run_id,
            step_name=self.step_name,
            step_order=self.step_order,
            started_at=self.started_at,
            critical_flag=self.critical_flag,
            notes=self.notes,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        record_step_run_finish(
            self.connection,
            step_run_id=self.step_run_id,
            finished_at=utc_now(),
            status=JobStatus.SUCCESS if exc is None else JobStatus.FAILED,
            notes=self.notes,
            error_message=None if exc is None else str(exc),
            details=None,
        )
        return False


class JobRunContext:
    def __init__(
        self,
        settings: Settings,
        connection: duckdb.DuckDBPyConnection,
        *,
        job_name: str,
        as_of_date: date | None,
        trigger_type: str = TriggerType.MANUAL,
        dry_run: bool = False,
        parent_run_id: str | None = None,
        root_run_id: str | None = None,
        recovery_of_run_id: str | None = None,
        policy_config_path: str | None = None,
        lock_name: str | None = None,
        notes: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.settings = settings
        self.connection = connection
        self.job_name = job_name
        self.as_of_date = as_of_date
        self.trigger_type = str(trigger_type)
        self.dry_run = dry_run
        self.parent_run_id = parent_run_id
        self.root_run_id = root_run_id
        self.recovery_of_run_id = recovery_of_run_id
        self.policy_config_path = policy_config_path
        self.lock_name = lock_name or job_name
        self.notes = notes
        self.details = details or {}
        self._context_cm = activate_run_context(job_name, as_of_date=as_of_date)
        self._artifacts: list[str] = []
        self._step_count = 0
        self._failed_step_count = 0
        self._status_override: str | None = None
        self._extra_notes: list[str] = []
        self._lock_acquired = False
        self._resolved_policy: ResolvedOpsPolicy | None = None

    def _ensure_connection(self) -> None:
        if self.connection is not None:
            return
        self.connection = connect_duckdb(self.settings.paths.duckdb_path, read_only=False)
        bootstrap_core_tables(self.connection)

    def _close_connection(self) -> None:
        if self.connection is None:
            return
        try:
            self.connection.close()
        finally:
            self.connection = None

    @property
    def run_id(self) -> str:
        return self.run_context.run_id

    @property
    def resolved_policy(self) -> ResolvedOpsPolicy:
        assert self._resolved_policy is not None
        return self._resolved_policy

    def __enter__(self) -> "JobRunContext":
        self.run_context = self._context_cm.__enter__()
        self._resolved_policy = OpsPolicyResolver(self.settings).resolve(
            self.connection,
            as_of_at=self.run_context.started_at,
            policy_config_path=self.policy_config_path,
        )
        _cleanup_orphaned_job_runs(
            self.connection,
            job_name=self.job_name,
            stale_after_minutes=max(5, self._resolved_policy.policy.stale_lock_minutes // 6),
        )
        record_job_run_start(
            self.connection,
            run_id=self.run_context.run_id,
            job_name=self.job_name,
            trigger_type=self.trigger_type,
            started_at=self.run_context.started_at,
            as_of_date=self.as_of_date,
            root_run_id=self.root_run_id or self.run_context.run_id,
            parent_run_id=self.parent_run_id,
            recovery_of_run_id=self.recovery_of_run_id,
            lock_name=self.lock_name,
            policy_id=self.resolved_policy.policy.policy_id,
            policy_version=self.resolved_policy.policy.policy_version,
            dry_run=self.dry_run,
            notes=self.notes,
            details={
                "policy_source": self.resolved_policy.source,
                "policy_path": self.resolved_policy.policy_path,
                **self.details,
            },
        )
        record_run_start(
            self.connection,
            run_id=self.run_context.run_id,
            run_type=self.job_name,
            started_at=self.run_context.started_at,
            as_of_date=self.as_of_date,
            notes=self.notes,
        )
        disk_report = measure_disk_usage(
            self.settings.paths.data_dir,
            warning_ratio=self.resolved_policy.policy.warn_ratio,
            prune_ratio=self.resolved_policy.policy.cleanup_ratio,
            limit_ratio=self.resolved_policy.policy.emergency_ratio,
        )
        if (
            self.job_name in self.resolved_policy.policy.high_frequency_blocked_jobs
            and disk_report.status == DiskWatermark.LIMIT
        ):
            error = JobBlockedError(
                f"Blocked by emergency disk watermark for {self.job_name}: "
                f"{disk_report.usage_ratio:.1%}"
            )
            record_job_run_finish(
                self.connection,
                run_id=self.run_context.run_id,
                finished_at=utc_now(),
                status=JobStatus.BLOCKED,
                step_count=0,
                failed_step_count=0,
                artifact_count=0,
                notes="Blocked by emergency disk watermark policy.",
                error_message=str(error),
                details={
                    "disk_status": str(disk_report.status),
                    "usage_ratio": disk_report.usage_ratio,
                },
            )
            record_run_finish(
                self.connection,
                run_id=self.run_context.run_id,
                finished_at=utc_now(),
                status=manifest_status(JobStatus.BLOCKED),
                output_artifacts=[],
                notes="Blocked by emergency disk watermark policy.",
                error_message=str(error),
            )
            self._context_cm.__exit__(None, None, None)
            raise error
        try:
            LockManager(self.connection).acquire(
                lock_name=self.lock_name,
                job_name=self.job_name,
                owner_run_id=self.run_context.run_id,
                stale_lock_minutes=self.resolved_policy.policy.stale_lock_minutes,
                details={
                    "trigger_type": self.trigger_type,
                    "dry_run": self.dry_run,
                    "pid": os.getpid(),
                    "hostname": socket.gethostname(),
                },
            )
            self._lock_acquired = True
        except LockConflictError as exc:
            record_job_run_finish(
                self.connection,
                run_id=self.run_context.run_id,
                finished_at=utc_now(),
                status=JobStatus.BLOCKED,
                step_count=0,
                failed_step_count=0,
                artifact_count=0,
                notes=f"Blocked by active lock: {exc.lock_name}",
                error_message=str(exc),
                details={"owner_run_id": exc.owner_run_id},
            )
            record_run_finish(
                self.connection,
                run_id=self.run_context.run_id,
                finished_at=utc_now(),
                status=manifest_status(JobStatus.BLOCKED),
                output_artifacts=[],
                notes=f"Blocked by active lock: {exc.lock_name}",
                error_message=str(exc),
            )
            self._context_cm.__exit__(None, None, None)
            raise
        return self

    def add_artifact(self, path: str) -> None:
        self._artifacts.append(path)

    def extend_artifacts(self, paths: list[str]) -> None:
        self._artifacts.extend(paths)

    def mark_partial_success(self, note: str) -> None:
        self._status_override = JobStatus.PARTIAL_SUCCESS
        self._extra_notes.append(note)

    def mark_degraded(self, note: str) -> None:
        self._status_override = JobStatus.DEGRADED_SUCCESS
        self._extra_notes.append(note)

    def skip(self, note: str, *, status: str = JobStatus.SKIPPED) -> None:
        self._status_override = status
        self._extra_notes.append(note)

    def block(self, note: str) -> None:
        self._status_override = JobStatus.BLOCKED
        self._extra_notes.append(note)

    def run_step(
        self,
        step_name: str,
        func: Callable[..., Any],
        *args: Any,
        critical: bool = True,
        notes: str | None = None,
        **kwargs: Any,
    ) -> Any:
        self._step_count += 1
        try:
            with StepRunContext(
                self.connection,
                job_run_id=self.run_context.run_id,
                step_name=step_name,
                step_order=self._step_count,
                critical_flag=critical,
                notes=notes,
            ):
                result = func(*args, **kwargs)
                if hasattr(result, "artifact_paths"):
                    self.extend_artifacts(list(result.artifact_paths))
                return result
        except Exception:
            self._failed_step_count += 1
            if critical:
                raise
            self.mark_degraded(f"Optional step failed: {step_name}")
            return None

    def run_detached_step(
        self,
        step_name: str,
        func: Callable[..., Any],
        *args: Any,
        critical: bool = True,
        notes: str | None = None,
        **kwargs: Any,
    ) -> Any:
        self._step_count += 1
        step_run_id = f"{self.run_context.run_id}:{self._step_count:02d}:{step_name}"
        started_at = utc_now()
        record_step_run_start(
            self.connection,
            step_run_id=step_run_id,
            job_run_id=self.run_context.run_id,
            step_name=step_name,
            step_order=self._step_count,
            started_at=started_at,
            critical_flag=critical,
            notes=notes,
        )
        self._close_connection()
        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            self._ensure_connection()
            record_step_run_finish(
                self.connection,
                step_run_id=step_run_id,
                finished_at=utc_now(),
                status=JobStatus.FAILED,
                notes=notes,
                error_message=str(exc),
                details=None,
            )
            self._failed_step_count += 1
            if critical:
                raise
            self.mark_degraded(f"Optional step failed: {step_name}")
            return None
        self._ensure_connection()
        record_step_run_finish(
            self.connection,
            step_run_id=step_run_id,
            finished_at=utc_now(),
            status=JobStatus.SUCCESS,
            notes=notes,
            error_message=None,
            details=None,
        )
        if hasattr(result, "artifact_paths"):
            self.extend_artifacts(list(result.artifact_paths))
        return result

    def __exit__(self, exc_type, exc, tb) -> bool:
        self._ensure_connection()
        finished_at = utc_now()
        if self._lock_acquired:
            LockManager(self.connection).release(
                lock_name=self.lock_name,
                owner_run_id=self.run_context.run_id,
                reason="job_finished" if exc is None else "job_failed",
            )
        if exc is not None:
            status = JobStatus.FAILED
            error_message = str(exc)
        elif self._status_override is not None:
            status = self._status_override
            error_message = None
        else:
            status = JobStatus.SUCCESS
            error_message = None
        note_parts = [part for part in [self.notes, *self._extra_notes] if part]
        notes = " | ".join(note_parts) if note_parts else None
        details = {
            "artifact_paths": self._artifacts,
            "trigger_type": self.trigger_type,
            "dry_run": self.dry_run,
            **self.details,
        }
        record_job_run_finish(
            self.connection,
            run_id=self.run_context.run_id,
            finished_at=finished_at,
            status=status,
            step_count=self._step_count,
            failed_step_count=self._failed_step_count,
            artifact_count=len(self._artifacts),
            notes=notes,
            error_message=error_message,
            details=details,
        )
        record_run_finish(
            self.connection,
            run_id=self.run_context.run_id,
            finished_at=finished_at,
            status=manifest_status(status),
            output_artifacts=self._artifacts,
            notes=notes,
            error_message=error_message,
        )
        self._context_cm.__exit__(exc_type, exc, tb)
        return False


def job_result_from_context(
    job: JobRunContext,
    *,
    notes: str,
    row_count: int | None = None,
) -> OpsJobResult:
    return OpsJobResult(
        run_id=job.run_id,
        job_name=job.job_name,
        status=job._status_override or JobStatus.SUCCESS,
        notes=notes,
        artifact_paths=list(job._artifacts),
        as_of_date=job.as_of_date,
        row_count=row_count,
    )
