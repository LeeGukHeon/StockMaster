from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum


class JobStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    DEGRADED_SUCCESS = "DEGRADED_SUCCESS"
    SKIPPED = "SKIPPED"
    SKIPPED_NON_TRADING_DAY = "SKIPPED_NON_TRADING_DAY"
    SKIPPED_ALREADY_DONE = "SKIPPED_ALREADY_DONE"
    SKIPPED_LOCKED = "SKIPPED_LOCKED"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"


class TriggerType(StrEnum):
    MANUAL = "MANUAL"
    SCHEDULED = "SCHEDULED"
    RECOVERY = "RECOVERY"
    VALIDATION = "VALIDATION"
    DRY_RUN = "DRY_RUN"


class AlertSeverity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class LockStatus(StrEnum):
    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"
    STALE_RELEASED = "STALE_RELEASED"


class RecoveryStatus(StrEnum):
    OPEN = "OPEN"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class OpsFrameworkError(RuntimeError):
    pass


class LockConflictError(OpsFrameworkError):
    def __init__(self, message: str, *, lock_name: str, owner_run_id: str | None = None) -> None:
        super().__init__(message)
        self.lock_name = lock_name
        self.owner_run_id = owner_run_id


class JobBlockedError(OpsFrameworkError):
    pass


@dataclass(slots=True)
class OpsJobResult:
    run_id: str
    job_name: str
    status: str
    notes: str
    artifact_paths: list[str] = field(default_factory=list)
    as_of_date: date | None = None
    row_count: int | None = None


@dataclass(slots=True)
class OpsValidationResult:
    run_id: str
    check_count: int
    warning_count: int
    notes: str
    artifact_paths: list[str] = field(default_factory=list)


def manifest_status(status: str) -> str:
    normalized = str(status).upper()
    if normalized in {JobStatus.SUCCESS, JobStatus.PARTIAL_SUCCESS, JobStatus.DEGRADED_SUCCESS}:
        return "success"
    if normalized in {
        JobStatus.SKIPPED,
        JobStatus.SKIPPED_NON_TRADING_DAY,
        JobStatus.SKIPPED_ALREADY_DONE,
        JobStatus.SKIPPED_LOCKED,
    }:
        return "skipped"
    if normalized == JobStatus.BLOCKED:
        return "blocked"
    if normalized == JobStatus.RUNNING:
        return "running"
    return "failed"
