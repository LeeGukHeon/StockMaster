from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import duckdb

from app.common.time import utc_now
from app.ops.common import LockConflictError, LockStatus
from app.ops.repository import insert_recovery_action, json_text


@dataclass(slots=True)
class LockHandle:
    lock_name: str
    job_name: str
    owner_run_id: str
    acquired_at: datetime
    expires_at: datetime


class LockManager:
    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.connection = connection

    def acquire(
        self,
        *,
        lock_name: str,
        job_name: str,
        owner_run_id: str,
        stale_lock_minutes: int,
        details: dict[str, Any] | None = None,
    ) -> LockHandle:
        now = utc_now()
        row = self.connection.execute(
            """
            SELECT owner_run_id
            FROM fact_active_lock
            WHERE lock_name = ?
              AND released_at IS NULL
            """,
            [lock_name],
        ).fetchone()
        if row is not None:
            raise LockConflictError(
                f"Active lock already exists for {lock_name}.",
                lock_name=lock_name,
                owner_run_id=str(row[0]) if row[0] is not None else None,
            )
        expires_at = now + timedelta(minutes=max(1, stale_lock_minutes))
        self.connection.execute(
            """
            INSERT OR REPLACE INTO fact_active_lock (
                lock_name,
                job_name,
                owner_run_id,
                acquired_at,
                expires_at,
                released_at,
                release_reason,
                status,
                details_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                lock_name,
                job_name,
                owner_run_id,
                now,
                expires_at,
                None,
                None,
                LockStatus.ACTIVE,
                json_text(details),
                now,
            ],
        )
        return LockHandle(
            lock_name=lock_name,
            job_name=job_name,
            owner_run_id=owner_run_id,
            acquired_at=now,
            expires_at=expires_at,
        )

    def release(
        self,
        *,
        lock_name: str,
        owner_run_id: str | None = None,
        reason: str,
        status: str = LockStatus.RELEASED,
    ) -> int:
        now = utc_now()
        if owner_run_id is None:
            count = int(
                self.connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_active_lock
                    WHERE lock_name = ?
                      AND released_at IS NULL
                    """,
                    [lock_name],
                ).fetchone()[0]
            )
        else:
            count = int(
                self.connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_active_lock
                    WHERE lock_name = ?
                      AND owner_run_id = ?
                      AND released_at IS NULL
                    """,
                    [lock_name, owner_run_id],
                ).fetchone()[0]
            )
        if owner_run_id is None:
            self.connection.execute(
                """
                UPDATE fact_active_lock
                SET released_at = ?,
                    release_reason = ?,
                    status = ?
                WHERE lock_name = ?
                  AND released_at IS NULL
                """,
                [now, reason, status, lock_name],
            )
        else:
            self.connection.execute(
                """
                UPDATE fact_active_lock
                SET released_at = ?,
                    release_reason = ?,
                    status = ?
                WHERE lock_name = ?
                  AND owner_run_id = ?
                  AND released_at IS NULL
                """,
                [now, reason, status, lock_name, owner_run_id],
            )
        return count

    def release_stale(
        self,
        *,
        stale_before: datetime | None = None,
        lock_name: str | None = None,
        triggered_by_run_id: str | None = None,
    ) -> int:
        stale_before = stale_before or utc_now()
        query = """
            SELECT lock_name, owner_run_id, job_name, acquired_at, expires_at
            FROM fact_active_lock
            WHERE released_at IS NULL
              AND expires_at < ?
        """
        params: list[Any] = [stale_before]
        if lock_name is not None:
            query += " AND lock_name = ?"
            params.append(lock_name)
        rows = self.connection.execute(query, params).fetchall()
        released = 0
        for current_lock_name, owner_run_id, job_name, acquired_at, expires_at in rows:
            released += self.release(
                lock_name=str(current_lock_name),
                reason="force_release_stale_lock",
                status=LockStatus.STALE_RELEASED,
            )
            insert_recovery_action(
                self.connection,
                recovery_action_id=f"recovery-lock-{current_lock_name}-{utc_now().strftime('%Y%m%dT%H%M%S')}",
                created_at=utc_now(),
                action_type="FORCE_RELEASE_STALE_LOCK",
                status="COMPLETED",
                target_job_run_id=str(owner_run_id) if owner_run_id is not None else None,
                triggered_by_run_id=triggered_by_run_id,
                recovery_run_id=None,
                lock_name=str(current_lock_name),
                notes="Released stale lock.",
                details={
                    "job_name": job_name,
                    "acquired_at": acquired_at,
                    "expires_at": expires_at,
                },
                finished_at=utc_now(),
            )
        return released
