from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import duckdb

from app.common.time import utc_now
from app.ops.common import LockConflictError, LockStatus
from app.ops.repository import insert_recovery_action, json_text
from app.settings import get_settings
from app.storage.metadata_postgres import execute_postgres_sql, metadata_postgres_enabled


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

    def _owner_run_is_terminal(self, owner_run_id: str | None) -> bool:
        if not owner_run_id:
            return False
        row = self.connection.execute(
            """
            SELECT status, finished_at
            FROM fact_job_run
            WHERE run_id = ?
            """,
            [owner_run_id],
        ).fetchone()
        if row is None:
            return True
        status, finished_at = row
        return finished_at is not None or str(status).upper() != "RUNNING"

    def _same_host_process_is_dead(self, details_json: str | None) -> bool:
        if not details_json:
            return False
        try:
            details = json.loads(str(details_json))
        except json.JSONDecodeError:
            return False
        hostname = str(details.get("hostname") or "")
        pid = details.get("pid")
        if not hostname or hostname != socket.gethostname():
            return False
        if pid in (None, "", 0):
            return False
        try:
            os.kill(int(pid), 0)
        except (OSError, ValueError):
            return True
        return False

    def _release_orphaned_lock(
        self,
        *,
        lock_name: str,
        owner_run_id: str | None,
        expires_at,
        details_json: str | None,
    ) -> bool:
        now = utc_now()
        if expires_at is not None and expires_at < now:
            self.release(
                lock_name=lock_name,
                reason="force_release_stale_lock",
                status=LockStatus.STALE_RELEASED,
            )
            return True
        if self._owner_run_is_terminal(owner_run_id):
            self.release(
                lock_name=lock_name,
                reason="owner_run_already_finished",
                status=LockStatus.STALE_RELEASED,
            )
            return True
        if self._same_host_process_is_dead(details_json):
            self.release(
                lock_name=lock_name,
                reason="owner_process_missing",
                status=LockStatus.STALE_RELEASED,
            )
            return True
        return False

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
            SELECT owner_run_id, expires_at, details_json
            FROM fact_active_lock
            WHERE lock_name = ?
              AND released_at IS NULL
            """,
            [lock_name],
        ).fetchone()
        if row is not None and self._release_orphaned_lock(
            lock_name=lock_name,
            owner_run_id=str(row[0]) if row[0] is not None else None,
            expires_at=row[1],
            details_json=str(row[2]) if row[2] is not None else None,
        ):
            row = self.connection.execute(
                """
                SELECT owner_run_id, expires_at, details_json
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
        query = """
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
            """
        params = [
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
        ]
        self.connection.execute(query, params)
        settings = get_settings()
        if metadata_postgres_enabled(settings):
            execute_postgres_sql(
                settings,
                """
                INSERT INTO fact_active_lock (
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
                ON CONFLICT (lock_name) DO UPDATE SET
                    job_name = EXCLUDED.job_name,
                    owner_run_id = EXCLUDED.owner_run_id,
                    acquired_at = EXCLUDED.acquired_at,
                    expires_at = EXCLUDED.expires_at,
                    released_at = EXCLUDED.released_at,
                    release_reason = EXCLUDED.release_reason,
                    status = EXCLUDED.status,
                    details_json = EXCLUDED.details_json,
                    created_at = EXCLUDED.created_at
                """,
                params,
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
            query = """
                UPDATE fact_active_lock
                SET released_at = ?,
                    release_reason = ?,
                    status = ?
                WHERE lock_name = ?
                  AND released_at IS NULL
                """
            params = [now, reason, status, lock_name]
        else:
            query = """
                UPDATE fact_active_lock
                SET released_at = ?,
                    release_reason = ?,
                    status = ?
                WHERE lock_name = ?
                  AND owner_run_id = ?
                  AND released_at IS NULL
                """
            params = [now, reason, status, lock_name, owner_run_id]
        self.connection.execute(query, params)
        execute_postgres_sql(get_settings(), query, params)
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
            SELECT lock_name, owner_run_id, job_name, acquired_at, expires_at, details_json
            FROM fact_active_lock
            WHERE released_at IS NULL
        """
        params: list[Any] = []
        if lock_name is not None:
            query += " AND lock_name = ?"
            params.append(lock_name)
        rows = self.connection.execute(query, params).fetchall()
        released = 0
        for current_lock_name, owner_run_id, job_name, acquired_at, expires_at, details_json in rows:
            if expires_at is not None and expires_at >= stale_before:
                if not self._owner_run_is_terminal(
                    str(owner_run_id) if owner_run_id is not None else None
                ) and not self._same_host_process_is_dead(
                    str(details_json) if details_json is not None else None
                ):
                    continue
            if not self._release_orphaned_lock(
                lock_name=str(current_lock_name),
                owner_run_id=str(owner_run_id) if owner_run_id is not None else None,
                expires_at=expires_at,
                details_json=str(details_json) if details_json is not None else None,
            ):
                continue
            released += 1
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
