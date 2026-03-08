from __future__ import annotations

from datetime import timedelta

from app.common.time import utc_now
from app.ops.locks import LockManager
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def test_lock_manager_can_release_stale_lock(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        manager = LockManager(connection)
        handle = manager.acquire(
            lock_name="ops-maintenance",
            job_name="run_ops_maintenance_bundle",
            owner_run_id="run-1",
            stale_lock_minutes=1,
        )
        connection.execute(
            """
            UPDATE fact_active_lock
            SET expires_at = ?
            WHERE lock_name = ?
            """,
            [utc_now() - timedelta(minutes=5), handle.lock_name],
        )
        released = manager.release_stale(triggered_by_run_id="ops-test")
        assert released == 1
        row = connection.execute(
            """
            SELECT status, released_at
            FROM fact_active_lock
            WHERE lock_name = ?
            """,
            [handle.lock_name],
        ).fetchone()
    assert row[0] == "STALE_RELEASED"
    assert row[1] is not None
