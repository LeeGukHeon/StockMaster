from __future__ import annotations

from datetime import timedelta

from app.common.time import utc_now
from app.ops.locks import LockManager
from app.ops.repository import record_job_run_finish, record_job_run_start
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
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


def test_lock_manager_reclaims_lock_when_owner_run_is_finished(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    now = utc_now()
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        record_job_run_start(
            connection,
            run_id="old-run",
            job_name="run_daily_close_bundle",
            trigger_type="MANUAL",
            started_at=now,
            as_of_date=None,
            root_run_id="old-run",
            parent_run_id=None,
            recovery_of_run_id=None,
            lock_name="scheduler_global_write",
            policy_id="default",
            policy_version="v1",
            dry_run=False,
            notes="seed",
            details={"hostname": "test-host", "pid": 1234},
        )
        record_run_start(
            connection,
            run_id="old-run",
            run_type="run_daily_close_bundle",
            started_at=now,
            as_of_date=None,
            notes="seed",
        )
        manager = LockManager(connection)
        manager.acquire(
            lock_name="scheduler_global_write",
            job_name="run_daily_close_bundle",
            owner_run_id="old-run",
            stale_lock_minutes=30,
            details={"hostname": "test-host", "pid": 1234},
        )
        record_job_run_finish(
            connection,
            run_id="old-run",
            finished_at=utc_now(),
            status="FAILED",
            step_count=0,
            failed_step_count=0,
            artifact_count=0,
            notes="done",
            error_message="seed",
            details={},
        )
        record_run_finish(
            connection,
            run_id="old-run",
            finished_at=utc_now(),
            status="failed",
            output_artifacts=[],
            notes="done",
            error_message="seed",
        )

        handle = manager.acquire(
            lock_name="scheduler_global_write",
            job_name="run_daily_close_bundle",
            owner_run_id="new-run",
            stale_lock_minutes=30,
            details={"hostname": "test-host", "pid": 4321},
        )
        row = connection.execute(
            """
            SELECT owner_run_id, status, release_reason
            FROM fact_active_lock
            WHERE lock_name = ?
            """,
            [handle.lock_name],
        ).fetchone()

    assert row[0] == "new-run"
    assert row[1] == "ACTIVE"
    assert row[2] is None
