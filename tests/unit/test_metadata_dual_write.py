from __future__ import annotations

from datetime import datetime, timezone

from app.ops.repository import record_job_run_start
from app.storage.manifests import record_run_start
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def test_record_run_start_mirrors_to_metadata_store(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    captured: list[tuple[str, list[object]]] = []

    monkeypatch.setattr("app.storage.manifests.get_settings", lambda: settings)
    monkeypatch.setattr(
        "app.storage.manifests.execute_postgres_sql",
        lambda _settings, query, params: captured.append((query, list(params))),
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        record_run_start(
            connection,
            run_id="run-1",
            run_type="daily_pipeline",
            started_at=datetime.now(timezone.utc),
            notes="seed",
        )

    assert captured
    assert "INSERT INTO ops_run_manifest" in captured[0][0]
    assert captured[0][1][0] == "run-1"


def test_record_job_run_start_mirrors_to_metadata_store(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    captured: list[tuple[str, list[object]]] = []

    monkeypatch.setattr("app.ops.repository.get_settings", lambda: settings)
    monkeypatch.setattr(
        "app.ops.repository.execute_postgres_sql",
        lambda _settings, query, params: captured.append((query, list(params))),
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        record_job_run_start(
            connection,
            run_id="job-run-1",
            job_name="run_daily_close_bundle",
            trigger_type="MANUAL",
            started_at=datetime.now(timezone.utc),
            as_of_date=None,
            root_run_id="job-run-1",
            parent_run_id=None,
            recovery_of_run_id=None,
            lock_name="scheduler_global_write",
            policy_id="default",
            policy_version="v1",
            dry_run=False,
            notes="seed",
            details={"trigger_type": "MANUAL"},
        )

    assert captured
    assert "INSERT INTO fact_job_run" in captured[0][0]
    assert captured[0][1][0] == "job-run-1"
