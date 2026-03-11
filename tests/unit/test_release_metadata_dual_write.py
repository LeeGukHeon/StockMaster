from __future__ import annotations

from app.release.snapshot import build_latest_app_snapshot, build_ui_freshness_snapshot
from app.release.validation import validate_page_contracts
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def test_build_latest_app_snapshot_mirrors_to_metadata_store(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    captured: list[tuple[str, list[object]]] = []

    monkeypatch.setattr(
        "app.release.snapshot.execute_postgres_sql",
        lambda _settings, query, params=None: captured.append((query, list(params or []))),
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        build_latest_app_snapshot(settings, connection=connection)

    assert captured
    assert "INSERT INTO fact_latest_app_snapshot" in captured[0][0]


def test_build_ui_freshness_snapshot_mirrors_to_metadata_store(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    captured: list[list[object]] = []

    monkeypatch.setattr(
        "app.release.snapshot.executemany_postgres_sql",
        lambda _settings, query, rows: captured.extend(rows),
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        build_ui_freshness_snapshot(settings, connection=connection)

    assert captured


def test_validate_page_contracts_mirrors_checks_to_metadata_store(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    captured: list[list[object]] = []

    monkeypatch.setattr(
        "app.release.validation.executemany_postgres_sql",
        lambda _settings, query, rows: captured.extend(rows),
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        validate_page_contracts(settings, connection=connection, persist_results=True)

    assert captured
