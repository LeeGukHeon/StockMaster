from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from app.storage import metadata_postgres
from tests._ticket003_support import build_test_settings


class _FakeCursor:
    def __init__(self, events: list[tuple[str, object, object]]) -> None:
        self._events = events
        self.description = [SimpleNamespace(name="value")]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: list[object]) -> None:
        self._events.append(("execute", query, tuple(params)))

    def executemany(self, query: str, rows: list[list | tuple]) -> None:
        self._events.append(("executemany", query, tuple(tuple(row) for row in rows)))

    def fetchall(self):
        return [(1,)]

    def fetchone(self):
        return (1,)


class _FakeConnection:
    def __init__(self, events: list[tuple[str, object, object]]) -> None:
        self._events = events

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._events)


@contextmanager
def _fake_connection(events: list[tuple[str, object, object]]):
    yield _FakeConnection(events)


def test_fetchdf_postgres_sql_bootstraps_store_before_query(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    settings.metadata.enabled = True
    settings.metadata.backend = "postgres"
    settings.metadata.db_url = "postgresql://stockmaster:test@metadata_db:5432/stockmaster_meta"
    settings.metadata.db_schema = "public"

    metadata_postgres._BOOTSTRAPPED_POSTGRES_METADATA_KEYS.clear()
    events: list[tuple[str, object, object]] = []
    bootstrap_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        metadata_postgres,
        "bootstrap_postgres_metadata_store",
        lambda active_settings: bootstrap_calls.append(
            (active_settings.metadata.db_url or "", active_settings.metadata.db_schema)
        )
        or metadata_postgres._BOOTSTRAPPED_POSTGRES_METADATA_KEYS.add(
            metadata_postgres._postgres_metadata_cache_key(active_settings)
        ),
    )
    monkeypatch.setattr(
        metadata_postgres,
        "postgres_metadata_connection",
        lambda active_settings: _fake_connection(events),
    )

    frame = metadata_postgres.fetchdf_postgres_sql(settings, "SELECT 1 AS value")

    assert bootstrap_calls == [(settings.metadata.db_url or "", "public")]
    assert events == [("execute", "SELECT 1 AS value", ())]
    assert list(frame.columns) == ["value"]
    assert frame.iloc[0]["value"] == 1


def test_fetchdf_postgres_sql_bootstraps_only_once_per_process(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    settings.metadata.enabled = True
    settings.metadata.backend = "postgres"
    settings.metadata.db_url = "postgresql://stockmaster:test@metadata_db:5432/stockmaster_meta"
    settings.metadata.db_schema = "public"

    metadata_postgres._BOOTSTRAPPED_POSTGRES_METADATA_KEYS.clear()
    bootstrap_calls: list[int] = []

    monkeypatch.setattr(
        metadata_postgres,
        "bootstrap_postgres_metadata_store",
        lambda active_settings: bootstrap_calls.append(1)
        or metadata_postgres._BOOTSTRAPPED_POSTGRES_METADATA_KEYS.add(
            metadata_postgres._postgres_metadata_cache_key(active_settings)
        ),
    )
    monkeypatch.setattr(
        metadata_postgres,
        "postgres_metadata_connection",
        lambda active_settings: _fake_connection([]),
    )

    metadata_postgres.fetchdf_postgres_sql(settings, "SELECT 1 AS value")
    metadata_postgres.fetchdf_postgres_sql(settings, "SELECT 1 AS value")

    assert bootstrap_calls == [1]
