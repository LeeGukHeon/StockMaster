from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import duckdb

from app.storage.duckdb import connect_duckdb, duckdb_connection


class _DummyConnection:
    pass


def test_connect_duckdb_falls_back_from_read_only_on_config_conflict(monkeypatch, tmp_path):
    calls: list[bool] = []
    dummy = _DummyConnection()

    def _fake_connect(path: str, *, read_only: bool):
        calls.append(read_only)
        if read_only:
            raise duckdb.ConnectionException(
                "Can't open a connection to same database file with a different "
                "configuration than existing connections"
            )
        return dummy

    monkeypatch.setattr("app.storage.duckdb.duckdb.connect", _fake_connect)

    result = connect_duckdb(Path(tmp_path) / "test.duckdb", read_only=True)

    assert result is dummy
    assert calls == [True, False]


def test_connect_duckdb_raises_non_conflict_read_only_errors(monkeypatch, tmp_path):
    def _fake_connect(path: str, *, read_only: bool):
        raise duckdb.ConnectionException("some other failure")

    monkeypatch.setattr("app.storage.duckdb.duckdb.connect", _fake_connect)

    try:
        connect_duckdb(Path(tmp_path) / "test.duckdb", read_only=True)
    except duckdb.ConnectionException as exc:
        assert "some other failure" in str(exc)
    else:
        raise AssertionError("Expected ConnectionException to be raised")


def test_duckdb_connection_uses_snapshot_on_read_only_lock_conflict(monkeypatch, tmp_path):
    dummy = _DummyConnection()

    def _fake_connect_duckdb(path: Path, *, read_only: bool):
        raise duckdb.IOException("Could not set lock on file")

    @contextmanager
    def _fake_snapshot_connection(path: Path):
        yield dummy

    monkeypatch.setattr("app.storage.duckdb.connect_duckdb", _fake_connect_duckdb)
    monkeypatch.setattr("app.storage.duckdb.duckdb_snapshot_connection", _fake_snapshot_connection)

    with duckdb_connection(Path(tmp_path) / "test.duckdb", read_only=True) as connection:
        assert connection is dummy
