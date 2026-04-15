from __future__ import annotations

import pandas as pd

from app.discord_bot import read_store


def test_fetch_discord_bot_snapshot_rows_orders_by_latest_snapshot(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(read_store, "metadata_postgres_enabled", lambda _settings: True)

    def fake_fetchdf(settings, query, params):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(read_store, "fetchdf_postgres_sql", fake_fetchdf)

    read_store.fetch_discord_bot_snapshot_rows(object(), snapshot_type="status", limit=1)

    query = str(captured["query"])
    assert "ORDER BY snapshot_ts DESC, sort_order NULLS LAST, snapshot_key" in query


def test_fetch_active_job_runs_requires_unreleased_active_lock(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(read_store, "metadata_postgres_enabled", lambda _settings: True)

    def fake_fetchdf(settings, query, params):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(read_store, "fetchdf_postgres_sql", fake_fetchdf)

    read_store.fetch_active_job_runs(object(), limit=5)

    query = str(captured["query"])
    assert "JOIN fact_active_lock AS active_lock" in query
    assert "active_lock.owner_run_id = job.run_id" in query
    assert "active_lock.released_at IS NULL" in query
