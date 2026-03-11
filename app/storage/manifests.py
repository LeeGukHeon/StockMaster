from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import duckdb
import pandas as pd

from app.settings import get_settings
from app.storage.metadata_postgres import (
    execute_postgres_sql,
    fetchdf_postgres_sql,
    fetchone_postgres_sql,
    metadata_postgres_enabled,
)


def _json_text(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def record_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_type: str,
    started_at: datetime,
    as_of_date: date | None = None,
    input_sources: list[str] | None = None,
    notes: str | None = None,
    git_commit: str | None = None,
    ranking_version: str | None = None,
) -> None:
    query = """
        INSERT INTO ops_run_manifest (
            run_id,
            run_type,
            as_of_date,
            started_at,
            finished_at,
            status,
            input_sources_json,
            output_artifacts_json,
            model_version,
            feature_version,
            ranking_version,
            git_commit,
            notes,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    params = [
        run_id,
        run_type,
        as_of_date,
        started_at,
        None,
        "running",
        _json_text(input_sources or []),
        _json_text([]),
        None,
        None,
        ranking_version,
        git_commit,
        notes,
        None,
    ]
    connection.execute(query, params)
    settings = get_settings()
    execute_postgres_sql(settings, query, params)


def record_run_finish(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    finished_at: datetime,
    status: str,
    output_artifacts: list[str] | None = None,
    notes: str | None = None,
    error_message: str | None = None,
    model_version: str | None = None,
    feature_version: str | None = None,
    ranking_version: str | None = None,
) -> None:
    query = """
        UPDATE ops_run_manifest
        SET finished_at = ?,
            status = ?,
            output_artifacts_json = ?,
            notes = ?,
            error_message = ?,
            model_version = ?,
            feature_version = ?,
            ranking_version = ?
        WHERE run_id = ?
        """
    params = [
        finished_at,
        status,
        _json_text(output_artifacts or []),
        notes,
        error_message,
        model_version,
        feature_version,
        ranking_version,
        run_id,
    ]
    connection.execute(query, params)
    settings = get_settings()
    execute_postgres_sql(settings, query, params)


def append_artifact(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    artifact_path: str,
) -> None:
    existing = connection.execute(
        "SELECT output_artifacts_json FROM ops_run_manifest WHERE run_id = ?",
        [run_id],
    ).fetchone()
    artifacts = json.loads(existing[0]) if existing and existing[0] else []
    artifacts.append(artifact_path)
    query = "UPDATE ops_run_manifest SET output_artifacts_json = ? WHERE run_id = ?"
    params = [_json_text(artifacts), run_id]
    connection.execute(query, params)
    settings = get_settings()
    if metadata_postgres_enabled(settings):
        existing_pg = fetchone_postgres_sql(
            settings,
            "SELECT output_artifacts_json FROM ops_run_manifest WHERE run_id = ?",
            [run_id],
        )
        pg_artifacts = json.loads(existing_pg[0]) if existing_pg and existing_pg[0] else []
        pg_artifacts.append(artifact_path)
        execute_postgres_sql(
            settings,
            query,
            [_json_text(pg_artifacts), run_id],
        )


def fetch_recent_runs(connection: duckdb.DuckDBPyConnection, limit: int = 10):
    query = """
        SELECT
            run_id,
            run_type,
            as_of_date,
            started_at,
            finished_at,
            status,
            notes,
            error_message,
            output_artifacts_json
            ,
            feature_version,
            ranking_version
        FROM ops_run_manifest
        ORDER BY started_at DESC
        LIMIT ?
        """
    settings = get_settings()
    if metadata_postgres_enabled(settings):
        return fetchdf_postgres_sql(settings, query, [limit])
    return connection.execute(query, [limit]).fetchdf()
