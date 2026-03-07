from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import duckdb


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
) -> None:
    connection.execute(
        """
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
            git_commit,
            notes,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
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
            git_commit,
            notes,
            None,
        ],
    )


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
) -> None:
    connection.execute(
        """
        UPDATE ops_run_manifest
        SET finished_at = ?,
            status = ?,
            output_artifacts_json = ?,
            notes = ?,
            error_message = ?,
            model_version = ?,
            feature_version = ?
        WHERE run_id = ?
        """,
        [
            finished_at,
            status,
            _json_text(output_artifacts or []),
            notes,
            error_message,
            model_version,
            feature_version,
            run_id,
        ],
    )


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
    connection.execute(
        "UPDATE ops_run_manifest SET output_artifacts_json = ? WHERE run_id = ?",
        [_json_text(artifacts), run_id],
    )


def fetch_recent_runs(connection: duckdb.DuckDBPyConnection, limit: int = 10):
    return connection.execute(
        """
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
        FROM ops_run_manifest
        ORDER BY started_at DESC
        LIMIT ?
        """,
        [limit],
    ).fetchdf()
