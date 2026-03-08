from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Iterable

import duckdb


def json_text(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _executemany_dicts(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    rows: Iterable[dict[str, Any]],
    columns: list[str],
) -> None:
    payload = list(rows)
    if not payload:
        return
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    connection.executemany(
        sql,
        [[row.get(column) for column in columns] for row in payload],
    )


def record_job_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    job_name: str,
    trigger_type: str,
    started_at: datetime,
    as_of_date: date | None,
    root_run_id: str,
    parent_run_id: str | None,
    recovery_of_run_id: str | None,
    lock_name: str | None,
    policy_id: str | None,
    policy_version: str | None,
    dry_run: bool,
    notes: str | None,
    details: dict[str, Any] | None,
) -> None:
    connection.execute(
        """
        INSERT INTO fact_job_run (
            run_id,
            job_name,
            trigger_type,
            status,
            as_of_date,
            started_at,
            finished_at,
            root_run_id,
            parent_run_id,
            recovery_of_run_id,
            lock_name,
            policy_id,
            policy_version,
            dry_run,
            step_count,
            failed_step_count,
            artifact_count,
            notes,
            error_message,
            details_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            job_name,
            trigger_type,
            "RUNNING",
            as_of_date,
            started_at,
            None,
            root_run_id,
            parent_run_id,
            recovery_of_run_id,
            lock_name,
            policy_id,
            policy_version,
            dry_run,
            0,
            0,
            0,
            notes,
            None,
            json_text(details),
            started_at,
        ],
    )


def record_job_run_finish(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    finished_at: datetime,
    status: str,
    step_count: int,
    failed_step_count: int,
    artifact_count: int,
    notes: str | None,
    error_message: str | None,
    details: dict[str, Any] | None,
) -> None:
    connection.execute(
        """
        UPDATE fact_job_run
        SET finished_at = ?,
            status = ?,
            step_count = ?,
            failed_step_count = ?,
            artifact_count = ?,
            notes = ?,
            error_message = ?,
            details_json = ?
        WHERE run_id = ?
        """,
        [
            finished_at,
            status,
            step_count,
            failed_step_count,
            artifact_count,
            notes,
            error_message,
            json_text(details),
            run_id,
        ],
    )


def record_step_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    step_run_id: str,
    job_run_id: str,
    step_name: str,
    step_order: int,
    started_at: datetime,
    critical_flag: bool,
    notes: str | None,
) -> None:
    connection.execute(
        """
        INSERT INTO fact_job_step_run (
            step_run_id,
            job_run_id,
            step_name,
            step_order,
            status,
            started_at,
            finished_at,
            critical_flag,
            notes,
            error_message,
            details_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            step_run_id,
            job_run_id,
            step_name,
            step_order,
            "RUNNING",
            started_at,
            None,
            critical_flag,
            notes,
            None,
            None,
            started_at,
        ],
    )


def record_step_run_finish(
    connection: duckdb.DuckDBPyConnection,
    *,
    step_run_id: str,
    finished_at: datetime,
    status: str,
    notes: str | None,
    error_message: str | None,
    details: dict[str, Any] | None,
) -> None:
    connection.execute(
        """
        UPDATE fact_job_step_run
        SET finished_at = ?,
            status = ?,
            notes = ?,
            error_message = ?,
            details_json = ?
        WHERE step_run_id = ?
        """,
        [
            finished_at,
            status,
            notes,
            error_message,
            json_text(details),
            step_run_id,
        ],
    )


def insert_pipeline_dependency_rows(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
) -> None:
    _executemany_dicts(
        connection,
        table_name="fact_pipeline_dependency_state",
        rows=rows,
        columns=[
            "checked_at",
            "pipeline_name",
            "dependency_name",
            "status",
            "ready_flag",
            "required_state",
            "observed_state",
            "as_of_date",
            "details_json",
            "job_run_id",
            "created_at",
        ],
    )


def insert_health_snapshot_rows(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
) -> None:
    _executemany_dicts(
        connection,
        table_name="fact_health_snapshot",
        rows=rows,
        columns=[
            "snapshot_at",
            "health_scope",
            "component_name",
            "status",
            "metric_name",
            "metric_value_double",
            "metric_value_text",
            "as_of_date",
            "details_json",
            "job_run_id",
            "created_at",
        ],
    )


def insert_disk_watermark_event(
    connection: duckdb.DuckDBPyConnection,
    *,
    event_id: str,
    measured_at: datetime,
    disk_status: str,
    usage_ratio: float,
    used_gb: float,
    available_gb: float,
    total_gb: float,
    policy_id: str | None,
    policy_version: str | None,
    cleanup_required_flag: bool,
    emergency_block_flag: bool,
    notes: str | None,
    details: dict[str, Any] | None,
    job_run_id: str | None,
) -> None:
    connection.execute(
        """
        INSERT INTO fact_disk_watermark_event (
            event_id,
            measured_at,
            disk_status,
            usage_ratio,
            used_gb,
            available_gb,
            total_gb,
            policy_id,
            policy_version,
            cleanup_required_flag,
            emergency_block_flag,
            notes,
            details_json,
            job_run_id,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            event_id,
            measured_at,
            disk_status,
            usage_ratio,
            used_gb,
            available_gb,
            total_gb,
            policy_id,
            policy_version,
            cleanup_required_flag,
            emergency_block_flag,
            notes,
            json_text(details),
            job_run_id,
            measured_at,
        ],
    )


def insert_retention_cleanup_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    cleanup_run_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    dry_run: bool,
    cleanup_scope: str,
    removed_file_count: int,
    reclaimed_bytes: int,
    target_paths: list[str],
    notes: str | None,
    job_run_id: str | None,
    details: dict[str, Any] | None,
) -> None:
    connection.execute(
        """
        INSERT INTO fact_retention_cleanup_run (
            cleanup_run_id,
            started_at,
            finished_at,
            status,
            dry_run,
            cleanup_scope,
            removed_file_count,
            reclaimed_bytes,
            target_paths_json,
            notes,
            details_json,
            job_run_id,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            cleanup_run_id,
            started_at,
            finished_at,
            status,
            dry_run,
            cleanup_scope,
            removed_file_count,
            reclaimed_bytes,
            json_text(target_paths),
            notes,
            json_text(details),
            job_run_id,
            started_at,
        ],
    )


def insert_alert_event(
    connection: duckdb.DuckDBPyConnection,
    *,
    alert_id: str,
    created_at: datetime,
    alert_type: str,
    severity: str,
    component_name: str,
    status: str,
    message: str,
    details: dict[str, Any] | None,
    job_run_id: str | None,
    resolved_at: datetime | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id,
            created_at,
            alert_type,
            severity,
            component_name,
            status,
            message,
            details_json,
            job_run_id,
            resolved_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            alert_id,
            created_at,
            alert_type,
            severity,
            component_name,
            status,
            message,
            json_text(details),
            job_run_id,
            resolved_at,
        ],
    )


def insert_recovery_action(
    connection: duckdb.DuckDBPyConnection,
    *,
    recovery_action_id: str,
    created_at: datetime,
    action_type: str,
    status: str,
    target_job_run_id: str | None,
    triggered_by_run_id: str | None,
    recovery_run_id: str | None,
    lock_name: str | None,
    notes: str | None,
    details: dict[str, Any] | None,
    finished_at: datetime | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO fact_recovery_action (
            recovery_action_id,
            created_at,
            action_type,
            status,
            target_job_run_id,
            triggered_by_run_id,
            recovery_run_id,
            lock_name,
            notes,
            details_json,
            finished_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            recovery_action_id,
            created_at,
            action_type,
            status,
            target_job_run_id,
            triggered_by_run_id,
            recovery_run_id,
            lock_name,
            notes,
            json_text(details),
            finished_at,
        ],
    )


def update_recovery_action(
    connection: duckdb.DuckDBPyConnection,
    *,
    recovery_action_id: str,
    status: str,
    notes: str | None,
    details: dict[str, Any] | None,
    recovery_run_id: str | None = None,
    finished_at: datetime | None = None,
) -> None:
    connection.execute(
        """
        UPDATE fact_recovery_action
        SET status = ?,
            notes = ?,
            details_json = ?,
            recovery_run_id = COALESCE(?, recovery_run_id),
            finished_at = COALESCE(?, finished_at)
        WHERE recovery_action_id = ?
        """,
        [
            status,
            notes,
            json_text(details),
            recovery_run_id,
            finished_at,
            recovery_action_id,
        ],
    )


def deactivate_active_ops_policies(
    connection: duckdb.DuckDBPyConnection,
    *,
    effective_to_at: datetime,
) -> None:
    connection.execute(
        """
        UPDATE fact_active_ops_policy
        SET active_flag = FALSE,
            effective_to_at = ?
        WHERE active_flag = TRUE
          AND (effective_to_at IS NULL OR effective_to_at > ?)
        """,
        [effective_to_at, effective_to_at],
    )


def insert_active_ops_policy(
    connection: duckdb.DuckDBPyConnection,
    *,
    registry_id: str,
    policy_id: str,
    policy_version: str,
    policy_name: str,
    policy_path: str,
    effective_from_at: datetime,
    effective_to_at: datetime | None,
    active_flag: bool,
    promotion_type: str,
    note: str | None,
    rollback_of_registry_id: str | None,
    config_payload: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO fact_active_ops_policy (
            ops_policy_registry_id,
            policy_id,
            policy_version,
            policy_name,
            policy_path,
            effective_from_at,
            effective_to_at,
            active_flag,
            promotion_type,
            note,
            rollback_of_registry_id,
            config_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            registry_id,
            policy_id,
            policy_version,
            policy_name,
            policy_path,
            effective_from_at,
            effective_to_at,
            active_flag,
            promotion_type,
            note,
            rollback_of_registry_id,
            json_text(config_payload),
            effective_from_at,
        ],
    )
