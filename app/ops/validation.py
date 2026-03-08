from __future__ import annotations

import duckdb

from app.common.time import utc_now
from app.ops.common import OpsValidationResult
from app.ops.policy import load_active_or_default_ops_policy
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables


def validate_health_framework(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
) -> OpsValidationResult:
    bootstrap_core_tables(connection)
    checks = [
        (
            "health_snapshot_exists",
            bool(
                connection.execute("SELECT COUNT(*) FROM fact_health_snapshot").fetchone()[0]
            ),
        ),
        (
            "dependency_state_exists",
            bool(
                connection.execute(
                    "SELECT COUNT(*) FROM fact_pipeline_dependency_state"
                ).fetchone()[0]
            ),
        ),
        (
            "disk_watermark_event_exists",
            bool(
                connection.execute("SELECT COUNT(*) FROM fact_disk_watermark_event").fetchone()[0]
            ),
        ),
    ]
    warning_count = sum(1 for _, passed in checks if not passed)
    return OpsValidationResult(
        run_id=job_run_id or "embedded",
        check_count=len(checks),
        warning_count=warning_count,
        notes=f"Health framework validated. warnings={warning_count}",
    )


def validate_ops_framework(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date,
    job_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsValidationResult:
    bootstrap_core_tables(connection)
    resolved = load_active_or_default_ops_policy(
        settings,
        connection,
        as_of_at=utc_now(),
        policy_config_path=policy_config_path,
    )
    active_policy_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_active_ops_policy
            WHERE active_flag = TRUE
              AND effective_from_at <= ?
              AND (effective_to_at IS NULL OR effective_to_at >= ?)
            """,
            [utc_now(), utc_now()],
        ).fetchone()[0]
    )
    checks = [
        ("resolved_policy_id", bool(resolved.policy.policy_id)),
        ("active_policy_overlap", active_policy_count <= 1),
        (
            "recovery_table_exists",
            bool(
                connection.execute("SELECT COUNT(*) FROM fact_recovery_action").fetchone()[0]
                >= 0
            ),
        ),
        (
            "lock_table_exists",
            bool(connection.execute("SELECT COUNT(*) FROM fact_active_lock").fetchone()[0] >= 0),
        ),
        (
            "job_run_table_exists",
            bool(connection.execute("SELECT COUNT(*) FROM fact_job_run").fetchone()[0] >= 0),
        ),
        (
            "step_run_table_exists",
            bool(connection.execute("SELECT COUNT(*) FROM fact_job_step_run").fetchone()[0] >= 0),
        ),
    ]
    warning_count = sum(1 for _, passed in checks if not passed)
    return OpsValidationResult(
        run_id=job_run_id or "embedded",
        check_count=len(checks),
        warning_count=warning_count,
        notes=(
            "Ops framework validated. "
            f"warnings={warning_count} as_of_date={as_of_date.isoformat()}"
        ),
    )
