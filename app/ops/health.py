from __future__ import annotations

from datetime import date

import duckdb

from app.common.disk import DiskWatermark, measure_disk_usage
from app.common.time import now_local, utc_now
from app.ops.common import AlertSeverity, JobStatus, OpsJobResult
from app.ops.policy import load_active_or_default_ops_policy
from app.ops.repository import (
    insert_alert_event,
    insert_health_snapshot_rows,
    insert_pipeline_dependency_rows,
    json_text,
)
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables


def _scalar(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    params: list[object] | None = None,
) -> object | None:
    row = connection.execute(query, params or []).fetchone()
    if row is None:
        return None
    return row[0]


def check_pipeline_dependencies(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date | None = None,
    job_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    checked_at = now_local(settings.app.timezone)
    resolved = load_active_or_default_ops_policy(
        settings,
        connection,
        as_of_at=checked_at,
        policy_config_path=policy_config_path,
    )
    disk_report = measure_disk_usage(
        settings.paths.data_dir,
        warning_ratio=resolved.policy.warn_ratio,
        prune_ratio=resolved.policy.cleanup_ratio,
        limit_ratio=resolved.policy.emergency_ratio,
    )
    latest_universe = _scalar(connection, "SELECT MAX(as_of_date) FROM dim_symbol")
    latest_calendar = _scalar(
        connection,
        "SELECT MAX(trading_date) FROM dim_trading_calendar WHERE is_trading_day",
    )
    latest_selection = _scalar(
        connection,
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = 'selection_engine_v2'
        """,
    )
    latest_prediction = _scalar(
        connection,
        """
        SELECT MAX(as_of_date)
        FROM fact_prediction
        WHERE ranking_version = 'selection_engine_v2'
        """,
    )
    latest_portfolio_target = _scalar(
        connection,
        "SELECT MAX(as_of_date) FROM fact_portfolio_target_book",
    )
    latest_nav = _scalar(
        connection,
        "SELECT MAX(snapshot_date) FROM fact_portfolio_nav_snapshot",
    )
    latest_evaluation = _scalar(
        connection,
        "SELECT MAX(summary_date) FROM fact_evaluation_summary",
    )
    rows = [
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_research_pipeline",
            "dependency_name": "universe_ready",
            "status": JobStatus.SUCCESS if latest_universe is not None else JobStatus.BLOCKED,
            "ready_flag": latest_universe is not None,
            "required_state": "dim_symbol available",
            "observed_state": str(latest_universe) if latest_universe is not None else "missing",
            "as_of_date": as_of_date,
            "details_json": json_text({"latest_universe_date": latest_universe}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_research_pipeline",
            "dependency_name": "calendar_ready",
            "status": JobStatus.SUCCESS if latest_calendar is not None else JobStatus.BLOCKED,
            "ready_flag": latest_calendar is not None,
            "required_state": "dim_trading_calendar available",
            "observed_state": str(latest_calendar) if latest_calendar is not None else "missing",
            "as_of_date": as_of_date,
            "details_json": json_text({"latest_calendar_date": latest_calendar}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_research_pipeline",
            "dependency_name": "disk_safe",
            "status": JobStatus.SUCCESS
            if disk_report.status != DiskWatermark.LIMIT
            else JobStatus.BLOCKED,
            "ready_flag": disk_report.status != DiskWatermark.LIMIT,
            "required_state": "below emergency watermark",
            "observed_state": str(disk_report.status),
            "as_of_date": as_of_date,
            "details_json": json_text({"usage_ratio": disk_report.usage_ratio}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_post_close_bundle",
            "dependency_name": "selection_v2_ready",
            "status": JobStatus.SUCCESS if latest_selection is not None else JobStatus.BLOCKED,
            "ready_flag": latest_selection is not None,
            "required_state": "selection_engine_v2 ranking available",
            "observed_state": str(latest_selection) if latest_selection is not None else "missing",
            "as_of_date": as_of_date,
            "details_json": json_text({"latest_selection_date": latest_selection}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_post_close_bundle",
            "dependency_name": "portfolio_policy_resolved",
            "status": JobStatus.SUCCESS,
            "ready_flag": True,
            "required_state": "ops policy resolved",
            "observed_state": resolved.policy.policy_id,
            "as_of_date": as_of_date,
            "details_json": json_text({"policy_source": resolved.source}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_evaluation_bundle",
            "dependency_name": "prediction_ready",
            "status": JobStatus.SUCCESS if latest_prediction is not None else JobStatus.BLOCKED,
            "ready_flag": latest_prediction is not None,
            "required_state": "prediction snapshot available",
            "observed_state": (
                str(latest_prediction) if latest_prediction is not None else "missing"
            ),
            "as_of_date": as_of_date,
            "details_json": json_text({"latest_prediction_date": latest_prediction}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_evaluation_bundle",
            "dependency_name": "evaluation_ready",
            "status": JobStatus.SUCCESS if latest_evaluation is not None else JobStatus.BLOCKED,
            "ready_flag": latest_evaluation is not None,
            "required_state": "evaluation summary available",
            "observed_state": (
                str(latest_evaluation) if latest_evaluation is not None else "missing"
            ),
            "as_of_date": as_of_date,
            "details_json": json_text({"latest_evaluation_date": latest_evaluation}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_evaluation_bundle",
            "dependency_name": "portfolio_nav_ready",
            "status": JobStatus.SUCCESS if latest_nav is not None else JobStatus.BLOCKED,
            "ready_flag": latest_nav is not None,
            "required_state": "portfolio nav available",
            "observed_state": str(latest_nav) if latest_nav is not None else "missing",
            "as_of_date": as_of_date,
            "details_json": json_text({"latest_nav_date": latest_nav}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "ops_maintenance_bundle",
            "dependency_name": "portfolio_target_ready",
            "status": (
                JobStatus.SUCCESS
                if latest_portfolio_target is not None
                else JobStatus.DEGRADED_SUCCESS
            ),
            "ready_flag": latest_portfolio_target is not None,
            "required_state": "portfolio target book optional",
            "observed_state": (
                str(latest_portfolio_target)
                if latest_portfolio_target is not None
                else "missing"
            ),
            "as_of_date": as_of_date,
            "details_json": json_text({"latest_target_date": latest_portfolio_target}),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
    ]
    insert_pipeline_dependency_rows(connection, rows)
    ready_count = sum(1 for row in rows if bool(row["ready_flag"]))
    notes = f"Dependency readiness materialized. rows={len(rows)} ready={ready_count}"
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="check_pipeline_dependencies",
        status=JobStatus.SUCCESS,
        notes=notes,
        row_count=len(rows),
    )


def materialize_health_snapshots(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date | None = None,
    job_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    snapshot_at = now_local(settings.app.timezone)
    resolved = load_active_or_default_ops_policy(
        settings,
        connection,
        as_of_at=snapshot_at,
        policy_config_path=policy_config_path,
    )
    disk_report = measure_disk_usage(
        settings.paths.data_dir,
        warning_ratio=resolved.policy.warn_ratio,
        prune_ratio=resolved.policy.cleanup_ratio,
        limit_ratio=resolved.policy.emergency_ratio,
    )
    failed_24h = int(
        _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM fact_job_run
            WHERE status = 'FAILED'
              AND started_at >= NOW() - INTERVAL '24 hours'
            """,
        )
        or 0
    )
    failed_7d = int(
        _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM fact_job_run
            WHERE status = 'FAILED'
              AND started_at >= NOW() - INTERVAL '7 days'
            """,
        )
        or 0
    )
    active_lock_count = int(
        _scalar(
            connection,
            "SELECT COUNT(*) FROM fact_active_lock WHERE released_at IS NULL",
        )
        or 0
    )
    stale_lock_count = int(
        _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM fact_active_lock
            WHERE released_at IS NULL
              AND expires_at < NOW()
            """,
        )
        or 0
    )
    open_alert_count = int(
        _scalar(
            connection,
            "SELECT COUNT(*) FROM fact_alert_event WHERE status = 'OPEN'",
        )
        or 0
    )
    latest_report_date = _scalar(
        connection,
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = 'selection_engine_v2'
        """,
    )
    latest_evaluation_date = _scalar(
        connection,
        "SELECT MAX(summary_date) FROM fact_evaluation_summary",
    )
    latest_target_date = _scalar(
        connection,
        "SELECT MAX(as_of_date) FROM fact_portfolio_target_book",
    )
    latest_nav_date = _scalar(
        connection,
        "SELECT MAX(snapshot_date) FROM fact_portfolio_nav_snapshot",
    )
    if disk_report.status == DiskWatermark.LIMIT or stale_lock_count > 0:
        overall_status = JobStatus.FAILED
    elif failed_24h > 0 or open_alert_count > 0 or disk_report.status != DiskWatermark.NORMAL:
        overall_status = JobStatus.DEGRADED_SUCCESS
    else:
        overall_status = JobStatus.SUCCESS
    rows = [
        {
            "snapshot_at": snapshot_at,
            "health_scope": "overall",
            "component_name": "platform",
            "status": overall_status,
            "metric_name": metric_name,
            "metric_value_double": (
                metric_value if isinstance(metric_value, (int, float)) else None
            ),
            "metric_value_text": (
                None if isinstance(metric_value, (int, float)) else str(metric_value)
            ),
            "as_of_date": as_of_date,
            "details_json": None,
            "job_run_id": job_run_id,
            "created_at": snapshot_at,
        }
        for metric_name, metric_value in {
            "failed_run_count_24h": failed_24h,
            "failed_run_count_7d": failed_7d,
            "active_lock_count": active_lock_count,
            "stale_lock_count": stale_lock_count,
            "open_alert_count": open_alert_count,
            "disk_usage_ratio": round(disk_report.usage_ratio, 6),
            "latest_daily_report_date": latest_report_date,
            "latest_evaluation_date": latest_evaluation_date,
            "latest_portfolio_target_date": latest_target_date,
            "latest_portfolio_nav_date": latest_nav_date,
            "disk_watermark": str(disk_report.status),
        }.items()
    ]
    rows.extend(
        [
            {
                "snapshot_at": snapshot_at,
                "health_scope": "pipeline",
                "component_name": component_name,
                "status": (
                    JobStatus.SUCCESS
                    if latest_value is not None
                    else JobStatus.DEGRADED_SUCCESS
                ),
                "metric_name": "latest_successful_output",
                "metric_value_double": None,
                "metric_value_text": str(latest_value) if latest_value is not None else "missing",
                "as_of_date": as_of_date,
                "details_json": None,
                "job_run_id": job_run_id,
                "created_at": snapshot_at,
            }
            for component_name, latest_value in {
                "daily_report": latest_report_date,
                "evaluation_summary": latest_evaluation_date,
                "portfolio_target_book": latest_target_date,
                "portfolio_nav_snapshot": latest_nav_date,
            }.items()
        ]
    )
    insert_health_snapshot_rows(connection, rows)
    alerts: list[tuple[str, str, str, dict[str, object]]] = []
    if disk_report.status != DiskWatermark.NORMAL:
        alerts.append(
            (
                "DISK_WATERMARK",
                (
                    AlertSeverity.CRITICAL
                    if disk_report.status == DiskWatermark.LIMIT
                    else AlertSeverity.WARNING
                ),
                f"Disk watermark reached: {disk_report.status}",
                {"usage_ratio": disk_report.usage_ratio},
            )
        )
    if stale_lock_count > 0:
        alerts.append(
            (
                "STALE_LOCK",
                AlertSeverity.WARNING,
                "One or more stale locks require intervention.",
                {"stale_lock_count": stale_lock_count},
            )
        )
    if failed_24h > 0:
        alerts.append(
            (
                "FAILED_RUNS_24H",
                AlertSeverity.WARNING,
                "Recent failed jobs were detected.",
                {"failed_run_count_24h": failed_24h},
            )
        )
    for alert_type, severity, message, details in alerts:
        insert_alert_event(
            connection,
            alert_id=f"alert-{alert_type.lower()}-{utc_now().strftime('%Y%m%dT%H%M%S%f')}",
            created_at=snapshot_at,
            alert_type=alert_type,
            severity=severity,
            component_name="health",
            status="OPEN",
            message=message,
            details=details,
            job_run_id=job_run_id,
        )
    notes = f"Health snapshots materialized. rows={len(rows)} alerts={len(alerts)}"
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="materialize_health_snapshots",
        status=overall_status,
        notes=notes,
        row_count=len(rows),
    )
