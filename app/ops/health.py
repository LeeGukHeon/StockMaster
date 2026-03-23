from __future__ import annotations

from datetime import date

import duckdb

from app.common.disk import DiskWatermark, measure_disk_usage
from app.common.time import now_local, utc_now
from app.ops.common import AlertSeverity, JobStatus, OpsJobResult
from app.ops.policy import load_active_or_default_ops_policy
from app.ops.scheduler import expected_job_reference_date
from app.ops.repository import (
    insert_health_snapshot_rows,
    insert_pipeline_dependency_rows,
    json_text,
    sync_open_alert_event,
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


def _date_dependency_status(
    *,
    latest_date: date | None,
    required_date: date | None,
    optional: bool = False,
) -> tuple[str, bool, str, dict[str, object]]:
    ready = latest_date is not None and (required_date is None or latest_date >= required_date)
    lag_days = 0
    if latest_date is not None and required_date is not None and latest_date < required_date:
        lag_days = (required_date - latest_date).days
    if latest_date is None:
        observed_state = (
            "missing"
            if required_date is None
            else f"missing (required>={required_date.isoformat()})"
        )
    elif required_date is None:
        observed_state = str(latest_date)
    else:
        observed_state = (
            f"latest={latest_date.isoformat()} required={required_date.isoformat()} lag_days={lag_days}"
        )
    status = JobStatus.SUCCESS if ready else (
        JobStatus.DEGRADED_SUCCESS if optional else JobStatus.BLOCKED
    )
    details = {
        "latest_date": str(latest_date) if latest_date is not None else None,
        "required_date": str(required_date) if required_date is not None else None,
        "lag_days": lag_days,
    }
    return status, ready, observed_state, details


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
    expected_daily_close_date = expected_job_reference_date(
        settings,
        job_key="daily_close",
        as_of_date=as_of_date,
        now_ts=checked_at,
        connection=connection,
    )
    expected_evaluation_date = expected_job_reference_date(
        settings,
        job_key="evaluation",
        as_of_date=as_of_date,
        now_ts=checked_at,
        connection=connection,
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
    calendar_status, calendar_ready, calendar_observed_state, calendar_details = _date_dependency_status(
        latest_date=latest_calendar,
        required_date=expected_daily_close_date,
    )
    selection_status, selection_ready, selection_observed_state, selection_details = _date_dependency_status(
        latest_date=latest_selection,
        required_date=expected_daily_close_date,
    )
    prediction_status, prediction_ready, prediction_observed_state, prediction_details = _date_dependency_status(
        latest_date=latest_prediction,
        required_date=expected_daily_close_date,
    )
    evaluation_status, evaluation_ready, evaluation_observed_state, evaluation_details = _date_dependency_status(
        latest_date=latest_evaluation,
        required_date=expected_evaluation_date,
    )
    nav_status, nav_ready, nav_observed_state, nav_details = _date_dependency_status(
        latest_date=latest_nav,
        required_date=expected_evaluation_date,
    )
    target_status, target_ready, target_observed_state, target_details = _date_dependency_status(
        latest_date=latest_portfolio_target,
        required_date=expected_daily_close_date,
        optional=True,
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
            "status": calendar_status,
            "ready_flag": calendar_ready,
            "required_state": (
                "dim_trading_calendar available and current through "
                f"{expected_daily_close_date}"
                if expected_daily_close_date is not None
                else "dim_trading_calendar available"
            ),
            "observed_state": calendar_observed_state,
            "as_of_date": as_of_date,
            "details_json": json_text(
                {
                    "expected_job_key": "daily_close",
                    **calendar_details,
                }
            ),
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
            "status": selection_status,
            "ready_flag": selection_ready,
            "required_state": (
                "selection_engine_v2 ranking available through "
                f"{expected_daily_close_date}"
                if expected_daily_close_date is not None
                else "selection_engine_v2 ranking available"
            ),
            "observed_state": selection_observed_state,
            "as_of_date": as_of_date,
            "details_json": json_text(
                {
                    "expected_job_key": "daily_close",
                    **selection_details,
                }
            ),
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
            "status": prediction_status,
            "ready_flag": prediction_ready,
            "required_state": (
                "prediction snapshot available through "
                f"{expected_daily_close_date}"
                if expected_daily_close_date is not None
                else "prediction snapshot available"
            ),
            "observed_state": prediction_observed_state,
            "as_of_date": as_of_date,
            "details_json": json_text(
                {
                    "expected_job_key": "daily_close",
                    **prediction_details,
                }
            ),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_evaluation_bundle",
            "dependency_name": "evaluation_ready",
            "status": evaluation_status,
            "ready_flag": evaluation_ready,
            "required_state": (
                "evaluation summary available through "
                f"{expected_evaluation_date}"
                if expected_evaluation_date is not None
                else "evaluation summary available"
            ),
            "observed_state": evaluation_observed_state,
            "as_of_date": as_of_date,
            "details_json": json_text(
                {
                    "expected_job_key": "evaluation",
                    **evaluation_details,
                }
            ),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "daily_evaluation_bundle",
            "dependency_name": "portfolio_nav_ready",
            "status": nav_status,
            "ready_flag": nav_ready,
            "required_state": (
                "portfolio nav available through "
                f"{expected_evaluation_date}"
                if expected_evaluation_date is not None
                else "portfolio nav available"
            ),
            "observed_state": nav_observed_state,
            "as_of_date": as_of_date,
            "details_json": json_text(
                {
                    "expected_job_key": "evaluation",
                    **nav_details,
                }
            ),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
        {
            "checked_at": checked_at,
            "pipeline_name": "ops_maintenance_bundle",
            "dependency_name": "portfolio_target_ready",
            "status": target_status,
            "ready_flag": target_ready,
            "required_state": (
                "portfolio target book optional, current through "
                f"{expected_daily_close_date}"
                if expected_daily_close_date is not None
                else "portfolio target book optional"
            ),
            "observed_state": target_observed_state,
            "as_of_date": as_of_date,
            "details_json": json_text(
                {
                    "expected_job_key": "daily_close",
                    **target_details,
                }
            ),
            "job_run_id": job_run_id,
            "created_at": checked_at,
        },
    ]
    insert_pipeline_dependency_rows(connection, rows)
    ready_count = sum(1 for row in rows if bool(row["ready_flag"]))
    blocked_count = sum(1 for row in rows if row["status"] == JobStatus.BLOCKED)
    degraded_count = sum(1 for row in rows if row["status"] == JobStatus.DEGRADED_SUCCESS)
    status = JobStatus.SUCCESS
    if blocked_count > 0:
        status = JobStatus.BLOCKED
    elif degraded_count > 0:
        status = JobStatus.DEGRADED_SUCCESS
    notes = (
        f"Dependency readiness materialized. rows={len(rows)} ready={ready_count} "
        f"blocked={blocked_count} degraded={degraded_count}"
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="check_pipeline_dependencies",
        status=status,
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
              AND NOT regexp_matches(
                  COALESCE(details_json, ''),
                  '"cleanup_recovered"\\s*:\\s*true'
              )
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
              AND NOT regexp_matches(
                  COALESCE(details_json, ''),
                  '"cleanup_recovered"\\s*:\\s*true'
              )
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
    blocked_dependency_count = int(
        _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM vw_latest_pipeline_dependency_state
            WHERE status = 'BLOCKED'
            """,
        )
        or 0
    )
    degraded_dependency_count = int(
        _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM vw_latest_pipeline_dependency_state
            WHERE status = 'DEGRADED_SUCCESS'
            """,
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
    alert_specs = [
        {
            "alert_type": "DISK_WATERMARK",
            "severity": (
                AlertSeverity.CRITICAL
                if disk_report.status == DiskWatermark.LIMIT
                else AlertSeverity.WARNING
            ),
            "message": f"Disk watermark reached: {disk_report.status}",
            "details": {"usage_ratio": disk_report.usage_ratio},
            "active": disk_report.status != DiskWatermark.NORMAL,
        },
        {
            "alert_type": "STALE_LOCK",
            "severity": AlertSeverity.WARNING,
            "message": "One or more stale locks require intervention.",
            "details": {"stale_lock_count": stale_lock_count},
            "active": stale_lock_count > 0,
        },
        {
            "alert_type": "FAILED_RUNS_24H",
            "severity": AlertSeverity.WARNING,
            "message": "Recent failed jobs were detected.",
            "details": {"failed_run_count_24h": failed_24h},
            "active": failed_24h > 0,
        },
    ]
    inserted_alert_count = 0
    for spec in alert_specs:
        inserted_alert_count += int(
            sync_open_alert_event(
            connection,
            alert_id=f"alert-{spec['alert_type'].lower()}-{utc_now().strftime('%Y%m%dT%H%M%S%f')}",
            created_at=snapshot_at,
            alert_type=str(spec["alert_type"]),
            severity=str(spec["severity"]),
            component_name="health",
            message=str(spec["message"]),
            details=spec["details"],
            job_run_id=job_run_id,
            active=bool(spec["active"]),
            )
        )
    open_alert_count = int(
        _scalar(
            connection,
            "SELECT COUNT(*) FROM fact_alert_event WHERE status = 'OPEN'",
        )
        or 0
    )
    if disk_report.status == DiskWatermark.LIMIT or stale_lock_count > 0:
        overall_status = JobStatus.FAILED
    elif (
        failed_24h > 0
        or open_alert_count > 0
        or blocked_dependency_count > 0
        or degraded_dependency_count > 0
        or disk_report.status != DiskWatermark.NORMAL
    ):
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
            "blocked_dependency_count": blocked_dependency_count,
            "degraded_dependency_count": degraded_dependency_count,
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
    notes = (
        f"Health snapshots materialized. rows={len(rows)} "
        f"alerts_open={open_alert_count} alerts_inserted={inserted_alert_count}"
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="materialize_health_snapshots",
        status=overall_status,
        notes=notes,
        row_count=len(rows),
    )
