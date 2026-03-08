from __future__ import annotations

from datetime import date

from app.common.time import today_local
from app.ops.common import OpsJobResult, TriggerType
from app.ops.health import check_pipeline_dependencies, materialize_health_snapshots
from app.ops.maintenance import (
    cleanup_disk_watermark,
    reconcile_failed_runs,
    recover_incomplete_runs,
    rotate_and_compress_logs,
    summarize_storage_usage,
)
from app.ops.report import publish_discord_ops_alerts
from app.ops.runtime import JobRunContext, job_result_from_context
from app.portfolio.allocation import (
    evaluate_portfolio_policies,
    materialize_portfolio_nav,
    materialize_portfolio_position_snapshots,
    materialize_portfolio_rebalance_plan,
    materialize_portfolio_target_book,
)
from app.portfolio.candidate_book import (
    build_portfolio_candidate_book,
    validate_portfolio_candidate_book,
)
from app.portfolio.report import render_portfolio_report
from app.scheduler.jobs import run_daily_pipeline_job, run_evaluation_job
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection


def _resolve_pipeline_date(settings: Settings, *, fallback: date | None = None) -> date:
    target_date = fallback or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            """
            SELECT trading_date
            FROM dim_trading_calendar
            WHERE trading_date <= ?
              AND is_trading_day
            ORDER BY trading_date DESC
            LIMIT 1
            """,
            [target_date],
        ).fetchone()
    return row[0] if row and row[0] is not None else target_date


def _resolve_latest_selection_date(settings: Settings, *, fallback: date | None = None) -> date:
    target_date = fallback or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            """
            SELECT MAX(as_of_date)
            FROM fact_ranking
            WHERE ranking_version = 'selection_engine_v2'
            """,
        ).fetchone()
    return row[0] if row and row[0] is not None else target_date


def _resolve_recent_start_date(settings: Settings, *, end_date: date, trading_days: int) -> date:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            """
            SELECT MIN(trading_date)
            FROM (
                SELECT trading_date
                FROM dim_trading_calendar
                WHERE trading_date <= ?
                  AND is_trading_day
                ORDER BY trading_date DESC
                LIMIT ?
            )
            """,
            [end_date, trading_days],
        ).fetchone()
    return row[0] if row and row[0] is not None else end_date


def run_daily_research_pipeline(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    target_date = _resolve_pipeline_date(settings, fallback=as_of_date)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name="run_daily_research_pipeline",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            notes=f"Daily research bundle for {target_date.isoformat()}",
        ) as job:
            if dry_run:
                job.skip("Dry-run: research pipeline step skipped.")
            else:
                job.run_step("daily_pipeline", run_daily_pipeline_job, settings)
            job.run_step(
                "check_pipeline_dependencies",
                check_pipeline_dependencies,
                settings,
                connection=connection,
                as_of_date=target_date,
                job_run_id=job.run_id,
                policy_config_path=policy_config_path,
                critical=False,
            )
            job.run_step(
                "materialize_health_snapshots",
                materialize_health_snapshots,
                settings,
                connection=connection,
                as_of_date=target_date,
                job_run_id=job.run_id,
                policy_config_path=policy_config_path,
                critical=False,
            )
            notes = f"Daily research bundle completed for {target_date.isoformat()}."
            return job_result_from_context(job, notes=notes)


def run_daily_post_close_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    target_date = _resolve_latest_selection_date(settings, fallback=as_of_date)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name="run_daily_post_close_bundle",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            notes=f"Daily post-close bundle for {target_date.isoformat()}",
        ) as job:
            if dry_run:
                job.skip("Dry-run: portfolio materialization steps skipped.")
            else:
                job.run_step(
                    "build_portfolio_candidate_book",
                    build_portfolio_candidate_book,
                    settings,
                    as_of_date=target_date,
                    policy_config_path=policy_config_path,
                )
                job.run_step(
                    "validate_portfolio_candidate_book",
                    validate_portfolio_candidate_book,
                    settings,
                    as_of_date=target_date,
                )
                job.run_step(
                    "materialize_portfolio_target_book",
                    materialize_portfolio_target_book,
                    settings,
                    as_of_date=target_date,
                    policy_config_path=policy_config_path,
                )
                plan_result = job.run_step(
                    "materialize_portfolio_rebalance_plan",
                    materialize_portfolio_rebalance_plan,
                    settings,
                    as_of_date=target_date,
                    policy_config_path=policy_config_path,
                )
                job.run_step(
                    "materialize_portfolio_position_snapshots",
                    materialize_portfolio_position_snapshots,
                    settings,
                    as_of_date=target_date,
                    policy_config_path=policy_config_path,
                )
                session_row = connection.execute(
                    """
                    SELECT MAX(session_date)
                    FROM fact_portfolio_rebalance_plan
                    WHERE as_of_date = ?
                    """,
                    [target_date],
                ).fetchone()
                if plan_result is not None and session_row and session_row[0] is not None:
                    session_date = session_row[0]
                    job.run_step(
                        "materialize_portfolio_nav",
                        materialize_portfolio_nav,
                        settings,
                        start_date=session_date,
                        end_date=session_date,
                        policy_config_path=policy_config_path,
                    )
                job.run_step(
                    "render_portfolio_report",
                    render_portfolio_report,
                    settings,
                    as_of_date=target_date,
                    dry_run=True,
                    critical=False,
                )
            job.run_step(
                "materialize_health_snapshots",
                materialize_health_snapshots,
                settings,
                connection=connection,
                as_of_date=target_date,
                job_run_id=job.run_id,
                policy_config_path=policy_config_path,
                critical=False,
            )
            return job_result_from_context(
                job,
                notes=f"Daily post-close bundle completed for {target_date.isoformat()}.",
            )


def run_daily_evaluation_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    target_date = _resolve_latest_selection_date(settings, fallback=as_of_date)
    start_date = _resolve_recent_start_date(settings, end_date=target_date, trading_days=60)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name="run_daily_evaluation_bundle",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            notes=f"Daily evaluation bundle through {target_date.isoformat()}",
        ) as job:
            if dry_run:
                job.skip("Dry-run: evaluation bundle steps skipped.")
            else:
                job.run_step("evaluation_pipeline", run_evaluation_job, settings)
                job.run_step(
                    "evaluate_portfolio_policies",
                    evaluate_portfolio_policies,
                    settings,
                    start_date=start_date,
                    end_date=target_date,
                    policy_config_path=policy_config_path,
                    critical=False,
                )
            job.run_step(
                "materialize_health_snapshots",
                materialize_health_snapshots,
                settings,
                connection=connection,
                as_of_date=target_date,
                job_run_id=job.run_id,
                policy_config_path=policy_config_path,
                critical=False,
            )
            return job_result_from_context(
                job,
                notes=f"Daily evaluation bundle completed for {target_date.isoformat()}.",
            )


def run_ops_maintenance_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    target_date = as_of_date or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name="run_ops_maintenance_bundle",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            notes=f"Ops maintenance bundle for {target_date.isoformat()}",
        ) as job:
            job.run_step(
                "check_pipeline_dependencies",
                check_pipeline_dependencies,
                settings,
                connection=connection,
                as_of_date=target_date,
                job_run_id=job.run_id,
                policy_config_path=policy_config_path,
            )
            job.run_step(
                "materialize_health_snapshots",
                materialize_health_snapshots,
                settings,
                connection=connection,
                as_of_date=target_date,
                job_run_id=job.run_id,
                policy_config_path=policy_config_path,
            )
            job.run_step(
                "summarize_storage_usage",
                summarize_storage_usage,
                settings,
                critical=False,
            )
            job.run_step(
                "rotate_and_compress_logs",
                rotate_and_compress_logs,
                settings,
                dry_run=dry_run,
                critical=False,
            )
            job.run_step(
                "cleanup_disk_watermark",
                cleanup_disk_watermark,
                settings,
                connection=connection,
                job_run_id=job.run_id,
                dry_run=dry_run,
                policy_config_path=policy_config_path,
                critical=False,
            )
            job.run_step(
                "reconcile_failed_runs",
                reconcile_failed_runs,
                settings,
                connection=connection,
                job_run_id=job.run_id,
                critical=False,
            )
            job.run_step(
                "recover_incomplete_runs",
                recover_incomplete_runs,
                settings,
                connection=connection,
                job_run_id=job.run_id,
                dry_run=dry_run,
                critical=False,
            )
            job.run_step(
                "publish_discord_ops_alerts",
                publish_discord_ops_alerts,
                settings,
                connection=connection,
                as_of_date=target_date,
                job_run_id=job.run_id,
                dry_run=True if dry_run else not settings.discord.enabled,
                critical=False,
            )
            return job_result_from_context(
                job,
                notes=f"Ops maintenance bundle completed for {target_date.isoformat()}.",
            )
