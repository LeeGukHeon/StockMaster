from __future__ import annotations

from datetime import date

from app.audit.checks import run_artifact_reference_checks, run_latest_layer_checks
from app.common.time import today_local
from app.intraday.adjusted_decisions import materialize_intraday_adjusted_entry_decisions
from app.intraday.data import (
    backfill_intraday_candidate_bars,
    backfill_intraday_candidate_quote_summary,
    backfill_intraday_candidate_trade_summary,
)
from app.intraday.decisions import materialize_intraday_entry_decisions
from app.intraday.meta_inference import (
    evaluate_intraday_meta_models,
    materialize_intraday_final_actions,
    materialize_intraday_meta_predictions,
)
from app.intraday.meta_report import (
    publish_discord_intraday_meta_summary,
    render_intraday_meta_model_report,
)
from app.intraday.meta_training import (
    calibrate_intraday_meta_thresholds,
    run_intraday_meta_walkforward,
    train_intraday_meta_models,
)
from app.intraday.policy import (
    evaluate_intraday_policy_ablation,
    materialize_intraday_policy_candidates,
    materialize_intraday_policy_recommendations,
    run_intraday_policy_calibration,
    run_intraday_policy_walkforward,
)
from app.intraday.policy_report import (
    publish_discord_intraday_policy_summary,
    render_intraday_policy_research_report,
)
from app.intraday.postmortem import (
    publish_discord_intraday_postmortem,
    render_intraday_postmortem_report,
)
from app.intraday.research_mode import (
    intraday_research_feature_flags,
    materialize_intraday_research_capability,
)
from app.intraday.session import materialize_intraday_candidate_session
from app.intraday.signals import materialize_intraday_signal_snapshots
from app.intraday.strategy import materialize_intraday_decision_outcomes
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ops.common import OpsJobResult, TriggerType
from app.ops.health import check_pipeline_dependencies, materialize_health_snapshots
from app.ops.maintenance import (
    cleanup_disk_watermark,
    cleanup_docker_build_cache,
    cleanup_model_artifacts,
    cleanup_stale_job_runs,
    reconcile_failed_runs,
    recover_incomplete_runs,
    reset_open_recovery_actions,
    rotate_and_compress_logs,
    summarize_storage_usage,
)
from app.ops.report import publish_discord_ops_alerts
from app.ops.runtime import JobRunContext, job_result_from_context
from app.ops.scheduler import (
    DEFAULT_INTRADAY_CHECKPOINTS,
    bundle_already_completed,
    expected_job_reference_date,
    is_trading_day,
    resolve_due_intraday_checkpoint,
    resolve_news_collection_dates,
    resolve_previous_trading_date,
    resolve_reference_trading_date,
)
from app.pipelines.news_metadata import sync_news_metadata
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
from app.release.reporting import (
    render_daily_research_report,
    render_evaluation_report,
    render_intraday_summary_report,
    render_release_candidate_checklist,
)
from app.release.snapshot import (
    build_latest_app_snapshot,
    build_report_index,
    build_ui_freshness_snapshot,
)
from app.release.validation import validate_release_candidate
from app.reports.close_brief import publish_discord_close_brief
from app.reports.discord_eod import publish_discord_eod_report
from app.scheduler.jobs import run_daily_pipeline_job, run_evaluation_job
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection

DEFAULT_HORIZONS: tuple[int, ...] = (1, 5)
SCHEDULER_GLOBAL_LOCK = "scheduler_global_write"
WEEKLY_INTRADAY_REQUIRED_SESSIONS = 40 + 10 + 10 + max(DEFAULT_HORIZONS)
WEEKLY_INTRADAY_OUTCOME_CHUNK_DAYS = 7


def _resolve_pipeline_date(
    settings: Settings,
    *,
    fallback: date | None = None,
    connection=None,
) -> date:
    target_date = fallback or today_local(settings.app.timezone)
    if connection is not None:
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
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as read_connection:
        bootstrap_core_tables(read_connection)
        return _resolve_pipeline_date(
            settings,
            fallback=target_date,
            connection=read_connection,
        )


def _resolve_latest_selection_date(
    settings: Settings,
    *,
    fallback: date | None = None,
    connection=None,
) -> date:
    target_date = fallback or today_local(settings.app.timezone)
    if connection is not None:
        row = connection.execute(
            """
            SELECT MAX(as_of_date)
            FROM fact_ranking
            WHERE ranking_version = 'selection_engine_v2'
            """,
        ).fetchone()
        return row[0] if row and row[0] is not None else target_date
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as read_connection:
        bootstrap_core_tables(read_connection)
        return _resolve_latest_selection_date(
            settings,
            fallback=target_date,
            connection=read_connection,
        )


def _resolve_recent_start_date(
    settings: Settings,
    *,
    end_date: date,
    trading_days: int,
    connection=None,
) -> date:
    if connection is not None:
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
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as read_connection:
        bootstrap_core_tables(read_connection)
        return _resolve_recent_start_date(
            settings,
            end_date=end_date,
            trading_days=trading_days,
            connection=read_connection,
        )


def _resolve_intraday_session_start_date(
    settings: Settings,
    *,
    end_date: date,
    required_sessions: int,
    connection=None,
) -> date:
    if connection is not None:
        row = connection.execute(
            """
            SELECT MIN(session_date)
            FROM (
                SELECT DISTINCT session_date
                FROM fact_intraday_adjusted_entry_decision
                WHERE session_date <= ?
                ORDER BY session_date DESC
                LIMIT ?
            )
            """,
            [end_date, required_sessions],
        ).fetchone()
        if row and row[0] is not None:
            return row[0]
        return _resolve_recent_start_date(
            settings,
            end_date=end_date,
            trading_days=required_sessions,
            connection=connection,
        )
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as read_connection:
        bootstrap_core_tables(read_connection)
        return _resolve_intraday_session_start_date(
            settings,
            end_date=end_date,
            required_sessions=required_sessions,
            connection=read_connection,
        )


def _session_date_chunks(
    connection,
    *,
    start_date: date,
    end_date: date,
    chunk_size: int,
) -> list[tuple[date, date]]:
    rows = connection.execute(
        """
        SELECT DISTINCT session_date
        FROM fact_intraday_candidate_session
        WHERE session_date BETWEEN ? AND ?
        ORDER BY session_date
        """,
        [start_date, end_date],
    ).fetchall()
    session_dates = [row[0] for row in rows if row and row[0] is not None]
    if not session_dates:
        return [(start_date, end_date)]
    effective_chunk_size = max(1, int(chunk_size))
    return [
        (session_dates[index], session_dates[min(index + effective_chunk_size - 1, len(session_dates) - 1)])
        for index in range(0, len(session_dates), effective_chunk_size)
    ]


def _materialize_intraday_decision_outcome_chunks(
    job: JobRunContext,
    *,
    settings: Settings,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    chunk_size: int = WEEKLY_INTRADAY_OUTCOME_CHUNK_DAYS,
) -> None:
    chunks = _session_date_chunks(
        job.connection,
        start_date=start_session_date,
        end_date=end_session_date,
        chunk_size=chunk_size,
    )
    horizon_label = "x".join(str(value) for value in horizons)
    for index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        job.run_step(
            f"materialize_intraday_decision_outcomes_h{horizon_label}_chunk{index:02d}",
            materialize_intraday_decision_outcomes,
            settings,
            start_session_date=chunk_start,
            end_session_date=chunk_end,
            horizons=horizons,
            critical=False,
        )


def _latest_table_date(
    connection,
    *,
    table_name: str,
    column_name: str,
    where_clause: str | None = None,
    params: list[object] | None = None,
):
    query = f"SELECT MAX({column_name}) FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    row = connection.execute(query, params or []).fetchone()
    return row[0] if row and row[0] is not None else None


def _block_if_required_snapshot_missing(
    job: JobRunContext,
    *,
    target_date: date,
    table_name: str,
    column_name: str,
    where_clause: str | None,
    params: list[object] | None,
    label: str,
) -> OpsJobResult | None:
    latest_date = _latest_table_date(
        job.connection,
        table_name=table_name,
        column_name=column_name,
        where_clause=where_clause,
        params=params,
    )
    if latest_date is not None and latest_date >= target_date:
        return None
    latest_label = str(latest_date) if latest_date is not None else "missing"
    note = (
        f"{job.job_name}: required snapshot '{label}' is stale. "
        f"required_date={target_date.isoformat()} latest_date={latest_label}."
    )
    job.block(note)
    return job_result_from_context(job, notes=note, row_count=0)


def _skip_if_non_trading_day(
    job: JobRunContext,
    *,
    target_date: date,
    label: str,
) -> OpsJobResult | None:
    if is_trading_day(job.settings, target_date=target_date, connection=job.connection):
        return None
    note = f"{label}: non-trading day self-skip for {target_date.isoformat()}."
    job.skip(note, status="SKIPPED_NON_TRADING_DAY")
    return job_result_from_context(job, notes=note, row_count=0)


def _skip_if_already_completed(
    job: JobRunContext,
    *,
    bundle_phase: str,
    checkpoint_time: str | None = None,
    profile: str | None = None,
) -> OpsJobResult | None:
    if job.dry_run:
        return None
    if bundle_already_completed(
        job.connection,
        job_name=job.job_name,
        as_of_date=job.as_of_date,
        bundle_phase=bundle_phase,
        checkpoint_time=checkpoint_time,
        profile=profile,
    ):
        note = (
            f"{job.job_name}: already completed for as_of_date={job.as_of_date} "
            f"phase={bundle_phase} checkpoint={checkpoint_time or '-'} profile={profile or '-'}."
        )
        job.skip(note, status="SKIPPED_ALREADY_DONE")
        return job_result_from_context(job, notes=note, row_count=0)
    return None


def _skip_if_intraday_feature_disabled(
    job: JobRunContext,
    *,
    feature_slug: str,
) -> OpsJobResult | None:
    flags = intraday_research_feature_flags(job.settings)
    if flags.get(feature_slug, False):
        return None
    note = (
        f"{job.job_name}: intraday research feature '{feature_slug}' is disabled for "
        f"env={job.settings.app.env}."
    )
    job.skip(note, status="SKIPPED")
    return job_result_from_context(job, notes=note, row_count=0)


def _refresh_release_views(
    job: JobRunContext,
    *,
    settings: Settings,
    connection,
    as_of_date: date,
) -> None:
    job.run_step(
        "build_report_index",
        build_report_index,
        settings,
        connection=connection,
        job_run_id=job.run_id,
        critical=False,
    )
    job.run_step(
        "build_ui_freshness_snapshot",
        build_ui_freshness_snapshot,
        settings,
        connection=connection,
        job_run_id=job.run_id,
        critical=False,
    )
    job.run_step(
        "build_latest_app_snapshot",
        build_latest_app_snapshot,
        settings,
        connection=connection,
        as_of_date=as_of_date,
        job_run_id=job.run_id,
        critical=False,
    )


def run_daily_research_pipeline(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    run_training: bool = True,
    publish_discord: bool = True,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    target_date = _resolve_pipeline_date(settings, fallback=as_of_date)
    should_publish_discord = publish_discord and trigger_type != TriggerType.RECOVERY
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
                job.run_step(
                    "daily_pipeline",
                    run_daily_pipeline_job,
                    settings,
                    pipeline_date=target_date,
                    run_training=run_training,
                    publish_discord=should_publish_discord,
                )
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
    requested_date = as_of_date or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        target_date = _scheduler_target_date(
            settings,
            requested_date=requested_date,
            connection=connection,
        )
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
            blocked = _block_if_required_snapshot_missing(
                job,
                target_date=target_date,
                table_name="fact_ranking",
                column_name="as_of_date",
                where_clause="ranking_version = ?",
                params=[SELECTION_ENGINE_V2_VERSION],
                label="selection_engine_v2",
            )
            if blocked is not None:
                return blocked
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
    requested_date = as_of_date or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        target_date = _scheduler_target_date(
            settings,
            requested_date=requested_date,
            connection=connection,
        )
        start_date = _resolve_intraday_session_start_date(
            settings,
            end_date=target_date,
            required_sessions=WEEKLY_INTRADAY_REQUIRED_SESSIONS,
            connection=connection,
        )
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
            blocked = _block_if_required_snapshot_missing(
                job,
                target_date=target_date,
                table_name="fact_ranking",
                column_name="as_of_date",
                where_clause="ranking_version = ?",
                params=[SELECTION_ENGINE_V2_VERSION],
                label="selection_engine_v2",
            )
            if blocked is not None:
                return blocked
            blocked = _block_if_required_snapshot_missing(
                job,
                target_date=target_date,
                table_name="fact_prediction",
                column_name="as_of_date",
                where_clause="ranking_version = ?",
                params=[SELECTION_ENGINE_V2_VERSION],
                label="prediction_snapshot",
            )
            if blocked is not None:
                return blocked
            if dry_run:
                job.skip("Dry-run: evaluation bundle steps skipped.")
            else:
                job.run_step(
                    "evaluation_pipeline",
                    run_evaluation_job,
                    settings,
                    selection_end_date=target_date,
                )
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
    should_publish_ops_alerts = trigger_type == TriggerType.MANUAL and settings.discord.enabled
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
                "cleanup_docker_build_cache",
                cleanup_docker_build_cache,
                settings,
                connection=connection,
                job_run_id=job.run_id,
                dry_run=dry_run,
                policy_config_path=policy_config_path,
                critical=False,
            )
            job.run_step(
                "cleanup_model_artifacts",
                cleanup_model_artifacts,
                settings,
                connection=connection,
                job_run_id=job.run_id,
                dry_run=dry_run,
                policy_config_path=policy_config_path,
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
                "cleanup_stale_job_runs",
                cleanup_stale_job_runs,
                settings,
                connection=connection,
                job_run_id=job.run_id,
                critical=False,
            )
            job.run_step(
                "reset_open_recovery_actions",
                reset_open_recovery_actions,
                settings,
                connection=connection,
                job_run_id=job.run_id,
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
                dry_run=True if dry_run else not should_publish_ops_alerts,
                critical=False,
            )
            return job_result_from_context(
                job,
                notes=f"Ops maintenance bundle completed for {target_date.isoformat()}.",
            )


def run_docker_build_cache_cleanup_bundle(
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
            job_name="run_docker_build_cache_cleanup_bundle",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            notes=f"Docker build cache cleanup bundle for {target_date.isoformat()}",
            details={
                "bundle_phase": "docker_build_cache_cleanup",
                "date_semantics": "calendar_day",
            },
        ) as job:
            job.run_step(
                "cleanup_docker_build_cache",
                cleanup_docker_build_cache,
                settings,
                connection=connection,
                job_run_id=job.run_id,
                dry_run=dry_run,
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
                notes=f"Docker build cache cleanup bundle completed for {target_date.isoformat()}.",
            )


def _scheduler_target_date(
    settings: Settings,
    *,
    requested_date: date | None,
    connection,
) -> date:
    target = requested_date or today_local(settings.app.timezone)
    return resolve_reference_trading_date(settings, target_date=target, connection=connection)


def _scheduler_calendar_date(
    settings: Settings,
    *,
    requested_date: date | None,
) -> date:
    return requested_date or today_local(settings.app.timezone)


def _mark_audit_suite(
    job: JobRunContext,
    *,
    label: str,
    suite,
) -> None:
    if suite.fail_count:
        job.mark_degraded(f"{label}: fail={suite.fail_count}")
    elif suite.warn_count:
        job.mark_degraded(f"{label}: warn={suite.warn_count}")


def run_news_sync_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    profile: str = "after_close",
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    force: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    requested_date = _scheduler_calendar_date(settings, requested_date=as_of_date)
    should_publish_close_brief = (
        profile == "after_close"
        and trigger_type != TriggerType.RECOVERY
        and settings.discord.enabled
    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        reference_trading_date = _scheduler_target_date(
            settings,
            requested_date=requested_date,
            connection=connection,
        )
        with JobRunContext(
            settings,
            connection,
            job_name="run_news_sync_bundle",
            as_of_date=requested_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            lock_name=SCHEDULER_GLOBAL_LOCK,
            notes=f"Scheduler news sync bundle profile={profile} for {requested_date.isoformat()}",
            details={
                "bundle_phase": "news_sync",
                "profile": profile,
                "date_semantics": "calendar_day",
                "reference_trading_date": reference_trading_date.isoformat(),
            },
        ) as job:
            completed = _skip_if_already_completed(
                job,
                bundle_phase="news_sync",
                profile=profile,
            )
            if completed is not None and not force:
                return completed
            collection_dates = resolve_news_collection_dates(
                settings,
                target_date=requested_date,
                profile=profile,
                connection=connection,
            )
            if dry_run:
                job.skip(
                    f"Dry-run: news sync skipped for profile={profile} dates="
                    f"{', '.join(item.isoformat() for item in collection_dates)}.",
                )
            else:
                for index, signal_date in enumerate(collection_dates, start=1):
                    job.run_step(
                        f"sync_news_metadata_{index:02d}",
                        sync_news_metadata,
                        settings,
                        signal_date=signal_date,
                        mode="market_and_focus",
                        critical=False,
                    )
                if profile == "after_close":
                    job.run_step(
                        "publish_discord_close_brief",
                        publish_discord_close_brief,
                        settings,
                        as_of_date=requested_date,
                        dry_run=True if dry_run else not should_publish_close_brief,
                        critical=False,
                    )
                _refresh_release_views(
                    job,
                    settings=settings,
                    connection=connection,
                    as_of_date=requested_date,
                )
                job.run_step(
                    "materialize_health_snapshots",
                    materialize_health_snapshots,
                    settings,
                    connection=connection,
                    as_of_date=requested_date,
                    job_run_id=job.run_id,
                    policy_config_path=policy_config_path,
                    critical=False,
                )
            notes = (
                f"News sync bundle completed. profile={profile} "
                f"dates={', '.join(item.isoformat() for item in collection_dates)}"
            )
            return job_result_from_context(job, notes=notes, row_count=len(collection_dates))


def run_daily_close_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    force: bool = False,
    publish_discord: bool = True,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    requested_date = as_of_date or today_local(settings.app.timezone)
    should_publish_discord = publish_discord and trigger_type != TriggerType.RECOVERY
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        target_date = _scheduler_target_date(
            settings,
            requested_date=requested_date,
            connection=connection,
        )
        with JobRunContext(
            settings,
            connection,
            job_name="run_daily_close_bundle",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            lock_name=SCHEDULER_GLOBAL_LOCK,
            notes=f"Scheduler daily close bundle for {target_date.isoformat()}",
            details={
                "bundle_phase": "daily_close",
                "profile": "final_news_and_report",
                "date_semantics": "trading_day",
            },
        ) as job:
            skipped = _skip_if_non_trading_day(
                job,
                target_date=requested_date,
                label="run_daily_close_bundle",
            )
            if skipped is not None:
                return skipped
            completed = _skip_if_already_completed(job, bundle_phase="daily_close")
            if completed is not None and not force:
                return completed
            if dry_run:
                job.skip("Dry-run: daily close bundle skipped.")
            else:
                job.run_step(
                    "daily_pipeline",
                    run_daily_pipeline_job,
                    settings,
                    pipeline_date=target_date,
                    run_training=True,
                    publish_discord=False,
                )
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
                    critical=False,
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
                    job.run_step(
                        "materialize_portfolio_nav",
                        materialize_portfolio_nav,
                        settings,
                        start_date=session_row[0],
                        end_date=session_row[0],
                        policy_config_path=policy_config_path,
                    )
                job.run_step(
                    "render_daily_research_report",
                    render_daily_research_report,
                    settings,
                    connection=connection,
                    as_of_date=target_date,
                    job_run_id=job.run_id,
                    dry_run=dry_run,
                    critical=False,
                )
                job.run_step(
                    "render_portfolio_report",
                    render_portfolio_report,
                    settings,
                    as_of_date=target_date,
                    dry_run=dry_run,
                    critical=False,
                )
                _refresh_release_views(
                    job,
                    settings=settings,
                    connection=connection,
                    as_of_date=target_date,
                )
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
                if publish_discord:
                    job.run_step(
                        "publish_discord_eod_report",
                        publish_discord_eod_report,
                        settings,
                        as_of_date=target_date,
                        dry_run=(not should_publish_discord) or (not settings.discord.enabled),
                        critical=False,
                    )
            return job_result_from_context(
                job,
                notes=(
                    "Daily close bundle completed with final news recollect, "
                    "selection, portfolio, and post-close reports."
                ),
            )


def run_evaluation_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    force: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    requested_date = as_of_date or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        target_date = _scheduler_target_date(
            settings,
            requested_date=requested_date,
            connection=connection,
        )
        source_snapshot_date = (
            expected_job_reference_date(
                settings,
                job_key="daily_close",
                as_of_date=requested_date,
                connection=connection,
            )
            or target_date
        )
        start_date = _resolve_recent_start_date(
            settings,
            end_date=target_date,
            trading_days=60,
            connection=connection,
        )
        with JobRunContext(
            settings,
            connection,
            job_name="run_evaluation_bundle",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            lock_name=SCHEDULER_GLOBAL_LOCK,
            notes=f"Scheduler evaluation bundle through {target_date.isoformat()}",
            details={"bundle_phase": "evaluation", "date_semantics": "trading_day"},
        ) as job:
            skipped = _skip_if_non_trading_day(
                job,
                target_date=requested_date,
                label="run_evaluation_bundle",
            )
            if skipped is not None:
                return skipped
            completed = _skip_if_already_completed(job, bundle_phase="evaluation")
            if completed is not None and not force:
                return completed
            blocked = _block_if_required_snapshot_missing(
                job,
                target_date=source_snapshot_date,
                table_name="fact_ranking",
                column_name="as_of_date",
                where_clause="ranking_version = ?",
                params=[SELECTION_ENGINE_V2_VERSION],
                label="selection_engine_v2",
            )
            if blocked is not None:
                return blocked
            blocked = _block_if_required_snapshot_missing(
                job,
                target_date=source_snapshot_date,
                table_name="fact_prediction",
                column_name="as_of_date",
                where_clause="ranking_version = ?",
                params=[SELECTION_ENGINE_V2_VERSION],
                label="prediction_snapshot",
            )
            if blocked is not None:
                return blocked
            if dry_run:
                job.skip("Dry-run: evaluation bundle skipped.")
            else:
                job.run_step(
                    "evaluation_pipeline",
                    run_evaluation_job,
                    settings,
                    selection_end_date=target_date,
                )
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
                    "render_evaluation_report",
                    render_evaluation_report,
                    settings,
                    connection=connection,
                    as_of_date=target_date,
                    job_run_id=job.run_id,
                    dry_run=dry_run,
                    critical=False,
                )
                if settings.intraday_research.postmortem_enabled:
                    job.run_step(
                        "render_intraday_postmortem_report",
                        render_intraday_postmortem_report,
                        settings,
                        session_date=target_date,
                        horizons=list(DEFAULT_HORIZONS),
                        dry_run=dry_run,
                        critical=False,
                    )
                    if (
                        settings.intraday_research.discord_summary_enabled
                        and settings.discord.enabled
                    ):
                        job.run_step(
                            "publish_discord_intraday_postmortem",
                            publish_discord_intraday_postmortem,
                            settings,
                            session_date=target_date,
                            horizons=list(DEFAULT_HORIZONS),
                            dry_run=dry_run,
                            critical=False,
                        )
                job.run_step(
                    "materialize_intraday_research_capability",
                    materialize_intraday_research_capability,
                    settings,
                    as_of_date=target_date,
                    run_id=job.run_id,
                    connection=connection,
                    critical=False,
                )
                _refresh_release_views(
                    job,
                    settings=settings,
                    connection=connection,
                    as_of_date=target_date,
                )
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
            return job_result_from_context(
                job,
                notes=f"Evaluation bundle completed for {target_date.isoformat()}.",
            )


def run_intraday_assist_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    checkpoint_time: str | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    force: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    requested_date = as_of_date or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        due_checkpoint = checkpoint_time or resolve_due_intraday_checkpoint(settings)
        checkpoint_key = due_checkpoint or "PREP"
        with JobRunContext(
            settings,
            connection,
            job_name="run_intraday_assist_bundle",
            as_of_date=requested_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            lock_name=SCHEDULER_GLOBAL_LOCK,
            notes=f"Scheduler intraday assist bundle for {requested_date.isoformat()}",
            details={
                "bundle_phase": "intraday_assist",
                "checkpoint_time": checkpoint_key,
                "date_semantics": "trading_day",
            },
        ) as job:
            capability_skip = _skip_if_intraday_feature_disabled(
                job,
                feature_slug="intraday_assist",
            )
            if capability_skip is not None:
                return capability_skip
            skipped = _skip_if_non_trading_day(
                job,
                target_date=requested_date,
                label="run_intraday_assist_bundle",
            )
            if skipped is not None:
                return skipped
            completed = _skip_if_already_completed(
                job,
                bundle_phase="intraday_assist",
                checkpoint_time=checkpoint_key,
            )
            if completed is not None and not force:
                return completed
            selection_date = resolve_previous_trading_date(
                settings,
                target_date=requested_date,
                connection=connection,
            )
            if selection_date is None:
                job.block(
                    "run_intraday_assist_bundle: "
                    f"no previous trading date for {requested_date.isoformat()}."
                )
                return job_result_from_context(
                    job,
                    notes="Intraday assist blocked: missing previous trading day.",
                )
            if dry_run:
                job.skip(
                    "Dry-run: intraday assist skipped for "
                    f"session={requested_date.isoformat()} checkpoint={checkpoint_key}."
                )
            else:
                job.run_step(
                    "materialize_intraday_candidate_session",
                    materialize_intraday_candidate_session,
                    settings,
                    selection_date=selection_date,
                    horizons=list(DEFAULT_HORIZONS),
                    max_candidates=30,
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                    critical=False,
                )
                job.run_step(
                    "backfill_intraday_candidate_bars",
                    backfill_intraday_candidate_bars,
                    settings,
                    session_date=requested_date,
                    horizons=list(DEFAULT_HORIZONS),
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                    critical=False,
                )
                job.run_step(
                    "backfill_intraday_candidate_trade_summary",
                    backfill_intraday_candidate_trade_summary,
                    settings,
                    session_date=requested_date,
                    horizons=list(DEFAULT_HORIZONS),
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                    checkpoint_times=list(DEFAULT_INTRADAY_CHECKPOINTS),
                    critical=False,
                )
                job.run_step(
                    "backfill_intraday_candidate_quote_summary",
                    backfill_intraday_candidate_quote_summary,
                    settings,
                    session_date=requested_date,
                    horizons=list(DEFAULT_HORIZONS),
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                    checkpoint_times=list(DEFAULT_INTRADAY_CHECKPOINTS),
                    critical=False,
                )
                if due_checkpoint is None:
                    job.mark_degraded(
                        "Intraday assist prepared candidate session but checkpoint is not due yet."
                    )
                else:
                    job.run_step(
                        "materialize_intraday_signal_snapshots",
                        materialize_intraday_signal_snapshots,
                        settings,
                        session_date=requested_date,
                        checkpoint=due_checkpoint,
                        horizons=list(DEFAULT_HORIZONS),
                        ranking_version=SELECTION_ENGINE_V2_VERSION,
                        critical=False,
                    )
                    job.run_step(
                        "materialize_intraday_entry_decisions",
                        materialize_intraday_entry_decisions,
                        settings,
                        session_date=requested_date,
                        checkpoint=due_checkpoint,
                        horizons=list(DEFAULT_HORIZONS),
                        ranking_version=SELECTION_ENGINE_V2_VERSION,
                        critical=False,
                    )
                    job.run_step(
                        "materialize_intraday_adjusted_entry_decisions",
                        materialize_intraday_adjusted_entry_decisions,
                        settings,
                        session_date=requested_date,
                        checkpoint=due_checkpoint,
                        horizons=list(DEFAULT_HORIZONS),
                        ranking_version=SELECTION_ENGINE_V2_VERSION,
                        critical=False,
                    )
                    job.run_step(
                        "materialize_intraday_meta_predictions",
                        materialize_intraday_meta_predictions,
                        settings,
                        session_date=requested_date,
                        horizons=list(DEFAULT_HORIZONS),
                        ranking_version=SELECTION_ENGINE_V2_VERSION,
                        critical=False,
                    )
                    job.run_step(
                        "materialize_intraday_final_actions",
                        materialize_intraday_final_actions,
                        settings,
                        session_date=requested_date,
                        horizons=list(DEFAULT_HORIZONS),
                        ranking_version=SELECTION_ENGINE_V2_VERSION,
                        critical=False,
                    )
                    job.run_step(
                        "render_intraday_summary_report",
                        render_intraday_summary_report,
                        settings,
                        connection=connection,
                        session_date=requested_date,
                        job_run_id=job.run_id,
                        dry_run=dry_run,
                        critical=False,
                    )
                job.run_step(
                    "materialize_intraday_research_capability",
                    materialize_intraday_research_capability,
                    settings,
                    as_of_date=requested_date,
                    run_id=job.run_id,
                    connection=connection,
                    critical=False,
                )
                _refresh_release_views(
                    job,
                    settings=settings,
                    connection=connection,
                    as_of_date=requested_date,
                )
                job.run_step(
                    "materialize_health_snapshots",
                    materialize_health_snapshots,
                    settings,
                    connection=connection,
                    as_of_date=requested_date,
                    job_run_id=job.run_id,
                    policy_config_path=policy_config_path,
                    critical=False,
                )
            return job_result_from_context(
                job,
                notes=(
                    f"Intraday assist bundle completed for {requested_date.isoformat()} "
                    f"checkpoint={checkpoint_key}."
                ),
            )


def run_weekly_training_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    force: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    requested_date = as_of_date or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        target_date = _scheduler_target_date(
            settings,
            requested_date=requested_date,
            connection=connection,
        )
        start_date = _resolve_intraday_session_start_date(
            settings,
            end_date=target_date,
            required_sessions=WEEKLY_INTRADAY_REQUIRED_SESSIONS,
            connection=connection,
        )
        with JobRunContext(
            settings,
            connection,
            job_name="run_weekly_training_bundle",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            lock_name=SCHEDULER_GLOBAL_LOCK,
            notes=f"Scheduler weekly training candidate bundle through {target_date.isoformat()}",
            details={
                "bundle_phase": "weekly_training_candidate",
                "date_semantics": "hybrid",
                "scheduled_calendar_date": requested_date.isoformat(),
                "reference_trading_date": target_date.isoformat(),
            },
        ) as job:
            capability_skip = _skip_if_intraday_feature_disabled(
                job,
                feature_slug="intraday_meta_model",
            )
            if capability_skip is not None:
                return capability_skip
            completed = _skip_if_already_completed(job, bundle_phase="weekly_training_candidate")
            if completed is not None and not force:
                return completed
            if dry_run:
                job.skip("Dry-run: weekly training candidate bundle skipped.")
            else:
                for horizon in DEFAULT_HORIZONS:
                    horizon_list = [int(horizon)]
                    horizon_label = f"h{horizon}"
                    job.run_step(
                        f"train_intraday_meta_models_{horizon_label}",
                        train_intraday_meta_models,
                        settings,
                        train_end_date=target_date,
                        horizons=horizon_list,
                        start_session_date=start_date,
                        validation_sessions=10,
                        critical=False,
                    )
                    job.run_step(
                        f"run_intraday_meta_walkforward_{horizon_label}",
                        run_intraday_meta_walkforward,
                        settings,
                        start_session_date=start_date,
                        end_session_date=target_date,
                        mode="rolling",
                        train_sessions=40,
                        validation_sessions=10,
                        test_sessions=10,
                        step_sessions=5,
                        horizons=horizon_list,
                        critical=False,
                    )
                    job.run_step(
                        f"evaluate_intraday_meta_models_{horizon_label}",
                        evaluate_intraday_meta_models,
                        settings,
                        start_session_date=start_date,
                        end_session_date=target_date,
                        horizons=horizon_list,
                        critical=False,
                    )
                if settings.intraday_research.research_reports_enabled:
                    job.run_step(
                        "render_intraday_meta_model_report",
                        render_intraday_meta_model_report,
                        settings,
                        as_of_date=target_date,
                        horizons=list(DEFAULT_HORIZONS),
                        dry_run=dry_run,
                        critical=False,
                    )
                    if (
                        settings.intraday_research.discord_summary_enabled
                        and settings.discord.enabled
                    ):
                        job.run_step(
                            "publish_discord_intraday_meta_summary",
                            publish_discord_intraday_meta_summary,
                            settings,
                            as_of_date=target_date,
                            horizons=list(DEFAULT_HORIZONS),
                            dry_run=dry_run,
                            critical=False,
                        )
                job.mark_degraded(
                    "Automatic weekly training only creates retrain candidates. "
                    "Active meta-model is never auto-promoted."
                )
                job.run_step(
                    "materialize_intraday_research_capability",
                    materialize_intraday_research_capability,
                    settings,
                    as_of_date=target_date,
                    run_id=job.run_id,
                    connection=connection,
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
                notes=(
                    "Weekly training candidate bundle completed. "
                    "Candidates were generated without activating any model."
                ),
            )


def run_weekly_calibration_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    force: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    requested_date = as_of_date or today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        target_date = _scheduler_target_date(
            settings,
            requested_date=requested_date,
            connection=connection,
        )
        start_date = _resolve_intraday_session_start_date(
            settings,
            end_date=target_date,
            required_sessions=WEEKLY_INTRADAY_REQUIRED_SESSIONS,
            connection=connection,
        )
        checkpoints = list(DEFAULT_INTRADAY_CHECKPOINTS)
        scopes = ["GLOBAL", "HORIZON", "HORIZON_CHECKPOINT", "HORIZON_REGIME_CLUSTER"]
        with JobRunContext(
            settings,
            connection,
            job_name="run_weekly_calibration_bundle",
            as_of_date=target_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            lock_name=SCHEDULER_GLOBAL_LOCK,
            notes=f"Scheduler weekly calibration bundle through {target_date.isoformat()}",
            details={
                "bundle_phase": "weekly_calibration",
                "date_semantics": "hybrid",
                "scheduled_calendar_date": requested_date.isoformat(),
                "reference_trading_date": target_date.isoformat(),
            },
        ) as job:
            capability_skip = _skip_if_intraday_feature_disabled(
                job,
                feature_slug="intraday_policy_adjustment",
            )
            if capability_skip is not None:
                return capability_skip
            completed = _skip_if_already_completed(job, bundle_phase="weekly_calibration")
            if completed is not None and not force:
                return completed
            if dry_run:
                job.skip("Dry-run: weekly calibration bundle skipped.")
            else:
                job.run_step(
                    "materialize_intraday_policy_candidates",
                    materialize_intraday_policy_candidates,
                    settings,
                    search_space_version="pcal_v1",
                    horizons=list(DEFAULT_HORIZONS),
                    checkpoints=checkpoints,
                    scopes=scopes,
                    critical=False,
                )
                for horizon in DEFAULT_HORIZONS:
                    horizon_list = [int(horizon)]
                    horizon_label = f"h{horizon}"
                    _materialize_intraday_decision_outcome_chunks(
                        job,
                        settings=settings,
                        start_session_date=start_date,
                        end_session_date=target_date,
                        horizons=horizon_list,
                    )
                    job.run_step(
                        f"run_intraday_policy_calibration_{horizon_label}",
                        run_intraday_policy_calibration,
                        settings,
                        start_session_date=start_date,
                        end_session_date=target_date,
                        horizons=horizon_list,
                        checkpoints=checkpoints,
                        objective_version="ip_obj_v1",
                        split_version="wf_40_10_10_step5",
                        search_space_version="pcal_v1",
                        refresh_decision_outcomes=False,
                        critical=False,
                    )
                    job.run_step(
                        f"run_intraday_policy_walkforward_{horizon_label}",
                        run_intraday_policy_walkforward,
                        settings,
                        start_session_date=start_date,
                        end_session_date=target_date,
                        mode="rolling",
                        train_sessions=40,
                        validation_sessions=10,
                        test_sessions=10,
                        step_sessions=5,
                        horizons=horizon_list,
                        checkpoints=checkpoints,
                        objective_version="ip_obj_v1",
                        split_version="wf_40_10_10_step5",
                        search_space_version="pcal_v1",
                        refresh_decision_outcomes=False,
                        critical=False,
                    )
                    job.run_step(
                        f"evaluate_intraday_policy_ablation_{horizon_label}",
                        evaluate_intraday_policy_ablation,
                        settings,
                        start_session_date=start_date,
                        end_session_date=target_date,
                        horizons=horizon_list,
                        base_policy_source="latest_recommendation",
                        critical=False,
                    )
                job.run_step(
                    "materialize_intraday_policy_recommendations",
                    materialize_intraday_policy_recommendations,
                    settings,
                    as_of_date=target_date,
                    horizons=list(DEFAULT_HORIZONS),
                    minimum_test_sessions=10,
                    critical=False,
                )
                job.run_step(
                    "calibrate_intraday_meta_thresholds",
                    calibrate_intraday_meta_thresholds,
                    settings,
                    as_of_date=target_date,
                    horizons=list(DEFAULT_HORIZONS),
                    critical=False,
                )
                if settings.intraday_research.research_reports_enabled:
                    job.run_step(
                        "render_intraday_policy_research_report",
                        render_intraday_policy_research_report,
                        settings,
                        as_of_date=target_date,
                        horizons=list(DEFAULT_HORIZONS),
                        dry_run=dry_run,
                        critical=False,
                    )
                    if (
                        settings.intraday_research.discord_summary_enabled
                        and settings.discord.enabled
                    ):
                        job.run_step(
                            "publish_discord_intraday_policy_summary",
                            publish_discord_intraday_policy_summary,
                            settings,
                            as_of_date=target_date,
                            horizons=list(DEFAULT_HORIZONS),
                            dry_run=dry_run,
                            critical=False,
                        )
                job.mark_degraded(
                    "Automatic weekly calibration updates recommendations and thresholds only. "
                    "Active policy and active meta-model are never auto-activated."
                )
                job.run_step(
                    "materialize_intraday_research_capability",
                    materialize_intraday_research_capability,
                    settings,
                    as_of_date=target_date,
                    run_id=job.run_id,
                    connection=connection,
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
                notes=(
                    "Weekly calibration bundle completed. "
                    "Recommendations were refreshed without automatic activation."
                ),
            )


def run_daily_audit_lite_bundle(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    trigger_type: str = TriggerType.MANUAL,
    dry_run: bool = False,
    force: bool = False,
    parent_run_id: str | None = None,
    root_run_id: str | None = None,
    recovery_of_run_id: str | None = None,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    ensure_storage_layout(settings)
    requested_date = _scheduler_calendar_date(settings, requested_date=as_of_date)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name="run_daily_audit_lite_bundle",
            as_of_date=requested_date,
            trigger_type=trigger_type,
            dry_run=dry_run,
            parent_run_id=parent_run_id,
            root_run_id=root_run_id,
            recovery_of_run_id=recovery_of_run_id,
            policy_config_path=policy_config_path,
            lock_name=SCHEDULER_GLOBAL_LOCK,
            notes=f"Scheduler daily audit-lite bundle for {requested_date.isoformat()}",
            details={
                "bundle_phase": "daily_audit_lite",
                "date_semantics": "calendar_day",
            },
        ) as job:
            completed = _skip_if_already_completed(job, bundle_phase="daily_audit_lite")
            if completed is not None and not force:
                return completed
            if dry_run:
                job.skip("Dry-run: daily audit-lite bundle skipped.")
            else:
                latest_suite = job.run_step(
                    "run_latest_layer_checks",
                    run_latest_layer_checks,
                    settings,
                    connection=connection,
                    critical=False,
                )
                artifact_suite = job.run_step(
                    "run_artifact_reference_checks",
                    run_artifact_reference_checks,
                    settings,
                    connection=connection,
                    critical=False,
                )
                release_validation = job.run_step(
                    "validate_release_candidate",
                    validate_release_candidate,
                    settings,
                    connection=connection,
                    as_of_date=requested_date,
                    critical=False,
                )
                if latest_suite is not None:
                    _mark_audit_suite(job, label="latest_layer_checks", suite=latest_suite)
                if artifact_suite is not None:
                    _mark_audit_suite(job, label="artifact_reference_checks", suite=artifact_suite)
                if release_validation is not None and release_validation.warning_count:
                    job.mark_degraded(
                        f"release_candidate_validation: warnings={release_validation.warning_count}"
                    )
                job.run_step(
                    "render_release_candidate_checklist",
                    render_release_candidate_checklist,
                    settings,
                    connection=connection,
                    as_of_date=requested_date,
                    job_run_id=job.run_id,
                    dry_run=dry_run,
                    critical=False,
                )
                job.run_step(
                    "materialize_intraday_research_capability",
                    materialize_intraday_research_capability,
                    settings,
                    as_of_date=requested_date,
                    run_id=job.run_id,
                    connection=connection,
                    critical=False,
                )
                _refresh_release_views(
                    job,
                    settings=settings,
                    connection=connection,
                    as_of_date=requested_date,
                )
                job.run_step(
                    "materialize_health_snapshots",
                    materialize_health_snapshots,
                    settings,
                    connection=connection,
                    as_of_date=requested_date,
                    job_run_id=job.run_id,
                    policy_config_path=policy_config_path,
                    critical=False,
                )
            return job_result_from_context(
                job,
                notes=f"Daily audit-lite bundle completed for {requested_date.isoformat()}.",
            )
