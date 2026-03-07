from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler

from app.common.disk import measure_disk_usage
from app.common.run_context import activate_run_context
from app.common.time import get_timezone, now_local, today_local
from app.features.feature_store import build_feature_store
from app.pipelines.daily_ohlcv import sync_daily_ohlcv
from app.pipelines.fundamentals_snapshot import sync_fundamentals_snapshot
from app.pipelines.news_metadata import sync_news_metadata
from app.ranking.explanatory_score import materialize_explanatory_ranking
from app.regime.snapshot import build_market_regime_snapshot
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout, log_disk_usage
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class JobExecutionResult:
    run_id: str
    run_type: str
    status: str
    notes: str


def _remove_files_older_than(root: Path, *, days: int) -> int:
    if days <= 0 or not root.exists():
        return 0
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    removed = 0
    for candidate in root.rglob("*"):
        if candidate.is_file():
            modified_at = datetime.fromtimestamp(candidate.stat().st_mtime, tz=timezone.utc)
            if modified_at < cutoff:
                candidate.unlink()
                removed += 1
    return removed


def _run_skeleton_job(settings: Settings, *, run_type: str, notes: str) -> JobExecutionResult:
    ensure_storage_layout(settings)
    as_of_date = today_local(settings.app.timezone)

    with activate_run_context(run_type, as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                notes=notes,
            )
            try:
                disk_report = measure_disk_usage(
                    settings.paths.data_dir,
                    warning_ratio=settings.storage.warning_ratio,
                    prune_ratio=settings.storage.prune_ratio,
                    limit_ratio=settings.storage.limit_ratio,
                )
                log_disk_usage(
                    connection,
                    report=disk_report,
                    measured_at=now_local(settings.app.timezone),
                    action_taken=run_type,
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                )
                return JobExecutionResult(
                    run_id=run_context.run_id,
                    run_type=run_type,
                    status="success",
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=notes,
                    error_message=str(exc),
                )
                raise


def _resolve_pipeline_date(settings: Settings) -> date:
    target_date = today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
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
    return row[0] if row is not None else target_date


def run_daily_pipeline_job(settings: Settings) -> JobExecutionResult:
    ensure_storage_layout(settings)
    pipeline_date = _resolve_pipeline_date(settings)
    artifact_paths: list[str] = []

    with activate_run_context("daily_pipeline", as_of_date=pipeline_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as manifest_connection:
            bootstrap_core_tables(manifest_connection)
            record_run_start(
                manifest_connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "sync_daily_ohlcv",
                    "sync_fundamentals_snapshot",
                    "sync_news_metadata",
                    "build_feature_store",
                    "build_market_regime_snapshot",
                    "materialize_explanatory_ranking",
                ],
                notes=(
                    f"Run the TICKET-003 daily research pipeline for {pipeline_date.isoformat()}"
                ),
            )
        try:
            ohlcv_result = sync_daily_ohlcv(settings, trading_date=pipeline_date)
            fundamentals_result = sync_fundamentals_snapshot(
                settings,
                as_of_date=pipeline_date,
            )
            news_result = sync_news_metadata(
                settings,
                signal_date=pipeline_date,
                mode="market_and_focus",
            )
            feature_result = build_feature_store(
                settings,
                as_of_date=pipeline_date,
            )
            regime_result = build_market_regime_snapshot(
                settings,
                as_of_date=pipeline_date,
            )
            ranking_result = materialize_explanatory_ranking(
                settings,
                as_of_date=pipeline_date,
                horizons=[1, 5],
            )
            artifact_paths.extend(ohlcv_result.artifact_paths)
            artifact_paths.extend(fundamentals_result.artifact_paths)
            artifact_paths.extend(news_result.artifact_paths)
            artifact_paths.extend(feature_result.artifact_paths)
            artifact_paths.extend(regime_result.artifact_paths)
            artifact_paths.extend(ranking_result.artifact_paths)

            notes = (
                f"Daily pipeline completed for {pipeline_date.isoformat()}. "
                f"ohlcv_rows={ohlcv_result.row_count}, "
                f"fundamentals_rows={fundamentals_result.row_count}, "
                f"news_rows={news_result.deduped_row_count}, "
                f"feature_rows={feature_result.feature_row_count}, "
                f"regime_rows={regime_result.row_count}, "
                f"ranking_rows={ranking_result.row_count}"
            )
            with duckdb_connection(settings.paths.duckdb_path) as manifest_connection:
                bootstrap_core_tables(manifest_connection)
                disk_report = measure_disk_usage(
                    settings.paths.data_dir,
                    warning_ratio=settings.storage.warning_ratio,
                    prune_ratio=settings.storage.prune_ratio,
                    limit_ratio=settings.storage.limit_ratio,
                )
                log_disk_usage(
                    manifest_connection,
                    report=disk_report,
                    measured_at=now_local(settings.app.timezone),
                    action_taken="daily_pipeline",
                )
                record_run_finish(
                    manifest_connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    feature_version=feature_result.feature_version,
                    ranking_version=ranking_result.ranking_version,
                )
            return JobExecutionResult(
                run_id=run_context.run_id,
                run_type="daily_pipeline",
                status="success",
                notes=notes,
            )
        except Exception as exc:
            with duckdb_connection(settings.paths.duckdb_path) as manifest_connection:
                bootstrap_core_tables(manifest_connection)
                record_run_finish(
                    manifest_connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=artifact_paths,
                    notes="Daily pipeline failed.",
                    error_message=str(exc),
                )
            raise


def run_evaluation_job(settings: Settings) -> JobExecutionResult:
    notes = "Evaluation skeleton executed. D+1 and D+5 scoring logic is pending."
    return _run_skeleton_job(settings, run_type="evaluation", notes=notes)


def run_prune_storage_job(settings: Settings) -> JobExecutionResult:
    removed_cache = _remove_files_older_than(
        settings.paths.cache_dir,
        days=settings.retention.report_cache_days,
    )
    removed_logs = _remove_files_older_than(
        settings.paths.logs_dir,
        days=settings.retention.log_days,
    )
    notes = (
        f"Storage prune executed. Removed {removed_cache} cache files and {removed_logs} log files."
    )
    return _run_skeleton_job(settings, run_type="prune_storage", notes=notes)


def build_scheduler(settings: Settings) -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone=get_timezone(settings.app.timezone))
    scheduler.add_job(
        run_daily_pipeline_job,
        "cron",
        hour=18,
        minute=5,
        args=[settings],
        id="daily",
    )
    scheduler.add_job(
        run_evaluation_job,
        "cron",
        hour=16,
        minute=20,
        args=[settings],
        id="evaluation",
    )
    scheduler.add_job(
        run_prune_storage_job,
        "cron",
        hour=3,
        minute=0,
        args=[settings],
        id="prune",
    )
    return scheduler
