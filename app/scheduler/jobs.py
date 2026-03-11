from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler

from app.common.disk import measure_disk_usage
from app.common.run_context import activate_run_context
from app.common.time import get_timezone, now_local, today_local
from app.evaluation.alpha_shadow import (
    materialize_alpha_shadow_evaluation_summary,
    materialize_alpha_shadow_selection_outcomes,
)
from app.evaluation.calibration_diagnostics import materialize_calibration_diagnostics
from app.evaluation.outcomes import materialize_selection_outcomes
from app.evaluation.summary import materialize_prediction_evaluation
from app.evaluation.validation import validate_evaluation_pipeline
from app.features.feature_store import build_feature_store
from app.ml.constants import MODEL_VERSION as ALPHA_MODEL_VERSION
from app.ml.inference import materialize_alpha_predictions_v1
from app.ml.promotion import run_alpha_auto_promotion
from app.ml.shadow import materialize_alpha_shadow_candidates
from app.ml.training import train_alpha_candidate_models, train_alpha_model_v1
from app.pipelines.daily_ohlcv import sync_daily_ohlcv
from app.pipelines.fundamentals_snapshot import sync_fundamentals_snapshot
from app.pipelines.investor_flow import sync_investor_flow
from app.pipelines.news_metadata import sync_news_metadata
from app.ranking.explanatory_score import materialize_explanatory_ranking
from app.regime.snapshot import build_market_regime_snapshot
from app.reports.discord_eod import publish_discord_eod_report
from app.reports.postmortem import publish_discord_postmortem_report
from app.selection.calibration import PREDICTION_VERSION, calibrate_proxy_prediction_bands
from app.selection.engine_v1 import materialize_selection_engine_v1
from app.selection.engine_v2 import materialize_selection_engine_v2
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


def _resolve_latest_ranking_date(settings: Settings) -> date:
    fallback = today_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            """
            SELECT MAX(as_of_date)
            FROM fact_ranking
            """
        ).fetchone()
    return row[0] if row is not None and row[0] is not None else fallback


def _resolve_latest_matured_evaluation_date(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
) -> date | None:
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            """
            SELECT MAX(evaluation_date)
            FROM fact_selection_outcome
            WHERE selection_date BETWEEN ? AND ?
              AND outcome_status = 'matured'
            """,
            [start_selection_date, end_selection_date],
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0]


def _resolve_lookback_start_date(
    settings: Settings,
    *,
    end_date: date,
    trading_days: int,
) -> date:
    with duckdb_connection(settings.paths.duckdb_path) as connection:
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
    if row is None or row[0] is None:
        return end_date
    return row[0]


def _is_optional_calibration_error(exc: RuntimeError) -> bool:
    message = str(exc)
    return any(
        token in message
        for token in (
            "No overlapping selection-engine rows and forward labels were available",
            "No selection engine rows exist at or before the calibration end date",
        )
    )


def run_daily_pipeline_job(
    settings: Settings,
    *,
    pipeline_date: date | None = None,
    run_training: bool = True,
    publish_discord: bool = True,
) -> JobExecutionResult:
    ensure_storage_layout(settings)
    pipeline_date = pipeline_date or _resolve_pipeline_date(settings)
    calibration_start_date = _resolve_lookback_start_date(
        settings,
        end_date=pipeline_date,
        trading_days=60,
    )
    artifact_paths: list[str] = []
    calibration_note = ""

    with activate_run_context("daily_pipeline", as_of_date=pipeline_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as manifest_connection:
            bootstrap_core_tables(manifest_connection)
            input_sources = [
                "sync_daily_ohlcv",
                "sync_fundamentals_snapshot",
                "sync_news_metadata",
                "sync_investor_flow",
                "build_feature_store",
                "build_market_regime_snapshot",
                "materialize_explanatory_ranking",
                "materialize_selection_engine_v1",
                "materialize_alpha_shadow_candidates",
                "run_alpha_auto_promotion",
                "materialize_alpha_predictions_v1",
                "materialize_selection_engine_v2",
                "calibrate_proxy_prediction_bands",
            ]
            if run_training:
                input_sources.insert(8, "train_alpha_model_v1")
                input_sources.insert(9, "train_alpha_candidate_models")
            if publish_discord:
                input_sources.append("publish_discord_eod_report")
            record_run_start(
                manifest_connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=input_sources,
                notes=(
                    f"Run the TICKET-004 daily research pipeline for {pipeline_date.isoformat()}"
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
            flow_result = sync_investor_flow(
                settings,
                trading_date=pipeline_date,
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
            selection_result = materialize_selection_engine_v1(
                settings,
                as_of_date=pipeline_date,
                horizons=[1, 5],
            )
            alpha_training_result = (
                train_alpha_model_v1(
                    settings,
                    train_end_date=pipeline_date,
                    horizons=[1, 5],
                    min_train_days=120,
                    validation_days=20,
                )
                if run_training
                else None
            )
            alpha_candidate_training_result = (
                train_alpha_candidate_models(
                    settings,
                    train_end_date=pipeline_date,
                    horizons=[1, 5],
                    min_train_days=120,
                    validation_days=20,
                )
                if run_training
                else None
            )
            alpha_shadow_result = materialize_alpha_shadow_candidates(
                settings,
                as_of_date=pipeline_date,
                horizons=[1, 5],
            )
            alpha_promotion_result = run_alpha_auto_promotion(
                settings,
                as_of_date=pipeline_date,
                horizons=[1, 5],
            )
            alpha_prediction_result = materialize_alpha_predictions_v1(
                settings,
                as_of_date=pipeline_date,
                horizons=[1, 5],
            )
            selection_v2_result = materialize_selection_engine_v2(
                settings,
                as_of_date=pipeline_date,
                horizons=[1, 5],
            )
            try:
                calibration_result = calibrate_proxy_prediction_bands(
                    settings,
                    start_date=calibration_start_date,
                    end_date=pipeline_date,
                    horizons=[1, 5],
                )
            except RuntimeError as exc:
                if not _is_optional_calibration_error(exc):
                    raise
                calibration_result = None
                calibration_note = f" calibration_skipped={exc}"
            discord_result = (
                publish_discord_eod_report(
                    settings,
                    as_of_date=pipeline_date,
                    dry_run=not settings.discord.enabled,
                )
                if publish_discord
                else None
            )
            artifact_paths.extend(ohlcv_result.artifact_paths)
            artifact_paths.extend(fundamentals_result.artifact_paths)
            artifact_paths.extend(news_result.artifact_paths)
            artifact_paths.extend(flow_result.artifact_paths)
            artifact_paths.extend(feature_result.artifact_paths)
            artifact_paths.extend(regime_result.artifact_paths)
            artifact_paths.extend(ranking_result.artifact_paths)
            artifact_paths.extend(selection_result.artifact_paths)
            if alpha_training_result is not None:
                artifact_paths.extend(alpha_training_result.artifact_paths)
            if alpha_candidate_training_result is not None:
                artifact_paths.extend(alpha_candidate_training_result.artifact_paths)
            artifact_paths.extend(alpha_shadow_result.artifact_paths)
            artifact_paths.extend(alpha_promotion_result.artifact_paths)
            artifact_paths.extend(alpha_prediction_result.artifact_paths)
            artifact_paths.extend(selection_v2_result.artifact_paths)
            if calibration_result is not None:
                artifact_paths.extend(calibration_result.artifact_paths)
            if discord_result is not None:
                artifact_paths.extend(discord_result.artifact_paths)

            alpha_candidate_training_run_count = (
                alpha_candidate_training_result.training_run_count
                if alpha_candidate_training_result
                else 0
            )
            notes = (
                f"Daily pipeline completed for {pipeline_date.isoformat()}. "
                f"ohlcv_rows={ohlcv_result.row_count}, "
                f"fundamentals_rows={fundamentals_result.row_count}, "
                f"news_rows={news_result.deduped_row_count}, "
                f"flow_rows={flow_result.row_count}, "
                f"feature_rows={feature_result.feature_row_count}, "
                f"regime_rows={regime_result.row_count}, "
                f"ranking_rows={ranking_result.row_count}, "
                f"selection_rows={selection_result.row_count}, "
                "alpha_training_runs="
                f"{alpha_training_result.training_run_count if alpha_training_result else 0}, "
                "alpha_candidate_training_runs="
                f"{alpha_candidate_training_run_count}, "
                f"alpha_promotion_rows={alpha_promotion_result.row_count}, "
                "alpha_auto_promoted_horizons="
                f"{alpha_promotion_result.promoted_horizon_count}, "
                f"alpha_prediction_rows={alpha_prediction_result.row_count}, "
                f"alpha_shadow_prediction_rows={alpha_shadow_result.prediction_row_count}, "
                f"alpha_shadow_ranking_rows={alpha_shadow_result.ranking_row_count}, "
                f"selection_v2_rows={selection_v2_result.row_count}, "
                f"prediction_rows={calibration_result.row_count if calibration_result else 0}, "
                f"discord_published={discord_result.published if discord_result else False}"
            )
            if calibration_note:
                notes = f"{notes}.{calibration_note}"
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
                    model_version=ALPHA_MODEL_VERSION,
                    feature_version=feature_result.feature_version,
                    ranking_version=selection_v2_result.ranking_version,
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


def run_evaluation_job(
    settings: Settings,
    *,
    selection_end_date: date | None = None,
) -> JobExecutionResult:
    ensure_storage_layout(settings)
    selection_end_date = selection_end_date or _resolve_latest_ranking_date(settings)
    selection_start_date = _resolve_lookback_start_date(
        settings,
        end_date=selection_end_date,
        trading_days=60,
    )
    artifact_paths: list[str] = []

    with activate_run_context("evaluation", as_of_date=selection_end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as manifest_connection:
            bootstrap_core_tables(manifest_connection)
            record_run_start(
                manifest_connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "materialize_selection_outcomes",
                    "materialize_alpha_shadow_selection_outcomes",
                    "materialize_prediction_evaluation",
                    "materialize_alpha_shadow_evaluation_summary",
                    "materialize_calibration_diagnostics",
                    "publish_discord_postmortem_report",
                    "validate_evaluation_pipeline",
                ],
                notes=(
                    "Run the TICKET-005 evaluation pipeline. "
                    f"selection_range={selection_start_date.isoformat()}.."
                    f"{selection_end_date.isoformat()}"
                ),
            )
        try:
            outcome_result = materialize_selection_outcomes(
                settings,
                start_selection_date=selection_start_date,
                end_selection_date=selection_end_date,
                horizons=[1, 5],
            )
            shadow_outcome_result = materialize_alpha_shadow_selection_outcomes(
                settings,
                start_selection_date=selection_start_date,
                end_selection_date=selection_end_date,
                horizons=[1, 5],
            )
            summary_result = materialize_prediction_evaluation(
                settings,
                start_selection_date=selection_start_date,
                end_selection_date=selection_end_date,
                horizons=[1, 5],
                rolling_windows=[20, 60],
            )
            shadow_summary_result = materialize_alpha_shadow_evaluation_summary(
                settings,
                start_selection_date=selection_start_date,
                end_selection_date=selection_end_date,
                horizons=[1, 5],
                rolling_windows=[20, 60],
            )
            diagnostic_result = materialize_calibration_diagnostics(
                settings,
                start_selection_date=selection_start_date,
                end_selection_date=selection_end_date,
                horizons=[1, 5],
                bin_count=10,
            )
            evaluation_date = (
                _resolve_latest_matured_evaluation_date(
                    settings,
                    start_selection_date=selection_start_date,
                    end_selection_date=selection_end_date,
                )
                or selection_end_date
            )
            postmortem_result = publish_discord_postmortem_report(
                settings,
                evaluation_date=evaluation_date,
                horizons=[1, 5],
                dry_run=not settings.discord.enabled,
            )
            validation_result = validate_evaluation_pipeline(
                settings,
                start_selection_date=selection_start_date,
                end_selection_date=selection_end_date,
                horizons=[1, 5],
            )

            artifact_paths.extend(outcome_result.artifact_paths)
            artifact_paths.extend(shadow_outcome_result.artifact_paths)
            artifact_paths.extend(summary_result.artifact_paths)
            artifact_paths.extend(shadow_summary_result.artifact_paths)
            artifact_paths.extend(diagnostic_result.artifact_paths)
            artifact_paths.extend(postmortem_result.artifact_paths)
            artifact_paths.extend(validation_result.artifact_paths)

            notes = (
                "Evaluation pipeline completed. selection_range="
                f"{selection_start_date.isoformat()}.."
                f"{selection_end_date.isoformat()}, outcome_rows={outcome_result.row_count}, "
                f"shadow_outcome_rows={shadow_outcome_result.row_count}, "
                f"evaluation_rows={summary_result.row_count}, "
                f"shadow_summary_rows={shadow_summary_result.row_count}, "
                f"diagnostic_rows={diagnostic_result.row_count}, "
                f"postmortem_published={postmortem_result.published}, "
                f"validation_checks={validation_result.row_count}"
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
                    action_taken="evaluation",
                )
                record_run_finish(
                    manifest_connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=PREDICTION_VERSION,
                    ranking_version="selection_engine_v1,explanatory_ranking_v0",
                )
            return JobExecutionResult(
                run_id=run_context.run_id,
                run_type="evaluation",
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
                    notes="Evaluation pipeline failed.",
                    error_message=str(exc),
                )
            raise


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
