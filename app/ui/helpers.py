from __future__ import annotations

import json
import math
import numbers
import re
import secrets
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from hashlib import sha256
from pathlib import Path

import pandas as pd
import streamlit as st

from app.common.artifacts import resolve_artifact_path
from app.common.disk import DiskUsageReport, measure_disk_usage
from app.common.time import today_local
from app.features.builders.flow_features import build_flow_feature_frame
from app.features.builders.fundamentals_features import build_fundamentals_feature_frame
from app.features.builders.liquidity_features import build_liquidity_feature_frame
from app.features.builders.news_features import build_news_feature_frame
from app.features.builders.price_features import build_price_feature_frame
from app.features.builders.quality_features import build_data_quality_feature_frame
from app.features.feature_store import (
    _load_feature_symbol_frame,
    _load_investor_flow_history,
    _load_latest_fundamentals,
    _load_ohlcv_history,
    _load_recent_news,
    _register_symbol_stage,
    _unregister_symbol_stage,
    load_feature_matrix,
)
from app.intraday.meta_common import (
    ENTER_PANEL,
    INTRADAY_META_MODEL_DOMAIN,
    INTRADAY_META_MODEL_VERSION,
    WAIT_PANEL,
)
from app.intraday.policy import apply_active_intraday_policy_frame
from app.ml.constants import MODEL_DOMAIN as ALPHA_MODEL_DOMAIN
from app.ml.constants import MODEL_VERSION as ALPHA_MODEL_VERSION
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ml.inference import (
    _resolve_training_run_for_inference,
    build_prediction_frame_from_training_run,
)
from app.ml.promotion import load_alpha_promotion_summary
from app.ml.registry import load_model_artifact
from app.ops.scheduler import (
    bundle_last_result_frame as scheduler_bundle_last_result_frame,
)
from app.ops.scheduler import (
    schedule_job_catalog_frame as scheduled_job_catalog_frame,
)
from app.ops.scheduler import (
    scheduler_state_frame as scheduled_state_frame,
)
from app.providers.base import ProviderHealth
from app.providers.dart.client import DartProvider
from app.providers.kis.client import KISProvider
from app.providers.krx.client import KrxProvider
from app.providers.krx.registry import KRX_SERVICE_REGISTRY
from app.providers.naver_news.client import NaverNewsProvider
from app.ranking.explanatory_score import (
    RANKING_VERSION as EXPLANATORY_RANKING_VERSION,
)
from app.ranking.explanatory_score import _load_regime_map
from app.selection.calibration import PREDICTION_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.selection.engine_v2 import build_selection_engine_v2_rankings
from app.selection.sector_outlook import sector_outlook_frame
from app.settings import Settings, load_settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import fetch_recent_runs
from app.storage.metadata_postgres import (
    fetchdf_postgres_sql,
    fetchone_postgres_sql,
    metadata_postgres_enabled,
)
from app.ui.navigation import safe_dashboard_page_keys

try:
    from streamlit.runtime.scriptrunner import get_script_run_ctx
except Exception:  # pragma: no cover - streamlit runtime import can vary outside app runs
    get_script_run_ctx = None


def _metadata_frame(
    settings: Settings,
    query: str,
    params: list[object] | None = None,
) -> pd.DataFrame:
    if metadata_postgres_enabled(settings):
        return fetchdf_postgres_sql(settings, query, params or [])
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(query, params or []).fetchdf()


def _metadata_available(settings: Settings) -> bool:
    return metadata_postgres_enabled(settings) or settings.paths.duckdb_path.exists()


def _metadata_fetchone(
    settings: Settings,
    query: str,
    params: list[object] | None = None,
):
    if metadata_postgres_enabled(settings):
        return fetchone_postgres_sql(settings, query, params or [])
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(query, params or []).fetchone()


SAFE_DASHBOARD_PAGE_KEYS: frozenset[str] = safe_dashboard_page_keys()


@dataclass(frozen=True, slots=True)
class DashboardActivityState:
    writer_active: bool
    lock_names: tuple[str, ...]
    running_job_names: tuple[str, ...]
    source: str


def _active_serial_lock_names(settings: Settings) -> tuple[str, ...]:
    lock_root = settings.paths.cache_dir / "scheduler_serial_locks"
    if not lock_root.exists():
        return ()
    return tuple(sorted(path.name for path in lock_root.iterdir() if path.is_dir()))


def dashboard_activity_state(settings: Settings) -> DashboardActivityState:
    lock_names: tuple[str, ...] = ()
    running_job_names: tuple[str, ...] = ()
    source = "filesystem"
    if metadata_postgres_enabled(settings):
        try:
            lock_frame = fetchdf_postgres_sql(
                settings,
                """
                SELECT lock_name
                FROM fact_active_lock
                WHERE released_at IS NULL
                ORDER BY acquired_at DESC
                LIMIT 20
                """,
            )
            run_frame = fetchdf_postgres_sql(
                settings,
                """
                SELECT job_name
                FROM fact_job_run
                WHERE status = 'RUNNING'
                ORDER BY started_at DESC
                LIMIT 20
                """,
            )
            lock_names = tuple(
                str(value)
                for value in lock_frame.get("lock_name", pd.Series(dtype="object")).dropna().tolist()
            )
            running_job_names = tuple(
                str(value)
                for value in run_frame.get("job_name", pd.Series(dtype="object")).dropna().tolist()
            )
            source = "metadata_postgres"
        except Exception:
            lock_names = ()
            running_job_names = ()
            source = "filesystem_fallback"
    if not lock_names and not running_job_names:
        lock_names = _active_serial_lock_names(settings)
    return DashboardActivityState(
        writer_active=bool(lock_names or running_job_names),
        lock_names=lock_names,
        running_job_names=running_job_names,
        source=source,
    )


def load_ui_base_settings(project_root: Path) -> Settings:
    settings = load_settings(project_root=project_root)
    _require_dashboard_access(settings)
    ensure_storage_layout(settings)
    return settings


def _dashboard_access_message(page_title: str, activity: DashboardActivityState) -> str:
    lock_text = ", ".join(activity.lock_names[:3]) if activity.lock_names else "-"
    running_text = ", ".join(activity.running_job_names[:3]) if activity.running_job_names else "-"
    return (
        f"`{page_title}` 화면은 현재 학습/백필 같은 쓰기 작업이 진행 중이라 잠시 잠겨 있습니다. "
        f"active_lock={lock_text}, running_jobs={running_text}. "
        "이 시간에는 `오늘`, `문서 / 도움말` 화면만 안전하게 볼 수 있습니다."
    )


def load_ui_page_context(
    project_root: Path,
    *,
    page_key: str,
    page_title: str,
) -> tuple[Settings, DashboardActivityState]:
    base_settings = load_ui_base_settings(project_root)
    activity = dashboard_activity_state(base_settings)
    if activity.writer_active and page_key not in SAFE_DASHBOARD_PAGE_KEYS:
        st.warning(_dashboard_access_message(page_title, activity))
        st.stop()
    if activity.writer_active:
        return base_settings, activity
    return load_ui_settings(project_root), activity


def _latest_manifest_preview(settings: Settings, *, run_type: str) -> str | None:
    if not _metadata_available(settings):
        return None
    row = _metadata_fetchone(
        settings,
        """
        SELECT output_artifacts_json
        FROM ops_run_manifest
        WHERE run_type = ?
          AND status = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """,
        [run_type],
    )
    if row is None or not row[0]:
        return None
    artifacts = json.loads(row[0])
    preview_candidates = [
        resolve_artifact_path(settings, item)
        for item in artifacts
        if str(item).endswith(".md")
    ]
    preview_candidates = [path for path in preview_candidates if path is not None]
    if not preview_candidates:
        return None
    preview_path = preview_candidates[-1]
    return preview_path.read_text(encoding="utf-8")


def latest_job_runs_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                run_id,
                job_name,
                trigger_type,
                status,
                as_of_date,
                started_at,
                finished_at,
                step_count,
                failed_step_count,
                notes,
                error_message
            FROM fact_job_run
            ORDER BY started_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_step_failure_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                job_run_id,
                step_name,
                status,
                started_at,
                finished_at,
                error_message,
                notes
            FROM fact_job_step_run
            WHERE status = 'FAILED'
            ORDER BY started_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_pipeline_dependency_frame(settings: Settings, limit: int = 50) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                pipeline_name,
                dependency_name,
                status,
                ready_flag,
                required_state,
                observed_state,
                checked_at
            FROM vw_latest_pipeline_dependency_state
            ORDER BY pipeline_name, dependency_name
            LIMIT ?
        """,
        [limit],
    )


def latest_health_snapshot_frame(settings: Settings, limit: int = 100) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                health_scope,
                component_name,
                status,
                metric_name,
                metric_value_double,
                metric_value_text,
                snapshot_at
            FROM vw_latest_health_snapshot
            ORDER BY health_scope, component_name, metric_name
            LIMIT ?
        """,
        [limit],
    )


def latest_disk_watermark_event_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                measured_at,
                disk_status,
                usage_ratio,
                used_gb,
                available_gb,
                cleanup_required_flag,
                emergency_block_flag,
                notes
            FROM fact_disk_watermark_event
            ORDER BY measured_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_retention_cleanup_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                started_at,
                finished_at,
                status,
                dry_run,
                cleanup_scope,
                removed_file_count,
                reclaimed_bytes,
                notes
            FROM fact_retention_cleanup_run
            ORDER BY started_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_active_lock_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                lock_name,
                job_name,
                owner_run_id,
                acquired_at,
                expires_at,
                status,
                release_reason
            FROM fact_active_lock
            WHERE released_at IS NULL
            ORDER BY acquired_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_recovery_queue_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                recovery_action_id,
                action_type,
                status,
                target_job_run_id,
                triggered_by_run_id,
                recovery_run_id,
                lock_name,
                created_at,
                finished_at,
                notes
            FROM fact_recovery_action
            ORDER BY created_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_alert_event_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                created_at,
                alert_type,
                severity,
                component_name,
                status,
                message,
                resolved_at
            FROM fact_alert_event
            ORDER BY created_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_active_ops_policy_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                policy_id,
                policy_version,
                policy_name,
                policy_path,
                active_flag,
                promotion_type,
                effective_from_at,
                effective_to_at,
                note
            FROM fact_active_ops_policy
            ORDER BY effective_from_at DESC
            LIMIT ?
        """,
        [limit],
    )


def scheduler_job_catalog_frame(settings: Settings) -> pd.DataFrame:
    return scheduled_job_catalog_frame(settings)


def latest_scheduler_state_frame(settings: Settings, limit: int = 50) -> pd.DataFrame:
    return scheduled_state_frame(settings, limit=limit)


def latest_scheduler_bundle_result_frame(settings: Settings, limit: int = 50) -> pd.DataFrame:
    return scheduler_bundle_last_result_frame(settings, limit=limit)


def latest_intraday_policy_apply_compare_frame(settings: Settings, limit: int = 30) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_recommendation AS (
                SELECT *
                FROM vw_latest_intraday_policy_selection_recommendation
                WHERE recommendation_rank = 1
            ),
            active_policy AS (
                SELECT
                    active.horizon,
                    active.scope_type,
                    active.scope_key,
                    active.policy_candidate_id AS active_policy_candidate_id,
                    candidate.template_id AS active_template_id,
                    active.source_recommendation_date,
                    active.effective_from_date,
                    active.note AS active_note
                FROM fact_intraday_active_policy AS active
                LEFT JOIN fact_intraday_policy_candidate AS candidate
                  ON active.policy_candidate_id = candidate.policy_candidate_id
                WHERE active.active_flag = TRUE
            ),
            latest_metrics AS (
                SELECT *
                FROM vw_latest_intraday_policy_evaluation
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY policy_candidate_id
                    ORDER BY
                        CASE split_name
                            WHEN 'test' THEN 1
                            WHEN 'validation' THEN 2
                            ELSE 3
                        END,
                        window_end_date DESC,
                        created_at DESC
                ) = 1
            )
            SELECT
                COALESCE(active_policy.horizon, latest_recommendation.horizon) AS horizon,
                COALESCE(active_policy.scope_type, latest_recommendation.scope_type) AS scope_type,
                COALESCE(active_policy.scope_key, latest_recommendation.scope_key) AS scope_key,
                active_policy.active_policy_candidate_id,
                active_policy.active_template_id,
                active_policy.source_recommendation_date,
                active_policy.effective_from_date,
                latest_recommendation.recommendation_date,
                latest_recommendation.policy_candidate_id AS recommended_policy_candidate_id,
                latest_recommendation.template_id AS recommended_template_id,
                active_metric.objective_score AS before_objective_score,
                latest_metric.objective_score AS after_objective_score,
                latest_metric.objective_score - active_metric.objective_score
                    AS objective_score_delta,
                active_metric.mean_realized_excess_return AS before_mean_excess_return,
                latest_metric.mean_realized_excess_return AS after_mean_excess_return,
                latest_metric.mean_realized_excess_return
                    - active_metric.mean_realized_excess_return
                    AS mean_excess_return_delta,
                active_metric.hit_rate AS before_hit_rate,
                latest_metric.hit_rate AS after_hit_rate,
                latest_metric.hit_rate - active_metric.hit_rate AS hit_rate_delta,
                active_metric.execution_rate AS before_execution_rate,
                latest_metric.execution_rate AS after_execution_rate,
                latest_metric.execution_rate - active_metric.execution_rate
                    AS execution_rate_delta,
                COALESCE(latest_metric.manual_review_required_flag, FALSE)
                    AS manual_review_required_flag
            FROM active_policy
            FULL OUTER JOIN latest_recommendation
              ON active_policy.horizon = latest_recommendation.horizon
             AND active_policy.scope_type = latest_recommendation.scope_type
             AND COALESCE(active_policy.scope_key, '') = COALESCE(
                 latest_recommendation.scope_key,
                 ''
             )
            LEFT JOIN latest_metrics AS active_metric
              ON active_policy.active_policy_candidate_id = active_metric.policy_candidate_id
            LEFT JOIN latest_metrics AS latest_metric
              ON latest_recommendation.policy_candidate_id = latest_metric.policy_candidate_id
            ORDER BY horizon, scope_type, scope_key
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_meta_apply_compare_frame(settings: Settings, limit: int = 30) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_candidate AS (
                SELECT
                    training_run_id,
                    horizon,
                    panel_name,
                    train_end_date,
                    validation_row_count,
                    validation_session_count,
                    feature_count,
                    fallback_flag,
                    fallback_reason
                FROM vw_latest_model_training_run
                WHERE model_domain = ?
                  AND model_version = ?
            ),
            active_model AS (
                SELECT
                    horizon,
                    panel_name,
                    active_meta_model_id,
                    training_run_id AS active_training_run_id,
                    effective_from_date,
                    note AS active_note
                FROM vw_latest_intraday_active_meta_model
            ),
            metric_panel AS (
                SELECT
                    training_run_id,
                    MAX(CASE WHEN metric_name = 'macro_f1' THEN metric_value END) AS macro_f1,
                    MAX(CASE WHEN metric_name = 'log_loss' THEN metric_value END) AS log_loss
                FROM vw_latest_model_metric_summary
                WHERE model_domain = ?
                  AND model_version = ?
                  AND split_name = 'validation'
                  AND metric_scope = 'panel'
                GROUP BY training_run_id
            )
            SELECT
                COALESCE(active_model.horizon, latest_candidate.horizon) AS horizon,
                COALESCE(active_model.panel_name, latest_candidate.panel_name) AS panel_name,
                active_model.active_meta_model_id,
                active_model.active_training_run_id,
                active_model.effective_from_date,
                latest_candidate.training_run_id AS candidate_training_run_id,
                latest_candidate.train_end_date,
                active_metric.macro_f1 AS before_macro_f1,
                candidate_metric.macro_f1 AS after_macro_f1,
                candidate_metric.macro_f1 - active_metric.macro_f1 AS macro_f1_delta,
                active_metric.log_loss AS before_log_loss,
                candidate_metric.log_loss AS after_log_loss,
                candidate_metric.log_loss - active_metric.log_loss AS log_loss_delta,
                latest_candidate.validation_row_count,
                latest_candidate.validation_session_count,
                latest_candidate.feature_count,
                latest_candidate.fallback_flag,
                latest_candidate.fallback_reason
            FROM active_model
            FULL OUTER JOIN latest_candidate
              ON active_model.horizon = latest_candidate.horizon
             AND active_model.panel_name = latest_candidate.panel_name
            LEFT JOIN metric_panel AS active_metric
              ON active_model.active_training_run_id = active_metric.training_run_id
            LEFT JOIN metric_panel AS candidate_metric
              ON latest_candidate.training_run_id = candidate_metric.training_run_id
            ORDER BY horizon, panel_name
            LIMIT ?
            """,
            [
                INTRADAY_META_MODEL_DOMAIN,
                INTRADAY_META_MODEL_VERSION,
                INTRADAY_META_MODEL_DOMAIN,
                INTRADAY_META_MODEL_VERSION,
                limit,
            ],
        ).fetchdf()


def latest_successful_pipeline_output_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                component_name,
                status,
                metric_value_text,
                snapshot_at
            FROM vw_latest_health_snapshot
            WHERE health_scope = 'pipeline'
              AND metric_name = 'latest_successful_output'
            ORDER BY component_name
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_app_snapshot_frame(settings: Settings) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT *
            FROM vw_latest_app_snapshot
        """,
    )


def latest_report_index_frame(
    settings: Settings,
    *,
    limit: int = 20,
    report_type: str | None = None,
    latest_only: bool = False,
) -> pd.DataFrame:
    source = "vw_latest_report_index" if latest_only else "fact_latest_report_index"
    where_clause = ""
    params: list[object] = []
    if report_type:
        where_clause = "WHERE report_type = ?"
        params.append(report_type)
    params.append(limit)
    return _metadata_frame(
        settings,
        f"""
            SELECT
                report_type,
                report_key,
                as_of_date,
                generated_ts,
                status,
                run_id,
                artifact_path,
                artifact_format,
                published_flag,
                dry_run_flag,
                summary_json
            FROM {source}
            {where_clause}
            ORDER BY generated_ts DESC, created_at DESC
            LIMIT ?
        """,
        params,
    )


def latest_release_candidate_check_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    return _metadata_frame(
        settings,
        """
            SELECT
                check_ts,
                check_name,
                status,
                severity,
                recommended_action,
                detail_json
            FROM fact_release_candidate_check
            ORDER BY check_ts DESC, check_name
            LIMIT ?
        """,
        [limit],
    )


def latest_ui_freshness_frame(
    settings: Settings,
    *,
    page_name: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    where_clause = ""
    params: list[object] = []
    if page_name:
        where_clause = "WHERE page_name = ?"
        params.append(page_name)
    params.append(limit)
    return _metadata_frame(
        settings,
        f"""
            SELECT
                snapshot_ts,
                page_name,
                dataset_name,
                latest_available_ts,
                freshness_seconds,
                stale_flag,
                warning_level,
                notes
            FROM vw_latest_ui_data_freshness_snapshot
            {where_clause}
            ORDER BY page_name, dataset_name
            LIMIT ?
        """,
        params,
    )


HOME_OPERATIONAL_CRITICAL_FRESHNESS_KEYS: tuple[tuple[str, str], ...] = (
    ("오늘", "selection_v2"),
    ("오늘", "report_index"),
    ("시장 현황", "market_regime"),
    ("시장 현황", "market_news"),
    ("리더보드", "selection_v2"),
    ("포트폴리오", "target_book"),
    ("포트폴리오", "nav_snapshot"),
    ("장중 콘솔", "intraday_final_action"),
    ("운영", "health_snapshot"),
    ("운영", "job_run"),
    ("헬스 대시보드", "health_snapshot"),
)


def home_banner_freshness_levels(freshness: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if freshness.empty:
        return freshness, freshness

    normalized = freshness.copy()
    normalized["warning_level"] = normalized["warning_level"].astype(str).str.upper()
    operational_mask = normalized.apply(
        lambda row: (str(row.get("page_name")), str(row.get("dataset_name")))
        in HOME_OPERATIONAL_CRITICAL_FRESHNESS_KEYS,
        axis=1,
    )
    critical = normalized[
        (normalized["warning_level"] == "CRITICAL") & operational_mask
    ].copy()
    warning = normalized[
        (normalized["warning_level"] == "WARNING")
        | ((normalized["warning_level"] == "CRITICAL") & ~operational_mask)
    ].copy()
    return critical, warning


def latest_ops_report_preview(settings: Settings) -> str | None:
    report_root = settings.paths.artifacts_dir / "ops" / "report"
    if not report_root.exists():
        return None
    previews = sorted(report_root.rglob("ops_report_preview.md"), reverse=True)
    if not previews:
        return None
    return previews[0].read_text(encoding="utf-8")


def latest_daily_research_report_preview(settings: Settings) -> str | None:
    report_root = settings.paths.artifacts_dir / "daily_research_report"
    if not report_root.exists():
        return None
    previews = sorted(report_root.rglob("daily_research_report_preview.md"), reverse=True)
    if not previews:
        return None
    return previews[0].read_text(encoding="utf-8")


def latest_evaluation_report_preview(settings: Settings) -> str | None:
    report_root = settings.paths.artifacts_dir / "evaluation_report"
    if not report_root.exists():
        return None
    previews = sorted(report_root.rglob("evaluation_report_preview.md"), reverse=True)
    if not previews:
        return None
    return previews[0].read_text(encoding="utf-8")


def latest_intraday_summary_report_preview(settings: Settings) -> str | None:
    report_root = settings.paths.artifacts_dir / "intraday_summary_report"
    if not report_root.exists():
        return None
    previews = sorted(report_root.rglob("intraday_summary_report_preview.md"), reverse=True)
    if not previews:
        return None
    return previews[0].read_text(encoding="utf-8")


def latest_release_candidate_preview(settings: Settings) -> str | None:
    report_root = settings.paths.artifacts_dir / "release_candidate_checklist"
    if not report_root.exists():
        return None
    previews = sorted(report_root.rglob("release_candidate_checklist_preview.md"), reverse=True)
    if not previews:
        return None
    return previews[0].read_text(encoding="utf-8")


def load_ui_settings(project_root: Path, *, bootstrap_duckdb: bool = True) -> Settings:
    settings = load_ui_base_settings(project_root)
    if not bootstrap_duckdb:
        return settings
    read_only = settings.paths.duckdb_path.exists()
    with duckdb_connection(settings.paths.duckdb_path, read_only=read_only) as connection:
        bootstrap_core_tables(connection)
    return settings


def _streamlit_runtime_active() -> bool:
    if get_script_run_ctx is None:
        return False
    return get_script_run_ctx() is not None


def _dashboard_access_fingerprint(settings: Settings) -> str:
    access = settings.dashboard_access
    raw = f"{access.username}\0{access.password or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _require_dashboard_access(settings: Settings) -> None:
    access = settings.dashboard_access
    if not access.enabled or not _streamlit_runtime_active():
        return

    session_key = "_dashboard_access_fingerprint"
    auth_fingerprint = _dashboard_access_fingerprint(settings)
    if st.session_state.get(session_key) == auth_fingerprint:
        return

    st.title("Dashboard Sign In")
    st.caption("This dashboard is protected. Sign in to continue.")
    with st.form("dashboard_access_form", clear_on_submit=False):
        username = st.text_input("Username", value="")
        password = st.text_input("Password", type="password", value="")
        submitted = st.form_submit_button("Sign in", use_container_width=True)
        if submitted:
            username_ok = secrets.compare_digest(username.strip(), access.username)
            password_ok = secrets.compare_digest(password, access.password or "")
            if username_ok and password_ok:
                st.session_state[session_key] = auth_fingerprint
                st.rerun()
            st.error("Invalid dashboard credentials.")
    st.stop()


UI_COLUMN_LABELS: dict[str, str] = {
    "page_name": "페이지",
    "dataset_name": "데이터셋",
    "warning_level": "경고 수준",
    "stale_flag": "지연 여부",
    "freshness_seconds": "지연 시간(초)",
    "latest_available_ts": "최신 가용 시각",
    "check_ts": "점검 시각",
    "check_name": "체크 이름",
    "severity": "심각도",
    "recommended_action": "권장 조치",
    "report_type": "리포트 종류",
    "report_key": "리포트 키",
    "generated_ts": "생성 시각",
    "artifact_path": "파일 위치",
    "artifact_format": "파일 형식",
    "published_flag": "발행 여부",
    "dry_run_flag": "시험 실행 여부",
    "summary_json": "한눈 요약",
    "snapshot_ts": "스냅샷 시각",
    "latest_daily_bundle_run_id": "최근 일일 배치 실행 ID",
    "latest_daily_bundle_status": "최근 일일 배치 상태",
    "latest_evaluation_run_id": "최근 평가 실행 ID",
    "latest_intraday_session_date": "최근 장중 세션일",
    "latest_intraday_run_id": "최근 장중 실행 ID",
    "latest_portfolio_as_of_date": "최근 포트폴리오 기준일",
    "latest_portfolio_run_id": "최근 포트폴리오 실행 ID",
    "active_intraday_policy_id": "활성 장중 정책",
    "active_meta_model_ids_json": "사용 중인 메타 모델",
    "active_portfolio_policy_id": "활성 포트폴리오 정책",
    "active_ops_policy_id": "활성 운영 정책",
    "health_status": "헬스 상태",
    "market_regime_family": "시장 흐름",
    "top_actionable_symbol_list_json": "지금 볼 종목",
    "latest_report_bundle_id": "최근 리포트 묶음",
    "critical_alert_count": "치명 알림 수",
    "warning_alert_count": "경고 알림 수",
    "threshold": "임계치",
    "ratio": "비율",
    "provider": "제공처",
    "configured": "설정됨",
    "status": "상태",
    "detail": "상세",
    "total_symbols": "전체 종목",
    "kospi_symbols": "코스피 종목",
    "kosdaq_symbols": "코스닥 종목",
    "dart_mapped_symbols": "DART 매핑 종목",
    "active_common_stock_count": "활성 보통주 수",
    "min_trading_date": "최소 거래일",
    "max_trading_date": "최대 거래일",
    "total_days": "전체 일수",
    "trading_days": "거래일 수",
    "override_days": "오버라이드 일수",
    "run_type": "실행 종류",
    "run_id": "실행 ID",
    "job_run_id": "작업 실행 ID",
    "owner_run_id": "잠금 보유 실행 ID",
    "target_job_run_id": "복구 대상 실행 ID",
    "triggered_by_run_id": "복구 요청 실행 ID",
    "recovery_run_id": "복구 실행 ID",
    "started_at": "시작 시각",
    "finished_at": "종료 시각",
    "notes": "메모",
    "error_message": "오류 메시지",
    "latest_ohlcv_date": "최신 시세 날짜",
    "detail_json": "세부 내용",
    "explanatory_score_json": "점수 근거",
    "ensemble_weight_json": "모델별 반영 비중",
    "source_notes_json": "참고 메모",
    "blocked_reason": "제외 사유",
    "latest_ohlcv_rows": "최신 OHLCV 행수",
    "latest_fundamentals_date": "최신 재무 날짜",
    "latest_fundamentals_rows": "최신 재무 행수",
    "latest_news_date": "최신 뉴스 날짜",
    "latest_news_rows": "최신 뉴스 행수",
    "latest_news_unmatched": "최신 뉴스 미매칭",
    "latest_flow_date": "최신 수급 날짜",
    "latest_flow_rows": "최신 수급 행수",
    "latest_feature_date": "최신 피처 날짜",
    "latest_feature_rows": "최신 피처 행수",
    "latest_label_date": "최신 라벨 날짜",
    "latest_available_label_rows": "최신 사용 가능 라벨 행수",
    "latest_regime_date": "최신 시장 상태 날짜",
    "latest_explanatory_ranking_date": "최신 설명형 순위 날짜",
    "latest_explanatory_ranking_rows": "최신 설명형 순위 행수",
    "latest_selection_date": "최신 선정 엔진 날짜",
    "latest_selection_rows": "최신 선정 엔진 행수",
    "latest_prediction_date": "최신 예측 밴드 날짜",
    "latest_prediction_rows": "최신 예측 밴드 행수",
    "latest_outcome_date": "최신 성과 날짜",
    "latest_outcome_rows": "최신 성과 행수",
    "latest_evaluation_summary_date": "최신 평가 요약 날짜",
    "latest_evaluation_summary_rows": "최신 평가 요약 행수",
    "latest_calibration_date": "최신 보정 진단 날짜",
    "latest_calibration_rows": "최신 보정 진단 행수",
    "feature_name": "피처명",
    "symbol_rows": "종목 수",
    "null_ratio": "결측 비율",
    "as_of_date": "기준일",
    "market_scope": "시장 범위",
    "regime_state": "시장 상태",
    "regime_score": "상태 점수",
    "breadth_up_ratio": "상승 종목 비율",
    "median_symbol_return_1d": "중앙값 1일 수익률",
    "market_realized_vol_20d": "20일 시장 실현 변동성",
    "turnover_burst_z": "거래대금 급증 Z",
    "latest_feature_version": "최신 피처 버전",
    "latest_ranking_version": "최신 설명형 순위 버전",
        "latest_selection_version": "최신 추천 모델 버전",
    "latest_prediction_version": "최신 예측 밴드 버전",
    "trading_date": "거래일",
    "signal_date": "신호일",
    "published_at": "발행 시각",
    "title": "제목",
    "publisher": "언론사",
    "query_bucket": "쿼리 묶음",
    "link": "링크",
    "symbol": "종목코드",
    "company_name": "종목명",
    "market": "시장",
    "sector": "섹터",
    "industry": "업종",
    "horizon": "기간",
    "final_selection_value": "최종 선택 점수",
    "final_selection_rank_pct": "선택 상위 비율",
    "grade": "등급",
    "ranking_version": "순위 버전",
    "reasons": "주요 사유",
    "risks": "위험 신호",
    "expected_excess_return": "예상 초과수익률",
    "expected_excess_return_at_selection": "선정 시 예상 초과수익률",
    "lower_band": "하단 밴드",
    "median_band": "중앙 밴드",
    "upper_band": "상단 밴드",
    "outcome_status": "성과 상태",
    "realized_excess_return": "실현 초과수익률",
    "band_status": "밴드 판정",
    "row_count": "행수",
    "foreign_value_coverage": "외국인 금액 커버리지",
    "institution_value_coverage": "기관 금액 커버리지",
    "individual_value_coverage": "개인 금액 커버리지",
    "avg_expected_excess_return": "평균 예상 초과수익률",
    "avg_band_width": "평균 밴드 폭",
    "start_date": "시작일",
    "end_date": "종료일",
    "bucket_type": "구간 유형",
    "bucket_name": "구간명",
    "symbol_count": "종목 수",
    "avg_gross_forward_return": "평균 총 수익률",
    "avg_excess_forward_return": "평균 초과수익률",
    "median_excess_forward_return": "중앙값 초과수익률",
    "hit_rate": "적중률",
    "avg_prediction_error": "평균 예측 오차",
    "top_decile_gap": "상하위 10% 격차",
    "evaluation_date": "평가일",
    "matured_rows": "평가 완료 행수",
    "summary_date": "요약일",
    "window_type": "집계 창",
    "segment_value": "세그먼트",
    "count_evaluated": "평가 완료 수",
    "selection_avg_excess": "추천 모델 평균 초과수익률",
    "explanatory_avg_excess": "설명형 평균 초과수익률",
    "avg_excess_gap": "평균 초과수익률 차이",
    "hit_rate_gap": "적중률 차이",
    "diagnostic_date": "진단일",
    "bin_type": "구간 유형",
    "bin_value": "구간값",
    "sample_count": "표본 수",
    "expected_median": "예상 중앙값",
    "observed_mean": "관측 평균",
    "coverage_rate": "커버리지",
    "median_bias": "중앙값 편향",
    "quality_flag": "품질 플래그",
    "selection_date": "선정일",
    "next_entry_trade_date": "진입 예정일",
    "selection_close_price": "추천 기준 종가",
    "flat_target_price": "참고 목표가(시장 보합 가정)",
    "flat_upper_target_price": "강한 흐름 목표가(시장 보합 가정)",
    "flat_stop_price": "참고 손절선(시장 보합 가정)",
    "active_alpha_model_id": "사용 중인 알파 모델 ID",
    "model_spec_id": "사용 모델 방식",
    "plan_horizon": "관찰 기간",
    "entry_trade_date": "진입 예정일",
    "exit_trade_date": "관찰 종료일",
    "entry_basis": "진입 기준",
    "exit_basis": "종료 기준",
    "action_plan_label": "추천 행동",
    "target_return": "기본 목표수익 참고치",
    "stretch_target_return": "강한 흐름 목표수익 참고치",
    "stop_return": "손절 참고치",
    "action_target_price": "기본 목표가",
    "action_stretch_price": "강한 흐름 목표가",
    "action_stop_price": "손절 참고선",
    "investor_flow_rows": "수급 행수",
    "foreign_positive_ratio": "외국인 순매수 비율",
    "institution_positive_ratio": "기관 순매수 비율",
    "selection_rows": "선정 엔진 행수",
    "outlook_label": "강세 예상 업종",
    "broad_sector": "상위 섹터",
    "sample_symbols": "대표 종목",
    "top10_count": "상위 10위 내 종목 수",
    "rank_weight_sum": "상위권 집중도",
    "outlook_score": "섹터 강도 점수",
    "prediction_rows": "예측 밴드 행수",
    "open": "시가",
    "high": "고가",
    "low": "저가",
    "close": "종가",
    "volume": "거래량",
    "turnover_value": "거래대금",
    "revenue": "매출액",
    "operating_income": "영업이익",
    "net_income": "순이익",
    "roe": "ROE",
    "debt_ratio": "부채비율",
    "ret_5d": "5일 수익률",
    "ret_20d": "20일 수익률",
    "adv_20": "20일 평균 거래대금",
    "news_count_3d": "3일 뉴스 수",
    "foreign_net_value_ratio_5d": "5일 외국인 순매수 비율",
    "smart_money_flow_ratio_20d": "20일 스마트머니 수급 비율",
    "flow_coverage_flag": "수급 커버리지",
    "d1_selection_value": "1거래일 기준 선택 점수",
    "d1_grade": "1거래일 기준 등급",
    "d5_selection_value": "5거래일 기준 선택 점수",
    "d5_grade": "5거래일 기준 등급",
    "d5_expected_excess_return": "5거래일 기준 예상 초과수익률",
    "d5_lower_band": "5거래일 기준 하단 범위",
    "d5_upper_band": "5거래일 기준 상단 범위",
    "d1_realized_excess_return": "1거래일 뒤 실현 초과수익률",
    "d1_band_status": "1거래일 기준 범위 판정",
    "d5_realized_excess_return": "5거래일 뒤 실현 초과수익률",
    "d5_band_status": "5거래일 기준 범위 판정",
    "foreign_net_value": "외국인 순매수금액",
    "institution_net_value": "기관 순매수금액",
    "individual_net_value": "개인 순매수금액",
    "foreign_net_volume": "외국인 순매수수량",
    "institution_net_volume": "기관 순매수수량",
    "individual_net_volume": "개인 순매수수량",
}

UI_VALUE_LABELS: dict[str, dict[str, str]] = {
    "threshold": {
        "warning": "경고",
        "prune": "정리",
        "limit": "한계",
    },
    "provider": {
        "kis": "한국투자",
        "dart": "DART",
        "krx": "KRX",
        "naver_news": "네이버 뉴스",
        "KIS": "한국투자",
        "DART": "DART",
        "KRX": "KRX",
        "NAVER_NEWS": "네이버 뉴스",
    },
    "status": {
        "normal": "정상",
        "warning": "주의",
        "prune": "정리 필요",
        "limit": "한계",
        "success": "성공",
        "failed": "실패",
        "error": "오류",
        "ok": "정상",
        "running": "실행 중",
        "pending": "대기",
        "healthy": "정상",
        "unhealthy": "비정상",
    },
    "market": {
        "ALL": "전체",
        "KOSPI": "코스피",
        "KOSDAQ": "코스닥",
    },
    "market_scope": {
        "KR_ALL": "국내 전체",
        "KOSPI": "코스피",
        "KOSDAQ": "코스닥",
    },
    "ranking_version": {
        EXPLANATORY_RANKING_VERSION: "설명형 순위 v0",
        SELECTION_ENGINE_VERSION: "선정 엔진 v1",
    },
    "prediction_version": {
        PREDICTION_VERSION: "예상 범위 모델 v1",
    },
    "run_type": {
        "bootstrap": "초기화",
        "sync_universe": "종목 유니버스 동기화",
        "sync_trading_calendar": "거래일 캘린더 동기화",
        "provider_smoke_check": "프로바이더 스모크 체크",
        "sync_daily_ohlcv": "일봉 동기화",
        "sync_fundamentals_snapshot": "재무 스냅샷 동기화",
        "sync_news_metadata": "뉴스 메타데이터 동기화",
        "sync_investor_flow": "수급 데이터 동기화",
        "build_feature_store": "피처 스토어 생성",
        "build_forward_labels": "미래 수익률 라벨 생성",
        "build_market_regime_snapshot": "시장 상태 스냅샷 생성",
        "materialize_explanatory_ranking": "설명형 순위 생성",
        "validate_explanatory_ranking": "설명형 순위 검증",
        "materialize_selection_engine_v1": "이전 추천 모델 계산",
        "calibrate_proxy_prediction_bands": "참고 범위 보정",
        "validate_selection_engine_v1": "기존 추천 모델 검증",
        "render_discord_eod_report": "디스코드 장마감 요약 만들기",
        "publish_discord_eod_report": "디스코드 장마감 요약 보내기",
        "render_discord_close_brief": "디스코드 마감 직후 브리핑 만들기",
        "publish_discord_close_brief": "디스코드 마감 직후 브리핑 보내기",
        "materialize_selection_outcomes": "추천 결과 사후 기록 생성",
        "materialize_prediction_evaluation": "예측 사후 평가 요약 생성",
        "materialize_calibration_diagnostics": "예측 범위 진단 생성",
        "render_postmortem_report": "사후 점검 리포트 만들기",
        "publish_discord_postmortem_report": "디스코드 사후 점검 보내기",
        "validate_evaluation_pipeline": "평가 파이프라인 검증",
        "run_daily_pipeline": "일일 파이프라인 실행",
        "run_evaluation": "평가 실행",
        "prune_storage": "저장소 정리",
    },
    "regime_state": {
        "panic": "공포 장",
        "risk_off": "방어 우위 장",
        "neutral": "방향성 탐색 장",
        "risk_on": "상승 우위 장",
        "euphoria": "과열 장",
    },
    "outcome_status": {
        "matured": "평가 완료",
        "pending": "대기",
        "unavailable": "평가 불가",
    },
    "band_status": {
        "in_band": "예상 범위 안",
        "above_upper": "예상보다 강함",
        "below_lower": "예상보다 약함",
        "band_missing": "예상 범위 없음",
        "label_pending": "아직 결과 대기",
    },
    "window_type": {
        "cohort": "코호트",
        "rolling_20d": "20거래일 롤링",
        "rolling_60d": "60거래일 롤링",
    },
    "segment_value": {
        "all": "전체",
        "top_decile": "상위 10%",
        "report_candidates": "리포트 후보",
    },
    "bucket_type": {
        "grade": "등급",
        "decile": "10분위",
        "overall": "전체",
        "expected_return_bin": "예상수익 구간",
    },
    "quality_flag": {
        "ok": "양호",
        "coverage_drift": "예상 범위 적중 흔들림",
        "low_sample": "표본 부족",
        "band_missing": "예상 범위 없음",
    },
}

UI_VALUE_LABELS.setdefault("ranking_version", {}).update(
    {
        SELECTION_ENGINE_V2_VERSION: "선정 엔진 v2",
    }
)
UI_VALUE_LABELS.setdefault("prediction_version", {}).update(
    {
        ALPHA_PREDICTION_VERSION: "ML 알파 예측 v1",
    }
)
UI_VALUE_LABELS.setdefault("status", {}).update(
    {
        "SKIPPED_NON_TRADING_DAY": "휴장일로 건너뜀",
        "SKIPPED_ALREADY_DONE": "이미 완료되어 건너뜀",
        "SKIPPED_LOCKED": "다른 작업이 먼저 진행 중",
    }
)
UI_VALUE_LABELS.setdefault("job_key", {}).update(
    {
        "ops_maintenance": "운영 유지보수",
        "news_morning": "아침 뉴스 수집",
        "intraday_assist": "장중 후보군 보조",
        "news_after_close": "마감 직후 뉴스 수집",
        "evaluation": "장후 평가",
        "daily_close": "장후 추천 생성",
        "daily_audit_lite": "일일 경량 감사",
        "weekly_training_candidate": "주간 학습 후보 생성",
        "weekly_calibration": "주간 보정/정책 실험",
    }
)
UI_VALUE_LABELS.setdefault("job_name", {}).update(
    {
        "run_news_sync_bundle": "뉴스 수집 번들",
        "run_daily_close_bundle": "장후 추천 번들",
        "run_evaluation_bundle": "장후 평가 번들",
        "run_intraday_assist_bundle": "장중 후보군 보조 번들",
        "run_weekly_training_bundle": "주간 학습 후보 번들",
        "run_weekly_calibration_bundle": "주간 보정 번들",
        "run_daily_audit_lite_bundle": "일일 경량 감사 번들",
    }
)
UI_VALUE_LABELS.setdefault("date_semantics", {}).update(
    {
        "calendar_day": "달력일 기준",
        "trading_day": "거래일 기준",
        "hybrid": "혼합 기준",
    }
)

UI_COLUMN_LABELS.update(
    {
        "schedule_label": "실행 주기",
        "next_run_at": "다음 실행 예정",
        "last_status": "최근 상태",
        "last_finished_at": "최근 종료 시각",
        "last_notes": "최근 메모",
        "last_run_id": "최근 실행 ID",
        "date_semantics": "날짜 기준",
        "trading_day_required": "거래일 전용",
        "heavy_job": "무거운 작업",
        "manual_local_command": "로컬 수동 실행",
        "manual_server_command": "서버 수동 실행",
        "timer_name": "systemd 타이머",
        "service_name": "systemd 서비스",
        "on_calendar": "OnCalendar",
        "identity_json": "실행 대상",
        "active_policy_candidate_id": "현재 활성 정책 후보 ID",
        "active_template_id": "현재 활성 템플릿",
        "source_recommendation_date": "현재 반영 추천일",
        "recommendation_date": "새 추천일",
        "recommended_policy_candidate_id": "새 추천 정책 후보 ID",
        "recommended_template_id": "새 추천 템플릿",
        "before_objective_score": "현재 목표 점수",
        "after_objective_score": "추천 목표 점수",
        "objective_score_delta": "목표 점수 변화",
        "before_mean_excess_return": "현재 평균 초과수익률",
        "after_mean_excess_return": "추천 평균 초과수익률",
        "mean_excess_return_delta": "평균 초과수익률 변화",
        "before_hit_rate": "현재 적중률",
        "after_hit_rate": "추천 적중률",
        "hit_rate_delta": "적중률 변화",
        "before_execution_rate": "현재 실행률",
        "after_execution_rate": "추천 실행률",
        "execution_rate_delta": "실행률 변화",
        "active_training_run_id": "현재 활성 학습 실행 ID",
        "candidate_training_run_id": "새 학습 후보 실행 ID",
        "before_macro_f1": "현재 종합 분류 점수",
        "after_macro_f1": "후보 종합 분류 점수",
        "macro_f1_delta": "종합 분류 점수 변화",
        "before_log_loss": "현재 확률 오차",
        "after_log_loss": "후보 확률 오차",
        "log_loss_delta": "확률 오차 변화",
        "validation_session_count": "검증 세션 수",
    }
)
UI_VALUE_LABELS.setdefault("run_type", {}).update(
    {
        "build_model_training_dataset": "모델 학습 데이터셋 생성",
        "train_alpha_model_v1": "ML 알파 모델 학습",
        "backfill_alpha_oof_predictions": "알파 OOF 백필",
        "materialize_alpha_predictions_v1": "ML 알파 추론 생성",
        "materialize_selection_engine_v2": "선정 엔진 v2 생성",
        "validate_alpha_model_v1": "알파 모델 검증",
        "compare_selection_engines": "선정 엔진 비교",
        "render_model_diagnostic_report": "모델 진단 리포트 만들기",
        "materialize_intraday_market_context_snapshots": "장중 시장 컨텍스트 생성",
        "materialize_intraday_regime_adjustments": "장중 레짐 조정 생성",
        "materialize_intraday_adjusted_entry_decisions": "장중 조정 진입 판단 생성",
        "materialize_intraday_decision_outcomes": "장중 판단 성과 생성",
        "evaluate_intraday_strategy_comparison": "장중 전략 비교 평가",
        "materialize_intraday_timing_calibration": "장중 타이밍 보정 진단 생성",
        "render_intraday_postmortem_report": "장중 사후 분석 리포트 만들기",
        "publish_discord_intraday_postmortem": "장중 사후 분석 디스코드 보내기",
        "validate_intraday_strategy_pipeline": "장중 전략 파이프라인 검증",
    }
)


def _latest_portfolio_as_of_date(settings: Settings):
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            "SELECT MAX(as_of_date) FROM fact_portfolio_target_book"
        ).fetchone()
    return None if not row or row[0] is None else pd.Timestamp(row[0]).date()


def _latest_portfolio_session_date(settings: Settings, *, as_of_date=None):
    target_date = as_of_date or _latest_portfolio_as_of_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            """
            SELECT MAX(session_date)
            FROM fact_portfolio_target_book
            WHERE as_of_date = ?
            """,
            [target_date],
        ).fetchone()
    return None if not row or row[0] is None else pd.Timestamp(row[0]).date()


def latest_recommendation_timeline(settings: Settings) -> dict[str, object]:
    if not settings.paths.duckdb_path.exists():
        return {}
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        selection_row = connection.execute(
            """
            SELECT MAX(as_of_date)
            FROM fact_ranking
            WHERE ranking_version = 'selection_engine_v2'
            """
        ).fetchone()
        selection_as_of_date = (
            None
            if selection_row is None or selection_row[0] is None
            else pd.Timestamp(selection_row[0]).date()
        )
        portfolio_as_of_date = _latest_portfolio_as_of_date(settings)
        portfolio_session_date = _latest_portfolio_session_date(
            settings,
            as_of_date=portfolio_as_of_date,
        )
        intraday_row = connection.execute(
            "SELECT MAX(session_date) FROM fact_intraday_meta_decision"
        ).fetchone()
        intraday_session_date = (
            None
            if intraday_row is None or intraday_row[0] is None
            else pd.Timestamp(intraday_row[0]).date()
        )
    return {
        "selection_as_of_date": selection_as_of_date,
        "portfolio_as_of_date": portfolio_as_of_date,
        "portfolio_session_date": portfolio_session_date,
        "intraday_session_date": intraday_session_date,
    }


def latest_recommendation_timeline_text(settings: Settings) -> str:
    timeline = latest_recommendation_timeline(settings)
    selection_as_of_date = timeline.get("selection_as_of_date")
    portfolio_session_date = timeline.get("portfolio_session_date")
    intraday_session_date = timeline.get("intraday_session_date")
    if selection_as_of_date is None:
        return "추천 기준일 데이터가 아직 없습니다."
    if portfolio_session_date is None:
        return (
            f"현재 추천은 {format_ui_date(selection_as_of_date)} 장마감 후 산출한 결과입니다. "
            "다음 거래일 진입용 포트폴리오 목표북은 아직 생성되지 않았습니다."
        )
    if intraday_session_date is not None:
        return (
            f"현재 추천은 {format_ui_date(selection_as_of_date)} 장마감 후 산출한 결과이며, "
            f"실제 신규 진입 검토 기준일은 {format_ui_date(portfolio_session_date)}입니다. "
            f"장중 연구 세션은 {format_ui_date(intraday_session_date)} 기준으로 이어집니다."
        )
    return (
        f"현재 추천은 {format_ui_date(selection_as_of_date)} 장마감 후 산출한 결과이며, "
        f"실제 신규 진입 검토 기준일은 {format_ui_date(portfolio_session_date)}입니다."
    )


def _latest_portfolio_snapshot_date(settings: Settings):
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            "SELECT MAX(snapshot_date) FROM fact_portfolio_nav_snapshot"
        ).fetchone()
    return None if not row or row[0] is None else pd.Timestamp(row[0]).date()


def latest_portfolio_policy_registry_frame(
    settings: Settings,
    *,
    active_only: bool = False,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    query = """
        SELECT
            active_portfolio_policy_id,
            portfolio_policy_id,
            portfolio_policy_version,
            display_name,
            source_type,
            promotion_type,
            effective_from_date,
            effective_to_date,
            active_flag,
            rollback_of_active_portfolio_policy_id,
            note,
            created_at
        FROM fact_portfolio_policy_registry
    """
    if active_only:
        query += """
            WHERE active_flag = TRUE
              AND effective_from_date <= CURRENT_DATE
              AND (effective_to_date IS NULL OR effective_to_date >= CURRENT_DATE)
        """
    query += " ORDER BY effective_from_date DESC, created_at DESC LIMIT ?"
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(query, [limit]).fetchdf()


def latest_portfolio_candidate_frame(
    settings: Settings,
    *,
    as_of_date=None,
    execution_mode: str | None = None,
    limit: int = 30,
) -> pd.DataFrame:
    target_date = as_of_date or _latest_portfolio_as_of_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    filters = ["as_of_date = ?"]
    params: list[object] = [target_date]
    if execution_mode:
        filters.append("execution_mode = ?")
        params.append(str(execution_mode).upper())
    query = f"""
        SELECT
            as_of_date,
            session_date,
            execution_mode,
            symbol,
            company_name,
            market,
            sector,
            candidate_rank,
            candidate_state,
            final_selection_value,
            effective_alpha_long,
            risk_scaled_conviction,
            timing_action,
            timing_gate_status,
            current_holding_flag
        FROM fact_portfolio_candidate
        WHERE {' AND '.join(filters)}
        ORDER BY execution_mode, candidate_rank, symbol
        LIMIT ?
    """
    params.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(query, params).fetchdf()


def latest_portfolio_target_book_frame(
    settings: Settings,
    *,
    as_of_date=None,
    execution_mode: str | None = None,
    include_cash: bool = False,
    included_only: bool = False,
    limit: int = 30,
) -> pd.DataFrame:
    target_date = as_of_date or _latest_portfolio_as_of_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    filters = ["as_of_date = ?"]
    params: list[object] = [target_date]
    if execution_mode:
        filters.append("execution_mode = ?")
        params.append(str(execution_mode).upper())
    if not include_cash:
        filters.append("symbol <> '__CASH__'")
    if included_only:
        filters.append("included_flag = TRUE")
    query = f"""
        SELECT
            as_of_date,
            session_date,
            execution_mode,
            symbol,
            company_name,
            market,
            sector,
            candidate_state,
            target_rank,
            target_weight,
            target_notional,
            target_shares,
            target_price,
            plan_horizon,
            entry_trade_date,
            exit_trade_date,
            entry_basis,
            exit_basis,
            model_spec_id,
            active_alpha_model_id,
            action_plan_label,
            target_return,
            stretch_target_return,
            stop_return,
            action_target_price,
            action_stretch_price,
            action_stop_price,
            current_shares,
            current_weight,
            score_value,
            gate_status,
            included_flag,
            waitlist_flag,
            waitlist_rank,
            blocked_flag,
            CASE
                WHEN blocked_flag THEN constraint_flags_json
                ELSE NULL
            END AS blocked_reason
        FROM fact_portfolio_target_book
        WHERE {' AND '.join(filters)}
        ORDER BY execution_mode, target_rank, symbol
        LIMIT ?
    """
    params.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(query, params).fetchdf()


def latest_portfolio_waitlist_frame(
    settings: Settings,
    *,
    as_of_date=None,
    execution_mode: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    target_date = as_of_date or _latest_portfolio_as_of_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    filters = ["as_of_date = ?", "(waitlist_flag = TRUE OR blocked_flag = TRUE)"]
    params: list[object] = [target_date]
    if execution_mode:
        filters.append("execution_mode = ?")
        params.append(str(execution_mode).upper())
    query = f"""
        SELECT
            as_of_date,
            execution_mode,
            symbol,
            company_name,
            candidate_state,
            gate_status,
            waitlist_flag,
            waitlist_rank,
            blocked_flag,
            CASE
                WHEN blocked_flag THEN constraint_flags_json
                ELSE NULL
            END AS blocked_reason
        FROM fact_portfolio_target_book
        WHERE {' AND '.join(filters)}
        ORDER BY execution_mode, blocked_flag DESC, waitlist_rank, symbol
        LIMIT ?
    """
    params.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(query, params).fetchdf()


def latest_portfolio_rebalance_plan_frame(
    settings: Settings,
    *,
    as_of_date=None,
    execution_mode: str | None = None,
    limit: int = 40,
) -> pd.DataFrame:
    target_date = as_of_date or _latest_portfolio_as_of_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    filters = ["as_of_date = ?"]
    params: list[object] = [target_date]
    if execution_mode:
        filters.append("execution_mode = ?")
        params.append(str(execution_mode).upper())
    query = f"""
        SELECT
            as_of_date,
            session_date,
            execution_mode,
            symbol,
            company_name,
            rebalance_action,
            action_sequence,
            gate_status,
            current_shares,
            target_shares,
            delta_shares,
            reference_price,
            notional_delta,
            cash_delta,
            blocked_reason
        FROM fact_portfolio_rebalance_plan
        WHERE {' AND '.join(filters)}
        ORDER BY execution_mode, action_sequence, symbol
        LIMIT ?
    """
    params.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(query, params).fetchdf()


def latest_portfolio_nav_frame(
    settings: Settings,
    *,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                snapshot_date,
                execution_mode,
                portfolio_policy_id,
                portfolio_policy_version,
                nav_value,
                cumulative_return,
                drawdown,
                turnover_ratio,
                cash_weight,
                holding_count,
                max_single_weight,
                top3_weight
            FROM fact_portfolio_nav_snapshot
            ORDER BY snapshot_date DESC, execution_mode
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_portfolio_evaluation_frame(
    settings: Settings,
    *,
    limit: int = 40,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                evaluation_date,
                start_date,
                end_date,
                execution_mode,
                comparison_key,
                metric_name,
                metric_value,
                sample_count
            FROM fact_portfolio_evaluation_summary
            ORDER BY evaluation_date DESC, execution_mode, comparison_key, metric_name
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_portfolio_constraint_frame(
    settings: Settings,
    *,
    as_of_date=None,
    limit: int = 30,
) -> pd.DataFrame:
    target_date = as_of_date or _latest_portfolio_as_of_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                as_of_date,
                execution_mode,
                symbol,
                constraint_type,
                event_code,
                requested_value,
                applied_value,
                limit_value,
                message
            FROM fact_portfolio_constraint_event
            WHERE as_of_date = ?
            ORDER BY execution_mode, symbol, constraint_type
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def latest_portfolio_run_status_frame(settings: Settings, *, limit: int = 12) -> pd.DataFrame:
    if not _metadata_available(settings):
        return pd.DataFrame()
    return _metadata_frame(
        settings,
        """
            SELECT
                run_type,
                status,
                as_of_date,
                started_at,
                finished_at,
                error_message
            FROM ops_run_manifest
            WHERE run_type IN (
                'build_portfolio_candidate_book',
                'validate_portfolio_candidate_book',
                'freeze_active_portfolio_policy',
                'rollback_active_portfolio_policy',
                'materialize_portfolio_target_book',
                'materialize_portfolio_rebalance_plan',
                'materialize_portfolio_position_snapshots',
                'materialize_portfolio_nav',
                'run_portfolio_walkforward',
                'evaluate_portfolio_policies',
                'render_portfolio_report',
                'publish_discord_portfolio_summary',
                'validate_portfolio_framework'
            )
            ORDER BY started_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_portfolio_report_preview(settings: Settings) -> str | None:
    artifact_root = settings.paths.artifacts_dir / "portfolio_report"
    if not artifact_root.exists():
        return None
    previews = sorted(artifact_root.glob("**/portfolio_report_preview.md"))
    if not previews:
        return None
    return previews[-1].read_text(encoding="utf-8")
UI_VALUE_LABELS.setdefault("split_name", {}).update(
    {
        "train": "학습",
        "validation": "검증",
        "inference": "추론",
    }
)
UI_VALUE_LABELS.setdefault("member_name", {}).update(
    {
        "elasticnet": "ElasticNet",
        "hist_gbm": "HistGBM",
        "extra_trees": "ExtraTrees",
        "ensemble": "Ensemble",
    }
)
UI_COLUMN_LABELS.update(
    {
        "latest_selection_v2_ranking_version": "최신 추천 모델 버전",
        "latest_alpha_model_version": "최신 알파 모델 버전",
        "latest_alpha_prediction_version": "최신 알파 예측 버전",
        "uncertainty_score": "불확실성 점수",
        "disagreement_score": "불일치 점수",
        "fallback_flag": "대체 계산 사용 여부",
        "fallback_reason": "대체 계산 사유",
        "latest_model_train_date": "최신 모델 학습일",
        "latest_model_train_rows": "최신 모델 학습 행수",
        "latest_model_prediction_date": "최신 알파 예측일",
        "latest_model_prediction_rows": "최신 알파 예측 행수",
        "latest_selection_v2_date": "최신 추천 모델 기준일",
        "latest_selection_v2_rows": "최신 추천 모델 행 수",
        "d1_selection_v2_value": "1거래일 기준 추천 점수",
        "d1_selection_v2_grade": "1거래일 기준 추천 등급",
        "d5_selection_v2_value": "5거래일 기준 추천 점수",
        "d5_selection_v2_grade": "5거래일 기준 추천 등급",
        "d5_alpha_expected_excess_return": "5거래일 기준 알파 예상 초과수익률",
        "d5_alpha_lower_band": "5거래일 기준 알파 하단 범위",
        "d5_alpha_upper_band": "5거래일 기준 알파 상단 범위",
        "d5_alpha_uncertainty_score": "5거래일 기준 알파 불확실성",
        "d5_alpha_disagreement_score": "5거래일 기준 알파 불일치",
        "d5_alpha_fallback_flag": "5거래일 기준 알파 fallback 여부",
        "d5_selection_v2_realized_excess_return": "5거래일 뒤 추천 모델 실현 초과수익률",
        "d5_selection_v2_band_status": "5거래일 기준 추천 모델 범위 판정",
        "train_row_count": "학습 행수",
        "validation_row_count": "검증 행수",
        "member_name": "모델 구성원",
        "split_name": "분할",
        "metric_name": "지표명",
        "metric_value": "지표값",
        "sample_count": "표본 수",
        "selection_v2_avg_excess": "현재 추천 모델 평균 초과수익률",
        "selection_v1_avg_excess": "이전 추천 모델 평균 초과수익률",
        "explanatory_v0_avg_excess": "설명형 v0 평균 초과수익률",
        "v2_vs_v1_gap": "v2-v1 차이",
        "v2_vs_explanatory_gap": "v2-설명형 차이",
        "adjusted_symbols": "조정 판단 종목 수",
        "adjusted_enter_now_count": "조정 즉시 진입 수",
        "adjusted_wait_recheck_count": "조정 재확인 수",
        "adjusted_avoid_today_count": "조정 오늘 회피 수",
        "adjusted_data_insufficient_count": "조정 데이터 부족 수",
        "context_scope": "컨텍스트 범위",
        "market_session_state": "장중 세션 상태",
        "prior_daily_regime_state": "전일 일간 레짐",
        "prior_daily_regime_score": "전일 레짐 점수",
        "advancers_count": "상승 종목 수",
        "decliners_count": "하락 종목 수",
        "market_breadth_ratio": "시장 상승 비율",
        "kospi_return_from_open": "코스피 시가 대비 수익률",
        "kosdaq_return_from_open": "코스닥 시가 대비 수익률",
        "candidate_mean_return_from_open": "후보 평균 시가 대비 수익률",
        "candidate_median_return_from_open": "후보 중앙값 시가 대비 수익률",
        "candidate_hit_ratio_from_open": "후보 플러스 비율",
        "candidate_mean_relative_volume": "후보 평균 상대 거래활동",
        "candidate_mean_spread_bps": "후보 평균 스프레드(bps)",
        "candidate_mean_execution_strength": "후보 평균 체결 강도",
        "candidate_mean_orderbook_imbalance": "후보 평균 호가 불균형",
        "candidate_mean_gap_score": "후보 평균 갭 점수",
        "candidate_mean_signal_quality": "후보 평균 신호 품질",
        "market_shock_proxy": "시장 충격 지표",
        "intraday_volatility_proxy": "장중 변동성 지표",
        "dispersion_proxy": "흩어짐 지표",
        "bar_coverage_ratio": "1분봉 커버리지",
        "trade_coverage_ratio": "체결 요약 커버리지",
        "quote_coverage_ratio": "호가 요약 커버리지",
        "provider_latency_ms": "프로바이더 지연(ms)",
        "context_reason_codes_json": "컨텍스트 사유",
        "market_regime_family": "장중 시장 흐름",
        "adjustment_profile": "조정 프로파일",
        "raw_action": "원래 판단",
        "adjusted_action": "보정 후 판단",
        "raw_timing_score": "원판 타이밍 점수",
        "adjusted_timing_score": "조정 타이밍 점수",
        "selection_confidence_bucket": "선정 신뢰 구간",
        "signal_quality_flag": "신호 품질 구간",
        "eligible_to_execute_flag": "실행 가능 여부",
        "adjustment_reason_codes_json": "조정 사유",
        "decision_notes_json": "판단 메모",
        "strategy_id": "비교 방식",
        "strategy_family": "전략 계열",
        "cutoff_checkpoint_time": "컷오프 체크포인트",
        "entry_checkpoint_time": "진입 체크포인트",
        "entry_action_source": "진입 액션 소스",
        "executed_flag": "실행 여부",
        "no_entry_flag": "미진입 여부",
        "entry_timestamp": "진입 시각",
        "entry_price": "진입 가격",
        "exit_trade_date": "청산 거래일",
        "exit_price": "청산 가격",
        "baseline_open_price": "기준 시가",
        "baseline_open_return": "기준 시가 수익률",
        "baseline_open_excess_return": "기준 시가 초과수익률",
        "realized_return": "실현 수익률",
        "timing_edge_vs_open_return": "시가 대비 타이밍 엣지 수익률",
        "timing_edge_vs_open_bps": "시가 대비 타이밍 엣지(bps)",
        "skip_reason_code": "스킵 사유",
        "skip_saved_loss_flag": "스킵 손실 회피 여부",
        "missed_winner_flag": "스킵 후 승자 놓침 여부",
        "comparison_scope": "비교 범위",
        "comparison_value": "비교 값",
        "matured_count": "평가 완료 수",
        "executed_count": "실행 수",
        "no_entry_count": "미진입 수",
        "execution_rate": "실행 비율",
        "mean_realized_excess_return": "평균 실현 초과수익률",
        "median_realized_excess_return": "중앙값 실현 초과수익률",
        "mean_timing_edge_vs_open_bps": "평균 시가 대비 타이밍 엣지(bps)",
        "median_timing_edge_vs_open_bps": "중앙값 시가 대비 타이밍 엣지(bps)",
        "positive_timing_edge_rate": "양의 타이밍 엣지 비율",
        "skip_saved_loss_rate": "스킵 손실 회피 비율",
        "missed_winner_rate": "놓친 승자 비율",
        "coverage_ok_rate": "커버리지 양호 비율",
        "window_start_date": "평가 창 시작일",
        "window_end_date": "평가 창 종료일",
        "grouping_key": "진단 그룹 기준",
        "grouping_value": "진단 그룹 값",
    }
)
UI_VALUE_LABELS.setdefault("action", {}).update(
    {
        "ENTER_NOW": "즉시 진입",
        "WAIT_RECHECK": "재확인 대기",
        "AVOID_TODAY": "오늘 회피",
        "DATA_INSUFFICIENT": "데이터 부족",
    }
)
UI_VALUE_LABELS["raw_action"] = UI_VALUE_LABELS["action"]
UI_VALUE_LABELS["adjusted_action"] = UI_VALUE_LABELS["action"]
UI_VALUE_LABELS["selected_action"] = UI_VALUE_LABELS["action"]
UI_VALUE_LABELS.setdefault("session_status", {}).update(
    {"planned": "예정", "active": "진행 중", "historical": "과거"}
)
UI_VALUE_LABELS.setdefault("market_session_state", {}).update(
    {"planned": "예정", "active": "진행 중", "historical": "과거"}
)
UI_VALUE_LABELS.setdefault("context_scope", {}).update({"market": "시장"})
UI_VALUE_LABELS.setdefault("data_quality_flag", {}).update(
    {"weak": "약함", "partial": "부분", "strong": "양호"}
)
UI_VALUE_LABELS.setdefault("signal_quality_flag", {}).update(
    {"critical": "치명", "low": "낮음", "medium": "보통", "high": "높음"}
)
UI_VALUE_LABELS.setdefault("selection_confidence_bucket", {}).update(
    {"top": "최상위", "high": "상위", "medium": "중간", "low": "낮음", "unknown": "미상"}
)
UI_VALUE_LABELS.setdefault("market_regime_family", {}).update(
    {
        "panic": "공포 장",
        "risk_off": "방어 우위 장",
        "neutral": "방향성 탐색 장",
        "risk_on": "상승 우위 장",
        "euphoria": "과열 장",
        "PANIC_OPEN": "공포성 급락 출발",
        "WEAK_RISK_OFF": "방어 우위",
        "NEUTRAL_CHOP": "방향성 약한 박스권",
        "HEALTHY_TREND": "건강한 상승 추세",
        "OVERHEATED_GAP_CHASE": "과열 갭 추격",
        "DATA_WEAK": "데이터 신뢰 낮음",
        "unknown": "미상",
    }
)
UI_VALUE_LABELS.setdefault("adjustment_profile", {}).update(
    {
        "DEFENSIVE": "방어형",
        "NEUTRAL": "중립형",
        "SELECTIVE_RISK_ON": "선별 공격형",
        "GAP_CHASE_GUARD": "갭 추격 방지",
        "DATA_WEAK_GUARD": "데이터 신뢰 낮음 방어",
    }
)
UI_VALUE_LABELS.setdefault("strategy_id", {}).update(
    {
        "SEL_V2_OPEN_ALL": "추천 모델 시가 일괄 진입",
        "SEL_V2_TIMING_RAW_FIRST_ENTER": "추천 모델 원래 판단 첫 진입",
        "SEL_V2_TIMING_ADJ_FIRST_ENTER": "추천 모델 보정 판단 첫 진입",
        "SEL_V2_TIMING_ADJ_0930_ONLY": "추천 모델 09:30 진입 고정",
        "SEL_V2_TIMING_ADJ_1000_ONLY": "추천 모델 10:00 진입 고정",
    }
)
UI_VALUE_LABELS.setdefault("strategy_family", {}).update(
    {
        "open_baseline": "시가 기준",
        "raw_timing": "원판 타이밍",
        "adjusted_timing": "조정 타이밍",
        "adjusted_timing_fixed": "조정 고정 체크포인트",
    }
)
UI_VALUE_LABELS.setdefault("entry_action_source", {}).update(UI_VALUE_LABELS["strategy_family"])
UI_VALUE_LABELS.setdefault("comparison_scope", {}).update(
    {
        "all": "전체",
        "regime_family": "레짐 가족",
        "strategy_id": "전략 ID",
        "selection_confidence_bucket": "선정 신뢰 구간",
    }
)
UI_VALUE_LABELS.setdefault("grouping_key", {}).update(
    {
        "overall": "전체",
        "strategy_id": "전략 ID",
        "regime_family": "레짐 가족",
        "selection_confidence_bucket": "선정 신뢰 구간",
    }
)
UI_VALUE_LABELS.setdefault("quality_flag", {}).update({"thin_sample": "표본 부족"})
UI_VALUE_LABELS.setdefault("skip_reason_code", {}).update(
    {
        "baseline_open_missing": "기준 시가 없음",
        "no_raw_enter_before_cutoff": "컷오프 전 원판 진입 없음",
        "no_adjusted_enter_before_cutoff": "컷오프 전 조정 진입 없음",
        "0930_not_enter": "09:30 진입 아님",
        "1000_not_enter": "10:00 진입 아님",
    }
)

UI_REASON_TAG_LABELS: dict[str, str] = {
    "short_term_momentum_strong": "단기 모멘텀 강함",
    "breakout_near_20d_high": "20일 고점 근접",
    "turnover_surge": "거래대금 급증",
    "fresh_news_catalyst": "신규 뉴스 촉매",
    "quality_metrics_supportive": "질적 지표 우호",
    "low_drawdown_relative": "낙폭 안정적",
    "foreign_institution_flow_supportive": "외국인·기관 수급 우호",
    "implementation_friction_contained": "실행 마찰 낮음",
}

UI_RISK_TAG_LABELS: dict[str, str] = {
    "high_realized_volatility": "실현 변동성 높음",
    "large_recent_drawdown": "최근 낙폭 큼",
    "weak_fundamental_coverage": "재무 커버리지 약함",
    "thin_liquidity": "유동성 부족",
    "news_link_low_confidence": "뉴스 연결 신뢰 낮음",
    "data_missingness_high": "데이터 결손 높음",
    "uncertainty_proxy_high": "불확실성 지표 높음",
    "implementation_friction_high": "실행 마찰 높음",
    "flow_coverage_missing": "수급 커버리지 부족",
}

UI_NOTE_TAG_LABELS: dict[str, str] = {
    "missing_price": "가격 데이터 없음",
    "stale_price": "가격 데이터 지연",
    "adv20_below_threshold": "20일 평균 거래대금 기준 미달",
    "feature_missingness_high": "피처 결손 높음",
    **UI_RISK_TAG_LABELS,
}

UI_REASON_TAG_LABELS.update(
    {
        "ml_alpha_supportive": "ML 알파 지지",
        "prediction_fallback_used": "예측 fallback 사용",
    }
)
UI_RISK_TAG_LABELS.update(
    {
        "model_uncertainty_high": "모델 불확실성 높음",
        "model_disagreement_high": "모델 불일치 높음",
        "prediction_fallback": "예측 fallback 사용",
    }
)
UI_NOTE_TAG_LABELS.update(UI_RISK_TAG_LABELS)
UI_NOTE_TAG_LABELS.update(
    {
        "panic_open_guard": "패닉 오픈 방어",
        "weak_risk_off_guard": "방어 우위 구간 방어",
        "healthy_trend_support": "건강한 상승 추세 지지",
        "gap_chase_guard": "갭 추격 방어",
        "data_weak_guard": "데이터 약함 방어",
        "critical_signal_quality": "신호 품질 치명",
        "low_signal_quality": "신호 품질 낮음",
        "friction_penalty": "마찰 패널티",
        "quote_unavailable": "호가 미가용",
        "trade_unavailable": "체결 미가용",
        "selection_fallback_penalty": "추천 모델 대체 계산 패널티",
        "uncertainty_high": "불확실성 높음",
        "disagreement_high": "불일치 높음",
        "raw_data_insufficient_locked": "원판 데이터 부족 유지",
        "raw_avoid_preserved": "원판 회피 유지",
        "critical_signal_quality_guard": "신호 품질 치명 방어",
        "signal_quality_requires_recheck": "신호 품질 재확인 필요",
        "signal_quality_guard": "신호 품질 방어",
        "eligibility_gate_block": "실행 가능성 차단",
        "adjusted_score_below_avoid": "조정 점수 회피 구간",
        "profile_blocks_enter": "프로파일상 진입 차단",
        "adjusted_enter_threshold_hit": "조정 진입 기준 충족",
        "enter_downgraded_to_wait": "즉시 진입에서 대기로 하향",
        "adjusted_wait_zone": "조정 대기 구간",
        "checkpoint_recheck_needed": "체크포인트 재확인 필요",
        "avoid_by_risk_rule": "리스크 규칙상 회피",
        "momentum_confirmed": "모멘텀 확인",
        "timing_supportive": "타이밍 우호",
        "late_checkpoint_entry": "늦은 체크포인트 진입",
        "final_checkpoint_not_strong_enough": "마지막 체크포인트 강도 부족",
    }
)


def _translate_scalar(column: str, value: object) -> object:
    if pd.isna(value):
        return value
    if isinstance(value, bool):
        return "예" if value else "아니오"
    if column in {"latest_ranking_version", "latest_selection_version"}:
        return _translate_scalar("ranking_version", value)
    if column == "latest_prediction_version":
        return _translate_scalar("prediction_version", value)
    mapping = UI_VALUE_LABELS.get(column)
    if mapping is None:
        return value
    text = str(value)
    if text in mapping:
        return mapping[text]
    lowered = text.lower()
    if lowered in mapping:
        return mapping[lowered]
    uppered = text.upper()
    if uppered in mapping:
        return mapping[uppered]
    return value


def _translate_json_list(value: object, mapping: dict[str, str]) -> object:
    parsed = _try_parse_json_value(value)
    if parsed is None:
        return value
    if not isinstance(parsed, list):
        return value
    translated = [mapping.get(str(item), str(item)) for item in parsed]
    return ", ".join(translated) if translated else "-"


def _try_parse_json_value(value: object) -> object | None:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text in {"", "-", "None", "nan", "NaN", "NaT"}:
            return None
        if not text.startswith(("{", "[")):
            return None
        try:
            return json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    return None


def _translate_generic_token(value: object) -> object:
    if value is None:
        return value
    if isinstance(value, bool):
        return "예" if value else "아니오"

    text = str(value).strip()
    if not text:
        return value

    for mapping in (UI_NOTE_TAG_LABELS, UI_REASON_TAG_LABELS, UI_RISK_TAG_LABELS):
        if text in mapping:
            return mapping[text]

    fallback_columns = (
        "status",
        "severity",
        "warning_level",
        "health_status",
        "trigger_type",
        "promotion_type",
        "action",
        "raw_action",
        "adjusted_action",
        "selected_action",
        "final_action",
        "report_type",
        "run_type",
        "job_name",
        "job_key",
        "quality_flag",
        "skip_reason_code",
        "market_regime_family",
        "regime_state",
        "comparison_key",
        "execution_mode",
        "portfolio_execution_mode",
        "split_name",
        "metric_scope",
        "panel_name",
        "candidate_state",
        "gate_status",
        "timing_gate_status",
        "fallback_mode",
        "service_slug",
        "provider",
        "provider_name",
    )
    for column in fallback_columns:
        translated = _translate_scalar(column, text)
        if translated != text:
            return translated
    return value


def _friendly_json_key_label(key: str) -> str:
    label = UI_COLUMN_LABELS.get(key)
    if label:
        return label

    normalized = key.removesuffix("_json")
    simple_labels = {
        "symbol": "종목",
        "grade": "등급",
        "label": "이름",
        "name": "이름",
        "score": "점수",
        "value": "값",
        "weight": "비중",
        "count": "개수",
        "date": "날짜",
        "time": "시각",
        "reason": "사유",
        "reasons": "사유",
        "note": "메모",
        "notes": "메모",
        "status": "상태",
        "probability": "확률",
        "message": "메시지",
        "model": "모델",
        "policy": "정책",
        "candidate": "후보",
        "source": "출처",
        "target": "대상",
    }
    if normalized in simple_labels:
        return simple_labels[normalized]

    token_labels = {
        "active": "현재",
        "latest": "최근",
        "symbol": "종목",
        "grade": "등급",
        "score": "점수",
        "weight": "비중",
        "count": "개수",
        "date": "날짜",
        "time": "시각",
        "reason": "사유",
        "notes": "메모",
        "status": "상태",
        "model": "모델",
        "policy": "정책",
        "candidate": "후보",
        "source": "출처",
        "target": "대상",
        "run": "실행",
        "report": "리포트",
        "detail": "세부 내용",
        "summary": "요약",
    }
    return " ".join(token_labels.get(part, part) for part in normalized.split("_"))


def _format_json_scalar(column: str, value: object) -> str:
    translated = _format_scalar_for_display(column, value)
    if translated == value:
        translated = _translate_generic_token(value)
    text = str(translated).strip()
    return text if text else "-"


def _format_json_record(record: dict[str, object]) -> str:
    parts: list[str] = []
    for key, item in record.items():
        label = _friendly_json_key_label(str(key))
        parsed = _try_parse_json_value(item)
        if parsed is not None:
            value = _format_json_for_display(str(key), item)
        else:
            value = _format_json_scalar(str(key), item)
        if value in {"", "-", "None"}:
            continue
        separator = " " if len(label) <= 4 else ": "
        parts.append(f"{label}{separator}{value}")
    return " / ".join(parts) if parts else "-"


def _format_json_for_display(column: str, value: object) -> str:
    parsed = _try_parse_json_value(value)
    if parsed is None:
        return _format_json_scalar(column, value)

    if column == "active_meta_model_ids_json" and isinstance(parsed, list):
        if not parsed:
            return "-"
        return f"사용 중 {len(parsed)}개"

    if column == "top_actionable_symbol_list_json" and isinstance(parsed, list):
        items: list[str] = []
        for entry in parsed[:4]:
            if isinstance(entry, dict):
                symbol = str(entry.get("symbol") or "").strip()
                grade = _translate_generic_token(entry.get("grade"))
                if symbol and grade:
                    items.append(f"{symbol} {grade}")
                elif symbol:
                    items.append(symbol)
        if not items:
            return f"{len(parsed)}개 종목" if parsed else "-"
        suffix = f" 외 {len(parsed) - len(items)}개" if len(parsed) > len(items) else ""
        return ", ".join(items) + suffix

    if isinstance(parsed, list):
        if not parsed:
            return "-"
        parts: list[str] = []
        for item in parsed[:4]:
            if isinstance(item, dict):
                parts.append(_format_json_record(item))
            else:
                parts.append(_format_json_scalar(column, item))
        parts = [part for part in parts if part not in {"", "-", "None"}]
        if not parts:
            return "-"
        suffix = f" 외 {len(parsed) - len(parts)}건" if len(parsed) > len(parts) else ""
        return ", ".join(parts) + suffix

    if isinstance(parsed, dict):
        return _format_json_record(parsed)

    return _format_json_scalar(column, parsed)


PERCENT_COLUMN_TOKENS: tuple[str, ...] = (
    "_return",
    "_band",
    "_weight",
    "_ratio",
    "_rate",
    "_pct",
    "_probability",
    "_margin",
)
PERCENT_COLUMN_EXACT: set[str] = {
    "drawdown",
}
PRICE_COLUMN_TOKENS: tuple[str, ...] = ("_price",)
PRICE_COLUMN_EXACT: set[str] = {"open", "high", "low", "close", "target_price", "reference_price"}
DATE_COLUMN_EXACT: set[str] = {
    "date_kst",
    "window_end",
}
DATE_COLUMN_EXCLUDED: set[str] = {
    "request_date_field",
}
TIME_COLUMN_EXACT: set[str] = {
    "bar_time",
    "checkpoint_time",
    "cutoff_checkpoint_time",
    "entry_checkpoint_time",
    "selected_checkpoint_time",
}
DATETIME_COLUMN_EXACT: set[str] = {
    "latest_available_ts",
}
DATETIME_COLUMN_SUFFIXES: tuple[str, ...] = ("_at", "_ts")
RUN_ID_COLUMN_EXACT: set[str] = {"run_id", "job_run_id"}
SCORE_COLUMN_TOKENS: tuple[str, ...] = ("_score",)
SCORE_COLUMN_EXACT: set[str] = {
    "avg_signal_quality",
    "before_objective_score",
    "d1_selection_value",
    "d1_selection_v2_value",
    "d5_selection_value",
    "d5_selection_v2_value",
    "final_selection_value",
    "live_d1_selection_v2_value",
    "live_d5_selection_v2_value",
    "objective_score",
    "score_value",
    "stability_score",
    "tuned_score",
}
RUN_ID_PATTERN = re.compile(
    r"^(?P<prefix>.+)-(?P<stamp>\d{8}T\d{6})(?:-(?P<suffix>[A-Za-z0-9]+))?$"
)
TIME_ONLY_PATTERN = re.compile(r"^(?P<hour>\d{1,2}):?(?P<minute>\d{2})(?::\d{2})?$")
UI_DISPLAY_TIMEZONE = "Asia/Seoul"


def _normalize_ui_timestamp(value: object) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        timestamp = value
    elif isinstance(value, datetime):
        timestamp = pd.Timestamp(value)
    elif isinstance(value, date):
        timestamp = pd.Timestamp(value)
    else:
        try:
            timestamp = pd.Timestamp(value)
        except (TypeError, ValueError):
            return None
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is not None:
        return timestamp.tz_convert(UI_DISPLAY_TIMEZONE)
    return timestamp


def _should_include_year(target_date: date, *, include_year: bool | None = None) -> bool:
    if include_year is not None:
        return include_year
    return target_date.year != today_local(UI_DISPLAY_TIMEZONE).year


def format_ui_date(value: object, *, include_year: bool | None = None) -> str:
    timestamp = _normalize_ui_timestamp(value)
    if timestamp is None:
        return "-"
    target_date = timestamp.date()
    if _should_include_year(target_date, include_year=include_year):
        return f"{target_date.year}년 {target_date.month}월 {target_date.day}일"
    return f"{target_date.month}월 {target_date.day}일"


def format_ui_time(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    if isinstance(value, time):
        return value.strftime("%H:%M")
    if isinstance(value, str):
        text = value.strip()
        if not text or text in {"nan", "NaN", "NaT", "None"}:
            return "-"
        matched = TIME_ONLY_PATTERN.match(text)
        if matched:
            hour = int(matched.group("hour"))
            minute = int(matched.group("minute"))
            return f"{hour:02d}:{minute:02d}"
    timestamp = _normalize_ui_timestamp(value)
    if timestamp is None:
        return str(value)
    return timestamp.strftime("%H:%M")


def format_ui_datetime(value: object, *, include_year: bool | None = None) -> str:
    timestamp = _normalize_ui_timestamp(value)
    if timestamp is None:
        return "-"
    return f"{format_ui_date(timestamp, include_year=include_year)} {timestamp.strftime('%H:%M')}"


def format_ui_run_id(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return "-"
    matched = RUN_ID_PATTERN.match(text)
    if matched is None:
        return text
    try:
        timestamp = (
            pd.Timestamp(datetime.strptime(matched.group("stamp"), "%Y%m%dT%H%M%S"))
            .tz_localize("UTC")
            .tz_convert(UI_DISPLAY_TIMEZONE)
        )
    except ValueError:
        return text
    pieces = [matched.group("prefix"), format_ui_datetime(timestamp)]
    suffix = matched.group("suffix")
    if suffix:
        pieces.append(suffix)
    return " · ".join(piece for piece in pieces if piece)


def resolve_ui_artifact_path(settings: Settings, path_value: object) -> Path | None:
    return resolve_artifact_path(settings, path_value)


def format_ui_number(value: object, *, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        return str(value)
    numeric = float(value)
    if not math.isfinite(numeric):
        return "-"
    if abs(numeric - round(numeric)) < 1e-9:
        return f"{int(round(numeric)):,}"
    text = f"{numeric:,.{decimals}f}"
    return text.rstrip("0").rstrip(".")


def is_ui_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() in {"", "-", "None", "nan", "NaN", "NaT"}
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        numeric = float(value)
        return not math.isfinite(numeric)
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def format_ui_percent(
    value: object,
    *,
    decimals: int = 2,
    signed: bool = False,
    percent_points: bool = False,
    missing: str = "-",
) -> str:
    if is_ui_missing_value(value):
        return missing
    if isinstance(value, bool):
        return str(value)
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return str(value)
    scaled = float(numeric) * 100.0
    if not math.isfinite(scaled):
        return missing
    sign = "+" if signed else ""
    suffix = "%p" if percent_points else "%"
    return f"{scaled:{sign}.{decimals}f}{suffix}"


def format_ui_delta(
    value: object,
    *,
    decimals: int = 2,
    signed: bool = True,
    percent_points: bool = False,
    missing: str = "-",
) -> str:
    if is_ui_missing_value(value):
        return missing
    if isinstance(value, bool):
        return str(value)
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return str(value)
    delta = float(numeric)
    if not math.isfinite(delta):
        return missing
    if percent_points:
        return f"{delta * 100:{'+' if signed else ''}.{decimals}f}%p"
    return f"{delta:{'+' if signed else ''}.{decimals}f}"


def _coerce_float(value: object, default: float = 0.0) -> float:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return default
    return float(numeric)


def _is_percent_display_column(column: str) -> bool:
    return column in PERCENT_COLUMN_EXACT or any(
        token in column for token in PERCENT_COLUMN_TOKENS
    )


def _is_price_display_column(column: str) -> bool:
    return column in PRICE_COLUMN_EXACT or any(
        token in column for token in PRICE_COLUMN_TOKENS
    )


def _is_date_display_column(column: str) -> bool:
    return (
        column not in TIME_COLUMN_EXACT
        and column not in DATETIME_COLUMN_EXACT
        and not any(column.endswith(suffix) for suffix in DATETIME_COLUMN_SUFFIXES)
        and column not in DATE_COLUMN_EXCLUDED
        and (column in DATE_COLUMN_EXACT or column.endswith("_date"))
    )


def _is_time_display_column(column: str) -> bool:
    return column in TIME_COLUMN_EXACT


def _is_datetime_display_column(column: str) -> bool:
    return column in DATETIME_COLUMN_EXACT or any(
        column.endswith(suffix) for suffix in DATETIME_COLUMN_SUFFIXES
    )


def _is_run_id_display_column(column: str) -> bool:
    return column in RUN_ID_COLUMN_EXACT or column.endswith("_run_id")


def _is_score_display_column(column: str) -> bool:
    return column in SCORE_COLUMN_EXACT or any(
        token in column for token in SCORE_COLUMN_TOKENS
    )


def _format_percent_value(value: object) -> object:
    if is_ui_missing_value(value) or isinstance(value, bool):
        return "-"
    if not isinstance(value, numbers.Real):
        return value
    scaled = float(value) * 100.0
    if not math.isfinite(scaled):
        return "-"
    if abs(scaled) < 0.005:
        scaled = 0.0
    return f"{scaled:.2f}%"


def _format_price_value(value: object) -> object:
    if pd.isna(value) or isinstance(value, bool):
        return value
    if not isinstance(value, numbers.Real):
        return value
    return f"{float(value):,.0f}원"


def _format_score_value(value: object) -> object:
    if is_ui_missing_value(value) or isinstance(value, bool):
        return "-"
    if not isinstance(value, numbers.Real):
        return value
    decimals = 1 if abs(float(value)) >= 10 else 2
    return format_ui_number(value, decimals=decimals)


def _format_scalar_for_display(column: str, value: object) -> object:
    if pd.isna(value):
        return "-"
    if _is_run_id_display_column(column):
        return format_ui_run_id(value)
    if _is_time_display_column(column):
        return format_ui_time(value)
    if _is_date_display_column(column):
        return format_ui_date(value)
    if _is_datetime_display_column(column):
        return format_ui_datetime(value)
    translated = _translate_scalar(column, value)
    if translated == value:
        translated = _translate_generic_token(value)
    if isinstance(translated, str) and translated.strip() in {"", "nan", "NaN", "NaT", "None"}:
        return "-"
    if _is_percent_display_column(column):
        return _format_percent_value(translated)
    if _is_price_display_column(column):
        return _format_price_value(translated)
    if _is_score_display_column(column):
        return _format_score_value(translated)
    if isinstance(translated, numbers.Real) and not isinstance(translated, bool):
        return format_ui_number(translated)
    return translated


def _deduplicate_display_columns(columns: list[object]) -> list[str]:
    counts: dict[str, int] = {}
    deduplicated: list[str] = []
    for column in columns:
        label = str(column)
        counts[label] = counts.get(label, 0) + 1
        if counts[label] == 1:
            deduplicated.append(label)
        else:
            deduplicated.append(f"{label} ({counts[label]})")
    return deduplicated


def localize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    localized = frame.copy()
    for column in localized.columns:
        if column in {
            "reasons",
            "top_reason_tags_json",
            "action_reason_json",
            "context_reason_codes_json",
            "adjustment_reason_codes_json",
        }:
            localized[column] = localized[column].map(
                lambda value: _translate_json_list(value, UI_NOTE_TAG_LABELS)
            )
            continue
        if column in {"risks", "risk_flags_json"}:
            localized[column] = localized[column].map(
                lambda value: _translate_json_list(value, UI_RISK_TAG_LABELS)
            )
            continue
        if column == "eligibility_notes_json":
            localized[column] = localized[column].map(
                lambda value: _translate_json_list(value, UI_NOTE_TAG_LABELS)
            )
            continue
        if column.endswith("_json") or column in {"blocked_reason"}:
            localized[column] = localized[column].map(
                lambda value, current_column=column: _format_json_for_display(
                    current_column,
                    value,
                )
            )
            continue
        localized[column] = localized[column].map(
            lambda value, current_column=column: _format_scalar_for_display(
                current_column,
                value,
            )
        )
    localized = localized.rename(columns=UI_COLUMN_LABELS)
    localized.columns = _deduplicate_display_columns(list(localized.columns))
    return localized


def format_ranking_version_label(value: str) -> str:
    return str(_translate_scalar("ranking_version", value))


def format_market_label(value: str) -> str:
    translated = _translate_scalar("market", value)
    if translated == value:
        translated = _translate_scalar("market_scope", value)
    return str(translated)


def format_execution_mode_label(value: str) -> str:
    return str(_translate_scalar("execution_mode", value))


def format_ui_value(column: str, value: object) -> str:
    return str(_format_scalar_for_display(column, value))


def format_disk_status_label(value: object) -> str:
    return str(_translate_scalar("status", value))


def recent_runs_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not _metadata_available(settings):
        return pd.DataFrame()
    if metadata_postgres_enabled(settings):
        return fetch_recent_runs(None, limit=limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return fetch_recent_runs(connection, limit=limit)


def disk_report(settings: Settings) -> DiskUsageReport:
    return measure_disk_usage(
        settings.paths.data_dir,
        warning_ratio=settings.storage.warning_ratio,
        prune_ratio=settings.storage.prune_ratio,
        limit_ratio=settings.storage.limit_ratio,
    )


def provider_health_frame(settings: Settings) -> pd.DataFrame:
    providers = [
        KISProvider(settings),
        DartProvider(settings),
        KrxProvider(settings),
        NaverNewsProvider(settings),
    ]
    rows: list[ProviderHealth] = []
    try:
        for provider in providers:
            try:
                rows.append(provider.health_check())
            except Exception as exc:
                rows.append(
                    ProviderHealth(
                        provider=provider.provider_name,
                        configured=provider.is_configured(),
                        status="error",
                        detail=str(exc),
                    )
                )
    finally:
        for provider in providers:
            provider.close()
    return pd.DataFrame([asdict(row) for row in rows])


def krx_service_registry_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "service_slug": service.service_slug,
                "display_name_ko": service.display_name_ko,
                "category": service.category,
                "endpoint_url": service.endpoint_url,
                "request_date_field": service.request_date_field,
                "approval_required": service.approval_required,
                "expected_usage": service.expected_usage,
                "request_cost_weight": service.request_cost_weight,
            }
            for service in KRX_SERVICE_REGISTRY
        ]
    )


def latest_krx_service_status_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                service_slug,
                display_name_ko,
                approval_expected,
                enabled_by_env,
                last_smoke_status,
                last_smoke_ts,
                last_success_ts,
                last_http_status,
                last_error_class,
                fallback_mode
            FROM vw_latest_krx_service_status
            ORDER BY display_name_ko
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_krx_budget_snapshot_frame(settings: Settings, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                provider_name,
                date_kst,
                request_budget,
                requests_used,
                usage_ratio,
                throttle_state,
                snapshot_ts
            FROM vw_latest_external_api_budget_snapshot
            WHERE provider_name = 'krx'
            ORDER BY date_kst DESC, snapshot_ts DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_krx_request_log_frame(settings: Settings, limit: int = 30) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                request_ts,
                provider_name,
                service_slug,
                as_of_date,
                http_status,
                status,
                latency_ms,
                rows_received,
                used_fallback,
                error_code
            FROM fact_external_api_request_log
            WHERE provider_name = 'krx'
            ORDER BY request_ts DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_krx_source_attribution_frame(settings: Settings, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                snapshot_ts,
                as_of_date,
                page_slug,
                component_slug,
                source_label,
                provider_name,
                active_flag
            FROM vw_latest_source_attribution_snapshot
            WHERE provider_name = 'krx'
            ORDER BY snapshot_ts DESC, page_slug, component_slug
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def watermark_frame(settings: Settings) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"threshold": "warning", "ratio": settings.storage.warning_ratio},
            {"threshold": "prune", "ratio": settings.storage.prune_ratio},
            {"threshold": "limit", "ratio": settings.storage.limit_ratio},
        ]
    )


def _preferred_ranking_versions() -> list[str]:
    return [
        SELECTION_ENGINE_V2_VERSION,
        SELECTION_ENGINE_VERSION,
        EXPLANATORY_RANKING_VERSION,
    ]


def _prediction_version_for_ranking(ranking_version: str | None) -> str | None:
    if ranking_version == SELECTION_ENGINE_V2_VERSION:
        return ALPHA_PREDICTION_VERSION
    if ranking_version == SELECTION_ENGINE_VERSION:
        return PREDICTION_VERSION
    return None


def _resolve_latest_ranking_version(connection, ranking_version: str | None) -> str | None:
    if ranking_version:
        return ranking_version
    preferred_versions = _preferred_ranking_versions()
    order_clause = " ".join(
        [
            f"WHEN ranking_version = '{value}' THEN {index}"
            for index, value in enumerate(preferred_versions)
        ]
    )
    row = connection.execute(
        f"""
        SELECT ranking_version
        FROM fact_ranking
        ORDER BY
            CASE {order_clause} ELSE {len(preferred_versions)} END,
            as_of_date DESC,
            created_at DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else str(row[0])


def _resolve_latest_ranking_date(connection, ranking_version: str | None) -> object:
    effective_version = _resolve_latest_ranking_version(connection, ranking_version)
    if effective_version is None:
        return None
    return connection.execute(
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = ?
        """,
        [effective_version],
    ).fetchone()[0]


def universe_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                COUNT(*) AS total_symbols,
                COUNT(*) FILTER (WHERE market = 'KOSPI') AS kospi_symbols,
                COUNT(*) FILTER (WHERE market = 'KOSDAQ') AS kosdaq_symbols,
                COUNT(*) FILTER (WHERE dart_corp_code IS NOT NULL) AS dart_mapped_symbols,
                COUNT(*) FILTER (
                    WHERE market IN ('KOSPI', 'KOSDAQ')
                      AND COALESCE(is_common_stock, FALSE)
                      AND NOT COALESCE(is_etf, FALSE)
                      AND NOT COALESCE(is_etn, FALSE)
                      AND NOT COALESCE(is_spac, FALSE)
                      AND NOT COALESCE(is_reit, FALSE)
                      AND NOT COALESCE(is_delisted, FALSE)
                ) AS active_common_stock_count
            FROM dim_symbol
            """
        ).fetchdf()


def calendar_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                MIN(trading_date) AS min_trading_date,
                MAX(trading_date) AS max_trading_date,
                COUNT(*) AS total_days,
                COUNT(*) FILTER (WHERE is_trading_day) AS trading_days,
                COUNT(*) FILTER (WHERE is_override) AS override_days
            FROM dim_trading_calendar
            """
        ).fetchdf()


def latest_sync_runs_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    return _metadata_frame(
        settings,
        """
            WITH ranked_runs AS (
                SELECT
                    run_type,
                    started_at,
                    finished_at,
                    status,
                    notes,
                    ROW_NUMBER() OVER (
                        PARTITION BY run_type
                        ORDER BY started_at DESC
                    ) AS row_number
                FROM ops_run_manifest
                WHERE run_type IN (
                    'sync_universe',
                    'sync_trading_calendar',
                    'sync_daily_ohlcv',
                    'sync_fundamentals_snapshot',
                    'sync_news_metadata',
                    'sync_investor_flow',
                    'build_feature_store',
                    'build_forward_labels',
                    'build_market_regime_snapshot',
                    'materialize_explanatory_ranking',
                    'materialize_selection_engine_v1',
                    'calibrate_proxy_prediction_bands',
                    'materialize_selection_outcomes',
                    'materialize_prediction_evaluation',
                    'materialize_calibration_diagnostics',
                    'validate_explanatory_ranking',
                    'validate_selection_engine_v1',
                    'validate_evaluation_pipeline',
                    'render_discord_eod_report',
                    'publish_discord_eod_report',
                    'render_postmortem_report',
                    'publish_discord_postmortem_report'
                )
            )
            SELECT
                run_type,
                started_at,
                finished_at,
                status,
                notes
            FROM ranked_runs
            WHERE row_number = 1
            ORDER BY run_type
        """,
    )


def research_data_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                (SELECT MAX(trading_date) FROM fact_daily_ohlcv) AS latest_ohlcv_date,
                (SELECT COUNT(*) FROM fact_daily_ohlcv WHERE trading_date = (
                    SELECT MAX(trading_date) FROM fact_daily_ohlcv
                )) AS latest_ohlcv_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_fundamentals_snapshot
                ) AS latest_fundamentals_date,
                (SELECT COUNT(*) FROM fact_fundamentals_snapshot WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_fundamentals_snapshot
                )) AS latest_fundamentals_rows,
                (SELECT MAX(signal_date) FROM fact_news_item) AS latest_news_date,
                (SELECT COUNT(*) FROM fact_news_item WHERE signal_date = (
                    SELECT MAX(signal_date) FROM fact_news_item
                )) AS latest_news_rows,
                (SELECT COUNT(*) FROM fact_news_item WHERE signal_date = (
                    SELECT MAX(signal_date) FROM fact_news_item
                ) AND COALESCE(symbol_candidates, '[]') = '[]') AS latest_news_unmatched,
                (SELECT MAX(trading_date) FROM fact_investor_flow) AS latest_flow_date,
                (SELECT COUNT(*) FROM fact_investor_flow WHERE trading_date = (
                    SELECT MAX(trading_date) FROM fact_investor_flow
                )) AS latest_flow_rows,
                (SELECT MAX(as_of_date) FROM fact_feature_snapshot) AS latest_feature_date,
                (SELECT COUNT(*) FROM fact_feature_snapshot WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_feature_snapshot
                )) AS latest_feature_rows,
                (SELECT MAX(as_of_date) FROM fact_forward_return_label) AS latest_label_date,
                (SELECT COUNT(*) FROM fact_forward_return_label WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_forward_return_label
                ) AND label_available_flag) AS latest_available_label_rows,
                (SELECT MAX(as_of_date) FROM fact_market_regime_snapshot) AS latest_regime_date,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = 'explanatory_ranking_v0'
                ) AS latest_explanatory_ranking_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = 'explanatory_ranking_v0'
                ) AND ranking_version = 'explanatory_ranking_v0')
                    AS latest_explanatory_ranking_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = ?
                ) AS latest_selection_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_ranking WHERE ranking_version = ?
                ) AND ranking_version = ?) AS latest_selection_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_prediction
                    WHERE prediction_version = ?
                ) AS latest_prediction_date,
                (SELECT COUNT(*) FROM fact_prediction WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_prediction WHERE prediction_version = ?
                ) AND prediction_version = ?) AS latest_prediction_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_prediction
                    WHERE prediction_version = ?
                ) AS latest_model_prediction_date,
                (SELECT COUNT(*) FROM fact_prediction WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_prediction WHERE prediction_version = ?
                ) AND prediction_version = ?) AS latest_model_prediction_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = ?
                ) AS latest_selection_v2_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_ranking WHERE ranking_version = ?
                ) AND ranking_version = ?) AS latest_selection_v2_rows,
                (
                    SELECT MAX(train_end_date)
                    FROM fact_model_training_run
                    WHERE model_version = ?
                ) AS latest_model_train_date,
                (
                    SELECT COALESCE(SUM(train_row_count), 0)
                    FROM fact_model_training_run
                    WHERE train_end_date = (
                        SELECT MAX(train_end_date)
                        FROM fact_model_training_run
                        WHERE model_version = ?
                    )
                      AND model_version = ?
                ) AS latest_model_train_rows,
                (SELECT MAX(evaluation_date) FROM fact_selection_outcome) AS latest_outcome_date,
                (SELECT COUNT(*) FROM fact_selection_outcome WHERE evaluation_date = (
                    SELECT MAX(evaluation_date) FROM fact_selection_outcome
                )) AS latest_outcome_rows,
                (
                    SELECT MAX(summary_date)
                    FROM fact_evaluation_summary
                ) AS latest_evaluation_summary_date,
                (SELECT COUNT(*) FROM fact_evaluation_summary WHERE summary_date = (
                    SELECT MAX(summary_date) FROM fact_evaluation_summary
                )) AS latest_evaluation_summary_rows,
                (
                    SELECT MAX(diagnostic_date)
                    FROM fact_calibration_diagnostic
                ) AS latest_calibration_date,
                (SELECT COUNT(*) FROM fact_calibration_diagnostic WHERE diagnostic_date = (
                    SELECT MAX(diagnostic_date) FROM fact_calibration_diagnostic
                )) AS latest_calibration_rows
            """,
            [
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_VERSION,
                PREDICTION_VERSION,
                PREDICTION_VERSION,
                PREDICTION_VERSION,
                ALPHA_PREDICTION_VERSION,
                ALPHA_PREDICTION_VERSION,
                ALPHA_PREDICTION_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                ALPHA_MODEL_VERSION,
                ALPHA_MODEL_VERSION,
                ALPHA_MODEL_VERSION,
            ],
        ).fetchdf()


def recent_failure_runs_frame(settings: Settings, *, limit: int = 5) -> pd.DataFrame:
    if not _metadata_available(settings):
        return pd.DataFrame()
    return _metadata_frame(
        settings,
        """
            SELECT
                run_type,
                as_of_date,
                started_at,
                finished_at,
                error_message
            FROM ops_run_manifest
            WHERE status = 'failed'
            ORDER BY started_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_ohlcv_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                trading_date,
                symbol,
                open,
                high,
                low,
                close,
                volume
            FROM fact_daily_ohlcv
            ORDER BY trading_date DESC, symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_fundamentals_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                as_of_date,
                symbol,
                revenue,
                operating_income,
                net_income,
                roe,
                debt_ratio
            FROM fact_fundamentals_snapshot
            ORDER BY as_of_date DESC, symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_news_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                signal_date,
                published_at,
                title,
                publisher,
                symbol_candidates,
                query_bucket
            FROM fact_news_item
            ORDER BY signal_date DESC, published_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_feature_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT *
            FROM vw_feature_matrix_latest
            ORDER BY symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_label_coverage_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(as_of_date) AS as_of_date
                FROM fact_forward_return_label
            )
            SELECT
                label.horizon,
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE label_available_flag) AS available_rows,
                AVG(CASE WHEN label_available_flag THEN 1.0 ELSE 0.0 END) AS coverage_ratio
            FROM fact_forward_return_label AS label
            JOIN latest_date
              ON label.as_of_date = latest_date.as_of_date
            GROUP BY label.horizon
            ORDER BY label.horizon
            """
        ).fetchdf()


def latest_feature_coverage_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(as_of_date) AS as_of_date
                FROM fact_feature_snapshot
            )
            SELECT
                feature_name,
                COUNT(*) AS symbol_rows,
                AVG(CASE WHEN feature_value IS NULL THEN 1.0 ELSE 0.0 END) AS null_ratio
            FROM fact_feature_snapshot
            WHERE as_of_date = (SELECT as_of_date FROM latest_date)
              AND feature_name IN (
                'ret_5d',
                'ret_20d',
                'adv_20',
                'roe_latest',
                'debt_ratio_latest',
                'news_count_3d',
                'foreign_net_value_ratio_5d',
                'smart_money_flow_ratio_20d',
                'flow_coverage_flag',
                'data_confidence_score'
              )
            GROUP BY feature_name
            ORDER BY feature_name
            """
        ).fetchdf()


def latest_regime_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                as_of_date,
                market_scope,
                regime_state,
                regime_score,
                breadth_up_ratio,
                median_symbol_return_1d,
                market_realized_vol_20d,
                turnover_burst_z
            FROM vw_market_regime_latest
            ORDER BY market_scope
            """
        ).fetchdf()


def latest_version_frame(settings: Settings) -> pd.DataFrame:
    if not _metadata_available(settings):
        return pd.DataFrame()
    return _metadata_frame(
        settings,
        """
            SELECT
                (
                    SELECT feature_version
                    FROM ops_run_manifest
                    WHERE run_type = 'build_feature_store'
                      AND status = 'success'
                      AND feature_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_feature_version,
                (
                    SELECT ranking_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_explanatory_ranking'
                      AND status = 'success'
                      AND ranking_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_explanatory_ranking_version,
                (
                    SELECT ranking_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_selection_engine_v1'
                      AND status = 'success'
                      AND ranking_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_selection_ranking_version,
                (
                    SELECT ranking_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_selection_engine_v2'
                      AND status = 'success'
                      AND ranking_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_selection_v2_ranking_version,
                (
                    SELECT model_version
                    FROM ops_run_manifest
                    WHERE run_type = 'calibrate_proxy_prediction_bands'
                      AND status = 'success'
                      AND model_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_prediction_version
                ,
                (
                    SELECT model_version
                    FROM ops_run_manifest
                    WHERE run_type = 'train_alpha_model_v1'
                      AND status = 'success'
                      AND model_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_alpha_model_version
                ,
                (
                    SELECT model_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_alpha_predictions_v1'
                      AND status = 'success'
                      AND model_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_alpha_prediction_version
            """
    )


def latest_validation_summary_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                start_date,
                end_date,
                horizon,
                bucket_type,
                bucket_name,
                symbol_count,
                avg_gross_forward_return,
                avg_excess_forward_return,
                median_excess_forward_return,
                top_decile_gap
            FROM vw_latest_ranking_validation_summary
            ORDER BY bucket_type, horizon, bucket_name
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def available_ranking_versions(settings: Settings) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        preferred_versions = _preferred_ranking_versions()
        order_clause = " ".join(
            [
                f"WHEN ranking_version = '{value}' THEN {index}"
                for index, value in enumerate(preferred_versions)
            ]
        )
        rows = connection.execute(
            f"""
            SELECT DISTINCT ranking_version
            FROM fact_ranking
            ORDER BY
                CASE {order_clause} ELSE {len(preferred_versions)} END,
                ranking_version
            """
        ).fetchall()
    return [str(row[0]) for row in rows]


def available_ranking_dates(settings: Settings, *, ranking_version: str | None = None) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return []
        rows = connection.execute(
            """
            SELECT DISTINCT as_of_date
            FROM fact_ranking
            WHERE ranking_version = ?
            ORDER BY as_of_date DESC
            """,
            [effective_version],
        ).fetchall()
    return [str(row[0]) for row in rows]


def available_evaluation_dates(settings: Settings) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT evaluation_date
            FROM fact_selection_outcome
            WHERE evaluation_date IS NOT NULL
            ORDER BY evaluation_date DESC
            """
        ).fetchall()
    return [str(row[0]) for row in rows]


def leaderboard_frame(
    settings: Settings,
    *,
    as_of_date: str | None = None,
    horizon: int = 5,
    market: str = "ALL",
    limit: int = 20,
    ranking_version: str | None = None,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return pd.DataFrame()
        prediction_version = _prediction_version_for_ranking(effective_version)
        selected_date = as_of_date or _resolve_latest_ranking_date(connection, effective_version)
        if selected_date is None:
            return pd.DataFrame()
        frame = connection.execute(
            """
            SELECT
                ranking.as_of_date,
                ranking.as_of_date AS selection_date,
                (
                    SELECT MIN(calendar.trading_date)
                    FROM dim_trading_calendar AS calendar
                    WHERE calendar.trading_date > ranking.as_of_date
                      AND calendar.is_trading_day
                ) AS next_entry_trade_date,
                ranking.symbol,
                symbol.company_name,
                symbol.market,
                symbol.sector,
                symbol.industry,
                ranking.horizon,
                ranking.final_selection_value,
                ranking.final_selection_rank_pct,
                ranking.grade,
                ranking.regime_state,
                ranking.ranking_version,
                ranking.top_reason_tags_json,
                ranking.risk_flags_json,
                ranking.explanatory_score_json,
                prediction.expected_excess_return,
                prediction.lower_band,
                prediction.median_band,
                prediction.upper_band,
                prediction.model_spec_id,
                prediction.active_alpha_model_id,
                prediction.uncertainty_score,
                prediction.disagreement_score,
                prediction.fallback_flag,
                prediction.fallback_reason,
                daily.close AS selection_close_price,
                outcome.outcome_status,
                outcome.realized_excess_return,
                outcome.band_status
            FROM fact_ranking AS ranking
            JOIN dim_symbol AS symbol
              ON ranking.symbol = symbol.symbol
            LEFT JOIN fact_prediction AS prediction
              ON ranking.as_of_date = prediction.as_of_date
             AND ranking.symbol = prediction.symbol
             AND ranking.horizon = prediction.horizon
              AND prediction.prediction_version = ?
              AND prediction.ranking_version = ranking.ranking_version
            LEFT JOIN fact_daily_ohlcv AS daily
              ON ranking.symbol = daily.symbol
             AND ranking.as_of_date = daily.trading_date
            LEFT JOIN fact_selection_outcome AS outcome
              ON ranking.as_of_date = outcome.selection_date
             AND ranking.symbol = outcome.symbol
             AND ranking.horizon = outcome.horizon
             AND ranking.ranking_version = outcome.ranking_version
            WHERE ranking.as_of_date = ?
              AND ranking.horizon = ?
              AND ranking.ranking_version = ?
            ORDER BY ranking.final_selection_value DESC, ranking.symbol
            """,
            [prediction_version, selected_date, horizon, effective_version],
        ).fetchdf()
    if frame.empty:
        return frame
    if market.upper() != "ALL":
        frame = frame.loc[frame["market"].str.upper() == market.upper()].copy()
    base_price = pd.to_numeric(frame.get("selection_close_price"), errors="coerce")
    expected = pd.to_numeric(frame.get("expected_excess_return"), errors="coerce")
    upper = pd.to_numeric(frame.get("upper_band"), errors="coerce")
    lower = pd.to_numeric(frame.get("lower_band"), errors="coerce")
    frame["flat_target_price"] = base_price * (1.0 + expected)
    frame["flat_upper_target_price"] = base_price * (1.0 + upper)
    frame["flat_stop_price"] = base_price * (1.0 + lower)
    frame["reasons"] = frame["top_reason_tags_json"].fillna("[]")
    frame["risks"] = frame["risk_flags_json"].fillna("[]")
    return frame.head(limit).reset_index(drop=True)


def latest_sector_outlook_frame(
    settings: Settings,
    *,
    as_of_date: str | None = None,
    horizon: int = 5,
    ranking_version: str | None = None,
    limit: int = 3,
    candidate_limit: int = 40,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return pd.DataFrame()
        prediction_version = _prediction_version_for_ranking(effective_version)
        selected_date = as_of_date or _resolve_latest_ranking_date(connection, effective_version)
        if selected_date is None:
            return pd.DataFrame()
        return sector_outlook_frame(
            connection,
            as_of_date=selected_date,
            ranking_version=effective_version,
            prediction_version=prediction_version,
            horizon=int(horizon),
            candidate_limit=int(candidate_limit),
            limit=int(limit),
        )


def leaderboard_grade_count_frame(
    settings: Settings,
    *,
    as_of_date: str | None = None,
    horizon: int = 5,
    ranking_version: str | None = None,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return pd.DataFrame()
        selected_date = as_of_date or _resolve_latest_ranking_date(connection, effective_version)
        if selected_date is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT grade, COUNT(*) AS row_count
            FROM fact_ranking
            WHERE as_of_date = ?
              AND horizon = ?
              AND ranking_version = ?
            GROUP BY grade
            ORDER BY grade
            """,
            [selected_date, horizon, effective_version],
        ).fetchdf()


def latest_flow_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(trading_date) AS trading_date
                FROM fact_investor_flow
            )
            SELECT
                flow.trading_date,
                COUNT(*) AS row_count,
                AVG(
                    CASE WHEN foreign_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS foreign_value_coverage,
                AVG(
                    CASE WHEN institution_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS institution_value_coverage,
                AVG(
                    CASE WHEN individual_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS individual_value_coverage
            FROM fact_investor_flow AS flow
            JOIN latest_date
              ON flow.trading_date = latest_date.trading_date
            GROUP BY flow.trading_date
            """
        ).fetchdf()


def latest_prediction_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_dates AS (
                SELECT
                    prediction_version,
                    MAX(as_of_date) AS as_of_date
                FROM fact_prediction
                WHERE prediction_version IN (?, ?)
                GROUP BY prediction_version
            )
            SELECT
                prediction.prediction_version,
                horizon,
                COUNT(*) AS row_count,
                AVG(expected_excess_return) AS avg_expected_excess_return,
                AVG(upper_band - lower_band) AS avg_band_width,
                AVG(uncertainty_score) AS uncertainty_score,
                AVG(disagreement_score) AS disagreement_score
            FROM fact_prediction AS prediction
            JOIN latest_dates
              ON prediction.prediction_version = latest_dates.prediction_version
             AND prediction.as_of_date = latest_dates.as_of_date
            GROUP BY prediction.prediction_version, horizon
            ORDER BY prediction.prediction_version, horizon
            """,
            [PREDICTION_VERSION, ALPHA_PREDICTION_VERSION],
        ).fetchdf()


def latest_model_training_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                horizon,
                train_end_date,
                train_row_count,
                validation_row_count,
                fallback_flag,
                fallback_reason
            FROM vw_latest_model_training_run
            WHERE model_version = ?
            ORDER BY horizon
            """,
            [ALPHA_MODEL_VERSION],
        ).fetchdf()


def latest_model_metric_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                horizon,
                member_name,
                split_name,
                metric_name,
                metric_value,
                sample_count
            FROM vw_latest_model_metric_summary
            WHERE model_version = ?
              AND split_name = 'validation'
            ORDER BY horizon, member_name, metric_name
            """,
            [ALPHA_MODEL_VERSION],
        ).fetchdf()


def latest_selection_engine_comparison_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_summary AS (
                SELECT *
                FROM vw_latest_evaluation_summary
                WHERE segment_type = 'coverage'
                  AND segment_value = 'all'
                  AND ranking_version IN (?, ?, ?)
            )
            SELECT
                v2.summary_date,
                v2.window_type,
                v2.horizon,
                v2.mean_realized_excess_return AS selection_v2_avg_excess,
                v1.mean_realized_excess_return AS selection_v1_avg_excess,
                expl.mean_realized_excess_return AS explanatory_v0_avg_excess,
                v2.mean_realized_excess_return - v1.mean_realized_excess_return
                    AS v2_vs_v1_gap,
                v2.mean_realized_excess_return - expl.mean_realized_excess_return
                    AS v2_vs_explanatory_gap
            FROM latest_summary AS v2
            LEFT JOIN latest_summary AS v1
              ON v2.summary_date = v1.summary_date
             AND v2.window_type = v1.window_type
             AND v2.horizon = v1.horizon
             AND v1.ranking_version = ?
            LEFT JOIN latest_summary AS expl
              ON v2.summary_date = expl.summary_date
             AND v2.window_type = expl.window_type
             AND v2.horizon = expl.horizon
             AND expl.ranking_version = ?
            WHERE v2.ranking_version = ?
            ORDER BY v2.window_type, v2.horizon
            """,
            [
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_VERSION,
                EXPLANATORY_RANKING_VERSION,
                SELECTION_ENGINE_VERSION,
                EXPLANATORY_RANKING_VERSION,
                SELECTION_ENGINE_V2_VERSION,
            ],
        ).fetchdf()


def latest_selection_validation_summary_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                start_date,
                end_date,
                horizon,
                bucket_type,
                bucket_name,
                symbol_count,
                avg_excess_forward_return,
                median_excess_forward_return,
                hit_rate,
                avg_expected_excess_return,
                avg_prediction_error,
                top_decile_gap
            FROM vw_latest_selection_validation_summary
            WHERE ranking_version = ?
            ORDER BY bucket_type, horizon, bucket_name
            LIMIT ?
            """,
            [SELECTION_ENGINE_VERSION, limit],
        ).fetchdf()


def latest_outcome_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(evaluation_date) AS evaluation_date
                FROM fact_selection_outcome
            )
            SELECT
                evaluation_date,
                horizon,
                ranking_version,
                COUNT(*) AS row_count,
                COUNT(*) FILTER (WHERE outcome_status = 'matured') AS matured_rows,
                AVG(realized_excess_return) AS avg_realized_excess_return,
                AVG(CASE WHEN realized_excess_return > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate
            FROM fact_selection_outcome
            WHERE evaluation_date = (SELECT evaluation_date FROM latest_date)
            GROUP BY evaluation_date, horizon, ranking_version
            ORDER BY horizon, ranking_version
            """
        ).fetchdf()


def latest_evaluation_summary_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                summary_date,
                window_type,
                horizon,
                ranking_version,
                segment_value,
                count_evaluated,
                mean_realized_excess_return,
                hit_rate,
                avg_expected_excess_return
            FROM vw_latest_evaluation_summary
            WHERE segment_type = 'coverage'
              AND segment_value = 'all'
            ORDER BY window_type, horizon, ranking_version
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_evaluation_comparison_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_summary AS (
                SELECT *
                FROM vw_latest_evaluation_summary
                WHERE segment_type = 'coverage'
                  AND segment_value = 'all'
            )
            SELECT
                selection.summary_date,
                selection.window_type,
                selection.horizon,
                selection.mean_realized_excess_return AS selection_avg_excess,
                explanatory.mean_realized_excess_return AS explanatory_avg_excess,
                selection.mean_realized_excess_return
                    - explanatory.mean_realized_excess_return AS avg_excess_gap,
                selection.hit_rate - explanatory.hit_rate AS hit_rate_gap
            FROM latest_summary AS selection
            JOIN latest_summary AS explanatory
              ON selection.summary_date = explanatory.summary_date
             AND selection.window_type = explanatory.window_type
             AND selection.horizon = explanatory.horizon
             AND selection.ranking_version = ?
             AND explanatory.ranking_version = ?
            ORDER BY selection.window_type, selection.horizon
            """,
            [SELECTION_ENGINE_VERSION, EXPLANATORY_RANKING_VERSION],
        ).fetchdf()


def latest_alpha_promotion_summary_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        frame = load_alpha_promotion_summary(connection)
    if frame.empty:
        return frame
    return frame.head(limit).copy()


def latest_alpha_active_model_frame(
    settings: Settings,
    *,
    as_of_date=None,
    limit: int = 20,
    active_only: bool = True,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        target_date = as_of_date
        if target_date is None:
            row = connection.execute(
                "SELECT MAX(effective_from_date) FROM fact_alpha_active_model"
            ).fetchone()
            target_date = None if row is None or row[0] is None else pd.Timestamp(row[0]).date()
        if target_date is None:
            return pd.DataFrame()
        if active_only:
            return connection.execute(
                """
                SELECT
                    active.horizon,
                    active.model_spec_id,
                    active.training_run_id,
                    train.train_end_date,
                    active.model_version,
                    active.source_type,
                    active.promotion_type,
                    active.effective_from_date,
                    active.effective_to_date,
                    active.note
                FROM fact_alpha_active_model AS active
                LEFT JOIN fact_model_training_run AS train
                  ON active.training_run_id = train.training_run_id
                WHERE active.effective_from_date <= ?
                  AND (active.effective_to_date IS NULL OR active.effective_to_date >= ?)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY active.horizon
                    ORDER BY active.effective_from_date DESC, active.created_at DESC
                ) = 1
                ORDER BY active.horizon
                LIMIT ?
                """,
                [target_date, target_date, limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                horizon,
                model_spec_id,
                training_run_id,
                model_version,
                source_type,
                promotion_type,
                effective_from_date,
                effective_to_date,
                active_flag,
                rollback_of_active_alpha_model_id,
                note,
                updated_at
            FROM fact_alpha_active_model
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_alpha_training_candidate_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest AS (
                SELECT
                    model_spec_id,
                    horizon,
                    estimation_scheme,
                    rolling_window_days,
                    train_end_date,
                    training_run_id,
                    model_version,
                    fallback_flag,
                    fallback_reason,
                    created_at
                FROM fact_model_training_run
                WHERE model_domain = ?
                  AND status = 'success'
                  AND artifact_uri IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY model_spec_id, horizon
                    ORDER BY train_end_date DESC, created_at DESC
                ) = 1
            )
            SELECT
                model_spec_id,
                horizon,
                estimation_scheme,
                rolling_window_days,
                train_end_date,
                training_run_id,
                model_version,
                fallback_flag,
                fallback_reason
            FROM latest
            ORDER BY model_spec_id, horizon
            LIMIT ?
            """,
            [ALPHA_MODEL_DOMAIN, limit],
        ).fetchdf()


def latest_alpha_model_spec_frame(
    settings: Settings,
    *,
    limit: int = 20,
    active_only: bool = True,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    query = """
        SELECT
            model_spec_id,
            model_domain,
            model_version,
            estimation_scheme,
            rolling_window_days,
            feature_version,
            label_version,
            selection_engine_version,
            active_candidate_flag,
            updated_at
        FROM dim_alpha_model_spec
    """
    parameters: list[object] = []
    if active_only:
        query += " WHERE active_candidate_flag = TRUE"
    query += " ORDER BY model_spec_id LIMIT ?"
    parameters.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(query, parameters).fetchdf()


def latest_alpha_rollback_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                horizon,
                model_spec_id,
                training_run_id,
                promotion_type,
                rollback_of_active_alpha_model_id,
                effective_from_date,
                note,
                updated_at
            FROM fact_alpha_active_model
            WHERE promotion_type = 'ROLLBACK'
               OR rollback_of_active_alpha_model_id IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


UI_VALUE_LABELS.setdefault("run_type", {}).update(
    {
        "freeze_alpha_active_model": "알파 모델 수동 반영",
        "rollback_alpha_active_model": "알파 모델 되돌리기",
        "run_alpha_auto_promotion": "알파 모델 자동 반영 점검",
    }
)
UI_VALUE_LABELS.setdefault("model_spec_id", {}).update(
    {
        "alpha_recursive_expanding_v1": "확장형 누적 학습",
        "alpha_rolling_120_v1": "최근 120거래일 중심 학습",
        "alpha_rolling_250_v1": "최근 250거래일 중심 학습",
        "alpha_recursive_rolling_combo": "누적+최근 구간 혼합",
    }
)
UI_VALUE_LABELS.setdefault("entry_basis", {}).update(
    {
        "next_open": "다음 거래일 시가 기준",
        "cash_buffer": "현금 유지",
    }
)
UI_VALUE_LABELS.setdefault("exit_basis", {}).update(
    {
        "same_day_close": "같은 날 종가 기준",
        "future_close": "관찰 종료일 종가 기준",
    }
)
UI_VALUE_LABELS.setdefault("estimation_scheme", {}).update(
    {
        "recursive": "Recursive",
        "rolling": "Rolling",
    }
)
UI_VALUE_LABELS.setdefault("promotion_type", {}).update(
    {
        "MANUAL_FREEZE": "수동 반영",
    }
)
UI_VALUE_LABELS.setdefault("promotion_type", {}).update(
    {
        "AUTO_PROMOTION": "자동 반영",
        "ROLLBACK": "이전 값으로 되돌림",
    }
)
UI_COLUMN_LABELS.update(
    {
        "summary_title": "요약 제목",
        "decision_label": "판단 결과",
        "decision_reason_label": "판단 사유",
        "active_model_label": "현재 사용 모델",
        "comparison_model_label": "비교 후보 모델",
        "comparison_role_label": "비교 역할",
        "active_top10_mean_excess_return": "현재 모델 상위 10종목 평균 초과수익률",
        "comparison_top10_mean_excess_return": "비교 후보 상위 10종목 평균 초과수익률",
        "promotion_gap": "수익률 차이",
        "active_point_loss": "현재 모델 점수 손실",
        "comparison_point_loss": "비교 후보 점수 손실",
        "active_rank_ic": "현재 모델 순위 일치도",
        "comparison_rank_ic": "비교 후보 순위 일치도",
        "superior_set_label": "우세 구간",
        "active_effective_from_date": "현재 모델 사용 시작일",
        "active_source_type": "현재 모델 반영 경로",
        "active_promotion_type": "현재 모델 반영 방식",
        "active_candidate_flag": "후보 사용 가능",
        "rolling_window_days": "비교 구간 길이",
        "rollback_of_active_alpha_model_id": "되돌린 대상 알파 모델 ID",
        "train_end_date": "학습 기준 종료일",
    }
)


def latest_calibration_diagnostic_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                diagnostic_date,
                horizon,
                bin_type,
                bin_value,
                sample_count,
                expected_median,
                observed_mean,
                coverage_rate,
                median_bias,
                quality_flag
            FROM vw_latest_calibration_diagnostic
            ORDER BY horizon, bin_type, bin_value
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def evaluation_outcomes_frame(
    settings: Settings,
    *,
    evaluation_date: str | None = None,
    horizon: int = 5,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    limit: int = 50,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if evaluation_date is None:
            row = connection.execute(
                """
                SELECT MAX(evaluation_date)
                FROM fact_selection_outcome
                """
            ).fetchone()
            if row is None or row[0] is None:
                return pd.DataFrame()
            evaluation_date = str(row[0])
        return connection.execute(
            """
            SELECT
                outcome.evaluation_date,
                outcome.selection_date,
                outcome.symbol,
                meta.company_name,
                meta.market,
                outcome.horizon,
                outcome.ranking_version,
                outcome.final_selection_value,
                outcome.expected_excess_return_at_selection,
                outcome.realized_excess_return,
                outcome.band_status,
                outcome.outcome_status
            FROM fact_selection_outcome AS outcome
            JOIN dim_symbol AS meta
              ON outcome.symbol = meta.symbol
            WHERE outcome.evaluation_date = ?
              AND outcome.horizon = ?
              AND outcome.ranking_version = ?
            ORDER BY outcome.realized_excess_return DESC, outcome.symbol
            LIMIT ?
            """,
            [evaluation_date, horizon, ranking_version, limit],
        ).fetchdf()


def market_pulse_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                regime.as_of_date,
                regime.regime_state,
                regime.regime_score,
                regime.breadth_up_ratio,
                regime.market_realized_vol_20d,
                flow.row_count AS investor_flow_rows,
                flow.foreign_positive_ratio,
                flow.institution_positive_ratio,
                selection.selection_rows,
                prediction.prediction_rows
            FROM (
                SELECT *
                FROM vw_market_regime_latest
                WHERE market_scope = 'KR_ALL'
            ) AS regime
            LEFT JOIN (
                SELECT
                    trading_date,
                    COUNT(*) AS row_count,
                    AVG(
                        CASE WHEN foreign_net_value > 0 THEN 1.0 ELSE 0.0 END
                    ) AS foreign_positive_ratio,
                    AVG(
                        CASE WHEN institution_net_value > 0 THEN 1.0 ELSE 0.0 END
                    ) AS institution_positive_ratio
                FROM fact_investor_flow
                WHERE trading_date = (SELECT MAX(trading_date) FROM fact_investor_flow)
                GROUP BY trading_date
            ) AS flow
              ON regime.as_of_date = flow.trading_date
            LEFT JOIN (
                SELECT as_of_date, COUNT(*) AS selection_rows
                FROM fact_ranking
                WHERE ranking_version = ?
                GROUP BY as_of_date
                QUALIFY ROW_NUMBER() OVER (ORDER BY as_of_date DESC) = 1
            ) AS selection
              ON regime.as_of_date = selection.as_of_date
            LEFT JOIN (
                SELECT as_of_date, COUNT(*) AS prediction_rows
                FROM fact_prediction
                WHERE prediction_version = ?
                GROUP BY as_of_date
                QUALIFY ROW_NUMBER() OVER (ORDER BY as_of_date DESC) = 1
            ) AS prediction
              ON regime.as_of_date = prediction.as_of_date
            """,
            [SELECTION_ENGINE_VERSION, PREDICTION_VERSION],
        ).fetchdf()


def latest_market_news_frame(settings: Settings, *, limit: int = 5) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT signal_date, title, publisher, link
            FROM fact_news_item
            WHERE signal_date = (SELECT MAX(signal_date) FROM fact_news_item)
              AND COALESCE(is_market_wide, FALSE)
            ORDER BY published_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def available_symbols(settings: Settings, *, limit: int | None = None) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if limit is not None and int(limit) > 0:
            rows = connection.execute(
                """
                SELECT symbol
                FROM dim_symbol
                WHERE market IN ('KOSPI', 'KOSDAQ')
                ORDER BY symbol
                LIMIT ?
                """,
                [int(limit)],
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT symbol
                FROM dim_symbol
                WHERE market IN ('KOSPI', 'KOSDAQ')
                ORDER BY symbol
                """
            ).fetchall()
    return [str(row[0]).zfill(6) for row in rows]


def available_symbol_options(
    settings: Settings,
    *,
    limit: int | None = None,
) -> list[tuple[str, str | None]]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if limit is not None and int(limit) > 0:
            rows = connection.execute(
                """
                SELECT symbol, company_name
                FROM dim_symbol
                WHERE market IN ('KOSPI', 'KOSDAQ')
                ORDER BY symbol
                LIMIT ?
                """,
                [int(limit)],
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT symbol, company_name
                FROM dim_symbol
                WHERE market IN ('KOSPI', 'KOSDAQ')
                ORDER BY symbol
                """
            ).fetchall()
    return [(str(row[0]).zfill(6), row[1]) for row in rows]


def stock_workbench_summary_frame(settings: Settings, *, symbol: str) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                feature.symbol,
                symbol_meta.company_name,
                symbol_meta.market,
                feature.as_of_date,
                feature.ret_5d,
                feature.ret_20d,
                feature.adv_20,
                feature.news_count_3d,
                feature.foreign_net_value_ratio_5d,
                feature.smart_money_flow_ratio_20d,
                feature.flow_coverage_flag,
                selection_v2_1.final_selection_value AS d1_selection_v2_value,
                selection_v2_1.grade AS d1_selection_v2_grade,
                selection_1.final_selection_value AS d1_selection_value,
                selection_1.grade AS d1_grade,
                selection_v2_5.final_selection_value AS d5_selection_v2_value,
                selection_v2_5.grade AS d5_selection_v2_grade,
                selection_5.final_selection_value AS d5_selection_value,
                selection_5.grade AS d5_grade,
                prediction_alpha_5.expected_excess_return AS d5_alpha_expected_excess_return,
                prediction_alpha_5.lower_band AS d5_alpha_lower_band,
                prediction_alpha_5.upper_band AS d5_alpha_upper_band,
                prediction_alpha_5.uncertainty_score AS d5_alpha_uncertainty_score,
                prediction_alpha_5.disagreement_score AS d5_alpha_disagreement_score,
                prediction_alpha_5.fallback_flag AS d5_alpha_fallback_flag,
                prediction_5.expected_excess_return AS d5_expected_excess_return,
                prediction_5.lower_band AS d5_lower_band,
                prediction_5.upper_band AS d5_upper_band,
                outcome_1.realized_excess_return AS d1_realized_excess_return,
                outcome_1.band_status AS d1_band_status,
                outcome_v2_5.realized_excess_return AS d5_selection_v2_realized_excess_return,
                outcome_v2_5.band_status AS d5_selection_v2_band_status,
                outcome_5.realized_excess_return AS d5_realized_excess_return,
                outcome_5.band_status AS d5_band_status
            FROM vw_feature_matrix_latest AS feature
            JOIN dim_symbol AS symbol_meta
              ON feature.symbol = symbol_meta.symbol
            LEFT JOIN vw_ranking_latest AS selection_v2_1
              ON feature.symbol = selection_v2_1.symbol
             AND selection_v2_1.horizon = 1
             AND selection_v2_1.ranking_version = ?
            LEFT JOIN vw_ranking_latest AS selection_1
              ON feature.symbol = selection_1.symbol
             AND selection_1.horizon = 1
             AND selection_1.ranking_version = ?
            LEFT JOIN vw_ranking_latest AS selection_v2_5
              ON feature.symbol = selection_v2_5.symbol
             AND selection_v2_5.horizon = 5
             AND selection_v2_5.ranking_version = ?
            LEFT JOIN vw_ranking_latest AS selection_5
              ON feature.symbol = selection_5.symbol
             AND selection_5.horizon = 5
             AND selection_5.ranking_version = ?
            LEFT JOIN vw_prediction_latest AS prediction_alpha_5
              ON feature.symbol = prediction_alpha_5.symbol
             AND prediction_alpha_5.horizon = 5
             AND prediction_alpha_5.prediction_version = ?
            LEFT JOIN vw_prediction_latest AS prediction_5
              ON feature.symbol = prediction_5.symbol
             AND prediction_5.horizon = 5
             AND prediction_5.prediction_version = ?
            LEFT JOIN vw_selection_outcome_latest AS outcome_1
             ON feature.symbol = outcome_1.symbol
             AND outcome_1.horizon = 1
             AND outcome_1.ranking_version = ?
            LEFT JOIN vw_selection_outcome_latest AS outcome_v2_5
              ON feature.symbol = outcome_v2_5.symbol
             AND outcome_v2_5.horizon = 5
             AND outcome_v2_5.ranking_version = ?
            LEFT JOIN vw_selection_outcome_latest AS outcome_5
              ON feature.symbol = outcome_5.symbol
             AND outcome_5.horizon = 5
             AND outcome_5.ranking_version = ?
            WHERE feature.symbol = ?
            """,
            [
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_VERSION,
                ALPHA_PREDICTION_VERSION,
                PREDICTION_VERSION,
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_VERSION,
                symbol,
            ],
        ).fetchdf()


def _latest_workbench_as_of_date(connection) -> date | None:
    row = connection.execute(
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = ?
        """,
        [SELECTION_ENGINE_V2_VERSION],
    ).fetchone()
    if row is not None and row[0] is not None:
        return pd.Timestamp(row[0]).date()
    row = connection.execute("SELECT MAX(as_of_date) FROM fact_feature_snapshot").fetchone()
    if row is None or row[0] is None:
        return None
    return pd.Timestamp(row[0]).date()


def _build_workbench_live_feature_row(
    connection,
    *,
    as_of_date: date,
    symbol: str,
) -> pd.DataFrame:
    symbol_frame = _load_feature_symbol_frame(
        connection,
        as_of_date=as_of_date,
        symbols=[symbol],
        limit_symbols=None,
        market="ALL",
    )
    if symbol_frame.empty:
        return pd.DataFrame()

    _register_symbol_stage(connection, symbol_frame)
    try:
        ohlcv_history = _load_ohlcv_history(connection, as_of_date=as_of_date)
        latest_fundamentals = _load_latest_fundamentals(connection, as_of_date=as_of_date)
        investor_flow_history = _load_investor_flow_history(connection, as_of_date=as_of_date)
        recent_news = _load_recent_news(connection, as_of_date=as_of_date)
    finally:
        _unregister_symbol_stage(connection)

    latest_price_dates = (
        ohlcv_history.groupby("symbol", as_index=False)["trading_date"].max()
        if not ohlcv_history.empty
        else pd.DataFrame(columns=["symbol", "trading_date"])
    ).rename(columns={"trading_date": "latest_price_date"})
    latest_close = (
        ohlcv_history.loc[
            pd.to_datetime(ohlcv_history["trading_date"]).dt.date == as_of_date,
            ["symbol", "close", "market_cap"],
        ]
        if not ohlcv_history.empty
        else pd.DataFrame(columns=["symbol", "close", "market_cap"])
    )

    feature_matrix = (
        symbol_frame[["symbol", "company_name", "market"]]
        .merge(latest_price_dates, on="symbol", how="left")
        .merge(latest_close, on="symbol", how="left")
        .merge(build_price_feature_frame(ohlcv_history, as_of_date=as_of_date), on="symbol", how="left")
        .merge(build_liquidity_feature_frame(ohlcv_history, as_of_date=as_of_date), on="symbol", how="left")
        .merge(
            build_fundamentals_feature_frame(latest_fundamentals, as_of_date=as_of_date),
            on="symbol",
            how="left",
        )
        .merge(
            build_flow_feature_frame(
                investor_flow_history,
                ohlcv_history=ohlcv_history,
                as_of_date=as_of_date,
            ),
            on="symbol",
            how="left",
        )
        .merge(build_news_feature_frame(recent_news, as_of_date=as_of_date), on="symbol", how="left")
    )

    feature_matrix["earnings_yield_proxy"] = feature_matrix["net_income_latest"] / feature_matrix[
        "market_cap"
    ].replace(0, pd.NA)
    feature_matrix["value_proxy_available_flag"] = (
        feature_matrix[
            [
                "earnings_yield_proxy",
                "low_debt_preference_proxy",
                "profitability_support_proxy",
            ]
        ]
        .notna()
        .any(axis=1)
        .astype(float)
    )
    feature_matrix["liquidity_rank_pct"] = 1.0
    quality_features = build_data_quality_feature_frame(feature_matrix, as_of_date=as_of_date)
    feature_matrix = feature_matrix.merge(
        quality_features,
        on="symbol",
        how="left",
        suffixes=("", "_dup"),
    )
    feature_matrix = feature_matrix.drop(
        columns=[column for column in feature_matrix.columns if column.endswith("_dup")]
    )
    feature_matrix.insert(0, "as_of_date", as_of_date)
    return feature_matrix


def _workbench_latest_reference_price(
    connection,
    *,
    symbol: str,
    as_of_date: date,
) -> tuple[date | None, float | None]:
    row = connection.execute(
        """
        SELECT trading_date, close
        FROM fact_daily_ohlcv
        WHERE symbol = ?
          AND trading_date <= ?
        ORDER BY trading_date DESC
        LIMIT 1
        """,
        [symbol, as_of_date],
    ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        return None, None
    return pd.Timestamp(row[0]).date(), float(row[1])


def _load_live_prediction_row(
    settings: Settings,
    connection,
    *,
    feature_row: pd.DataFrame,
    as_of_date: date,
    horizon: int,
) -> pd.DataFrame:
    training_run, active_alpha_model, training_run_source = _resolve_training_run_for_inference(
        connection,
        as_of_date=as_of_date,
        horizon=int(horizon),
    )
    if training_run is None or not training_run.get("artifact_uri"):
        return pd.DataFrame()
    resolved_artifact_path = resolve_ui_artifact_path(settings, training_run.get("artifact_uri"))
    if resolved_artifact_path is None:
        return pd.DataFrame()
    resolved_training_run = dict(training_run)
    resolved_training_run["artifact_uri"] = str(resolved_artifact_path)
    result_frame, _ = build_prediction_frame_from_training_run(
        run_id="stock-workbench-live",
        as_of_date=as_of_date,
        horizon=int(horizon),
        feature_frame=feature_row,
        training_run=resolved_training_run,
        training_run_source=training_run_source,
        active_alpha_model_id=(
            active_alpha_model.get("active_alpha_model_id")
            if active_alpha_model is not None
            else None
        ),
        persist_member_predictions=False,
    )
    return result_frame


def stock_workbench_live_recommendation_frame(
    settings: Settings,
    *,
    symbol: str,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    normalized_symbol = str(symbol).zfill(6)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        as_of_date = _latest_workbench_as_of_date(connection)
        if as_of_date is None:
            return pd.DataFrame()

        feature_context = load_feature_matrix(connection, as_of_date=as_of_date, market="ALL")
        if feature_context.empty:
            return pd.DataFrame()

        live_feature_row = _build_workbench_live_feature_row(
            connection,
            as_of_date=as_of_date,
            symbol=normalized_symbol,
        )
        if live_feature_row.empty:
            return pd.DataFrame()

        feature_context = feature_context.loc[
            feature_context["symbol"].astype(str).ne(normalized_symbol)
        ].copy()
        feature_matrix = pd.concat([feature_context, live_feature_row], ignore_index=True)

        prediction_frames_by_horizon: dict[int, pd.DataFrame] = {}
        live_prediction_rows: dict[int, pd.DataFrame] = {}
        for horizon in (1, 5):
            stored_prediction_frame = connection.execute(
                """
                SELECT
                    symbol,
                    expected_excess_return,
                    lower_band,
                    median_band,
                    upper_band,
                    uncertainty_score,
                    disagreement_score,
                    fallback_flag,
                    fallback_reason,
                    prediction_version,
                    member_count,
                    ensemble_weight_json,
                    source_notes_json
                FROM fact_prediction
                WHERE as_of_date = ?
                  AND horizon = ?
                  AND prediction_version = ?
                  AND ranking_version = ?
                """,
                [as_of_date, horizon, ALPHA_PREDICTION_VERSION, SELECTION_ENGINE_V2_VERSION],
            ).fetchdf()
            live_prediction_row = _load_live_prediction_row(
                settings,
                connection,
                feature_row=live_feature_row,
                as_of_date=as_of_date,
                horizon=horizon,
            )
            live_prediction_rows[horizon] = live_prediction_row
            if not live_prediction_row.empty:
                stored_prediction_frame = stored_prediction_frame.loc[
                    stored_prediction_frame["symbol"].astype(str).ne(normalized_symbol)
                ].copy()
                stored_prediction_frame = pd.concat(
                    [
                        stored_prediction_frame,
                        live_prediction_row[
                            [
                                "symbol",
                                "expected_excess_return",
                                "lower_band",
                                "median_band",
                                "upper_band",
                                "uncertainty_score",
                                "disagreement_score",
                                "fallback_flag",
                                "fallback_reason",
                                "prediction_version",
                                "member_count",
                                "ensemble_weight_json",
                                "source_notes_json",
                            ]
                        ],
                    ],
                    ignore_index=True,
                )
            prediction_frames_by_horizon[horizon] = stored_prediction_frame

        ranking_frames = build_selection_engine_v2_rankings(
            feature_matrix=feature_matrix,
            as_of_date=as_of_date,
            horizons=[1, 5],
            regime_map=_load_regime_map(connection, as_of_date=as_of_date),
            prediction_frames_by_horizon=prediction_frames_by_horizon,
            run_id="stock-workbench-live",
            settings=settings,
        )

        ranking_by_horizon: dict[int, pd.Series] = {}
        for frame in ranking_frames:
            symbol_row = frame.loc[frame["symbol"].astype(str) == normalized_symbol]
            if symbol_row.empty:
                continue
            ranking_by_horizon[int(symbol_row["horizon"].iloc[0])] = symbol_row.iloc[0]
        if not ranking_by_horizon:
            return pd.DataFrame()

        reference_date, reference_price = _workbench_latest_reference_price(
            connection,
            symbol=normalized_symbol,
            as_of_date=as_of_date,
        )
        latest_target_row = connection.execute(
            """
            SELECT as_of_date, entry_trade_date, execution_mode, included_flag, target_weight, gate_status
            FROM fact_portfolio_target_book
            WHERE symbol = ?
              AND as_of_date = (SELECT MAX(as_of_date) FROM fact_portfolio_target_book)
            ORDER BY included_flag DESC, target_weight DESC NULLS LAST, execution_mode
            LIMIT 1
            """,
            [normalized_symbol],
        ).fetchdf()
        latest_target = latest_target_row.iloc[0].to_dict() if not latest_target_row.empty else {}

        d5_prediction_row = None
        if 5 in live_prediction_rows and not live_prediction_rows[5].empty:
            d5_prediction_row = live_prediction_rows[5].iloc[0]
        expected = (
            None
            if d5_prediction_row is None or pd.isna(d5_prediction_row.get("expected_excess_return"))
            else float(d5_prediction_row["expected_excess_return"])
        )
        upper = (
            None
            if d5_prediction_row is None or pd.isna(d5_prediction_row.get("upper_band"))
            else float(d5_prediction_row["upper_band"])
        )
        lower = (
            None
            if d5_prediction_row is None or pd.isna(d5_prediction_row.get("lower_band"))
            else float(d5_prediction_row["lower_band"])
        )

        return pd.DataFrame(
            [
                {
                    "symbol": normalized_symbol,
                    "company_name": live_feature_row.iloc[0].get("company_name"),
                    "market": live_feature_row.iloc[0].get("market"),
                    "live_as_of_date": as_of_date,
                    "live_reference_date": reference_date,
                    "live_reference_price": reference_price,
                    "live_d1_selection_v2_value": ranking_by_horizon.get(1, {}).get("final_selection_value"),
                    "live_d1_selection_v2_grade": ranking_by_horizon.get(1, {}).get("grade"),
                    "live_d1_eligible_flag": ranking_by_horizon.get(1, {}).get("eligible_flag"),
                    "live_d1_report_candidate_flag": ranking_by_horizon.get(1, {}).get("report_candidate_flag"),
                    "live_d5_selection_v2_value": ranking_by_horizon.get(5, {}).get("final_selection_value"),
                    "live_d5_selection_v2_grade": ranking_by_horizon.get(5, {}).get("grade"),
                    "live_d5_eligible_flag": ranking_by_horizon.get(5, {}).get("eligible_flag"),
                    "live_d5_report_candidate_flag": ranking_by_horizon.get(5, {}).get("report_candidate_flag"),
                    "live_d5_expected_excess_return": expected,
                    "live_d5_target_price": None if reference_price is None or expected is None else reference_price * (1.0 + expected),
                    "live_d5_upper_target_price": None if reference_price is None or upper is None else reference_price * (1.0 + upper),
                    "live_d5_stop_price": None if reference_price is None or lower is None else reference_price * (1.0 + lower),
                    "latest_portfolio_as_of_date": latest_target.get("as_of_date"),
                    "latest_portfolio_entry_trade_date": latest_target.get("entry_trade_date"),
                    "latest_portfolio_execution_mode": latest_target.get("execution_mode"),
                    "latest_portfolio_included_flag": latest_target.get("included_flag"),
                    "latest_portfolio_target_weight": latest_target.get("target_weight"),
                    "latest_portfolio_gate_status": latest_target.get("gate_status"),
                }
            ]
        )


def stock_workbench_price_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT trading_date, open, high, low, close, volume, turnover_value
            FROM fact_daily_ohlcv
            WHERE symbol = ?
            ORDER BY trading_date DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def stock_workbench_flow_frame(settings: Settings, *, symbol: str, limit: int = 30) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                trading_date,
                foreign_net_value,
                institution_net_value,
                individual_net_value,
                foreign_net_volume,
                institution_net_volume,
                individual_net_volume
            FROM fact_investor_flow
            WHERE symbol = ?
            ORDER BY trading_date DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def stock_workbench_news_frame(settings: Settings, *, symbol: str, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT signal_date, published_at, title, publisher, query_bucket, link
            FROM fact_news_item
            WHERE symbol_candidates LIKE ?
            ORDER BY signal_date DESC, published_at DESC
            LIMIT ?
            """,
            [f"%{symbol}%", limit],
        ).fetchdf()


def stock_workbench_outcome_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                selection_date,
                evaluation_date,
                horizon,
                ranking_version,
                final_selection_value,
                expected_excess_return_at_selection,
                realized_excess_return,
                band_status,
                outcome_status
            FROM fact_selection_outcome
            WHERE symbol = ?
            ORDER BY selection_date DESC, ranking_version, horizon
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def latest_discord_preview(settings: Settings) -> str | None:
    return _latest_manifest_preview(settings, run_type="render_discord_eod_report")


def latest_postmortem_preview(settings: Settings) -> str | None:
    return _latest_manifest_preview(settings, run_type="render_postmortem_report")


UI_COLUMN_LABELS.update(
    {
        "session_date": "세션 날짜",
        "selection_date": "선정 날짜",
        "candidate_count": "후보 수",
        "candidate_symbols": "후보 종목 수",
        "bar_symbols": "1분봉 종목 수",
        "trade_symbols": "체결 요약 종목 수",
        "quote_symbols": "호가 요약 종목 수",
        "signal_symbols": "신호 종목 수",
        "decision_symbols": "판단 종목 수",
        "avg_bar_latency_ms": "평균 1분봉 지연(ms)",
        "avg_quote_latency_ms": "평균 호가 지연(ms)",
        "checkpoint_time": "체크포인트",
        "avg_signal_quality": "평균 신호 품질",
        "enter_now_count": "즉시 진입 수",
        "wait_recheck_count": "재확인 수",
        "avoid_today_count": "오늘 회피 수",
        "data_insufficient_count": "데이터 부족 수",
        "quote_unavailable_count": "호가 미가용 수",
        "trade_unavailable_count": "체결 미가용 수",
        "candidate_rank": "후보 순위",
        "session_status": "세션 상태",
        "timing_adjustment_score": "타이밍 조정 점수",
        "signal_quality_score": "신호 품질 점수",
        "gap_opening_quality_score": "갭/시가 품질",
        "micro_trend_score": "미세 추세",
        "relative_activity_score": "상대 활동성",
        "orderbook_score": "호가 점수",
        "execution_strength_score": "체결 강도 점수",
        "risk_friction_score": "마찰/충격 리스크",
        "action": "액션",
        "action_score": "액션 점수",
        "entry_reference_price": "판단 기준 가격",
        "selected_checkpoint_time": "선택 체크포인트",
        "selected_action": "선택 액션",
        "execution_flag": "진입 실행 여부",
        "naive_open_price": "시가 기준 가격",
        "decision_entry_price": "판단 진입 가격",
        "future_exit_price": "미래 청산 가격",
        "realized_return_from_open": "시가 기준 수익률",
        "realized_return_from_decision": "판단 기준 수익률",
        "timing_edge_return": "타이밍 엣지 수익률",
        "timing_edge_bps": "타이밍 엣지(bps)",
        "quote_status": "호가 상태",
        "trade_summary_status": "체결 상태",
    }
)


def _latest_intraday_session_date(settings: Settings):
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            "SELECT MAX(session_date) FROM fact_intraday_candidate_session"
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return pd.Timestamp(row[0]).date()


def latest_intraday_status_frame(settings: Settings) -> pd.DataFrame:
    session_date = _latest_intraday_session_date(settings)
    if session_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                candidate.session_date,
                COUNT(DISTINCT candidate.symbol) AS candidate_symbols,
                COUNT(DISTINCT bar.symbol) AS bar_symbols,
                COUNT(DISTINCT trade.symbol) AS trade_symbols,
                COUNT(DISTINCT quote.symbol) AS quote_symbols,
                COUNT(DISTINCT signal.symbol) AS signal_symbols,
                COUNT(DISTINCT decision.symbol) AS raw_decision_symbols,
                COUNT(DISTINCT adjusted.symbol) AS adjusted_symbols,
                COUNT(DISTINCT meta_prediction.symbol) AS meta_prediction_symbols,
                COUNT(DISTINCT meta_decision.symbol) AS meta_decision_symbols,
                COUNT(DISTINCT final_action.symbol) AS final_action_symbols,
                AVG(bar.fetch_latency_ms) AS avg_bar_latency_ms,
                AVG(quote.fetch_latency_ms) AS avg_quote_latency_ms
            FROM fact_intraday_candidate_session AS candidate
            LEFT JOIN fact_intraday_bar_1m AS bar
              ON candidate.session_date = bar.session_date
             AND candidate.symbol = bar.symbol
            LEFT JOIN fact_intraday_trade_summary AS trade
              ON candidate.session_date = trade.session_date
             AND candidate.symbol = trade.symbol
            LEFT JOIN fact_intraday_quote_summary AS quote
              ON candidate.session_date = quote.session_date
             AND candidate.symbol = quote.symbol
            LEFT JOIN fact_intraday_signal_snapshot AS signal
              ON candidate.session_date = signal.session_date
             AND candidate.symbol = signal.symbol
            LEFT JOIN fact_intraday_entry_decision AS decision
              ON candidate.session_date = decision.session_date
             AND candidate.symbol = decision.symbol
            LEFT JOIN fact_intraday_adjusted_entry_decision AS adjusted
              ON candidate.session_date = adjusted.session_date
             AND candidate.symbol = adjusted.symbol
            LEFT JOIN fact_intraday_meta_prediction AS meta_prediction
              ON candidate.session_date = meta_prediction.session_date
             AND candidate.symbol = meta_prediction.symbol
            LEFT JOIN fact_intraday_meta_decision AS meta_decision
              ON candidate.session_date = meta_decision.session_date
             AND candidate.symbol = meta_decision.symbol
            LEFT JOIN fact_intraday_final_action AS final_action
              ON candidate.session_date = final_action.session_date
             AND candidate.symbol = final_action.symbol
            WHERE candidate.session_date = ?
            GROUP BY candidate.session_date
            """,
            [session_date],
        ).fetchdf()


def latest_intraday_research_capability_frame(
    settings: Settings,
    *,
    as_of_date=None,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if as_of_date is None:
            return connection.execute(
                """
                SELECT
                    as_of_date,
                    feature_slug,
                    enabled_flag,
                    rollout_mode,
                    dependency_ready_flag,
                    blocking_dependency,
                    report_available_flag,
                    latest_report_type,
                    last_successful_run_id,
                    last_degraded_run_id,
                    last_skip_reason
                FROM vw_latest_intraday_research_capability
                ORDER BY feature_slug
                LIMIT ?
                """,
                [limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                as_of_date,
                feature_slug,
                enabled_flag,
                rollout_mode,
                dependency_ready_flag,
                blocking_dependency,
                report_available_flag,
                latest_report_type,
                last_successful_run_id,
                last_degraded_run_id,
                last_skip_reason
            FROM fact_intraday_research_capability
            WHERE as_of_date = ?
            ORDER BY feature_slug
            LIMIT ?
            """,
            [as_of_date, limit],
        ).fetchdf()


def latest_intraday_decision_lineage_frame(
    settings: Settings,
    *,
    session_date=None,
    symbol: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    target_date = (
        session_date
        or _latest_intraday_meta_session_date(settings)
        or _latest_intraday_session_date(settings)
    )
    if target_date is None:
        return pd.DataFrame()
    clauses = ["session_date = ?"]
    params: list[object] = [target_date]
    if symbol:
        clauses.append("symbol = ?")
        params.append(symbol.zfill(6))
    params.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            f"""
            SELECT
                session_date,
                selection_date,
                checkpoint_time,
                symbol,
                company_name,
                horizon,
                market,
                ranking_version,
                raw_action,
                adjusted_action,
                final_action,
                predicted_class,
                predicted_class_probability,
                confidence_margin,
                uncertainty_score,
                disagreement_score,
                candidate_session_run_id,
                ranking_run_id,
                raw_decision_run_id,
                adjusted_decision_run_id,
                meta_decision_run_id,
                prediction_run_id,
                portfolio_target_run_id,
                portfolio_execution_mode,
                gate_status,
                target_weight,
                target_notional,
                target_shares,
                market_regime_state,
                final_selection_value,
                expected_excess_return
            FROM vw_intraday_decision_lineage
            WHERE {" AND ".join(clauses)}
            ORDER BY horizon, symbol, checkpoint_time
            LIMIT ?
            """,
            params,
        ).fetchdf()


UI_COLUMN_LABELS.update(
    {
        "feature_slug": "장중 기능",
        "rollout_mode": "운영 모드",
        "blocking_dependency": "차단 의존성",
        "dependency_ready_flag": "의존성 준비",
        "report_available_flag": "보고서 존재",
        "latest_report_type": "최근 생성된 보고서",
        "last_successful_run_id": "최근 성공 실행 ID",
        "last_degraded_run_id": "최근 경고 실행 ID",
        "last_skip_reason": "최근 건너뛴 사유",
        "candidate_session_run_id": "후보군 실행 ID",
        "ranking_run_id": "리더보드 실행 ID",
        "raw_decision_run_id": "원정책 실행 ID",
        "adjusted_decision_run_id": "조정정책 실행 ID",
        "meta_decision_run_id": "메타 실행 ID",
        "prediction_run_id": "예측 실행 ID",
        "portfolio_target_run_id": "포트폴리오 실행 ID",
        "portfolio_execution_mode": "포트폴리오 실행 모드",
        "gate_status": "진입 판단 상태",
        "target_weight": "목표 비중",
        "target_notional": "목표 금액",
        "target_shares": "목표 수량",
        "selection_date": "선정 기준일",
        "market_regime_state": "시장 흐름",
        "raw_decision_symbols": "원정책 종목 수",
        "meta_prediction_symbols": "메타 예측 종목 수",
        "meta_decision_symbols": "메타 판단 종목 수",
        "final_action_symbols": "최종 판단 종목 수",
    }
)
UI_VALUE_LABELS.setdefault("feature_slug", {}).update(
    {
        "intraday_assist": "장중 후보군 보조",
        "intraday_policy_adjustment": "장중 정책 조정",
        "intraday_meta_model": "장중 메타 모델",
        "intraday_postmortem": "장중 사후 분석",
        "intraday_research_reports": "장중 연구 리포트",
        "intraday_discord_summary": "장중 디스코드 요약",
        "intraday_writeback": "장중 판단 저장",
    }
)
UI_VALUE_LABELS.setdefault("rollout_mode", {}).update(
    {
        "RESEARCH_NON_TRADING": "리서치 전용 / 비매매",
    }
)
UI_VALUE_LABELS.setdefault("market_regime_state", {}).update(UI_VALUE_LABELS["regime_state"])
UI_VALUE_LABELS.setdefault("prior_daily_regime_state", {}).update(UI_VALUE_LABELS["regime_state"])
UI_VALUE_LABELS.setdefault("latest_daily_bundle_status", {}).update(UI_VALUE_LABELS["status"])
UI_VALUE_LABELS.setdefault("latest_report_type", {}).update(
    {
        "intraday_summary_report": "장중 요약 리포트",
        "intraday_postmortem_report": "장중 사후 분석 리포트",
        "intraday_policy_research_report": "장중 정책 연구 리포트",
        "intraday_meta_model_report": "장중 메타 모델 리포트",
    }
)


def latest_intraday_checkpoint_health_frame(settings: Settings) -> pd.DataFrame:
    session_date = _latest_intraday_session_date(settings)
    if session_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                signal.checkpoint_time,
                AVG(signal.signal_quality_score) AS avg_signal_quality,
                SUM(CASE WHEN decision.action = 'ENTER_NOW' THEN 1 ELSE 0 END) AS enter_now_count,
                SUM(
                    CASE WHEN decision.action = 'WAIT_RECHECK' THEN 1 ELSE 0 END
                ) AS wait_recheck_count,
                SUM(
                    CASE WHEN decision.action = 'AVOID_TODAY' THEN 1 ELSE 0 END
                ) AS avoid_today_count,
                SUM(
                    CASE WHEN decision.action = 'DATA_INSUFFICIENT' THEN 1 ELSE 0 END
                ) AS data_insufficient_count,
                SUM(
                    CASE WHEN adjusted.adjusted_action = 'ENTER_NOW' THEN 1 ELSE 0 END
                ) AS adjusted_enter_now_count,
                SUM(
                    CASE WHEN adjusted.adjusted_action = 'WAIT_RECHECK' THEN 1 ELSE 0 END
                ) AS adjusted_wait_recheck_count,
                SUM(
                    CASE WHEN adjusted.adjusted_action = 'AVOID_TODAY' THEN 1 ELSE 0 END
                ) AS adjusted_avoid_today_count,
                SUM(
                    CASE WHEN adjusted.adjusted_action = 'DATA_INSUFFICIENT' THEN 1 ELSE 0 END
                ) AS adjusted_data_insufficient_count,
                SUM(
                    CASE WHEN quote.quote_status = 'unavailable' THEN 1 ELSE 0 END
                ) AS quote_unavailable_count,
                SUM(
                    CASE WHEN trade.trade_summary_status = 'unavailable' THEN 1 ELSE 0 END
                ) AS trade_unavailable_count
            FROM fact_intraday_signal_snapshot AS signal
            LEFT JOIN fact_intraday_entry_decision AS decision
              ON signal.session_date = decision.session_date
             AND signal.symbol = decision.symbol
             AND signal.horizon = decision.horizon
             AND signal.checkpoint_time = decision.checkpoint_time
             AND signal.ranking_version = decision.ranking_version
            LEFT JOIN fact_intraday_quote_summary AS quote
              ON signal.session_date = quote.session_date
             AND signal.symbol = quote.symbol
             AND signal.checkpoint_time = quote.checkpoint_time
            LEFT JOIN fact_intraday_trade_summary AS trade
              ON signal.session_date = trade.session_date
             AND signal.symbol = trade.symbol
             AND signal.checkpoint_time = trade.checkpoint_time
            LEFT JOIN fact_intraday_adjusted_entry_decision AS adjusted
              ON signal.session_date = adjusted.session_date
             AND signal.symbol = adjusted.symbol
             AND signal.horizon = adjusted.horizon
             AND signal.checkpoint_time = adjusted.checkpoint_time
             AND signal.ranking_version = adjusted.ranking_version
            WHERE signal.session_date = ?
            GROUP BY signal.checkpoint_time
            ORDER BY signal.checkpoint_time
            """,
            [session_date],
        ).fetchdf()


def intraday_console_candidate_frame(
    settings: Settings,
    *,
    session_date=None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                selection_date,
                symbol,
                company_name,
                market,
                horizon,
                candidate_rank,
                final_selection_value,
                grade,
                expected_excess_return,
                session_status
            FROM fact_intraday_candidate_session
            WHERE session_date = ?
            ORDER BY horizon, candidate_rank, symbol
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def intraday_console_signal_frame(
    settings: Settings,
    *,
    session_date=None,
    checkpoint: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if checkpoint is None:
            row = connection.execute(
                """
                SELECT MAX(checkpoint_time)
                FROM fact_intraday_signal_snapshot
                WHERE session_date = ?
                """,
                [target_date],
            ).fetchone()
            checkpoint = row[0] if row and row[0] else None
        if checkpoint is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT
                session_date,
                checkpoint_time,
                symbol,
                horizon,
                gap_opening_quality_score,
                micro_trend_score,
                relative_activity_score,
                orderbook_score,
                execution_strength_score,
                risk_friction_score,
                signal_quality_score,
                timing_adjustment_score
            FROM fact_intraday_signal_snapshot
            WHERE session_date = ?
              AND checkpoint_time = ?
            ORDER BY horizon, timing_adjustment_score DESC, symbol
            LIMIT ?
            """,
            [target_date, checkpoint, limit],
        ).fetchdf()


def intraday_console_decision_frame(
    settings: Settings,
    *,
    session_date=None,
    checkpoint: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if checkpoint is None:
            row = connection.execute(
                """
                SELECT MAX(checkpoint_time)
                FROM fact_intraday_entry_decision
                WHERE session_date = ?
                """,
                [target_date],
            ).fetchone()
            checkpoint = row[0] if row and row[0] else None
        if checkpoint is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT
                decision.session_date,
                decision.checkpoint_time,
                decision.symbol,
                candidate.company_name,
                decision.horizon,
                decision.action,
                decision.action_score,
                decision.signal_quality_score,
                decision.entry_reference_price
            FROM fact_intraday_entry_decision AS decision
            LEFT JOIN fact_intraday_candidate_session AS candidate
              ON decision.session_date = candidate.session_date
             AND decision.symbol = candidate.symbol
             AND decision.horizon = candidate.horizon
             AND decision.ranking_version = candidate.ranking_version
            WHERE decision.session_date = ?
              AND decision.checkpoint_time = ?
            ORDER BY decision.horizon, decision.action_score DESC, decision.symbol
            LIMIT ?
            """,
            [target_date, checkpoint, limit],
        ).fetchdf()


def intraday_console_timing_frame(settings: Settings, *, limit: int = 30) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                symbol,
                horizon,
                selected_checkpoint_time,
                selected_action,
                timing_edge_bps,
                realized_return_from_open,
                realized_return_from_decision,
                outcome_status
            FROM fact_intraday_timing_outcome
            ORDER BY session_date DESC, horizon, symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_market_context_frame(
    settings: Settings,
    *,
    session_date=None,
    limit: int = 20,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                checkpoint_time,
                context_scope,
                market_session_state,
                prior_daily_regime_state,
                market_breadth_ratio,
                candidate_mean_return_from_open,
                candidate_mean_relative_volume,
                candidate_mean_signal_quality,
                bar_coverage_ratio,
                trade_coverage_ratio,
                quote_coverage_ratio,
                data_quality_flag
            FROM fact_intraday_market_context_snapshot
            WHERE session_date = ?
            ORDER BY checkpoint_time, context_scope
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def latest_market_mood_summary(settings: Settings) -> dict[str, str]:
    today = today_local(settings.app.timezone)
    intraday = latest_intraday_market_context_frame(settings, session_date=today, limit=50)
    if not intraday.empty:
        market_only = intraday.loc[intraday["context_scope"].astype(str) == "market"].copy()
        if not market_only.empty:
            latest = market_only.sort_values("checkpoint_time").iloc[-1]
            bar_coverage = _coerce_float(latest.get("bar_coverage_ratio"))
            trade_coverage = _coerce_float(latest.get("trade_coverage_ratio"))
            quote_coverage = _coerce_float(latest.get("quote_coverage_ratio"))
            min_coverage = min(bar_coverage, trade_coverage, quote_coverage)
            market_state = str(latest.get("market_session_state") or "")
            checkpoint = format_ui_time(latest.get("checkpoint_time"))
            breadth = _coerce_float(latest.get("market_breadth_ratio"))
            candidate_return = _coerce_float(latest.get("candidate_mean_return_from_open"))
            data_quality = str(latest.get("data_quality_flag") or "")
            prior_regime = format_ui_value(
                "prior_daily_regime_state",
                latest.get("prior_daily_regime_state") or "unknown",
            )

            if market_state != "planned" and data_quality != "weak" and min_coverage >= 0.35:
                if breadth <= 0.40 or candidate_return <= -0.005:
                    headline = "장중 약세"
                elif breadth >= 0.60 and candidate_return >= 0.003:
                    headline = "장중 강세"
                else:
                    headline = "장중 혼조"
                return {
                    "mode": "intraday",
                    "headline": headline,
                    "label": f"{format_ui_date(today)} {checkpoint} 기준",
                    "detail": (
                        f"체크포인트 {checkpoint} 기준입니다. "
                        f"상승 비율 {breadth:.0%}, 후보 평균 시가 대비 수익률 {candidate_return:+.2%}입니다."
                    ),
                }

            if market_state == "planned":
                return {
                    "mode": "preopen",
                    "headline": "장 시작 전",
                    "label": f"{format_ui_date(today)} 장전 기준",
                    "detail": (
                        f"아직 장중 데이터가 쌓이기 전이라 전일 종가 기준 흐름을 함께 봐야 합니다. "
                        f"직전 장 국면은 {prior_regime}입니다."
                    ),
                }

            return {
                "mode": "intraday_stale",
                "headline": "장중 데이터 보강 중",
                "label": f"{format_ui_date(today)} {checkpoint} 기준",
                "detail": (
                    "오늘 장중 컨텍스트는 잡혔지만 커버리지가 낮아 분위기 판단을 보류합니다. "
                    f"직전 장 국면은 {prior_regime}입니다."
                ),
            }

    regime = latest_regime_frame(settings)
    if regime.empty:
        return {
            "mode": "missing",
            "headline": "시장 분위기 데이터 없음",
            "label": "-",
            "detail": "장중 컨텍스트와 일간 시장 국면 데이터가 아직 없습니다.",
        }

    regime_frame = regime.copy()
    if "market_scope" in regime_frame.columns and regime_frame["market_scope"].astype(str).eq("KR_ALL").any():
        row = regime_frame.loc[regime_frame["market_scope"].astype(str) == "KR_ALL"].iloc[0]
    else:
        row = regime_frame.iloc[0]
    as_of_date = row.get("as_of_date")
    headline = format_ui_value("regime_state", row.get("regime_state"))
    return {
        "mode": "daily",
        "headline": headline,
        "label": f"{format_ui_date(as_of_date)} 종가 기준",
        "detail": "오늘 장중 컨텍스트가 아직 없어 마지막 일간 시장 국면을 보여줍니다.",
    }


def latest_intraday_console_basis_summary(settings: Settings) -> dict[str, str]:
    session_date = _latest_intraday_session_date(settings)
    if session_date is None:
        return {
            "mode": "missing",
            "headline": "장중 세션 없음",
            "label": "-",
            "detail": "저장된 장중 보조 세션이 아직 없습니다.",
        }

    context = latest_intraday_market_context_frame(settings, session_date=session_date, limit=50)
    checkpoint = "-"
    market_state = ""
    prior_regime = "미상"
    data_quality = "weak"
    min_coverage = 0.0

    if not context.empty:
        market_only = context.loc[context["context_scope"].astype(str) == "market"].copy()
        if not market_only.empty:
            latest = market_only.sort_values("checkpoint_time").iloc[-1]
            checkpoint = format_ui_time(latest.get("checkpoint_time"))
            market_state = str(latest.get("market_session_state") or "")
            prior_regime = format_ui_value("prior_daily_regime_state", latest.get("prior_daily_regime_state"))
            data_quality = str(latest.get("data_quality_flag") or "weak")
            min_coverage = min(
                _coerce_float(latest.get("bar_coverage_ratio")),
                _coerce_float(latest.get("trade_coverage_ratio")),
                _coerce_float(latest.get("quote_coverage_ratio")),
            )

    session_label = format_ui_date(session_date)
    if session_date != today_local(settings.app.timezone):
        return {
            "mode": "historical",
            "headline": "마지막 저장 세션",
            "label": f"{session_label} {checkpoint} 기준",
            "detail": (
                f"오늘 장중이 아니라 마지막으로 저장된 장중 세션입니다. "
                f"직전 일간 국면은 {prior_regime}였습니다."
            ),
        }

    if market_state == "planned":
        return {
            "mode": "preopen",
            "headline": "장 시작 전",
            "label": f"{session_label} 장전 기준",
            "detail": f"아직 오늘 장중 판단 전입니다. 직전 일간 국면은 {prior_regime}입니다.",
        }

    if data_quality == "weak" or min_coverage < 0.35:
        return {
            "mode": "stale",
            "headline": "장중 데이터 보강 중",
            "label": f"{session_label} {checkpoint} 기준",
            "detail": (
                "현재 세션은 열려 있지만 장중 커버리지가 낮아 판단을 보류합니다. "
                f"직전 일간 국면은 {prior_regime}입니다."
            ),
        }

    return {
        "mode": "live",
        "headline": "오늘 장중 세션",
        "label": f"{session_label} {checkpoint} 기준",
        "detail": "장중 체크포인트와 커버리지가 확보된 세션입니다.",
    }


def latest_intraday_adjustment_frame(
    settings: Settings,
    *,
    session_date=None,
    checkpoint: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if checkpoint is None:
            row = connection.execute(
                """
                SELECT MAX(checkpoint_time)
                FROM fact_intraday_adjusted_entry_decision
                WHERE session_date = ?
                """,
                [target_date],
            ).fetchone()
            checkpoint = row[0] if row and row[0] else None
        if checkpoint is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT
                adjusted.session_date,
                adjusted.checkpoint_time,
                adjusted.symbol,
                candidate.company_name,
                adjusted.horizon,
                adjusted.market_regime_family,
                adjusted.adjustment_profile,
                adjusted.raw_action,
                adjusted.adjusted_action,
                adjusted.raw_timing_score,
                adjusted.adjusted_timing_score,
                adjusted.selection_confidence_bucket,
                adjusted.signal_quality_flag,
                adjusted.eligible_to_execute_flag,
                adjusted.fallback_flag
            FROM fact_intraday_adjusted_entry_decision AS adjusted
            LEFT JOIN fact_intraday_candidate_session AS candidate
              ON adjusted.session_date = candidate.session_date
             AND adjusted.symbol = candidate.symbol
             AND adjusted.horizon = candidate.horizon
             AND adjusted.ranking_version = candidate.ranking_version
            WHERE adjusted.session_date = ?
              AND adjusted.checkpoint_time = ?
            ORDER BY adjusted.horizon, adjusted.adjusted_timing_score DESC, adjusted.symbol
            LIMIT ?
            """,
            [target_date, checkpoint, limit],
        ).fetchdf()


def latest_intraday_adjustment_summary_frame(
    settings: Settings,
    *,
    session_date=None,
    limit: int = 30,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                checkpoint_time,
                market_regime_family,
                adjustment_profile,
                adjusted_action,
                COUNT(*) AS row_count
            FROM fact_intraday_adjusted_entry_decision
            WHERE session_date = ?
            GROUP BY
                session_date,
                checkpoint_time,
                market_regime_family,
                adjustment_profile,
                adjusted_action
            ORDER BY checkpoint_time, market_regime_family, adjustment_profile, adjusted_action
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def latest_intraday_strategy_comparison_frame(
    settings: Settings,
    *,
    end_session_date=None,
    comparison_scope: str = "all",
    limit: int = 30,
) -> pd.DataFrame:
    target_date = end_session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                end_session_date,
                horizon,
                strategy_id,
                comparison_scope,
                comparison_value,
                cutoff_checkpoint_time,
                sample_count,
                matured_count,
                executed_count,
                no_entry_count,
                execution_rate,
                mean_realized_excess_return,
                median_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                positive_timing_edge_rate,
                skip_saved_loss_rate,
                missed_winner_rate,
                coverage_ok_rate
            FROM fact_intraday_strategy_comparison
            WHERE end_session_date = ?
              AND comparison_scope = ?
            ORDER BY horizon, comparison_value, strategy_id
            LIMIT ?
            """,
            [target_date, comparison_scope, limit],
        ).fetchdf()


def latest_intraday_timing_calibration_frame(
    settings: Settings,
    *,
    window_end_date=None,
    grouping_key: str | None = None,
    limit: int = 30,
) -> pd.DataFrame:
    target_date = window_end_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if grouping_key is None:
            return connection.execute(
                """
                SELECT
                    window_end_date,
                    horizon,
                    grouping_key,
                    grouping_value,
                    sample_count,
                    executed_count,
                    execution_rate,
                    mean_realized_excess_return,
                    hit_rate,
                    mean_timing_edge_vs_open_bps,
                    skip_saved_loss_rate,
                    missed_winner_rate,
                    quality_flag
                FROM fact_intraday_timing_calibration
                WHERE window_end_date = ?
                  AND grouping_key IN ('overall', 'strategy_id', 'regime_family')
                ORDER BY horizon, grouping_key, grouping_value
                LIMIT ?
                """,
                [target_date, limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                window_end_date,
                horizon,
                grouping_key,
                grouping_value,
                sample_count,
                executed_count,
                execution_rate,
                mean_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                skip_saved_loss_rate,
                missed_winner_rate,
                quality_flag
            FROM fact_intraday_timing_calibration
            WHERE window_end_date = ?
              AND grouping_key = ?
            ORDER BY horizon, grouping_value
            LIMIT ?
            """,
            [target_date, grouping_key, limit],
        ).fetchdf()


def latest_intraday_publish_status_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not _metadata_available(settings):
        return pd.DataFrame()
    return _metadata_frame(
        settings,
        """
            WITH ranked_runs AS (
                SELECT
                    run_type,
                    started_at,
                    finished_at,
                    status,
                    notes,
                    ROW_NUMBER() OVER (
                        PARTITION BY run_type
                        ORDER BY started_at DESC
                    ) AS row_number
                FROM ops_run_manifest
                WHERE run_type IN (
                    'materialize_intraday_market_context_snapshots',
                    'materialize_intraday_regime_adjustments',
                    'materialize_intraday_adjusted_entry_decisions',
                    'materialize_intraday_decision_outcomes',
                    'evaluate_intraday_strategy_comparison',
                    'materialize_intraday_timing_calibration',
                    'render_intraday_postmortem_report',
                    'publish_discord_intraday_postmortem',
                    'validate_intraday_strategy_pipeline'
                )
            )
            SELECT
                run_type,
                started_at,
                finished_at,
                status,
                notes
            FROM ranked_runs
            WHERE row_number = 1
            ORDER BY started_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_intraday_postmortem_preview(settings: Settings) -> str | None:
    return _latest_manifest_preview(settings, run_type="render_intraday_postmortem_report")


def intraday_console_market_context_frame(
    settings: Settings,
    *,
    session_date=None,
    limit: int = 20,
) -> pd.DataFrame:
    return latest_intraday_market_context_frame(
        settings,
        session_date=session_date,
        limit=limit,
    )


def intraday_console_adjusted_decision_frame(
    settings: Settings,
    *,
    session_date=None,
    checkpoint: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    return latest_intraday_adjustment_frame(
        settings,
        session_date=session_date,
        checkpoint=checkpoint,
        limit=limit,
    )


def intraday_console_strategy_trace_frame(
    settings: Settings,
    *,
    session_date=None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                symbol,
                company_name,
                horizon,
                strategy_id,
                entry_checkpoint_time,
                market_regime_family,
                adjustment_profile,
                executed_flag,
                no_entry_flag,
                realized_excess_return,
                timing_edge_vs_open_bps,
                skip_reason_code,
                outcome_status
            FROM fact_intraday_strategy_result
            WHERE session_date = ?
            ORDER BY horizon, symbol, strategy_id
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def stock_workbench_intraday_decision_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                raw.session_date,
                raw.checkpoint_time,
                raw.horizon,
                raw.action AS raw_action,
                adjusted.adjusted_action,
                adjusted.market_regime_family,
                adjusted.adjustment_profile,
                raw.action_score AS raw_timing_score,
                adjusted.adjusted_timing_score,
                adjusted.signal_quality_flag,
                adjusted.fallback_flag
            FROM fact_intraday_entry_decision AS raw
            LEFT JOIN fact_intraday_adjusted_entry_decision AS adjusted
              ON raw.session_date = adjusted.session_date
             AND raw.symbol = adjusted.symbol
             AND raw.horizon = adjusted.horizon
             AND raw.checkpoint_time = adjusted.checkpoint_time
             AND raw.ranking_version = adjusted.ranking_version
            WHERE raw.symbol = ?
            ORDER BY raw.session_date DESC, raw.checkpoint_time DESC, raw.horizon
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def stock_workbench_intraday_timing_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                horizon,
                strategy_id,
                entry_checkpoint_time,
                market_regime_family,
                adjustment_profile,
                executed_flag,
                no_entry_flag,
                realized_excess_return,
                timing_edge_vs_open_bps,
                outcome_status
            FROM fact_intraday_strategy_result
            WHERE symbol = ?
            ORDER BY session_date DESC, horizon, strategy_id
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


UI_VALUE_LABELS.setdefault("run_type", {}).update(
    {
        "materialize_intraday_policy_candidates": "?μ쨷 ?뺤콉??후보 생성",
        "run_intraday_policy_calibration": "?μ쨷 ?뺤콉??보정 실행",
        "run_intraday_policy_walkforward": "장중 정책 기간별 재검증",
        "evaluate_intraday_policy_ablation": "장중 정책 항목 제거 평가",
        "materialize_intraday_policy_recommendations": "?μ쨷 ?뺤콉??추천 생성",
        "freeze_intraday_active_policy": "장중 정책 수동 반영",
        "rollback_intraday_active_policy": "장중 정책 되돌리기",
        "render_intraday_policy_research_report": "장중 정책 연구 리포트 만들기",
        "publish_discord_intraday_policy_summary": "장중 정책 디스코드 요약 보내기",
        "validate_intraday_policy_framework": "장중 정책 동작 점검",
    }
)
UI_VALUE_LABELS.setdefault("scope_type", {}).update(
    {
        "GLOBAL": "전역",
        "HORIZON": "기간별",
        "HORIZON_CHECKPOINT": "기간+체크포인트",
        "HORIZON_REGIME_CLUSTER": "기간+레짐 클러스터",
        "HORIZON_CHECKPOINT_REGIME_FAMILY": "기간+체크포인트+레짐 패밀리",
    }
)
UI_VALUE_LABELS.setdefault("promotion_type", {}).update(
    {
        "MANUAL_FREEZE": "수동 반영",
        "AUTO_PROMOTION": "자동 반영",
        "ROLLBACK_RESTORE": "되돌리기 복원",
    }
)
UI_VALUE_LABELS.setdefault("experiment_type", {}).update(
    {
        "policy_calibration": "정책 보정",
        "policy_walkforward": "정책 시계열 검증",
        "policy_ablation": "정책 제거 실험",
    }
)
UI_VALUE_LABELS.setdefault("split_mode", {}).update(
    {
        "ANCHORED_WALKFORWARD": "Anchored Walk-Forward",
        "ROLLING_WALKFORWARD": "Rolling Walk-Forward",
    }
)
UI_VALUE_LABELS.setdefault("split_name", {}).update(
    {
        "test": "테스트",
        "all": "전체",
    }
)
UI_COLUMN_LABELS.update(
    {
        "experiment_run_id": "실험 실행 ID",
        "experiment_name": "실험명",
        "experiment_type": "실험 유형",
        "search_space_version": "검색 공간 버전",
        "objective_version": "목표 함수 버전",
        "split_version": "분할 버전",
        "split_mode": "분할 방식",
        "split_name": "분할 구간",
        "split_index": "분할 순번",
        "window_start_date": "윈도우 시작일",
        "window_end_date": "윈도우 종료일",
        "train_start_date": "학습 시작일",
        "train_end_date": "학습 종료일",
        "validation_start_date": "검증 시작일",
        "validation_end_date": "검증 종료일",
        "test_start_date": "테스트 시작일",
        "test_end_date": "테스트 종료일",
        "selected_policy_candidate_id": "선택 정책 후보 ID",
        "policy_candidate_id": "정책 후보 ID",
        "template_id": "정책 템플릿",
        "scope_type": "적용 범위",
        "scope_key": "범위 키",
        "candidate_label": "후보 라벨",
        "parameter_hash": "파라미터 해시",
        "regime_cluster": "레짐 클러스터",
        "regime_family": "레짐 패밀리",
        "enter_threshold_delta": "진입 임계치 조정",
        "wait_threshold_delta": "대기 임계치 조정",
        "avoid_threshold_delta": "회피 임계치 조정",
        "min_selection_confidence_gate": "최소 선별 신뢰도",
        "min_signal_quality_gate": "최소 신호 품질",
        "uncertainty_penalty_weight": "불확실성 패널티",
        "spread_penalty_weight": "스프레드 패널티",
        "friction_penalty_weight": "마찰 패널티",
        "gap_chase_penalty_weight": "갭 추격 패널티",
        "cohort_weakness_penalty_weight": "코호트 약세 패널티",
        "market_shock_penalty_weight": "시장 충격 패널티",
        "data_weak_guard_strength": "데이터 약세 가드",
        "max_gap_up_allowance_pct": "최대 갭 상승 허용률",
        "min_execution_strength_gate": "최소 체결 강도",
        "min_orderbook_imbalance_gate": "최소 호가 불균형",
        "allow_enter_under_data_weak": "데이터 약세 진입 허용",
        "allow_wait_override": "대기 오버라이드 허용",
        "selection_rank_cap": "선별 순위 상한",
        "test_session_count": "테스트 세션 수",
        "window_session_count": "윈도우 세션 수",
        "no_entry_count": "미진입 수",
        "mean_realized_excess_return": "평균 실현 초과수익률",
        "median_realized_excess_return": "중앙 실현 초과수익률",
        "mean_timing_edge_vs_open_bps": "평균 timing edge(bps)",
        "median_timing_edge_vs_open_bps": "중앙 timing edge(bps)",
        "positive_timing_edge_rate": "양수 timing edge 비율",
        "skip_saved_loss_rate": "손실 회피 비율",
        "missed_winner_rate": "상승 놓침 비율",
        "left_tail_proxy": "하방 위험 지표",
        "stability_score": "안정성 점수",
        "objective_score": "목표 점수",
        "manual_review_required_flag": "수동 검토 필요",
        "fallback_scope_type": "대체 범위",
        "fallback_scope_key": "대체 범위 키",
        "recommendation_date": "추천일",
        "recommendation_rank": "추천 순위",
        "source_experiment_run_id": "원본 실험 실행 ID",
        "source_recommendation_date": "원본 추천일",
        "promotion_type": "승격 유형",
        "source_type": "원본 유형",
        "effective_from_date": "효력 시작일",
        "effective_to_date": "효력 종료일",
        "active_flag": "활성 여부",
        "rollback_of_active_policy_id": "되돌릴 정책 ID",
        "active_policy_id": "활성 정책 ID",
        "active_policy_candidate_id": "활성 정책 후보 ID",
        "active_policy_template_id": "활성 정책 템플릿",
        "active_policy_scope_type": "활성 정책 범위",
        "active_policy_scope_key": "활성 정책 범위 키",
        "tuned_action": "튜닝 액션",
        "tuned_score": "튜닝 점수",
        "policy_trace": "정책 추적",
        "policy_reason_codes_json": "정책 판단 사유",
        "fallback_used_flag": "대체 계산 사용",
        "ablation_name": "항목 제거 실험",
        "base_policy_source": "기준 정책 소스",
        "base_policy_candidate_id": "기준 정책 후보 ID",
        "mean_realized_excess_return_delta": "평균 초과수익률 변화",
        "median_realized_excess_return_delta": "중앙 초과수익률 변화",
        "hit_rate_delta": "적중률 변화",
        "mean_timing_edge_vs_open_bps_delta": "timing edge 변화(bps)",
        "execution_rate_delta": "실행률 변화",
        "skip_saved_loss_rate_delta": "손실 회피 변화",
        "missed_winner_rate_delta": "상승 놓침 변화",
        "left_tail_proxy_delta": "좌측 꼬리 변화",
        "stability_score_delta": "안정성 변화",
        "objective_score_delta": "목표 점수 변화",
    }
)


def latest_intraday_policy_experiment_frame(
    settings: Settings,
    *,
    limit: int = 30,
    experiment_type: str | None = None,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if experiment_type is None:
            return connection.execute(
                """
                SELECT
                    experiment_name,
                    experiment_type,
                    search_space_version,
                    objective_version,
                    split_version,
                    split_mode,
                    horizon,
                    candidate_count,
                    selected_policy_candidate_id,
                    fallback_used_flag,
                    status,
                    created_at
                FROM vw_latest_intraday_policy_experiment_run
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                experiment_name,
                experiment_type,
                search_space_version,
                objective_version,
                split_version,
                split_mode,
                horizon,
                candidate_count,
                selected_policy_candidate_id,
                fallback_used_flag,
                status,
                created_at
            FROM vw_latest_intraday_policy_experiment_run
            WHERE experiment_type = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            [experiment_type, limit],
        ).fetchdf()


def latest_intraday_policy_evaluation_frame(
    settings: Settings,
    *,
    split_name: str = "test",
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        base_query = """
            SELECT
                experiment_run_id,
                split_name,
                split_index,
                horizon,
                template_id,
                scope_type,
                scope_key,
                checkpoint_time,
                regime_cluster,
                regime_family,
                window_session_count,
                sample_count,
                matured_count,
                executed_count,
                execution_rate,
                mean_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                skip_saved_loss_rate,
                missed_winner_rate,
                left_tail_proxy,
                stability_score,
                objective_score,
                manual_review_required_flag,
                fallback_scope_type,
                fallback_scope_key
            FROM vw_latest_intraday_policy_evaluation
            WHERE split_name = ?
            ORDER BY window_end_date DESC, horizon, objective_score DESC NULLS LAST
            LIMIT ?
        """
        split_order = [split_name]
        if split_name == "test":
            split_order.extend(["validation", "all"])
        for target_split in split_order:
            frame = connection.execute(base_query, [target_split, limit]).fetchdf()
            if not frame.empty:
                return frame
        return pd.DataFrame()


def latest_intraday_policy_ablation_frame(
    settings: Settings,
    *,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                ablation_date,
                horizon,
                base_policy_source,
                ablation_name,
                sample_count,
                mean_realized_excess_return_delta,
                hit_rate_delta,
                mean_timing_edge_vs_open_bps_delta,
                execution_rate_delta,
                skip_saved_loss_rate_delta,
                missed_winner_rate_delta,
                left_tail_proxy_delta,
                stability_score_delta,
                objective_score_delta
            FROM vw_latest_intraday_policy_ablation_result
            ORDER BY ablation_date DESC, horizon, ablation_name
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_policy_recommendation_frame(
    settings: Settings,
    *,
    recommendation_date=None,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        target_date = recommendation_date
        if target_date is None:
            row = connection.execute(
                "SELECT MAX(recommendation_date) FROM fact_intraday_policy_selection_recommendation"
            ).fetchone()
            target_date = None if row is None or row[0] is None else pd.Timestamp(row[0]).date()
        if target_date is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT
                recommendation_date,
                horizon,
                scope_type,
                scope_key,
                recommendation_rank,
                policy_candidate_id,
                template_id,
                test_session_count,
                executed_count,
                execution_rate,
                mean_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                stability_score,
                objective_score,
                manual_review_required_flag,
                fallback_scope_type,
                fallback_scope_key
            FROM fact_intraday_policy_selection_recommendation
            WHERE recommendation_date = ?
            ORDER BY horizon, recommendation_rank, scope_type, scope_key
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def latest_intraday_active_policy_frame(
    settings: Settings,
    *,
    as_of_date=None,
    limit: int = 30,
    active_only: bool = True,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        target_date = as_of_date
        if target_date is None:
            row = connection.execute(
                "SELECT MAX(effective_from_date) FROM fact_intraday_active_policy"
            ).fetchone()
            target_date = None if row is None or row[0] is None else pd.Timestamp(row[0]).date()
        if target_date is None:
            return pd.DataFrame()
        if active_only:
            return connection.execute(
                """
                SELECT
                    active.horizon,
                    active.scope_type,
                    active.scope_key,
                    active.checkpoint_time,
                    active.regime_cluster,
                    active.regime_family,
                    active.policy_candidate_id,
                    candidate.template_id,
                    active.source_recommendation_date,
                    active.promotion_type,
                    active.effective_from_date,
                    active.effective_to_date,
                    active.fallback_scope_type,
                    active.fallback_scope_key,
                    active.note
                FROM fact_intraday_active_policy AS active
                JOIN fact_intraday_policy_candidate AS candidate
                  ON active.policy_candidate_id = candidate.policy_candidate_id
                WHERE active.effective_from_date <= ?
                  AND (active.effective_to_date IS NULL OR active.effective_to_date >= ?)
                ORDER BY active.horizon, active.scope_type, active.scope_key
                LIMIT ?
                """,
                [target_date, target_date, limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                horizon,
                scope_type,
                scope_key,
                policy_candidate_id,
                promotion_type,
                effective_from_date,
                effective_to_date,
                active_flag,
                rollback_of_active_policy_id,
                note,
                updated_at
            FROM fact_intraday_active_policy
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_policy_rollback_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                horizon,
                scope_type,
                scope_key,
                policy_candidate_id,
                promotion_type,
                rollback_of_active_policy_id,
                effective_from_date,
                note,
                updated_at
            FROM fact_intraday_active_policy
            WHERE promotion_type = 'ROLLBACK_RESTORE'
               OR rollback_of_active_policy_id IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_policy_publish_status_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not _metadata_available(settings):
        return pd.DataFrame()
    return _metadata_frame(
        settings,
        """
            WITH ranked_runs AS (
                SELECT
                    run_type,
                    started_at,
                    finished_at,
                    status,
                    notes,
                    ROW_NUMBER() OVER (
                        PARTITION BY run_type
                        ORDER BY started_at DESC
                    ) AS row_number
                FROM ops_run_manifest
                WHERE run_type IN (
                    'materialize_intraday_policy_candidates',
                    'run_intraday_policy_calibration',
                    'run_intraday_policy_walkforward',
                    'evaluate_intraday_policy_ablation',
                    'materialize_intraday_policy_recommendations',
                    'freeze_intraday_active_policy',
                    'rollback_intraday_active_policy',
                    'render_intraday_policy_research_report',
                    'publish_discord_intraday_policy_summary',
                    'validate_intraday_policy_framework'
                )
            )
            SELECT
                run_type,
                started_at,
                finished_at,
                status,
                notes
            FROM ranked_runs
            WHERE row_number = 1
            ORDER BY started_at DESC
            LIMIT ?
        """,
        [limit],
    )


def latest_intraday_policy_report_preview(settings: Settings) -> str | None:
    return _latest_manifest_preview(settings, run_type="render_intraday_policy_research_report")


def intraday_console_tuned_action_frame(
    settings: Settings,
    *,
    session_date=None,
    symbol: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    frame = apply_active_intraday_policy_frame(
        settings,
        session_date=target_date,
        symbol=symbol,
        limit=limit,
    )
    if frame.empty:
        return frame
    columns = [
        "session_date",
        "checkpoint_time",
        "symbol",
        "company_name",
        "horizon",
        "market_regime_family",
        "adjusted_action",
        "tuned_action",
        "adjusted_timing_score",
        "tuned_score",
        "active_policy_candidate_id",
        "active_policy_template_id",
        "active_policy_scope_type",
        "active_policy_scope_key",
        "policy_trace",
        "fallback_used_flag",
    ]
    available = [column for column in columns if column in frame.columns]
    return frame.loc[:, available].copy()


def stock_workbench_intraday_tuned_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            """
            SELECT MAX(session_date)
            FROM fact_intraday_candidate_session
            WHERE symbol = ?
            """,
            [symbol.zfill(6)],
        ).fetchone()
    if row is None or row[0] is None:
        return pd.DataFrame()
    return intraday_console_tuned_action_frame(
        settings,
        session_date=pd.Timestamp(row[0]).date(),
        symbol=symbol.zfill(6),
        limit=limit,
    )


UI_COLUMN_LABELS.update(
    {
        "training_run_id": "학습 실행 ID",
        "panel_name": "패널",
        "predicted_class": "예측 클래스",
        "predicted_class_probability": "예측 확률",
        "confidence_margin": "신뢰도 마진",
        "active_meta_model_id": "사용 중인 메타 모델 ID",
        "active_meta_training_run_id": "사용 중인 메타 학습 실행 ID",
        "final_action": "최종 판단",
        "override_applied_flag": "오버라이드 적용",
        "override_type": "오버라이드 유형",
        "hard_guard_block_flag": "하드가드 차단",
        "rollback_of_active_meta_model_id": "되돌린 대상 메타 모델 ID",
        "calibration_bucket": "보정 구간",
        "avg_confidence": "평균 신뢰도",
        "observed_accuracy": "관측 정확도",
        "feature_name": "피처명",
        "importance": "중요도",
        "source_type": "출처",
        "promotion_type": "반영 유형",
    }
)
UI_VALUE_LABELS.setdefault("panel_name", {}).update(
    {
        ENTER_PANEL: "진입 패널",
        WAIT_PANEL: "대기 패널",
    }
)
UI_VALUE_LABELS.setdefault("predicted_class", {}).update(
    {
        "KEEP_ENTER": "진입 유지",
        "DOWNGRADE_WAIT": "대기로 하향",
        "DOWNGRADE_AVOID": "회피로 하향",
        "KEEP_WAIT": "대기 유지",
        "UPGRADE_ENTER": "진입으로 상향",
    }
)
UI_VALUE_LABELS.setdefault("final_action", {}).update(
    {
        "ENTER_NOW": "지금 진입",
        "WAIT_RECHECK": "재확인 대기",
        "AVOID_TODAY": "오늘 회피",
        "DATA_INSUFFICIENT": "데이터 부족",
    }
)


def latest_intraday_meta_training_frame(
    settings: Settings,
    *,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                training_run_id,
                model_version,
                horizon,
                panel_name,
                train_end_date,
                train_row_count,
                validation_row_count,
                train_session_count,
                validation_session_count,
                feature_count,
                fallback_flag,
                fallback_reason,
                created_at
            FROM vw_latest_model_training_run
            WHERE model_domain = ?
              AND model_version = ?
            ORDER BY train_end_date DESC, horizon, panel_name
            LIMIT ?
            """,
            [INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION, limit],
        ).fetchdf()


def latest_intraday_meta_active_model_frame(
    settings: Settings,
    *,
    as_of_date=None,
    limit: int = 30,
    active_only: bool = True,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        target_date = as_of_date
        if target_date is None:
            row = connection.execute(
                "SELECT MAX(effective_from_date) FROM fact_intraday_active_meta_model"
            ).fetchone()
            target_date = None if row is None or row[0] is None else pd.Timestamp(row[0]).date()
        if target_date is None:
            return pd.DataFrame()
        if active_only:
            return connection.execute(
                """
                SELECT
                    active.horizon,
                    active.panel_name,
                    active.active_meta_model_id,
                    active.training_run_id,
                    active.model_version,
                    active.source_type,
                    active.promotion_type,
                    active.effective_from_date,
                    active.effective_to_date,
                    active.note,
                    train.fallback_flag,
                    train.fallback_reason
                FROM fact_intraday_active_meta_model AS active
                LEFT JOIN fact_model_training_run AS train
                  ON active.training_run_id = train.training_run_id
                WHERE active.effective_from_date <= ?
                  AND (active.effective_to_date IS NULL OR active.effective_to_date >= ?)
                  AND active.active_flag = TRUE
                ORDER BY active.horizon, active.panel_name
                LIMIT ?
                """,
                [target_date, target_date, limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                horizon,
                panel_name,
                active_meta_model_id,
                training_run_id,
                model_version,
                source_type,
                promotion_type,
                effective_from_date,
                effective_to_date,
                active_flag,
                rollback_of_active_meta_model_id,
                note,
                updated_at
            FROM fact_intraday_active_meta_model
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_meta_rollback_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                horizon,
                panel_name,
                active_meta_model_id,
                training_run_id,
                promotion_type,
                rollback_of_active_meta_model_id,
                effective_from_date,
                note,
                updated_at
            FROM fact_intraday_active_meta_model
            WHERE promotion_type = 'ROLLBACK_RESTORE'
               OR rollback_of_active_meta_model_id IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_meta_run_status_frame(
    settings: Settings,
    *,
    limit: int = 12,
) -> pd.DataFrame:
    if not _metadata_available(settings):
        return pd.DataFrame()
    return _metadata_frame(
        settings,
        """
            WITH ranked_runs AS (
                SELECT
                    run_type,
                    started_at,
                    finished_at,
                    status,
                    notes,
                    ROW_NUMBER() OVER (
                        PARTITION BY run_type
                        ORDER BY started_at DESC
                    ) AS row_number
                FROM ops_run_manifest
                WHERE run_type IN (
                    'build_intraday_meta_training_dataset',
                    'validate_intraday_meta_dataset',
                    'train_intraday_meta_models',
                    'run_intraday_meta_walkforward',
                    'calibrate_intraday_meta_thresholds',
                    'evaluate_intraday_meta_models',
                    'materialize_intraday_meta_predictions',
                    'materialize_intraday_final_actions',
                    'freeze_intraday_active_meta_model',
                    'rollback_intraday_active_meta_model',
                    'render_intraday_meta_model_report',
                    'publish_discord_intraday_meta_summary',
                    'validate_intraday_meta_model_framework'
                )
            )
            SELECT
                run_type,
                started_at,
                finished_at,
                status,
                notes
            FROM ranked_runs
            WHERE row_number = 1
            ORDER BY started_at DESC
            LIMIT ?
        """,
        [limit],
    )


def _latest_intraday_meta_session_date(settings: Settings):
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            """
            SELECT COALESCE(
                (SELECT MAX(session_date) FROM fact_intraday_meta_decision),
                (SELECT MAX(session_date) FROM fact_intraday_meta_prediction)
            )
            """
        ).fetchone()
    return None if row is None or row[0] is None else pd.Timestamp(row[0]).date()


def latest_intraday_meta_prediction_frame(
    settings: Settings,
    *,
    session_date=None,
    symbol: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_meta_session_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    symbol_filter = ""
    params: list[object] = [target_date]
    if symbol:
        symbol_filter = " AND prediction.symbol = ?"
        params.append(symbol.zfill(6))
    params.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            f"""
            SELECT
                prediction.session_date,
                prediction.checkpoint_time,
                prediction.symbol,
                symbol_dim.company_name,
                prediction.horizon,
                prediction.panel_name,
                prediction.tuned_action,
                prediction.predicted_class,
                prediction.predicted_class_probability,
                prediction.confidence_margin,
                prediction.uncertainty_score,
                prediction.disagreement_score,
                prediction.fallback_flag,
                prediction.fallback_reason
            FROM fact_intraday_meta_prediction AS prediction
            LEFT JOIN dim_symbol AS symbol_dim
              ON prediction.symbol = symbol_dim.symbol
            WHERE prediction.session_date = ?
              {symbol_filter}
            ORDER BY prediction.horizon, prediction.symbol, prediction.checkpoint_time
            LIMIT ?
            """,
            params,
        ).fetchdf()


def latest_intraday_meta_decision_frame(
    settings: Settings,
    *,
    session_date=None,
    symbol: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_meta_session_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    symbol_filter = ""
    params: list[object] = [target_date]
    if symbol:
        symbol_filter = " AND decision.symbol = ?"
        params.append(symbol.zfill(6))
    params.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            f"""
            SELECT
                decision.session_date,
                decision.checkpoint_time,
                decision.symbol,
                symbol_dim.company_name,
                decision.horizon,
                decision.raw_action,
                decision.adjusted_action,
                decision.tuned_action,
                decision.final_action,
                decision.panel_name,
                decision.predicted_class,
                decision.predicted_class_probability,
                decision.confidence_margin,
                decision.uncertainty_score,
                decision.disagreement_score,
                decision.override_applied_flag,
                decision.override_type,
                decision.hard_guard_block_flag,
                decision.fallback_flag,
                decision.fallback_reason,
                decision.active_meta_model_id
            FROM fact_intraday_meta_decision AS decision
            LEFT JOIN dim_symbol AS symbol_dim
              ON decision.symbol = symbol_dim.symbol
            WHERE decision.session_date = ?
              {symbol_filter}
            ORDER BY decision.horizon, decision.symbol, decision.checkpoint_time
            LIMIT ?
            """,
            params,
        ).fetchdf()


def latest_intraday_meta_overlay_comparison_frame(
    settings: Settings,
    *,
    metric_scope: str = "overlay",
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if metric_scope == "overlay":
            return connection.execute(
                """
                SELECT
                    horizon,
                    panel_name,
                    MAX(
                        CASE
                            WHEN metric_name = 'policy_only_mean_excess_return'
                                THEN metric_value
                        END
                    ) AS policy_only_mean_excess_return,
                    MAX(
                        CASE
                            WHEN metric_name = 'meta_overlay_mean_excess_return'
                                THEN metric_value
                        END
                    ) AS meta_overlay_mean_excess_return,
                    MAX(
                        CASE
                            WHEN metric_name = 'same_exit_lift_mean_excess_return'
                                THEN metric_value
                        END
                    ) AS same_exit_lift_mean_excess_return,
                    MAX(
                        CASE
                            WHEN metric_name = 'same_exit_lift_mean_timing_edge_bps'
                                THEN metric_value
                        END
                    ) AS same_exit_lift_mean_timing_edge_bps,
                    MAX(
                        CASE
                            WHEN metric_name = 'override_rate'
                                THEN metric_value
                        END
                    ) AS override_rate,
                    MAX(
                        CASE
                            WHEN metric_name = 'fallback_rate'
                                THEN metric_value
                        END
                    ) AS fallback_rate,
                    MAX(
                        CASE
                            WHEN metric_name = 'upgrade_precision'
                                THEN metric_value
                        END
                    ) AS upgrade_precision,
                    MAX(
                        CASE
                            WHEN metric_name = 'downgrade_precision'
                                THEN metric_value
                        END
                    ) AS downgrade_precision,
                    MAX(
                        CASE
                            WHEN metric_name = 'saved_loss_rate'
                                THEN metric_value
                        END
                    ) AS saved_loss_rate,
                    MAX(
                        CASE
                            WHEN metric_name = 'missed_winner_rate'
                                THEN metric_value
                        END
                    ) AS missed_winner_rate,
                    MAX(sample_count) AS sample_count
                FROM vw_latest_model_metric_summary
                WHERE model_domain = ?
                  AND model_version = ?
                  AND split_name = 'evaluation'
                  AND metric_scope = 'overlay'
                  AND comparison_key = 'overall'
                GROUP BY horizon, panel_name
                ORDER BY horizon, panel_name
                LIMIT ?
                """,
                [INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION, limit],
            ).fetchdf()
        comparison_scope = "market_regime_family" if metric_scope == "regime" else "checkpoint_time"
        return connection.execute(
            """
            SELECT
                horizon,
                panel_name,
                comparison_key,
                metric_value AS same_exit_lift_mean_excess_return,
                sample_count
            FROM vw_latest_model_metric_summary
            WHERE model_domain = ?
              AND model_version = ?
              AND split_name = 'evaluation'
              AND metric_scope = ?
              AND metric_name = 'same_exit_lift_mean_excess_return'
            ORDER BY horizon, panel_name, comparison_key
            LIMIT ?
            """,
            [
                INTRADAY_META_MODEL_DOMAIN,
                INTRADAY_META_MODEL_VERSION,
                comparison_scope,
                limit,
            ],
        ).fetchdf()


def _latest_intraday_meta_training_row(
    settings: Settings,
    *,
    horizon: int,
    panel_name: str,
):
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        frame = connection.execute(
            """
            SELECT *
            FROM vw_latest_model_training_run
            WHERE model_domain = ?
              AND model_version = ?
              AND horizon = ?
              AND panel_name = ?
            ORDER BY train_end_date DESC, created_at DESC
            LIMIT 1
            """,
            [INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION, horizon, panel_name],
        ).fetchdf()
    if frame.empty:
        return None
    return frame.iloc[0]


def _intraday_meta_diagnostic_payload(
    settings: Settings,
    *,
    horizon: int,
    panel_name: str,
) -> tuple[pd.Series | None, dict[str, object], object]:
    training_row = _latest_intraday_meta_training_row(
        settings,
        horizon=horizon,
        panel_name=panel_name,
    )
    if training_row is None:
        return None, {}, None
    diagnostics_payload: dict[str, object] = {}
    diagnostic_path = resolve_ui_artifact_path(settings, training_row.get("diagnostic_artifact_uri"))
    if diagnostic_path is not None:
        diagnostics_payload = json.loads(diagnostic_path.read_text(encoding="utf-8"))
    model_payload = None
    artifact_path = resolve_ui_artifact_path(settings, training_row.get("artifact_uri"))
    if artifact_path is not None:
        model_payload = load_model_artifact(artifact_path)
    return training_row, diagnostics_payload, model_payload


def intraday_meta_feature_importance_frame(
    settings: Settings,
    *,
    horizon: int,
    panel_name: str,
    limit: int = 20,
) -> pd.DataFrame:
    _, diagnostics_payload, _ = _intraday_meta_diagnostic_payload(
        settings,
        horizon=horizon,
        panel_name=panel_name,
    )
    rows = diagnostics_payload.get("feature_importance", [])
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame = frame.sort_values(["member_name", "importance"], ascending=[True, False]).reset_index(
        drop=True
    )
    return frame.head(limit).copy()


def intraday_meta_confusion_matrix_frame(
    settings: Settings,
    *,
    horizon: int,
    panel_name: str,
) -> pd.DataFrame:
    _, _, model_payload = _intraday_meta_diagnostic_payload(
        settings,
        horizon=horizon,
        panel_name=panel_name,
    )
    if not model_payload:
        return pd.DataFrame()
    validation_frame = model_payload.get("validation_prediction_frame")
    if validation_frame is None or len(validation_frame) == 0:
        return pd.DataFrame()
    frame = validation_frame.copy()
    if frame.empty:
        return pd.DataFrame()
    output = (
        frame.groupby(["target_class", "predicted_class"], dropna=False)
        .size()
        .reset_index(name="sample_count")
    )
    return output.sort_values(["target_class", "predicted_class"]).reset_index(drop=True)


def intraday_meta_calibration_frame(
    settings: Settings,
    *,
    horizon: int,
    panel_name: str,
) -> pd.DataFrame:
    _, _, model_payload = _intraday_meta_diagnostic_payload(
        settings,
        horizon=horizon,
        panel_name=panel_name,
    )
    if not model_payload:
        return pd.DataFrame()
    validation_frame = model_payload.get("validation_prediction_frame")
    if validation_frame is None or len(validation_frame) == 0:
        return pd.DataFrame()
    frame = validation_frame.copy()
    if frame.empty:
        return pd.DataFrame()
    frame["correct_flag"] = (
        frame["target_class"].astype(str) == frame["predicted_class"].astype(str)
    )
    frame["calibration_bucket"] = pd.cut(
        pd.to_numeric(frame["predicted_class_probability"], errors="coerce"),
        bins=[0.0, 0.5, 0.65, 0.8, 0.9, 1.0],
        labels=["0.00-0.50", "0.50-0.65", "0.65-0.80", "0.80-0.90", "0.90-1.00"],
        include_lowest=True,
    )
    output = (
        frame.groupby(["predicted_class", "calibration_bucket"], dropna=False)
        .agg(
            sample_count=("symbol", "count"),
            avg_confidence=("predicted_class_probability", "mean"),
            observed_accuracy=("correct_flag", "mean"),
        )
        .reset_index()
    )
    output["calibration_bucket"] = output["calibration_bucket"].astype(str)
    return output.sort_values(["predicted_class", "calibration_bucket"]).reset_index(drop=True)


UI_COLUMN_LABELS.update(
    {
        "portfolio_policy_id": "포트폴리오 정책 ID",
        "portfolio_policy_version": "포트폴리오 정책 버전",
        "active_portfolio_policy_id": "활성 포트폴리오 정책 ID",
        "target_weight": "목표 비중",
        "target_notional": "목표 금액",
        "target_shares": "목표 수량",
        "target_price": "목표 기준가",
        "live_as_of_date": "즉석 계산 기준일",
        "live_reference_date": "기준 가격 일자",
        "live_reference_price": "즉석 기준가",
        "live_d1_selection_v2_value": "즉석 D1 추천 점수",
        "live_d1_selection_v2_grade": "즉석 D1 등급",
        "live_d1_eligible_flag": "즉석 D1 추천 여부",
        "live_d1_report_candidate_flag": "즉석 D1 리포트 후보",
        "live_d5_selection_v2_value": "즉석 D5 추천 점수",
        "live_d5_selection_v2_grade": "즉석 D5 등급",
        "live_d5_eligible_flag": "즉석 D5 추천 여부",
        "live_d5_report_candidate_flag": "즉석 D5 리포트 후보",
        "live_d5_expected_excess_return": "즉석 D5 예상 초과수익률",
        "live_d5_target_price": "즉석 목표가",
        "live_d5_upper_target_price": "즉석 강한 흐름 목표가",
        "live_d5_stop_price": "즉석 손절 참고선",
        "latest_portfolio_as_of_date": "최신 포트폴리오 기준일",
        "latest_portfolio_entry_trade_date": "최신 포트폴리오 진입 예정일",
        "latest_portfolio_execution_mode": "최신 포트폴리오 실행 모드",
        "latest_portfolio_included_flag": "최신 포트폴리오 편입 여부",
        "latest_portfolio_target_weight": "최신 포트폴리오 목표 비중",
        "latest_portfolio_gate_status": "최신 포트폴리오 진입 상태",
        "current_shares": "현재 수량",
        "current_weight": "현재 비중",
        "score_value": "할당 점수",
        "candidate_state": "후보 상태",
        "timing_gate_status": "타이밍 게이트",
        "rebalance_action": "리밸런스 액션",
        "action_sequence": "리밸런스 순서",
        "delta_shares": "수량 변화",
        "reference_price": "기준 가격",
        "notional_delta": "금액 변화",
        "cash_delta": "현금 변화",
        "waitlist_flag": "대기열 여부",
        "waitlist_rank": "대기열 순번",
        "blocked_flag": "차단 여부",
        "blocked_reason": "차단 사유",
        "constraint_type": "제약 유형",
        "event_code": "제약 코드",
        "requested_value": "요청 값",
        "applied_value": "적용 값",
        "limit_value": "한도 값",
        "active_flag": "활성 여부",
        "rollback_of_active_portfolio_policy_id": "되돌릴 정책 ID",
        "snapshot_date": "스냅샷 일자",
        "average_cost": "평균 단가",
        "close_price": "종가",
        "market_value": "평가 금액",
        "actual_weight": "실제 비중",
        "cash_like_flag": "현금 행 여부",
        "nav_value": "NAV",
        "invested_value": "투자 금액",
        "cash_value": "현금 금액",
        "gross_exposure": "총 익스포저",
        "net_exposure": "순 익스포저",
        "daily_return": "일간 수익률",
        "cumulative_return": "누적 수익률",
        "drawdown": "드로다운",
        "turnover_ratio": "회전율",
        "cash_weight": "현금 비중",
        "holding_count": "보유 종목 수",
        "max_single_weight": "최대 단일 비중",
        "top3_weight": "상위 3종목 비중",
        "comparison_key": "비교 키",
        "metric_name": "지표명",
        "metric_value": "지표값",
        "sample_count": "표본 수",
    }
)

UI_VALUE_LABELS.setdefault("execution_mode", {}).update(
    {
        "OPEN_ALL": "시가 일괄 진입",
        "TIMING_ASSISTED": "장중 타이밍 보조",
    }
)
UI_VALUE_LABELS.setdefault("portfolio_execution_mode", {}).update(UI_VALUE_LABELS["execution_mode"])
UI_VALUE_LABELS.setdefault("latest_portfolio_execution_mode", {}).update(UI_VALUE_LABELS["execution_mode"])
UI_VALUE_LABELS.setdefault("candidate_state", {}).update(
    {
        "NEW_ENTRY_CANDIDATE": "신규 진입 후보",
        "HOLD_CANDIDATE": "보유 유지 후보",
        "TRIM_CANDIDATE": "비중 축소 후보",
        "EXIT_CANDIDATE": "청산 후보",
        "WATCH_ONLY": "관찰 전용",
        "BLOCKED": "차단",
        "CASH": "현금",
    }
)
UI_VALUE_LABELS.setdefault("timing_gate_status", {}).update(
    {
        "OPEN_ALL": "시가 진입 모드",
        "TIMING_UNAVAILABLE": "타이밍 없음",
        "ENTER_ALLOWED": "진입 허용",
        "WAIT_GATE": "재확인 대기",
        "BLOCKED_BY_TIMING": "타이밍 차단",
        "CASH_BUFFER": "현금 버퍼",
    }
)
UI_VALUE_LABELS.setdefault("gate_status", {}).update(UI_VALUE_LABELS["timing_gate_status"])
UI_VALUE_LABELS.setdefault("latest_portfolio_gate_status", {}).update(UI_VALUE_LABELS["timing_gate_status"])
UI_VALUE_LABELS.setdefault("rebalance_action", {}).update(
    {
        "BUY_NEW": "신규 매수",
        "ADD": "추가 매수",
        "HOLD": "보유 유지",
        "TRIM": "비중 축소",
        "EXIT": "청산",
        "SKIP": "건너뜀",
        "NO_ACTION": "조치 없음",
    }
)
UI_VALUE_LABELS.setdefault("comparison_key", {}).update(
    {
        "OPEN_ALL": "시가 일괄 진입",
        "TIMING_ASSISTED": "타이밍 보조",
        "EQUAL_WEIGHT_BASELINE": "동일가중 기준선",
    }
)
UI_VALUE_LABELS.setdefault("report_type", {}).update(
    {
        "daily_research_report": "일일 리서치 리포트",
        "daily_discord_preview": "디스코드 장마감 요약",
        "after_close_brief": "장마감 직후 브리핑",
        "portfolio_report": "포트폴리오 리포트",
        "evaluation_report": "사후 평가 리포트",
        "evaluation_postmortem_report": "사후 점검 리포트",
        "intraday_summary_report": "장중 요약 리포트",
        "intraday_postmortem_report": "장중 사후 분석 리포트",
        "intraday_policy_research_report": "장중 정책 연구 리포트",
        "intraday_meta_model_report": "장중 메타 모델 리포트",
        "ops_report": "운영 리포트",
        "release_candidate_checklist": "릴리스 점검표",
    }
)
UI_VALUE_LABELS.setdefault("warning_level", {}).update(
    {
        "OK": "정상",
        "WARNING": "경고",
        "CRITICAL": "치명",
    }
)
UI_VALUE_LABELS.setdefault("severity", {}).update(
    {
        "OK": "정상",
        "INFO": "안내",
        "WARNING": "경고",
        "ERROR": "오류",
        "CRITICAL": "치명",
    }
)
UI_VALUE_LABELS.setdefault("health_status", {}).update(
    {
        "OK": "정상",
        "WARNING": "주의",
        "CRITICAL": "치명",
        "DEGRADED": "주의",
        "DEGRADED_SUCCESS": "주의",
    }
)
UI_VALUE_LABELS.setdefault("trigger_type", {}).update(
    {
        "SCHEDULED": "자동 실행",
        "MANUAL": "수동 실행",
        "RECOVERY": "자동 복구",
        "DEPENDENCY": "의존성 실행",
    }
)
UI_VALUE_LABELS.setdefault("run_type", {}).update(
    {
        "build_portfolio_candidate_book": "포트폴리오 후보군 생성",
        "validate_portfolio_candidate_book": "포트폴리오 후보군 검증",
        "freeze_active_portfolio_policy": "포트폴리오 정책 수동 반영",
        "rollback_active_portfolio_policy": "포트폴리오 정책 되돌리기",
        "materialize_portfolio_target_book": "포트폴리오 목표북 생성",
        "materialize_portfolio_rebalance_plan": "포트폴리오 리밸런스 계획",
        "materialize_portfolio_position_snapshots": "포트폴리오 포지션 스냅샷",
        "materialize_portfolio_nav": "포트폴리오 NAV 생성",
        "run_portfolio_walkforward": "포트폴리오 기간별 재검증",
        "evaluate_portfolio_policies": "포트폴리오 정책 평가",
        "render_portfolio_report": "포트폴리오 리포트 생성",
        "publish_discord_portfolio_summary": "포트폴리오 디스코드 발행",
        "validate_portfolio_framework": "포트폴리오 프레임워크 검증",
        "build_latest_app_snapshot": "현재 기준 스냅샷 생성",
        "build_report_index": "리포트 목록 색인 생성",
        "build_ui_freshness_snapshot": "화면 신선도 스냅샷 생성",
        "render_daily_research_report": "일일 리서치 리포트 생성",
        "render_evaluation_report": "사후 평가 리포트 생성",
        "render_intraday_summary_report": "장중 요약 리포트 생성",
        "render_release_candidate_checklist": "릴리스 점검표 생성",
        "validate_page_contracts": "화면 계약 검증",
        "validate_report_artifacts": "리포트 산출물 검증",
        "validate_navigation_integrity": "화면 이동 구조 검증",
        "validate_release_candidate": "릴리스 후보 검증",
    }
)
UI_VALUE_LABELS.setdefault("run_type", {}).update(
    {
        "materialize_intraday_policy_candidates": "장중 정책 후보 생성",
        "run_intraday_policy_calibration": "장중 정책 보정 실행",
        "run_intraday_policy_walkforward": "장중 정책 기간별 재검증",
        "evaluate_intraday_policy_ablation": "장중 정책 제거 실험 평가",
        "materialize_intraday_policy_recommendations": "장중 정책 추천 생성",
        "freeze_intraday_active_policy": "장중 정책 수동 반영",
        "rollback_intraday_active_policy": "장중 정책 되돌리기",
        "render_intraday_policy_research_report": "장중 정책 연구 리포트 생성",
        "publish_discord_intraday_policy_summary": "장중 정책 디스코드 요약 발행",
        "validate_intraday_policy_framework": "장중 정책 프레임워크 검증",
        "freeze_active_portfolio_policy": "포트폴리오 정책 수동 반영",
        "rollback_active_portfolio_policy": "포트폴리오 정책 되돌리기",
        "materialize_portfolio_nav": "포트폴리오 순자산 가치 생성",
        "freeze_intraday_active_meta_model": "장중 메타 모형 수동 반영",
        "rollback_intraday_active_meta_model": "장중 메타 모형 되돌리기",
        "train_alpha_model_v1": "알파 모형 학습",
        "materialize_alpha_predictions_v1": "알파 모형 예측 생성",
    }
)
UI_VALUE_LABELS.setdefault("strategy_id", {}).update(
    {
        "SEL_V2_OPEN_ALL": "선정 엔진 v2 시가 일괄",
        "SEL_V2_TIMING_RAW_FIRST_ENTER": "선정 엔진 v2 원시 첫 진입",
        "SEL_V2_TIMING_ADJ_FIRST_ENTER": "선정 엔진 v2 조정 첫 진입",
        "SEL_V2_TIMING_ADJ_0930_ONLY": "선정 엔진 v2 조정 09:30 고정",
        "SEL_V2_TIMING_ADJ_1000_ONLY": "선정 엔진 v2 조정 10:00 고정",
    }
)
UI_VALUE_LABELS.setdefault("reason_tag", {}).update(
    {
        "ml_alpha_supportive": "알파 모형 지지",
    }
)
UI_COLUMN_LABELS.update(
    {
        "latest_selection_v2_ranking_version": "최신 추천 모델 버전",
        "latest_selection_v2_date": "최신 추천 모델 기준일",
        "latest_selection_v2_rows": "최신 추천 모델 행 수",
        "d1_selection_v2_value": "1거래일 기준 추천 점수",
        "d1_selection_v2_grade": "1거래일 기준 추천 등급",
        "d5_selection_v2_value": "5거래일 기준 추천 점수",
        "d5_selection_v2_grade": "5거래일 기준 추천 등급",
        "d5_selection_v2_realized_excess_return": "5거래일 뒤 추천 모델 실현 초과수익률",
        "d5_selection_v2_band_status": "5거래일 기준 추천 모델 범위 판정",
        "selection_v2_avg_excess": "추천 모델 평균 초과수익률",
        "nav_value": "순자산 가치",
        "active_meta_model_id": "사용 중인 메타 모델 ID",
        "rollback_of_active_meta_model_id": "되돌릴 메타 모델 ID",
        "active_meta_model_ids_json": "사용 중인 메타 모델",
        "rollback_of_active_policy_id": "되돌릴 정책 ID",
        "ablation_name": "항목 제거 실험",
    }
)
UI_VALUE_LABELS.setdefault("prediction_version", {}).update(
    {
        ALPHA_PREDICTION_VERSION: "알파 모형 예측 v1",
    }
)
UI_VALUE_LABELS.setdefault("metric_scope", {}).update(
    {
        "policy_ablation": "정책 제거 실험",
        "overlay": "메타 보조",
        "regime": "시장 국면",
        "checkpoint": "체크포인트",
    }
)
UI_VALUE_LABELS.setdefault("run_type", {}).update(
    {
        "render_discord_eod_report": "디스코드 장마감 리포트 생성",
        "publish_discord_eod_report": "디스코드 장마감 리포트 발행",
        "publish_discord_postmortem_report": "디스코드 사후 분석 리포트 발행",
        "publish_discord_intraday_postmortem": "디스코드 장중 사후 분석 리포트 발행",
        "publish_discord_portfolio_summary": "디스코드 포트폴리오 요약 보내기",
    }
)
UI_COLUMN_LABELS.update(
    {
        "provider_name": "제공처",
        "service_slug": "서비스 코드",
        "display_name_ko": "서비스명",
        "endpoint_url": "엔드포인트 URL",
        "request_date_field": "요청 일자 필드",
        "approval_required": "승인 필요",
        "expected_usage": "예상 용도",
        "request_cost_weight": "요청 가중치",
        "enabled_by_env": "환경 활성화",
        "last_smoke_status": "최근 스모크 상태",
        "last_smoke_ts": "최근 스모크 시각",
        "last_success_ts": "최근 성공 시각",
        "last_http_status": "최근 HTTP 상태",
        "last_error_class": "최근 오류 분류",
        "fallback_mode": "대체 동작",
        "request_budget": "일 요청 예산",
        "requests_used": "사용 요청 수",
        "usage_ratio": "사용 비율",
        "throttle_state": "예산 상태",
        "request_ts": "요청 시각",
        "rows_received": "수신 행 수",
        "used_fallback": "폴백 사용",
        "error_code": "오류 코드",
        "source_label": "출처 표기",
        "page_slug": "페이지",
        "component_slug": "컴포넌트",
        "active_flag": "활성 여부",
    }
)
UI_VALUE_LABELS.setdefault("provider_name", {}).update(
    {
        "krx": "한국거래소",
        "kis": "한국투자증권",
        "dart": "OpenDART",
        "naver_news": "네이버 뉴스",
    }
)
UI_VALUE_LABELS.setdefault("service_slug", {}).update(
    {
        "stock_kospi_daily_trade": "유가증권 일별매매정보",
        "stock_kosdaq_daily_trade": "코스닥 일별매매정보",
        "stock_kospi_symbol_master": "유가증권 종목기본정보",
        "stock_kosdaq_symbol_master": "코스닥 종목기본정보",
        "index_krx_daily": "KRX 시리즈 일별시세정보",
        "index_kospi_daily": "KOSPI 시리즈 일별시세정보",
        "index_kosdaq_daily": "KOSDAQ 시리즈 일별시세정보",
        "etf_daily_trade": "ETF 일별매매정보",
    }
)
UI_VALUE_LABELS.setdefault("expected_usage", {}).update(
    {
        "reference": "참조 데이터",
        "market_statistics": "시장 통계",
        "index_statistics": "지수 통계",
        "etf_statistics": "ETF 통계",
        "reference_or_statistics": "참조/통계",
    }
)
UI_VALUE_LABELS.setdefault("throttle_state", {}).update(
    {
        "OK": "정상",
        "WARNING": "경고",
        "FALLBACK_ONLY": "폴백 전용",
        "BLOCKED": "차단",
        "NO_SNAPSHOT": "스냅샷 없음",
    }
)
UI_VALUE_LABELS.setdefault("fallback_mode", {}).update(
    {
        "primary_live": "라이브 우선",
        "fallback_only": "폴백 전용",
    }
)
