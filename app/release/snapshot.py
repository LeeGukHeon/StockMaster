# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import duckdb

from app.common.time import now_local
from app.ops.common import JobStatus, OpsJobResult
from app.ops.repository import json_text
from app.settings import Settings, get_settings
from app.storage.duckdb import bootstrap_core_tables
from app.storage.metadata_postgres import (
    execute_postgres_sql,
    executemany_postgres_sql,
    fetchone_postgres_sql,
    metadata_postgres_enabled,
)


@dataclass(frozen=True, slots=True)
class FreshnessSpec:
    page_name: str
    dataset_name: str
    sql: str
    warning_seconds: int
    critical_seconds: int
    notes: str
    trading_day_aware: bool = False
    warning_trading_days: int | None = None
    critical_trading_days: int | None = None


REPORT_TYPE_BY_PREVIEW_NAME: dict[str, str] = {
    "daily_research_report_preview.md": "daily_research_report",
    "portfolio_report_preview.md": "portfolio_report",
    "evaluation_report_preview.md": "evaluation_report",
    "intraday_summary_report_preview.md": "intraday_summary_report",
    "release_candidate_checklist_preview.md": "release_candidate_checklist",
    "ops_report_preview.md": "ops_report",
    "discord_preview.md": "daily_discord_preview",
    "after_close_brief_preview.md": "after_close_brief",
    "postmortem_preview.md": "evaluation_postmortem_report",
    "intraday_monitor_preview.md": "intraday_monitor_report",
    "intraday_postmortem_preview.md": "intraday_postmortem_report",
    "intraday_policy_research_preview.md": "intraday_policy_research_report",
    "intraday_meta_model_preview.md": "intraday_meta_model_report",
}


FRESHNESS_SPECS: tuple[FreshnessSpec, ...] = (
    FreshnessSpec(
        page_name="오늘",
        dataset_name="selection_v2",
        sql="""
        SELECT MAX(CAST(as_of_date AS TIMESTAMPTZ))
        FROM fact_ranking
        WHERE ranking_version = 'selection_engine_v2'
        """,
        warning_seconds=36 * 3600,
        critical_seconds=72 * 3600,
        notes="오늘 화면의 대표 선별 결과",
        trading_day_aware=True,
        warning_trading_days=1,
        critical_trading_days=2,
    ),
    FreshnessSpec(
        page_name="오늘",
        dataset_name="report_index",
        sql="SELECT MAX(generated_ts) FROM fact_latest_report_index",
        warning_seconds=24 * 3600,
        critical_seconds=72 * 3600,
        notes="최신 보고서 인덱스",
    ),
    FreshnessSpec(
        page_name="시장 현황",
        dataset_name="market_regime",
        sql="SELECT MAX(CAST(as_of_date AS TIMESTAMPTZ)) FROM fact_market_regime_snapshot",
        warning_seconds=36 * 3600,
        critical_seconds=72 * 3600,
        notes="시장 regime 요약",
        trading_day_aware=True,
        warning_trading_days=1,
        critical_trading_days=2,
    ),
    FreshnessSpec(
        page_name="시장 현황",
        dataset_name="market_news",
        sql="SELECT MAX(published_at) FROM fact_news_item",
        warning_seconds=24 * 3600,
        critical_seconds=72 * 3600,
        notes="시장 뉴스 메타데이터",
    ),
    FreshnessSpec(
        page_name="리더보드",
        dataset_name="selection_v2",
        sql="""
        SELECT MAX(CAST(as_of_date AS TIMESTAMPTZ))
        FROM fact_ranking
        WHERE ranking_version = 'selection_engine_v2'
        """,
        warning_seconds=36 * 3600,
        critical_seconds=72 * 3600,
        notes="selection v2 순위표",
        trading_day_aware=True,
        warning_trading_days=1,
        critical_trading_days=2,
    ),
    FreshnessSpec(
        page_name="리더보드",
        dataset_name="alpha_prediction",
        sql="""
        SELECT MAX(CAST(as_of_date AS TIMESTAMPTZ))
        FROM fact_prediction
        WHERE prediction_version = 'alpha_prediction_v1'
        """,
        warning_seconds=36 * 3600,
        critical_seconds=72 * 3600,
        notes="ML 알파 예측",
        trading_day_aware=True,
        warning_trading_days=1,
        critical_trading_days=2,
    ),
    FreshnessSpec(
        page_name="포트폴리오",
        dataset_name="target_book",
        sql="SELECT MAX(CAST(as_of_date AS TIMESTAMPTZ)) FROM fact_portfolio_target_book",
        warning_seconds=36 * 3600,
        critical_seconds=72 * 3600,
        notes="포트폴리오 목표 비중",
        trading_day_aware=True,
        warning_trading_days=1,
        critical_trading_days=2,
    ),
    FreshnessSpec(
        page_name="포트폴리오",
        dataset_name="nav_snapshot",
        sql="SELECT MAX(CAST(snapshot_date AS TIMESTAMPTZ)) FROM fact_portfolio_nav_snapshot",
        warning_seconds=36 * 3600,
        critical_seconds=72 * 3600,
        notes="포트폴리오 NAV",
        trading_day_aware=True,
        warning_trading_days=1,
        critical_trading_days=2,
    ),
    FreshnessSpec(
        page_name="장중 콘솔",
        dataset_name="intraday_final_action",
        sql="SELECT MAX(CAST(session_date AS TIMESTAMPTZ)) FROM fact_intraday_meta_decision",
        warning_seconds=18 * 3600,
        critical_seconds=48 * 3600,
        notes="장중 최종 액션",
    ),
    FreshnessSpec(
        page_name="장중 콘솔",
        dataset_name="intraday_market_context",
        sql="SELECT MAX(CAST(session_date AS TIMESTAMPTZ)) FROM fact_intraday_market_context_snapshot",
        warning_seconds=18 * 3600,
        critical_seconds=48 * 3600,
        notes="장중 market context",
    ),
    FreshnessSpec(
        page_name="사후 평가",
        dataset_name="evaluation_summary",
        sql="SELECT MAX(CAST(summary_date AS TIMESTAMPTZ)) FROM fact_evaluation_summary",
        warning_seconds=48 * 3600,
        critical_seconds=96 * 3600,
        notes="사후 평가 요약",
        trading_day_aware=True,
        warning_trading_days=2,
        critical_trading_days=4,
    ),
    FreshnessSpec(
        page_name="사후 평가",
        dataset_name="calibration",
        sql="SELECT MAX(CAST(diagnostic_date AS TIMESTAMPTZ)) FROM fact_calibration_diagnostic",
        warning_seconds=48 * 3600,
        critical_seconds=96 * 3600,
        notes="예측 band calibration",
        trading_day_aware=True,
        warning_trading_days=2,
        critical_trading_days=4,
    ),
    FreshnessSpec(
        page_name="종목 분석",
        dataset_name="selection_outcome",
        sql="SELECT MAX(CAST(selection_date AS TIMESTAMPTZ)) FROM fact_selection_outcome",
        warning_seconds=48 * 3600,
        critical_seconds=96 * 3600,
        notes="종목별 사후 결과",
        trading_day_aware=True,
        warning_trading_days=2,
        critical_trading_days=4,
    ),
    FreshnessSpec(
        page_name="리서치 랩",
        dataset_name="model_training",
        sql="SELECT MAX(CAST(train_end_date AS TIMESTAMPTZ)) FROM fact_model_training_run",
        warning_seconds=7 * 24 * 3600,
        critical_seconds=21 * 24 * 3600,
        notes="모델 학습 이력",
    ),
    FreshnessSpec(
        page_name="리서치 랩",
        dataset_name="policy_experiment",
        sql="SELECT MAX(created_at) FROM fact_intraday_policy_experiment_run",
        warning_seconds=7 * 24 * 3600,
        critical_seconds=21 * 24 * 3600,
        notes="정책 실험 이력",
    ),
    FreshnessSpec(
        page_name="운영",
        dataset_name="health_snapshot",
        sql="SELECT MAX(snapshot_at) FROM fact_health_snapshot",
        warning_seconds=6 * 3600,
        critical_seconds=24 * 3600,
        notes="운영 health snapshot",
    ),
    FreshnessSpec(
        page_name="운영",
        dataset_name="job_run",
        sql="SELECT MAX(started_at) FROM fact_job_run",
        warning_seconds=6 * 3600,
        critical_seconds=24 * 3600,
        notes="최근 job run",
    ),
    FreshnessSpec(
        page_name="헬스 대시보드",
        dataset_name="health_snapshot",
        sql="SELECT MAX(snapshot_at) FROM fact_health_snapshot",
        warning_seconds=6 * 3600,
        critical_seconds=24 * 3600,
        notes="헬스 대시보드 snapshot",
    ),
    FreshnessSpec(
        page_name="문서 / 도움말",
        dataset_name="release_candidate_check",
        sql="SELECT MAX(check_ts) FROM fact_release_candidate_check",
        warning_seconds=24 * 3600,
        critical_seconds=7 * 24 * 3600,
        notes="릴리즈 체크리스트",
    ),
)


def _scalar(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    params: list[object] | None = None,
) -> Any:
    row = connection.execute(query, params or []).fetchone()
    return row[0] if row else None


def _normalize_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    text = str(value)
    for parser in (datetime.fromisoformat, date.fromisoformat):
        try:
            parsed = parser(text)
            if isinstance(parsed, datetime):
                return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
            return datetime.combine(parsed, time.min, tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _path_hash(path: Path) -> str:
    return hashlib.sha1(path.as_posix().encode("utf-8")).hexdigest()[:16]


def _deduplicate_report_index(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(REPORT_INDEX_DEDUP_SQL)


REPORT_INDEX_DEDUP_SQL = """
    DELETE FROM fact_latest_report_index
    WHERE report_index_id IN (
        SELECT report_index_id
        FROM (
            SELECT
                report_index_id,
                ROW_NUMBER() OVER (
                    PARTITION BY report_key
                    ORDER BY generated_ts DESC, created_at DESC, report_index_id DESC
                ) AS row_number
            FROM fact_latest_report_index
        ) ranked
        WHERE row_number > 1
    )
"""


def _parse_as_of_date(path: Path) -> date | None:
    for part in path.parts:
        if "=" not in part:
            continue
        _, _, raw_value = part.partition("=")
        try:
            return date.fromisoformat(raw_value)
        except ValueError:
            continue
    return None


def _extract_run_id(path: Path) -> str | None:
    run_candidate = path.parent.name
    return run_candidate if "-" in run_candidate else None


def _resolve_report_type(path: Path) -> str:
    return REPORT_TYPE_BY_PREVIEW_NAME.get(path.name, path.parent.parent.name)


def _resolve_run_status(connection: duckdb.DuckDBPyConnection, run_id: str | None) -> str:
    if not run_id:
        return JobStatus.SUCCESS
    status = _scalar(connection, "SELECT status FROM fact_job_run WHERE run_id = ?", [run_id])
    if status:
        return str(status)
    settings = get_settings()
    if metadata_postgres_enabled(settings):
        row = fetchone_postgres_sql(
            settings,
            "SELECT status FROM ops_run_manifest WHERE run_id = ?",
            [run_id],
        )
        manifest_status = row[0] if row else None
    else:
        manifest_status = _scalar(
            connection,
            "SELECT status FROM ops_run_manifest WHERE run_id = ?",
            [run_id],
        )
    if manifest_status:
        normalized = str(manifest_status).upper()
        if normalized == "SUCCESS":
            return JobStatus.SUCCESS
        if normalized == "SKIPPED":
            return JobStatus.SKIPPED
        if normalized == "BLOCKED":
            return JobStatus.BLOCKED
        return JobStatus.FAILED
    return JobStatus.SUCCESS


def _payload_summary(preview_path: Path) -> dict[str, Any]:
    payload_path = preview_path.with_name(preview_path.name.replace("_preview.md", "_payload.json"))
    summary: dict[str, Any] = {
        "preview_path": str(preview_path),
        "payload_path": str(payload_path) if payload_path.exists() else None,
        "preview_name": preview_path.name,
        "byte_size": preview_path.stat().st_size,
    }
    if not payload_path.exists():
        return summary
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        summary["payload_error"] = "invalid_json"
        return summary
    summary["message_count"] = payload.get("message_count")
    summary["dry_run"] = bool(payload.get("dry_run", False))
    summary["published"] = bool(
        payload.get("published")
        or payload.get("published_flag")
        or payload.get("status") == "published"
    )
    return summary


def _latest_trading_day(
    connection: duckdb.DuckDBPyConnection,
    *,
    on_or_before: date,
    inclusive: bool,
) -> date | None:
    operator = "<=" if inclusive else "<"
    row = connection.execute(
        f"""
        SELECT MAX(trading_date)
        FROM dim_trading_calendar
        WHERE is_trading_day = TRUE
          AND trading_date {operator} ?
        """,
        [on_or_before],
    ).fetchone()
    return row[0] if row and row[0] else None


def _expected_trading_data_date(
    connection: duckdb.DuckDBPyConnection,
    snapshot_ts: datetime,
) -> date:
    snapshot_local = snapshot_ts.astimezone()
    snapshot_date = snapshot_local.date()
    market_close_cutoff = time(16, 30)
    same_day = connection.execute(
        """
        SELECT is_trading_day
        FROM dim_trading_calendar
        WHERE trading_date = ?
        """,
        [snapshot_date],
    ).fetchone()
    is_trading_day = bool(same_day and same_day[0])
    if is_trading_day and snapshot_local.time() >= market_close_cutoff:
        return snapshot_date
    previous_trading_date = _latest_trading_day(
        connection,
        on_or_before=snapshot_date,
        inclusive=not is_trading_day,
    )
    return previous_trading_date or snapshot_date


def _trading_day_lag(
    connection: duckdb.DuckDBPyConnection,
    *,
    latest_date: date,
    expected_date: date,
) -> int:
    if latest_date >= expected_date:
        return 0
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_trading_calendar
        WHERE is_trading_day = TRUE
          AND trading_date > ?
          AND trading_date <= ?
        """,
        [latest_date, expected_date],
    ).fetchone()
    return int(row[0]) if row and row[0] else 0


def _classify_freshness(
    connection: duckdb.DuckDBPyConnection,
    *,
    spec: FreshnessSpec,
    latest_ts: datetime | None,
    snapshot_ts: datetime,
) -> tuple[float | None, bool, str]:
    if latest_ts is None:
        return None, True, "CRITICAL"
    freshness_seconds = max((snapshot_ts - latest_ts).total_seconds(), 0.0)
    if spec.trading_day_aware:
        latest_date = latest_ts.astimezone().date()
        expected_date = _expected_trading_data_date(connection, snapshot_ts)
        lag_days = _trading_day_lag(
            connection,
            latest_date=latest_date,
            expected_date=expected_date,
        )
        warning_days = spec.warning_trading_days or 1
        critical_days = spec.critical_trading_days or (warning_days + 1)
        if lag_days >= critical_days:
            return freshness_seconds, True, "CRITICAL"
        if lag_days >= warning_days:
            return freshness_seconds, False, "WARNING"
        return freshness_seconds, False, "OK"
    if freshness_seconds >= spec.critical_seconds:
        return freshness_seconds, True, "CRITICAL"
    if freshness_seconds >= spec.warning_seconds:
        return freshness_seconds, False, "WARNING"
    return freshness_seconds, False, "OK"


def build_latest_app_snapshot(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date | None = None,
    job_run_id: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    snapshot_ts = now_local(settings.app.timezone)
    latest_selection_date = _scalar(
        connection,
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = 'selection_engine_v2'
        """,
    )
    snapshot_as_of_date = as_of_date or latest_selection_date
    snapshot_effective_date = snapshot_as_of_date or snapshot_ts.date()

    daily_bundle = connection.execute(
        """
        SELECT run_id, status
        FROM fact_job_run
        WHERE job_name = 'run_daily_research_pipeline'
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()
    latest_evaluation_date = _scalar(connection, "SELECT MAX(summary_date) FROM fact_evaluation_summary")
    latest_evaluation_run_id = _scalar(
        connection,
        """
        SELECT run_id
        FROM fact_job_run
        WHERE job_name = 'run_daily_evaluation_bundle'
        ORDER BY started_at DESC
        LIMIT 1
        """,
    )
    latest_intraday_session_date = _scalar(
        connection,
        "SELECT MAX(session_date) FROM fact_intraday_meta_decision",
    )
    latest_intraday_run_id = _scalar(
        connection,
        """
        SELECT run_id
        FROM fact_job_run
        WHERE job_name IN ('materialize_intraday_final_actions', 'run_intraday_candidate_collector')
        ORDER BY started_at DESC
        LIMIT 1
        """,
    )
    latest_portfolio_as_of_date = _scalar(
        connection,
        "SELECT MAX(as_of_date) FROM fact_portfolio_target_book",
    )
    latest_portfolio_run_id = _scalar(
        connection,
        """
        SELECT run_id
        FROM fact_portfolio_target_book
        ORDER BY as_of_date DESC, created_at DESC
        LIMIT 1
        """,
    )
    active_intraday_policy_id = _scalar(
        connection,
        """
        SELECT active_policy_id
        FROM fact_intraday_active_policy
        WHERE active_flag = TRUE
          AND effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
        ORDER BY effective_from_date DESC, updated_at DESC
        LIMIT 1
        """,
        [snapshot_effective_date, snapshot_effective_date],
    )
    active_meta_models = connection.execute(
        """
        SELECT horizon, panel_name, active_meta_model_id
        FROM fact_intraday_active_meta_model
        WHERE active_flag = TRUE
          AND effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
        ORDER BY horizon, panel_name
        """,
        [snapshot_effective_date, snapshot_effective_date],
    ).fetchdf()
    active_portfolio_policy_id = _scalar(
        connection,
        """
        SELECT active_portfolio_policy_id
        FROM fact_portfolio_policy_registry
        WHERE active_flag = TRUE
          AND effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
        ORDER BY effective_from_date DESC, updated_at DESC
        LIMIT 1
        """,
        [snapshot_effective_date, snapshot_effective_date],
    )
    active_ops_policy_id = _scalar(
        connection,
        """
        SELECT policy_id
        FROM fact_active_ops_policy
        WHERE active_flag = TRUE
          AND effective_from_at <= ?
          AND (effective_to_at IS NULL OR effective_to_at >= ?)
        ORDER BY effective_from_at DESC, created_at DESC
        LIMIT 1
        """,
        [snapshot_ts, snapshot_ts],
    )
    health_status = _scalar(
        connection,
        """
        SELECT status
        FROM vw_latest_health_snapshot
        WHERE health_scope = 'overall'
          AND component_name = 'platform'
        ORDER BY snapshot_at DESC
        LIMIT 1
        """,
    )
    market_regime_family = _scalar(
        connection,
        """
        SELECT regime_state
        FROM fact_market_regime_snapshot
        ORDER BY as_of_date DESC, created_at DESC
        LIMIT 1
        """,
    )
    latest_report_bundle_id = _scalar(
        connection,
        """
        SELECT run_id
        FROM fact_latest_report_index
        ORDER BY generated_ts DESC, created_at DESC
        LIMIT 1
        """,
    )
    critical_alert_count = int(
        _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM fact_alert_event
            WHERE status = 'OPEN'
              AND severity = 'CRITICAL'
            """,
        )
        or 0
    )
    warning_alert_count = int(
        _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM fact_alert_event
            WHERE status = 'OPEN'
              AND severity IN ('WARNING', 'CRITICAL')
            """,
        )
        or 0
    )
    actionable = connection.execute(
        """
        SELECT
            ranking.symbol,
            symbol_dim.company_name,
            ranking.grade,
            ranking.final_selection_value
        FROM fact_ranking AS ranking
        LEFT JOIN dim_symbol AS symbol_dim
          ON symbol_dim.symbol = ranking.symbol
        WHERE ranking.ranking_version = 'selection_engine_v2'
          AND ranking.as_of_date = (
              SELECT MAX(as_of_date)
              FROM fact_ranking
              WHERE ranking_version = 'selection_engine_v2'
          )
        ORDER BY ranking.final_selection_value DESC, ranking.symbol
        LIMIT 5
        """
    ).fetchdf()
    snapshot_row = {
        "snapshot_id": f"latest-app-snapshot-{snapshot_ts.strftime('%Y%m%dT%H%M%S%f')}",
        "snapshot_ts": snapshot_ts,
        "as_of_date": snapshot_as_of_date,
        "latest_daily_bundle_run_id": daily_bundle[0] if daily_bundle else None,
        "latest_daily_bundle_status": daily_bundle[1] if daily_bundle else None,
        "latest_evaluation_date": latest_evaluation_date,
        "latest_evaluation_run_id": latest_evaluation_run_id,
        "latest_intraday_session_date": latest_intraday_session_date,
        "latest_intraday_run_id": latest_intraday_run_id,
        "latest_portfolio_as_of_date": latest_portfolio_as_of_date,
        "latest_portfolio_run_id": latest_portfolio_run_id,
        "active_intraday_policy_id": active_intraday_policy_id,
        "active_meta_model_ids_json": json_text(active_meta_models.to_dict(orient="records")),
        "active_portfolio_policy_id": active_portfolio_policy_id,
        "active_ops_policy_id": active_ops_policy_id,
        "health_status": health_status,
        "market_regime_family": market_regime_family,
        "top_actionable_symbol_list_json": json_text(actionable.to_dict(orient="records")),
        "latest_report_bundle_id": latest_report_bundle_id,
        "critical_alert_count": critical_alert_count,
        "warning_alert_count": warning_alert_count,
        "notes": "Home/Today current truth snapshot",
        "details_json": json_text(
            {
                "latest_selection_date": latest_selection_date,
                "top_actionable_count": len(actionable),
                "active_meta_model_count": len(active_meta_models),
            }
        ),
        "created_at": snapshot_ts,
    }
    columns = list(snapshot_row.keys())
    connection.execute(
        f"""
        INSERT INTO fact_latest_app_snapshot ({", ".join(columns)})
        VALUES ({", ".join("?" for _ in columns)})
        """,
        [snapshot_row[column] for column in columns],
    )
    execute_postgres_sql(
        settings,
        f"""
        INSERT INTO fact_latest_app_snapshot ({", ".join(columns)})
        VALUES ({", ".join("?" for _ in columns)})
        """,
        [snapshot_row[column] for column in columns],
    )
    return OpsJobResult(
        run_id=job_run_id or snapshot_row["snapshot_id"],
        job_name="build_latest_app_snapshot",
        status=JobStatus.SUCCESS,
        notes=(
            "Latest app snapshot materialized. "
            f"as_of_date={snapshot_as_of_date} "
            f"alerts={critical_alert_count}/{warning_alert_count}"
        ),
        row_count=1,
    )


def build_report_index(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    preview_paths = sorted(settings.paths.artifacts_dir.rglob("*_preview.md"))
    rows: list[dict[str, Any]] = []
    created_at = now_local(settings.app.timezone)
    for preview_path in preview_paths:
        report_type = _resolve_report_type(preview_path)
        run_id = _extract_run_id(preview_path)
        payload_summary = _payload_summary(preview_path)
        rows.append(
            {
                "report_index_id": f"report-index-{_path_hash(preview_path)}",
                "report_type": report_type,
                "report_key": f"{report_type}:{_parse_as_of_date(preview_path) or 'na'}:{run_id or preview_path.parent.name}",
                "as_of_date": _parse_as_of_date(preview_path),
                "generated_ts": datetime.fromtimestamp(preview_path.stat().st_mtime, tz=timezone.utc),
                "status": _resolve_run_status(connection, run_id),
                "run_id": run_id,
                "artifact_path": str(preview_path),
                "artifact_format": "markdown",
                "published_flag": bool(payload_summary.get("published", False)),
                "dry_run_flag": bool(payload_summary.get("dry_run", False)),
                "summary_json": json_text(payload_summary),
                "created_at": created_at,
            }
        )
    if rows:
        columns = list(rows[0].keys())
        connection.executemany(
            f"""
            INSERT OR REPLACE INTO fact_latest_report_index ({", ".join(columns)})
            VALUES ({", ".join("?" for _ in columns)})
            """,
            [[row[column] for column in columns] for row in rows],
        )
        _deduplicate_report_index(connection)
        executemany_postgres_sql(
            settings,
            f"""
            INSERT INTO fact_latest_report_index ({", ".join(columns)})
            VALUES ({", ".join("?" for _ in columns)})
            ON CONFLICT (report_index_id) DO UPDATE SET
                report_type = EXCLUDED.report_type,
                report_key = EXCLUDED.report_key,
                as_of_date = EXCLUDED.as_of_date,
                generated_ts = EXCLUDED.generated_ts,
                status = EXCLUDED.status,
                run_id = EXCLUDED.run_id,
                artifact_path = EXCLUDED.artifact_path,
                artifact_format = EXCLUDED.artifact_format,
                published_flag = EXCLUDED.published_flag,
                dry_run_flag = EXCLUDED.dry_run_flag,
                summary_json = EXCLUDED.summary_json,
                created_at = EXCLUDED.created_at
            """,
            [[row[column] for column in columns] for row in rows],
        )
        execute_postgres_sql(settings, REPORT_INDEX_DEDUP_SQL)
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="build_report_index",
        status=JobStatus.SUCCESS,
        notes=f"Report index materialized. rows={len(rows)}",
        row_count=len(rows),
    )


def build_ui_freshness_snapshot(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    snapshot_ts = now_local(settings.app.timezone)
    rows: list[dict[str, Any]] = []
    for spec in FRESHNESS_SPECS:
        latest_value = _scalar(connection, spec.sql)
        latest_ts = _normalize_timestamp(latest_value)
        freshness_seconds, stale_flag, warning_level = _classify_freshness(
            connection,
            spec=spec,
            latest_ts=latest_ts,
            snapshot_ts=snapshot_ts,
        )
        rows.append(
            {
                "freshness_snapshot_id": (
                    f"freshness-{spec.page_name}-{spec.dataset_name}-"
                    f"{snapshot_ts.strftime('%Y%m%dT%H%M%S%f')}"
                ),
                "snapshot_ts": snapshot_ts,
                "page_name": spec.page_name,
                "dataset_name": spec.dataset_name,
                "latest_available_ts": latest_ts,
                "freshness_seconds": freshness_seconds,
                "stale_flag": stale_flag,
                "warning_level": warning_level,
                "notes": spec.notes,
                "created_at": snapshot_ts,
            }
        )
    columns = list(rows[0].keys())
    connection.executemany(
        f"""
        INSERT INTO fact_ui_data_freshness_snapshot ({", ".join(columns)})
        VALUES ({", ".join("?" for _ in columns)})
        """,
        [[row[column] for column in columns] for row in rows],
    )
    executemany_postgres_sql(
        settings,
        f"""
        INSERT INTO fact_ui_data_freshness_snapshot ({", ".join(columns)})
        VALUES ({", ".join("?" for _ in columns)})
        """,
        [[row[column] for column in columns] for row in rows],
    )
    stale_count = sum(1 for row in rows if row["stale_flag"])
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="build_ui_freshness_snapshot",
        status=JobStatus.SUCCESS if stale_count == 0 else JobStatus.DEGRADED_SUCCESS,
        notes=f"UI freshness snapshot materialized. rows={len(rows)} stale={stale_count}",
        row_count=len(rows),
    )
