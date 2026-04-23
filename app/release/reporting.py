# ruff: noqa: E501

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import duckdb

from app.ml.constants import MODEL_SPEC_ID
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ops.common import JobStatus, OpsJobResult
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables

EXECUTION_MODE_LABELS = {
    "OPEN_ALL": "시가 일괄 진입",
    "TIMING_ASSISTED": "장중 보조 진입",
}

MODEL_SPEC_LABELS = {
    "alpha_recursive_expanding_v1": "확장형 누적 학습",
    "alpha_rolling_120_v1": "최근 120거래일 중심 학습",
    "alpha_rolling_250_v1": "최근 250거래일 중심 학습",
    "alpha_rank_rolling_120_v1": "5일 지속성 비교 기준",
    "alpha_topbucket_h1_rolling_120_v1": "하루 선행 비교 기준",
    "alpha_lead_d1_v1": "하루 선행 포착 v1",
    "alpha_swing_d5_v1": "5일 지속 포착 v1",
}


@dataclass(frozen=True, slots=True)
class RenderedReport:
    artifact_paths: list[str]
    content: str
    payload: dict[str, Any]


def _frame(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    params: list[object] | None = None,
):
    return connection.execute(query, params or []).fetchdf()


def _payload_messages(content: str) -> list[dict[str, str]]:
    chunk_size = 1800
    return [
        {"content": content[index : index + chunk_size]}
        for index in range(0, len(content), chunk_size)
    ]


def _json_object_value(payload: str | None, key: str) -> float | None:
    if not payload:
        return None


def _translate_model_spec(model_spec_id: object) -> str:
    text = str(model_spec_id or "-")
    return MODEL_SPEC_LABELS.get(text, text)
    try:
        loaded = json.loads(payload)
    except json.JSONDecodeError:
        return None
    value = loaded.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _write_report_artifacts(
    settings: Settings,
    *,
    folder_name: str,
    partition_key: str,
    preview_name: str,
    payload_name: str,
    run_id: str,
    content: str,
    dry_run: bool,
    extra_payload: dict[str, Any] | None = None,
) -> RenderedReport:
    artifact_dir = settings.paths.artifacts_dir / folder_name / partition_key / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    preview_path = artifact_dir / preview_name
    preview_path.write_text(content, encoding="utf-8")
    payload = {
        "username": settings.discord.username,
        "dry_run": dry_run,
        "message_count": max(1, (len(content) + 1799) // 1800),
        "messages": _payload_messages(content),
    }
    if extra_payload:
        payload.update(extra_payload)
    payload_path = artifact_dir / payload_name
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return RenderedReport(
        artifact_paths=[str(preview_path), str(payload_path)],
        content=content,
        payload=payload,
    )


def render_daily_research_report(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    regime = _frame(
        connection,
        """
        SELECT as_of_date, regime_state, breadth_up_ratio, market_realized_vol_20d
        FROM fact_market_regime_snapshot
        WHERE as_of_date <= ?
        ORDER BY as_of_date DESC, created_at DESC
        LIMIT 1
        """,
        [as_of_date],
    )
    leaderboard = _frame(
        connection,
        """
        SELECT
            ranking.symbol,
            symbol_dim.company_name,
            ranking.grade,
            ranking.final_selection_value,
            ranking.explanatory_score_json,
            prediction.expected_excess_return,
            prediction.uncertainty_score,
            prediction.disagreement_score
        FROM fact_ranking AS ranking
        LEFT JOIN dim_symbol AS symbol_dim
          ON symbol_dim.symbol = ranking.symbol
        LEFT JOIN fact_prediction AS prediction
          ON prediction.as_of_date = ranking.as_of_date
         AND prediction.symbol = ranking.symbol
         AND prediction.horizon = ranking.horizon
         AND prediction.prediction_version = 'alpha_prediction_v1'
        WHERE ranking.ranking_version = ?
          AND ranking.horizon = 5
          AND ranking.as_of_date = (
              SELECT MAX(as_of_date)
              FROM fact_ranking
              WHERE ranking_version = ?
                AND as_of_date <= ?
          )
        ORDER BY ranking.final_selection_value DESC, ranking.symbol
        LIMIT 10
        """,
        [SELECTION_ENGINE_V2_VERSION, SELECTION_ENGINE_V2_VERSION, as_of_date],
    )
    news = _frame(
        connection,
        """
        SELECT signal_date, title, publisher, query_bucket
        FROM fact_news_item
        WHERE signal_date <= ?
        ORDER BY published_at DESC
        LIMIT 8
        """,
        [as_of_date],
    )
    portfolio = _frame(
        connection,
        """
        SELECT execution_mode, symbol, company_name, target_weight, gate_status
        FROM fact_portfolio_target_book
        WHERE as_of_date = (
            SELECT MAX(as_of_date)
            FROM fact_portfolio_target_book
            WHERE as_of_date <= ?
        )
        ORDER BY execution_mode, target_rank
        LIMIT 10
        """,
        [as_of_date],
    )
    lines = ["# 오늘 리서치 요약", "", f"- 기준일: {as_of_date.isoformat()}", ""]
    lines.append("## 한눈에 보기")
    if regime.empty:
        lines.append("- 오늘 시장 흐름 자료가 아직 없습니다.")
    else:
        row = regime.iloc[0]
        lines.append(
            "- 시장 흐름은 {state}이며, 상승 종목 비율은 {breadth:.1%}, 최근 변동성 참고치는 {vol:.2%}입니다.".format(
                state=row["regime_state"],
                breadth=float(row["breadth_up_ratio"] or 0.0),
                vol=float(row["market_realized_vol_20d"] or 0.0),
            )
        )
    lines.append("")
    lines.append("## 오늘 주목할 종목")
    if leaderboard.empty:
        lines.append("- 오늘 주목할 종목이 아직 없습니다.")
    else:
        for row in leaderboard.itertuples(index=False):
            flow_score = _json_object_value(row.explanatory_score_json, "flow_score")
            implementation_penalty = _json_object_value(
                row.explanatory_score_json,
                "implementation_penalty_score",
            )
            lines.append(
                f"- {row.symbol} {row.company_name or ''} | 등급 {row.grade} | 종합점수 {float(row.final_selection_value or 0.0):+.3f} "
                f"| 참고 기대수익 {float(row.expected_excess_return or 0.0):+.2%} "
                f"| 수급 점수 {float(flow_score or 0.0):+.2f} "
                f"| 예측 흔들림 {float(row.uncertainty_score or 0.0):.2f} "
                f"| 모델 의견 갈림 {float(row.disagreement_score or 0.0):.2f} "
                f"| 실행 부담 {float(implementation_penalty or 0.0):.2f}"
            )
    lines.append("")
    lines.append("## 포트폴리오 관점")
    if portfolio.empty:
        lines.append("- 현재 연결된 목표 보유안이 없습니다.")
    else:
        for row in portfolio.itertuples(index=False):
            lines.append(
                f"- {row.symbol}: 목표 비중 {float(row.target_weight or 0.0):.2%} | 진입 방식 {EXECUTION_MODE_LABELS.get(str(row.execution_mode), row.execution_mode)} | 진입 판단 {row.gate_status}"
            )
    lines.append("")
    lines.append("## 오늘 본 뉴스")
    if news.empty:
        lines.append("- 최근 뉴스 요약이 아직 없습니다.")
    else:
        for row in news.itertuples(index=False):
            lines.append(f"- {row.signal_date} | {row.title} ({row.publisher})")
    rendered = _write_report_artifacts(
        settings,
        folder_name="daily_research_report",
        partition_key=f"as_of_date={as_of_date.isoformat()}",
        preview_name="daily_research_report_preview.md",
        payload_name="daily_research_report_payload.json",
        run_id=job_run_id or "embedded",
        content="\n".join(lines),
        dry_run=dry_run,
        extra_payload={"report_type": "daily_research_report"},
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_daily_research_report",
        status=JobStatus.SUCCESS,
        notes=f"Daily research report rendered. dry_run={dry_run}",
        artifact_paths=rendered.artifact_paths,
    )


def render_evaluation_report(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    summary = _frame(
        connection,
        """
        SELECT summary_date, window_type, horizon, ranking_version, segment_type, segment_value,
               count_evaluated, mean_realized_excess_return, hit_rate, band_coverage_rate
        FROM fact_evaluation_summary
        WHERE summary_date <= ?
        ORDER BY summary_date DESC, horizon, ranking_version
        LIMIT 16
        """,
        [as_of_date],
    )
    calibration = _frame(
        connection,
        """
        SELECT diagnostic_date, horizon, bin_type, bin_value, sample_count, coverage_rate, median_bias
        FROM fact_calibration_diagnostic
        WHERE diagnostic_date <= ?
        ORDER BY diagnostic_date DESC, horizon, bin_value
        LIMIT 16
        """,
        [as_of_date],
    )
    lines = ["# 사후 평가 요약", "", f"- 기준일: {as_of_date.isoformat()}", ""]
    lines.append("## 최근 확정된 추천 결과")
    if summary.empty:
        lines.append("- 아직 확정된 평가 요약이 없습니다.")
    else:
        for row in summary.itertuples(index=False):
            model_label = (
                "현재 추천 모델"
                if str(row.ranking_version) == SELECTION_ENGINE_V2_VERSION
                else "비교 기준 모델"
            )
            lines.append(
                f"- {row.summary_date} | {int(row.horizon)}거래일 | {model_label} "
                f"| 평가 수 {int(row.count_evaluated or 0)} "
                f"| 평균 초과수익률 {float(row.mean_realized_excess_return or 0.0):+.3%} "
                f"| 수익 플러스 비율 {float(row.hit_rate or 0.0):+.2%} "
                f"| 예상 범위 적중률 {float(row.band_coverage_rate or 0.0):.2%}"
            )
    lines.append("")
    lines.append("## 예상 범위 점검")
    if calibration.empty:
        lines.append("- 예상 범위 점검 자료가 아직 없습니다.")
    else:
        for row in calibration.itertuples(index=False):
            lines.append(
                f"- {row.diagnostic_date} | {int(row.horizon)}거래일 | 구간 {row.bin_value} "
                f"| 표본 수 {int(row.sample_count or 0)} "
                f"| 적중률 {float(row.coverage_rate or 0.0):.2%} "
                f"| 치우침 {float(row.median_bias or 0.0):+.3%}"
            )
    rendered = _write_report_artifacts(
        settings,
        folder_name="evaluation_report",
        partition_key=f"as_of_date={as_of_date.isoformat()}",
        preview_name="evaluation_report_preview.md",
        payload_name="evaluation_report_payload.json",
        run_id=job_run_id or "embedded",
        content="\n".join(lines),
        dry_run=dry_run,
        extra_payload={"report_type": "evaluation_report"},
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_evaluation_report",
        status=JobStatus.SUCCESS,
        notes=f"Evaluation report rendered. dry_run={dry_run}",
        artifact_paths=rendered.artifact_paths,
    )


def render_intraday_summary_report(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    session_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    decisions = _frame(
        connection,
        """
        SELECT checkpoint_time, adjusted_action, final_action, COUNT(*) AS row_count
        FROM fact_intraday_meta_decision
        WHERE session_date = ?
        GROUP BY checkpoint_time, adjusted_action, final_action
        ORDER BY checkpoint_time, adjusted_action, final_action
        """,
        [session_date],
    )
    timing = _frame(
        connection,
        """
        SELECT
            end_session_date,
            strategy_id,
            horizon,
            executed_count,
            execution_rate,
            mean_realized_excess_return,
            mean_timing_edge_vs_open_bps
        FROM fact_intraday_strategy_comparison
        WHERE end_session_date = (
            SELECT MAX(end_session_date)
            FROM fact_intraday_strategy_comparison
            WHERE end_session_date <= ?
        )
        ORDER BY strategy_id, horizon
        LIMIT 20
        """,
        [session_date],
    )
    lines = ["# 장중 요약", "", f"- 세션일: {session_date.isoformat()}", ""]
    lines.append("## 체크포인트별 최종 판단")
    if decisions.empty:
        lines.append("- 장중 최종 판단 기록이 없습니다.")
    else:
        for row in decisions.itertuples(index=False):
            lines.append(
                f"- {row.checkpoint_time} | 보정 후 판단 {row.adjusted_action} | 최종 판단 {row.final_action} | 해당 종목 수 {int(row.row_count)}"
            )
    lines.append("")
    lines.append("## 장중 시점 비교")
    if timing.empty:
        lines.append("- 장중 시점 비교 자료가 없습니다.")
    else:
        for row in timing.itertuples(index=False):
            lines.append(
                f"- {row.end_session_date} | {row.strategy_id} | {int(row.horizon)}거래일 "
                f"| 실행 수 {int(row.executed_count or 0)} "
                f"| 실행 비율 {float(row.execution_rate or 0.0):.2%} "
                f"| 평균 초과수익률 {float(row.mean_realized_excess_return or 0.0):+.3%} "
                f"| 시가 대비 유불리 {float(row.mean_timing_edge_vs_open_bps or 0.0):+.1f}bp"
            )
    rendered = _write_report_artifacts(
        settings,
        folder_name="intraday_summary_report",
        partition_key=f"session_date={session_date.isoformat()}",
        preview_name="intraday_summary_report_preview.md",
        payload_name="intraday_summary_report_payload.json",
        run_id=job_run_id or "embedded",
        content="\n".join(lines),
        dry_run=dry_run,
        extra_payload={"report_type": "intraday_summary_report"},
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_intraday_summary_report",
        status=JobStatus.SUCCESS,
        notes=f"Intraday summary report rendered. dry_run={dry_run}",
        artifact_paths=rendered.artifact_paths,
    )


def render_release_candidate_checklist(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    checks = _frame(
        connection,
        """
        SELECT check_name, status, severity, recommended_action, check_ts
        FROM vw_latest_release_candidate_check
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'WARNING' THEN 2
                ELSE 3
            END,
            check_name
        """
    )
    active_models = _frame(
        connection,
        """
        SELECT
            horizon,
            model_spec_id,
            active_alpha_model_id,
            effective_from_date,
            promotion_type
        FROM fact_alpha_active_model
        WHERE effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
          AND active_flag = TRUE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY horizon
            ORDER BY effective_from_date DESC, created_at DESC, active_alpha_model_id DESC
        ) = 1
        ORDER BY horizon
        """,
        [as_of_date, as_of_date],
    )
    gap_rows = _frame(
        connection,
        """
        SELECT
            summary_date,
            window_name,
            horizon,
            model_spec_id,
            insufficient_history_flag,
            matured_selection_date_count,
            required_selection_date_count,
            selected_top5_mean_realized_excess_return,
            drag_vs_raw_top5
        FROM fact_alpha_shadow_selection_gap_scorecard
        WHERE summary_date = (
            SELECT MAX(summary_date)
            FROM fact_alpha_shadow_selection_gap_scorecard
            WHERE summary_date <= ?
        )
          AND window_name = 'rolling_20'
          AND segment_name = 'top5'
        ORDER BY horizon, model_spec_id
        """,
        [as_of_date],
    )
    lines = ["# Release Candidate Checklist", "", f"- as_of_date: {as_of_date.isoformat()}", ""]
    lines.append("## Checks")
    if checks.empty:
        lines.append("- no release-candidate checks have been materialized")
    else:
        for row in checks.itertuples(index=False):
            lines.append(
                f"- [{row.severity}] {row.check_name}: {row.status} "
                f"action={row.recommended_action or 'none'}"
            )
    lines.append("")
    lines.append("## Discord Bot Snapshot")
    lines.append("- Discord bot snapshot is managed in metadata Postgres, not in main DuckDB.")
    lines.append("")
    lines.append("## Alpha serving baseline")
    if active_models.empty:
        lines.append("- active serving spec 정보가 아직 없습니다.")
    else:
        for row in active_models.itertuples(index=False):
            lines.append(
                f"- D+{int(row.horizon)} | active serving spec {_translate_model_spec(row.model_spec_id)} "
                f"| active_alpha_model_id={row.active_alpha_model_id} "
                f"| effective_from={row.effective_from_date} "
                f"| fallback baseline={_translate_model_spec(MODEL_SPEC_ID)}"
            )
    lines.append("")
    lines.append("## Selection gap gate")
    if gap_rows.empty:
        lines.append("- selection gap scorecard가 아직 없습니다.")
    else:
        for row in gap_rows.itertuples(index=False):
            if bool(row.insufficient_history_flag):
                lines.append(
                    f"- D+{int(row.horizon)} | {_translate_model_spec(row.model_spec_id)} | "
                    f"insufficient history {int(row.matured_selection_date_count or 0)}/"
                    f"{int(row.required_selection_date_count or 0)}"
                )
            else:
                lines.append(
                    f"- D+{int(row.horizon)} | {_translate_model_spec(row.model_spec_id)} | "
                    f"selected_top5={float(row.selected_top5_mean_realized_excess_return or 0.0):+.2%} "
                    f"| drag_vs_raw={float(row.drag_vs_raw_top5 or 0.0):+.2%}"
                )
    rendered = _write_report_artifacts(
        settings,
        folder_name="release_candidate_checklist",
        partition_key=f"as_of_date={as_of_date.isoformat()}",
        preview_name="release_candidate_checklist_preview.md",
        payload_name="release_candidate_checklist_payload.json",
        run_id=job_run_id or "embedded",
        content="\n".join(lines),
        dry_run=dry_run,
        extra_payload={"report_type": "release_candidate_checklist"},
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_release_candidate_checklist",
        status=JobStatus.SUCCESS,
        notes=f"Release candidate checklist rendered. dry_run={dry_run}",
        artifact_paths=rendered.artifact_paths,
    )
