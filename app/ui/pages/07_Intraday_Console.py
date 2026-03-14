# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_report_preview,
    render_record_cards,
    render_screen_guide,
    render_warning_banner,
)
from app.ui.helpers import (
    format_ui_time,
    intraday_console_adjusted_decision_frame,
    intraday_console_candidate_frame,
    intraday_console_decision_frame,
    intraday_console_market_context_frame,
    intraday_console_signal_frame,
    intraday_console_strategy_trace_frame,
    intraday_console_timing_frame,
    intraday_console_tuned_action_frame,
    latest_intraday_active_policy_frame,
    latest_intraday_checkpoint_health_frame,
    latest_intraday_console_basis_summary,
    latest_intraday_decision_lineage_frame,
    latest_intraday_meta_active_model_frame,
    latest_intraday_meta_decision_frame,
    latest_intraday_meta_prediction_frame,
    latest_intraday_policy_recommendation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_postmortem_preview,
    latest_intraday_research_capability_frame,
    latest_intraday_status_frame,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_summary_report_preview,
    load_ui_settings,
)


def _safe_int(value: object) -> int:
    if value is None or pd.isna(value):
        return 0
    return int(float(value))


def _safe_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


@st.cache_data(ttl=30, show_spinner=False)
def _load_intraday_console_section(project_root_str: str, section: str) -> dict[str, object]:
    settings = load_ui_settings(Path(project_root_str))
    if section == "한눈에 보기":
        return {
            "status_frame": latest_intraday_status_frame(settings),
            "basis_summary": latest_intraday_console_basis_summary(settings),
            "capability_frame": latest_intraday_research_capability_frame(settings, limit=20),
            "checkpoint_health": latest_intraday_checkpoint_health_frame(settings),
            "market_context": intraday_console_market_context_frame(settings, limit=12),
            "active_policy_frame": latest_intraday_active_policy_frame(settings, limit=20),
            "active_meta_model_frame": latest_intraday_meta_active_model_frame(settings, limit=20),
            "recommendation_frame": latest_intraday_policy_recommendation_frame(settings, limit=20),
        }
    if section == "판단 흐름":
        return {
            "candidate_frame": intraday_console_candidate_frame(settings, limit=20),
            "signal_frame": intraday_console_signal_frame(settings, limit=20),
            "decision_frame": intraday_console_decision_frame(settings, limit=20),
            "adjusted_decision_frame": intraday_console_adjusted_decision_frame(settings, limit=20),
            "meta_prediction_frame": latest_intraday_meta_prediction_frame(settings, limit=20),
            "meta_decision_frame": latest_intraday_meta_decision_frame(settings, limit=20),
            "tuned_decision_frame": intraday_console_tuned_action_frame(settings, limit=20),
        }
    if section == "결과 비교":
        return {
            "timing_frame": intraday_console_timing_frame(settings, limit=20),
            "strategy_trace_frame": intraday_console_strategy_trace_frame(settings, limit=20),
            "same_exit_frame": latest_intraday_strategy_comparison_frame(settings, limit=12),
            "lineage_frame": latest_intraday_decision_lineage_frame(settings, limit=20),
        }
    return {
        "capability_frame": latest_intraday_research_capability_frame(settings, limit=20),
        "active_policy_frame": latest_intraday_active_policy_frame(settings, limit=20),
        "active_meta_model_frame": latest_intraday_meta_active_model_frame(settings, limit=20),
        "recommendation_frame": latest_intraday_policy_recommendation_frame(settings, limit=20),
        "tuned_decision_frame": intraday_console_tuned_action_frame(settings, limit=20),
        "summary_preview": latest_intraday_summary_report_preview(settings),
        "postmortem_preview": latest_intraday_postmortem_preview(settings),
        "policy_preview": latest_intraday_policy_report_preview(settings),
    }


def _latest_checkpoint_text(payload: dict[str, object]) -> str:
    for key in ("checkpoint_health", "market_context"):
        frame = payload.get(key, pd.DataFrame())
        if isinstance(frame, pd.DataFrame) and not frame.empty and "checkpoint_time" in frame.columns:
            values = frame["checkpoint_time"].dropna()
            if not values.empty:
                return format_ui_time(values.astype(str).max())
    return "-"


def _render_overview(payload: dict[str, object]) -> None:
    status_frame = payload["status_frame"]
    basis_summary = payload["basis_summary"]
    capability_frame = payload["capability_frame"]
    checkpoint_health = payload["checkpoint_health"]
    market_context = payload["market_context"]
    active_policy_frame = payload["active_policy_frame"]
    active_meta_model_frame = payload["active_meta_model_frame"]
    recommendation_frame = payload["recommendation_frame"]

    render_narrative_card(
        "표시 기준",
        f"{basis_summary.get('headline', '-')} | {basis_summary.get('label', '-')}. {basis_summary.get('detail', '')}",
    )
    if basis_summary.get("mode") == "historical":
        render_warning_banner("INFO", "현재 장중이 아니라 마지막으로 저장된 장중 세션을 보고 있습니다.")
    elif basis_summary.get("mode") == "stale":
        render_warning_banner("WARNING", "오늘 장중 세션은 열려 있지만 커버리지가 낮아 판단을 보류하는 상태입니다.")
    elif basis_summary.get("mode") == "preopen":
        render_warning_banner("INFO", "장 시작 전이라 오늘 장중 판단 대신 준비 상태와 직전 장 기준을 함께 봐야 합니다.")

    if status_frame.empty:
        render_narrative_card("오늘 상태", "아직 오늘 장중 보조 세션이 없습니다.")
        return

    row = status_frame.iloc[0]
    render_narrative_card(
        "오늘 상태",
        (
            f"오늘 후보는 {_safe_int(row.get('candidate_symbols'))}개, 최종 액션까지 내려온 종목은 "
            f"{_safe_int(row.get('final_action_symbols'))}개입니다. 규칙 기반 1차 판단 "
            f"{_safe_int(row.get('raw_decision_symbols'))}개, 보정 판단 "
            f"{_safe_int(row.get('adjusted_symbols'))}개가 기록됐습니다."
        ),
    )

    metric_a, metric_b, metric_c, metric_d, metric_e = st.columns(5)
    metric_a.metric("후보 종목", _safe_int(row.get("candidate_symbols")))
    metric_b.metric("신호 계산", _safe_int(row.get("signal_symbols")))
    metric_c.metric("최종 액션", _safe_int(row.get("final_action_symbols")))
    metric_d.metric("최근 확인 시각", _latest_checkpoint_text(payload))
    bar_latency = _safe_float(row.get("avg_bar_latency_ms"))
    quote_latency = _safe_float(row.get("avg_quote_latency_ms"))
    metric_e.metric(
        "수집 지연",
        (
            f"bar {bar_latency:.0f}ms / quote {quote_latency:.0f}ms"
            if bar_latency is not None and quote_latency is not None
            else "-"
        ),
    )

    left, right = st.columns(2)
    with left:
        render_record_cards(
            checkpoint_health,
            title="저장 세션 시각별 처리 현황",
            primary_column="checkpoint_time",
            secondary_columns=["status"],
            detail_columns=[
                "candidate_symbols",
                "raw_decision_symbols",
                "adjusted_symbols",
                "final_action_symbols",
            ],
            limit=5,
            empty_message="시각별 현황이 없습니다.",
            table_expander_label="시각별 상세 보기",
        )
    with right:
        render_record_cards(
            market_context,
            title="저장 세션 시장 맥락",
            primary_column="checkpoint_time",
            secondary_columns=["market_session_state"],
            detail_columns=[
                "prior_daily_regime_state",
                "market_breadth_ratio",
                "candidate_mean_signal_quality",
                "data_quality_flag",
            ],
            limit=5,
            empty_message="시장 맥락 데이터가 없습니다.",
            table_expander_label="시장 맥락 상세 보기",
        )

    policy_col, meta_col, rec_col = st.columns(3)
    with policy_col:
        render_record_cards(
            active_policy_frame,
            title="현재 적용 정책",
            primary_column="template_id",
            secondary_columns=["scope_type"],
            detail_columns=["horizon", "effective_from_date", "note"],
            limit=3,
            empty_message="활성 정책이 없습니다.",
            table_expander_label="정책 상세 보기",
        )
    with meta_col:
        render_record_cards(
            active_meta_model_frame,
            title="현재 메타 모델",
            primary_column="model_version",
            secondary_columns=["panel_name"],
            detail_columns=["horizon", "effective_from_date", "note"],
            limit=4,
            empty_message="활성 메타 모델이 없습니다.",
            table_expander_label="메타 모델 상세 보기",
        )
    with rec_col:
        render_record_cards(
            recommendation_frame,
            title="최근 정책 추천",
            primary_column="recommended_policy_template",
            secondary_columns=["scope_type"],
            detail_columns=["horizon", "objective_score"],
            limit=4,
            empty_message="최근 정책 추천이 없습니다.",
            table_expander_label="정책 추천 상세 보기",
        )


def _render_flow(payload: dict[str, object]) -> None:
    candidate_frame = payload["candidate_frame"]
    signal_frame = payload["signal_frame"]
    decision_frame = payload["decision_frame"]
    adjusted_decision_frame = payload["adjusted_decision_frame"]
    meta_prediction_frame = payload["meta_prediction_frame"]
    meta_decision_frame = payload["meta_decision_frame"]
    tuned_decision_frame = payload["tuned_decision_frame"]

    render_narrative_card(
        "읽는 순서",
        "후보 종목 -> 장중 신호 -> 1차 판단 -> 보정 판단 -> 메타 예측 -> 최종 액션 순서로 보면 됩니다. "
        "핵심만 보려면 마지막 두 블록만 확인해도 충분합니다.",
    )

    sub = st.segmented_control(
        "판단 흐름 단계",
        options=["후보", "신호", "1차 판단", "보정 판단", "메타 예측", "최종 액션"],
        default="최종 액션",
    )

    if sub == "후보":
        render_narrative_card(
            "후보 종목이란?",
            "전일 추천 결과에서 오늘 장중에 다시 볼 종목들입니다. 여기서 오늘 볼 대상을 정합니다.",
        )
        render_record_cards(
            candidate_frame,
            title="오늘 후보 종목",
            primary_column="symbol",
            secondary_columns=["company_name", "grade"],
            detail_columns=["horizon", "candidate_rank", "final_selection_value", "expected_excess_return"],
            limit=8,
            empty_message="후보 종목이 없습니다.",
            table_expander_label="후보 종목 원본 보기",
        )
    elif sub == "신호":
        render_narrative_card(
            "장중 신호란?",
            "호가, 체결, 활동성, 변동성 같은 장중 데이터로 지금 타이밍이 좋은지 점수화한 값입니다.",
        )
        render_record_cards(
            signal_frame,
            title="최근 확인 시각 신호",
            primary_column="symbol",
            secondary_columns=["checkpoint_time", "horizon"],
            detail_columns=[
                "signal_quality_score",
                "timing_adjustment_score",
                "risk_friction_score",
                "relative_activity_score",
            ],
            limit=8,
            empty_message="장중 신호가 없습니다.",
            table_expander_label="장중 신호 원본 보기",
        )
    elif sub == "1차 판단":
        render_narrative_card(
            "1차 판단이란?",
            "정책과 메타모델을 아직 쓰지 않은 규칙 기반 초안입니다.",
        )
        render_record_cards(
            decision_frame,
            title="규칙 기반 1차 판단",
            primary_column="symbol",
            secondary_columns=["company_name", "action"],
            detail_columns=["checkpoint_time", "horizon", "action_score", "signal_quality_score"],
            limit=8,
            empty_message="1차 판단 기록이 없습니다.",
            table_expander_label="1차 판단 원본 보기",
        )
    elif sub == "보정 판단":
        render_narrative_card(
            "보정 판단이란?",
            "시장 상태와 리스크를 반영해서 1차 판단을 조정한 단계입니다.",
        )
        render_record_cards(
            adjusted_decision_frame,
            title="시장 상황 반영 후 판단",
            primary_column="symbol",
            secondary_columns=["company_name", "adjusted_action"],
            detail_columns=[
                "checkpoint_time",
                "market_regime_family",
                "raw_action",
                "adjusted_timing_score",
                "fallback_flag",
            ],
            limit=8,
            empty_message="보정 판단 기록이 없습니다.",
            table_expander_label="보정 판단 원본 보기",
        )
    elif sub == "메타 예측":
        render_narrative_card(
            "메타 예측이란?",
            "메타모델이 최종 액션에 얼마나 자신이 있는지, 판단이 얼마나 엇갈리는지 보여주는 단계입니다.",
        )
        render_record_cards(
            meta_prediction_frame,
            title="메타모델 예측",
            primary_column="symbol",
            secondary_columns=["company_name", "predicted_class"],
            detail_columns=[
                "checkpoint_time",
                "predicted_class_probability",
                "confidence_margin",
                "uncertainty_score",
                "disagreement_score",
            ],
            limit=8,
            empty_message="메타 예측 기록이 없습니다.",
            table_expander_label="메타 예측 원본 보기",
        )
    else:
        final_frame = meta_decision_frame if not meta_decision_frame.empty else tuned_decision_frame
        render_narrative_card(
            "최종 액션이란?",
            "오늘 화면에서 가장 먼저 봐야 하는 결론입니다. 실제로는 이 블록이 Enter / Wait / Avoid 결론입니다.",
        )
        render_record_cards(
            final_frame,
            title="오늘 최종 액션",
            primary_column="symbol",
            secondary_columns=["company_name", "final_action" if "final_action" in final_frame.columns else "tuned_action"],
            detail_columns=[
                "checkpoint_time",
                "raw_action",
                "adjusted_action",
                "tuned_action",
                "final_action",
                "confidence_margin",
                "fallback_flag",
            ],
            limit=8,
            empty_message="최종 액션 기록이 없습니다.",
            table_expander_label="최종 액션 원본 보기",
        )


def _render_results(payload: dict[str, object]) -> None:
    timing_frame = payload["timing_frame"]
    strategy_trace_frame = payload["strategy_trace_frame"]
    same_exit_frame = payload["same_exit_frame"]
    lineage_frame = payload["lineage_frame"]

    sub = st.segmented_control(
        "결과 비교 종류",
        options=["시점 결과", "전략 추적", "비교 요약", "판단 흐름"],
        default="시점 결과",
    )

    if sub == "시점 결과":
        render_record_cards(
            timing_frame,
            title="확인 시각별 결과",
            primary_column="symbol",
            secondary_columns=["session_date", "selected_checkpoint_time"],
            detail_columns=[
                "horizon",
                "selected_action",
                "timing_edge_bps",
                "realized_return_from_open",
                "outcome_status",
            ],
            limit=8,
            empty_message="시점 결과가 없습니다.",
            table_expander_label="시점 결과 원본 보기",
        )
    elif sub == "전략 추적":
        render_record_cards(
            strategy_trace_frame,
            title="전략 추적",
            primary_column="symbol",
            secondary_columns=["company_name", "strategy_id"],
            detail_columns=["horizon", "executed_flag", "timing_edge_vs_open_bps", "outcome_status"],
            limit=8,
            empty_message="전략 추적 이력이 없습니다.",
            table_expander_label="전략 추적 원본 보기",
        )
    elif sub == "비교 요약":
        render_record_cards(
            same_exit_frame,
            title="같은 종료 기준 비교",
            primary_column="strategy_id",
            secondary_columns=["horizon"],
            detail_columns=[
                "executed_count",
                "execution_rate",
                "mean_realized_excess_return",
                "mean_timing_edge_vs_open_bps",
            ],
            limit=8,
            empty_message="비교 요약이 없습니다.",
            table_expander_label="비교 요약 원본 보기",
        )
    else:
        render_record_cards(
            lineage_frame,
            title="판단 흐름",
            primary_column="symbol",
            secondary_columns=["company_name", "selection_date"],
            detail_columns=[
                "checkpoint_time",
                "raw_action",
                "adjusted_action",
                "final_action",
                "portfolio_execution_mode",
                "gate_status",
            ],
            limit=8,
            empty_message="판단 흐름 이력이 없습니다.",
            table_expander_label="판단 흐름 원본 보기",
        )


def _render_policy(payload: dict[str, object]) -> None:
    capability_frame = payload["capability_frame"]
    active_policy_frame = payload["active_policy_frame"]
    active_meta_model_frame = payload["active_meta_model_frame"]
    recommendation_frame = payload["recommendation_frame"]
    tuned_decision_frame = payload["tuned_decision_frame"]
    summary_preview = payload["summary_preview"]
    postmortem_preview = payload["postmortem_preview"]
    policy_preview = payload["policy_preview"]

    top_left, top_right = st.columns(2)
    with top_left:
        render_record_cards(
            capability_frame,
            title="기능 가동 상태",
            primary_column="feature_slug",
            secondary_columns=["rollout_mode"],
            detail_columns=["dependency_ready_flag", "report_available_flag", "last_skip_reason"],
            limit=8,
            empty_message="기능 가동 상태가 없습니다.",
            table_expander_label="기능 상태 원본 보기",
        )
    with top_right:
        render_record_cards(
            active_policy_frame,
            title="정책 적용 범위",
            primary_column="template_id",
            secondary_columns=["scope_type"],
            detail_columns=["horizon", "scope_key", "effective_from_date", "note"],
            limit=8,
            empty_message="정책 적용 정보가 없습니다.",
            table_expander_label="정책 적용 원본 보기",
        )

    bottom_left, bottom_mid, bottom_right = st.columns(3)
    with bottom_left:
        render_record_cards(
            recommendation_frame,
            title="최근 정책 추천",
            primary_column="recommended_policy_template",
            secondary_columns=["scope_type"],
            detail_columns=["horizon", "objective_score"],
            limit=5,
            empty_message="정책 추천 결과가 없습니다.",
            table_expander_label="정책 추천 원본 보기",
        )
    with bottom_mid:
        render_record_cards(
            active_meta_model_frame,
            title="메타 모델 현황",
            primary_column="model_version",
            secondary_columns=["panel_name"],
            detail_columns=["horizon", "effective_from_date", "note"],
            limit=6,
            empty_message="활성 메타 모델이 없습니다.",
            table_expander_label="메타 모델 원본 보기",
        )
    with bottom_right:
        render_record_cards(
            tuned_decision_frame,
            title="정책 튜닝 결과",
            primary_column="symbol",
            secondary_columns=["company_name", "tuned_action"],
            detail_columns=["checkpoint_time", "adjusted_action", "tuned_score", "fallback_used_flag"],
            limit=6,
            empty_message="정책 튜닝 결과가 없습니다.",
            table_expander_label="정책 튜닝 원본 보기",
        )

    if summary_preview:
        with st.expander("장중 요약 리포트 미리보기", expanded=False):
            render_report_preview(title="장중 요약 리포트", preview=summary_preview)

    if policy_preview:
        with st.expander("장중 정책 연구 리포트 미리보기", expanded=False):
            render_report_preview(title="장중 정책 연구 리포트", preview=policy_preview)

    if postmortem_preview:
        with st.expander("장중 사후 분석 리포트 미리보기", expanded=False):
            render_report_preview(title="장중 사후 분석 리포트", preview=postmortem_preview)


render_page_header(
    load_ui_settings(PROJECT_ROOT),
    page_name="장중 콘솔",
    title="장중 콘솔",
    description="장중 보조 판단이 오늘 어떻게 흘러가는지, 최종 액션이 무엇인지 한눈에 보는 화면입니다.",
)
render_screen_guide(
    summary="기본 진입은 `한눈에 보기`만 불러옵니다. 필요한 섹션만 선택해서 보는 방식이라 이전보다 훨씬 가볍습니다.",
    bullets=[
        "한눈에 보기: 오늘 결론 요약",
        "판단 흐름: 후보 -> 신호 -> 최종 액션",
        "결과 비교: 사후 결과와 전략 비교",
        "정책·리포트: 운영용 상세 정보",
    ],
)
render_warning_banner(
    "INFO",
    "이 화면은 주문 화면이 아니라 해석용 콘솔입니다. 최종 액션은 참고 신호이며 자동 주문으로 이어지지 않습니다.",
)

section = st.segmented_control(
    "콘솔 보기",
    options=["한눈에 보기", "판단 흐름", "결과 비교", "정책·리포트"],
    default="한눈에 보기",
)

with st.spinner("장중 콘솔 데이터를 불러오는 중..."):
    payload = _load_intraday_console_section(PROJECT_ROOT.as_posix(), section)

if section == "한눈에 보기":
    _render_overview(payload)
elif section == "판단 흐름":
    _render_flow(payload)
elif section == "결과 비교":
    _render_results(payload)
else:
    _render_policy(payload)

render_page_footer(load_ui_settings(PROJECT_ROOT), page_name="장중 콘솔")
