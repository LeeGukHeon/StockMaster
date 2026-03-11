# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

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

settings = load_ui_settings(PROJECT_ROOT)

status_frame = latest_intraday_status_frame(settings)
capability_frame = latest_intraday_research_capability_frame(settings, limit=20)
checkpoint_health = latest_intraday_checkpoint_health_frame(settings)
candidate_frame = intraday_console_candidate_frame(settings, limit=40)
market_context = intraday_console_market_context_frame(settings, limit=10)
signal_frame = intraday_console_signal_frame(settings, limit=40)
decision_frame = intraday_console_decision_frame(settings, limit=40)
adjusted_decision_frame = intraday_console_adjusted_decision_frame(settings, limit=40)
tuned_decision_frame = intraday_console_tuned_action_frame(settings, limit=40)
meta_prediction_frame = latest_intraday_meta_prediction_frame(settings, limit=40)
meta_decision_frame = latest_intraday_meta_decision_frame(settings, limit=40)
active_policy_frame = latest_intraday_active_policy_frame(settings, limit=20)
active_meta_model_frame = latest_intraday_meta_active_model_frame(settings, limit=20)
recommendation_frame = latest_intraday_policy_recommendation_frame(settings, limit=20)
strategy_trace_frame = intraday_console_strategy_trace_frame(settings, limit=50)
timing_frame = intraday_console_timing_frame(settings, limit=30)
lineage_frame = latest_intraday_decision_lineage_frame(settings, limit=40)
same_exit_frame = latest_intraday_strategy_comparison_frame(settings, limit=20)
summary_preview = latest_intraday_summary_report_preview(settings)
postmortem_preview = latest_intraday_postmortem_preview(settings)
policy_preview = latest_intraday_policy_report_preview(settings)

render_page_header(
    settings,
    page_name="장중 콘솔",
    title="장중 콘솔",
    description="원정책, 조정정책, 메타 오버레이, 최종 액션을 연구용 기준으로 한 화면에서 확인합니다.",
)
render_screen_guide(
    summary="장중에 추천 종목을 어떻게 다시 걸러냈는지 보는 연구용 화면입니다. 실제 주문 화면이 아니라, 시간이 지나며 판단이 어떻게 바뀌는지를 확인하는 콘솔입니다.",
    bullets=[
        "세션 요약과 후보 종목을 먼저 보면 오늘 장중 판단의 큰 흐름을 이해하기 쉽습니다.",
        "원래 판단 → 보정 후 판단 → 최종 판단 순서로 내려가며 보면 왜 보수적으로 바뀌었는지 파악할 수 있습니다.",
        "수치가 많아 보이면 최종 판단과 시점 결과만 먼저 보세요.",
    ],
)
render_warning_banner(
    "INFO",
    "이 화면은 연구용 비매매 출력입니다. 자동 주문, 자동 체결, 자동 승격은 하지 않습니다.",
)

if status_frame.empty:
    render_narrative_card(
        "장중 연구 상태",
        "아직 장중 세션 데이터가 없습니다. 후보 세션 생성과 장중 보조 번들 실행 여부를 먼저 확인해 주세요.",
    )
else:
    row = status_frame.iloc[0]
    render_narrative_card(
        "장중 연구 상태",
        (
            f"최신 세션은 {row.get('session_date', '-')}, 후보 {row.get('candidate_symbols', '-')}종목, "
            f"원정책 {row.get('raw_decision_symbols', '-')}, 조정정책 {row.get('adjusted_symbols', '-')}, "
            f"최종 액션 {row.get('final_action_symbols', '-')}종목입니다."
        ),
    )

render_record_cards(
    status_frame,
    title="세션 요약",
    primary_column="session_date",
    detail_columns=[
        "candidate_symbols",
        "raw_decision_symbols",
        "adjusted_symbols",
        "final_action_symbols",
        "latest_checkpoint_time",
    ],
    limit=3,
    empty_message="세션 요약이 없습니다.",
    table_expander_label="세션 요약 원본 표 보기",
)

render_record_cards(
    capability_frame,
    title="연구 기능 상태",
    primary_column="feature_slug",
    secondary_columns=["rollout_mode"],
    detail_columns=["dependency_ready_flag", "report_available_flag", "last_skip_reason"],
    limit=8,
    empty_message="연구 기능 상태가 없습니다.",
    table_expander_label="기능 상태 원본 표 보기",
)

render_record_cards(
    market_context,
    title="시장 맥락",
    primary_column="checkpoint_time",
    secondary_columns=["market_session_state"],
    detail_columns=["prior_daily_regime_state", "market_breadth_ratio", "data_quality_flag"],
    limit=8,
    empty_message="시장 맥락이 없습니다.",
    table_expander_label="시장 맥락 원본 표 보기",
)

render_record_cards(
    checkpoint_health,
    title="체크포인트 상태",
    primary_column="checkpoint_time",
    secondary_columns=["status"],
    detail_columns=[
        "candidate_symbols",
        "raw_decision_symbols",
        "adjusted_symbols",
        "final_action_symbols",
    ],
    limit=8,
    empty_message="체크포인트 상태가 없습니다.",
    table_expander_label="체크포인트 원본 표 보기",
)

render_record_cards(
    candidate_frame,
    title="후보 종목",
    primary_column="symbol",
    secondary_columns=["company_name", "grade"],
    detail_columns=["selection_date", "horizon", "candidate_rank", "expected_excess_return"],
    limit=8,
    empty_message="후보 종목이 없습니다.",
    table_expander_label="후보 종목 원본 표 보기",
)

render_record_cards(
    signal_frame,
    title="장중 신호",
    primary_column="symbol",
    secondary_columns=["checkpoint_time", "horizon"],
    detail_columns=["signal_quality_score", "timing_adjustment_score", "risk_friction_score"],
    limit=8,
    empty_message="장중 신호가 없습니다.",
    table_expander_label="신호 원본 표 보기",
)

render_record_cards(
    decision_frame,
    title="처음 판단",
    primary_column="symbol",
    secondary_columns=["company_name", "checkpoint_time"],
    detail_columns=["horizon", "action", "action_score", "signal_quality_score"],
    limit=8,
    empty_message="처음 판단 기록이 없습니다.",
    table_expander_label="처음 판단 원본 표 보기",
)

render_record_cards(
    adjusted_decision_frame,
    title="보정 후 판단",
    primary_column="symbol",
    secondary_columns=["company_name", "market_regime_family"],
    detail_columns=["checkpoint_time", "raw_action", "adjusted_action", "fallback_flag"],
    limit=8,
    empty_message="보정 후 판단 기록이 없습니다.",
    table_expander_label="보정 후 판단 원본 표 보기",
)

render_record_cards(
    meta_prediction_frame,
    title="메타 예측",
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
    empty_message="메타 예측 이력이 없습니다.",
    table_expander_label="메타 예측 원본 표 보기",
)

render_record_cards(
    meta_decision_frame,
    title="메타 보정 / 최종 판단",
    primary_column="symbol",
    secondary_columns=["company_name", "final_action"],
    detail_columns=[
        "checkpoint_time",
        "raw_action",
        "adjusted_action",
        "predicted_class",
        "confidence_margin",
        "fallback_flag",
    ],
    limit=8,
    empty_message="메타 보정 기록이 없습니다.",
    table_expander_label="메타 보정 원본 표 보기",
)

render_record_cards(
    lineage_frame,
    title="장중 라인리지",
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
    empty_message="라인리지 이력이 없습니다.",
    table_expander_label="라인리지 원본 표 보기",
)

render_record_cards(
    strategy_trace_frame,
    title="시점 추적 / 동일 종료 비교",
    primary_column="symbol",
    secondary_columns=["company_name", "strategy_id"],
    detail_columns=["horizon", "executed_flag", "timing_edge_vs_open_bps", "outcome_status"],
    limit=8,
    empty_message="전략 추적 이력이 없습니다.",
    table_expander_label="전략 추적 원본 표 보기",
)

render_record_cards(
    same_exit_frame,
    title="동일 종료 전략 비교",
    primary_column="strategy_id",
    secondary_columns=["horizon"],
    detail_columns=[
        "executed_count",
        "execution_rate",
        "mean_realized_excess_return",
        "mean_timing_edge_vs_open_bps",
    ],
    limit=8,
    empty_message="동일 종료 비교 데이터가 없습니다.",
    table_expander_label="동일 종료 비교 원본 표 보기",
)

render_record_cards(
    active_policy_frame,
    title="활성 장중 정책",
    primary_column="policy_template",
    secondary_columns=["scope_type"],
    detail_columns=["effective_from_date", "note"],
    limit=5,
    empty_message="활성 장중 정책이 없습니다.",
    table_expander_label="장중 정책 원본 표 보기",
)

render_record_cards(
    recommendation_frame,
    title="정책 추천 결과",
    primary_column="recommended_policy_template",
    secondary_columns=["scope_type", "horizon"],
    detail_columns=["objective_score"],
    limit=8,
    empty_message="정책 추천 결과가 없습니다.",
    table_expander_label="정책 추천 원본 표 보기",
)

render_record_cards(
    active_meta_model_frame,
    title="활성 메타 모델",
    primary_column="model_version",
    secondary_columns=["panel_name", "horizon"],
    detail_columns=["effective_from_date", "note"],
    limit=8,
    empty_message="활성 메타 모델이 없습니다.",
    table_expander_label="메타 모델 원본 표 보기",
)

render_record_cards(
    timing_frame,
    title="시점 결과",
    primary_column="symbol",
    secondary_columns=["session_date", "selected_checkpoint_time"],
    detail_columns=["horizon", "selected_action", "timing_edge_bps", "outcome_status"],
    limit=8,
    empty_message="시점 결과가 없습니다.",
    table_expander_label="시점 결과 원본 표 보기",
)

render_record_cards(
    tuned_decision_frame,
    title="튜닝 액션 요약",
    primary_column="symbol",
    secondary_columns=["company_name", "final_action"],
    detail_columns=["checkpoint_time", "tuned_action", "confidence_margin", "uncertainty_score"],
    limit=8,
    empty_message="튜닝 액션 데이터가 없습니다.",
    table_expander_label="튜닝 액션 원본 표 보기",
)

if summary_preview:
    with st.expander("최신 장중 요약 리포트 미리보기", expanded=False):
        render_report_preview(title="장중 요약 리포트", preview=summary_preview)

if policy_preview:
    with st.expander("최신 장중 정책 연구 리포트 미리보기", expanded=False):
        render_report_preview(title="장중 정책 연구 리포트", preview=policy_preview)

if postmortem_preview:
    with st.expander("최신 장중 사후 분석 미리보기", expanded=False):
        render_report_preview(title="장중 사후 분석 리포트", preview=postmortem_preview)

render_page_footer(settings, page_name="장중 콘솔")
