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
    localize_frame,
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
    description=(
        "장중 후보군 보조 엔진의 원정책, 조정정책, 메타 오버레이, 최종 행동을 "
        "리서치 전용 기준으로 한 화면에서 확인합니다."
    ),
)
render_warning_banner(
    "INFO",
    "이 화면은 리서치 전용 / 비매매 출력입니다. 실제 주문, 자동 실행, 자동 승격은 수행하지 않습니다.",
)

if status_frame.empty:
    render_narrative_card(
        "장중 리서치 상태",
        "아직 장중 후보군 세션이나 수집 결과가 없습니다. 후보군 세션 생성과 장중 보조 번들 실행 여부를 먼저 확인하세요.",
    )
else:
    row = status_frame.iloc[0]
    render_narrative_card(
        "장중 리서치 상태",
        (
            f"최신 세션은 {row.get('session_date', '-')}, 후보군 {row.get('candidate_symbols', '-')}종목, "
            f"원정책 {row.get('raw_decision_symbols', '-')}, 조정정책 {row.get('adjusted_symbols', '-')}, "
            f"메타 최종 {row.get('final_action_symbols', '-')} 종목입니다."
        ),
    )

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("장중 세션 요약")
    st.dataframe(localize_frame(status_frame), width="stretch", hide_index=True)
with top_right:
    st.subheader("리서치 기능 활성 상태")
    st.dataframe(localize_frame(capability_frame), width="stretch", hide_index=True)

context_left, context_right = st.columns(2)
with context_left:
    st.subheader("시장 맥락")
    st.dataframe(localize_frame(market_context), width="stretch", hide_index=True)
with context_right:
    st.subheader("체크포인트 상태")
    st.dataframe(localize_frame(checkpoint_health), width="stretch", hide_index=True)

candidate_left, candidate_right = st.columns(2)
with candidate_left:
    st.subheader("후보군")
    st.dataframe(localize_frame(candidate_frame), width="stretch", hide_index=True)
with candidate_right:
    st.subheader("원정책 판단")
    st.dataframe(localize_frame(decision_frame), width="stretch", hide_index=True)

signal_left, signal_right = st.columns(2)
with signal_left:
    st.subheader("장중 신호")
    st.dataframe(localize_frame(signal_frame), width="stretch", hide_index=True)
with signal_right:
    st.subheader("조정정책 판단")
    st.dataframe(localize_frame(adjusted_decision_frame), width="stretch", hide_index=True)

meta_left, meta_right = st.columns(2)
with meta_left:
    st.subheader("메타 예측")
    st.dataframe(localize_frame(meta_prediction_frame), width="stretch", hide_index=True)
with meta_right:
    st.subheader("메타 오버레이 / 최종 행동")
    st.dataframe(localize_frame(meta_decision_frame), width="stretch", hide_index=True)

lineage_left, lineage_right = st.columns(2)
with lineage_left:
    st.subheader("의사결정 라인리지")
    st.dataframe(localize_frame(lineage_frame), width="stretch", hide_index=True)
with lineage_right:
    st.subheader("전략 추적 / 동일 종료 비교")
    st.dataframe(localize_frame(strategy_trace_frame), width="stretch", hide_index=True)
    st.dataframe(localize_frame(same_exit_frame), width="stretch", hide_index=True)

policy_left, policy_right = st.columns(2)
with policy_left:
    st.subheader("활성 장중 정책 / 추천")
    st.dataframe(localize_frame(active_policy_frame), width="stretch", hide_index=True)
    st.dataframe(localize_frame(recommendation_frame), width="stretch", hide_index=True)
with policy_right:
    st.subheader("활성 메타 모델 / 타이밍 엣지")
    st.dataframe(localize_frame(active_meta_model_frame), width="stretch", hide_index=True)
    st.dataframe(localize_frame(timing_frame), width="stretch", hide_index=True)

st.subheader("튜닝된 최종 액션")
st.dataframe(localize_frame(tuned_decision_frame), width="stretch", hide_index=True)

if summary_preview:
    with st.expander("최신 장중 요약 리포트 미리보기", expanded=False):
        st.code(summary_preview)

if policy_preview:
    with st.expander("최신 장중 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_preview)

if postmortem_preview:
    with st.expander("최신 장중 사후 분석 미리보기", expanded=False):
        st.code(postmortem_preview)

render_page_footer(settings, page_name="장중 콘솔")
