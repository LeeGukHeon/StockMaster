# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import render_narrative_card, render_page_footer, render_page_header
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
    latest_intraday_meta_active_model_frame,
    latest_intraday_meta_decision_frame,
    latest_intraday_meta_prediction_frame,
    latest_intraday_policy_recommendation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_postmortem_preview,
    latest_intraday_status_frame,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)

status_frame = latest_intraday_status_frame(settings)
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
postmortem_preview = latest_intraday_postmortem_preview(settings)
policy_preview = latest_intraday_policy_report_preview(settings)

render_page_header(
    settings,
    page_name="장중 콘솔",
    title="장중 콘솔",
    description="candidate-only 장중 보조 화면입니다. raw action, adjusted action, meta overlay, final action, stale warning을 함께 보여줍니다.",
)

if status_frame.empty:
    render_narrative_card(
        "Intraday Narrative",
        "아직 장중 세션이나 collector 결과가 없습니다. candidate session materialization과 intraday backfill을 먼저 확인하세요.",
    )
else:
    row = status_frame.iloc[0]
    render_narrative_card(
        "Intraday Narrative",
        f"최신 세션은 {row.get('session_date', '-')}, checkpoint 상태는 {row.get('status', '-')}, "
        f"candidate coverage는 {row.get('candidate_count', '-')}, stale warning은 {row.get('stale_flag', '-')}"
        " 기준으로 표시됩니다.",
    )

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("최신 장중 세션 상태")
    st.dataframe(localize_frame(status_frame), width="stretch", hide_index=True)
with top_right:
    st.subheader("체크포인트 헬스")
    st.dataframe(localize_frame(checkpoint_health), width="stretch", hide_index=True)

context_left, context_right = st.columns(2)
with context_left:
    st.subheader("시장 Context")
    st.dataframe(localize_frame(market_context), width="stretch", hide_index=True)
with context_right:
    st.subheader("후보군 세션")
    st.dataframe(localize_frame(candidate_frame), width="stretch", hide_index=True)

signal_left, signal_right = st.columns(2)
with signal_left:
    st.subheader("Signal Snapshot")
    st.dataframe(localize_frame(signal_frame), width="stretch", hide_index=True)
with signal_right:
    st.subheader("Raw Decision")
    st.dataframe(localize_frame(decision_frame), width="stretch", hide_index=True)

decision_left, decision_right = st.columns(2)
with decision_left:
    st.subheader("Adjusted Action")
    st.dataframe(localize_frame(adjusted_decision_frame), width="stretch", hide_index=True)
with decision_right:
    st.subheader("Tuned / Final Action")
    st.dataframe(localize_frame(tuned_decision_frame), width="stretch", hide_index=True)

meta_left, meta_right = st.columns(2)
with meta_left:
    st.subheader("ML Class Probability / Margin")
    st.dataframe(localize_frame(meta_prediction_frame), width="stretch", hide_index=True)
with meta_right:
    st.subheader("Meta Final Decision")
    st.dataframe(localize_frame(meta_decision_frame), width="stretch", hide_index=True)

policy_left, policy_right = st.columns(2)
with policy_left:
    st.subheader("Active Timing Policy")
    st.dataframe(localize_frame(active_policy_frame), width="stretch", hide_index=True)
    st.subheader("Policy Recommendation")
    st.dataframe(localize_frame(recommendation_frame), width="stretch", hide_index=True)
with policy_right:
    st.subheader("Active Meta Model")
    st.dataframe(localize_frame(active_meta_model_frame), width="stretch", hide_index=True)

trace_left, trace_right = st.columns(2)
with trace_left:
    st.subheader("Strategy Trace")
    st.dataframe(localize_frame(strategy_trace_frame), width="stretch", hide_index=True)
with trace_right:
    st.subheader("Timing Edge vs Open")
    st.dataframe(localize_frame(timing_frame), width="stretch", hide_index=True)

if policy_preview:
    with st.expander("최신 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_preview)

if postmortem_preview:
    with st.expander("최신 장중 postmortem 미리보기", expanded=False):
        st.code(postmortem_preview)

render_page_footer(settings, page_name="장중 콘솔")
