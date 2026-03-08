# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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

st.title("장중 콘솔")
st.caption(
    "Selection v2 후보군 위에서 raw -> adjusted -> tuned -> "
    "meta overlay -> final action 흐름을 보는 화면입니다. "
    "자동매매 화면이 아니라 candidate-only 장중 보조 콘솔입니다."
)

if status_frame.empty:
    st.info("아직 장중 후보 세션이나 수집 결과가 없습니다.")
else:
    st.subheader("최신 장중 세션 상태")
    st.dataframe(localize_frame(status_frame), width="stretch", hide_index=True)

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("체크포인트 헬스")
    if checkpoint_health.empty:
        st.info("체크포인트 상태 데이터가 없습니다.")
    else:
        st.dataframe(localize_frame(checkpoint_health), width="stretch", hide_index=True)
with top_right:
    st.subheader("시장 컨텍스트")
    if market_context.empty:
        st.info("장중 market context snapshot이 없습니다.")
    else:
        st.dataframe(localize_frame(market_context), width="stretch", hide_index=True)

st.subheader("후보 세션")
if candidate_frame.empty:
    st.info("장중 후보 세션 데이터가 없습니다.")
else:
    st.dataframe(localize_frame(candidate_frame), width="stretch", hide_index=True)

signal_left, signal_right = st.columns(2)
with signal_left:
    st.subheader("신호 스냅샷")
    if signal_frame.empty:
        st.info("장중 신호 스냅샷이 없습니다.")
    else:
        st.dataframe(localize_frame(signal_frame), width="stretch", hide_index=True)
with signal_right:
    st.subheader("Raw 진입 판단")
    if decision_frame.empty:
        st.info("raw entry decision이 없습니다.")
    else:
        st.dataframe(localize_frame(decision_frame), width="stretch", hide_index=True)

decision_left, decision_right = st.columns(2)
with decision_left:
    st.subheader("Adjusted 진입 판단")
    if adjusted_decision_frame.empty:
        st.info("adjusted entry decision이 없습니다.")
    else:
        st.dataframe(localize_frame(adjusted_decision_frame), width="stretch", hide_index=True)
with decision_right:
    st.subheader("Tuned 정책 액션")
    if tuned_decision_frame.empty:
        st.info("active policy 기반 tuned action이 없습니다.")
    else:
        st.dataframe(localize_frame(tuned_decision_frame), width="stretch", hide_index=True)

meta_left, meta_right = st.columns(2)
with meta_left:
    st.subheader("ML 메타 예측")
    if meta_prediction_frame.empty:
        st.info("intraday meta prediction 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_prediction_frame), width="stretch", hide_index=True)
with meta_right:
    st.subheader("최종 액션")
    if meta_decision_frame.empty:
        st.info("intraday final action 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_decision_frame), width="stretch", hide_index=True)

policy_left, policy_right = st.columns(2)
with policy_left:
    st.subheader("현재 활성 정책")
    if active_policy_frame.empty:
        st.info("활성 정책 레지스트리가 없습니다.")
    else:
        st.dataframe(localize_frame(active_policy_frame), width="stretch", hide_index=True)
    st.subheader("최신 정책 추천")
    if recommendation_frame.empty:
        st.info("정책 추천 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(recommendation_frame), width="stretch", hide_index=True)
with policy_right:
    st.subheader("현재 활성 메타모델")
    if active_meta_model_frame.empty:
        st.info("활성 메타모델 레지스트리가 없습니다.")
    else:
        st.dataframe(localize_frame(active_meta_model_frame), width="stretch", hide_index=True)

trace_left, trace_right = st.columns(2)
with trace_left:
    st.subheader("전략 추적")
    if strategy_trace_frame.empty:
        st.info("strategy trace 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(strategy_trace_frame), width="stretch", hide_index=True)
with trace_right:
    st.subheader("Timing outcome")
    if timing_frame.empty:
        st.info("장중 timing outcome 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(timing_frame), width="stretch", hide_index=True)

if policy_preview:
    with st.expander("최신 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_preview)

if postmortem_preview:
    with st.expander("최신 장중 사후 분석 미리보기", expanded=False):
        st.code(postmortem_preview)
