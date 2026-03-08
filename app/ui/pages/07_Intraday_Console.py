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
    latest_intraday_checkpoint_health_frame,
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
strategy_trace_frame = intraday_console_strategy_trace_frame(settings, limit=50)
timing_frame = intraday_console_timing_frame(settings, limit=30)
postmortem_preview = latest_intraday_postmortem_preview(settings)

st.title("장중 콘솔")
st.caption(
    "selection engine v2 후보군을 기준으로 "
    "1분봉, 체결 요약, 호가 요약, 체크포인트 액션, "
    "레짐 조정 결과를 확인합니다. 자동매매 화면이 아니라 "
    "후보군 보조/모니터링 화면입니다."
)

st.subheader("최신 장중 세션 상태")
if status_frame.empty:
    st.info("아직 장중 후보 세션이나 intraday 적재 결과가 없습니다.")
else:
    st.dataframe(localize_frame(status_frame), width="stretch", hide_index=True)

left, right = st.columns(2)
with left:
    st.subheader("체크포인트 상태")
    if checkpoint_health.empty:
        st.info("체크포인트 상태 데이터가 아직 없습니다.")
    else:
        st.dataframe(localize_frame(checkpoint_health), width="stretch", hide_index=True)
with right:
    st.subheader("최신 타이밍 평가")
    if timing_frame.empty:
        st.info("장중 타이밍 평가 결과가 아직 없습니다.")
    else:
        st.dataframe(localize_frame(timing_frame), width="stretch", hide_index=True)

st.subheader("시장 컨텍스트")
if market_context.empty:
    st.info("장중 시장 컨텍스트 스냅샷이 아직 없습니다.")
else:
    st.dataframe(localize_frame(market_context), width="stretch", hide_index=True)

st.subheader("후보 세션 미리보기")
if candidate_frame.empty:
    st.info("후보 세션 데이터가 아직 없습니다.")
else:
    st.dataframe(localize_frame(candidate_frame), width="stretch", hide_index=True)

signal_left, signal_right = st.columns(2)
with signal_left:
    st.subheader("신호 스냅샷")
    if signal_frame.empty:
        st.info("신호 스냅샷이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(signal_frame), width="stretch", hide_index=True)
with signal_right:
    st.subheader("원판 진입 판단")
    if decision_frame.empty:
        st.info("진입 판단 결과가 아직 없습니다.")
    else:
        st.dataframe(localize_frame(decision_frame), width="stretch", hide_index=True)

adjust_left, adjust_right = st.columns(2)
with adjust_left:
    st.subheader("조정 진입 판단")
    if adjusted_decision_frame.empty:
        st.info("조정 진입 판단 결과가 아직 없습니다.")
    else:
        st.dataframe(localize_frame(adjusted_decision_frame), width="stretch", hide_index=True)
with adjust_right:
    st.subheader("전략 추적")
    if strategy_trace_frame.empty:
        st.info("전략 추적 결과가 아직 없습니다.")
    else:
        st.dataframe(localize_frame(strategy_trace_frame), width="stretch", hide_index=True)

if postmortem_preview:
    with st.expander("최신 장중 사후 분석 미리보기", expanded=False):
        st.code(postmortem_preview)
