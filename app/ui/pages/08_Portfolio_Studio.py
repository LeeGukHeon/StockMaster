# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    latest_portfolio_candidate_frame,
    latest_portfolio_constraint_frame,
    latest_portfolio_policy_registry_frame,
    latest_portfolio_rebalance_plan_frame,
    latest_portfolio_report_preview,
    latest_portfolio_target_book_frame,
    latest_portfolio_waitlist_frame,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)

execution_mode = st.selectbox(
    "실행 모드",
    options=["OPEN_ALL", "TIMING_ASSISTED"],
    index=1,
)

active_policy = latest_portfolio_policy_registry_frame(settings, active_only=True, limit=10)
candidate_book = latest_portfolio_candidate_frame(settings, execution_mode=execution_mode, limit=30)
target_book = latest_portfolio_target_book_frame(settings, execution_mode=execution_mode, limit=30)
waitlist = latest_portfolio_waitlist_frame(settings, execution_mode=execution_mode, limit=20)
rebalance = latest_portfolio_rebalance_plan_frame(settings, execution_mode=execution_mode, limit=30)
constraints = latest_portfolio_constraint_frame(settings, limit=20)
report_preview = latest_portfolio_report_preview(settings)

st.title("포트폴리오 스튜디오")
st.caption(
    "Selection v2와 장중 timing 결과를 downstream으로 받아 "
    "long-only 목표 보유안과 리밸런스 계획을 확인하는 화면입니다."
)

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("활성 포트폴리오 정책")
    if active_policy.empty:
        st.info("활성 포트폴리오 정책이 아직 freeze되지 않았습니다.")
    else:
        st.dataframe(localize_frame(active_policy), width="stretch", hide_index=True)
with top_right:
    st.subheader("포트폴리오 후보군")
    if candidate_book.empty:
        st.info("포트폴리오 후보군이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(candidate_book), width="stretch", hide_index=True)

body_left, body_right = st.columns(2)
with body_left:
    st.subheader("목표 보유안")
    if target_book.empty:
        st.info("목표 비중/수량이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(target_book), width="stretch", hide_index=True)
with body_right:
    st.subheader("리밸런스 모니터")
    if rebalance.empty:
        st.info("리밸런스 계획이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(rebalance), width="stretch", hide_index=True)

lower_left, lower_right = st.columns(2)
with lower_left:
    st.subheader("대기열 / 차단")
    if waitlist.empty:
        st.info("대기열 또는 차단 종목이 없습니다.")
    else:
        st.dataframe(localize_frame(waitlist), width="stretch", hide_index=True)
with lower_right:
    st.subheader("제약 이벤트")
    if constraints.empty:
        st.info("기록된 제약 이벤트가 없습니다.")
    else:
        st.dataframe(localize_frame(constraints), width="stretch", hide_index=True)

if report_preview:
    with st.expander("최신 포트폴리오 리포트 미리보기", expanded=False):
        st.code(report_preview)
