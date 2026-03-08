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
    format_execution_mode_label,
    latest_portfolio_candidate_frame,
    latest_portfolio_constraint_frame,
    latest_portfolio_nav_frame,
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
    format_func=format_execution_mode_label,
)

active_policy = latest_portfolio_policy_registry_frame(settings, active_only=True, limit=10)
candidate_book = latest_portfolio_candidate_frame(settings, execution_mode=execution_mode, limit=30)
target_book = latest_portfolio_target_book_frame(settings, execution_mode=execution_mode, limit=30)
waitlist = latest_portfolio_waitlist_frame(settings, execution_mode=execution_mode, limit=20)
rebalance = latest_portfolio_rebalance_plan_frame(settings, execution_mode=execution_mode, limit=30)
constraints = latest_portfolio_constraint_frame(settings, limit=20)
nav_frame = latest_portfolio_nav_frame(settings, limit=20)
report_preview = latest_portfolio_report_preview(settings)

render_page_header(
    settings,
    page_name="포트폴리오",
    title="포트폴리오",
    description="선정 엔진과 장중 타이밍 보조 결과를 바탕으로 매수 후보, 목표 비중, 리밸런스 계획을 제안하는 화면입니다.",
)

if active_policy.empty:
    render_narrative_card(
        "포트폴리오 요약",
        "활성 포트폴리오 정책이 아직 동결되지 않았습니다. 그래도 시험 실행 기준으로 후보 목록, 목표 편입안, 리밸런스 계획은 확인할 수 있습니다.",
    )
else:
    row = active_policy.iloc[0]
    render_narrative_card(
        "포트폴리오 요약",
        f"현재 활성 정책은 {row.get('active_portfolio_policy_id', '-')}, 실행 모드는 {format_execution_mode_label(execution_mode)}입니다. 신규 진입과 추가 매수는 장중 판단 결과를 선택적으로 반영합니다.",
    )

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("활성 포트폴리오 정책")
    st.dataframe(localize_frame(active_policy), width="stretch", hide_index=True)
with top_right:
    st.subheader("후보 종목 목록")
    st.dataframe(localize_frame(candidate_book), width="stretch", hide_index=True)

body_left, body_right = st.columns(2)
with body_left:
    st.subheader("목표 편입안")
    st.dataframe(localize_frame(target_book), width="stretch", hide_index=True)
with body_right:
    st.subheader("리밸런스 점검")
    st.dataframe(localize_frame(rebalance), width="stretch", hide_index=True)

lower_left, lower_right = st.columns(2)
with lower_left:
    st.subheader("대기 / 차단 종목")
    if waitlist.empty:
        st.info("현재 대기 또는 차단 종목이 없습니다.")
    else:
        st.dataframe(localize_frame(waitlist), width="stretch", hide_index=True)

    st.subheader("제약 요약")
    if constraints.empty:
        st.info("기록된 제약 이벤트가 없습니다.")
    else:
        st.dataframe(localize_frame(constraints), width="stretch", hide_index=True)
with lower_right:
    st.subheader("최근 순자산 가치 / 익스포저")
    if nav_frame.empty:
        st.info("순자산 가치 스냅샷이 없습니다.")
    else:
        st.dataframe(localize_frame(nav_frame), width="stretch", hide_index=True)

if report_preview:
    with st.expander("최신 포트폴리오 리포트 미리보기", expanded=False):
        st.code(report_preview)

render_page_footer(
    settings,
    page_name="포트폴리오",
    extra_items=[f"실행 모드: {format_execution_mode_label(execution_mode)}"],
)
