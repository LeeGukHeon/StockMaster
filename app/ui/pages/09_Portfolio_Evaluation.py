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
    latest_portfolio_evaluation_frame,
    latest_portfolio_nav_frame,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
nav_frame = latest_portfolio_nav_frame(settings, limit=60)
evaluation_frame = latest_portfolio_evaluation_frame(settings, limit=80)

render_page_header(
    settings,
    page_name="포트폴리오 평가",
    title="포트폴리오 평가",
    description="순자산 가치, 낙폭, 회전율, 보유 종목 수와 시가 일괄 진입 대비 장중 타이밍 보조 결과를 비교하는 화면입니다.",
)

render_narrative_card(
    "포트폴리오 평가 요약",
    "포트폴리오 평가는 결정론적 배분 결과를 기준으로 합니다. 자동매매가 아니라 포트폴리오 제안을 사후에 비교하는 평가 레이어입니다.",
)

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("순자산 가치 추이")
    if nav_frame.empty:
        st.info("포트폴리오 순자산 가치 이력이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(nav_frame), width="stretch", hide_index=True)
with top_right:
    st.subheader("평가 요약")
    if evaluation_frame.empty:
        st.info("포트폴리오 평가 요약이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(evaluation_frame), width="stretch", hide_index=True)

render_page_footer(settings, page_name="포트폴리오 평가")
