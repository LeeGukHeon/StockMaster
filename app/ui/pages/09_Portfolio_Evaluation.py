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
    render_screen_guide,
)
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
    page_name="추천안 평가",
    title="추천안 평가",
    description="가상 추천 묶음의 순자산 가치, 낙폭, 회전율과 시가 일괄 진입 대비 장중 타이밍 보조 결과를 비교하는 화면입니다.",
)
render_screen_guide(
    summary="추천 구성안이 시간이 지나면서 어떤 성과를 냈는지 사후에 비교하는 화면입니다. 실제 자동매매 손익장이 아니라 제안 성과 비교표로 이해하면 됩니다.",
    bullets=[
        "왼쪽은 시간에 따른 전체 계좌 흐름, 오른쪽은 평가 요약이라고 보면 됩니다.",
        "시가 일괄 진입과 장중 보조 방식을 비교해 어떤 방식이 더 안정적이었는지 확인할 때 유용합니다.",
    ],
)

render_narrative_card(
    "추천안 평가 요약",
    "추천안 평가는 결정론적 배분 결과를 기준으로 합니다. 자동매매가 아니라 추천 구성안을 사후에 비교하는 평가 레이어입니다.",
)

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("가상 계좌 흐름")
    if nav_frame.empty:
        st.info("추천안 기준 가상 계좌 이력이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(nav_frame), width="stretch", hide_index=True)
with top_right:
    st.subheader("평가 요약")
    if evaluation_frame.empty:
        st.info("추천안 평가 요약이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(evaluation_frame), width="stretch", hide_index=True)

render_page_footer(settings, page_name="추천안 평가")
