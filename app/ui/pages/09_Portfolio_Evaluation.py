# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_data_sheet,
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_screen_guide,
)
from app.ui.helpers import (
    latest_portfolio_evaluation_frame,
    latest_portfolio_nav_frame,
    load_ui_page_context,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="portfolio_evaluation",
    page_title="추천안 평가",
)
nav_frame = latest_portfolio_nav_frame(settings, limit=60)
evaluation_frame = latest_portfolio_evaluation_frame(settings, limit=80)

render_page_header(
    settings,
    page_name="추천 평가",
    title="추천 평가",
    description="추천 포트폴리오의 자산 흐름과 평가 요약을 모바일에서 읽기 쉬운 세로 시트로 보여줍니다.",
)
render_screen_guide(
    summary="가치 흐름과 평가 요약을 같은 화면에 두되, 모바일에서는 필요한 블록만 골라 볼 수 있게 나눴습니다.",
    bullets=[
        "자산 흐름에서는 날짜별 NAV와 누적 추이를 먼저 확인합니다.",
        "평가 요약에서는 성과, 비교 기준, 보조 전략 결과를 읽습니다.",
    ],
)

render_narrative_card(
    "추천 평가 요약",
    "추천 포트폴리오의 자산 흐름과 평가 요약을 같은 기준일로 정리했습니다. 모바일에서는 표 대신 시트 형식으로 핵심 필드를 먼저 보여줍니다.",
)

view = st.segmented_control(
    "평가 보기",
    options=["자산 흐름", "평가 요약"],
    default="자산 흐름",
)

if view == "자산 흐름":
    render_data_sheet(
        nav_frame,
        title="가상 계좌 흐름",
        limit=12,
        empty_message="추천 포트폴리오 NAV 이력이 아직 없습니다.",
        caption="가장 최근 흐름부터 세로로 읽을 수 있게 정리했습니다.",
    )
else:
    render_data_sheet(
        evaluation_frame,
        title="평가 요약",
        limit=12,
        empty_message="추천 평가 요약이 아직 없습니다.",
    )

render_page_footer(settings, page_name="추천 평가")
