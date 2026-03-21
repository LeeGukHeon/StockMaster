# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ui.components import (
    render_data_sheet,
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_screen_guide,
)
from app.ui.helpers import (
    latest_flow_summary_frame,
    latest_market_news_frame,
    latest_market_mood_summary,
    latest_regime_frame,
    leaderboard_frame,
    load_ui_page_context,
    market_pulse_frame,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="market_pulse",
    page_title="시장 현황",
)
pulse = market_pulse_frame(settings)
regime = latest_regime_frame(settings)
flow = latest_flow_summary_frame(settings)
news = latest_market_news_frame(settings, limit=12)
market_mood = latest_market_mood_summary(settings)
leaders = leaderboard_frame(
    settings,
    ranking_version=SELECTION_ENGINE_V2_VERSION,
    horizon=5,
    limit=12,
)

render_page_header(
    settings,
    page_name="시장 현황",
    title="시장 현황",
    description="시장 국면, 수급, 변동성, 뉴스, 상위 종목을 모바일에서도 읽기 쉬운 요약 시트로 보여줍니다.",
)
render_screen_guide(
    summary="표를 가로로 밀어 보지 않도록 핵심 수치를 세로 시트로 정리한 화면입니다. 상단 전환 버튼으로 필요한 덩어리만 골라 보세요.",
    bullets=[
        "핵심에서는 시장 분위기와 수급 변화를 먼저 봅니다.",
        "뉴스에서는 시장에 영향을 주는 최근 기사와 연결 종목을 빠르게 읽습니다.",
        "리더보드에서는 오늘 강한 종목 후보와 주요 리스크를 확인합니다.",
    ],
)

if pulse.empty and regime.empty:
    render_narrative_card(
        "시장 요약",
        "현재 시장 현황 데이터가 없습니다. 시장 국면 스냅샷과 일일 리서치 적재 상태를 먼저 확인하세요.",
    )
else:
    render_narrative_card(
        "시장 요약",
        f"현재 시장 분위기는 {market_mood.get('headline', '미확인')}이며, {market_mood.get('label', '-')} 기준입니다. {market_mood.get('detail', '')}",
    )

view = st.segmented_control(
    "시장 보기",
    options=["핵심", "뉴스", "리더보드"],
    default="핵심",
)

if view == "핵심":
    render_data_sheet(
        pulse,
        title="시장 펄스",
        limit=8,
        empty_message="시장 펄스 데이터가 없습니다.",
        caption="핵심 지표를 세로 시트로 정리해 모바일에서 한 눈에 읽을 수 있게 했습니다.",
    )
    render_data_sheet(
        regime,
        title="시장 국면",
        limit=6,
        empty_message="시장 국면 데이터가 없습니다.",
    )
    render_data_sheet(
        flow,
        title="수급 흐름",
        limit=6,
        empty_message="수급 요약 데이터가 없습니다.",
    )
elif view == "뉴스":
    render_data_sheet(
        news,
        title="시장 뉴스",
        primary_column="title",
        secondary_columns=["provider", "published_at"],
        detail_columns=["linked_symbols", "news_category"],
        limit=10,
        empty_message="시장 뉴스가 없습니다.",
        table_expander_label="시장 뉴스 전체 표 보기",
    )
else:
    render_data_sheet(
        leaders,
        title="상위 종목 미리보기",
        primary_column="symbol",
        secondary_columns=["company_name", "grade"],
        detail_columns=[
            "final_selection_value",
            "expected_excess_return",
            "flow_score",
            "risks",
        ],
        limit=10,
        empty_message="상위 종목 데이터가 없습니다.",
        table_expander_label="리더보드 전체 표 보기",
    )

render_page_footer(settings, page_name="시장 현황")
