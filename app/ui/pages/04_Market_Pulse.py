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
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_screen_guide,
)
from app.ui.helpers import (
    latest_flow_summary_frame,
    latest_market_news_frame,
    latest_regime_frame,
    leaderboard_frame,
    load_ui_settings,
    localize_frame,
    market_pulse_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
pulse = market_pulse_frame(settings)
regime = latest_regime_frame(settings)
flow = latest_flow_summary_frame(settings)
news = latest_market_news_frame(settings, limit=12)
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
    description="시장 국면, 상승 폭, 변동성, 수급, 시장 뉴스 묶음, 서술형 요약을 함께 보는 화면입니다.",
)
render_screen_guide(
    summary="시장 전체 분위기를 빠르게 읽는 화면입니다. 종목을 고르기 전, 오늘 장이 강한지 약한지 먼저 파악할 때 보면 됩니다.",
    bullets=[
        "시장 흐름과 상승 비율을 먼저 보고, 오늘 장이 편한 장인지 불안한 장인지 확인하세요.",
        "그다음 시장 뉴스와 주목 종목 미리보기를 읽으면 왜 특정 종목이 올라오는지 이해하기 쉽습니다.",
    ],
)

if pulse.empty and regime.empty:
    render_narrative_card(
        "시장 요약",
        "현재 시장 현황 스냅샷이 없습니다. 시장 국면 스냅샷과 일일 리서치 적재 상태를 먼저 확인하세요.",
    )
else:
    regime_text = regime.iloc[0]["regime_state"] if not regime.empty else "미확인"
    render_narrative_card(
        "시장 요약",
        f"현재 시장 국면은 {regime_text}입니다. 상승 폭, 수급, 최신 뉴스 묶음을 함께 보고 리더보드와 포트폴리오 화면으로 이어서 확인하는 흐름을 권장합니다.",
    )

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("시장 현황 스냅샷")
    st.dataframe(localize_frame(pulse), width="stretch", hide_index=True)
with top_right:
    st.subheader("시장 국면 / 수급")
    if not regime.empty:
        st.dataframe(localize_frame(regime), width="stretch", hide_index=True)
    if not flow.empty:
        st.dataframe(localize_frame(flow), width="stretch", hide_index=True)

news_left, news_right = st.columns(2)
with news_left:
    st.subheader("뉴스 묶음")
    st.dataframe(localize_frame(news), width="stretch", hide_index=True)
with news_right:
    st.subheader("주목 종목 미리보기")
    display = leaders[
        [
            column
            for column in [
                "symbol",
                "company_name",
                "grade",
                "final_selection_value",
                "expected_excess_return",
                "flow_score",
                "risks",
            ]
            if column in leaders.columns
        ]
    ].copy()
    st.dataframe(localize_frame(display), width="stretch", hide_index=True)

render_page_footer(settings, page_name="시장 현황")
