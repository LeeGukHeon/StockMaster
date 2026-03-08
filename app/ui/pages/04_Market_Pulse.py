# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ui.components import render_narrative_card, render_page_footer, render_page_header
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
    description="regime, breadth, 변동성, 수급, 뉴스 cluster, narrative summary를 한 화면에서 확인하는 페이지입니다.",
)

if pulse.empty and regime.empty:
    render_narrative_card(
        "Market Narrative",
        "현재 market pulse 스냅샷이 없습니다. regime snapshot과 daily research ingestion 상태를 먼저 확인하세요.",
    )
else:
    regime_text = regime.iloc[0]["regime_state"] if not regime.empty else "unknown"
    render_narrative_card(
        "Market Narrative",
        f"현재 시장 regime은 {regime_text} 입니다. breadth, 수급, 최근 뉴스 cluster를 함께 읽고 리더보드와 포트폴리오로 drill-down 하는 것이 권장 흐름입니다.",
    )

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("Market Pulse Snapshot")
    st.dataframe(localize_frame(pulse), width="stretch", hide_index=True)
with top_right:
    st.subheader("Regime / Flow")
    if not regime.empty:
        st.dataframe(localize_frame(regime), width="stretch", hide_index=True)
    if not flow.empty:
        st.dataframe(localize_frame(flow), width="stretch", hide_index=True)

news_left, news_right = st.columns(2)
with news_left:
    st.subheader("News Clusters")
    st.dataframe(localize_frame(news), width="stretch", hide_index=True)
with news_right:
    st.subheader("Top Actionable Preview")
    display = leaders[
        [column for column in ["symbol", "company_name", "grade", "final_selection_value", "expected_excess_return", "flow_score", "risks"] if column in leaders.columns]
    ].copy()
    st.dataframe(localize_frame(display), width="stretch", hide_index=True)

render_page_footer(settings, page_name="시장 현황")
