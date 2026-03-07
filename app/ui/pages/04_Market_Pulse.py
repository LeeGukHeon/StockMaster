# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.helpers import (
    latest_market_news_frame,
    leaderboard_frame,
    load_ui_settings,
    localize_frame,
    market_pulse_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
pulse = market_pulse_frame(settings)
news = latest_market_news_frame(settings, limit=8)
d1_board = leaderboard_frame(
    settings,
    horizon=1,
    limit=10,
    ranking_version=SELECTION_ENGINE_VERSION,
)
d5_board = leaderboard_frame(
    settings,
    horizon=5,
    limit=10,
    ranking_version=SELECTION_ENGINE_VERSION,
)

st.title("시장 현황")
st.caption("시장 상태, 수급 폭, 선정 엔진 v1 결과를 함께 보는 화면입니다.")

st.subheader("시장 현황 스냅샷")
st.dataframe(localize_frame(pulse), width="stretch", hide_index=True)

left, right = st.columns(2)
with left:
    st.subheader("상위 D+1 후보")
    d1_display = d1_board[
        [
            "symbol",
            "company_name",
            "market",
            "final_selection_value",
            "grade",
            "reasons",
            "risks",
        ]
    ].copy()
    st.dataframe(localize_frame(d1_display), width="stretch", hide_index=True)
with right:
    st.subheader("상위 D+5 후보")
    d5_display = d5_board[
        [
            "symbol",
            "company_name",
            "market",
            "final_selection_value",
            "grade",
            "expected_excess_return",
            "lower_band",
            "upper_band",
            "reasons",
            "risks",
        ]
    ].copy()
    st.dataframe(localize_frame(d5_display), width="stretch", hide_index=True)

st.subheader("시장 전체 뉴스")
st.dataframe(localize_frame(news), width="stretch", hide_index=True)
