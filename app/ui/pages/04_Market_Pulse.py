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
    market_pulse_frame,
)

st.set_page_config(page_title="Market Pulse", page_icon="SM", layout="wide")

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

st.title("Market Pulse")
st.caption("Regime, investor flow breadth, and latest selection engine v1 output in one place.")

st.subheader("Pulse Snapshot")
st.dataframe(pulse, width="stretch", hide_index=True)

left, right = st.columns(2)
with left:
    st.subheader("Top D+1")
    st.dataframe(
        d1_board[
            [
                "symbol",
                "company_name",
                "market",
                "final_selection_value",
                "grade",
                "reasons",
                "risks",
            ]
        ],
        width="stretch",
        hide_index=True,
    )
with right:
    st.subheader("Top D+5")
    st.dataframe(
        d5_board[
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
        ],
        width="stretch",
        hide_index=True,
    )

st.subheader("Market-wide News")
st.dataframe(news, width="stretch", hide_index=True)
