# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    latest_fundamentals_sample_frame,
    latest_news_sample_frame,
    latest_ohlcv_sample_frame,
    load_ui_settings,
)

st.set_page_config(page_title="Research Placeholder", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
ohlcv_sample = latest_ohlcv_sample_frame(settings, limit=10)
fundamentals_sample = latest_fundamentals_sample_frame(settings, limit=10)
news_sample = latest_news_sample_frame(settings, limit=10)

st.title("Placeholder Research")
st.caption("TICKET-002 adds the first core research datasets and simple inspection tables.")

st.subheader("Planned Panels")
st.markdown(
    """
    - Market Pulse: regime summary and breadth indicators
    - Leaderboard: expected D+1 and D+5 excess return candidates
    - Symbol Workbench: OHLCV, filings, and news trace by ticker
    """
)

st.subheader("Current Upstream Dependencies")
st.markdown(
    """
    - `fact_daily_ohlcv` now carries price and turnover history
    - `fact_fundamentals_snapshot` now carries availability-aware DART snapshots
    - `fact_news_item` now carries metadata-only news rows with dedupe and symbol candidates
    """
)

st.subheader("Latest OHLCV")
st.dataframe(ohlcv_sample, use_container_width=True, hide_index=True)

st.subheader("Latest Fundamentals Snapshot")
st.dataframe(fundamentals_sample, use_container_width=True, hide_index=True)

st.subheader("Latest News Metadata")
st.dataframe(news_sample, use_container_width=True, hide_index=True)
