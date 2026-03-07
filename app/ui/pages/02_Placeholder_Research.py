# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

st.set_page_config(page_title="Research Placeholder", page_icon="SM", layout="wide")

st.title("Placeholder Research")
st.caption("TICKET-001 stops at provider activation and reference data.")

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
    - `dim_symbol` now carries the tradable universe contract
    - `dim_trading_calendar` now carries prev/next trading dates
    - KIS and DART have minimal provider probes for smoke checks
    """
)
