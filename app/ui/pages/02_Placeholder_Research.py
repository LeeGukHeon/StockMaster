from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

st.set_page_config(page_title="Research Placeholder", page_icon="SM", layout="wide")

st.title("Research Placeholder")
st.caption("Reserved space for Market Pulse, Leaderboard, and Stock Workbench pages.")

market_col, leaderboard_col = st.columns(2)

with market_col:
    st.subheader("Market Pulse")
    st.info("Post-market regime summary, issue clusters, and breadth metrics will be added here.")
    st.markdown(
        """
        - Regime label
        - KOSPI/KOSDAQ breadth
        - Flow and liquidity summary
        - News clusters and commentary
        """
    )

with leaderboard_col:
    st.subheader("Leaderboard")
    st.info("Ranked stock candidates and explanatory scores will be added here.")
    st.markdown(
        """
        - D+1 / D+5 ranked candidates
        - Explanatory score cards
        - Risk flags
        - Drilldown links into future workbench pages
        """
    )
