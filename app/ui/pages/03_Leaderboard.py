# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    available_ranking_dates,
    latest_validation_summary_frame,
    leaderboard_frame,
    leaderboard_grade_count_frame,
    load_ui_settings,
)

st.set_page_config(page_title="Leaderboard", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
ranking_dates = available_ranking_dates(settings)

st.title("Leaderboard")
st.caption("Explanatory ranking v0. The score is an inspection layer, not a predictive model.")

if not ranking_dates:
    st.info("No ranking snapshots are available yet. Run the TICKET-003 build scripts first.")
else:
    selected_date = st.selectbox("As-of date", options=ranking_dates, index=0)
    horizon = st.selectbox("Horizon", options=[1, 5], index=1)
    market = st.selectbox("Market", options=["ALL", "KOSPI", "KOSDAQ"], index=0)
    limit = st.slider("Rows", min_value=10, max_value=100, value=25, step=5)

    board = leaderboard_frame(
        settings,
        as_of_date=selected_date,
        horizon=horizon,
        market=market,
        limit=limit,
    )
    grade_counts = leaderboard_grade_count_frame(
        settings,
        as_of_date=selected_date,
        horizon=horizon,
    )
    validation = latest_validation_summary_frame(settings, limit=50)

    top_left, top_right = st.columns((2, 1))
    with top_left:
        st.subheader("Ranking Table")
        if board.empty:
            st.info("No ranking rows match the current filter.")
        else:
            display = board[
                [
                    "symbol",
                    "company_name",
                    "market",
                    "final_selection_value",
                    "final_selection_rank_pct",
                    "grade",
                    "regime_state",
                    "reasons",
                    "risks",
                ]
            ].copy()
            display["final_selection_rank_pct"] = (
                pd.to_numeric(display["final_selection_rank_pct"], errors="coerce") * 100.0
            ).round(1)
            st.dataframe(display, use_container_width=True, hide_index=True)
    with top_right:
        st.subheader("Grade Mix")
        if grade_counts.empty:
            st.info("No grade mix available.")
        else:
            st.dataframe(grade_counts, use_container_width=True, hide_index=True)

    st.subheader("Latest Validation Summary")
    if validation.empty:
        st.info(
            "Validation rows are empty. Build historical rankings and forward "
            "labels before validating."
        )
    else:
        filtered = validation.loc[validation["horizon"] == horizon].copy()
        if filtered.empty:
            st.info("No validation rows for this horizon yet.")
        else:
            st.dataframe(filtered, use_container_width=True, hide_index=True)
