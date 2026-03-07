# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.helpers import (
    available_ranking_dates,
    available_ranking_versions,
    latest_evaluation_comparison_frame,
    latest_selection_validation_summary_frame,
    latest_validation_summary_frame,
    leaderboard_frame,
    leaderboard_grade_count_frame,
    load_ui_settings,
)

st.set_page_config(page_title="Leaderboard", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
ranking_versions = available_ranking_versions(settings)
evaluation_comparison = latest_evaluation_comparison_frame(settings)

st.title("Leaderboard")
st.caption(
    "Compare explanatory ranking v0 and selection engine v1. "
    "Selection v1 may include calibrated proxy bands; these are not ML forecasts."
)

if not ranking_versions:
    st.info("No ranking snapshots are available yet. Run the build scripts first.")
else:
    default_version_index = (
        ranking_versions.index(SELECTION_ENGINE_VERSION)
        if SELECTION_ENGINE_VERSION in ranking_versions
        else 0
    )
    selected_version = st.selectbox(
        "Ranking version",
        options=ranking_versions,
        index=default_version_index,
    )
    ranking_dates = available_ranking_dates(settings, ranking_version=selected_version)
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
        ranking_version=selected_version,
    )
    grade_counts = leaderboard_grade_count_frame(
        settings,
        as_of_date=selected_date,
        horizon=horizon,
        ranking_version=selected_version,
    )
    validation = (
        latest_selection_validation_summary_frame(settings, limit=50)
        if selected_version == SELECTION_ENGINE_VERSION
        else latest_validation_summary_frame(settings, limit=50)
    )

    top_left, top_right = st.columns((2, 1))
    with top_left:
        st.subheader("Ranking Table")
        if board.empty:
            st.info("No ranking rows match the current filter.")
        else:
            columns = [
                "symbol",
                "company_name",
                "market",
                "final_selection_value",
                "final_selection_rank_pct",
                "grade",
                "regime_state",
                "outcome_status",
                "realized_excess_return",
                "band_status",
                "reasons",
                "risks",
            ]
            if selected_version == SELECTION_ENGINE_VERSION:
                columns.extend(["expected_excess_return", "lower_band", "upper_band"])
            display = board[columns].copy()
            display["final_selection_rank_pct"] = (
                pd.to_numeric(display["final_selection_rank_pct"], errors="coerce") * 100.0
            ).round(1)
            st.dataframe(display, width="stretch", hide_index=True)
    with top_right:
        st.subheader("Grade Mix")
        if grade_counts.empty:
            st.info("No grade mix available.")
        else:
            st.dataframe(grade_counts, width="stretch", hide_index=True)

    st.subheader("Latest Validation Summary")
    if validation.empty:
        st.info("Validation rows are empty for the selected version.")
    else:
        filtered = validation.loc[validation["horizon"] == horizon].copy()
        st.dataframe(filtered, width="stretch", hide_index=True)

    st.subheader("Latest Selection v1 vs Explanatory v0")
    if evaluation_comparison.empty:
        st.info("No evaluation comparison rows are available yet.")
    else:
        st.dataframe(evaluation_comparison, width="stretch", hide_index=True)
