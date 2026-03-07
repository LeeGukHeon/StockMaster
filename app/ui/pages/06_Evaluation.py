# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.helpers import (
    available_evaluation_dates,
    evaluation_outcomes_frame,
    latest_calibration_diagnostic_frame,
    latest_evaluation_comparison_frame,
    latest_evaluation_summary_frame,
    latest_postmortem_preview,
    load_ui_settings,
)

st.set_page_config(page_title="Evaluation", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
evaluation_dates = available_evaluation_dates(settings)
latest_summary = latest_evaluation_summary_frame(settings, limit=30)
latest_comparison = latest_evaluation_comparison_frame(settings)
latest_calibration = latest_calibration_diagnostic_frame(settings, limit=30)
postmortem_preview = latest_postmortem_preview(settings)

st.title("Evaluation")
st.caption(
    "Frozen selection/prediction snapshots vs realized next-open to future-close outcomes. "
    "All metrics shown here are pre-cost."
)

if not evaluation_dates:
    st.info("No evaluation outcomes are available yet. Run the TICKET-005 scripts first.")
else:
    selected_date = st.selectbox("Evaluation date", options=evaluation_dates, index=0)
    horizon = st.selectbox("Horizon", options=[1, 5], index=1)
    ranking_version = st.selectbox(
        "Ranking version",
        options=[SELECTION_ENGINE_VERSION, EXPLANATORY_RANKING_VERSION],
        index=0,
    )
    limit = st.slider("Rows", min_value=10, max_value=100, value=25, step=5)

    outcomes = evaluation_outcomes_frame(
        settings,
        evaluation_date=selected_date,
        horizon=horizon,
        ranking_version=ranking_version,
        limit=limit,
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Latest Evaluation Summary")
        st.dataframe(latest_summary, width="stretch", hide_index=True)
        st.subheader("Latest Selection vs Explanatory")
        st.dataframe(latest_comparison, width="stretch", hide_index=True)
    with right:
        st.subheader("Latest Calibration Diagnostics")
        st.dataframe(latest_calibration, width="stretch", hide_index=True)
        if postmortem_preview:
            with st.expander("Latest Postmortem Preview", expanded=False):
                st.code(postmortem_preview)

    st.subheader("Matured Outcome Rows")
    st.dataframe(outcomes, width="stretch", hide_index=True)
