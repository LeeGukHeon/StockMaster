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
    latest_feature_coverage_frame,
    latest_feature_sample_frame,
    latest_flow_summary_frame,
    latest_label_coverage_frame,
    latest_prediction_summary_frame,
    latest_regime_frame,
    latest_selection_validation_summary_frame,
    latest_validation_summary_frame,
    leaderboard_frame,
    load_ui_settings,
    market_pulse_frame,
)

st.set_page_config(page_title="Research", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
feature_sample = latest_feature_sample_frame(settings, limit=10)
feature_coverage = latest_feature_coverage_frame(settings)
label_coverage = latest_label_coverage_frame(settings)
flow_summary = latest_flow_summary_frame(settings)
prediction_summary = latest_prediction_summary_frame(settings)
regime_snapshot = latest_regime_frame(settings)
market_pulse = market_pulse_frame(settings)
selection_preview = leaderboard_frame(
    settings,
    horizon=5,
    limit=10,
    ranking_version=SELECTION_ENGINE_VERSION,
)
selection_validation = latest_selection_validation_summary_frame(settings, limit=10)
explanatory_validation = latest_validation_summary_frame(settings, limit=10)

st.title("Research")
st.caption(
    "Inspection workspace for feature store v2, labels, market regime, "
    "explanatory ranking v0, and selection engine v1."
)

st.subheader("Active Research Layers")
st.markdown(
    """
    - Feature store snapshot with price / fundamentals / news / flow / data-quality groups
    - Next-open D+1 / D+5 forward return labels and excess returns vs same-market baseline
    - Market regime classification for `KR_ALL`, `KOSPI`, and `KOSDAQ`
    - Explanatory ranking v0 for human inspection
    - Selection engine v1 with active `flow_score`, uncertainty proxy, and implementation penalty
    - Calibrated proxy prediction bands attached to latest selection rows
    """
)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("Latest Feature Coverage")
    st.dataframe(feature_coverage, width="stretch", hide_index=True)
    st.subheader("Latest Label Coverage")
    st.dataframe(label_coverage, width="stretch", hide_index=True)
    st.subheader("Latest Flow Coverage")
    st.dataframe(flow_summary, width="stretch", hide_index=True)
with summary_right:
    st.subheader("Market Pulse")
    st.dataframe(market_pulse, width="stretch", hide_index=True)
    st.subheader("Latest Regime Snapshot")
    st.dataframe(regime_snapshot, width="stretch", hide_index=True)
    st.subheader("Latest Proxy Prediction Summary")
    st.dataframe(prediction_summary, width="stretch", hide_index=True)

st.subheader("Feature Matrix Sample")
st.dataframe(feature_sample, width="stretch", hide_index=True)

st.subheader("Selection Engine v1 Preview (D+5)")
st.dataframe(
    selection_preview[
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

validation_left, validation_right = st.columns(2)
with validation_left:
    st.subheader("Selection Validation")
    st.dataframe(selection_validation, width="stretch", hide_index=True)
with validation_right:
    st.subheader("Explanatory Validation")
    st.dataframe(explanatory_validation, width="stretch", hide_index=True)
