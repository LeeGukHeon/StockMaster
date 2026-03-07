# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    latest_feature_coverage_frame,
    latest_feature_sample_frame,
    latest_fundamentals_sample_frame,
    latest_label_coverage_frame,
    latest_news_sample_frame,
    latest_ohlcv_sample_frame,
    latest_regime_frame,
    latest_validation_summary_frame,
    leaderboard_frame,
    load_ui_settings,
)

st.set_page_config(page_title="Research", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
ohlcv_sample = latest_ohlcv_sample_frame(settings, limit=10)
fundamentals_sample = latest_fundamentals_sample_frame(settings, limit=10)
news_sample = latest_news_sample_frame(settings, limit=10)
feature_sample = latest_feature_sample_frame(settings, limit=10)
feature_coverage = latest_feature_coverage_frame(settings)
label_coverage = latest_label_coverage_frame(settings)
regime_snapshot = latest_regime_frame(settings)
leaderboard_preview = leaderboard_frame(settings, horizon=5, limit=5)
validation_summary = latest_validation_summary_frame(settings, limit=10)

st.title("Research")
st.caption(
    "Inspection workspace for the feature store, labels, regime state, "
    "and explanatory ranking."
)

st.subheader("Active Research Layers")
st.markdown(
    """
    - Feature store snapshot with cross-sectional ranks and z-scores
    - Next-open D+1 / D+5 forward return labels and excess returns vs same-market baseline
    - Market regime classification for `KR_ALL`, `KOSPI`, and `KOSDAQ`
    - Explanatory ranking v0 with reason tags, risk flags, and grade bands
    """
)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("Latest Feature Coverage")
    if feature_coverage.empty:
        st.info("Run `python scripts/build_feature_store.py` first.")
    else:
        st.dataframe(feature_coverage, use_container_width=True, hide_index=True)
    st.subheader("Latest Label Coverage")
    if label_coverage.empty:
        st.info("Run `python scripts/build_forward_labels.py` first.")
    else:
        st.dataframe(label_coverage, use_container_width=True, hide_index=True)
with summary_right:
    st.subheader("Latest Regime Snapshot")
    if regime_snapshot.empty:
        st.info("Run `python scripts/build_market_regime_snapshot.py` first.")
    else:
        st.dataframe(regime_snapshot, use_container_width=True, hide_index=True)
    st.subheader("Validation Summary")
    if validation_summary.empty:
        st.info(
            "Run `python scripts/validate_explanatory_ranking.py` after "
            "historical rankings exist."
        )
    else:
        st.dataframe(validation_summary, use_container_width=True, hide_index=True)

st.subheader("Feature Matrix Sample")
st.dataframe(feature_sample, use_container_width=True, hide_index=True)

st.subheader("Leaderboard Preview (D+5)")
if leaderboard_preview.empty:
    st.info("Leaderboard preview will appear after ranking materialization.")
else:
    st.dataframe(
        leaderboard_preview[
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
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Latest OHLCV")
st.dataframe(ohlcv_sample, use_container_width=True, hide_index=True)

st.subheader("Latest Fundamentals Snapshot")
st.dataframe(fundamentals_sample, use_container_width=True, hide_index=True)

st.subheader("Latest News Metadata")
st.dataframe(news_sample, use_container_width=True, hide_index=True)
