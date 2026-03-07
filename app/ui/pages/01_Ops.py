# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    calendar_summary_frame,
    disk_report,
    latest_discord_preview,
    latest_feature_coverage_frame,
    latest_flow_summary_frame,
    latest_label_coverage_frame,
    latest_prediction_summary_frame,
    latest_regime_frame,
    latest_selection_validation_summary_frame,
    latest_sync_runs_frame,
    latest_validation_summary_frame,
    latest_version_frame,
    load_ui_settings,
    provider_health_frame,
    recent_failure_runs_frame,
    recent_runs_frame,
    research_data_summary_frame,
    universe_summary_frame,
    watermark_frame,
)

st.set_page_config(page_title="Ops", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
runs = recent_runs_frame(settings, limit=20)
storage_report = disk_report(settings)
watermarks = watermark_frame(settings)
universe_summary = universe_summary_frame(settings)
calendar_summary = calendar_summary_frame(settings)
provider_health = provider_health_frame(settings)
latest_sync_runs = latest_sync_runs_frame(settings)
research_summary = research_data_summary_frame(settings)
failed_runs = recent_failure_runs_frame(settings)
feature_coverage = latest_feature_coverage_frame(settings)
label_coverage = latest_label_coverage_frame(settings)
flow_summary = latest_flow_summary_frame(settings)
prediction_summary = latest_prediction_summary_frame(settings)
latest_regime = latest_regime_frame(settings)
latest_versions = latest_version_frame(settings)
selection_validation = latest_selection_validation_summary_frame(settings, limit=20)
explanatory_validation = latest_validation_summary_frame(settings, limit=20)
discord_preview = latest_discord_preview(settings)

st.title("Ops")
st.caption(
    "Operational summary for ingestion, feature builds, explanatory ranking v0, "
    "selection engine v1, proxy bands, and Discord report rendering."
)

top_left, top_right = st.columns(2)
with top_left:
    st.metric(
        "Current Usage",
        f"{storage_report.usage_ratio:.1%}",
        f"{storage_report.used_gb:.2f} GB used",
    )
    st.write(storage_report.message)
with top_right:
    st.metric("Available", f"{storage_report.available_gb:.2f} GB", storage_report.status.upper())
    st.dataframe(watermarks, use_container_width=True, hide_index=True)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("Universe")
    st.dataframe(universe_summary, use_container_width=True, hide_index=True)
with summary_right:
    st.subheader("Trading Calendar")
    st.dataframe(calendar_summary, use_container_width=True, hide_index=True)

st.subheader("Latest Sync Status")
st.dataframe(latest_sync_runs, use_container_width=True, hide_index=True)

st.subheader("Research Data Freshness")
st.dataframe(research_summary, use_container_width=True, hide_index=True)

ops_left, ops_right = st.columns(2)
with ops_left:
    st.subheader("Feature Coverage")
    st.dataframe(feature_coverage, use_container_width=True, hide_index=True)
    st.subheader("Label Coverage")
    st.dataframe(label_coverage, use_container_width=True, hide_index=True)
    st.subheader("Flow Summary")
    st.dataframe(flow_summary, use_container_width=True, hide_index=True)
with ops_right:
    st.subheader("Version Tracking")
    st.dataframe(latest_versions, use_container_width=True, hide_index=True)
    st.subheader("Prediction Summary")
    st.dataframe(prediction_summary, use_container_width=True, hide_index=True)
    st.subheader("Latest Regime Snapshot")
    st.dataframe(latest_regime, use_container_width=True, hide_index=True)

validation_left, validation_right = st.columns(2)
with validation_left:
    st.subheader("Selection Validation")
    st.dataframe(selection_validation, use_container_width=True, hide_index=True)
with validation_right:
    st.subheader("Explanatory Validation")
    st.dataframe(explanatory_validation, use_container_width=True, hide_index=True)

st.subheader("Provider Health")
st.dataframe(provider_health, use_container_width=True, hide_index=True)

if discord_preview:
    with st.expander("Latest Discord Preview", expanded=False):
        st.code(discord_preview)

st.subheader("Run Manifest")
st.dataframe(runs, use_container_width=True, hide_index=True)

st.subheader("Recent Failures")
if failed_runs.empty:
    st.success("No failed runs recorded.")
else:
    st.dataframe(failed_runs, use_container_width=True, hide_index=True)
