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
    latest_sync_runs_frame,
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

st.title("Ops")
st.caption("Operational summary for provider health, symbol universe, and calendar sync.")

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
    if universe_summary.empty:
        st.info("Universe has not been synced yet.")
    else:
        st.dataframe(universe_summary, use_container_width=True, hide_index=True)
with summary_right:
    st.subheader("Trading Calendar")
    if calendar_summary.empty:
        st.info("Trading calendar has not been synced yet.")
    else:
        st.dataframe(calendar_summary, use_container_width=True, hide_index=True)

st.subheader("Latest Sync Status")
if latest_sync_runs.empty:
    st.info("No sync runs recorded yet.")
else:
    st.dataframe(latest_sync_runs, use_container_width=True, hide_index=True)

st.subheader("Research Data Freshness")
if research_summary.empty or research_summary.iloc[0].isna().all():
    st.info("Core research tables have not been populated yet.")
else:
    st.dataframe(research_summary, use_container_width=True, hide_index=True)

st.subheader("Provider Health")
st.dataframe(provider_health, use_container_width=True, hide_index=True)

st.subheader("Run Manifest")
if runs.empty:
    st.info("No runs recorded yet.")
else:
    st.dataframe(runs, use_container_width=True, hide_index=True)

st.subheader("Recent Failures")
if failed_runs.empty:
    st.success("No failed runs recorded.")
else:
    st.dataframe(failed_runs, use_container_width=True, hide_index=True)
