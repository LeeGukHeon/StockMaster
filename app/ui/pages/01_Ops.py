# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import disk_report, load_ui_settings, recent_runs_frame, watermark_frame

st.set_page_config(page_title="Ops", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
runs = recent_runs_frame(settings, limit=20)
storage_report = disk_report(settings)
watermarks = watermark_frame(settings)

st.title("Ops")
st.caption("Run manifest, storage watermark, and operational placeholders.")

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

st.subheader("Run Manifest")
if runs.empty:
    st.info("No runs recorded yet.")
else:
    st.dataframe(runs, use_container_width=True, hide_index=True)

st.subheader("Warnings")
st.info(
    "API error counters, retry telemetry, and Discord delivery status will land "
    "in follow-up tickets."
)
