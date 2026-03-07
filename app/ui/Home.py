# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    disk_report,
    load_ui_settings,
    provider_health_frame,
    recent_runs_frame,
)

st.set_page_config(page_title="StockMaster", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
runs = recent_runs_frame(settings, limit=10)
storage_report = disk_report(settings)
provider_health = provider_health_frame(settings)

st.title(settings.app.display_name)
st.caption("Foundation dashboard for the KR stock research platform skeleton.")

col_env, col_disk, col_status = st.columns(3)
col_env.metric("Environment", settings.app.env.upper(), settings.app.timezone)
col_disk.metric(
    "Disk Usage",
    f"{storage_report.usage_ratio:.1%}",
    f"{storage_report.used_gb:.2f} GB used",
)
col_status.metric("Watermark", storage_report.status.upper(), storage_report.message)

path_col, db_col = st.columns(2)
with path_col:
    st.subheader("Data Root")
    st.code(str(settings.paths.data_dir))
with db_col:
    st.subheader("DuckDB Path")
    st.code(str(settings.paths.duckdb_path))

st.subheader("Recent Runs")
if runs.empty:
    st.info("No run history yet. Execute `python scripts/bootstrap.py` first.")
else:
    st.dataframe(runs, use_container_width=True, hide_index=True)

st.subheader("Provider Health")
st.dataframe(provider_health, use_container_width=True, hide_index=True)

st.subheader("Implementation Checklist")
checklist = pd.DataFrame(
    [
        {"area": "Settings", "status": "implemented", "notes": "YAML + .env + typed models"},
        {
            "area": "Logging",
            "status": "implemented",
            "notes": "Structured console/file logging",
        },
        {
            "area": "Run manifest",
            "status": "implemented",
            "notes": "Bootstrap and skeleton jobs persist runs",
        },
        {
            "area": "Providers",
            "status": "stub",
            "notes": "Health checks and fetch placeholders only",
        },
        {
            "area": "Research engine",
            "status": "pending",
            "notes": "Feature store and ranking tickets pending",
        },
        {"area": "Reports / Discord", "status": "pending", "notes": "Follow-up ticket scope"},
    ]
)
st.dataframe(checklist, use_container_width=True, hide_index=True)
