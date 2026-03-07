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
    calendar_summary_frame,
    disk_report,
    latest_fundamentals_sample_frame,
    latest_news_sample_frame,
    latest_ohlcv_sample_frame,
    latest_sync_runs_frame,
    load_ui_settings,
    provider_health_frame,
    recent_runs_frame,
    research_data_summary_frame,
    universe_summary_frame,
)

st.set_page_config(page_title="StockMaster", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
runs = recent_runs_frame(settings, limit=10)
storage_report = disk_report(settings)
provider_health = provider_health_frame(settings)
universe_summary = universe_summary_frame(settings)
calendar_summary = calendar_summary_frame(settings)
latest_sync_runs = latest_sync_runs_frame(settings)
research_summary = research_data_summary_frame(settings)
latest_ohlcv = latest_ohlcv_sample_frame(settings, limit=5)
latest_fundamentals = latest_fundamentals_sample_frame(settings, limit=5)
latest_news = latest_news_sample_frame(settings, limit=5)

st.title(settings.app.display_name)
st.caption("Reference data dashboard for the KR stock research platform.")

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

st.subheader("Reference Data Summary")
summary_left, summary_right = st.columns(2)
with summary_left:
    if universe_summary.empty:
        st.info("No symbol universe loaded yet. Run `python scripts/sync_universe.py`.")
    else:
        row = universe_summary.iloc[0]
        metric_cols = st.columns(3)
        metric_cols[0].metric("Total Symbols", int(row["total_symbols"]))
        metric_cols[1].metric("KOSPI", int(row["kospi_symbols"]))
        metric_cols[2].metric("KOSDAQ", int(row["kosdaq_symbols"]))
        metric_cols = st.columns(2)
        metric_cols[0].metric("Active Common", int(row["active_common_stock_count"]))
        metric_cols[1].metric("DART Mapped", int(row["dart_mapped_symbols"]))
with summary_right:
    if calendar_summary.empty or pd.isna(calendar_summary.iloc[0]["min_trading_date"]):
        st.info("No trading calendar loaded yet. Run `python scripts/sync_trading_calendar.py`.")
    else:
        row = calendar_summary.iloc[0]
        metric_cols = st.columns(2)
        metric_cols[0].metric("Calendar Min", str(row["min_trading_date"]))
        metric_cols[1].metric("Calendar Max", str(row["max_trading_date"]))
        metric_cols = st.columns(2)
        metric_cols[0].metric("Trading Days", int(row["trading_days"]))
        metric_cols[1].metric("Override Days", int(row["override_days"]))

st.subheader("Latest Syncs")
if latest_sync_runs.empty:
    st.info("No sync history yet.")
else:
    st.dataframe(latest_sync_runs, use_container_width=True, hide_index=True)

st.subheader("Research Data Freshness")
if research_summary.empty or pd.isna(research_summary.iloc[0]["latest_ohlcv_date"]):
    st.info(
        "No core research data loaded yet. Run daily OHLCV, fundamentals, and news sync scripts."
    )
else:
    row = research_summary.iloc[0]
    top, mid, bottom = st.columns(3)
    top.metric("Latest OHLCV", str(row["latest_ohlcv_date"]), int(row["latest_ohlcv_rows"]))
    mid.metric(
        "Latest Fundamentals",
        str(row["latest_fundamentals_date"]),
        int(row["latest_fundamentals_rows"]),
    )
    bottom.metric(
        "Latest News",
        str(row["latest_news_date"]),
        f"rows={int(row['latest_news_rows'])} unmatched={int(row['latest_news_unmatched'])}",
    )

st.subheader("Recent Runs")
if runs.empty:
    st.info("No run history yet. Execute `python scripts/bootstrap.py` first.")
else:
    st.dataframe(runs, use_container_width=True, hide_index=True)

st.subheader("Provider Health")
st.dataframe(provider_health, use_container_width=True, hide_index=True)

sample_left, sample_right = st.columns(2)
with sample_left:
    st.subheader("Latest OHLCV Sample")
    st.dataframe(latest_ohlcv, use_container_width=True, hide_index=True)
    st.subheader("Latest Fundamentals Sample")
    st.dataframe(latest_fundamentals, use_container_width=True, hide_index=True)
with sample_right:
    st.subheader("Latest News Sample")
    st.dataframe(latest_news, use_container_width=True, hide_index=True)
