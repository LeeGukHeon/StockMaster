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
    latest_feature_coverage_frame,
    latest_feature_sample_frame,
    latest_fundamentals_sample_frame,
    latest_label_coverage_frame,
    latest_news_sample_frame,
    latest_ohlcv_sample_frame,
    latest_regime_frame,
    latest_sync_runs_frame,
    latest_validation_summary_frame,
    latest_version_frame,
    leaderboard_frame,
    leaderboard_grade_count_frame,
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
latest_feature_sample = latest_feature_sample_frame(settings, limit=5)
latest_feature_coverage = latest_feature_coverage_frame(settings)
latest_label_coverage = latest_label_coverage_frame(settings)
latest_regime = latest_regime_frame(settings)
latest_versions = latest_version_frame(settings)
leaderboard = leaderboard_frame(settings, horizon=5, limit=10)
leaderboard_grades = leaderboard_grade_count_frame(settings, horizon=5)
validation_summary = latest_validation_summary_frame(settings, limit=12)

st.title(settings.app.display_name)
st.caption("Operational home for the KR research stack. Ranking is explanatory, not predictive.")

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
    top, mid, bottom = st.columns(3)
    top.metric(
        "Latest Feature Snapshot",
        str(row["latest_feature_date"]),
        int(row["latest_feature_rows"]) if pd.notna(row["latest_feature_rows"]) else 0,
    )
    mid.metric(
        "Latest Available Labels",
        str(row["latest_label_date"]),
        int(row["latest_available_label_rows"])
        if pd.notna(row["latest_available_label_rows"])
        else 0,
    )
    bottom.metric(
        "Latest Ranking",
        str(row["latest_ranking_date"]),
        int(row["latest_ranking_rows"]) if pd.notna(row["latest_ranking_rows"]) else 0,
    )
    st.caption(f"Latest regime snapshot date: {row['latest_regime_date']}")

st.subheader("Feature, Regime, and Ranking Snapshot")
snapshot_left, snapshot_right = st.columns((1, 2))
with snapshot_left:
    if latest_versions.empty:
        st.info("No feature or ranking version metadata yet.")
    else:
        st.dataframe(latest_versions, use_container_width=True, hide_index=True)
    st.markdown("**Latest Label Coverage**")
    if latest_label_coverage.empty:
        st.info("Forward labels are not built yet.")
    else:
        st.dataframe(latest_label_coverage, use_container_width=True, hide_index=True)
    st.markdown("**Latest Regime**")
    if latest_regime.empty:
        st.info("Run `python scripts/build_market_regime_snapshot.py` first.")
    else:
        st.dataframe(latest_regime, use_container_width=True, hide_index=True)
with snapshot_right:
    st.markdown("**Leaderboard Preview (D+5)**")
    if leaderboard.empty:
        st.info("No ranking snapshot yet. Run the TICKET-003 scripts first.")
    else:
        preview = leaderboard[
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
        preview["final_selection_rank_pct"] = (
            pd.to_numeric(preview["final_selection_rank_pct"], errors="coerce") * 100.0
        ).round(1)
        st.dataframe(preview, use_container_width=True, hide_index=True)
    st.markdown("**Grade Mix (D+5)**")
    if leaderboard_grades.empty:
        st.info("No ranking grade mix available yet.")
    else:
        st.dataframe(leaderboard_grades, use_container_width=True, hide_index=True)

coverage_left, coverage_right = st.columns(2)
with coverage_left:
    st.subheader("Feature Coverage")
    if latest_feature_coverage.empty:
        st.info("Feature coverage will appear after the feature store is built.")
    else:
        st.dataframe(latest_feature_coverage, use_container_width=True, hide_index=True)
with coverage_right:
    st.subheader("Latest Feature Sample")
    if latest_feature_sample.empty:
        st.info("No feature matrix sample available yet.")
    else:
        st.dataframe(latest_feature_sample, use_container_width=True, hide_index=True)

st.subheader("Recent Validation")
if validation_summary.empty:
    st.info("No ranking validation summary yet.")
else:
    st.dataframe(validation_summary, use_container_width=True, hide_index=True)

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
