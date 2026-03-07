# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.helpers import (
    calendar_summary_frame,
    disk_report,
    latest_calibration_diagnostic_frame,
    latest_discord_preview,
    latest_evaluation_comparison_frame,
    latest_evaluation_summary_frame,
    latest_feature_coverage_frame,
    latest_feature_sample_frame,
    latest_flow_summary_frame,
    latest_label_coverage_frame,
    latest_market_news_frame,
    latest_outcome_summary_frame,
    latest_postmortem_preview,
    latest_prediction_summary_frame,
    latest_regime_frame,
    latest_selection_validation_summary_frame,
    latest_sync_runs_frame,
    latest_validation_summary_frame,
    latest_version_frame,
    leaderboard_frame,
    leaderboard_grade_count_frame,
    load_ui_settings,
    market_pulse_frame,
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
latest_flow_summary = latest_flow_summary_frame(settings)
latest_prediction_summary = latest_prediction_summary_frame(settings)
latest_outcomes = latest_outcome_summary_frame(settings)
latest_evaluation_summary = latest_evaluation_summary_frame(settings, limit=12)
latest_evaluation_comparison = latest_evaluation_comparison_frame(settings)
latest_calibration = latest_calibration_diagnostic_frame(settings, limit=12)
market_pulse = market_pulse_frame(settings)
latest_market_news = latest_market_news_frame(settings, limit=5)
latest_feature_sample = latest_feature_sample_frame(settings, limit=5)
latest_feature_coverage = latest_feature_coverage_frame(settings)
latest_label_coverage = latest_label_coverage_frame(settings)
latest_regime = latest_regime_frame(settings)
latest_versions = latest_version_frame(settings)
selection_preview = leaderboard_frame(
    settings,
    horizon=5,
    limit=10,
    ranking_version=SELECTION_ENGINE_VERSION,
)
selection_grades = leaderboard_grade_count_frame(
    settings,
    horizon=5,
    ranking_version=SELECTION_ENGINE_VERSION,
)
explanatory_validation = latest_validation_summary_frame(settings, limit=8)
selection_validation = latest_selection_validation_summary_frame(settings, limit=8)
discord_preview = latest_discord_preview(settings)
postmortem_preview = latest_postmortem_preview(settings)

st.title(settings.app.display_name)
st.caption(
    "Operational home for the KR research stack. "
    "Explanatory ranking v0 remains inspection-only; "
    "selection engine v1 adds flow and proxy penalties."
)

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
if research_summary.empty or research_summary.iloc[0].isna().all():
    st.info("No core research data loaded yet.")
else:
    row = research_summary.iloc[0]
    top, mid, bottom = st.columns(3)
    top.metric("Latest OHLCV", str(row["latest_ohlcv_date"]), int(row["latest_ohlcv_rows"] or 0))
    mid.metric(
        "Latest Fundamentals",
        str(row["latest_fundamentals_date"]),
        int(row["latest_fundamentals_rows"] or 0),
    )
    bottom.metric(
        "Latest News",
        str(row["latest_news_date"]),
        (
            f"rows={int(row['latest_news_rows'] or 0)} "
            f"unmatched={int(row['latest_news_unmatched'] or 0)}"
        ),
    )
    top, mid, bottom = st.columns(3)
    top.metric("Latest Flow", str(row["latest_flow_date"]), int(row["latest_flow_rows"] or 0))
    mid.metric(
        "Latest Feature Snapshot",
        str(row["latest_feature_date"]),
        int(row["latest_feature_rows"] or 0),
    )
    bottom.metric(
        "Latest Labels",
        str(row["latest_label_date"]),
        int(row["latest_available_label_rows"] or 0),
    )
    top, mid, bottom = st.columns(3)
    top.metric(
        "Latest Selection v1",
        str(row["latest_selection_date"]),
        int(row["latest_selection_rows"] or 0),
    )
    mid.metric(
        "Latest Prediction",
        str(row["latest_prediction_date"]),
        int(row["latest_prediction_rows"] or 0),
    )
    bottom.metric(
        "Latest Explanatory v0",
        str(row["latest_explanatory_ranking_date"]),
        int(row["latest_explanatory_ranking_rows"] or 0),
    )
    top, mid, bottom = st.columns(3)
    top.metric(
        "Latest Outcomes",
        str(row["latest_outcome_date"]),
        int(row["latest_outcome_rows"] or 0),
    )
    mid.metric(
        "Latest Eval Summary",
        str(row["latest_evaluation_summary_date"]),
        int(row["latest_evaluation_summary_rows"] or 0),
    )
    bottom.metric(
        "Latest Calibration",
        str(row["latest_calibration_date"]),
        int(row["latest_calibration_rows"] or 0),
    )

st.subheader("Market Pulse and Selection")
pulse_left, pulse_right = st.columns((1, 2))
with pulse_left:
    if market_pulse.empty:
        st.info("Run regime, flow, and selection scripts to populate market pulse.")
    else:
        st.dataframe(market_pulse, use_container_width=True, hide_index=True)
    st.markdown("**Latest Flow Coverage**")
    if latest_flow_summary.empty:
        st.info("Investor flow summary is not available yet.")
    else:
        st.dataframe(latest_flow_summary, use_container_width=True, hide_index=True)
    st.markdown("**Latest Proxy Prediction Summary**")
    if latest_prediction_summary.empty:
        st.info("Proxy prediction bands are not available yet.")
    else:
        st.dataframe(latest_prediction_summary, use_container_width=True, hide_index=True)
with pulse_right:
    st.markdown("**Selection Engine v1 Preview (D+5)**")
    if selection_preview.empty:
        st.info("No selection engine v1 snapshot yet.")
    else:
        preview = selection_preview[
            [
                "symbol",
                "company_name",
                "market",
                "final_selection_value",
                "final_selection_rank_pct",
                "grade",
                "expected_excess_return",
                "lower_band",
                "upper_band",
                "reasons",
                "risks",
            ]
        ].copy()
        preview["final_selection_rank_pct"] = (
            pd.to_numeric(preview["final_selection_rank_pct"], errors="coerce") * 100.0
        ).round(1)
        st.dataframe(preview, use_container_width=True, hide_index=True)
    st.markdown("**Selection Grade Mix (D+5)**")
    if selection_grades.empty:
        st.info("No selection grade mix available yet.")
    else:
        st.dataframe(selection_grades, use_container_width=True, hide_index=True)

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

validation_left, validation_right = st.columns(2)
with validation_left:
    st.subheader("Selection Validation")
    if selection_validation.empty:
        st.info("No selection validation summary yet.")
    else:
        st.dataframe(selection_validation, use_container_width=True, hide_index=True)
with validation_right:
    st.subheader("Explanatory Validation")
    if explanatory_validation.empty:
        st.info("No explanatory validation summary yet.")
    else:
        st.dataframe(explanatory_validation, use_container_width=True, hide_index=True)

evaluation_left, evaluation_right = st.columns(2)
with evaluation_left:
    st.subheader("Latest Outcomes")
    if latest_outcomes.empty:
        st.info("No matured outcome summary yet.")
    else:
        st.dataframe(latest_outcomes, use_container_width=True, hide_index=True)
    st.subheader("Evaluation Comparison")
    if latest_evaluation_comparison.empty:
        st.info("No selection-vs-explanatory comparison yet.")
    else:
        st.dataframe(latest_evaluation_comparison, use_container_width=True, hide_index=True)
with evaluation_right:
    st.subheader("Rolling Evaluation Summary")
    if latest_evaluation_summary.empty:
        st.info("No evaluation summary rows yet.")
    else:
        st.dataframe(latest_evaluation_summary, use_container_width=True, hide_index=True)
    st.subheader("Calibration Diagnostics")
    if latest_calibration.empty:
        st.info("No calibration diagnostics yet.")
    else:
        st.dataframe(latest_calibration, use_container_width=True, hide_index=True)

news_left, news_right = st.columns(2)
with news_left:
    st.subheader("Latest Market-wide News")
    st.dataframe(latest_market_news, use_container_width=True, hide_index=True)
with news_right:
    st.subheader("Version Tracking")
    st.dataframe(latest_versions, use_container_width=True, hide_index=True)

st.subheader("Latest Regime Snapshot")
st.dataframe(latest_regime, use_container_width=True, hide_index=True)

if discord_preview:
    with st.expander("Latest Discord Preview", expanded=False):
        st.code(discord_preview)

if postmortem_preview:
    with st.expander("Latest Postmortem Preview", expanded=False):
        st.code(postmortem_preview)

st.subheader("Recent Runs")
if runs.empty:
    st.info("No run history yet. Execute `python scripts/bootstrap.py` first.")
else:
    st.dataframe(runs, use_container_width=True, hide_index=True)

st.subheader("Provider Health")
st.dataframe(provider_health, use_container_width=True, hide_index=True)
