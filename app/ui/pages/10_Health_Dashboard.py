# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    latest_active_lock_frame,
    latest_active_ops_policy_frame,
    latest_alert_event_frame,
    latest_disk_watermark_event_frame,
    latest_health_snapshot_frame,
    latest_job_runs_frame,
    latest_ops_report_preview,
    latest_pipeline_dependency_frame,
    latest_recovery_queue_frame,
    latest_retention_cleanup_frame,
    latest_step_failure_frame,
    latest_successful_pipeline_output_frame,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
health = latest_health_snapshot_frame(settings, limit=200)
runs = latest_job_runs_frame(settings, limit=30)
step_failures = latest_step_failure_frame(settings, limit=30)
dependencies = latest_pipeline_dependency_frame(settings, limit=100)
disk_events = latest_disk_watermark_event_frame(settings, limit=30)
cleanup_history = latest_retention_cleanup_frame(settings, limit=30)
locks = latest_active_lock_frame(settings, limit=30)
recovery = latest_recovery_queue_frame(settings, limit=30)
alerts = latest_alert_event_frame(settings, limit=30)
active_policy = latest_active_ops_policy_frame(settings, limit=20)
latest_outputs = latest_successful_pipeline_output_frame(settings, limit=20)
ops_preview = latest_ops_report_preview(settings)

st.title("헬스 대시보드")
st.caption(
    "운영 안정화 관점에서 최근 run, step failure, dependency, disk watermark, cleanup, "
    "lock, recovery queue, alert, latest output를 확인하는 화면입니다."
)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("Overall Health Summary")
    if health.empty:
        st.info("아직 health snapshot이 없습니다.")
    else:
        st.dataframe(localize_frame(health), width="stretch", hide_index=True)
with summary_right:
    st.subheader("Latest Successful Outputs")
    if latest_outputs.empty:
        st.info("최신 pipeline output 요약이 없습니다.")
    else:
        st.dataframe(localize_frame(latest_outputs), width="stretch", hide_index=True)

st.subheader("Dependency Readiness")
if dependencies.empty:
    st.info("dependency readiness 결과가 없습니다.")
else:
    st.dataframe(localize_frame(dependencies), width="stretch", hide_index=True)

run_left, run_right = st.columns(2)
with run_left:
    st.subheader("Recent Runs")
    if runs.empty:
        st.info("최근 job run 이력이 없습니다.")
    else:
        st.dataframe(localize_frame(runs), width="stretch", hide_index=True)
with run_right:
    st.subheader("Step Failure Explorer")
    if step_failures.empty:
        st.info("최근 step failure가 없습니다.")
    else:
        st.dataframe(localize_frame(step_failures), width="stretch", hide_index=True)

ops_left, ops_right = st.columns(2)
with ops_left:
    st.subheader("Disk Usage / Watermark")
    if disk_events.empty:
        st.info("disk watermark event가 없습니다.")
    else:
        st.dataframe(localize_frame(disk_events), width="stretch", hide_index=True)
    st.subheader("Retention & Cleanup History")
    if cleanup_history.empty:
        st.info("cleanup history가 없습니다.")
    else:
        st.dataframe(localize_frame(cleanup_history), width="stretch", hide_index=True)
with ops_right:
    st.subheader("Active Locks")
    if locks.empty:
        st.info("active lock이 없습니다.")
    else:
        st.dataframe(localize_frame(locks), width="stretch", hide_index=True)
    st.subheader("Recovery Queue")
    if recovery.empty:
        st.info("recovery queue가 비어 있습니다.")
    else:
        st.dataframe(localize_frame(recovery), width="stretch", hide_index=True)

policy_left, policy_right = st.columns(2)
with policy_left:
    st.subheader("Alerts")
    if alerts.empty:
        st.info("alert event가 없습니다.")
    else:
        st.dataframe(localize_frame(alerts), width="stretch", hide_index=True)
with policy_right:
    st.subheader("Active Ops Policy")
    if active_policy.empty:
        st.info("active ops policy registry가 없습니다.")
    else:
        st.dataframe(localize_frame(active_policy), width="stretch", hide_index=True)

if ops_preview:
    with st.expander("Latest Ops Report Preview", expanded=False):
        st.code(ops_preview)
