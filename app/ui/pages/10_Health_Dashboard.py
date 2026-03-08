# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import render_narrative_card, render_page_footer, render_page_header
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
health = latest_health_snapshot_frame(settings, limit=100)
runs = latest_job_runs_frame(settings, limit=30)
step_failures = latest_step_failure_frame(settings, limit=30)
dependencies = latest_pipeline_dependency_frame(settings, limit=60)
disk_events = latest_disk_watermark_event_frame(settings, limit=30)
cleanup_history = latest_retention_cleanup_frame(settings, limit=30)
locks = latest_active_lock_frame(settings, limit=30)
recovery = latest_recovery_queue_frame(settings, limit=30)
alerts = latest_alert_event_frame(settings, limit=30)
active_policy = latest_active_ops_policy_frame(settings, limit=20)
latest_outputs = latest_successful_pipeline_output_frame(settings, limit=20)
ops_preview = latest_ops_report_preview(settings)

render_page_header(
    settings,
    page_name="헬스 대시보드",
    title="헬스 대시보드",
    description="Overall health summary, recent runs, failed steps, dependency readiness, disk watermark, cleanup, lock, recovery queue를 집중해서 봅니다.",
)

if health.empty:
    render_narrative_card(
        "Health Narrative",
        "아직 health snapshot이 없습니다. materialize_health_snapshots와 ops maintenance bundle을 먼저 확인하세요.",
    )
else:
    latest_row = health.iloc[0]
    render_narrative_card(
        "Health Narrative",
        f"현재 health scope는 {latest_row.get('health_scope', '-')}, 상태는 {latest_row.get('status', '-')}, "
        f"component는 {latest_row.get('component_name', '-')} 입니다.",
    )

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("Overall Health Summary")
    st.dataframe(localize_frame(health), width="stretch", hide_index=True)
with summary_right:
    st.subheader("Latest Successful Outputs")
    st.dataframe(localize_frame(latest_outputs), width="stretch", hide_index=True)

st.subheader("Dependency Readiness")
st.dataframe(localize_frame(dependencies), width="stretch", hide_index=True)

run_left, run_right = st.columns(2)
with run_left:
    st.subheader("Recent Runs")
    st.dataframe(localize_frame(runs), width="stretch", hide_index=True)
with run_right:
    st.subheader("Step Failure Explorer")
    if step_failures.empty:
        st.success("최근 실패 step이 없습니다.")
    else:
        st.dataframe(localize_frame(step_failures), width="stretch", hide_index=True)

ops_left, ops_right = st.columns(2)
with ops_left:
    st.subheader("Disk Usage / Watermark")
    st.dataframe(localize_frame(disk_events), width="stretch", hide_index=True)
    st.subheader("Retention & Cleanup History")
    st.dataframe(localize_frame(cleanup_history), width="stretch", hide_index=True)
with ops_right:
    st.subheader("Active Locks")
    if locks.empty:
        st.success("활성 lock이 없습니다.")
    else:
        st.dataframe(localize_frame(locks), width="stretch", hide_index=True)
    st.subheader("Recovery Queue")
    if recovery.empty:
        st.info("현재 recovery queue는 비어 있습니다.")
    else:
        st.dataframe(localize_frame(recovery), width="stretch", hide_index=True)

alert_left, alert_right = st.columns(2)
with alert_left:
    st.subheader("Alerts")
    if alerts.empty:
        st.success("열린 alert event가 없습니다.")
    else:
        st.dataframe(localize_frame(alerts), width="stretch", hide_index=True)
with alert_right:
    st.subheader("Active Ops Policy")
    st.dataframe(localize_frame(active_policy), width="stretch", hide_index=True)

if ops_preview:
    with st.expander("Latest Ops Report Preview", expanded=False):
        st.code(ops_preview)

render_page_footer(settings, page_name="헬스 대시보드")
