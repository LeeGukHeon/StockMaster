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
    latest_scheduler_bundle_result_frame,
    latest_scheduler_state_frame,
    latest_step_failure_frame,
    latest_successful_pipeline_output_frame,
    load_ui_settings,
    localize_frame,
    scheduler_job_catalog_frame,
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
scheduler_catalog = scheduler_job_catalog_frame(settings)
scheduler_state = latest_scheduler_state_frame(settings, limit=30)
scheduler_runs = latest_scheduler_bundle_result_frame(settings, limit=30)

render_page_header(
    settings,
    page_name="헬스 대시보드",
    title="헬스 대시보드",
    description="전체 상태 요약, 최근 실행, 실패 단계, 의존성, 디스크 경보, 정리 이력, 잠금, 복구 대기열을 직접 확인하는 화면입니다.",
)

if health.empty:
    render_narrative_card(
        "상태 요약",
        "아직 상태 스냅샷이 없습니다. materialize_health_snapshots와 운영 유지보수 번들 상태를 먼저 확인하세요.",
    )
else:
    latest_row = health.iloc[0]
    render_narrative_card(
        "상태 요약",
        f"현재 상태 범위는 {latest_row.get('health_scope', '-')}, 상태는 {latest_row.get('status', '-')}, 구성요소는 {latest_row.get('component_name', '-')}입니다.",
    )

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("전체 상태 요약")
    st.dataframe(localize_frame(health), width="stretch", hide_index=True)
with summary_right:
    st.subheader("최신 정상 산출물")
    st.dataframe(localize_frame(latest_outputs), width="stretch", hide_index=True)

st.subheader("의존성 준비 상태")
st.dataframe(localize_frame(dependencies), width="stretch", hide_index=True)

st.subheader("스케줄러 개요")
scheduler_left, scheduler_right = st.columns(2)
with scheduler_left:
    st.dataframe(localize_frame(scheduler_catalog), width="stretch", hide_index=True)
with scheduler_right:
    if scheduler_state.empty:
        st.info("최근 스케줄러 상태가 없습니다.")
    else:
        st.dataframe(localize_frame(scheduler_state), width="stretch", hide_index=True)

with st.expander("최근 스케줄러 번들 결과", expanded=False):
    if scheduler_runs.empty:
        st.info("최근 스케줄러 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(scheduler_runs), width="stretch", hide_index=True)

run_left, run_right = st.columns(2)
with run_left:
    st.subheader("최근 실행 이력")
    st.dataframe(localize_frame(runs), width="stretch", hide_index=True)
with run_right:
    st.subheader("단계별 실패 현황")
    if step_failures.empty:
        st.success("최근 실패 단계가 없습니다.")
    else:
        st.dataframe(localize_frame(step_failures), width="stretch", hide_index=True)

ops_left, ops_right = st.columns(2)
with ops_left:
    st.subheader("디스크 사용량 / 경보선")
    st.dataframe(localize_frame(disk_events), width="stretch", hide_index=True)
    st.subheader("보관 정책 / 정리 이력")
    st.dataframe(localize_frame(cleanup_history), width="stretch", hide_index=True)
with ops_right:
    st.subheader("활성 잠금")
    if locks.empty:
        st.success("활성 잠금이 없습니다.")
    else:
        st.dataframe(localize_frame(locks), width="stretch", hide_index=True)
    st.subheader("복구 대기열")
    if recovery.empty:
        st.info("현재 복구 대기열은 비어 있습니다.")
    else:
        st.dataframe(localize_frame(recovery), width="stretch", hide_index=True)

alert_left, alert_right = st.columns(2)
with alert_left:
    st.subheader("알림")
    if alerts.empty:
        st.success("열린 알림 이벤트가 없습니다.")
    else:
        st.dataframe(localize_frame(alerts), width="stretch", hide_index=True)
with alert_right:
    st.subheader("활성 운영 정책")
    st.dataframe(localize_frame(active_policy), width="stretch", hide_index=True)

if ops_preview:
    with st.expander("최신 운영 리포트 미리보기", expanded=False):
        st.code(ops_preview)

render_page_footer(settings, page_name="헬스 대시보드")
