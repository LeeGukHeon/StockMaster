# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_report_center,
)
from app.ui.helpers import (
    latest_active_ops_policy_frame,
    latest_alert_event_frame,
    latest_app_snapshot_frame,
    latest_disk_watermark_event_frame,
    latest_health_snapshot_frame,
    latest_job_runs_frame,
    latest_ops_report_preview,
    latest_pipeline_dependency_frame,
    latest_recovery_queue_frame,
    latest_release_candidate_check_frame,
    latest_report_index_frame,
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
snapshot = latest_app_snapshot_frame(settings)
health = latest_health_snapshot_frame(settings, limit=40)
runs = latest_job_runs_frame(settings, limit=20)
step_failures = latest_step_failure_frame(settings, limit=20)
dependencies = latest_pipeline_dependency_frame(settings, limit=30)
disk_events = latest_disk_watermark_event_frame(settings, limit=20)
cleanup_history = latest_retention_cleanup_frame(settings, limit=20)
recovery = latest_recovery_queue_frame(settings, limit=20)
alerts = latest_alert_event_frame(settings, limit=20)
active_policy = latest_active_ops_policy_frame(settings, limit=10)
latest_outputs = latest_successful_pipeline_output_frame(settings, limit=20)
release_checks = latest_release_candidate_check_frame(settings, limit=20)
latest_reports = latest_report_index_frame(settings, limit=20)
ops_preview = latest_ops_report_preview(settings)
scheduler_catalog = scheduler_job_catalog_frame(settings)
scheduler_state = latest_scheduler_state_frame(settings, limit=30)
scheduler_runs = latest_scheduler_bundle_result_frame(settings, limit=30)

render_page_header(
    settings,
    page_name="운영",
    title="운영",
    description="최근 실행 이력, 단계별 실패, 의존성, 디스크, 정리 이력, 복구, 알림, 최신 산출물을 한 번에 점검하는 운영 화면입니다.",
)

if snapshot.empty:
    render_narrative_card(
        "운영 요약",
        "현재 기준 스냅샷이 아직 없어 운영 기준점이 완전히 정리되지 않았습니다. 스냅샷, 리포트 목록, 신선도 빌드를 먼저 실행하세요.",
    )
else:
    row = snapshot.iloc[0]
    render_narrative_card(
        "운영 요약",
        f"현재 기준일은 {row['as_of_date']}이고 운영 상태는 {row['health_status']}입니다. 치명 알림 {int(row['critical_alert_count'] or 0)}건, 경고 알림 {int(row['warning_alert_count'] or 0)}건이 열려 있습니다.",
    )

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("전체 상태 요약")
    st.dataframe(localize_frame(health), width="stretch", hide_index=True)
with top_right:
    st.subheader("최신 정상 산출물")
    st.dataframe(localize_frame(latest_outputs), width="stretch", hide_index=True)

st.subheader("의존성 준비 상태")
st.dataframe(localize_frame(dependencies), width="stretch", hide_index=True)

st.subheader("자동 스케줄러 상태")
scheduler_left, scheduler_right = st.columns(2)
with scheduler_left:
    st.dataframe(localize_frame(scheduler_catalog), width="stretch", hide_index=True)
with scheduler_right:
    if scheduler_state.empty:
        st.info("최근 스케줄러 상태가 없습니다.")
    else:
        st.dataframe(localize_frame(scheduler_state), width="stretch", hide_index=True)

with st.expander("최근 자동 번들 실행 결과와 수동 실행 명령", expanded=False):
    if scheduler_runs.empty:
        st.info("최근 자동 번들 실행 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(scheduler_runs), width="stretch", hide_index=True)

run_left, run_right = st.columns(2)
with run_left:
    st.subheader("최근 실행 이력")
    st.dataframe(localize_frame(runs), width="stretch", hide_index=True)
with run_right:
    st.subheader("단계별 실패 현황")
    if step_failures.empty:
        st.success("최근 단계 실패가 없습니다.")
    else:
        st.dataframe(localize_frame(step_failures), width="stretch", hide_index=True)

ops_left, ops_right = st.columns(2)
with ops_left:
    st.subheader("디스크 사용량 / 경보선")
    st.dataframe(localize_frame(disk_events), width="stretch", hide_index=True)
    st.subheader("보관 정책 / 정리 이력")
    st.dataframe(localize_frame(cleanup_history), width="stretch", hide_index=True)
with ops_right:
    st.subheader("복구 대기열")
    if recovery.empty:
        st.info("현재 복구 대기열은 비어 있습니다.")
    else:
        st.dataframe(localize_frame(recovery), width="stretch", hide_index=True)
    st.subheader("활성 운영 정책")
    st.dataframe(localize_frame(active_policy), width="stretch", hide_index=True)

alert_left, alert_right = st.columns(2)
with alert_left:
    st.subheader("알림")
    if alerts.empty:
        st.success("열린 운영 알림이 없습니다.")
    else:
        st.dataframe(localize_frame(alerts), width="stretch", hide_index=True)
with alert_right:
    st.subheader("릴리스 점검 항목")
    if release_checks.empty:
        st.info("릴리스 점검 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(release_checks), width="stretch", hide_index=True)

st.subheader("통합 리포트 목록")
render_report_center(settings, limit=12)

if not latest_reports.empty:
    with st.expander("전체 리포트 목록", expanded=False):
        st.dataframe(localize_frame(latest_reports), width="stretch", hide_index=True)

if ops_preview:
    with st.expander("최신 운영 리포트 미리보기", expanded=False):
        st.code(ops_preview)

render_page_footer(settings, page_name="운영")
