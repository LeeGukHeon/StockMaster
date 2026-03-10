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
    render_warning_banner,
)
from app.ui.helpers import (
    krx_service_registry_frame,
    latest_active_lock_frame,
    latest_alert_event_frame,
    latest_disk_watermark_event_frame,
    latest_health_snapshot_frame,
    latest_intraday_research_capability_frame,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_timing_calibration_frame,
    latest_job_runs_frame,
    latest_krx_budget_snapshot_frame,
    latest_krx_request_log_frame,
    latest_krx_service_status_frame,
    latest_krx_source_attribution_frame,
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
latest_outputs = latest_successful_pipeline_output_frame(settings, limit=20)
scheduler_catalog = scheduler_job_catalog_frame(settings)
scheduler_state = latest_scheduler_state_frame(settings, limit=30)
scheduler_runs = latest_scheduler_bundle_result_frame(settings, limit=30)
intraday_capability = latest_intraday_research_capability_frame(settings, limit=20)
intraday_strategy = latest_intraday_strategy_comparison_frame(settings, limit=20)
intraday_calibration = latest_intraday_timing_calibration_frame(settings, limit=20)
krx_budget = latest_krx_budget_snapshot_frame(settings, limit=10)
krx_status = latest_krx_service_status_frame(settings, limit=20)
krx_logs = latest_krx_request_log_frame(settings, limit=20)
krx_attribution = latest_krx_source_attribution_frame(settings, limit=20)
krx_registry = krx_service_registry_frame()

render_page_header(
    settings,
    page_name="헬스 대시보드",
    title="헬스 대시보드",
    description="운영 상태, 스케줄러 상태, 장중 리서치 기능 준비도, 복구 대기열을 한 화면에서 확인합니다.",
)
render_warning_banner(
    "INFO",
    "장중 리서치 기능은 리서치 전용 / 비매매 기준으로 켜져 있습니다. 자동 주문과 자동 승격은 수행하지 않습니다.",
)

if health.empty:
    render_narrative_card(
        "전체 상태 요약",
        "아직 health snapshot이 없습니다. 운영 보수 번들과 health materialization 상태를 먼저 확인하세요.",
    )
else:
    latest_row = health.iloc[0]
    render_narrative_card(
        "전체 상태 요약",
        (
            f"현재 범위는 {latest_row.get('health_scope', '-')}, 상태는 {latest_row.get('status', '-')}, "
            f"핵심 구성요소는 {latest_row.get('component_name', '-')}입니다."
        ),
    )

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("전체 헬스 요약")
    st.dataframe(localize_frame(health), width="stretch", hide_index=True)
with summary_right:
    st.subheader("최신 정상 산출물")
    st.dataframe(localize_frame(latest_outputs), width="stretch", hide_index=True)

st.subheader("의존성 준비 상태")
st.dataframe(localize_frame(dependencies), width="stretch", hide_index=True)

scheduler_left, scheduler_right = st.columns(2)
with scheduler_left:
    st.subheader("스케줄러 카탈로그")
    st.dataframe(localize_frame(scheduler_catalog), width="stretch", hide_index=True)
with scheduler_right:
    st.subheader("스케줄러 상태")
    if scheduler_state.empty:
        st.info("최근 scheduler 상태가 없습니다.")
    else:
        st.dataframe(localize_frame(scheduler_state), width="stretch", hide_index=True)

with st.expander("최근 스케줄러 실행 이력", expanded=False):
    if scheduler_runs.empty:
        st.info("최근 scheduler bundle 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(scheduler_runs), width="stretch", hide_index=True)

intraday_left, intraday_right = st.columns(2)
with intraday_left:
    st.subheader("장중 리서치 기능 상태")
    st.dataframe(localize_frame(intraday_capability), width="stretch", hide_index=True)
    st.subheader("장중 동일 종료 비교")
    st.dataframe(localize_frame(intraday_strategy), width="stretch", hide_index=True)
with intraday_right:
    st.subheader("장중 타이밍 보정")
    st.dataframe(localize_frame(intraday_calibration), width="stretch", hide_index=True)

krx_left, krx_right = st.columns(2)
with krx_left:
    st.subheader("KRX Live 서비스 상태")
    if krx_status.empty:
        st.info("아직 KRX live 상태 스냅샷이 없습니다.")
    else:
        st.dataframe(localize_frame(krx_status), width="stretch", hide_index=True)
    st.subheader("KRX 요청 예산")
    if krx_budget.empty:
        st.info("아직 KRX 요청 예산 스냅샷이 없습니다.")
    else:
        st.dataframe(localize_frame(krx_budget), width="stretch", hide_index=True)
with krx_right:
    st.subheader("KRX 서비스 레지스트리")
    st.dataframe(localize_frame(krx_registry), width="stretch", hide_index=True)
    with st.expander("KRX 요청 로그 / 출처 표기", expanded=False):
        if krx_logs.empty:
            st.info("아직 KRX 요청 로그가 없습니다.")
        else:
            st.dataframe(localize_frame(krx_logs), width="stretch", hide_index=True)
        if krx_attribution.empty:
            st.info("아직 KRX 출처 표기 스냅샷이 없습니다.")
        else:
            st.dataframe(localize_frame(krx_attribution), width="stretch", hide_index=True)

run_left, run_right = st.columns(2)
with run_left:
    st.subheader("최근 실행 이력")
    st.dataframe(localize_frame(runs), width="stretch", hide_index=True)
with run_right:
    st.subheader("단계 실패 탐색기")
    if step_failures.empty:
        st.success("최근 단계 실패가 없습니다.")
    else:
        st.dataframe(localize_frame(step_failures), width="stretch", hide_index=True)

ops_left, ops_right = st.columns(2)
with ops_left:
    st.subheader("디스크 사용량 / 워터마크")
    st.dataframe(localize_frame(disk_events), width="stretch", hide_index=True)
    st.subheader("보관 / 정리 이력")
    st.dataframe(localize_frame(cleanup_history), width="stretch", hide_index=True)
with ops_right:
    st.subheader("활성 락")
    if locks.empty:
        st.success("활성 락이 없습니다.")
    else:
        st.dataframe(localize_frame(locks), width="stretch", hide_index=True)
    st.subheader("복구 대기열")
    if recovery.empty:
        st.info("현재 복구 대기열은 비어 있습니다.")
    else:
        st.dataframe(localize_frame(recovery), width="stretch", hide_index=True)

st.subheader("경고")
if alerts.empty:
    st.success("열린 경고가 없습니다.")
else:
    st.dataframe(localize_frame(alerts), width="stretch", hide_index=True)

render_page_footer(settings, page_name="헬스 대시보드")
