# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_data_sheet,
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_screen_guide,
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
    description="운영 상태, 스케줄러, 장중 리서치, KRX 상태를 표 대신 모바일용 시트로 확인하는 화면입니다.",
)
render_screen_guide(
    summary="한 화면에 모든 표를 밀어넣지 않고, 섹션을 나눠 필요한 운영 상태만 선택해 볼 수 있게 재구성했습니다.",
    bullets=[
        "개요에서는 전체 헬스와 최신 정상 산출물을 먼저 확인합니다.",
        "스케줄에서는 의존성, 스케줄러 카탈로그, 실행 이력을 봅니다.",
        "장중·KRX에서는 리서치 기능 상태와 KRX 호출 상태를 확인합니다.",
        "운영에서는 디스크, 정리 이력, 복구 대기열, 경고를 봅니다.",
    ],
)
render_warning_banner(
    "INFO",
    "장중 리서치 기능은 리서치 전용 / 비매매 기준으로만 체크됩니다. 자동 주문과 자동 배포는 여기에 포함되지 않습니다.",
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

view = st.segmented_control(
    "헬스 보기",
    options=["개요", "스케줄", "장중·KRX", "운영"],
    default="개요",
)

if view == "개요":
    render_data_sheet(
        health,
        title="전체 헬스 요약",
        primary_column="component_name",
        secondary_columns=["status", "health_scope"],
        detail_columns=["snapshot_ts", "status_reason", "action_hint"],
        limit=12,
        empty_message="헬스 요약이 없습니다.",
    )
    render_data_sheet(
        latest_outputs,
        title="최신 정상 산출물",
        limit=10,
        empty_message="최신 정상 산출물이 없습니다.",
    )
    render_data_sheet(
        alerts,
        title="열린 경고",
        primary_column="message",
        secondary_columns=["severity", "component_name"],
        detail_columns=["created_at", "alert_type", "status"],
        limit=8,
        empty_message="열린 경고가 없습니다.",
        table_expander_label="경고 전체 표 보기",
    )
elif view == "스케줄":
    render_data_sheet(
        dependencies,
        title="의존성 준비 상태",
        primary_column="dependency_name",
        secondary_columns=["status", "required_date"],
        detail_columns=["latest_available_date", "lag_days", "status_reason"],
        limit=12,
        empty_message="의존성 상태가 없습니다.",
    )
    render_data_sheet(
        scheduler_catalog,
        title="스케줄러 카탈로그",
        limit=10,
        empty_message="스케줄러 카탈로그가 없습니다.",
    )
    render_data_sheet(
        scheduler_state,
        title="스케줄러 상태",
        limit=10,
        empty_message="최근 스케줄러 상태가 없습니다.",
    )
    render_data_sheet(
        scheduler_runs,
        title="최근 스케줄러 실행 이력",
        primary_column="job_name",
        secondary_columns=["status", "as_of_date"],
        detail_columns=["started_at", "finished_at", "run_id"],
        limit=10,
        empty_message="최근 scheduler bundle 결과가 없습니다.",
        table_expander_label="스케줄러 실행 전체 표 보기",
    )
elif view == "장중·KRX":
    render_data_sheet(
        intraday_capability,
        title="장중 리서치 기능 상태",
        limit=8,
        empty_message="장중 리서치 기능 상태가 없습니다.",
    )
    render_data_sheet(
        intraday_strategy,
        title="장중 동일 종료 비교",
        limit=8,
        empty_message="장중 전략 비교 결과가 없습니다.",
    )
    render_data_sheet(
        intraday_calibration,
        title="장중 타이밍 보정",
        limit=8,
        empty_message="장중 타이밍 보정 결과가 없습니다.",
    )
    render_data_sheet(
        krx_status,
        title="KRX Live 서비스 상태",
        limit=8,
        empty_message="아직 KRX live 상태 스냅샷이 없습니다.",
    )
    render_data_sheet(
        krx_budget,
        title="KRX 요청 예산",
        limit=8,
        empty_message="아직 KRX 요청 예산 스냅샷이 없습니다.",
    )
    render_data_sheet(
        krx_registry,
        title="KRX 서비스 레지스트리",
        limit=8,
        empty_message="KRX 서비스 레지스트리가 없습니다.",
    )
    with st.expander("KRX 요청 로그 / 출처 표기", expanded=False):
        render_data_sheet(
            krx_logs,
            title="KRX 요청 로그",
            limit=10,
            empty_message="아직 KRX 요청 로그가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            krx_attribution,
            title="KRX 출처 표기",
            limit=10,
            empty_message="아직 KRX 출처 표기 스냅샷이 없습니다.",
            show_table_expander=False,
        )
else:
    render_data_sheet(
        runs,
        title="최근 실행 이력",
        primary_column="job_name",
        secondary_columns=["status", "as_of_date"],
        detail_columns=["started_at", "finished_at", "run_id"],
        limit=12,
        empty_message="최근 실행 이력이 없습니다.",
    )
    if step_failures.empty:
        st.success("최근 단계 실패가 없습니다.")
    else:
        render_data_sheet(
            step_failures,
            title="단계 실패 탐색기",
            primary_column="step_name",
            secondary_columns=["job_name", "status"],
            detail_columns=["failed_at", "error_message"],
            limit=10,
            empty_message="최근 단계 실패가 없습니다.",
        )
    render_data_sheet(
        disk_events,
        title="디스크 사용량 / 워터마크",
        limit=10,
        empty_message="디스크 워터마크 이력이 없습니다.",
    )
    render_data_sheet(
        cleanup_history,
        title="보관 / 정리 이력",
        limit=10,
        empty_message="보관 정책 정리 이력이 없습니다.",
    )
    if locks.empty:
        st.success("활성 락이 없습니다.")
    else:
        render_data_sheet(
            locks,
            title="활성 락",
            limit=8,
            empty_message="활성 락이 없습니다.",
        )
    render_data_sheet(
        recovery,
        title="복구 대기열",
        limit=10,
        empty_message="현재 복구 대기열은 비어 있습니다.",
    )

render_page_footer(settings, page_name="헬스 대시보드")
