# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_page_footer,
    render_page_header,
    render_record_cards,
    render_screen_guide,
    render_story_stream,
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
    load_ui_page_context,
    scheduler_job_catalog_frame,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="health_dashboard",
    page_title="헬스 대시보드",
)
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


def _display_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def _build_overview_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in health.head(4).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": _display_text(row.get("health_scope")),
                "title": _display_text(row.get("component_name")),
                "body": f"{_display_text(row.get('metric_name'))} / {_display_text(row.get('metric_value_text'), _display_text(row.get('metric_value_double')))}",
                "meta": _display_text(row.get("snapshot_at")),
                "badge": _display_text(row.get("status")),
                "tone": str(row.get("status", "neutral")).lower(),
            }
        )
    for row in latest_outputs.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Output",
                "title": _display_text(row.get("component_name")),
                "body": _display_text(row.get("metric_value_text")),
                "meta": _display_text(row.get("snapshot_at")),
                "badge": _display_text(row.get("status")),
                "tone": str(row.get("status", "neutral")).lower(),
            }
        )
    for row in alerts.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Alert",
                "title": _display_text(row.get("message")),
                "body": f"{_display_text(row.get('component_name'))} / {_display_text(row.get('severity'))}",
                "meta": _display_text(row.get("created_at")),
                "badge": _display_text(row.get("severity")),
                "tone": str(row.get("severity", "neutral")).lower(),
            }
        )
    return items


def _build_scheduler_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in dependencies.head(3).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Dependency",
                "title": _display_text(row.get("dependency_name")),
                "body": f"required {_display_text(row.get('required_state'))} / observed {_display_text(row.get('observed_state'))}",
                "meta": _display_text(row.get("checked_at")),
                "badge": _display_text(row.get("status")),
                "tone": str(row.get("status", "neutral")).lower(),
            }
        )
    for row in scheduler_runs.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Scheduler",
                "title": _display_text(row.get("job_name")),
                "body": f"{_display_text(row.get('status'))} / 기준일 {_display_text(row.get('as_of_date'))}",
                "meta": f"{_display_text(row.get('started_at'))} -> {_display_text(row.get('finished_at'))}",
                "badge": _display_text(row.get("status")),
                "tone": str(row.get("status", "neutral")).lower(),
            }
        )
    for row in scheduler_state.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "State",
                "title": _display_text(row.get("job_key")),
                "body": f"status {_display_text(row.get('status'))} / last run {_display_text(row.get('last_run_id'))}",
                "meta": _display_text(row.get("finished_at")),
                "badge": _display_text(row.get("status")),
                "tone": str(row.get("status", "neutral")).lower(),
            }
        )
    return items


def _build_intraday_krx_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in intraday_capability.head(2).to_dict(orient="records"):
        items.append(
            {
            "eyebrow": "Intraday",
            "title": _display_text(row.get("feature_slug")),
            "body": f"dependency {_display_text(row.get('dependency_ready_flag'))} / report {_display_text(row.get('report_available_flag'))}",
            "meta": _display_text(row.get("rollout_mode")),
            "badge": "RESEARCH",
            "tone": "neutral",
            }
        )
    for row in intraday_strategy.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Strategy",
                "title": _display_text(row.get("strategy_id")),
                "body": f"mean excess {_display_text(row.get('mean_realized_excess_return'))} / execution {_display_text(row.get('execution_rate'))}",
                "meta": f"horizon {_display_text(row.get('horizon'))}",
                "badge": "TRACE",
                "tone": "accent",
            }
        )
    for row in krx_status.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "KRX",
                "title": _display_text(row.get("display_name_ko")),
                "body": f"smoke {_display_text(row.get('last_smoke_status'))} / fallback {_display_text(row.get('fallback_mode'))}",
                "meta": _display_text(row.get("last_success_ts")),
                "badge": _display_text(row.get("last_smoke_status"), "KRX"),
                "tone": str(row.get("last_smoke_status", "neutral")).lower(),
            }
        )
    for row in krx_budget.head(1).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Budget",
                "title": f"KRX {_display_text(row.get('date_kst'))}",
                "body": f"used {_display_text(row.get('requests_used'))} / budget {_display_text(row.get('request_budget'))}",
                "meta": f"usage {_display_text(row.get('usage_ratio'))}",
                "badge": _display_text(row.get("throttle_state")),
                "tone": "warning",
            }
        )
    return items


def _build_ops_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in runs.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Run",
                "title": _display_text(row.get("job_name")),
                "body": f"status {_display_text(row.get('status'))} / 기준일 {_display_text(row.get('as_of_date'))}",
                "meta": f"{_display_text(row.get('started_at'))} -> {_display_text(row.get('finished_at'))}",
                "badge": _display_text(row.get("status")),
                "tone": str(row.get("status", "neutral")).lower(),
            }
        )
    for row in step_failures.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Step Failure",
                "title": _display_text(row.get("step_name")),
                "body": _display_text(row.get("error_message")),
                "meta": _display_text(row.get("job_run_id")),
                "badge": _display_text(row.get("status")),
                "tone": "failed",
            }
        )
    for row in recovery.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Recovery",
                "title": _display_text(row.get("action_type")),
                "body": f"status {_display_text(row.get('status'))} / lock {_display_text(row.get('lock_name'))}",
                "meta": _display_text(row.get("created_at")),
                "badge": _display_text(row.get("status")),
                "tone": str(row.get("status", "neutral")).lower(),
            }
        )
    return items


render_page_header(
    settings,
    page_name="헬스 대시보드",
    title="헬스 대시보드",
    description="운영 건강도, 스케줄러, intraday research, KRX 상태를 표보다 먼저 브리프로 읽는 대시보드입니다.",
)
render_screen_guide(
    summary="overview, scheduler, intraday/KRX, 운영 로그를 나눠서 기본은 이야기형 브리프로 보여줍니다.",
    bullets=[
        "개요에서 health snapshot과 최신 출력물, 경고를 먼저 봅니다.",
        "scheduler 브리프에서 dependency와 최근 실행 흐름을 확인합니다.",
        "intraday/KRX 브리프에서 연구 capability와 외부 상태를 함께 봅니다.",
    ],
)
render_warning_banner(
    "INFO",
    "intraday research와 KRX 상태는 운영 참고용 상태 정보이며 자동 주문이나 실거래 반영과는 연결되지 않습니다.",
)

view = st.segmented_control(
    "헬스 보기",
    options=["개요", "scheduler", "intraday/KRX", "운영 로그"],
    default="개요",
)

if view == "개요":
    render_story_stream(
        title="전체 건강도 브리프",
        summary="health snapshot, latest output, alert를 한 줄씩 이어 읽습니다.",
        items=_build_overview_items(),
        empty_message="개요 브리프 데이터가 없습니다.",
    )
    with st.expander("개요 원본 보기", expanded=False):
        render_record_cards(
            health,
            title="health 원본",
            primary_column="component_name",
            secondary_columns=["status", "health_scope"],
            detail_columns=["metric_name", "metric_value_text", "snapshot_at"],
            limit=12,
            empty_message="health 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            latest_outputs,
            title="latest output 원본",
            primary_column="component_name",
            secondary_columns=["status"],
            detail_columns=["metric_value_text", "snapshot_at"],
            limit=10,
            empty_message="latest output 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            alerts,
            title="alert 원본",
            primary_column="message",
            secondary_columns=["severity", "component_name"],
            detail_columns=["created_at", "alert_type", "status"],
            limit=10,
            empty_message="alert 원본이 없습니다.",
            show_table_expander=False,
        )

elif view == "scheduler":
    render_story_stream(
        title="scheduler 브리프",
        summary="dependency, scheduler run, scheduler state를 한 흐름으로 읽습니다.",
        items=_build_scheduler_items(),
        empty_message="scheduler 브리프 데이터가 없습니다.",
    )
    with st.expander("scheduler 원본 보기", expanded=False):
        render_record_cards(
            dependencies,
            title="dependency 원본",
            primary_column="dependency_name",
            secondary_columns=["status"],
            detail_columns=["required_state", "observed_state", "checked_at"],
            limit=12,
            empty_message="dependency 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            scheduler_catalog,
            title="scheduler catalog 원본",
            primary_column="label",
            secondary_columns=["schedule_label", "last_status"],
            detail_columns=["next_run_at", "heavy_job", "timer_name"],
            limit=10,
            empty_message="scheduler catalog 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            scheduler_runs,
            title="scheduler run 원본",
            primary_column="job_name",
            secondary_columns=["status"],
            detail_columns=["as_of_date", "started_at", "finished_at", "run_id"],
            limit=10,
            empty_message="scheduler run 원본이 없습니다.",
            show_table_expander=False,
        )

elif view == "intraday/KRX":
    render_story_stream(
        title="intraday / KRX 브리프",
        summary="research capability, strategy 비교, KRX 상태를 함께 읽습니다.",
        items=_build_intraday_krx_items(),
        empty_message="intraday/KRX 브리프 데이터가 없습니다.",
    )
    with st.expander("intraday / KRX 원본 보기", expanded=False):
        render_record_cards(
            intraday_capability,
            title="intraday capability 원본",
            primary_column="feature_slug",
            secondary_columns=["rollout_mode"],
            detail_columns=["dependency_ready_flag", "report_available_flag", "last_skip_reason"],
            limit=8,
            empty_message="intraday capability 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            intraday_strategy,
            title="intraday strategy 원본",
            primary_column="strategy_id",
            secondary_columns=["horizon"],
            detail_columns=["executed_count", "execution_rate", "mean_realized_excess_return"],
            limit=8,
            empty_message="intraday strategy 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            intraday_calibration,
            title="intraday timing calibration 원본",
            primary_column="grouping_value",
            secondary_columns=["horizon"],
            detail_columns=["hit_rate", "mean_timing_edge_vs_open_bps", "quality_flag"],
            limit=8,
            empty_message="intraday timing calibration 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            krx_status,
            title="KRX status 원본",
            primary_column="display_name_ko",
            secondary_columns=["last_smoke_status"],
            detail_columns=["last_success_ts", "fallback_mode", "last_http_status"],
            limit=8,
            empty_message="KRX status 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            krx_logs,
            title="KRX log 원본",
            primary_column="request_ts",
            secondary_columns=["service_slug", "status"],
            detail_columns=["http_status", "latency_ms", "rows_received"],
            limit=8,
            empty_message="KRX log 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            krx_attribution,
            title="KRX attribution 원본",
            primary_column="page_slug",
            secondary_columns=["component_slug", "source_label"],
            detail_columns=["snapshot_ts", "as_of_date", "active_flag"],
            limit=8,
            empty_message="KRX attribution 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            krx_registry,
            title="KRX registry 원본",
            primary_column="display_name_ko",
            secondary_columns=["service_slug", "category"],
            detail_columns=["approval_required", "request_cost_weight", "endpoint_url"],
            limit=8,
            empty_message="KRX registry 원본이 없습니다.",
            show_table_expander=False,
        )

else:
    render_story_stream(
        title="운영 로그 브리프",
        summary="run, step failure, recovery, lock 상태를 압축해서 보여줍니다.",
        items=_build_ops_items(),
        empty_message="운영 로그 브리프 데이터가 없습니다.",
    )
    with st.expander("운영 로그 원본 보기", expanded=False):
        render_record_cards(
            runs,
            title="run 원본",
            primary_column="job_name",
            secondary_columns=["status"],
            detail_columns=["as_of_date", "started_at", "finished_at", "run_id"],
            limit=12,
            empty_message="run 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            step_failures,
            title="step failure 원본",
            primary_column="step_name",
            secondary_columns=["status"],
            detail_columns=["job_run_id", "started_at", "error_message"],
            limit=10,
            empty_message="step failure 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            disk_events,
            title="disk 원본",
            primary_column="measured_at",
            secondary_columns=["disk_status"],
            detail_columns=["usage_ratio", "used_gb", "available_gb"],
            limit=8,
            empty_message="disk 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            cleanup_history,
            title="cleanup 원본",
            primary_column="started_at",
            secondary_columns=["status", "cleanup_scope"],
            detail_columns=["removed_file_count", "reclaimed_bytes", "notes"],
            limit=8,
            empty_message="cleanup 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            locks,
            title="lock 원본",
            primary_column="lock_name",
            secondary_columns=["job_name", "status"],
            detail_columns=["acquired_at", "expires_at", "release_reason"],
            limit=8,
            empty_message="lock 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            recovery,
            title="recovery 원본",
            primary_column="action_type",
            secondary_columns=["status"],
            detail_columns=["lock_name", "created_at", "notes"],
            limit=8,
            empty_message="recovery 원본이 없습니다.",
            show_table_expander=False,
        )

render_page_footer(settings, page_name="헬스 대시보드")
