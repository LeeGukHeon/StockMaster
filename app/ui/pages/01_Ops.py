# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
from app.ml.active import freeze_alpha_active_model, rollback_alpha_active_model
from app.ml.constants import ALPHA_CANDIDATE_MODEL_SPECS, MODEL_SPEC_ID
from app.ml.promotion import format_alpha_model_spec_id
from app.ui.components import (
    render_page_footer,
    render_page_header,
    render_record_cards,
    render_report_preview,
    render_screen_guide,
    render_story_stream,
)
from app.ui.helpers import (
    format_ui_date,
    format_ui_value,
    krx_service_registry_frame,
    latest_active_ops_policy_frame,
    latest_alert_event_frame,
    latest_alpha_active_model_frame,
    latest_alpha_model_spec_frame,
    latest_alpha_promotion_summary_frame,
    latest_alpha_rollback_frame,
    latest_alpha_training_candidate_frame,
    latest_app_snapshot_frame,
    latest_disk_watermark_event_frame,
    latest_health_snapshot_frame,
    latest_job_runs_frame,
    latest_krx_budget_snapshot_frame,
    latest_krx_request_log_frame,
    latest_krx_service_status_frame,
    latest_krx_source_attribution_frame,
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
    load_ui_page_context,
    scheduler_job_catalog_frame,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="ops",
    page_title="운영",
)
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
alpha_promotion = latest_alpha_promotion_summary_frame(settings, limit=10)
alpha_active_models = latest_alpha_active_model_frame(settings, limit=10)
alpha_candidate_training = latest_alpha_training_candidate_frame(settings, limit=10)
alpha_model_specs = latest_alpha_model_spec_frame(settings, limit=10)
alpha_rollbacks = latest_alpha_rollback_frame(settings, limit=10)
latest_outputs = latest_successful_pipeline_output_frame(settings, limit=20)
release_checks = latest_release_candidate_check_frame(settings, limit=20)
latest_reports = latest_report_index_frame(settings, limit=20)
ops_preview = latest_ops_report_preview(settings)
scheduler_catalog = scheduler_job_catalog_frame(settings)
scheduler_state = latest_scheduler_state_frame(settings, limit=30)
scheduler_runs = latest_scheduler_bundle_result_frame(settings, limit=30)
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
    if not snapshot.empty:
        row = snapshot.iloc[0]
        items.append(
            {
                "eyebrow": "Snapshot",
                "title": f"기준일 {format_ui_date(row.get('as_of_date'))}",
                "body": (
                    f"운영상태 {format_ui_value('health_status', row.get('health_status'))} / "
                    f"치명 {int(row.get('critical_alert_count') or 0)} / "
                    f"경고 {int(row.get('warning_alert_count') or 0)}"
                ),
                "meta": f"ops policy {_display_text(row.get('active_ops_policy_id'))}",
                "badge": format_ui_value("health_status", row.get("health_status")),
                "tone": str(row.get("health_status", "neutral")).lower(),
            }
        )
    for row in health.head(3).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Health",
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
    return items


def _build_model_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in alpha_active_models.head(4).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": f"H{_display_text(row.get('horizon'))}",
                "title": _display_text(row.get("model_spec_id")),
                "body": (
                    f"train end {_display_text(row.get('train_end_date'))} / "
                    f"source {_display_text(row.get('source_type'))} / "
                    f"promotion {_display_text(row.get('promotion_type'))}"
                ),
                "meta": _display_text(row.get("effective_from_date")),
                "badge": "ACTIVE",
                "tone": "positive",
            }
        )
    for row in alpha_promotion.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": _display_text(row.get("summary_title"), "Promotion"),
                "title": f"{_display_text(row.get('active_model_label'))} vs {_display_text(row.get('comparison_model_label'))}",
                "body": f"{_display_text(row.get('decision_label'))} / gap {_display_text(row.get('promotion_gap'))}",
                "meta": f"표본 {_display_text(row.get('sample_count'))} · {_display_text(row.get('window_end'))}",
                "badge": _display_text(row.get("decision_label"), "MODEL"),
                "tone": "accent",
            }
        )
    for row in alpha_rollbacks.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Rollback",
                "title": _display_text(row.get("model_spec_id")),
                "body": f"horizon {_display_text(row.get('horizon'))} / {_display_text(row.get('promotion_type'))}",
                "meta": _display_text(row.get("effective_from_date")),
                "badge": "ROLLBACK",
                "tone": "warning",
            }
        )
    return items


def _build_schedule_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in scheduler_runs.head(3).to_dict(orient="records"):
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
                "title": f"KRX request budget {_display_text(row.get('date_kst'))}",
                "body": f"used {_display_text(row.get('requests_used'))} / budget {_display_text(row.get('request_budget'))}",
                "meta": f"usage ratio {_display_text(row.get('usage_ratio'))}",
                "badge": _display_text(row.get("throttle_state")),
                "tone": "accent",
            }
        )
    return items


def _build_log_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
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
    page_name="운영",
    title="운영",
    description="배치, 모델, 외부 연동, 경고 로그를 표보다 먼저 브리프 형태로 읽는 모바일 우선 운영 화면입니다.",
)
render_screen_guide(
    summary="운영 요약을 먼저 읽고, 원본 표와 조작은 아래 expander 안에서만 펼치도록 정리했습니다.",
    bullets=[
        "요약에서는 건강도와 최근 출력물, 알림 흐름을 먼저 봅니다.",
        "모델 탭에서는 active alpha와 promotion 흐름을 짧게 읽습니다.",
        "필요할 때만 원본 로그와 수동 조작 도구를 펼칩니다.",
    ],
)

alpha_notice = st.session_state.pop("alpha_ops_notice", None)
if isinstance(alpha_notice, dict):
    notice_level = str(alpha_notice.get("level") or "info")
    notice_message = str(alpha_notice.get("message") or "").strip()
    if notice_message:
        getattr(st, notice_level, st.info)(notice_message)

view = st.segmented_control(
    "운영 보기",
    options=["요약", "모델", "스케줄/외부", "로그"],
    default="요약",
)

if view == "요약":
    render_story_stream(
        title="운영 브리프",
        summary="현재 snapshot, health, latest output을 한 흐름으로 읽습니다.",
        items=_build_overview_items(),
        empty_message="운영 브리프 데이터가 없습니다.",
    )
    with st.expander("요약 원본 보기", expanded=False):
        render_record_cards(
            health,
            title="health 원본",
            primary_column="component_name",
            secondary_columns=["status", "health_scope"],
            detail_columns=["metric_name", "metric_value_text", "snapshot_at"],
            limit=10,
            empty_message="health 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            latest_outputs,
            title="최신 출력물 원본",
            primary_column="component_name",
            secondary_columns=["status"],
            detail_columns=["metric_value_text", "snapshot_at"],
            limit=10,
            empty_message="최신 출력물 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            dependencies,
            title="파이프라인 의존성 원본",
            primary_column="dependency_name",
            secondary_columns=["status"],
            detail_columns=["required_state", "observed_state", "checked_at"],
            limit=10,
            empty_message="의존성 원본이 없습니다.",
            show_table_expander=False,
        )

elif view == "모델":
    render_story_stream(
        title="알파 모델 브리프",
        summary="active alpha, 최근 promotion 판단, rollback 이력을 짧게 읽습니다.",
        items=_build_model_items(),
        empty_message="알파 모델 브리프 데이터가 없습니다.",
    )
    alpha_default_date = today_local(settings.app.timezone)
    alpha_default_train_end_date = alpha_default_date
    if not alpha_candidate_training.empty and "train_end_date" in alpha_candidate_training.columns:
        train_end_dates = alpha_candidate_training["train_end_date"].dropna().tolist()
        if train_end_dates:
            alpha_default_train_end_date = max(
                date.fromisoformat(str(value)) if isinstance(value, str) else value
                for value in train_end_dates
            )
    alpha_spec_options = (
        alpha_model_specs["model_spec_id"].astype(str).tolist()
        if not alpha_model_specs.empty and "model_spec_id" in alpha_model_specs.columns
        else [spec.model_spec_id for spec in ALPHA_CANDIDATE_MODEL_SPECS]
    )
    alpha_default_spec_id = MODEL_SPEC_ID if MODEL_SPEC_ID in alpha_spec_options else alpha_spec_options[0]
    alpha_default_spec_index = alpha_spec_options.index(alpha_default_spec_id)

    with st.expander("알파 모델 원본과 수동 조작", expanded=False):
        render_record_cards(
            alpha_active_models,
            title="active alpha 원본",
            primary_column="model_spec_id",
            secondary_columns=["horizon"],
            detail_columns=["train_end_date", "source_type", "promotion_type", "effective_from_date"],
            limit=10,
            empty_message="active alpha 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            alpha_candidate_training,
            title="최근 학습 원본",
            primary_column="model_spec_id",
            secondary_columns=["horizon"],
            detail_columns=["train_end_date", "estimation_scheme", "rolling_window_days"],
            limit=10,
            empty_message="최근 학습 원본이 없습니다.",
            show_table_expander=False,
        )

        alpha_control_left, alpha_control_right = st.columns(2)
        with alpha_control_left:
            with st.form("freeze_alpha_active_model_form", clear_on_submit=False):
                freeze_effective_date = st.date_input("반영 시작일", value=alpha_default_date)
                freeze_train_end_date = st.date_input("참조 학습 종료일", value=alpha_default_train_end_date)
                freeze_spec_id = st.selectbox(
                    "모델 묶음",
                    options=alpha_spec_options,
                    index=alpha_default_spec_index,
                    format_func=format_alpha_model_spec_id,
                )
                freeze_horizons = st.multiselect("대상 horizon", options=[1, 5], default=[1, 5])
                freeze_note = st.text_input("반영 메모", value="운영 화면에서 수동 반영")
                freeze_confirm = st.checkbox("비교를 확인하고 현재 모델 대신 이 후보를 반영하는 데 동의합니다.")
                if st.form_submit_button("알파 모델 수동 반영"):
                    if not freeze_confirm:
                        st.warning("먼저 확인 체크를 켜 주세요.")
                    elif not freeze_horizons:
                        st.warning("최소 한 개 이상의 horizon을 선택해 주세요.")
                    else:
                        try:
                            with st.spinner("알파 모델을 반영하는 중입니다..."):
                                freeze_result = freeze_alpha_active_model(
                                    settings,
                                    as_of_date=freeze_effective_date,
                                    source="ops_manual_freeze_ui",
                                    note=freeze_note,
                                    horizons=[int(value) for value in freeze_horizons],
                                    model_spec_id=str(freeze_spec_id),
                                    train_end_date=freeze_train_end_date,
                                )
                            st.session_state["alpha_ops_notice"] = {
                                "level": "success" if freeze_result.row_count > 0 else "warning",
                                "message": f"알파 모델 반영 완료. run_id={freeze_result.run_id} rows={freeze_result.row_count}",
                            }
                            st.rerun()
                        except Exception as exc:  # pragma: no cover
                            st.error(f"알파 모델 반영 실패: {exc}")

        with alpha_control_right:
            with st.form("rollback_alpha_active_model_form", clear_on_submit=False):
                rollback_effective_date = st.date_input("롤백 기준일", value=alpha_default_date)
                rollback_horizons = st.multiselect("롤백 horizon", options=[1, 5], default=[1, 5])
                rollback_note = st.text_input("롤백 메모", value="운영 화면에서 수동 롤백")
                rollback_confirm = st.checkbox("현재 모델을 직전 active 모델로 되돌리는 데 동의합니다.")
                if st.form_submit_button("알파 모델 롤백"):
                    if not rollback_confirm:
                        st.warning("먼저 확인 체크를 켜 주세요.")
                    elif not rollback_horizons:
                        st.warning("최소 한 개 이상의 horizon을 선택해 주세요.")
                    else:
                        try:
                            with st.spinner("알파 모델을 롤백하는 중입니다..."):
                                rollback_result = rollback_alpha_active_model(
                                    settings,
                                    as_of_date=rollback_effective_date,
                                    horizons=[int(value) for value in rollback_horizons],
                                    note=rollback_note,
                                )
                            st.session_state["alpha_ops_notice"] = {
                                "level": "success" if rollback_result.row_count > 0 else "warning",
                                "message": f"알파 모델 롤백 완료. run_id={rollback_result.run_id} rows={rollback_result.row_count}",
                            }
                            st.rerun()
                        except Exception as exc:  # pragma: no cover
                            st.error(f"알파 모델 롤백 실패: {exc}")

elif view == "스케줄/외부":
    render_story_stream(
        title="스케줄·외부 연동 브리프",
        summary="최근 scheduler 실행 결과와 KRX 상태를 한 흐름으로 정리했습니다.",
        items=_build_schedule_items(),
        empty_message="스케줄/외부 브리프 데이터가 없습니다.",
    )
    with st.expander("스케줄/외부 원본 보기", expanded=False):
        render_record_cards(
            scheduler_catalog,
            title="scheduler 카탈로그 원본",
            primary_column="label",
            secondary_columns=["schedule_label", "last_status"],
            detail_columns=["next_run_at", "heavy_job", "timer_name"],
            limit=10,
            empty_message="scheduler 카탈로그 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            scheduler_runs,
            title="최근 bundle 실행 원본",
            primary_column="job_name",
            secondary_columns=["status"],
            detail_columns=["as_of_date", "started_at", "finished_at", "run_id"],
            limit=10,
            empty_message="최근 bundle 실행 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            krx_status,
            title="KRX 상태 원본",
            primary_column="display_name_ko",
            secondary_columns=["last_smoke_status"],
            detail_columns=["last_success_ts", "fallback_mode", "last_http_status"],
            limit=10,
            empty_message="KRX 상태 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            krx_budget,
            title="KRX 예산 원본",
            primary_column="date_kst",
            secondary_columns=["requests_used"],
            detail_columns=["request_budget", "usage_ratio", "throttle_state"],
            limit=10,
            empty_message="KRX 예산 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            krx_logs,
            title="KRX 요청 로그 원본",
            primary_column="request_ts",
            secondary_columns=["service_slug", "status"],
            detail_columns=["http_status", "latency_ms", "rows_received"],
            limit=8,
            empty_message="KRX 로그 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            krx_attribution,
            title="KRX 출처 원본",
            primary_column="page_slug",
            secondary_columns=["component_slug", "source_label"],
            detail_columns=["snapshot_ts", "as_of_date", "active_flag"],
            limit=8,
            empty_message="KRX 출처 원본이 없습니다.",
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
        title="로그 브리프",
        summary="경고, 실패 단계, recovery 대기열을 한 화면에서 먼저 훑습니다.",
        items=_build_log_items(),
        empty_message="로그 브리프 데이터가 없습니다.",
    )
    with st.expander("로그 원본 보기", expanded=False):
        render_record_cards(
            runs,
            title="실행 이력 원본",
            primary_column="job_name",
            secondary_columns=["status"],
            detail_columns=["as_of_date", "started_at", "finished_at", "run_id"],
            limit=10,
            empty_message="실행 이력 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            step_failures,
            title="실패 단계 원본",
            primary_column="step_name",
            secondary_columns=["status"],
            detail_columns=["job_run_id", "started_at", "error_message"],
            limit=10,
            empty_message="실패 단계 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            disk_events,
            title="디스크 이벤트 원본",
            primary_column="measured_at",
            secondary_columns=["disk_status"],
            detail_columns=["usage_ratio", "used_gb", "available_gb"],
            limit=8,
            empty_message="디스크 이벤트 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            cleanup_history,
            title="정리 이력 원본",
            primary_column="started_at",
            secondary_columns=["status", "cleanup_scope"],
            detail_columns=["removed_file_count", "reclaimed_bytes", "notes"],
            limit=8,
            empty_message="정리 이력 원본이 없습니다.",
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
        render_record_cards(
            active_policy,
            title="active ops policy 원본",
            primary_column="policy_id",
            secondary_columns=["policy_version", "promotion_type"],
            detail_columns=["effective_from_at", "effective_to_at", "note"],
            limit=8,
            empty_message="active policy 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            release_checks,
            title="release check 원본",
            primary_column="check_name",
            secondary_columns=["status", "severity"],
            detail_columns=["check_ts", "recommended_action"],
            limit=8,
            empty_message="release check 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            latest_reports,
            title="report index 원본",
            primary_column="report_type",
            secondary_columns=["status"],
            detail_columns=["as_of_date", "generated_ts", "published_flag"],
            limit=8,
            empty_message="report index 원본이 없습니다.",
            show_table_expander=False,
        )
        if ops_preview:
            render_report_preview(
                title="운영 리포트 미리보기",
                preview=ops_preview,
            )

render_page_footer(settings, page_name="운영")
