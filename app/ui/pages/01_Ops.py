# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
from app.ml.active import freeze_alpha_active_model, rollback_alpha_active_model
from app.ml.constants import ALPHA_CANDIDATE_MODEL_SPECS, MODEL_SPEC_ID
from app.ml.promotion import format_alpha_model_spec_id
from app.ui.components import (
    render_data_sheet,
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_report_preview,
    render_record_cards,
    render_report_center,
    render_screen_guide,
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

render_page_header(
    settings,
    page_name="운영",
    title="운영",
    description="최근 실행 이력, 단계별 실패, 의존성, KRX 상태, 리포트를 모바일에서도 읽기 쉬운 운영 시트로 정리한 화면입니다.",
)
render_screen_guide(
    summary="모바일에서 가로 스크롤 표를 보지 않도록 운영 화면을 섹션형으로 재배열했습니다. 운영 요약, 알파 모델 제어, 스케줄·KRX, 운영 이력을 나눠서 확인하세요.",
    bullets=[
        "개요에서는 건강도, 최신 산출물, 의존성, 열린 알림을 먼저 확인합니다.",
        "알파 모델에서는 현재 활성 상태와 학습 이력, 수동 반영/되돌리기를 다룹니다.",
        "스케줄·KRX에서는 배치 흐름과 외부 데이터 서비스 상태를 확인합니다.",
        "운영 이력에서는 실행 로그, 디스크/정리, 복구, 릴리스 점검, 리포트를 봅니다.",
    ],
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
        (
            f"현재 기준일은 {format_ui_date(row.get('as_of_date'))}이고 "
            f"운영 상태는 {format_ui_value('health_status', row.get('health_status'))}입니다. "
            f"치명 알림 {int(row['critical_alert_count'] or 0)}건, "
            f"경고 알림 {int(row['warning_alert_count'] or 0)}건이 열려 있습니다."
        ),
    )

render_record_cards(
    alpha_promotion,
    title="알파 모델 비교 요약",
    primary_column="summary_title",
    secondary_columns=["active_model_label", "comparison_model_label"],
    detail_columns=[
        "decision_label",
        "decision_reason_label",
        "active_top10_mean_excess_return",
        "comparison_top10_mean_excess_return",
        "promotion_gap",
        "sample_count",
        "window_end",
        "active_promotion_type",
    ],
    limit=4,
    empty_message="아직 알파 모델 비교 기록이 없습니다.",
    table_expander_label="알파 모델 비교 원본 표 보기",
)

alpha_notice = st.session_state.pop("alpha_ops_notice", None)
if isinstance(alpha_notice, dict):
    notice_level = str(alpha_notice.get("level") or "info")
    notice_message = str(alpha_notice.get("message") or "").strip()
    if notice_message:
        getattr(st, notice_level, st.info)(notice_message)

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

view = st.segmented_control(
    "운영 보기",
    options=["개요", "알파 모델", "스케줄·KRX", "운영 이력"],
    default="개요",
)

if view == "개요":
    render_data_sheet(
        health,
        title="전체 상태 요약",
        primary_column="component_name",
        secondary_columns=["status", "health_scope"],
        detail_columns=["snapshot_ts", "status_reason", "action_hint"],
        limit=10,
        empty_message="헬스 요약이 없습니다.",
    )
    render_data_sheet(
        latest_outputs,
        title="최신 정상 산출물",
        limit=10,
        empty_message="최신 정상 산출물이 없습니다.",
    )
    render_data_sheet(
        dependencies,
        title="의존성 준비 상태",
        primary_column="dependency_name",
        secondary_columns=["status", "required_date"],
        detail_columns=["latest_available_date", "lag_days", "status_reason"],
        limit=10,
        empty_message="의존성 상태가 없습니다.",
    )
    render_data_sheet(
        alerts,
        title="알림",
        primary_column="message",
        secondary_columns=["severity", "component_name"],
        detail_columns=["created_at", "alert_type", "status"],
        limit=8,
        empty_message="열린 운영 알림이 없습니다.",
        table_expander_label="알림 전체 표 보기",
    )
elif view == "알파 모델":
    st.caption("현재 활성 상태와 최근 학습 이력을 먼저 보고, 정말 필요할 때만 아래 수동 반영/되돌리기를 사용하세요.")
    render_data_sheet(
        alpha_active_models,
        title="현재 사용 중인 알파 모델",
        primary_column="model_spec_id",
        secondary_columns=["horizon", "status"],
        detail_columns=["as_of_date", "train_end_date", "source"],
        limit=8,
        empty_message="현재 사용 중인 알파 모델 기록이 없습니다.",
    )
    render_data_sheet(
        alpha_candidate_training,
        title="최근 알파 모델 학습 이력",
        primary_column="model_spec_id",
        secondary_columns=["horizon", "status"],
        detail_columns=["train_end_date", "created_at", "row_count"],
        limit=8,
        empty_message="최근 알파 모델 학습 이력이 없습니다.",
    )
    render_data_sheet(
        alpha_rollbacks,
        title="알파 모델 되돌리기 기록",
        primary_column="model_spec_id",
        secondary_columns=["horizon", "status"],
        detail_columns=["as_of_date", "created_at", "note"],
        limit=8,
        empty_message="알파 모델 되돌리기 기록이 없습니다.",
    )
    with st.expander("알파 모델 사양 목록", expanded=False):
        render_data_sheet(
            alpha_model_specs,
            title="알파 모델 사양 목록",
            primary_column="model_spec_id",
            detail_columns=["label", "description"],
            limit=10,
            empty_message="알파 모델 사양 목록이 없습니다.",
            show_table_expander=False,
        )

    with st.expander("고급 모델 운영 도구", expanded=False):
        st.caption(
            "알파 모델 고정과 되돌리기는 일반 점검용이 아닙니다. 비교표를 충분히 확인한 뒤 정말 필요할 때만 사용하세요."
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
                freeze_horizons = st.multiselect("대상 기간", options=[1, 5], default=[1, 5])
                freeze_note = st.text_input("반영 메모", value="운영 화면에서 수동 반영")
                freeze_confirm = st.checkbox("비교표를 확인했고 현재 모델을 이 후보로 교체하는 데 동의합니다.")
                freeze_submit = st.form_submit_button("알파 모델 수동 반영")
                if freeze_submit:
                    if not freeze_confirm:
                        st.warning("먼저 비교표를 확인하고 확인 체크를 해주세요.")
                    elif not freeze_horizons:
                        st.warning("최소 한 개 이상의 기간을 선택해 주세요.")
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
                                "message": (
                                    f"알파 모델 반영을 완료했습니다. run_id={freeze_result.run_id} "
                                    f"rows={freeze_result.row_count}"
                                ),
                            }
                            st.rerun()
                        except Exception as exc:  # pragma: no cover - UI feedback path
                            st.error(f"알파 모델 반영에 실패했습니다: {exc}")

        with alpha_control_right:
            with st.form("rollback_alpha_active_model_form", clear_on_submit=False):
                rollback_effective_date = st.date_input("되돌릴 기준일", value=alpha_default_date)
                rollback_horizons = st.multiselect("대상 기간", options=[1, 5], default=[1, 5])
                rollback_note = st.text_input("되돌리기 메모", value="운영 화면에서 수동 되돌리기")
                rollback_confirm = st.checkbox("현재 모델 대신 직전 모델로 되돌리는 데 동의합니다.")
                rollback_submit = st.form_submit_button("알파 모델 되돌리기")
                if rollback_submit:
                    if not rollback_confirm:
                        st.warning("먼저 확인 체크를 해주세요.")
                    elif not rollback_horizons:
                        st.warning("최소 한 개 이상의 기간을 선택해 주세요.")
                    else:
                        try:
                            with st.spinner("알파 모델을 되돌리는 중입니다..."):
                                rollback_result = rollback_alpha_active_model(
                                    settings,
                                    as_of_date=rollback_effective_date,
                                    horizons=[int(value) for value in rollback_horizons],
                                    note=rollback_note,
                                )
                            st.session_state["alpha_ops_notice"] = {
                                "level": "success" if rollback_result.row_count > 0 else "warning",
                                "message": (
                                    f"알파 모델 되돌리기를 완료했습니다. "
                                    f"run_id={rollback_result.run_id} rows={rollback_result.row_count}"
                                ),
                            }
                            st.rerun()
                        except Exception as exc:  # pragma: no cover - UI feedback path
                            st.error(f"알파 모델 되돌리기에 실패했습니다: {exc}")
elif view == "스케줄·KRX":
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
        title="최근 자동 번들 실행 결과",
        primary_column="job_name",
        secondary_columns=["status", "as_of_date"],
        detail_columns=["started_at", "finished_at", "run_id"],
        limit=10,
        empty_message="최근 자동 번들 실행 결과가 없습니다.",
    )
    render_data_sheet(
        krx_status,
        title="KRX 라이브 상태",
        limit=8,
        empty_message="아직 KRX 서비스 상태 이력이 없습니다. smoke test를 먼저 실행하세요.",
    )
    render_data_sheet(
        krx_budget,
        title="KRX 요청 예산",
        limit=8,
        empty_message="아직 KRX 요청 예산 스냅샷이 없습니다.",
    )
    with st.expander("KRX 요청 로그 / 출처 표기 / 승인 서비스", expanded=False):
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
        render_data_sheet(
            krx_registry,
            title="승인 서비스 레지스트리",
            limit=10,
            empty_message="KRX 서비스 레지스트리가 없습니다.",
            show_table_expander=False,
        )
else:
    render_data_sheet(
        runs,
        title="최근 실행 이력",
        primary_column="job_name",
        secondary_columns=["status", "as_of_date"],
        detail_columns=["started_at", "finished_at", "run_id"],
        limit=10,
        empty_message="최근 실행 이력이 없습니다.",
    )
    if step_failures.empty:
        st.success("최근 단계 실패가 없습니다.")
    else:
        render_data_sheet(
            step_failures,
            title="단계별 실패 현황",
            primary_column="step_name",
            secondary_columns=["job_name", "status"],
            detail_columns=["failed_at", "error_message"],
            limit=10,
            empty_message="최근 단계 실패가 없습니다.",
        )
    render_data_sheet(
        disk_events,
        title="디스크 사용량 / 경보선",
        limit=8,
        empty_message="디스크 사용량 이력이 없습니다.",
    )
    render_data_sheet(
        cleanup_history,
        title="보관 정책 / 정리 이력",
        limit=8,
        empty_message="정리 이력이 없습니다.",
    )
    render_data_sheet(
        recovery,
        title="복구 대기열",
        limit=8,
        empty_message="현재 복구 대기열은 비어 있습니다.",
    )
    render_data_sheet(
        active_policy,
        title="활성 운영 정책",
        limit=6,
        empty_message="활성 운영 정책 기록이 없습니다.",
    )
    render_data_sheet(
        release_checks,
        title="릴리스 점검 항목",
        primary_column="check_name",
        secondary_columns=["status", "severity"],
        detail_columns=["check_ts", "recommended_action"],
        limit=8,
        empty_message="릴리스 점검 결과가 없습니다.",
    )
    render_report_center(settings, limit=12)
    if not latest_reports.empty:
        with st.expander("전체 리포트 목록", expanded=False):
            render_data_sheet(
                latest_reports,
                title="전체 리포트 목록",
                primary_column="report_type",
                secondary_columns=["status", "as_of_date"],
                detail_columns=["generated_ts", "published_flag"],
                limit=12,
                empty_message="리포트가 없습니다.",
                show_table_expander=False,
            )
    if ops_preview:
        with st.expander("최신 운영 리포트 미리보기", expanded=False):
            render_report_preview(
                title="운영 리포트 미리보기",
                preview=ops_preview,
            )

render_page_footer(settings, page_name="운영")
