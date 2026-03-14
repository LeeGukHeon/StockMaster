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
from app.intraday.meta_common import ENTER_PANEL, WAIT_PANEL
from app.intraday.meta_training import freeze_intraday_active_meta_model
from app.intraday.policy import freeze_intraday_active_policy
from app.ui.components import (
    render_data_sheet,
    render_page_footer,
    render_page_header,
    render_report_preview,
    render_screen_guide,
    render_warning_banner,
)
from app.ui.helpers import (
    intraday_meta_calibration_frame,
    intraday_meta_confusion_matrix_frame,
    intraday_meta_feature_importance_frame,
    latest_job_runs_frame,
    latest_intraday_active_policy_frame,
    latest_intraday_meta_active_model_frame,
    latest_intraday_meta_apply_compare_frame,
    latest_intraday_meta_overlay_comparison_frame,
    latest_intraday_meta_training_frame,
    latest_intraday_policy_ablation_frame,
    latest_intraday_policy_apply_compare_frame,
    latest_intraday_policy_evaluation_frame,
    latest_intraday_policy_experiment_frame,
    latest_intraday_policy_publish_status_frame,
    latest_intraday_policy_recommendation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_policy_rollback_frame,
    latest_intraday_research_capability_frame,
    latest_model_training_summary_frame,
    load_ui_settings,
)

settings = load_ui_settings(PROJECT_ROOT)

recent_runs = latest_job_runs_frame(settings, limit=60)
weekly_calibration_runs = (
    recent_runs.loc[recent_runs["job_name"].astype(str).eq("run_weekly_calibration_bundle")].copy()
    if not recent_runs.empty
    else recent_runs
)
weekly_training_runs = (
    recent_runs.loc[recent_runs["job_name"].astype(str).eq("run_weekly_training_bundle")].copy()
    if not recent_runs.empty
    else recent_runs
)

alpha_training_summary = latest_model_training_summary_frame(settings)
meta_training_summary = latest_intraday_meta_training_frame(settings, limit=20)
intraday_capability = latest_intraday_research_capability_frame(settings, limit=12)
policy_experiments = latest_intraday_policy_experiment_frame(settings, limit=20)
policy_calibration = latest_intraday_policy_evaluation_frame(settings, split_name="validation", limit=20)
policy_walkforward = latest_intraday_policy_evaluation_frame(settings, split_name="test", limit=20)
policy_ablation = latest_intraday_policy_ablation_frame(settings, limit=20)
policy_recommendation = latest_intraday_policy_recommendation_frame(settings, limit=20)
policy_rollbacks = latest_intraday_policy_rollback_frame(settings, limit=12)
policy_publish_status = latest_intraday_policy_publish_status_frame(settings, limit=12)
policy_report_preview = latest_intraday_policy_report_preview(settings)
active_policy = latest_intraday_active_policy_frame(settings, limit=12)
policy_apply_compare = latest_intraday_policy_apply_compare_frame(settings, limit=12)
active_meta_models = latest_intraday_meta_active_model_frame(settings, limit=12)
meta_apply_compare = latest_intraday_meta_apply_compare_frame(settings, limit=12)
meta_overlay = latest_intraday_meta_overlay_comparison_frame(settings, limit=20)
meta_regime_breakdown = latest_intraday_meta_overlay_comparison_frame(
    settings,
    metric_scope="regime",
    limit=20,
)

render_page_header(
    settings,
    page_name="리서치",
    title="리서치",
    description="주간 학습 결과 비교와 반영 버튼을 맨 위에 두고, 정책·메타 분석은 탭으로 나눠 보는 연구 화면입니다.",
)
render_screen_guide(
    summary="주간 학습 결과를 실제로 반영할 때 헤매지 않도록 첫 탭을 바로 반영 화면으로 바꿨습니다.",
    bullets=[
        "주간 반영 탭에서 현재 운영값과 다음 후보를 비교하고 바로 승인합니다.",
        "정책 보기 탭에서는 정책 실험과 기간별 재검증을 확인합니다.",
        "메타 보기 탭에서는 메타 보정 결과와 국면별 차이를 확인합니다.",
        "학습 요약 탭에서는 최근 학습 이력과 연구 리포트를 한 번에 봅니다.",
    ],
)
render_warning_banner(
    "INFO",
    "자동 반영은 없습니다. 아래 주간 반영 탭에서 비교하고 직접 승인해야만 운영값이 바뀝니다.",
)

action_tab, policy_tab, meta_tab, summary_tab = st.tabs(
    ["주간 반영", "정책 보기", "메타 보기", "학습 요약"]
)

with action_tab:
    st.subheader("이번 주 결과 반영")
    st.caption("1. 현재 운영값과 다음 후보를 비교한 뒤 2. 확인 체크를 하고 3. 바로 반영하세요.")

    policy_action_tab, meta_action_tab = st.tabs(["정책 교체", "메타 모델 교체"])

    with policy_action_tab:
        render_data_sheet(
            weekly_calibration_runs,
            title="최근 주말 보정 실행 결과",
            primary_column="started_at",
            secondary_columns=["status", "as_of_date"],
            detail_columns=["notes", "run_id"],
            limit=2,
            empty_message="최근 주말 보정 실행 기록이 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            policy_recommendation,
            title="주말 보정에서 나온 추천 정책",
            primary_column="policy_candidate_id",
            secondary_columns=["recommendation_date", "recommendation_rank"],
            detail_columns=["horizon", "objective_score", "manual_review_required_flag"],
            limit=4,
            empty_message="주말 보정 추천 결과가 아직 화면에 연결되지 않았습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            active_policy,
            title="현재 운영 정책",
            primary_column="policy_id",
            secondary_columns=["status", "as_of_date"],
            detail_columns=["source", "note"],
            limit=3,
            empty_message="현재 운영 정책 기록이 없습니다.",
        )
        render_data_sheet(
            policy_apply_compare,
            title="주말 보정 결과로 올라온 다음 정책",
            primary_column="policy_id",
            secondary_columns=["recommendation_label", "recommendation_date"],
            detail_columns=["status", "recommended_action", "score"],
            limit=3,
            empty_message="이번 주에 반영할 정책 후보가 없습니다.",
        )

        with st.form("apply_intraday_policy_form", clear_on_submit=False):
            policy_note = st.text_input(
                "정책 반영 메모",
                value="주간 보정 결과를 확인하고 운영 정책을 교체함",
            )
            policy_confirm = st.checkbox("비교 내용을 확인했고, 현재 운영 정책을 이 후보로 바꾸는 데 동의합니다.")
            policy_submit = st.form_submit_button("정책 바로 반영")
            if policy_submit:
                if not policy_confirm:
                    st.warning("먼저 비교 내용을 확인하고 동의 체크를 해주세요.")
                elif policy_apply_compare.empty:
                    st.warning("반영할 정책 후보가 없습니다.")
                else:
                    recommendation_dates = (
                        policy_apply_compare["recommendation_date"].dropna().astype(str).tolist()
                        if "recommendation_date" in policy_apply_compare.columns
                        else []
                    )
                    effective_date = (
                        max(recommendation_dates)
                        if recommendation_dates
                        else today_local(settings.app.timezone).isoformat()
                    )
                    policy_result = freeze_intraday_active_policy(
                        settings,
                        as_of_date=date.fromisoformat(effective_date),
                        promotion_type="MANUAL_FREEZE",
                        source="scheduler_latest_recommendation",
                        note=policy_note,
                    )
                    st.success(
                        f"정책 반영을 완료했습니다. run_id={policy_result.run_id} rows={policy_result.row_count}"
                    )

    with meta_action_tab:
        render_data_sheet(
            weekly_training_runs,
            title="최근 주말 학습 실행 결과",
            primary_column="started_at",
            secondary_columns=["status", "as_of_date"],
            detail_columns=["notes", "run_id"],
            limit=2,
            empty_message="최근 주말 학습 실행 기록이 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            meta_training_summary,
            title="주말 학습에서 나온 메타 후보",
            primary_column="model_id",
            secondary_columns=["status", "horizon"],
            detail_columns=["train_end_date", "created_at", "row_count"],
            limit=4,
            empty_message="주말 학습 메타 후보가 아직 화면에 연결되지 않았습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            active_meta_models,
            title="현재 운영 메타 모델",
            primary_column="model_id",
            secondary_columns=["status", "horizon"],
            detail_columns=["as_of_date", "train_end_date", "source"],
            limit=3,
            empty_message="현재 운영 메타 모델이 없습니다.",
        )
        render_data_sheet(
            meta_apply_compare,
            title="주말 학습 결과로 올라온 다음 메타 모델",
            primary_column="model_id",
            secondary_columns=["status", "horizon"],
            detail_columns=["train_end_date", "score", "recommended_action"],
            limit=3,
            empty_message="이번 주에 반영할 메타 모델 후보가 없습니다.",
        )

        with st.form("apply_intraday_meta_form", clear_on_submit=False):
            meta_note = st.text_input(
                "메타 모델 반영 메모",
                value="주간 학습 결과를 확인하고 메타 모델을 교체함",
            )
            meta_confirm = st.checkbox("비교 내용을 확인했고, 현재 메타 모델을 이 후보로 바꾸는 데 동의합니다.")
            meta_submit = st.form_submit_button("메타 모델 바로 반영")
            if meta_submit:
                if not meta_confirm:
                    st.warning("먼저 비교 내용을 확인하고 동의 체크를 해주세요.")
                elif meta_apply_compare.empty:
                    st.warning("반영할 메타 모델 후보가 없습니다.")
                else:
                    candidate_dates = (
                        meta_apply_compare["train_end_date"].dropna().astype(str).tolist()
                        if "train_end_date" in meta_apply_compare.columns
                        else []
                    )
                    effective_date = (
                        max(candidate_dates)
                        if candidate_dates
                        else today_local(settings.app.timezone).isoformat()
                    )
                    meta_result = freeze_intraday_active_meta_model(
                        settings,
                        as_of_date=date.fromisoformat(effective_date),
                        source="scheduler_latest_training_candidate",
                        note=meta_note,
                        horizons=[1, 5],
                    )
                    st.success(
                        f"메타 모델 반영을 완료했습니다. run_id={meta_result.run_id} rows={meta_result.row_count}"
                    )

with policy_tab:
    render_data_sheet(
        policy_recommendation,
        title="이번 주 정책 추천",
        primary_column="policy_id",
        secondary_columns=["recommendation_label", "recommendation_date"],
        detail_columns=["status", "score", "recommended_action"],
        limit=6,
        empty_message="정책 추천 결과가 없습니다.",
    )
    render_data_sheet(
        policy_experiments,
        title="정책 실험 기록",
        primary_column="policy_id",
        secondary_columns=["status", "split_name"],
        detail_columns=["as_of_date", "metric_name", "metric_value"],
        limit=6,
        empty_message="정책 실험 결과가 없습니다.",
    )
    render_data_sheet(
        policy_walkforward,
        title="기간별 재검증",
        primary_column="policy_id",
        secondary_columns=["status", "split_name"],
        detail_columns=["as_of_date", "metric_name", "metric_value"],
        limit=6,
        empty_message="기간별 재검증 결과가 없습니다.",
    )
    with st.expander("정책 발행 이력과 자세한 검증 보기", expanded=False):
        render_data_sheet(
            policy_publish_status,
            title="정책 발행 상태",
            primary_column="policy_id",
            secondary_columns=["status", "published_at"],
            detail_columns=["report_date", "run_id"],
            limit=6,
            empty_message="정책 발행 상태가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            policy_rollbacks,
            title="정책 되돌리기 이력",
            primary_column="policy_id",
            secondary_columns=["status", "as_of_date"],
            detail_columns=["created_at", "note"],
            limit=6,
            empty_message="정책 되돌리기 이력이 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            policy_calibration,
            title="검증용 세부 점수",
            primary_column="policy_id",
            secondary_columns=["status", "split_name"],
            detail_columns=["as_of_date", "metric_name", "metric_value"],
            limit=6,
            empty_message="검증용 세부 점수가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            policy_ablation,
            title="항목 제거 실험",
            primary_column="policy_id",
            secondary_columns=["status", "metric_scope"],
            detail_columns=["as_of_date", "metric_name", "metric_value"],
            limit=6,
            empty_message="항목 제거 실험 결과가 없습니다.",
            show_table_expander=False,
        )

with meta_tab:
    meta_horizon = st.selectbox("메타 모델 기간", options=[1, 5], index=0, format_func=lambda value: f"{value}거래일")
    meta_panel = st.selectbox(
        "메타 모델 구분",
        options=[ENTER_PANEL, WAIT_PANEL],
        index=0,
        format_func=lambda value: "진입 판단" if value == ENTER_PANEL else "대기 판단",
    )
    meta_calibration = intraday_meta_calibration_frame(settings, horizon=meta_horizon, panel_name=meta_panel)
    meta_confusion = intraday_meta_confusion_matrix_frame(settings, horizon=meta_horizon, panel_name=meta_panel)
    meta_feature_importance = intraday_meta_feature_importance_frame(
        settings,
        horizon=meta_horizon,
        panel_name=meta_panel,
        limit=20,
    )
    render_data_sheet(
        meta_overlay,
        title="메타 보정 결과",
        limit=6,
        empty_message="메타 보정 비교 결과가 없습니다.",
    )
    render_data_sheet(
        meta_regime_breakdown,
        title="국면별 차이",
        limit=6,
        empty_message="국면별 차이 결과가 없습니다.",
    )
    render_data_sheet(
        meta_training_summary,
        title="메타 모델 학습 이력",
        primary_column="model_id",
        secondary_columns=["status", "horizon"],
        detail_columns=["train_end_date", "created_at", "row_count"],
        limit=6,
        empty_message="메타 모델 학습 이력이 없습니다.",
    )
    with st.expander("메타 진단 자세히 보기", expanded=False):
        render_data_sheet(
            meta_calibration,
            title="메타 보정 세부 결과",
            limit=6,
            empty_message="메타 보정 결과가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            meta_confusion,
            title="혼동 행렬",
            limit=6,
            empty_message="혼동 행렬 결과가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            meta_feature_importance,
            title="중요하게 본 특징",
            limit=6,
            empty_message="특징 중요도 결과가 없습니다.",
            show_table_expander=False,
        )

with summary_tab:
    render_data_sheet(
        intraday_capability,
        title="장중 리서치 준비 상태",
        limit=6,
        empty_message="장중 리서치 기능 상태가 없습니다.",
    )
    render_data_sheet(
        alpha_training_summary,
        title="최근 알파 모델 학습",
        primary_column="model_spec_id",
        secondary_columns=["status", "horizon"],
        detail_columns=["train_end_date", "created_at", "row_count"],
        limit=6,
        empty_message="최근 알파 모델 학습 이력이 없습니다.",
    )
    render_data_sheet(
        meta_training_summary,
        title="최근 메타 모델 학습",
        primary_column="model_id",
        secondary_columns=["status", "horizon"],
        detail_columns=["train_end_date", "created_at", "row_count"],
        limit=6,
        empty_message="최근 메타 모델 학습 이력이 없습니다.",
    )
    if policy_report_preview:
        with st.expander("최신 장중 정책 연구 리포트 미리보기", expanded=False):
            render_report_preview(
                title="장중 정책 연구 리포트 미리보기",
                preview=policy_report_preview,
            )

render_page_footer(settings, page_name="리서치")
