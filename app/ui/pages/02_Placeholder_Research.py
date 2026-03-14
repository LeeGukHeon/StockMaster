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

alpha_training_summary = latest_model_training_summary_frame(settings)
meta_training_summary = latest_intraday_meta_training_frame(settings, limit=30)
intraday_capability = latest_intraday_research_capability_frame(settings, limit=20)
policy_experiments = latest_intraday_policy_experiment_frame(settings, limit=30)
policy_calibration = latest_intraday_policy_evaluation_frame(settings, split_name="validation", limit=30)
policy_walkforward = latest_intraday_policy_evaluation_frame(settings, split_name="test", limit=30)
policy_ablation = latest_intraday_policy_ablation_frame(settings, limit=30)
policy_recommendation = latest_intraday_policy_recommendation_frame(settings, limit=30)
policy_rollbacks = latest_intraday_policy_rollback_frame(settings, limit=20)
policy_publish_status = latest_intraday_policy_publish_status_frame(settings, limit=12)
policy_report_preview = latest_intraday_policy_report_preview(settings)
active_policy = latest_intraday_active_policy_frame(settings, limit=30)
policy_apply_compare = latest_intraday_policy_apply_compare_frame(settings, limit=30)
active_meta_models = latest_intraday_meta_active_model_frame(settings, limit=30)
meta_apply_compare = latest_intraday_meta_apply_compare_frame(settings, limit=30)

meta_horizon = st.selectbox("메타 모델 기간", options=[1, 5], index=0, format_func=lambda value: f"{value}거래일")
meta_panel = st.selectbox(
    "메타 모델 패널",
    options=[ENTER_PANEL, WAIT_PANEL],
    index=0,
    format_func=lambda value: "진입 패널" if value == ENTER_PANEL else "대기 패널",
)
show_technical = st.toggle("고급 진단 펼치기", value=False)

meta_calibration = intraday_meta_calibration_frame(settings, horizon=meta_horizon, panel_name=meta_panel)
meta_confusion = intraday_meta_confusion_matrix_frame(settings, horizon=meta_horizon, panel_name=meta_panel)
meta_feature_importance = intraday_meta_feature_importance_frame(
    settings,
    horizon=meta_horizon,
    panel_name=meta_panel,
    limit=30,
)
meta_overlay = latest_intraday_meta_overlay_comparison_frame(settings, limit=20)
meta_regime_breakdown = latest_intraday_meta_overlay_comparison_frame(
    settings,
    metric_scope="regime",
    limit=30,
)

render_page_header(
    settings,
    page_name="리서치",
    title="리서치",
    description="장중 정책, 메타 모델, 보정 결과, 실험, 수동 반영 후보를 모바일용 비교 시트로 정리한 화면입니다.",
)
render_screen_guide(
    summary="리서치 지표와 운영 반영 제어를 한 화면에 두되, 모바일에서는 개요·정책·메타·수동 반영으로 나눠 읽기 쉽게 재구성했습니다.",
    bullets=[
        "개요에서는 학습 상태와 오버레이를 먼저 봅니다.",
        "정책에서는 실험, 워크포워드, 추천, 발행/롤백 상태를 확인합니다.",
        "메타에서는 메타 모델 학습, 국면별 분해, 필요 시 고급 진단을 확인합니다.",
        "수동 반영에서는 현재 활성값과 다음 후보를 비교한 뒤 직접 반영합니다.",
    ],
)
render_warning_banner(
    "INFO",
    "자동 계산은 수행되지만 자동 반영은 하지 않습니다. 비교표를 확인한 뒤 체크박스와 버튼으로만 수동 반영할 수 있습니다.",
)

view = st.segmented_control(
    "리서치 보기",
    options=["개요", "정책", "메타", "수동 반영"],
    default="개요",
)

if view == "개요":
    render_data_sheet(
        intraday_capability,
        title="장중 리서치 기능 상태",
        limit=8,
        empty_message="장중 리서치 기능 상태가 없습니다.",
    )
    render_data_sheet(
        alpha_training_summary,
        title="최신 알파 모델 학습",
        primary_column="model_spec_id",
        secondary_columns=["status", "horizon"],
        detail_columns=["train_end_date", "created_at", "row_count"],
        limit=8,
        empty_message="최신 알파 모델 학습 이력이 없습니다.",
    )
    render_data_sheet(
        meta_training_summary,
        title="장중 메타 모델 학습",
        primary_column="model_id",
        secondary_columns=["status", "horizon"],
        detail_columns=["train_end_date", "created_at", "row_count"],
        limit=8,
        empty_message="장중 메타 모델 학습 이력이 없습니다.",
    )
    render_data_sheet(
        meta_overlay,
        title="정책 대비 메타 오버레이",
        limit=8,
        empty_message="메타 오버레이 비교 결과가 없습니다.",
    )
    render_data_sheet(
        meta_regime_breakdown,
        title="국면별 분해",
        limit=8,
        empty_message="국면별 분해 결과가 없습니다.",
    )
elif view == "정책":
    render_data_sheet(
        policy_experiments,
        title="정책 실험",
        primary_column="policy_id",
        secondary_columns=["status", "split_name"],
        detail_columns=["as_of_date", "metric_name", "metric_value"],
        limit=10,
        empty_message="정책 실험 결과가 없습니다.",
    )
    render_data_sheet(
        policy_walkforward,
        title="워크포워드",
        primary_column="policy_id",
        secondary_columns=["status", "split_name"],
        detail_columns=["as_of_date", "metric_name", "metric_value"],
        limit=10,
        empty_message="워크포워드 결과가 없습니다.",
    )
    render_data_sheet(
        policy_recommendation,
        title="정책 추천",
        primary_column="policy_id",
        secondary_columns=["recommendation_label", "recommendation_date"],
        detail_columns=["status", "score", "recommended_action"],
        limit=10,
        empty_message="정책 추천 결과가 없습니다.",
    )
    render_data_sheet(
        policy_publish_status,
        title="정책 발행 상태",
        primary_column="policy_id",
        secondary_columns=["status", "published_at"],
        detail_columns=["report_date", "run_id"],
        limit=8,
        empty_message="정책 발행 상태가 없습니다.",
    )
    render_data_sheet(
        policy_rollbacks,
        title="정책 롤백 이력",
        primary_column="policy_id",
        secondary_columns=["status", "as_of_date"],
        detail_columns=["created_at", "note"],
        limit=8,
        empty_message="정책 롤백 이력이 없습니다.",
    )
    if show_technical:
        render_data_sheet(
            policy_calibration,
            title="정책 검증",
            primary_column="policy_id",
            secondary_columns=["status", "split_name"],
            detail_columns=["as_of_date", "metric_name", "metric_value"],
            limit=8,
            empty_message="정책 검증 결과가 없습니다.",
        )
        render_data_sheet(
            policy_ablation,
            title="정책 제거 실험",
            primary_column="policy_id",
            secondary_columns=["status", "metric_scope"],
            detail_columns=["as_of_date", "metric_name", "metric_value"],
            limit=8,
            empty_message="정책 제거 실험 결과가 없습니다.",
        )
elif view == "메타":
    render_data_sheet(
        meta_training_summary,
        title="메타 모델 학습",
        primary_column="model_id",
        secondary_columns=["status", "horizon"],
        detail_columns=["train_end_date", "created_at", "row_count"],
        limit=10,
        empty_message="메타 모델 학습 이력이 없습니다.",
    )
    render_data_sheet(
        meta_overlay,
        title="메타 오버레이",
        limit=8,
        empty_message="메타 오버레이 비교 결과가 없습니다.",
    )
    render_data_sheet(
        meta_regime_breakdown,
        title="국면별 메타 분해",
        limit=8,
        empty_message="국면별 메타 분해 결과가 없습니다.",
    )
    if show_technical:
        render_data_sheet(
            meta_calibration,
            title="메타 보정",
            limit=8,
            empty_message="메타 보정 결과가 없습니다.",
        )
        render_data_sheet(
            meta_confusion,
            title="혼동 행렬",
            limit=8,
            empty_message="혼동 행렬 결과가 없습니다.",
        )
        render_data_sheet(
            meta_feature_importance,
            title="주요 특징 중요도",
            limit=8,
            empty_message="특징 중요도 결과가 없습니다.",
        )
else:
    st.info(
        "주간 학습 후보와 주간 보정 결과는 자동 계산만 합니다. 활성 정책과 활성 메타 모델은 절대 자동 승격하지 않으며, 아래 비교 후 직접 확인해야만 반영됩니다."
    )

    render_data_sheet(
        active_policy,
        title="현재 활성 정책",
        primary_column="policy_id",
        secondary_columns=["status", "as_of_date"],
        detail_columns=["source", "note"],
        limit=8,
        empty_message="현재 활성 정책 기록이 없습니다.",
    )
    render_data_sheet(
        policy_apply_compare,
        title="다음 반영 후보 정책",
        primary_column="policy_id",
        secondary_columns=["recommendation_label", "recommendation_date"],
        detail_columns=["status", "recommended_action", "score"],
        limit=8,
        empty_message="반영 후보 정책이 없습니다.",
    )

    with st.form("apply_intraday_policy_form", clear_on_submit=False):
        policy_note = st.text_input(
            "정책 반영 메모",
            value="T018 수동 확인: 주간 보정 결과 검토 후 반영",
        )
        policy_confirm = st.checkbox("비교표를 확인했고 현재 활성 정책을 추천 정책으로 수동 교체하는 데 동의합니다.")
        policy_submit = st.form_submit_button("추천 정책 반영")
        if policy_submit:
            if not policy_confirm:
                st.warning("먼저 비교표를 확인하고 확인 체크를 해주세요.")
            elif policy_apply_compare.empty:
                st.warning("반영할 정책 추천 결과가 없습니다.")
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
                    f"추천 정책 반영을 완료했습니다. run_id={policy_result.run_id} rows={policy_result.row_count}"
                )

    render_data_sheet(
        active_meta_models,
        title="현재 활성 메타 모델",
        primary_column="model_id",
        secondary_columns=["status", "horizon"],
        detail_columns=["as_of_date", "train_end_date", "source"],
        limit=8,
        empty_message="현재 활성 메타 모델이 없습니다.",
    )
    render_data_sheet(
        meta_apply_compare,
        title="다음 반영 후보 메타 모델",
        primary_column="model_id",
        secondary_columns=["status", "horizon"],
        detail_columns=["train_end_date", "score", "recommended_action"],
        limit=8,
        empty_message="반영 후보 메타 모델이 없습니다.",
    )

    with st.form("apply_intraday_meta_form", clear_on_submit=False):
        meta_note = st.text_input(
            "메타 모델 반영 메모",
            value="T018 수동 확인: retrain candidate 검토 후 반영",
        )
        meta_confirm = st.checkbox("비교표를 확인했고 현재 활성 메타 모델을 신규 학습 후보로 수동 교체하는 데 동의합니다.")
        meta_submit = st.form_submit_button("메타 모델 반영")
        if meta_submit:
            if not meta_confirm:
                st.warning("먼저 비교표를 확인하고 확인 체크를 해주세요.")
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

if policy_report_preview:
    with st.expander("최신 장중 정책 연구 리포트 미리보기", expanded=False):
        render_report_preview(
            title="장중 정책 연구 리포트 미리보기",
            preview=policy_report_preview,
        )

render_page_footer(settings, page_name="리서치")
