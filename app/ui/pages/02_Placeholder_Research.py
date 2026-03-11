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
    localize_frame,
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
    page_name="리서치 랩",
    title="리서치 랩",
    description="장중 정책, 메타 모델, 보정, 제거 실험, 수동 반영 후보를 비교하는 고급 연구 화면입니다.",
)
render_screen_guide(
    summary="일반 투자용 화면이 아니라, 추천 로직을 연구하고 비교하는 실험실입니다. 현재 정책과 새 후보를 비교한 뒤 사람이 직접 반영 여부를 결정할 때 사용합니다.",
    bullets=[
        "장중 기능 상태와 정책 대비 메타 오버레이부터 보면 전체 흐름을 이해하기 쉽습니다.",
        "고급 진단은 필요할 때만 펼쳐 보세요.",
        "수동 반영 전 비교 표는 실제 교체 버튼을 누르기 전 마지막 확인 영역입니다.",
    ],
)
render_warning_banner(
    "INFO",
    "자동 계산은 수행되지만 자동 반영은 하지 않습니다. 아래 비교표를 확인한 뒤 체크박스와 버튼으로만 수동 반영할 수 있습니다.",
)

st.subheader("장중 리서치 기능 상태")
st.dataframe(localize_frame(intraday_capability), width="stretch", hide_index=True)

st.subheader("최신 알파 모델 학습")
st.dataframe(localize_frame(alpha_training_summary), width="stretch", hide_index=True)

st.subheader("장중 메타 모델 학습")
st.dataframe(localize_frame(meta_training_summary), width="stretch", hide_index=True)

st.subheader("정책 대비 메타 오버레이")
st.dataframe(localize_frame(meta_overlay), width="stretch", hide_index=True)

st.subheader("국면별 분해")
st.dataframe(localize_frame(meta_regime_breakdown), width="stretch", hide_index=True)

if show_technical:
    diag_left, diag_right = st.columns(2)
    with diag_left:
        st.subheader("메타 보정")
        st.dataframe(localize_frame(meta_calibration), width="stretch", hide_index=True)
        st.subheader("혼동 행렬")
        st.dataframe(localize_frame(meta_confusion), width="stretch", hide_index=True)
    with diag_right:
        st.subheader("주요 특징 중요도")
        st.dataframe(localize_frame(meta_feature_importance), width="stretch", hide_index=True)
        st.subheader("정책 검증")
        st.dataframe(localize_frame(policy_calibration), width="stretch", hide_index=True)

policy_left, policy_right = st.columns(2)
with policy_left:
    st.subheader("정책 실험")
    st.dataframe(localize_frame(policy_experiments), width="stretch", hide_index=True)
    st.subheader("워크포워드")
    st.dataframe(localize_frame(policy_walkforward), width="stretch", hide_index=True)
with policy_right:
    st.subheader("정책 추천")
    st.dataframe(localize_frame(policy_recommendation), width="stretch", hide_index=True)
    st.subheader("정책 발행 / 롤백")
    if not policy_publish_status.empty:
        st.dataframe(localize_frame(policy_publish_status), width="stretch", hide_index=True)
    if not policy_rollbacks.empty:
        st.dataframe(localize_frame(policy_rollbacks), width="stretch", hide_index=True)

if show_technical:
    st.subheader("정책 제거 실험")
    st.dataframe(localize_frame(policy_ablation), width="stretch", hide_index=True)

st.subheader("수동 반영 전 비교")
st.info(
    "주간 학습 후보와 주간 보정 결과는 자동 계산만 합니다. 활성 정책과 활성 메타 모델은 절대 자동 승격하지 않으며, 아래 비교 후 직접 확인해야만 반영됩니다."
)

policy_compare_left, policy_compare_right = st.columns(2)
with policy_compare_left:
    st.caption("현재 활성 정책")
    st.dataframe(localize_frame(active_policy), width="stretch", hide_index=True)
with policy_compare_right:
    st.caption("다음 반영 후보 정책")
    st.dataframe(localize_frame(policy_apply_compare), width="stretch", hide_index=True)

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

meta_compare_left, meta_compare_right = st.columns(2)
with meta_compare_left:
    st.caption("현재 활성 메타 모델")
    st.dataframe(localize_frame(active_meta_models), width="stretch", hide_index=True)
with meta_compare_right:
    st.caption("다음 반영 후보 메타 모델")
    st.dataframe(localize_frame(meta_apply_compare), width="stretch", hide_index=True)

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

render_page_footer(settings, page_name="리서치 랩")
