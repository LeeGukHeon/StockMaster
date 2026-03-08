# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.meta_common import ENTER_PANEL, WAIT_PANEL
from app.ui.helpers import (
    intraday_meta_calibration_frame,
    intraday_meta_confusion_matrix_frame,
    intraday_meta_feature_importance_frame,
    latest_intraday_meta_overlay_comparison_frame,
    latest_intraday_meta_training_frame,
    latest_intraday_policy_ablation_frame,
    latest_intraday_policy_evaluation_frame,
    latest_intraday_policy_experiment_frame,
    latest_intraday_policy_publish_status_frame,
    latest_intraday_policy_recommendation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_policy_rollback_frame,
    latest_model_training_summary_frame,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)

alpha_training_summary = latest_model_training_summary_frame(settings)
meta_training_summary = latest_intraday_meta_training_frame(settings, limit=30)
policy_experiments = latest_intraday_policy_experiment_frame(settings, limit=30)
policy_calibration = latest_intraday_policy_evaluation_frame(
    settings,
    split_name="validation",
    limit=30,
)
policy_walkforward = latest_intraday_policy_evaluation_frame(
    settings,
    split_name="test",
    limit=30,
)
policy_ablation = latest_intraday_policy_ablation_frame(settings, limit=30)
policy_recommendation = latest_intraday_policy_recommendation_frame(settings, limit=30)
policy_rollbacks = latest_intraday_policy_rollback_frame(settings, limit=20)
policy_publish_status = latest_intraday_policy_publish_status_frame(settings, limit=12)
policy_report_preview = latest_intraday_policy_report_preview(settings)

meta_horizon = st.selectbox(
    "메타 모델 기간",
    options=[1, 5],
    index=0,
    format_func=lambda value: f"D+{value}",
)
meta_panel = st.selectbox(
    "메타 모델 패널",
    options=[ENTER_PANEL, WAIT_PANEL],
    index=0,
    format_func=lambda value: "진입 패널" if value == ENTER_PANEL else "대기 패널",
)
meta_calibration = intraday_meta_calibration_frame(
    settings,
    horizon=meta_horizon,
    panel_name=meta_panel,
)
meta_confusion = intraday_meta_confusion_matrix_frame(
    settings,
    horizon=meta_horizon,
    panel_name=meta_panel,
)
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

st.title("연구")
st.caption(
    "정책 실험 프레임과 TICKET-010 메타 모델 진단을 함께 보는 연구 화면입니다. "
    "메타 모델은 정책을 대체하는 엔진이 아니라 bounded overlay입니다."
)

st.subheader("최신 ML 알파 학습")
if alpha_training_summary.empty:
    st.info("ML 알파 학습 요약이 없습니다.")
else:
    st.dataframe(localize_frame(alpha_training_summary), width="stretch", hide_index=True)

st.subheader("장중 메타 모델 학습")
if meta_training_summary.empty:
    st.info("장중 메타 모델 학습 이력이 없습니다.")
else:
    st.dataframe(localize_frame(meta_training_summary), width="stretch", hide_index=True)

diag_left, diag_right = st.columns(2)
with diag_left:
    st.subheader("확률 보정 요약")
    if meta_calibration.empty:
        st.info("선택한 패널/기간의 calibration 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_calibration), width="stretch", hide_index=True)
    st.subheader("혼동 행렬")
    if meta_confusion.empty:
        st.info("선택한 패널/기간의 confusion matrix가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_confusion), width="stretch", hide_index=True)
with diag_right:
    st.subheader("피처 중요도")
    if meta_feature_importance.empty:
        st.info("선택한 패널/기간의 feature importance가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_feature_importance), width="stretch", hide_index=True)
    st.subheader("정책 대비 메타 overlay")
    if meta_overlay.empty:
        st.info("policy-only vs meta-overlay 비교가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_overlay), width="stretch", hide_index=True)

st.subheader("Regime 별 메타 overlay 분해")
if meta_regime_breakdown.empty:
    st.info("regime family 기준 overlay 분해가 없습니다.")
else:
    st.dataframe(localize_frame(meta_regime_breakdown), width="stretch", hide_index=True)

policy_top_left, policy_top_right = st.columns(2)
with policy_top_left:
    st.subheader("정책 실험 실행")
    if policy_experiments.empty:
        st.info("정책 experiment run 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_experiments), width="stretch", hide_index=True)
    st.subheader("검증 split 결과")
    if policy_calibration.empty:
        st.info("정책 validation split 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_calibration), width="stretch", hide_index=True)
with policy_top_right:
    st.subheader("Walk-Forward / Test")
    if policy_walkforward.empty:
        st.info("정책 walk-forward test 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_walkforward), width="stretch", hide_index=True)
    st.subheader("정책 추천")
    if policy_recommendation.empty:
        st.info("정책 recommendation 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_recommendation), width="stretch", hide_index=True)

policy_bottom_left, policy_bottom_right = st.columns(2)
with policy_bottom_left:
    st.subheader("정책 Ablation")
    if policy_ablation.empty:
        st.info("정책 ablation 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_ablation), width="stretch", hide_index=True)
    st.subheader("리포트 / Publish 상태")
    if policy_publish_status.empty:
        st.info("정책 연구 리포트 상태가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_publish_status), width="stretch", hide_index=True)
with policy_bottom_right:
    st.subheader("정책 Rollback 이력")
    if policy_rollbacks.empty:
        st.info("정책 rollback 이력이 없습니다.")
    else:
        st.dataframe(localize_frame(policy_rollbacks), width="stretch", hide_index=True)

if policy_report_preview:
    with st.expander("최신 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_report_preview)
