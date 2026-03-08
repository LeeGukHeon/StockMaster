# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    latest_intraday_active_policy_frame,
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

model_training_summary = latest_model_training_summary_frame(settings)
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
policy_active = latest_intraday_active_policy_frame(settings, limit=30)
policy_rollbacks = latest_intraday_policy_rollback_frame(settings, limit=20)
policy_publish_status = latest_intraday_policy_publish_status_frame(settings, limit=12)
policy_report_preview = latest_intraday_policy_report_preview(settings)

st.title("연구")
st.caption(
    "모델 학습 요약과 함께 장중 policy calibration, walk-forward, ablation, recommendation, "
    "active registry를 연구용으로 확인하는 페이지입니다."
)

st.subheader("최신 ML 알파 학습")
if model_training_summary.empty:
    st.info("모델 학습 요약이 없습니다.")
else:
    st.dataframe(localize_frame(model_training_summary), width="stretch", hide_index=True)

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("실험 실행 이력")
    if policy_experiments.empty:
        st.info("정책 experiment run 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_experiments), width="stretch", hide_index=True)
with top_right:
    st.subheader("정책 recommendation")
    if policy_recommendation.empty:
        st.info("정책 recommendation 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_recommendation), width="stretch", hide_index=True)

mid_left, mid_right = st.columns(2)
with mid_left:
    st.subheader("Validation split 결과")
    if policy_calibration.empty:
        st.info("정책 calibration validation 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_calibration), width="stretch", hide_index=True)
with mid_right:
    st.subheader("Test split / Walk-Forward 결과")
    if policy_walkforward.empty:
        st.info("정책 walk-forward test 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_walkforward), width="stretch", hide_index=True)

bottom_left, bottom_right = st.columns(2)
with bottom_left:
    st.subheader("Ablation 결과")
    if policy_ablation.empty:
        st.info("정책 ablation 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_ablation), width="stretch", hide_index=True)
    st.subheader("정책 report/publish 상태")
    if policy_publish_status.empty:
        st.info("정책 research report 상태가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_publish_status), width="stretch", hide_index=True)
with bottom_right:
    st.subheader("활성 정책 레지스트리")
    if policy_active.empty:
        st.info("활성 정책이 없습니다.")
    else:
        st.dataframe(localize_frame(policy_active), width="stretch", hide_index=True)
    st.subheader("Rollback 이력")
    if policy_rollbacks.empty:
        st.info("Rollback 이력이 없습니다.")
    else:
        st.dataframe(localize_frame(policy_rollbacks), width="stretch", hide_index=True)

if policy_report_preview:
    with st.expander("최신 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_report_preview)
