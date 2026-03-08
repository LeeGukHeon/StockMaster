# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.meta_common import ENTER_PANEL, WAIT_PANEL
from app.ui.components import render_page_footer, render_page_header
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
policy_calibration = latest_intraday_policy_evaluation_frame(settings, split_name="validation", limit=30)
policy_walkforward = latest_intraday_policy_evaluation_frame(settings, split_name="test", limit=30)
policy_ablation = latest_intraday_policy_ablation_frame(settings, limit=30)
policy_recommendation = latest_intraday_policy_recommendation_frame(settings, limit=30)
policy_rollbacks = latest_intraday_policy_rollback_frame(settings, limit=20)
policy_publish_status = latest_intraday_policy_publish_status_frame(settings, limit=12)
policy_report_preview = latest_intraday_policy_report_preview(settings)

meta_horizon = st.selectbox("메타 모델 기간", options=[1, 5], index=0, format_func=lambda value: f"D+{value}")
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
    description="모델, 정책, 보정, 워크포워드, 제거 실험 결과를 기술적으로 비교하는 고급 화면입니다.",
)

st.subheader("최신 알파 모형 학습")
st.dataframe(localize_frame(alpha_training_summary), width="stretch", hide_index=True)

st.subheader("장중 메타 모형 학습")
st.dataframe(localize_frame(meta_training_summary), width="stretch", hide_index=True)

st.subheader("정책 대비 메타 보조")
st.dataframe(localize_frame(meta_overlay), width="stretch", hide_index=True)

st.subheader("국면별 분해")
st.dataframe(localize_frame(meta_regime_breakdown), width="stretch", hide_index=True)

if show_technical:
    diag_left, diag_right = st.columns(2)
    with diag_left:
        st.subheader("보정 상태")
        st.dataframe(localize_frame(meta_calibration), width="stretch", hide_index=True)
        st.subheader("혼동 행렬")
        st.dataframe(localize_frame(meta_confusion), width="stretch", hide_index=True)
    with diag_right:
        st.subheader("주요 특성 중요도")
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

if policy_report_preview:
    with st.expander("최신 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_report_preview)

render_page_footer(settings, page_name="리서치 랩")
