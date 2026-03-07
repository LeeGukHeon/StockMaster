# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.helpers import (
    latest_feature_coverage_frame,
    latest_feature_sample_frame,
    latest_flow_summary_frame,
    latest_label_coverage_frame,
    latest_prediction_summary_frame,
    latest_regime_frame,
    latest_selection_validation_summary_frame,
    latest_validation_summary_frame,
    leaderboard_frame,
    load_ui_settings,
    localize_frame,
    market_pulse_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
feature_sample = latest_feature_sample_frame(settings, limit=10)
feature_coverage = latest_feature_coverage_frame(settings)
label_coverage = latest_label_coverage_frame(settings)
flow_summary = latest_flow_summary_frame(settings)
prediction_summary = latest_prediction_summary_frame(settings)
regime_snapshot = latest_regime_frame(settings)
market_pulse = market_pulse_frame(settings)
selection_preview = leaderboard_frame(
    settings,
    horizon=5,
    limit=10,
    ranking_version=SELECTION_ENGINE_VERSION,
)
selection_validation = latest_selection_validation_summary_frame(settings, limit=10)
explanatory_validation = latest_validation_summary_frame(settings, limit=10)

st.title("연구")
st.caption(
    "피처 스토어, 라벨, 시장 상태, 설명형 순위, "
    "선정 엔진 v1을 점검하는 연구 화면입니다."
)

st.subheader("현재 연구 계층")
st.markdown(
    """
    - 가격 / 재무 / 뉴스 / 수급 / 데이터 품질 기반 피처 스냅샷
    - 다음 시가 기준 D+1 / D+5 미래 수익률 라벨
    - 국내 전체 / 코스피 / 코스닥 시장 상태 분류
    - 사람 설명용 설명형 순위 v0
    - 수급, 불확실성 프록시, 실행 패널티가 반영된 Selection Engine v1
    - 최신 Selection 결과에 붙는 보정된 Proxy Prediction Band
    """
)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("최신 피처 커버리지")
    st.dataframe(localize_frame(feature_coverage), width="stretch", hide_index=True)
    st.subheader("최신 라벨 커버리지")
    st.dataframe(localize_frame(label_coverage), width="stretch", hide_index=True)
    st.subheader("최신 수급 커버리지")
    st.dataframe(localize_frame(flow_summary), width="stretch", hide_index=True)
with summary_right:
    st.subheader("시장 현황")
    st.dataframe(localize_frame(market_pulse), width="stretch", hide_index=True)
    st.subheader("최신 시장 상태")
    st.dataframe(localize_frame(regime_snapshot), width="stretch", hide_index=True)
    st.subheader("최신 예측 요약")
    st.dataframe(localize_frame(prediction_summary), width="stretch", hide_index=True)

st.subheader("피처 매트릭스 샘플")
st.dataframe(localize_frame(feature_sample), width="stretch", hide_index=True)

st.subheader("선정 엔진 v1 미리보기 (D+5)")
preview = selection_preview[
    [
        "symbol",
        "company_name",
        "market",
        "final_selection_value",
        "grade",
        "expected_excess_return",
        "lower_band",
        "upper_band",
        "reasons",
        "risks",
    ]
].copy()
st.dataframe(localize_frame(preview), width="stretch", hide_index=True)

validation_left, validation_right = st.columns(2)
with validation_left:
    st.subheader("선정 엔진 검증")
    st.dataframe(localize_frame(selection_validation), width="stretch", hide_index=True)
with validation_right:
    st.subheader("설명형 순위 검증")
    st.dataframe(localize_frame(explanatory_validation), width="stretch", hide_index=True)
