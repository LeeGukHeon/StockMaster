# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    calendar_summary_frame,
    disk_report,
    format_disk_status_label,
    latest_calibration_diagnostic_frame,
    latest_discord_preview,
    latest_evaluation_comparison_frame,
    latest_evaluation_summary_frame,
    latest_feature_coverage_frame,
    latest_flow_summary_frame,
    latest_label_coverage_frame,
    latest_model_metric_summary_frame,
    latest_model_training_summary_frame,
    latest_outcome_summary_frame,
    latest_postmortem_preview,
    latest_prediction_summary_frame,
    latest_regime_frame,
    latest_selection_engine_comparison_frame,
    latest_selection_validation_summary_frame,
    latest_sync_runs_frame,
    latest_validation_summary_frame,
    latest_version_frame,
    load_ui_settings,
    localize_frame,
    provider_health_frame,
    recent_failure_runs_frame,
    recent_runs_frame,
    research_data_summary_frame,
    universe_summary_frame,
    watermark_frame,
)


def _disk_message(report) -> str:
    status = str(report.status)
    usage = f"{report.usage_ratio:.1%}"
    if status == "limit":
        return f"디스크 사용률이 {usage}입니다. 즉시 수집량을 줄여야 합니다."
    if status == "prune":
        return f"디스크 사용률이 {usage}입니다. 정리 작업이 필요합니다."
    if status == "warning":
        return f"디스크 사용률이 {usage}입니다. 저장 공간을 주의 깊게 보세요."
    return f"디스크 사용률이 {usage}입니다. 현재는 정상 범위입니다."


settings = load_ui_settings(PROJECT_ROOT)
runs = recent_runs_frame(settings, limit=20)
storage_report = disk_report(settings)
watermarks = watermark_frame(settings)
universe_summary = universe_summary_frame(settings)
calendar_summary = calendar_summary_frame(settings)
provider_health = provider_health_frame(settings)
latest_sync_runs = latest_sync_runs_frame(settings)
research_summary = research_data_summary_frame(settings)
failed_runs = recent_failure_runs_frame(settings)
feature_coverage = latest_feature_coverage_frame(settings)
label_coverage = latest_label_coverage_frame(settings)
flow_summary = latest_flow_summary_frame(settings)
prediction_summary = latest_prediction_summary_frame(settings)
model_training_summary = latest_model_training_summary_frame(settings)
model_metric_summary = latest_model_metric_summary_frame(settings)
outcome_summary = latest_outcome_summary_frame(settings)
evaluation_summary = latest_evaluation_summary_frame(settings, limit=20)
evaluation_comparison = latest_evaluation_comparison_frame(settings)
selection_engine_comparison = latest_selection_engine_comparison_frame(settings)
calibration_summary = latest_calibration_diagnostic_frame(settings, limit=20)
latest_regime = latest_regime_frame(settings)
latest_versions = latest_version_frame(settings)
selection_validation = latest_selection_validation_summary_frame(settings, limit=20)
explanatory_validation = latest_validation_summary_frame(settings, limit=20)
discord_preview = latest_discord_preview(settings)
postmortem_preview = latest_postmortem_preview(settings)

st.title("운영")
st.caption(
    "적재, 피처 빌드, 설명형 순위, 선정 엔진 v1, "
    "프록시 밴드, 디스코드 리포트 상태를 한 화면에서 봅니다."
)

top_left, top_right = st.columns(2)
with top_left:
    st.metric(
        "현재 사용량",
        f"{storage_report.usage_ratio:.1%}",
        f"{storage_report.used_gb:.2f} GB 사용 중",
    )
    st.write(_disk_message(storage_report))
with top_right:
    st.metric(
        "가용 공간",
        f"{storage_report.available_gb:.2f} GB",
        format_disk_status_label(storage_report.status).upper(),
    )
    st.dataframe(localize_frame(watermarks), width="stretch", hide_index=True)

summary_left, summary_right = st.columns(2)
with summary_left:
    st.subheader("종목 유니버스")
    st.dataframe(localize_frame(universe_summary), width="stretch", hide_index=True)
with summary_right:
    st.subheader("거래일 캘린더")
    st.dataframe(localize_frame(calendar_summary), width="stretch", hide_index=True)

st.subheader("최근 동기화 상태")
st.dataframe(localize_frame(latest_sync_runs), width="stretch", hide_index=True)

st.subheader("연구 데이터 신선도")
st.dataframe(localize_frame(research_summary), width="stretch", hide_index=True)

ops_left, ops_right = st.columns(2)
with ops_left:
    st.subheader("피처 커버리지")
    st.dataframe(localize_frame(feature_coverage), width="stretch", hide_index=True)
    st.subheader("라벨 커버리지")
    st.dataframe(localize_frame(label_coverage), width="stretch", hide_index=True)
    st.subheader("수급 요약")
    st.dataframe(localize_frame(flow_summary), width="stretch", hide_index=True)
with ops_right:
    st.subheader("버전 추적")
    st.dataframe(localize_frame(latest_versions), width="stretch", hide_index=True)
    st.subheader("예측 요약")
    st.dataframe(localize_frame(prediction_summary), width="stretch", hide_index=True)
    st.subheader("ML 알파 학습 요약")
    st.dataframe(localize_frame(model_training_summary), width="stretch", hide_index=True)
    st.subheader("최신 시장 상태")
    st.dataframe(localize_frame(latest_regime), width="stretch", hide_index=True)

evaluation_left, evaluation_right = st.columns(2)
with evaluation_left:
    st.subheader("성과 요약")
    st.dataframe(localize_frame(outcome_summary), width="stretch", hide_index=True)
    st.subheader("평가 요약")
    st.dataframe(localize_frame(evaluation_summary), width="stretch", hide_index=True)
    st.subheader("알파 모델 검증 지표")
    st.dataframe(localize_frame(model_metric_summary), width="stretch", hide_index=True)
with evaluation_right:
    st.subheader("선정 엔진 대 설명형 순위 비교")
    st.dataframe(localize_frame(evaluation_comparison), width="stretch", hide_index=True)
    st.subheader("Selection v2 비교")
    st.dataframe(localize_frame(selection_engine_comparison), width="stretch", hide_index=True)
    st.subheader("보정 진단")
    st.dataframe(localize_frame(calibration_summary), width="stretch", hide_index=True)

validation_left, validation_right = st.columns(2)
with validation_left:
    st.subheader("선정 엔진 검증")
    st.dataframe(localize_frame(selection_validation), width="stretch", hide_index=True)
with validation_right:
    st.subheader("설명형 순위 검증")
    st.dataframe(localize_frame(explanatory_validation), width="stretch", hide_index=True)

st.subheader("프로바이더 상태")
st.dataframe(localize_frame(provider_health), width="stretch", hide_index=True)

if discord_preview:
    with st.expander("최신 디스코드 미리보기", expanded=False):
        st.code(discord_preview)

if postmortem_preview:
    with st.expander("최신 사후 분석 미리보기", expanded=False):
        st.code(postmortem_preview)

st.subheader("실행 이력")
st.dataframe(localize_frame(runs), width="stretch", hide_index=True)

st.subheader("최근 실패")
if failed_runs.empty:
    st.success("최근 실패 이력이 없습니다.")
else:
    st.dataframe(localize_frame(failed_runs), width="stretch", hide_index=True)
