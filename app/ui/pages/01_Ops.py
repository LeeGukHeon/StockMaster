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
    latest_intraday_active_policy_frame,
    latest_intraday_adjustment_summary_frame,
    latest_intraday_checkpoint_health_frame,
    latest_intraday_market_context_frame,
    latest_intraday_meta_active_model_frame,
    latest_intraday_meta_decision_frame,
    latest_intraday_meta_overlay_comparison_frame,
    latest_intraday_meta_prediction_frame,
    latest_intraday_meta_rollback_frame,
    latest_intraday_meta_run_status_frame,
    latest_intraday_meta_training_frame,
    latest_intraday_policy_ablation_frame,
    latest_intraday_policy_evaluation_frame,
    latest_intraday_policy_experiment_frame,
    latest_intraday_policy_publish_status_frame,
    latest_intraday_policy_recommendation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_policy_rollback_frame,
    latest_intraday_postmortem_preview,
    latest_intraday_publish_status_frame,
    latest_intraday_status_frame,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_timing_calibration_frame,
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
        return f"디스크 사용률이 {usage}입니다. 여유 공간을 주의 깊게 봐야 합니다."
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
intraday_status = latest_intraday_status_frame(settings)
intraday_checkpoint_health = latest_intraday_checkpoint_health_frame(settings)
intraday_market_context = latest_intraday_market_context_frame(settings, limit=12)
intraday_adjustment_summary = latest_intraday_adjustment_summary_frame(settings, limit=20)
intraday_strategy_comparison = latest_intraday_strategy_comparison_frame(settings, limit=20)
intraday_timing_calibration = latest_intraday_timing_calibration_frame(settings, limit=20)
intraday_publish_status = latest_intraday_publish_status_frame(settings, limit=12)
policy_experiments = latest_intraday_policy_experiment_frame(settings, limit=20)
policy_walkforward = latest_intraday_policy_evaluation_frame(settings, split_name="test", limit=20)
policy_ablation = latest_intraday_policy_ablation_frame(settings, limit=20)
policy_recommendation = latest_intraday_policy_recommendation_frame(settings, limit=20)
policy_active = latest_intraday_active_policy_frame(settings, limit=20)
policy_rollbacks = latest_intraday_policy_rollback_frame(settings, limit=20)
policy_publish_status = latest_intraday_policy_publish_status_frame(settings, limit=12)
meta_training = latest_intraday_meta_training_frame(settings, limit=20)
meta_active = latest_intraday_meta_active_model_frame(settings, limit=20)
meta_rollbacks = latest_intraday_meta_rollback_frame(settings, limit=20)
meta_run_status = latest_intraday_meta_run_status_frame(settings, limit=12)
meta_prediction = latest_intraday_meta_prediction_frame(settings, limit=20)
meta_decision = latest_intraday_meta_decision_frame(settings, limit=20)
meta_overlay = latest_intraday_meta_overlay_comparison_frame(settings, limit=20)
discord_preview = latest_discord_preview(settings)
postmortem_preview = latest_postmortem_preview(settings)
intraday_postmortem_preview = latest_intraday_postmortem_preview(settings)
policy_report_preview = latest_intraday_policy_report_preview(settings)

st.title("운영")
st.caption(
    "수집, feature, selection, intraday, policy calibration, report publish 상태를 한 화면에서 "
    "점검하는 운영 콘솔입니다."
)

top_left, top_right = st.columns(2)
with top_left:
    st.metric(
        "현재 사용률",
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

st.subheader("최신 수집 상태")
st.dataframe(localize_frame(latest_sync_runs), width="stretch", hide_index=True)

st.subheader("연구 데이터 최신성")
st.dataframe(localize_frame(research_summary), width="stretch", hide_index=True)

ops_left, ops_right = st.columns(2)
with ops_left:
    st.subheader("Feature coverage")
    st.dataframe(localize_frame(feature_coverage), width="stretch", hide_index=True)
    st.subheader("Label coverage")
    st.dataframe(localize_frame(label_coverage), width="stretch", hide_index=True)
    st.subheader("Flow summary")
    st.dataframe(localize_frame(flow_summary), width="stretch", hide_index=True)
with ops_right:
    st.subheader("버전 추적")
    st.dataframe(localize_frame(latest_versions), width="stretch", hide_index=True)
    st.subheader("Prediction summary")
    st.dataframe(localize_frame(prediction_summary), width="stretch", hide_index=True)
    st.subheader("모델 학습 요약")
    st.dataframe(localize_frame(model_training_summary), width="stretch", hide_index=True)
    st.subheader("최신 시장 상태")
    st.dataframe(localize_frame(latest_regime), width="stretch", hide_index=True)

st.subheader("장중 수집 상태")
intraday_left, intraday_right = st.columns(2)
with intraday_left:
    if intraday_status.empty:
        st.info("장중 세션 상태가 없습니다.")
    else:
        st.dataframe(localize_frame(intraday_status), width="stretch", hide_index=True)
with intraday_right:
    if intraday_checkpoint_health.empty:
        st.info("장중 체크포인트 헬스가 없습니다.")
    else:
        st.dataframe(localize_frame(intraday_checkpoint_health), width="stretch", hide_index=True)

intraday_detail_left, intraday_detail_right = st.columns(2)
with intraday_detail_left:
    st.subheader("장중 market context")
    if intraday_market_context.empty:
        st.info("장중 market context 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(intraday_market_context), width="stretch", hide_index=True)
    st.subheader("장중 adjustment 요약")
    if intraday_adjustment_summary.empty:
        st.info("장중 adjustment summary가 없습니다.")
    else:
        st.dataframe(localize_frame(intraday_adjustment_summary), width="stretch", hide_index=True)
with intraday_detail_right:
    st.subheader("장중 strategy comparison")
    if intraday_strategy_comparison.empty:
        st.info("장중 strategy comparison 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(intraday_strategy_comparison), width="stretch", hide_index=True)
    st.subheader("장중 timing calibration")
    if intraday_timing_calibration.empty:
        st.info("장중 timing calibration 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(intraday_timing_calibration), width="stretch", hide_index=True)

st.subheader("정책 실험 프레임워크")
policy_left, policy_right = st.columns(2)
with policy_left:
    st.subheader("실험 실행 이력")
    if policy_experiments.empty:
        st.info("정책 experiment run 이력이 없습니다.")
    else:
        st.dataframe(localize_frame(policy_experiments), width="stretch", hide_index=True)
    st.subheader("정책 Walk-Forward 결과")
    if policy_walkforward.empty:
        st.info("정책 walk-forward/test 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_walkforward), width="stretch", hide_index=True)
    st.subheader("정책 Ablation")
    if policy_ablation.empty:
        st.info("정책 ablation 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_ablation), width="stretch", hide_index=True)
with policy_right:
    st.subheader("최신 정책 추천")
    if policy_recommendation.empty:
        st.info("정책 recommendation 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_recommendation), width="stretch", hide_index=True)
    st.subheader("활성 정책 레지스트리")
    if policy_active.empty:
        st.info("활성 정책 레지스트리가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_active), width="stretch", hide_index=True)
    st.subheader("Rollback 이력")
    if policy_rollbacks.empty:
        st.info("정책 rollback 이력이 없습니다.")
    else:
        st.dataframe(localize_frame(policy_rollbacks), width="stretch", hide_index=True)

st.subheader("메타모델 운영 상태")
meta_left, meta_right = st.columns(2)
with meta_left:
    st.subheader("최신 메타모델 학습")
    if meta_training.empty:
        st.info("메타모델 학습 이력이 없습니다.")
    else:
        st.dataframe(localize_frame(meta_training), width="stretch", hide_index=True)
    st.subheader("활성 메타모델 레지스트리")
    if meta_active.empty:
        st.info("활성 메타모델 레지스트리가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_active), width="stretch", hide_index=True)
    st.subheader("메타모델 Rollback 이력")
    if meta_rollbacks.empty:
        st.info("메타모델 rollback 이력이 없습니다.")
    else:
        st.dataframe(localize_frame(meta_rollbacks), width="stretch", hide_index=True)
with meta_right:
    st.subheader("메타모델 실행 상태")
    if meta_run_status.empty:
        st.info("메타모델 run status가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_run_status), width="stretch", hide_index=True)
    st.subheader("최신 메타 예측")
    if meta_prediction.empty:
        st.info("메타 prediction 결과가 없습니다.")
    else:
        st.dataframe(localize_frame(meta_prediction), width="stretch", hide_index=True)
    st.subheader("최신 최종 액션 / overlay")
    if meta_decision.empty and meta_overlay.empty:
        st.info("메타 final action / overlay 비교 결과가 없습니다.")
    else:
        if not meta_decision.empty:
            st.dataframe(localize_frame(meta_decision), width="stretch", hide_index=True)
        if not meta_overlay.empty:
            st.dataframe(localize_frame(meta_overlay), width="stretch", hide_index=True)

publish_left, publish_right = st.columns(2)
with publish_left:
    st.subheader("장중 리포트 발행 상태")
    if intraday_publish_status.empty:
        st.info("장중 postmortem render/publish 상태가 없습니다.")
    else:
        st.dataframe(localize_frame(intraday_publish_status), width="stretch", hide_index=True)
with publish_right:
    st.subheader("정책 리포트 발행 상태")
    if policy_publish_status.empty:
        st.info("정책 research render/publish 상태가 없습니다.")
    else:
        st.dataframe(localize_frame(policy_publish_status), width="stretch", hide_index=True)

evaluation_left, evaluation_right = st.columns(2)
with evaluation_left:
    st.subheader("Outcome summary")
    st.dataframe(localize_frame(outcome_summary), width="stretch", hide_index=True)
    st.subheader("Evaluation summary")
    st.dataframe(localize_frame(evaluation_summary), width="stretch", hide_index=True)
    st.subheader("알파 모델 metric")
    st.dataframe(localize_frame(model_metric_summary), width="stretch", hide_index=True)
with evaluation_right:
    st.subheader("Selection vs 설명형 랭킹")
    st.dataframe(localize_frame(evaluation_comparison), width="stretch", hide_index=True)
    st.subheader("Selection v2 comparison")
    st.dataframe(localize_frame(selection_engine_comparison), width="stretch", hide_index=True)
    st.subheader("Calibration 진단")
    st.dataframe(localize_frame(calibration_summary), width="stretch", hide_index=True)

validation_left, validation_right = st.columns(2)
with validation_left:
    st.subheader("Selection validation")
    st.dataframe(localize_frame(selection_validation), width="stretch", hide_index=True)
with validation_right:
    st.subheader("설명형 랭킹 validation")
    st.dataframe(localize_frame(explanatory_validation), width="stretch", hide_index=True)

st.subheader("Provider 상태")
st.dataframe(localize_frame(provider_health), width="stretch", hide_index=True)

if discord_preview:
    with st.expander("최신 Discord EOD 미리보기", expanded=False):
        st.code(discord_preview)

if postmortem_preview:
    with st.expander("최신 selection postmortem 미리보기", expanded=False):
        st.code(postmortem_preview)

if intraday_postmortem_preview:
    with st.expander("최신 장중 postmortem 미리보기", expanded=False):
        st.code(intraday_postmortem_preview)

if policy_report_preview:
    with st.expander("최신 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_report_preview)

st.subheader("실행 이력")
st.dataframe(localize_frame(runs), width="stretch", hide_index=True)

st.subheader("최근 실패")
if failed_runs.empty:
    st.success("최근 실패 이력이 없습니다.")
else:
    st.dataframe(localize_frame(failed_runs), width="stretch", hide_index=True)
