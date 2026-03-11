# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.selection.engine_v2 import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ui.components import (
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_report_preview,
    render_record_cards,
    render_screen_guide,
    render_warning_banner,
)
from app.ui.helpers import (
    available_evaluation_dates,
    evaluation_outcomes_frame,
    format_ranking_version_label,
    latest_alpha_promotion_summary_frame,
    latest_calibration_diagnostic_frame,
    latest_evaluation_comparison_frame,
    latest_evaluation_summary_frame,
    latest_intraday_meta_overlay_comparison_frame,
    latest_intraday_policy_ablation_frame,
    latest_intraday_policy_evaluation_frame,
    latest_intraday_postmortem_preview,
    latest_intraday_research_capability_frame,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_timing_calibration_frame,
    latest_postmortem_preview,
    latest_selection_engine_comparison_frame,
    load_ui_settings,
)

settings = load_ui_settings(PROJECT_ROOT)
evaluation_dates = available_evaluation_dates(settings)

latest_summary = latest_evaluation_summary_frame(settings, limit=30)
latest_comparison = latest_evaluation_comparison_frame(settings)
latest_selection_v2_comparison = latest_selection_engine_comparison_frame(settings)
latest_alpha_promotion = latest_alpha_promotion_summary_frame(settings, limit=10)
latest_calibration = latest_calibration_diagnostic_frame(settings, limit=30)
intraday_capability = latest_intraday_research_capability_frame(settings, limit=20)
intraday_strategy_comparison = latest_intraday_strategy_comparison_frame(settings, limit=30)
intraday_regime_matrix = latest_intraday_strategy_comparison_frame(
    settings,
    comparison_scope="regime_family",
    limit=30,
)
intraday_timing_calibration = latest_intraday_timing_calibration_frame(settings, limit=30)
policy_walkforward = latest_intraday_policy_evaluation_frame(settings, split_name="test", limit=30)
policy_ablation = latest_intraday_policy_ablation_frame(settings, limit=30)
meta_overlay = latest_intraday_meta_overlay_comparison_frame(settings, limit=30)
meta_regime_breakdown = latest_intraday_meta_overlay_comparison_frame(
    settings,
    metric_scope="regime",
    limit=30,
)
meta_checkpoint_breakdown = latest_intraday_meta_overlay_comparison_frame(
    settings,
    metric_scope="checkpoint",
    limit=30,
)
postmortem_preview = latest_postmortem_preview(settings)
intraday_postmortem_preview = latest_intraday_postmortem_preview(settings)

render_page_header(
    settings,
    page_name="사후 평가",
    title="사후 평가",
    description="1거래일·5거래일 뒤 결과와 예측 범위 점검 상태, 장중 비교 결과를 한눈에 확인합니다.",
)
render_screen_guide(
    summary="추천이 시간이 지난 뒤 실제로 어땠는지 확인하는 화면입니다. 수익이 났는지, 예상 범위가 너무 낙관적이었는지, 최근 흐름이 좋아지는지 나빠지는지를 보는 곳입니다.",
    bullets=[
        "처음에는 최신 평가 요약과 평가 결과 샘플만 읽어도 충분합니다.",
        "비교표는 우리 추천 모델이 기준선보다 나았는지 확인하는 참고 자료로 보면 됩니다.",
        "장중 비교와 메타 오버레이는 연구용 참고이며 자동 주문과는 연결되지 않습니다.",
    ],
)
render_warning_banner(
    "INFO",
    "장중 비교와 메타 오버레이는 연구용 비매매 평가입니다. 자동 주문이나 자동 승격은 없습니다.",
)
render_narrative_card(
    "사후 평가 요약",
    "이 화면은 고정된 예측 스냅샷 기준입니다. 사후 값을 다시 계산해서 덮어쓰지 않고 같은 종료 기준으로 비교합니다.",
)

if not evaluation_dates:
    st.info("아직 평가 결과가 없습니다. 평가 스크립트를 먼저 실행해 주세요.")
else:
    selected_date = st.selectbox("평가일", options=evaluation_dates, index=0)
    horizon = st.selectbox("기간", options=[1, 5], index=1, format_func=lambda value: f"{value}거래일")
    ranking_version = st.selectbox(
        "순위 버전",
        options=[
            SELECTION_ENGINE_V2_VERSION,
            SELECTION_ENGINE_VERSION,
            EXPLANATORY_RANKING_VERSION,
        ],
        index=0,
        format_func=format_ranking_version_label,
    )
    limit = st.slider("표시 개수", min_value=10, max_value=100, value=25, step=5)
    outcomes = evaluation_outcomes_frame(
        settings,
        evaluation_date=selected_date,
        horizon=horizon,
        ranking_version=ranking_version,
        limit=limit,
    )

    render_record_cards(
        latest_summary,
        title="최신 평가 요약",
        primary_column="summary_name",
        secondary_columns=["window_type"],
        detail_columns=["summary_value", "horizon"],
        limit=8,
        empty_message="최신 평가 요약이 없습니다.",
        table_expander_label="평가 요약 원본 표 보기",
    )
    render_record_cards(
        latest_comparison,
        title="현재 추천 모델과 비교 기준 차이",
        primary_column="metric_name",
        secondary_columns=["horizon"],
        detail_columns=["selection_v2_avg_excess", "explanatory_avg_excess"],
        limit=8,
        empty_message="비교 평가 데이터가 없습니다.",
        table_expander_label="모델 비교 원본 표 보기",
    )
    render_record_cards(
        latest_selection_v2_comparison,
        title="추천 방식 비교",
        primary_column="metric_name",
        secondary_columns=["window_type"],
        detail_columns=["current_value", "prior_value"],
        limit=8,
        empty_message="선정 엔진 비교 데이터가 없습니다.",
        table_expander_label="추천 방식 비교 원본 표 보기",
    )
    render_record_cards(
        latest_alpha_promotion,
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
            "p_value",
        ],
        limit=4,
        empty_message="아직 알파 모델 비교 기록이 없습니다.",
        table_expander_label="알파 모델 비교 원본 표 보기",
    )
    render_record_cards(
        latest_calibration,
        title="밴드 보정 / 커버리지",
        primary_column="diagnostic_name",
        secondary_columns=["horizon"],
        detail_columns=["diagnostic_value", "window_end_date"],
        limit=8,
        empty_message="보정 데이터가 없습니다.",
        table_expander_label="보정 원본 표 보기",
    )
    render_record_cards(
        outcomes,
        title="평가 결과 샘플",
        primary_column="symbol",
        secondary_columns=["company_name", "outcome_status"],
        detail_columns=["selection_date", "horizon", "realized_excess_return", "band_status"],
        limit=8,
        empty_message="평가 결과 샘플이 없습니다.",
        table_expander_label="평가 결과 원본 표 보기",
    )

render_record_cards(
    intraday_capability,
    title="장중 연구 기능 상태",
    primary_column="feature_slug",
    secondary_columns=["rollout_mode"],
    detail_columns=["dependency_ready_flag", "report_available_flag", "last_skip_reason"],
    limit=8,
    empty_message="장중 연구 기능 상태가 없습니다.",
    table_expander_label="장중 기능 상태 원본 표 보기",
)

render_record_cards(
    intraday_strategy_comparison,
    title="장중 동일 종료 비교",
    primary_column="strategy_id",
    secondary_columns=["horizon"],
    detail_columns=["executed_count", "execution_rate", "mean_realized_excess_return"],
    limit=8,
    empty_message="장중 전략 비교 데이터가 없습니다.",
    table_expander_label="장중 전략 비교 원본 표 보기",
)
render_record_cards(
    intraday_regime_matrix,
    title="장중 구간별 비교",
    primary_column="comparison_value",
    secondary_columns=["strategy_id"],
    detail_columns=["horizon", "mean_realized_excess_return", "positive_timing_edge_rate"],
    limit=8,
    empty_message="구간별 장중 비교 데이터가 없습니다.",
    table_expander_label="구간별 장중 비교 원본 표 보기",
)
render_record_cards(
    intraday_timing_calibration,
    title="장중 타이밍 보정",
    primary_column="calibration_name",
    secondary_columns=["horizon"],
    detail_columns=["calibration_value", "window_end_date"],
    limit=8,
    empty_message="장중 타이밍 보정 데이터가 없습니다.",
    table_expander_label="장중 타이밍 보정 원본 표 보기",
)
render_record_cards(
    policy_walkforward,
    title="정책 워크포워드",
    primary_column="policy_template",
    secondary_columns=["scope_type", "horizon"],
    detail_columns=["objective_score", "test_session_count", "manual_review_required_flag"],
    limit=8,
    empty_message="정책 워크포워드 데이터가 없습니다.",
    table_expander_label="정책 워크포워드 원본 표 보기",
)
render_record_cards(
    meta_overlay,
    title="정책 대비 메타 오버레이",
    primary_column="metric_name",
    secondary_columns=["horizon"],
    detail_columns=["policy_only_value", "meta_overlay_value"],
    limit=8,
    empty_message="메타 오버레이 비교 데이터가 없습니다.",
    table_expander_label="메타 오버레이 원본 표 보기",
)
render_record_cards(
    meta_regime_breakdown,
    title="메타 오버레이 구간별 결과",
    primary_column="metric_name",
    secondary_columns=["comparison_value"],
    detail_columns=["policy_only_value", "meta_overlay_value"],
    limit=8,
    empty_message="구간별 메타 비교 데이터가 없습니다.",
    table_expander_label="구간별 메타 비교 원본 표 보기",
)
render_record_cards(
    meta_checkpoint_breakdown,
    title="메타 오버레이 체크포인트별 결과",
    primary_column="metric_name",
    secondary_columns=["comparison_value"],
    detail_columns=["policy_only_value", "meta_overlay_value"],
    limit=8,
    empty_message="체크포인트별 메타 비교 데이터가 없습니다.",
    table_expander_label="체크포인트별 메타 비교 원본 표 보기",
)
render_record_cards(
    policy_ablation,
    title="정책 제거 실험",
    primary_column="ablation_name",
    secondary_columns=["horizon"],
    detail_columns=["objective_score_delta", "manual_review_required_flag"],
    limit=8,
    empty_message="정책 제거 실험 데이터가 없습니다.",
    table_expander_label="정책 제거 실험 원본 표 보기",
)

if intraday_postmortem_preview:
    with st.expander("최신 장중 사후 분석 미리보기", expanded=False):
        render_report_preview(
            title="장중 사후 분석 미리보기",
            preview=intraday_postmortem_preview,
        )

if postmortem_preview:
    with st.expander("최신 일반 사후 분석 미리보기", expanded=False):
        render_report_preview(
            title="사후 점검 리포트 미리보기",
            preview=postmortem_preview,
        )

render_page_footer(settings, page_name="사후 평가")
