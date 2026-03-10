# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_warning_banner,
)
from app.ui.helpers import (
    intraday_console_adjusted_decision_frame,
    intraday_console_candidate_frame,
    intraday_console_decision_frame,
    intraday_console_market_context_frame,
    intraday_console_signal_frame,
    intraday_console_strategy_trace_frame,
    intraday_console_timing_frame,
    intraday_console_tuned_action_frame,
    latest_intraday_active_policy_frame,
    latest_intraday_checkpoint_health_frame,
    latest_intraday_decision_lineage_frame,
    latest_intraday_meta_active_model_frame,
    latest_intraday_meta_decision_frame,
    latest_intraday_meta_prediction_frame,
    latest_intraday_policy_recommendation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_postmortem_preview,
    latest_intraday_research_capability_frame,
    latest_intraday_status_frame,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_summary_report_preview,
    load_ui_settings,
    localize_frame,
)


def _compact_frame(frame, preferred_columns):
    if frame.empty:
        return frame
    columns = [column for column in preferred_columns if column in frame.columns]
    return frame[columns] if columns else frame


settings = load_ui_settings(PROJECT_ROOT)

status_frame = latest_intraday_status_frame(settings)
capability_frame = latest_intraday_research_capability_frame(settings, limit=20)
checkpoint_health = latest_intraday_checkpoint_health_frame(settings)
candidate_frame = intraday_console_candidate_frame(settings, limit=40)
market_context = intraday_console_market_context_frame(settings, limit=10)
signal_frame = intraday_console_signal_frame(settings, limit=40)
decision_frame = intraday_console_decision_frame(settings, limit=40)
adjusted_decision_frame = intraday_console_adjusted_decision_frame(settings, limit=40)
tuned_decision_frame = intraday_console_tuned_action_frame(settings, limit=40)
meta_prediction_frame = latest_intraday_meta_prediction_frame(settings, limit=40)
meta_decision_frame = latest_intraday_meta_decision_frame(settings, limit=40)
active_policy_frame = latest_intraday_active_policy_frame(settings, limit=20)
active_meta_model_frame = latest_intraday_meta_active_model_frame(settings, limit=20)
recommendation_frame = latest_intraday_policy_recommendation_frame(settings, limit=20)
strategy_trace_frame = intraday_console_strategy_trace_frame(settings, limit=50)
timing_frame = intraday_console_timing_frame(settings, limit=30)
lineage_frame = latest_intraday_decision_lineage_frame(settings, limit=40)
same_exit_frame = latest_intraday_strategy_comparison_frame(settings, limit=20)
summary_preview = latest_intraday_summary_report_preview(settings)
postmortem_preview = latest_intraday_postmortem_preview(settings)
policy_preview = latest_intraday_policy_report_preview(settings)

render_page_header(
    settings,
    page_name="장중 콘솔",
    title="장중 콘솔",
    description=(
        "장중 후보군 보조 엔진의 원 정책, 조정 정책, 메타 오버레이, 최종 액션을 "
        "연구용 기준으로 한 화면에서 확인합니다."
    ),
)
render_warning_banner(
    "INFO",
    "이 화면은 연구용 / 비매매 출력입니다. 자동 주문, 자동 체결, 자동 승격은 수행하지 않습니다.",
)

if status_frame.empty:
    render_narrative_card(
        "장중 연구 상태",
        "아직 장중 세션 데이터가 없습니다. 후보 세션 생성과 장중 보조 번들 실행 여부를 먼저 확인하세요.",
    )
else:
    row = status_frame.iloc[0]
    render_narrative_card(
        "장중 연구 상태",
        (
            f"최신 세션은 {row.get('session_date', '-')}, 후보 {row.get('candidate_symbols', '-')}종목, "
            f"원 정책 {row.get('raw_decision_symbols', '-')}, 조정 정책 {row.get('adjusted_symbols', '-')}, "
            f"최종 액션 {row.get('final_action_symbols', '-')}종목입니다."
        ),
    )

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("세션 요약")
    st.dataframe(
        localize_frame(
            _compact_frame(
                status_frame,
                [
                    "session_date",
                    "candidate_symbols",
                    "raw_decision_symbols",
                    "adjusted_symbols",
                    "final_action_symbols",
                    "latest_checkpoint_time",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )
with top_right:
    st.subheader("연구 기능 상태")
    st.dataframe(
        localize_frame(
            _compact_frame(
                capability_frame,
                [
                    "feature_slug",
                    "rollout_mode",
                    "dependency_ready_flag",
                    "report_available_flag",
                    "last_skip_reason",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

context_left, context_right = st.columns(2)
with context_left:
    st.subheader("시장 맥락")
    st.dataframe(
        localize_frame(
            _compact_frame(
                market_context,
                [
                    "checkpoint_time",
                    "market_session_state",
                    "prior_daily_regime_state",
                    "market_breadth_ratio",
                    "data_quality_flag",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )
with context_right:
    st.subheader("체크포인트 상태")
    st.dataframe(
        localize_frame(
            _compact_frame(
                checkpoint_health,
                [
                    "checkpoint_time",
                    "candidate_symbols",
                    "raw_decision_symbols",
                    "adjusted_symbols",
                    "final_action_symbols",
                    "status",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

candidate_left, candidate_right = st.columns(2)
with candidate_left:
    st.subheader("후보군")
    st.dataframe(
        localize_frame(
            _compact_frame(
                candidate_frame,
                [
                    "selection_date",
                    "symbol",
                    "company_name",
                    "horizon",
                    "candidate_rank",
                    "grade",
                    "expected_excess_return",
                    "session_status",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )
with candidate_right:
    st.subheader("원 정책 판단")
    st.dataframe(
        localize_frame(
            _compact_frame(
                decision_frame,
                [
                    "checkpoint_time",
                    "symbol",
                    "company_name",
                    "horizon",
                    "action",
                    "action_score",
                    "signal_quality_score",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

signal_left, signal_right = st.columns(2)
with signal_left:
    st.subheader("장중 신호")
    st.dataframe(
        localize_frame(
            _compact_frame(
                signal_frame,
                [
                    "checkpoint_time",
                    "symbol",
                    "horizon",
                    "signal_quality_score",
                    "timing_adjustment_score",
                    "risk_friction_score",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )
with signal_right:
    st.subheader("조정 정책 판단")
    st.dataframe(
        localize_frame(
            _compact_frame(
                adjusted_decision_frame,
                [
                    "checkpoint_time",
                    "symbol",
                    "company_name",
                    "horizon",
                    "market_regime_family",
                    "raw_action",
                    "adjusted_action",
                    "signal_quality_flag",
                    "fallback_flag",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

meta_left, meta_right = st.columns(2)
with meta_left:
    st.subheader("메타 예측")
    st.dataframe(
        localize_frame(
            _compact_frame(
                meta_prediction_frame,
                [
                    "checkpoint_time",
                    "symbol",
                    "company_name",
                    "horizon",
                    "predicted_class",
                    "predicted_class_probability",
                    "confidence_margin",
                    "uncertainty_score",
                    "disagreement_score",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )
with meta_right:
    st.subheader("메타 오버레이 / 최종 액션")
    st.dataframe(
        localize_frame(
            _compact_frame(
                meta_decision_frame,
                [
                    "checkpoint_time",
                    "symbol",
                    "company_name",
                    "horizon",
                    "raw_action",
                    "adjusted_action",
                    "final_action",
                    "predicted_class",
                    "confidence_margin",
                    "fallback_flag",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

lineage_left, lineage_right = st.columns(2)
with lineage_left:
    st.subheader("의사결정 라인리지")
    st.dataframe(
        localize_frame(
            _compact_frame(
                lineage_frame,
                [
                    "selection_date",
                    "checkpoint_time",
                    "symbol",
                    "company_name",
                    "horizon",
                    "raw_action",
                    "adjusted_action",
                    "final_action",
                    "portfolio_execution_mode",
                    "gate_status",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )
with lineage_right:
    st.subheader("전략 추적 / 동일 종료 비교")
    st.dataframe(
        localize_frame(
            _compact_frame(
                strategy_trace_frame,
                [
                    "symbol",
                    "company_name",
                    "horizon",
                    "strategy_id",
                    "executed_flag",
                    "timing_edge_vs_open_bps",
                    "outcome_status",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )
    st.dataframe(
        localize_frame(
            _compact_frame(
                same_exit_frame,
                [
                    "strategy_id",
                    "horizon",
                    "executed_count",
                    "execution_rate",
                    "mean_realized_excess_return",
                    "mean_timing_edge_vs_open_bps",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

policy_left, policy_right = st.columns(2)
with policy_left:
    st.subheader("활성 장중 정책 / 추천")
    st.dataframe(
        localize_frame(
            _compact_frame(
                active_policy_frame,
                ["policy_template", "scope_type", "effective_from_date", "note"],
            )
        ),
        width="stretch",
        hide_index=True,
    )
    st.dataframe(
        localize_frame(
            _compact_frame(
                recommendation_frame,
                ["scope_type", "horizon", "recommended_policy_template", "objective_score"],
            )
        ),
        width="stretch",
        hide_index=True,
    )
with policy_right:
    st.subheader("활성 메타 모델 / 타이밍 결과")
    st.dataframe(
        localize_frame(
            _compact_frame(
                active_meta_model_frame,
                ["horizon", "panel_name", "model_version", "effective_from_date", "note"],
            )
        ),
        width="stretch",
        hide_index=True,
    )
    st.dataframe(
        localize_frame(
            _compact_frame(
                timing_frame,
                [
                    "session_date",
                    "symbol",
                    "horizon",
                    "selected_checkpoint_time",
                    "selected_action",
                    "timing_edge_bps",
                    "outcome_status",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

st.subheader("튜닝 / 최종 액션")
st.dataframe(
    localize_frame(
        _compact_frame(
            tuned_decision_frame,
            [
                "checkpoint_time",
                "symbol",
                "company_name",
                "horizon",
                "tuned_action",
                "final_action",
                "confidence_margin",
                "uncertainty_score",
            ],
        )
    ),
    width="stretch",
    hide_index=True,
)

with st.expander("상세 원본 표 보기", expanded=False):
    st.dataframe(localize_frame(candidate_frame), width="stretch", hide_index=True)
    st.dataframe(localize_frame(signal_frame), width="stretch", hide_index=True)
    st.dataframe(localize_frame(adjusted_decision_frame), width="stretch", hide_index=True)
    st.dataframe(localize_frame(meta_decision_frame), width="stretch", hide_index=True)
    st.dataframe(localize_frame(lineage_frame), width="stretch", hide_index=True)

if summary_preview:
    with st.expander("최신 장중 요약 리포트 미리보기", expanded=False):
        st.code(summary_preview)

if policy_preview:
    with st.expander("최신 장중 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_preview)

if postmortem_preview:
    with st.expander("최신 장중 사후 분석 미리보기", expanded=False):
        st.code(postmortem_preview)

render_page_footer(settings, page_name="장중 콘솔")
