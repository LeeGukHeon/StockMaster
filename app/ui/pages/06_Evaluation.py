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
from app.ui.components import render_narrative_card, render_page_footer, render_page_header
from app.ui.helpers import (
    available_evaluation_dates,
    evaluation_outcomes_frame,
    format_ranking_version_label,
    latest_calibration_diagnostic_frame,
    latest_evaluation_comparison_frame,
    latest_evaluation_summary_frame,
    latest_intraday_meta_overlay_comparison_frame,
    latest_intraday_policy_ablation_frame,
    latest_intraday_policy_evaluation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_postmortem_preview,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_timing_calibration_frame,
    latest_postmortem_preview,
    latest_selection_engine_comparison_frame,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
evaluation_dates = available_evaluation_dates(settings)

latest_summary = latest_evaluation_summary_frame(settings, limit=30)
latest_comparison = latest_evaluation_comparison_frame(settings)
latest_selection_v2_comparison = latest_selection_engine_comparison_frame(settings)
latest_calibration = latest_calibration_diagnostic_frame(settings, limit=30)
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
policy_report_preview = latest_intraday_policy_report_preview(settings)

render_page_header(
    settings,
    page_name="사후 평가",
    title="사후 평가",
    description="D+1/D+5 matured summary, calibration, miss reason, intraday/policy/meta 비교를 same-exit 기준으로 확인합니다.",
)

render_narrative_card(
    "Evaluation Narrative",
    "이 화면은 pre-cost 기준입니다. selection, timing, policy, meta overlay를 다시 계산하지 않고 이미 freeze된 snapshot을 기준으로 비교합니다.",
)

if not evaluation_dates:
    st.info("아직 selection evaluation 결과가 없습니다. 평가 스크립트를 먼저 실행하세요.")
else:
    selected_date = st.selectbox("평가일", options=evaluation_dates, index=0)
    horizon = st.selectbox("기간", options=[1, 5], index=1, format_func=lambda value: f"D+{value}")
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
    limit = st.slider("표시 건수", min_value=10, max_value=100, value=25, step=5)
    outcomes = evaluation_outcomes_frame(
        settings,
        evaluation_date=selected_date,
        horizon=horizon,
        ranking_version=ranking_version,
        limit=limit,
    )

    top_left, top_right = st.columns(2)
    with top_left:
        st.subheader("최신 평가 요약")
        st.dataframe(localize_frame(latest_summary), width="stretch", hide_index=True)
        st.subheader("Selection vs Explanatory")
        st.dataframe(localize_frame(latest_comparison), width="stretch", hide_index=True)
        st.subheader("Selection v2 비교")
        st.dataframe(localize_frame(latest_selection_v2_comparison), width="stretch", hide_index=True)
    with top_right:
        st.subheader("Band Coverage / Calibration")
        st.dataframe(localize_frame(latest_calibration), width="stretch", hide_index=True)
        st.subheader("선택 결과 샘플")
        st.dataframe(localize_frame(outcomes), width="stretch", hide_index=True)

intraday_left, intraday_right = st.columns(2)
with intraday_left:
    st.subheader("장중 전략 비교")
    st.dataframe(localize_frame(intraday_strategy_comparison), width="stretch", hide_index=True)
    st.subheader("장중 Regime Matrix")
    st.dataframe(localize_frame(intraday_regime_matrix), width="stretch", hide_index=True)
with intraday_right:
    st.subheader("장중 Timing Calibration")
    st.dataframe(localize_frame(intraday_timing_calibration), width="stretch", hide_index=True)
    st.subheader("Policy Walk-Forward")
    st.dataframe(localize_frame(policy_walkforward), width="stretch", hide_index=True)

meta_left, meta_right = st.columns(2)
with meta_left:
    st.subheader("Policy-only vs Meta Overlay")
    st.dataframe(localize_frame(meta_overlay), width="stretch", hide_index=True)
    st.subheader("Meta Overlay Regime Breakdown")
    st.dataframe(localize_frame(meta_regime_breakdown), width="stretch", hide_index=True)
with meta_right:
    st.subheader("Meta Overlay Checkpoint Breakdown")
    st.dataframe(localize_frame(meta_checkpoint_breakdown), width="stretch", hide_index=True)
    st.subheader("Policy Ablation")
    st.dataframe(localize_frame(policy_ablation), width="stretch", hide_index=True)

if policy_report_preview:
    with st.expander("최신 정책 연구 리포트 미리보기", expanded=False):
        st.code(policy_report_preview)

if intraday_postmortem_preview:
    with st.expander("최신 장중 postmortem 미리보기", expanded=False):
        st.code(intraday_postmortem_preview)

if postmortem_preview:
    with st.expander("최신 selection postmortem 미리보기", expanded=False):
        st.code(postmortem_preview)

render_page_footer(settings, page_name="사후 평가")
