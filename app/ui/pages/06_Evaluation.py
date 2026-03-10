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
    render_warning_banner,
)
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
    latest_intraday_postmortem_preview,
    latest_intraday_research_capability_frame,
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
    description="D+1 / D+5 성과, 밴드 커버리지, 모델 비교, 장중 동일 종료 비교를 함께 확인합니다.",
)
render_warning_banner(
    "INFO",
    "장중 비교와 메타 오버레이는 리서치 전용 / 비매매 평가입니다. 자동 주문이나 자동 승격은 수행하지 않습니다.",
)

render_narrative_card(
    "사후 평가 요약",
    "이 화면은 고정된 예측 스냅샷 기준입니다. 장후 선정, 장중 조정, 메타 오버레이를 다시 계산하지 않고 같은 종료 기준으로 비교합니다.",
)

if not evaluation_dates:
    st.info("아직 평가 결과가 없습니다. 평가 스크립트를 먼저 실행하세요.")
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
        st.subheader("설명형 대비 선정 비교")
        st.dataframe(localize_frame(latest_comparison), width="stretch", hide_index=True)
        st.subheader("선정 v2 비교")
        st.dataframe(localize_frame(latest_selection_v2_comparison), width="stretch", hide_index=True)
    with top_right:
        st.subheader("밴드 커버리지 / 보정")
        st.dataframe(localize_frame(latest_calibration), width="stretch", hide_index=True)
        st.subheader("평가 결과 샘플")
        st.dataframe(localize_frame(outcomes), width="stretch", hide_index=True)

st.subheader("장중 리서치 기능 상태")
st.dataframe(localize_frame(intraday_capability), width="stretch", hide_index=True)

intraday_left, intraday_right = st.columns(2)
with intraday_left:
    st.subheader("장중 동일 종료 비교")
    st.dataframe(localize_frame(intraday_strategy_comparison), width="stretch", hide_index=True)
    st.subheader("장중 국면 매트릭스")
    st.dataframe(localize_frame(intraday_regime_matrix), width="stretch", hide_index=True)
with intraday_right:
    st.subheader("장중 타이밍 보정")
    st.dataframe(localize_frame(intraday_timing_calibration), width="stretch", hide_index=True)
    st.subheader("정책 워크포워드")
    st.dataframe(localize_frame(policy_walkforward), width="stretch", hide_index=True)

meta_left, meta_right = st.columns(2)
with meta_left:
    st.subheader("정책 대비 메타 오버레이")
    st.dataframe(localize_frame(meta_overlay), width="stretch", hide_index=True)
    st.subheader("메타 오버레이 국면별 분해")
    st.dataframe(localize_frame(meta_regime_breakdown), width="stretch", hide_index=True)
with meta_right:
    st.subheader("메타 오버레이 체크포인트 분해")
    st.dataframe(localize_frame(meta_checkpoint_breakdown), width="stretch", hide_index=True)
    st.subheader("정책 제거 실험")
    st.dataframe(localize_frame(policy_ablation), width="stretch", hide_index=True)

if intraday_postmortem_preview:
    with st.expander("최신 장중 사후 분석 미리보기", expanded=False):
        st.code(intraday_postmortem_preview)

if postmortem_preview:
    with st.expander("최신 장후 사후 분석 미리보기", expanded=False):
        st.code(postmortem_preview)

render_page_footer(settings, page_name="사후 평가")
