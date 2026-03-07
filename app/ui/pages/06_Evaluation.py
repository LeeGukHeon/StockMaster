# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.helpers import (
    available_evaluation_dates,
    evaluation_outcomes_frame,
    format_ranking_version_label,
    latest_calibration_diagnostic_frame,
    latest_evaluation_comparison_frame,
    latest_evaluation_summary_frame,
    latest_postmortem_preview,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
evaluation_dates = available_evaluation_dates(settings)
latest_summary = latest_evaluation_summary_frame(settings, limit=30)
latest_comparison = latest_evaluation_comparison_frame(settings)
latest_calibration = latest_calibration_diagnostic_frame(settings, limit=30)
postmortem_preview = latest_postmortem_preview(settings)

st.title("사후 평가")
st.caption(
    "선정 시점 Snapshot과 실제 성과를 비교하는 화면입니다. "
    "표시되는 평가는 모두 Pre-Cost 기준입니다."
)

if not evaluation_dates:
    st.info("평가 성과가 아직 없습니다. TICKET-005 스크립트를 먼저 실행하세요.")
else:
    selected_date = st.selectbox("평가일", options=evaluation_dates, index=0)
    horizon = st.selectbox("기간", options=[1, 5], index=1, format_func=lambda value: f"D+{value}")
    ranking_version = st.selectbox(
        "순위 버전",
        options=[SELECTION_ENGINE_VERSION, EXPLANATORY_RANKING_VERSION],
        index=0,
        format_func=format_ranking_version_label,
    )
    limit = st.slider("표시 행수", min_value=10, max_value=100, value=25, step=5)

    outcomes = evaluation_outcomes_frame(
        settings,
        evaluation_date=selected_date,
        horizon=horizon,
        ranking_version=ranking_version,
        limit=limit,
    )

    left, right = st.columns(2)
    with left:
        st.subheader("최신 평가 요약")
        st.dataframe(localize_frame(latest_summary), width="stretch", hide_index=True)
        st.subheader("선정 엔진 대 설명형 순위")
        st.dataframe(localize_frame(latest_comparison), width="stretch", hide_index=True)
    with right:
        st.subheader("최신 보정 진단")
        st.dataframe(localize_frame(latest_calibration), width="stretch", hide_index=True)
        if postmortem_preview:
            with st.expander("최신 사후 분석 미리보기", expanded=False):
                st.code(postmortem_preview)

    st.subheader("평가 가능 성과 행")
    st.dataframe(localize_frame(outcomes), width="stretch", hide_index=True)
