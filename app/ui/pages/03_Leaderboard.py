# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.helpers import (
    available_ranking_dates,
    available_ranking_versions,
    format_market_label,
    format_ranking_version_label,
    latest_evaluation_comparison_frame,
    latest_selection_validation_summary_frame,
    latest_validation_summary_frame,
    leaderboard_frame,
    leaderboard_grade_count_frame,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
ranking_versions = available_ranking_versions(settings)
evaluation_comparison = latest_evaluation_comparison_frame(settings)

st.title("순위표")
st.caption(
    "설명형 순위 v0와 선정 엔진 v1을 비교합니다. 선정 엔진 v1의 프록시 밴드는 ML 예측이 아닙니다."
)

if not ranking_versions:
    st.info("순위 스냅샷이 아직 없습니다. 관련 빌드 스크립트를 먼저 실행하세요.")
else:
    default_version_index = (
        ranking_versions.index(SELECTION_ENGINE_VERSION)
        if SELECTION_ENGINE_VERSION in ranking_versions
        else 0
    )
    selected_version = st.selectbox(
        "순위 버전",
        options=ranking_versions,
        index=default_version_index,
        format_func=format_ranking_version_label,
    )
    ranking_dates = available_ranking_dates(settings, ranking_version=selected_version)
    selected_date = st.selectbox("기준일", options=ranking_dates, index=0)
    horizon = st.selectbox("기간", options=[1, 5], index=1, format_func=lambda value: f"D+{value}")
    market = st.selectbox(
        "시장",
        options=["ALL", "KOSPI", "KOSDAQ"],
        index=0,
        format_func=format_market_label,
    )
    limit = st.slider("표시 행수", min_value=10, max_value=100, value=25, step=5)

    board = leaderboard_frame(
        settings,
        as_of_date=selected_date,
        horizon=horizon,
        market=market,
        limit=limit,
        ranking_version=selected_version,
    )
    grade_counts = leaderboard_grade_count_frame(
        settings,
        as_of_date=selected_date,
        horizon=horizon,
        ranking_version=selected_version,
    )
    validation = (
        latest_selection_validation_summary_frame(settings, limit=50)
        if selected_version == SELECTION_ENGINE_VERSION
        else latest_validation_summary_frame(settings, limit=50)
    )

    top_left, top_right = st.columns((2, 1))
    with top_left:
        st.subheader("순위 테이블")
        if board.empty:
            st.info("현재 필터에 맞는 순위 행이 없습니다.")
        else:
            columns = [
                "symbol",
                "company_name",
                "market",
                "final_selection_value",
                "final_selection_rank_pct",
                "grade",
                "regime_state",
                "outcome_status",
                "realized_excess_return",
                "band_status",
                "reasons",
                "risks",
            ]
            if selected_version in {SELECTION_ENGINE_VERSION, SELECTION_ENGINE_V2_VERSION}:
                columns.extend(
                    [
                        "expected_excess_return",
                        "lower_band",
                        "upper_band",
                        "uncertainty_score",
                        "disagreement_score",
                        "fallback_flag",
                    ]
                )
            display = board[columns].copy()
            display["final_selection_rank_pct"] = (
                pd.to_numeric(display["final_selection_rank_pct"], errors="coerce") * 100.0
            ).round(1)
            st.dataframe(localize_frame(display), width="stretch", hide_index=True)
    with top_right:
        st.subheader("등급 분포")
        if grade_counts.empty:
            st.info("등급 분포가 아직 없습니다.")
        else:
            st.dataframe(localize_frame(grade_counts), width="stretch", hide_index=True)

    st.subheader("최신 검증 요약")
    if validation.empty:
        st.info("선택한 버전에 대한 검증 행이 없습니다.")
    else:
        filtered = validation.loc[validation["horizon"] == horizon].copy()
        st.dataframe(localize_frame(filtered), width="stretch", hide_index=True)

    st.subheader("선정 엔진 v1 대 설명형 순위 v0")
    if evaluation_comparison.empty:
        st.info("비교 평가 행이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(evaluation_comparison), width="stretch", hide_index=True)
