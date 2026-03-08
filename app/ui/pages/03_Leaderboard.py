# ruff: noqa: E402, E501

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
from app.ui.components import render_glossary_hint, render_page_footer, render_page_header
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

render_page_header(
    settings,
    page_name="리더보드",
    title="리더보드",
    description="rank, grade, selection value, expected alpha, uncertainty, disagreement, implementation penalty, flow score, band, flags를 한 번에 보는 화면입니다.",
)
render_glossary_hint("Selection v2")

if not ranking_versions:
    st.info("리더보드 데이터가 아직 없습니다.")
else:
    default_version_index = (
        ranking_versions.index(SELECTION_ENGINE_V2_VERSION)
        if SELECTION_ENGINE_V2_VERSION in ranking_versions
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
    limit = st.slider("표시 건수", min_value=10, max_value=100, value=25, step=5)
    show_technical = st.toggle("기술 컬럼 보기", value=True)

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
            st.info("현재 조건에 맞는 순위 데이터가 없습니다.")
        else:
            columns = [
                "symbol",
                "company_name",
                "market",
                "grade",
                "final_selection_value",
                "final_selection_rank_pct",
                "expected_excess_return",
                "lower_band",
                "upper_band",
                "reasons",
                "risks",
            ]
            technical_columns = [
                "uncertainty_score",
                "disagreement_score",
                "implementation_penalty_score",
                "flow_score",
                "fallback_flag",
            ]
            if show_technical:
                columns.extend(technical_columns)
            display = board[[column for column in columns if column in board.columns]].copy()
            if "final_selection_rank_pct" in display.columns:
                display["final_selection_rank_pct"] = (
                    pd.to_numeric(display["final_selection_rank_pct"], errors="coerce") * 100.0
                ).round(1)
            st.dataframe(localize_frame(display), width="stretch", hide_index=True)
    with top_right:
        st.subheader("등급 분포")
        if grade_counts.empty:
            st.info("등급 분포가 없습니다.")
        else:
            st.dataframe(localize_frame(grade_counts), width="stretch", hide_index=True)

    st.subheader("최신 검증 요약")
    filtered = validation.loc[validation["horizon"] == horizon].copy() if not validation.empty else validation
    if filtered.empty:
        st.info("선택한 버전에 대한 검증 데이터가 없습니다.")
    else:
        st.dataframe(localize_frame(filtered), width="stretch", hide_index=True)

    st.subheader("Selection vs Explanatory 비교")
    if evaluation_comparison.empty:
        st.info("비교 평가 데이터가 없습니다.")
    else:
        st.dataframe(localize_frame(evaluation_comparison), width="stretch", hide_index=True)

render_page_footer(settings, page_name="리더보드")
