# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.components import (
    render_glossary_hint,
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_record_cards,
)
from app.ui.helpers import (
    available_ranking_dates,
    available_ranking_versions,
    format_market_label,
    format_ranking_version_label,
    latest_evaluation_comparison_frame,
    latest_recommendation_timeline_text,
    latest_selection_validation_summary_frame,
    latest_validation_summary_frame,
    leaderboard_frame,
    leaderboard_grade_count_frame,
    load_ui_settings,
)

settings = load_ui_settings(PROJECT_ROOT)
ranking_versions = available_ranking_versions(settings)
evaluation_comparison = latest_evaluation_comparison_frame(settings)

render_page_header(
    settings,
    page_name="리더보드",
    title="리더보드",
    description="오늘 바로 볼 추천 종목과 기대 알파, 위험 신호, 불확실성 정보를 빠르게 확인합니다.",
)
render_glossary_hint("Selection v2")
render_narrative_card("추천 기준 안내", latest_recommendation_timeline_text(settings))

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
    limit = st.slider("표시 개수", min_value=10, max_value=100, value=25, step=5)
    show_technical = st.toggle("세부 기술 지표 함께 보기", value=False)

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

    render_record_cards(
        board,
        title="오늘 추천 상위 종목",
        primary_column="symbol",
        secondary_columns=["company_name", "grade"],
        detail_columns=[
            "final_selection_value",
            "expected_excess_return",
            "final_selection_rank_pct",
            "lower_band",
            "upper_band",
            *(["uncertainty_score", "disagreement_score", "flow_score"] if show_technical else []),
        ],
        limit=min(limit, 8),
        empty_message="현재 조건에 맞는 순위 데이터가 없습니다.",
        table_expander_label="리더보드 원본 표 보기",
    )

    render_record_cards(
        grade_counts,
        title="등급 분포",
        primary_column="grade",
        detail_columns=["symbol_count"],
        limit=10,
        empty_message="등급 분포가 없습니다.",
        table_expander_label="등급 분포 원본 표 보기",
    )

    filtered = validation.loc[validation["horizon"] == horizon].copy() if not validation.empty else validation
    render_record_cards(
        filtered,
        title="최신 검증 요약",
        primary_column="summary_name",
        secondary_columns=["window_type"],
        detail_columns=["summary_value", "horizon"],
        limit=8,
        empty_message="선택한 버전에 대한 검증 데이터가 없습니다.",
        table_expander_label="검증 요약 원본 표 보기",
    )

    render_record_cards(
        evaluation_comparison,
        title="선정 엔진과 설명형 비교",
        primary_column="metric_name",
        secondary_columns=["horizon"],
        detail_columns=[
            "selection_v2_avg_excess",
            "explanatory_avg_excess",
        ],
        limit=8,
        empty_message="비교 평가 데이터가 없습니다.",
        table_expander_label="비교 평가 원본 표 보기",
    )

render_page_footer(settings, page_name="리더보드")
