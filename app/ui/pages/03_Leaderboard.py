# ruff: noqa: E402, E501

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.ui.components import (
    render_glossary_hint,
    render_page_footer,
    render_page_header,
    render_record_cards,
    render_story_stream,
)
from app.ui.helpers import (
    available_ranking_dates,
    available_ranking_versions,
    format_market_label,
    format_ranking_version_label,
    format_ui_date,
    format_ui_value,
    latest_evaluation_comparison_frame,
    latest_recommendation_timeline_text,
    latest_selection_validation_summary_frame,
    latest_validation_summary_frame,
    leaderboard_frame,
    leaderboard_grade_count_frame,
    load_ui_page_context,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="leaderboard",
    page_title="리더보드",
)


def _display_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def _loads_list(raw_value: object) -> list[str]:
    if raw_value in (None, "", "[]"):
        return []
    try:
        parsed = json.loads(str(raw_value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip()]


def _tone_from_grade(grade: object) -> str:
    value = str(grade).upper()
    if value.startswith("S") or value.startswith("A"):
        return "positive"
    if value.startswith("B"):
        return "accent"
    return "warning"


def _build_leader_items(board: pd.DataFrame) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in board.head(6).to_dict(orient="records"):
        reasons = ", ".join(_loads_list(row.get("reasons"))[:3])
        risks = ", ".join(_loads_list(row.get("risks"))[:2])
        body_parts = [
            _display_text(row.get("industry")),
            f"예상 초과수익률 {_display_text(row.get('expected_excess_return'))}",
        ]
        if reasons:
            body_parts.append(f"근거 {reasons}")
        if risks:
            body_parts.append(f"리스크 {risks}")
        items.append(
            {
                "eyebrow": f"{_display_text(row.get('market'))} · rank {_display_text(row.get('final_selection_rank_pct'))}",
                "title": f"{_display_text(row.get('symbol'))} · {_display_text(row.get('company_name'))}",
                "body": " / ".join(body_parts),
                "meta": (
                    f"진입일 {_display_text(row.get('next_entry_trade_date'))} · "
                    f"점수 {_display_text(row.get('final_selection_value'))} · "
                    f"모델 {_display_text(row.get('model_spec_id'))}"
                ),
                "badge": _display_text(row.get("grade")),
                "tone": _tone_from_grade(row.get("grade")),
            }
        )
    return items


def _build_grade_items(grade_counts: pd.DataFrame, *, selected_date, horizon: int) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in grade_counts.to_dict(orient="records"):
        items.append(
            {
                "eyebrow": f"{format_ui_date(selected_date)} · D+{horizon}",
                "title": f"{_display_text(row.get('grade'))} 등급",
                "body": f"현재 리더보드에 {_display_text(row.get('row_count'))}개 종목이 들어 있습니다.",
                "meta": "상단 등급일수록 selection 점수가 높습니다.",
                "badge": _display_text(row.get("grade")),
                "tone": _tone_from_grade(row.get("grade")),
            }
        )
    return items


def _build_validation_items(validation: pd.DataFrame, evaluation_comparison: pd.DataFrame) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in validation.head(4).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Validation",
                "title": _display_text(row.get("summary_name")),
                "body": f"{_display_text(row.get('window_type'))} / 값 {_display_text(row.get('summary_value'))}",
                "meta": f"horizon {_display_text(row.get('horizon'))}",
                "badge": "CHECK",
                "tone": "neutral",
            }
        )
    for row in evaluation_comparison.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Comparison",
                "title": f"{_display_text(row.get('metric_name'))} · D+{_display_text(row.get('horizon'))}",
                "body": (
                    f"selection {_display_text(row.get('selection_v2_avg_excess'))} / "
                    f"explanatory {_display_text(row.get('explanatory_avg_excess'))}"
                ),
                "meta": f"gap {_display_text(row.get('avg_excess_gap'))}",
                "badge": "MODEL",
                "tone": "accent",
            }
        )
    return items


ranking_versions = available_ranking_versions(settings)
evaluation_comparison = latest_evaluation_comparison_frame(settings)

render_page_header(
    settings,
    page_name="리더보드",
    title="리더보드",
    description="내일 후보군을 표 대신 서술형 스트림으로 훑어보고, 원본은 필요할 때만 펼쳐보는 모바일 우선 화면입니다.",
)
render_glossary_hint("Selection v2")
render_story_stream(
    title="리더보드 읽는 법",
    summary="추천 점수만 보는 대신 근거, 리스크, 등급 분포를 함께 읽도록 구성했습니다.",
    items=[
        {
            "eyebrow": "Guide",
            "title": "추천 기준 안내",
            "body": latest_recommendation_timeline_text(settings),
            "meta": "Selection v2와 validation 흐름 기준",
            "badge": "GUIDE",
            "tone": "accent",
        }
    ],
    empty_message="리더보드 가이드를 아직 만들지 못했습니다.",
)

if not ranking_versions:
    st.info("리더보드 데이터가 아직 없습니다.")
else:
    default_version_index = (
        ranking_versions.index(SELECTION_ENGINE_V2_VERSION)
        if SELECTION_ENGINE_V2_VERSION in ranking_versions
        else 0
    )
    selected_version = st.selectbox(
        "점수 버전",
        options=ranking_versions,
        index=default_version_index,
        format_func=format_ranking_version_label,
    )
    ranking_dates = available_ranking_dates(settings, ranking_version=selected_version)
    selected_date = st.selectbox("기준일", options=ranking_dates, index=0, format_func=format_ui_date)
    horizon = st.selectbox("기간", options=[1, 5], index=1, format_func=lambda value: f"D+{value}")
    market = st.selectbox(
        "시장",
        options=["ALL", "KOSPI", "KOSDAQ"],
        index=0,
        format_func=format_market_label,
    )
    limit = st.slider("후보 수", min_value=10, max_value=100, value=25, step=5)
    show_technical = st.toggle("불확실성·흐름 점수도 보기", value=False)

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
    filtered_validation = (
        validation.loc[validation["horizon"] == horizon].copy() if not validation.empty else validation
    )

    render_story_stream(
        title="상위 후보 스트림",
        summary="상단 종목의 점수, 기대수익, 리스크를 짧은 문장으로 바로 읽습니다.",
        items=_build_leader_items(board),
        empty_message="현재 조건에 맞는 상위 후보가 없습니다.",
    )
    render_story_stream(
        title="등급 분포",
        summary="오늘 리더보드가 어느 등급대에 몰려 있는지 빠르게 확인합니다.",
        items=_build_grade_items(grade_counts, selected_date=selected_date, horizon=horizon),
        empty_message="등급 분포 데이터가 없습니다.",
    )
    render_story_stream(
        title="검증 브리프",
        summary="선택 모델 검증값과 explanatory 비교를 표 대신 짧게 보여줍니다.",
        items=_build_validation_items(filtered_validation, evaluation_comparison),
        empty_message="검증 요약 데이터가 없습니다.",
    )

    with st.expander("원본 리더보드 보기", expanded=False):
        detail_columns = [
            "final_selection_value",
            "expected_excess_return",
            "final_selection_rank_pct",
            "lower_band",
            "upper_band",
        ]
        if show_technical:
            detail_columns.extend(["uncertainty_score", "disagreement_score", "fallback_reason"])
        render_record_cards(
            board,
            title="리더보드 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "grade"],
            detail_columns=detail_columns,
            limit=min(limit, 10),
            empty_message="리더보드 원본이 없습니다.",
            show_table_expander=False,
        )

    with st.expander("검증 원본 보기", expanded=False):
        render_record_cards(
            filtered_validation,
            title="검증 원본",
            primary_column="summary_name",
            secondary_columns=["window_type"],
            detail_columns=["summary_value", "horizon"],
            limit=8,
            empty_message="검증 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            evaluation_comparison,
            title="모델 비교 원본",
            primary_column="metric_name",
            secondary_columns=["horizon"],
            detail_columns=["selection_v2_avg_excess", "explanatory_avg_excess", "avg_excess_gap"],
            limit=8,
            empty_message="모델 비교 원본이 없습니다.",
            show_table_expander=False,
        )

render_page_footer(settings, page_name="리더보드")
