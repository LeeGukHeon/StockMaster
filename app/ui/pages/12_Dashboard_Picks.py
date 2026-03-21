# ruff: noqa: E402, E501

from __future__ import annotations
import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import render_record_cards, render_screen_guide, render_story_stream
from app.ui.dashboard_v2 import (
    DASHBOARD_DEFAULT_PICK_HORIZON,
    display_number,
    display_percent,
    display_text,
    display_token_list,
    filter_dashboard_leaderboard,
    load_dashboard_v2_context,
    read_dashboard_frame,
    render_dashboard_v2_empty,
    render_dashboard_v2_footer,
    render_dashboard_v2_header,
    recommendation_timeline_note,
)

settings, activity, manifest = load_dashboard_v2_context(PROJECT_ROOT)
leaderboard = read_dashboard_frame(settings, "leaderboard")
sector_outlook = read_dashboard_frame(settings, "sector_outlook")

def _tone_from_grade(grade: object) -> str:
    value = str(grade).upper()
    if value.startswith("S") or value.startswith("A"):
        return "positive"
    if value.startswith("B"):
        return "accent"
    return "warning"


render_dashboard_v2_header(
    title="추천 종목",
    description="내일 바로 볼 상위 후보와 섹터 흐름만 읽기 전용 스냅샷 기준으로 보여줍니다.",
    settings=settings,
    activity=activity,
    manifest=manifest,
)
render_screen_guide(
    summary="추천 종목 화면은 상단 후보와 섹터 포인트만 남긴 얇은 화면입니다.",
    bullets=[
        "상단 후보에서 종목 이유와 리스크를 먼저 읽습니다.",
        "섹터 포인트는 이번 스냅샷 기준 강한 묶음만 보여줍니다.",
    ],
)

if leaderboard.empty:
    render_dashboard_v2_empty("추천 종목 스냅샷이 아직 준비되지 않았습니다.")
else:
    market = st.segmented_control("시장 범위", options=["ALL", "KOSPI", "KOSDAQ"], default="ALL")
    horizon = st.segmented_control(
        "추천 기간",
        options=[1, 5],
        default=DASHBOARD_DEFAULT_PICK_HORIZON,
        format_func=lambda value: f"D+{value}",
    )

    filtered_board = filter_dashboard_leaderboard(leaderboard, horizon=int(horizon), market=market)

    filtered_sector = sector_outlook.copy()
    if not filtered_sector.empty and "horizon" in filtered_sector.columns:
        filtered_sector = filtered_sector.loc[filtered_sector["horizon"] == int(horizon)].copy()

    leader_items = []
    for row in filtered_board.head(5).to_dict(orient="records"):
        reasons = display_token_list(row.get("reasons"), fallback="", max_items=3)
        risks = display_token_list(row.get("risks"), fallback="", max_items=2)
        body_parts = [
            display_text(row.get("industry")),
            f"예상 초과수익률 {display_percent(row.get('expected_excess_return'), signed=True)}",
        ]
        if reasons:
            body_parts.append(f"핵심 근거 {reasons}")
        if risks:
            body_parts.append(f"유의할 리스크 {risks}")
        leader_items.append(
            {
                "eyebrow": f"{display_text(row.get('market'))} · 상위 {display_percent(row.get('final_selection_rank_pct'))}",
                "title": f"{display_text(row.get('symbol'))} · {display_text(row.get('company_name'))}",
                "body": " / ".join(body_parts),
                "meta": (
                    f"진입 예정일 {display_text(row.get('next_entry_trade_date'))} · "
                    f"선정 점수 {display_number(row.get('final_selection_value'))} · "
                    f"모델 {display_text(row.get('model_spec_id'))}"
                ),
                "badge": display_text(row.get("grade")),
                "tone": _tone_from_grade(row.get("grade")),
            }
        )

    sector_items = []
    for row in filtered_sector.head(4).to_dict(orient="records"):
        sector_items.append(
            {
                "eyebrow": "섹터 포인트",
                "title": display_text(row.get("outlook_label")),
                "body": (
                    f"표본 {display_number(row.get('symbol_count'))}개 / "
                    f"상위 {display_number(row.get('top10_count'))}개 / "
                    f"평균 기대수익 {display_percent(row.get('avg_expected_excess_return'), signed=True)}"
                ),
                "meta": display_text(row.get("sample_symbols")),
                "badge": display_text(row.get("broad_sector")),
                "tone": "neutral",
            }
        )

    render_story_stream(
        title="상단 후보",
        summary=recommendation_timeline_note(settings),
        items=leader_items,
        empty_message="현재 조건에 맞는 상단 후보가 없습니다.",
    )
    render_story_stream(
        title="섹터 포인트",
        summary="이번 스냅샷에서 강한 흐름이 모인 섹터와 업종만 추렸습니다.",
        items=sector_items,
        empty_message="섹터 포인트 데이터가 없습니다.",
    )

    with st.expander("원본 데이터 보기", expanded=False):
        render_record_cards(
            filtered_board,
            title="리더보드 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "grade"],
            detail_columns=["expected_excess_return", "final_selection_value", "model_spec_id"],
            limit=10,
            empty_message="리더보드 원본이 없습니다.",
            show_table_expander=False,
        )

render_dashboard_v2_footer(settings, manifest=manifest, page_name="추천 종목")
