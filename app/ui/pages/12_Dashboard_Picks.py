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

from app.ui.components import render_record_cards, render_screen_guide, render_story_stream
from app.ui.dashboard_v2 import (
    dashboard_snapshot_note,
    display_text,
    load_dashboard_v2_context,
    read_dashboard_frame,
    render_dashboard_v2_empty,
    render_dashboard_v2_footer,
    render_dashboard_v2_header,
    recommendation_timeline_note,
)

settings, activity, manifest = load_dashboard_v2_context(PROJECT_ROOT)
leaderboard = read_dashboard_frame(settings, "leaderboard")
target_book = read_dashboard_frame(settings, "portfolio_target_book")
sector_outlook = read_dashboard_frame(settings, "sector_outlook")


def _parse_list(raw_value: object) -> list[str]:
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


render_dashboard_v2_header(
    title="추천 종목",
    description="내일 바로 볼 종목과 공식 편입안을 최신 read-store 기준으로만 보여줍니다.",
    settings=settings,
    activity=activity,
    manifest=manifest,
)
render_screen_guide(
    summary="추천 종목 화면은 리더보드 상단 후보, 공식 target book, 섹터 집중 포인트만 남겼습니다.",
    bullets=[
        "상단 후보에서 종목 이유와 리스크를 먼저 읽습니다.",
        "공식 편입안에서 실제 target book과 진입일을 확인합니다.",
        "섹터 포인트는 이번 스냅샷 기준 강한 흐름 묶음만 봅니다.",
    ],
)

if leaderboard.empty and target_book.empty:
    render_dashboard_v2_empty("추천 종목 read-store가 아직 준비되지 않았습니다.")
else:
    market = st.segmented_control(
        "시장 범위",
        options=["ALL", "KOSPI", "KOSDAQ"],
        default="ALL",
    )
    horizon = st.segmented_control(
        "추천 기간",
        options=[1, 5],
        default=5,
        format_func=lambda value: f"D+{value}",
    )

    filtered_board = leaderboard.copy()
    if not filtered_board.empty:
        filtered_board = filtered_board.loc[filtered_board["horizon"] == int(horizon)].copy()
        if market != "ALL":
            filtered_board = filtered_board.loc[
                filtered_board["market"].astype(str).str.upper() == market
            ].copy()

    filtered_target = target_book.copy()
    if not filtered_target.empty:
        filtered_target = filtered_target.loc[
            filtered_target["included_flag"].fillna(False)
        ].copy()
        if market != "ALL":
            filtered_target = filtered_target.loc[
                filtered_target["market"].astype(str).str.upper() == market
            ].copy()

    filtered_sector = sector_outlook.copy()
    if not filtered_sector.empty and "horizon" in filtered_sector.columns:
        filtered_sector = filtered_sector.loc[filtered_sector["horizon"] == int(horizon)].copy()

    leader_items: list[dict[str, str]] = []
    for row in filtered_board.head(5).to_dict(orient="records"):
        reasons = ", ".join(_parse_list(row.get("reasons"))[:3])
        risks = ", ".join(_parse_list(row.get("risks"))[:2])
        leader_items.append(
            {
                "eyebrow": f"{display_text(row.get('market'))} · rank {display_text(row.get('final_selection_rank_pct'))}",
                "title": f"{display_text(row.get('symbol'))} · {display_text(row.get('company_name'))}",
                "body": " / ".join(
                    part
                    for part in [
                        display_text(row.get("industry")),
                        f"예상 초과수익률 {display_text(row.get('expected_excess_return'))}",
                        f"근거 {reasons}" if reasons else "",
                        f"리스크 {risks}" if risks else "",
                    ]
                    if part
                ),
                "meta": (
                    f"진입일 {display_text(row.get('next_entry_trade_date'))} · "
                    f"점수 {display_text(row.get('final_selection_value'))} · "
                    f"모델 {display_text(row.get('model_spec_id'))}"
                ),
                "badge": display_text(row.get("grade")),
                "tone": _tone_from_grade(row.get("grade")),
            }
        )

    target_items: list[dict[str, str]] = []
    for row in filtered_target.head(5).to_dict(orient="records"):
        target_items.append(
            {
                "eyebrow": display_text(row.get("execution_mode")),
                "title": f"{display_text(row.get('symbol'))} · {display_text(row.get('company_name'))}",
                "body": (
                    f"{display_text(row.get('action_plan_label'))} / "
                    f"비중 {display_text(row.get('target_weight'))} / "
                    f"목표가 {display_text(row.get('target_price'))}"
                ),
                "meta": (
                    f"진입일 {display_text(row.get('entry_trade_date'))} · "
                    f"gate {display_text(row.get('gate_status'))}"
                ),
                "badge": display_text(row.get("market")),
                "tone": "accent",
            }
        )

    sector_items: list[dict[str, str]] = []
    for row in filtered_sector.head(4).to_dict(orient="records"):
        sector_items.append(
            {
                "eyebrow": "Sector",
                "title": display_text(row.get("outlook_label")),
                "body": (
                    f"표본 {display_text(row.get('symbol_count'))}개 / "
                    f"상위 {display_text(row.get('top10_count'))}개 / "
                    f"기대 {display_text(row.get('avg_expected_excess_return'))}"
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
        title="공식 편입안",
        summary=dashboard_snapshot_note(manifest),
        items=target_items,
        empty_message="현재 스냅샷에 공식 편입안이 없습니다.",
    )
    render_story_stream(
        title="섹터 포인트",
        summary="이번 스냅샷에서 강한 섹터/업종 묶음만 추렸습니다.",
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
        render_record_cards(
            filtered_target,
            title="타겟북 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "action_plan_label"],
            detail_columns=["target_weight", "target_price", "entry_trade_date", "gate_status"],
            limit=10,
            empty_message="타겟북 원본이 없습니다.",
            show_table_expander=False,
        )

render_dashboard_v2_footer(settings, manifest=manifest, page_name="추천 종목")
