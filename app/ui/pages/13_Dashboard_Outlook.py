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
    dashboard_snapshot_note,
    display_bool,
    display_number,
    display_percent,
    display_text,
    display_value,
    load_dashboard_v2_context,
    read_dashboard_frame,
    render_dashboard_v2_empty,
    render_dashboard_v2_footer,
    render_dashboard_v2_header,
)

settings, activity, manifest = load_dashboard_v2_context(PROJECT_ROOT)
symbol_options = read_dashboard_frame(settings, "symbol_options")
summary_frame = read_dashboard_frame(settings, "stock_workbench_summary")
live_frame = read_dashboard_frame(settings, "stock_workbench_live_recommendation")
target_book = read_dashboard_frame(settings, "portfolio_target_book")


def _symbol_labels(frame) -> list[str]:
    labels: list[str] = []
    for row in frame.itertuples(index=False):
        symbol = display_text(getattr(row, "symbol", None))
        company_name = display_text(getattr(row, "company_name", None), "")
        labels.append(f"{symbol} | {company_name}".strip(" |"))
    return labels


render_dashboard_v2_header(
    title="즉석 종목 전망",
    description="특정 종목 하나를 눌렀을 때 최신 저장 스냅샷 기준 전망만 빠르게 읽는 화면입니다.",
    settings=settings,
    activity=activity,
    manifest=manifest,
)
render_screen_guide(
    summary="즉석 종목 전망은 실시간 본 DB 재계산이 아니라 최신 읽기 전용 스냅샷을 보여줍니다.",
    bullets=[
        "즉석 전망에서 D1/D5 기준점과 기대수익을 먼저 확인합니다.",
        "종가 기준 요약에서 최근 수익률과 알파 밴드를 읽습니다.",
        "포트폴리오 편입 여부와 target book 상태를 마지막에 확인합니다.",
    ],
)

if symbol_options.empty or summary_frame.empty:
    render_dashboard_v2_empty("종목 전망 스냅샷이 아직 준비되지 않았습니다.")
else:
    labels = _symbol_labels(symbol_options)
    selected_label = st.selectbox("종목 선택", options=labels, index=0)
    selected_symbol = selected_label.split("|", 1)[0].strip()

    summary = summary_frame.loc[summary_frame["symbol"].astype(str) == selected_symbol].copy()
    live = live_frame.loc[live_frame["symbol"].astype(str) == selected_symbol].copy()
    portfolio = target_book.loc[target_book["symbol"].astype(str) == selected_symbol].copy()
    summary_row = summary.iloc[0] if not summary.empty else None
    live_row = live.iloc[0] if not live.empty else None
    portfolio_row = portfolio.iloc[0] if not portfolio.empty else None

    outlook_items = []
    if live_row is not None:
        outlook_items.append(
            {
                "eyebrow": "즉석 스냅샷",
                "title": f"{display_text(live_row.get('symbol'))} · {display_text(live_row.get('company_name'))}",
                "body": (
                    f"D1 등급 {display_text(live_row.get('live_d1_selection_v2_grade'))} / "
                    f"D5 등급 {display_text(live_row.get('live_d5_selection_v2_grade'))} / "
                    f"예상 초과수익률 {display_percent(live_row.get('live_d5_expected_excess_return'), signed=True)}"
                ),
                "meta": (
                    f"기준일 {display_text(live_row.get('live_as_of_date'))} · "
                    f"기준가 {display_number(live_row.get('live_reference_price'))}"
                ),
                "badge": display_text(live_row.get("live_d5_selection_v2_grade"), "즉석"),
                "tone": "positive",
            }
        )
    if summary_row is not None:
        outlook_items.append(
            {
                "eyebrow": "종가 기준 요약",
                "title": "최근 흐름",
                "body": (
                    f"5일 수익률 {display_percent(summary_row.get('ret_5d'), signed=True)} / "
                    f"20일 수익률 {display_percent(summary_row.get('ret_20d'), signed=True)} / "
                    f"최근 3일 뉴스 {display_number(summary_row.get('news_count_3d'))}건"
                ),
                "meta": (
                    f"D5 알파 기대수익 {display_percent(summary_row.get('d5_alpha_expected_excess_return'), signed=True)} / "
                    f"불확실성 {display_number(summary_row.get('d5_alpha_uncertainty_score'))}"
                ),
                "badge": display_text(summary_row.get("d5_selection_v2_grade"), "요약"),
                "tone": "accent",
            }
        )
    if portfolio_row is not None:
        outlook_items.append(
            {
                "eyebrow": "포트폴리오 상태",
                "title": f"편입 여부 {display_bool(portfolio_row.get('included_flag'))}",
                "body": (
                    f"실행 방식 {display_value('execution_mode', portfolio_row.get('execution_mode'))} / "
                    f"목표 비중 {display_percent(portfolio_row.get('target_weight'))} / "
                    f"목표가 {display_number(portfolio_row.get('target_price'))}"
                ),
                "meta": (
                    f"진입 예정일 {display_text(portfolio_row.get('entry_trade_date'))} · "
                    f"게이트 {display_value('gate_status', portfolio_row.get('gate_status'))}"
                ),
                "badge": display_text(portfolio_row.get("market")),
                "tone": "neutral",
            }
        )

    render_story_stream(
        title="즉석 전망 브리프",
        summary=dashboard_snapshot_note(manifest),
        items=outlook_items,
        empty_message="이 종목의 최신 전망 스냅샷이 없습니다.",
    )

    with st.expander("원본 데이터 보기", expanded=False):
        render_record_cards(
            live,
            title="즉석 전망 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "live_d5_selection_v2_grade"],
            detail_columns=["live_d1_selection_v2_grade", "live_d5_expected_excess_return", "live_d5_target_price"],
            limit=1,
            empty_message="즉석 전망 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            summary,
            title="종가 요약 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "market"],
            detail_columns=["d1_selection_v2_grade", "d5_selection_v2_grade", "d5_alpha_expected_excess_return"],
            limit=1,
            empty_message="종가 요약 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            portfolio,
            title="포트폴리오 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "action_plan_label"],
            detail_columns=["target_weight", "target_price", "entry_trade_date", "gate_status"],
            limit=3,
            empty_message="포트폴리오 원본이 없습니다.",
            show_table_expander=False,
        )

render_dashboard_v2_footer(settings, manifest=manifest, page_name="즉석 종목 전망")
