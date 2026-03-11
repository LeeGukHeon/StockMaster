# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_record_cards,
    render_screen_guide,
    render_warning_banner,
)
from app.ui.helpers import (
    available_symbol_options,
    latest_intraday_decision_lineage_frame,
    load_ui_settings,
    stock_workbench_flow_frame,
    stock_workbench_intraday_decision_frame,
    stock_workbench_intraday_timing_frame,
    stock_workbench_intraday_tuned_frame,
    stock_workbench_news_frame,
    stock_workbench_outcome_frame,
    stock_workbench_price_frame,
    stock_workbench_summary_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
symbol_options = available_symbol_options(settings, limit=None)
symbol_lookup = {
    f"{symbol} | {company_name}" if company_name else symbol: symbol
    for symbol, company_name in symbol_options
}

render_page_header(
    settings,
    page_name="종목 분석",
    title="종목 분석",
    description="추천 이유, 제외 이유, 장중 판단, 사후 결과를 종목별로 한눈에 확인합니다.",
)
render_screen_guide(
    summary="한 종목만 깊게 보고 싶을 때 쓰는 화면입니다. 왜 추천됐는지, 왜 빠졌는지, 이후 결과가 어땠는지를 종목 단위로 확인합니다.",
    bullets=[
        "처음에는 종목 핵심 요약과 가격/밴드, 수급 추이만 봐도 충분합니다.",
        "장중 판단과 메타 오버레이는 연구용 참고 정보라서 실제 주문 내역이 아니라는 점을 함께 보세요.",
    ],
)
render_warning_banner(
    "INFO",
    "장중 판단과 메타 오버레이는 연구용 비매매 출력입니다. 실제 주문은 연결되지 않습니다.",
)

if not symbol_lookup:
    st.info("조회 가능한 종목이 아직 없습니다.")
else:
    selected_option = st.selectbox(
        "종목 검색",
        options=list(symbol_lookup.keys()),
        index=0,
        help="종목코드나 종목명으로 검색할 수 있습니다.",
    )
    selected_symbol = symbol_lookup[selected_option]
    summary = stock_workbench_summary_frame(settings, symbol=selected_symbol)
    price_history = stock_workbench_price_frame(settings, symbol=selected_symbol, limit=30)
    flow_history = stock_workbench_flow_frame(settings, symbol=selected_symbol, limit=30)
    news_history = stock_workbench_news_frame(settings, symbol=selected_symbol, limit=10)
    outcome_history = stock_workbench_outcome_frame(settings, symbol=selected_symbol, limit=20)
    intraday_decisions = stock_workbench_intraday_decision_frame(settings, symbol=selected_symbol, limit=20)
    intraday_tuned = stock_workbench_intraday_tuned_frame(settings, symbol=selected_symbol, limit=20)
    intraday_timing = stock_workbench_intraday_timing_frame(settings, symbol=selected_symbol, limit=20)
    lineage = latest_intraday_decision_lineage_frame(settings, symbol=selected_symbol, limit=20)

    if summary.empty:
        render_narrative_card(
            "종목 요약",
            f"{selected_symbol} 종목의 요약 데이터가 아직 없습니다. 유니버스와 적재 상태를 먼저 확인해 주세요.",
        )
    else:
        row = summary.iloc[0]
        render_narrative_card(
            "종목 요약",
            (
                f"{selected_symbol}의 현재 등급은 {row.get('grade', '-')}, "
                f"선정 점수는 {row.get('final_selection_value', '-')}, "
                f"포트폴리오 진입 가능 여부는 {row.get('portfolio_eligible_flag', '-')}입니다."
            ),
        )

    render_record_cards(
        summary,
        title="종목 핵심 요약",
        primary_column="symbol",
        secondary_columns=["company_name", "grade"],
        detail_columns=[
            "as_of_date",
            "final_selection_value",
            "expected_excess_return",
            "portfolio_eligible_flag",
        ],
        limit=1,
        empty_message="종목 요약이 없습니다.",
        table_expander_label="종목 요약 원본 표 보기",
    )

    render_record_cards(
        price_history,
        title="가격 / 밴드",
        primary_column="as_of_date",
        secondary_columns=["close"],
        detail_columns=["expected_excess_return", "lower_band", "upper_band"],
        limit=8,
        empty_message="가격/밴드 이력이 없습니다.",
        table_expander_label="가격 / 밴드 원본 표 보기",
    )

    render_record_cards(
        flow_history,
        title="수급 추이",
        primary_column="trading_date",
        detail_columns=[
            "foreign_net_buy_value",
            "institution_net_buy_value",
            "individual_net_buy_value",
        ],
        limit=8,
        empty_message="수급 이력이 없습니다.",
        table_expander_label="수급 원본 표 보기",
    )

    render_record_cards(
        outcome_history,
        title="선정 / 사후 기록",
        primary_column="selection_date",
        secondary_columns=["ranking_version", "outcome_status"],
        detail_columns=["horizon", "realized_excess_return", "band_status"],
        limit=8,
        empty_message="선정/사후 기록이 없습니다.",
        table_expander_label="선정 / 사후 원본 표 보기",
    )

    render_record_cards(
        intraday_decisions,
        title="장중 처음 판단 / 보정 후 판단",
        primary_column="session_date",
        secondary_columns=["checkpoint_time", "horizon"],
        detail_columns=["raw_action", "adjusted_action", "market_regime_family", "adjusted_timing_score"],
        limit=8,
        empty_message="장중 판단 기록이 없습니다.",
        table_expander_label="장중 판단 원본 표 보기",
    )

    render_record_cards(
        intraday_tuned,
        title="메타 보정 / 최종 판단",
        primary_column="session_date",
        secondary_columns=["checkpoint_time", "horizon"],
        detail_columns=["tuned_action", "final_action", "predicted_class", "confidence_margin"],
        limit=8,
        empty_message="장중 메타 보정 기록이 없습니다.",
        table_expander_label="메타 보정 원본 표 보기",
    )

    render_record_cards(
        lineage,
        title="장중 라인리지",
        primary_column="selection_date",
        secondary_columns=["checkpoint_time", "portfolio_execution_mode"],
        detail_columns=["raw_action", "adjusted_action", "final_action", "gate_status"],
        limit=8,
        empty_message="라인리지 기록이 없습니다.",
        table_expander_label="라인리지 원본 표 보기",
    )

    render_record_cards(
        intraday_timing,
        title="시점 대비 결과",
        primary_column="session_date",
        secondary_columns=["horizon", "selected_checkpoint_time"],
        detail_columns=["selected_action", "timing_edge_bps", "outcome_status"],
        limit=8,
        empty_message="시점 비교 결과가 없습니다.",
        table_expander_label="시점 비교 원본 표 보기",
    )

    render_record_cards(
        news_history,
        title="관련 뉴스 / 리포트",
        primary_column="title",
        secondary_columns=["provider", "published_at"],
        detail_columns=["news_category", "linked_symbols"],
        limit=8,
        empty_message="관련 뉴스가 없습니다.",
        table_expander_label="뉴스 원본 표 보기",
    )

render_page_footer(settings, page_name="종목 분석")
