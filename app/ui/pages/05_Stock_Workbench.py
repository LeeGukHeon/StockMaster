# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
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
    format_execution_mode_label,
    format_ui_date,
    format_ui_number,
    format_ui_time,
    format_ui_value,
    latest_intraday_decision_lineage_frame,
    load_ui_settings,
    stock_workbench_flow_frame,
    stock_workbench_live_recommendation_frame,
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


def _latest_axis_row(frame, *, date_column: str, time_column: str | None = None):
    if frame.empty:
        return None
    ordered = frame.copy()
    sort_columns = [column for column in (date_column, time_column) if column and column in ordered.columns]
    if sort_columns:
        ordered = ordered.sort_values(sort_columns, ascending=[False] * len(sort_columns))
    return ordered.iloc[0]

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
        "장중 판단과 메타 보정은 연구용 참고 정보라서 실제 주문 내역이 아니라는 점을 함께 보세요.",
    ],
)
render_warning_banner(
    "INFO",
    "장중 판단과 메타 보정은 연구용 비매매 출력입니다. 실제 주문은 연결되지 않습니다.",
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
    live_recommendation = stock_workbench_live_recommendation_frame(settings, symbol=selected_symbol)
    price_history = stock_workbench_price_frame(settings, symbol=selected_symbol, limit=30)
    flow_history = stock_workbench_flow_frame(settings, symbol=selected_symbol, limit=30)
    news_history = stock_workbench_news_frame(settings, symbol=selected_symbol, limit=10)
    outcome_history = stock_workbench_outcome_frame(settings, symbol=selected_symbol, limit=20)
    intraday_decisions = stock_workbench_intraday_decision_frame(settings, symbol=selected_symbol, limit=20)
    intraday_tuned = stock_workbench_intraday_tuned_frame(settings, symbol=selected_symbol, limit=20)
    intraday_timing = stock_workbench_intraday_timing_frame(settings, symbol=selected_symbol, limit=20)
    lineage = latest_intraday_decision_lineage_frame(settings, symbol=selected_symbol, limit=20)
    live_row = live_recommendation.iloc[0] if not live_recommendation.empty else None
    summary_row = summary.iloc[0] if not summary.empty else None
    intraday_row = _latest_axis_row(lineage, date_column="selection_date", time_column="checkpoint_time")
    if intraday_row is None:
        intraday_row = _latest_axis_row(
            intraday_decisions,
            date_column="session_date",
            time_column="checkpoint_time",
        )

    render_warning_banner(
        "INFO",
        "이 화면은 현재 다시 계산한 값, 마지막 장마감 배치값, 다음 거래일 계획, 저장된 장중 판단이 함께 있습니다. 각 카드 제목의 기준 축을 먼저 보고 읽으세요.",
    )

    axis_left, axis_right = st.columns(2)
    with axis_left:
        if live_row is not None:
            render_narrative_card(
                "즉석 계산 기준",
                (
                    f"{format_ui_date(live_row.get('live_as_of_date'))} 장마감 데이터로 현재 다시 계산한 값입니다. "
                    f"기준 가격은 {format_ui_date(live_row.get('live_reference_date'))} 종가 "
                    f"{format_ui_value('live_reference_price', live_row.get('live_reference_price'))}입니다."
                ),
            )
        else:
            render_narrative_card(
                "즉석 계산 기준",
                "현재 다시 계산한 즉석 추천 값이 아직 없습니다.",
            )
        if live_row is not None and live_row.get("latest_portfolio_as_of_date") is not None:
            render_narrative_card(
                "다음 거래일 계획 기준",
                (
                    f"{format_ui_date(live_row.get('latest_portfolio_as_of_date'))} 기준 목표북입니다. "
                    f"실행 모드는 {format_execution_mode_label(str(live_row.get('latest_portfolio_execution_mode') or '-'))}이고 "
                    f"진입 예정일은 {format_ui_date(live_row.get('latest_portfolio_entry_trade_date'))}입니다."
                ),
            )
        else:
            render_narrative_card(
                "다음 거래일 계획 기준",
                "아직 다음 거래일 목표북이 생성되지 않았습니다.",
            )
    with axis_right:
        if summary_row is not None:
            render_narrative_card(
                "마지막 장마감 기준",
                (
                    f"{format_ui_date(summary_row.get('as_of_date'))} 장마감 배치 결과입니다. "
                    "등급과 선정 점수는 현재 장중 실시간 값이 아니라 마지막 저장 배치값입니다."
                ),
            )
        else:
            render_narrative_card(
                "마지막 장마감 기준",
                "마지막 장마감 요약 데이터가 아직 없습니다.",
            )
        if intraday_row is not None:
            session_date = intraday_row.get("session_date") or intraday_row.get("selection_date")
            historical_suffix = (
                "오늘 장중 값이 아니라 마지막 저장 세션입니다."
                if session_date is not None and session_date != today_local(settings.app.timezone)
                else "오늘 장중 저장 세션 기준입니다."
            )
            render_narrative_card(
                "최근 장중 판단 기준",
                (
                    f"{format_ui_date(session_date)} {format_ui_time(intraday_row.get('checkpoint_time'))} 중간 확인 시각 기준입니다. "
                    f"{historical_suffix}"
                ),
            )
        else:
            render_narrative_card(
                "최근 장중 판단 기준",
                "저장된 장중 판단 기록이 아직 없습니다.",
            )

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
                f"선정 점수는 {format_ui_number(row.get('final_selection_value'))}, "
                f"포트폴리오 진입 가능 여부는 {row.get('portfolio_eligible_flag', '-')}입니다."
            ),
        )

    render_record_cards(
        live_recommendation,
        title="즉석 추천 계산 | 현재 다시 계산",
        primary_column="symbol",
        secondary_columns=["company_name", "live_d5_selection_v2_grade"],
        detail_columns=[
            "live_as_of_date",
            "live_reference_price",
            "live_d1_selection_v2_value",
            "live_d1_selection_v2_grade",
            "live_d1_eligible_flag",
            "live_d5_selection_v2_value",
            "live_d5_selection_v2_grade",
            "live_d5_eligible_flag",
            "live_d5_report_candidate_flag",
            "live_d5_expected_excess_return",
            "live_d5_target_price",
            "live_d5_upper_target_price",
            "live_d5_stop_price",
            "latest_portfolio_included_flag",
            "latest_portfolio_target_weight",
            "latest_portfolio_gate_status",
        ],
        limit=1,
        empty_message="즉석 추천 계산에 필요한 최신 기준 데이터가 아직 없습니다.",
        table_expander_label="즉석 추천 계산 원본 보기",
    )

    render_record_cards(
        summary,
        title="종목 핵심 요약 | 마지막 장마감 기준",
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
        title="가격 / 밴드 | 장마감 데이터 기준",
        primary_column="as_of_date",
        secondary_columns=["close"],
        detail_columns=["expected_excess_return", "lower_band", "upper_band"],
        limit=8,
        empty_message="가격/밴드 이력이 없습니다.",
        table_expander_label="가격 / 밴드 원본 표 보기",
    )

    render_record_cards(
        flow_history,
        title="수급 추이 | 일별 저장 데이터",
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
        title="선정 / 사후 기록 | 과거 배치 이력",
        primary_column="selection_date",
        secondary_columns=["ranking_version", "outcome_status"],
        detail_columns=["horizon", "realized_excess_return", "band_status"],
        limit=8,
        empty_message="선정/사후 기록이 없습니다.",
        table_expander_label="선정 / 사후 원본 표 보기",
    )

    render_record_cards(
        intraday_decisions,
        title="최근 장중 판단 | 저장된 장중 세션 기준",
        primary_column="session_date",
        secondary_columns=["checkpoint_time", "horizon"],
        detail_columns=["raw_action", "adjusted_action", "market_regime_family", "adjusted_timing_score"],
        limit=8,
        empty_message="장중 판단 기록이 없습니다.",
        table_expander_label="장중 판단 원본 표 보기",
    )

    render_record_cards(
        intraday_tuned,
        title="메타 보정 / 최종 판단 | 저장된 장중 세션 기준",
        primary_column="session_date",
        secondary_columns=["checkpoint_time", "horizon"],
        detail_columns=["tuned_action", "final_action", "predicted_class", "confidence_margin"],
        limit=8,
        empty_message="장중 메타 보정 기록이 없습니다.",
        table_expander_label="메타 보정 원본 표 보기",
    )

    render_record_cards(
        lineage,
        title="장중 판단 흐름 | 저장된 장중 세션 기준",
        primary_column="selection_date",
        secondary_columns=["checkpoint_time", "portfolio_execution_mode"],
        detail_columns=["raw_action", "adjusted_action", "final_action", "gate_status"],
        limit=8,
        empty_message="판단 흐름 기록이 없습니다.",
        table_expander_label="판단 흐름 원본 표 보기",
    )

    render_record_cards(
        intraday_timing,
        title="시점 대비 결과 | 과거 세션 평가",
        primary_column="session_date",
        secondary_columns=["horizon", "selected_checkpoint_time"],
        detail_columns=["selected_action", "timing_edge_bps", "outcome_status"],
        limit=8,
        empty_message="시점 비교 결과가 없습니다.",
        table_expander_label="시점 비교 원본 표 보기",
    )

    render_record_cards(
        news_history,
        title="관련 뉴스 / 리포트 | 저장된 게시 시각 기준",
        primary_column="title",
        secondary_columns=["provider", "published_at"],
        detail_columns=["news_category", "linked_symbols"],
        limit=8,
        empty_message="관련 뉴스가 없습니다.",
        table_expander_label="뉴스 원본 표 보기",
    )

render_page_footer(settings, page_name="종목 분석")
