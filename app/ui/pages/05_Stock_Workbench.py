# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
from app.ui.components import (
    render_page_footer,
    render_page_header,
    render_record_cards,
    render_screen_guide,
    render_story_stream,
    render_warning_banner,
)
from app.ui.helpers import (
    available_symbol_options,
    format_execution_mode_label,
    format_ui_date,
    latest_intraday_decision_lineage_frame,
    load_ui_page_context,
    stock_workbench_flow_frame,
    stock_workbench_intraday_decision_frame,
    stock_workbench_intraday_timing_frame,
    stock_workbench_intraday_tuned_frame,
    stock_workbench_live_recommendation_frame,
    stock_workbench_news_frame,
    stock_workbench_outcome_frame,
    stock_workbench_price_frame,
    stock_workbench_summary_frame,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="stock_workbench",
    page_title="종목 분석",
)
symbol_options = available_symbol_options(settings, limit=None)
symbol_lookup = {
    f"{symbol} | {company_name}" if company_name else symbol: symbol
    for symbol, company_name in symbol_options
}


def _display_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def _latest_axis_row(frame, *, date_column: str, time_column: str | None = None):
    if frame.empty:
        return None
    ordered = frame.copy()
    sort_columns = [column for column in (date_column, time_column) if column and column in ordered.columns]
    if sort_columns:
        ordered = ordered.sort_values(sort_columns, ascending=[False] * len(sort_columns))
    return ordered.iloc[0]


def _build_context_items(live_row, summary_row, intraday_row) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    if live_row is not None:
        items.append(
            {
                "eyebrow": "Live Recalc",
                "title": f"{_display_text(live_row.get('symbol'))} 즉석 추천",
                "body": (
                    f"D1 {_display_text(live_row.get('live_d1_selection_v2_grade'))} / "
                    f"D5 {_display_text(live_row.get('live_d5_selection_v2_grade'))} / "
                    f"예상 초과수익률 {_display_text(live_row.get('live_d5_expected_excess_return'))}"
                ),
                "meta": (
                    f"기준일 {format_ui_date(live_row.get('live_as_of_date'))} · "
                    f"기준가 {_display_text(live_row.get('live_reference_price'))}"
                ),
                "badge": _display_text(live_row.get("live_d5_selection_v2_grade"), "LIVE"),
                "tone": "positive",
            }
        )
    if summary_row is not None:
        items.append(
            {
                "eyebrow": "Batch Snapshot",
                "title": f"{_display_text(summary_row.get('company_name'))} 종가 기준 요약",
                "body": (
                    f"D1 {_display_text(summary_row.get('d1_selection_v2_grade'))} / "
                    f"D5 {_display_text(summary_row.get('d5_selection_v2_grade'))} / "
                    f"알파 기대 {_display_text(summary_row.get('d5_alpha_expected_excess_return'))}"
                ),
                "meta": f"기준일 {format_ui_date(summary_row.get('as_of_date'))} · 시장 {_display_text(summary_row.get('market'))}",
                "badge": _display_text(summary_row.get("d5_selection_v2_grade"), "BATCH"),
                "tone": "accent",
            }
        )
    if intraday_row is not None:
        session_date = intraday_row.get("session_date") or intraday_row.get("selection_date")
        items.append(
            {
                "eyebrow": "Intraday",
                "title": f"{format_ui_date(session_date)} {_display_text(intraday_row.get('checkpoint_time'))} 판단",
                "body": (
                    f"raw {_display_text(intraday_row.get('raw_action'))} / "
                    f"adjusted {_display_text(intraday_row.get('adjusted_action'))} / "
                    f"final {_display_text(intraday_row.get('final_action'))}"
                ),
                "meta": "오늘 장중 기준" if session_date == today_local(settings.app.timezone) else "최근 저장된 세션 기준",
                "badge": _display_text(intraday_row.get("final_action"), "FLOW"),
                "tone": "warning",
            }
        )
    return items


def _build_feature_items(summary: pd.DataFrame) -> list[dict[str, str]]:
    if summary.empty:
        return []
    row = summary.iloc[0]
    return [
        {
            "eyebrow": "Momentum",
            "title": "성과와 뉴스 흐름",
            "body": f"5일 {_display_text(row.get('ret_5d'))} / 20일 {_display_text(row.get('ret_20d'))} / 최근 뉴스 {_display_text(row.get('news_count_3d'))}",
            "meta": f"ADV20 {_display_text(row.get('adv_20'))}",
            "badge": "MOMENTUM",
            "tone": "neutral",
        },
        {
            "eyebrow": "Flow",
            "title": "수급 품질",
            "body": (
                f"외국인 5일 비율 {_display_text(row.get('foreign_net_value_ratio_5d'))} / "
                f"스마트머니 20일 {_display_text(row.get('smart_money_flow_ratio_20d'))}"
            ),
            "meta": f"flow coverage {_display_text(row.get('flow_coverage_flag'))}",
            "badge": "FLOW",
            "tone": "accent",
        },
        {
            "eyebrow": "Alpha",
            "title": "D5 알파 밴드",
            "body": (
                f"expected {_display_text(row.get('d5_alpha_expected_excess_return'))} / "
                f"lower {_display_text(row.get('d5_alpha_lower_band'))} / "
                f"upper {_display_text(row.get('d5_alpha_upper_band'))}"
            ),
            "meta": (
                f"uncertainty {_display_text(row.get('d5_alpha_uncertainty_score'))} · "
                f"disagreement {_display_text(row.get('d5_alpha_disagreement_score'))}"
            ),
            "badge": "ALPHA",
            "tone": "positive",
        },
    ]


def _build_market_items(price_history: pd.DataFrame, flow_history: pd.DataFrame, news_history: pd.DataFrame) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    if not price_history.empty:
        row = price_history.iloc[0]
        items.append(
            {
                "eyebrow": "Price",
                "title": f"{format_ui_date(row.get('trading_date'))} 종가 {_display_text(row.get('close'))}",
                "body": f"시가 {_display_text(row.get('open'))} / 고가 {_display_text(row.get('high'))} / 저가 {_display_text(row.get('low'))}",
                "meta": f"거래량 {_display_text(row.get('volume'))} · 거래대금 {_display_text(row.get('turnover_value'))}",
                "badge": "PRICE",
                "tone": "neutral",
            }
        )
    if not flow_history.empty:
        row = flow_history.iloc[0]
        items.append(
            {
                "eyebrow": "Flow",
                "title": f"{format_ui_date(row.get('trading_date'))} 수급 요약",
                "body": (
                    f"외국인 {_display_text(row.get('foreign_net_value'))} / "
                    f"기관 {_display_text(row.get('institution_net_value'))} / "
                    f"개인 {_display_text(row.get('individual_net_value'))}"
                ),
                "meta": "금액 기준 순매수/순매도",
                "badge": "FLOW",
                "tone": "accent",
            }
        )
    if not news_history.empty:
        row = news_history.iloc[0]
        items.append(
            {
                "eyebrow": _display_text(row.get("publisher"), "News"),
                "title": _display_text(row.get("title")),
                "body": _display_text(row.get("query_bucket")),
                "meta": f"{format_ui_date(row.get('signal_date'))} · {_display_text(row.get('published_at'))}",
                "badge": "NEWS",
                "tone": "neutral",
            }
        )
    return items


def _build_track_items(outcome_history: pd.DataFrame, intraday_tuned: pd.DataFrame, intraday_timing: pd.DataFrame) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    if not outcome_history.empty:
        row = outcome_history.iloc[0]
        items.append(
            {
                "eyebrow": "Outcome",
                "title": f"{format_ui_date(row.get('selection_date'))} 선택 결과",
                "body": (
                    f"horizon {_display_text(row.get('horizon'))} / "
                    f"realized {_display_text(row.get('realized_excess_return'))} / "
                    f"status {_display_text(row.get('outcome_status'))}"
                ),
                "meta": f"band {_display_text(row.get('band_status'))}",
                "badge": _display_text(row.get("outcome_status"), "OUTCOME"),
                "tone": "warning",
            }
        )
    if not intraday_tuned.empty:
        row = intraday_tuned.iloc[0]
        items.append(
            {
                "eyebrow": "Meta Overlay",
                "title": f"{format_ui_date(row.get('session_date'))} {_display_text(row.get('checkpoint_time'))}",
                "body": (
                    f"tuned {_display_text(row.get('tuned_action'))} / "
                    f"final {_display_text(row.get('final_action'))} / "
                    f"class {_display_text(row.get('predicted_class'))}"
                ),
                "meta": f"margin {_display_text(row.get('confidence_margin'))}",
                "badge": _display_text(row.get("final_action"), "META"),
                "tone": "accent",
            }
        )
    if not intraday_timing.empty:
        row = intraday_timing.iloc[0]
        items.append(
            {
                "eyebrow": "Timing",
                "title": f"{format_ui_date(row.get('session_date'))} 시점 비교",
                "body": (
                    f"checkpoint {_display_text(row.get('selected_checkpoint_time'))} / "
                    f"edge {_display_text(row.get('timing_edge_bps'))}"
                ),
                "meta": f"action {_display_text(row.get('selected_action'))} · status {_display_text(row.get('outcome_status'))}",
                "badge": "TIMING",
                "tone": "neutral",
            }
        )
    return items


render_page_header(
    settings,
    page_name="종목 분석",
    title="종목 분석",
    description="종목 하나를 깊게 파되, 큰 표 대신 현재 해석과 과거 흐름을 세로 브리프로 먼저 읽는 구조로 바꿨습니다.",
)
render_screen_guide(
    summary="종가 기준, 즉석 재계산, 장중 판단, 사후 결과를 한 종목 중심으로 이어 읽습니다.",
    bullets=[
        "현재 해석에서 가장 최신 기준점을 먼저 확인합니다.",
        "특징 브리프에서 점수와 수급, 알파 밴드를 읽습니다.",
        "시장·뉴스·사후 추적은 필요한 만큼만 펼쳐서 원본을 확인합니다.",
    ],
)
render_warning_banner(
    "INFO",
    "장중 판단과 메타 보정은 연구용 해석 정보이며, 실제 주문을 직접 수행하지 않습니다.",
)

if not symbol_lookup:
    st.info("조회 가능한 종목이 아직 없습니다.")
else:
    selected_option = st.selectbox(
        "종목 선택",
        options=list(symbol_lookup.keys()),
        index=0,
        help="종목코드 또는 종목명으로 빠르게 이동할 수 있습니다.",
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

    render_story_stream(
        title="현재 해석",
        summary="즉석 재계산, 마지막 종가 배치, 최근 장중 판단을 한 번에 이어 읽습니다.",
        items=_build_context_items(live_row, summary_row, intraday_row),
        empty_message="현재 해석에 필요한 최신 데이터가 아직 없습니다.",
    )
    render_story_stream(
        title="특징 브리프",
        summary="점수, 수급, 알파 기대 밴드를 큰 표 대신 핵심 문장으로 압축했습니다.",
        items=_build_feature_items(summary),
        empty_message="특징 브리프에 필요한 요약 데이터가 없습니다.",
    )
    render_story_stream(
        title="시장·뉴스 라운지",
        summary="가격, 수급, 뉴스 최신 상태를 한 줄씩 빠르게 읽습니다.",
        items=_build_market_items(price_history, flow_history, news_history),
        empty_message="가격·수급·뉴스 데이터가 아직 없습니다.",
    )
    render_story_stream(
        title="사후 추적",
        summary="결과, 메타 보정, 시점 비교를 최신 건부터 서술형으로 보여줍니다.",
        items=_build_track_items(outcome_history, intraday_tuned, intraday_timing),
        empty_message="사후 추적 데이터가 아직 없습니다.",
    )

    with st.expander("원본 상세 보기", expanded=False):
        render_record_cards(
            live_recommendation,
            title="즉석 추천 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "live_d5_selection_v2_grade"],
            detail_columns=[
                "live_as_of_date",
                "live_reference_price",
                "live_d1_selection_v2_value",
                "live_d1_selection_v2_grade",
                "live_d5_selection_v2_value",
                "live_d5_selection_v2_grade",
                "live_d5_expected_excess_return",
                "live_d5_target_price",
                "latest_portfolio_target_weight",
                "latest_portfolio_gate_status",
            ],
            limit=1,
            empty_message="즉석 추천 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            summary,
            title="종목 요약 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "market"],
            detail_columns=[
                "as_of_date",
                "d1_selection_v2_grade",
                "d5_selection_v2_grade",
                "d5_alpha_expected_excess_return",
                "ret_5d",
                "ret_20d",
                "news_count_3d",
            ],
            limit=1,
            empty_message="종목 요약 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            intraday_decisions,
            title="장중 판단 원본",
            primary_column="session_date",
            secondary_columns=["checkpoint_time", "horizon"],
            detail_columns=["raw_action", "adjusted_action", "market_regime_family", "adjusted_timing_score"],
            limit=6,
            empty_message="장중 판단 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            outcome_history,
            title="사후 결과 원본",
            primary_column="selection_date",
            secondary_columns=["ranking_version", "outcome_status"],
            detail_columns=["horizon", "realized_excess_return", "band_status"],
            limit=6,
            empty_message="사후 결과 원본이 없습니다.",
            show_table_expander=False,
        )

render_page_footer(settings, page_name="종목 분석")
