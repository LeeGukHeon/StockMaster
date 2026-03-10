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
    render_warning_banner,
)
from app.ui.helpers import (
    available_symbols,
    latest_intraday_decision_lineage_frame,
    load_ui_settings,
    localize_frame,
    stock_workbench_flow_frame,
    stock_workbench_intraday_decision_frame,
    stock_workbench_intraday_timing_frame,
    stock_workbench_intraday_tuned_frame,
    stock_workbench_news_frame,
    stock_workbench_outcome_frame,
    stock_workbench_price_frame,
    stock_workbench_summary_frame,
)


def _compact_frame(frame, preferred_columns):
    if frame.empty:
        return frame
    columns = [column for column in preferred_columns if column in frame.columns]
    return frame[columns] if columns else frame


settings = load_ui_settings(PROJECT_ROOT)
symbols = available_symbols(settings)

render_page_header(
    settings,
    page_name="종목 분석",
    title="종목 분석",
    description="추천 사유, 제외 사유, 장중 판단, 포트폴리오 연결, 사후 결과를 종목 단위로 추적합니다.",
)
render_warning_banner(
    "INFO",
    "장중 판단과 메타 오버레이는 연구용 / 비매매 출력입니다. 실제 주문은 자동 실행하지 않습니다.",
)

if not symbols:
    st.info("조회 가능한 종목이 아직 없습니다.")
else:
    selected_symbol = st.selectbox("종목코드", options=symbols, index=0)
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
            f"{selected_symbol} 종목의 요약 데이터가 아직 없습니다. 유니버스와 핵심 적재 상태를 먼저 확인하세요.",
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

    st.subheader("핵심 요약")
    st.dataframe(
        localize_frame(
            _compact_frame(
                summary,
                [
                    "as_of_date",
                    "symbol",
                    "company_name",
                    "grade",
                    "final_selection_value",
                    "expected_excess_return",
                    "portfolio_eligible_flag",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

    top_left, top_right = st.columns(2)
    with top_left:
        st.subheader("가격 / 밴드")
        st.dataframe(
            localize_frame(
                _compact_frame(
                    price_history,
                    [
                        "as_of_date",
                        "close",
                        "expected_excess_return",
                        "lower_band",
                        "upper_band",
                    ],
                )
            ),
            width="stretch",
            hide_index=True,
        )
    with top_right:
        st.subheader("수급")
        st.dataframe(
            localize_frame(
                _compact_frame(
                    flow_history,
                    [
                        "trading_date",
                        "foreign_net_buy_value",
                        "institution_net_buy_value",
                        "individual_net_buy_value",
                    ],
                )
            ),
            width="stretch",
            hide_index=True,
        )

    st.subheader("선정 / 제외 / 사후 기록")
    st.dataframe(
        localize_frame(
            _compact_frame(
                outcome_history,
                [
                    "selection_date",
                    "horizon",
                    "ranking_version",
                    "outcome_status",
                    "realized_excess_return",
                    "band_status",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

    intraday_left, intraday_right = st.columns(2)
    with intraday_left:
        st.subheader("장중 원 정책 / 조정 정책")
        st.dataframe(
            localize_frame(
                _compact_frame(
                    intraday_decisions,
                    [
                        "session_date",
                        "checkpoint_time",
                        "horizon",
                        "raw_action",
                        "adjusted_action",
                        "market_regime_family",
                        "adjusted_timing_score",
                    ],
                )
            ),
            width="stretch",
            hide_index=True,
        )
    with intraday_right:
        st.subheader("메타 오버레이 / 최종 액션")
        if intraday_tuned.empty:
            st.info("현재 기준 장중 메타 오버레이 이력이 없습니다.")
        else:
            st.dataframe(
                localize_frame(
                    _compact_frame(
                        intraday_tuned,
                        [
                            "session_date",
                            "checkpoint_time",
                            "horizon",
                            "tuned_action",
                            "final_action",
                            "predicted_class",
                            "confidence_margin",
                        ],
                    )
                ),
                width="stretch",
                hide_index=True,
            )

    st.subheader("장중 라인리지")
    st.dataframe(
        localize_frame(
            _compact_frame(
                lineage,
                [
                    "selection_date",
                    "checkpoint_time",
                    "symbol",
                    "company_name",
                    "horizon",
                    "raw_action",
                    "adjusted_action",
                    "final_action",
                    "portfolio_execution_mode",
                    "gate_status",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

    st.subheader("시점 대비 성과")
    st.dataframe(
        localize_frame(
            _compact_frame(
                intraday_timing,
                [
                    "session_date",
                    "horizon",
                    "selected_checkpoint_time",
                    "selected_action",
                    "timing_edge_bps",
                    "outcome_status",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

    st.subheader("관련 뉴스 / 리포트")
    st.dataframe(
        localize_frame(
            _compact_frame(
                news_history,
                [
                    "published_at",
                    "provider",
                    "title",
                    "news_category",
                    "linked_symbols",
                ],
            )
        ),
        width="stretch",
        hide_index=True,
    )

    with st.expander("상세 원본 표 보기", expanded=False):
        st.dataframe(localize_frame(summary), width="stretch", hide_index=True)
        st.dataframe(localize_frame(price_history), width="stretch", hide_index=True)
        st.dataframe(localize_frame(flow_history), width="stretch", hide_index=True)
        st.dataframe(localize_frame(outcome_history), width="stretch", hide_index=True)
        st.dataframe(localize_frame(intraday_decisions), width="stretch", hide_index=True)
        st.dataframe(localize_frame(intraday_tuned), width="stretch", hide_index=True)
        st.dataframe(localize_frame(lineage), width="stretch", hide_index=True)

render_page_footer(settings, page_name="종목 분석")
