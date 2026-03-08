# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import render_narrative_card, render_page_footer, render_page_header
from app.ui.helpers import (
    available_symbols,
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

settings = load_ui_settings(PROJECT_ROOT)
symbols = available_symbols(settings)

render_page_header(
    settings,
    page_name="종목 분석",
    title="종목 분석",
    description="한 종목을 기준으로 why-in / why-not, 리포트 노출, timing 결정, 사후 결과를 세로로 따라가는 화면입니다.",
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

    if summary.empty:
        render_narrative_card(
            "Workbench Narrative",
            f"{selected_symbol}는 아직 summary row가 없습니다. universe sync나 ranking materialization 상태를 먼저 확인하세요.",
        )
    else:
        row = summary.iloc[0]
        render_narrative_card(
            "Workbench Narrative",
            f"{selected_symbol}는 현재 grade {row.get('grade', '-')}, "
            f"selection value {row.get('final_selection_value', '-')}, "
            f"portfolio eligibility {row.get('portfolio_eligible_flag', '-')} 기준으로 추적 중입니다.",
        )

    st.subheader("Header Summary")
    st.dataframe(localize_frame(summary), width="stretch", hide_index=True)

    top_left, top_right = st.columns(2)
    with top_left:
        st.subheader("최근 가격 / 밴드")
        st.dataframe(localize_frame(price_history), width="stretch", hide_index=True)
    with top_right:
        st.subheader("최근 수급")
        st.dataframe(localize_frame(flow_history), width="stretch", hide_index=True)

    st.subheader("Why-in / Why-not / Postmortem")
    st.dataframe(localize_frame(outcome_history), width="stretch", hide_index=True)

    intraday_left, intraday_right = st.columns(2)
    with intraday_left:
        st.subheader("장중 Raw / Adjusted Timeline")
        st.dataframe(localize_frame(intraday_decisions), width="stretch", hide_index=True)
    with intraday_right:
        st.subheader("장중 Tuned / Final Timeline")
        if intraday_tuned.empty:
            st.info("활성 정책 기준 tuned timeline이 없습니다.")
        else:
            st.dataframe(localize_frame(intraday_tuned), width="stretch", hide_index=True)

    st.subheader("Timing Edge vs Open")
    st.dataframe(localize_frame(intraday_timing), width="stretch", hide_index=True)

    st.subheader("관련 뉴스 / 리포트 맥락")
    st.dataframe(localize_frame(news_history), width="stretch", hide_index=True)

render_page_footer(settings, page_name="종목 분석")
