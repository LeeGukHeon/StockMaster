# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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

st.title("종목 분석")
st.caption(
    "개별 종목 기준으로 가격, 수급, selection outcome, 장중 raw/adjusted/tuned timeline과 "
    "same-exit realized edge를 확인합니다."
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
    intraday_decisions = stock_workbench_intraday_decision_frame(
        settings,
        symbol=selected_symbol,
        limit=20,
    )
    intraday_tuned = stock_workbench_intraday_tuned_frame(
        settings,
        symbol=selected_symbol,
        limit=20,
    )
    intraday_timing = stock_workbench_intraday_timing_frame(
        settings,
        symbol=selected_symbol,
        limit=20,
    )

    st.subheader("요약")
    st.dataframe(localize_frame(summary), width="stretch", hide_index=True)

    top_left, top_right = st.columns(2)
    with top_left:
        st.subheader("최근 OHLCV")
        st.dataframe(localize_frame(price_history), width="stretch", hide_index=True)
    with top_right:
        st.subheader("최근 투자자 수급")
        st.dataframe(localize_frame(flow_history), width="stretch", hide_index=True)

    st.subheader("Selection outcome")
    st.dataframe(localize_frame(outcome_history), width="stretch", hide_index=True)

    intraday_left, intraday_right = st.columns(2)
    with intraday_left:
        st.subheader("장중 raw/adjusted timeline")
        st.dataframe(localize_frame(intraday_decisions), width="stretch", hide_index=True)
    with intraday_right:
        st.subheader("장중 tuned policy timeline")
        if intraday_tuned.empty:
            st.info("활성 정책 기준 tuned timeline이 없습니다.")
        else:
            st.dataframe(localize_frame(intraday_tuned), width="stretch", hide_index=True)

    st.subheader("장중 realized edge")
    st.dataframe(localize_frame(intraday_timing), width="stretch", hide_index=True)

    st.subheader("관련 뉴스 메타데이터")
    st.dataframe(localize_frame(news_history), width="stretch", hide_index=True)
