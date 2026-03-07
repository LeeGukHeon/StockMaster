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
    stock_workbench_news_frame,
    stock_workbench_outcome_frame,
    stock_workbench_price_frame,
    stock_workbench_summary_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
symbols = available_symbols(settings)

st.title("종목 분석")
st.caption("하나의 종목을 기준으로 피처, 수급, 가격, 순위, 연결 뉴스까지 확인합니다.")

if not symbols:
    st.info("아직 조회 가능한 종목이 없습니다.")
else:
    selected_symbol = st.selectbox("종목코드", options=symbols, index=0)
    summary = stock_workbench_summary_frame(settings, symbol=selected_symbol)
    price_history = stock_workbench_price_frame(settings, symbol=selected_symbol, limit=30)
    flow_history = stock_workbench_flow_frame(settings, symbol=selected_symbol, limit=30)
    news_history = stock_workbench_news_frame(settings, symbol=selected_symbol, limit=10)
    outcome_history = stock_workbench_outcome_frame(settings, symbol=selected_symbol, limit=20)

    st.subheader("요약")
    st.dataframe(localize_frame(summary), width="stretch", hide_index=True)

    left, right = st.columns(2)
    with left:
        st.subheader("최근 OHLCV")
        st.dataframe(localize_frame(price_history), width="stretch", hide_index=True)
    with right:
        st.subheader("최근 투자자 수급")
        st.dataframe(localize_frame(flow_history), width="stretch", hide_index=True)

    st.subheader("고정된 선정 성과")
    st.dataframe(localize_frame(outcome_history), width="stretch", hide_index=True)

    st.subheader("연결된 뉴스 메타데이터")
    st.dataframe(localize_frame(news_history), width="stretch", hide_index=True)
