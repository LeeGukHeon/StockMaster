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
    stock_workbench_flow_frame,
    stock_workbench_news_frame,
    stock_workbench_price_frame,
    stock_workbench_summary_frame,
)

st.set_page_config(page_title="Stock Workbench", page_icon="SM", layout="wide")

settings = load_ui_settings(PROJECT_ROOT)
symbols = available_symbols(settings)

st.title("Stock Workbench")
st.caption(
    "Inspect one symbol across features, flow, recent prices, "
    "selection ranks, and linked news."
)

if not symbols:
    st.info("No symbols are available yet.")
else:
    selected_symbol = st.selectbox("Symbol", options=symbols, index=0)
    summary = stock_workbench_summary_frame(settings, symbol=selected_symbol)
    price_history = stock_workbench_price_frame(settings, symbol=selected_symbol, limit=30)
    flow_history = stock_workbench_flow_frame(settings, symbol=selected_symbol, limit=30)
    news_history = stock_workbench_news_frame(settings, symbol=selected_symbol, limit=10)

    st.subheader("Summary")
    st.dataframe(summary, use_container_width=True, hide_index=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Recent OHLCV")
        st.dataframe(price_history, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Recent Investor Flow")
        st.dataframe(flow_history, use_container_width=True, hide_index=True)

    st.subheader("Linked News Metadata")
    st.dataframe(news_history, use_container_width=True, hide_index=True)
