# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.helpers import (
    latest_portfolio_evaluation_frame,
    latest_portfolio_nav_frame,
    load_ui_settings,
    localize_frame,
)

settings = load_ui_settings(PROJECT_ROOT)
nav_frame = latest_portfolio_nav_frame(settings, limit=60)
evaluation_frame = latest_portfolio_evaluation_frame(settings, limit=80)

st.title("포트폴리오 평가")
st.caption(
    "NAV, 드로다운, 회전율, 평균 보유수, "
    "OPEN_ALL vs TIMING_ASSISTED vs 동일가중 기준선을 비교합니다."
)

top_left, top_right = st.columns(2)
with top_left:
    st.subheader("NAV 스냅샷")
    if nav_frame.empty:
        st.info("포트폴리오 NAV 스냅샷이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(nav_frame), width="stretch", hide_index=True)
with top_right:
    st.subheader("평가 요약")
    if evaluation_frame.empty:
        st.info("포트폴리오 평가 요약이 아직 없습니다.")
    else:
        st.dataframe(localize_frame(evaluation_frame), width="stretch", hide_index=True)
