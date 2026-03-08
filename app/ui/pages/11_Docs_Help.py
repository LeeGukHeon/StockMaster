# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    read_markdown,
    render_page_footer,
    render_page_header,
    render_release_candidate_summary,
    render_report_center,
)
from app.ui.helpers import latest_release_candidate_preview, load_ui_settings

settings = load_ui_settings(PROJECT_ROOT)

render_page_header(
    settings,
    page_name="문서 / 도움말",
    title="문서 / 도움말",
    description="사용자 가이드, 일일 사용 흐름, 용어집, 알려진 한계, 최신 리포트 목록, 릴리스 점검 상태를 한 곳에서 보는 화면입니다.",
)

tabs = st.tabs(
    [
        "사용자 가이드",
        "일일 사용 흐름",
        "용어집",
        "알려진 한계",
        "리포트",
        "릴리스 점검",
    ]
)

with tabs[0]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/USER_GUIDE.md"))
with tabs[1]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/WORKFLOW_DAILY.md"))
with tabs[2]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/GLOSSARY.md"))
with tabs[3]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/KNOWN_LIMITATIONS.md"))
with tabs[4]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/REPORTS_AND_PAGES.md"))
    st.subheader("최신 리포트 목록")
    render_report_center(settings, limit=20)
with tabs[5]:
    st.subheader("릴리스 점검 항목")
    render_release_candidate_summary(settings, limit=20)
    preview = latest_release_candidate_preview(settings)
    if preview:
        with st.expander("최신 릴리스 점검표 미리보기", expanded=False):
            st.code(preview)

render_page_footer(settings, page_name="문서 / 도움말")
