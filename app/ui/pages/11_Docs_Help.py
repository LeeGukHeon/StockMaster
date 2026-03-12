# ruff: noqa: E402

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
    render_report_preview,
    render_release_candidate_summary,
    render_report_center,
    render_screen_guide,
)
from app.ui.helpers import latest_release_candidate_preview, load_ui_settings

settings = load_ui_settings(PROJECT_ROOT)

render_page_header(
    settings,
    page_name="문서 / 도움말",
    title="문서 / 도움말",
    description=(
        "사용자 가이드, 일일 운영 절차, 용어집, 통합 런북, "
        "장중 리서치 모드 문서를 한곳에서 확인합니다."
    ),
)
render_screen_guide(
    summary="각 화면이 무엇을 보는 곳인지 헷갈릴 때 가장 먼저 보는 도움말 화면입니다. 사용법, 용어, 운영 문서를 한곳에서 찾도록 정리해 둔 페이지입니다.",
    bullets=[
        "투자 판단 흐름이 궁금하면 사용자 가이드와 화면 안내부터 보세요.",
        "서버 운영, 배포, 스케줄러, 복구는 통합 런북 탭부터 보면 됩니다.",
    ],
)

tabs = st.tabs(
    [
        "사용자 가이드",
        "일일 워크플로",
        "용어집",
        "한계와 주의",
        "리포트 / 화면 안내",
        "장중 리서치 모드",
        "KRX Live",
        "KRX 서비스",
        "통합 런북",
        "운영 참조",
        "감사 / DB 문서",
        "최신 리포트",
        "릴리스 체크",
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

with tabs[5]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/INTRADAY_RESEARCH_MODE.md"))

with tabs[6]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/KRX_LIVE_INTEGRATION.md"))

with tabs[7]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/KRX_SERVICE_REGISTRY.md"))

with tabs[8]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/RUNBOOK_SERVER_OPERATIONS.md"))

with tabs[9]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/SCHEDULER_AUTOMATION.md"))
    st.markdown("---")
    st.markdown(read_markdown(PROJECT_ROOT / "docs/METADATA_HOST_WORKER_VALIDATION.md"))
    with st.expander("구조 배경", expanded=False):
        st.markdown(
            read_markdown(
                PROJECT_ROOT / "docs/architecture/DUCKDB_ANALYTICS_AND_METADATA_STORE_SPLIT.md"
            )
        )

with tabs[10]:
    st.markdown(read_markdown(PROJECT_ROOT / "docs/AUDIT_T000_T013_STATUS.md"))
    st.markdown("---")
    st.markdown(read_markdown(PROJECT_ROOT / "docs/DB_CONTRACT_MATRIX.md"))
    with st.expander("개선 backlog", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/GAP_REMEDIATION_BACKLOG.md"))
    with st.expander("케이스 runbook", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/CASE_RUNBOOK_T000_T013.md"))

with tabs[11]:
    st.subheader("최신 리포트 목록")
    render_report_center(settings, limit=20)

with tabs[12]:
    st.subheader("릴리스 체크 항목")
    render_release_candidate_summary(settings, limit=20)
    preview = latest_release_candidate_preview(settings)
    if preview:
        with st.expander("최신 릴리스 체크리스트 미리보기", expanded=False):
            render_report_preview(
                title="릴리스 체크리스트 미리보기",
                preview=preview,
            )

render_page_footer(settings, page_name="문서 / 도움말")
