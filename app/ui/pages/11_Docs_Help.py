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
from app.ui.helpers import latest_release_candidate_preview, load_ui_page_context

settings, activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="docs",
    page_title="문서 / 도움말",
)

render_page_header(
    settings,
    page_name="문서 / 도움말",
    title="문서 / 도움말",
    description="사용 가이드, 운영 문서, 점검 자료를 모바일에서 찾기 쉽게 섹션별로 모아둔 화면입니다.",
)
render_screen_guide(
    summary="긴 탭 바를 없애고 필요한 문서를 선택해서 보는 방식으로 바꿨습니다.",
    bullets=[
        "처음 쓰는 분은 시작하기를 먼저 보세요.",
        "배포, 스케줄, 복구는 운영 / 배포 섹션에서 찾으면 됩니다.",
        "최신 리포트와 릴리스 체크는 별도 섹션으로 바로 열 수 있습니다.",
    ],
)

section = st.selectbox(
    "문서 섹션",
    options=[
        "시작하기",
        "리포트와 화면",
        "장중 / KRX",
        "운영 / 배포",
        "감사 / DB",
        "최신 리포트",
        "릴리스 체크",
    ],
    index=0,
)

if section == "시작하기":
    st.markdown(read_markdown(PROJECT_ROOT / "docs/USER_GUIDE.md"))
    with st.expander("일일 흐름 보기", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/WORKFLOW_DAILY.md"))
    with st.expander("용어집 보기", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/GLOSSARY.md"))
    with st.expander("한계와 주의사항", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/KNOWN_LIMITATIONS.md"))

elif section == "리포트와 화면":
    st.markdown(read_markdown(PROJECT_ROOT / "docs/REPORTS_AND_PAGES.md"))

elif section == "장중 / KRX":
    st.markdown(read_markdown(PROJECT_ROOT / "docs/INTRADAY_RESEARCH_MODE.md"))
    st.markdown("---")
    st.markdown(read_markdown(PROJECT_ROOT / "docs/KRX_LIVE_INTEGRATION.md"))
    with st.expander("KRX 서비스 목록", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/KRX_SERVICE_REGISTRY.md"))

elif section == "운영 / 배포":
    st.markdown(read_markdown(PROJECT_ROOT / "docs/RUNBOOK_SERVER_OPERATIONS.md"))
    with st.expander("스케줄러 운영 문서", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/SCHEDULER_AUTOMATION.md"))
    with st.expander("메타데이터 검증 문서", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/METADATA_HOST_WORKER_VALIDATION.md"))
    with st.expander("구조 배경", expanded=False):
        st.markdown(
            read_markdown(
                PROJECT_ROOT / "docs/architecture/DUCKDB_ANALYTICS_AND_METADATA_STORE_SPLIT.md"
            )
        )

elif section == "감사 / DB":
    st.markdown(read_markdown(PROJECT_ROOT / "docs/AUDIT_T000_T013_STATUS.md"))
    st.markdown("---")
    st.markdown(read_markdown(PROJECT_ROOT / "docs/DB_CONTRACT_MATRIX.md"))
    with st.expander("개선 목록", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/GAP_REMEDIATION_BACKLOG.md"))
    with st.expander("케이스 운영 문서", expanded=False):
        st.markdown(read_markdown(PROJECT_ROOT / "docs/CASE_RUNBOOK_T000_T013.md"))

elif section == "최신 리포트":
    render_report_center(settings, limit=20)

else:
    render_release_candidate_summary(settings, limit=20)
    preview = latest_release_candidate_preview(settings)
    if preview:
        with st.expander("최신 릴리스 체크리스트 미리보기", expanded=False):
            render_report_preview(
                title="릴리스 체크리스트 미리보기",
                preview=preview,
            )

render_page_footer(settings, page_name="문서 / 도움말")
