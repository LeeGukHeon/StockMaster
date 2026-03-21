from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True, slots=True)
class PageSpec:
    key: str
    title: str
    icon: str
    url_path: str
    path: Path | None = None
    callable_name: str | None = None
    access_mode: str = "restricted"


def _page_path(project_root: Path, relative_path: str) -> Path:
    return project_root / relative_path


def page_specs(project_root: Path) -> tuple[PageSpec, ...]:
    return (
        PageSpec(
            key="today",
            title="대시보드",
            icon=":material/home:",
            url_path="dashboard",
            callable_name="render_today_page",
            access_mode="safe",
        ),
        PageSpec(
            key="picks",
            title="추천 종목",
            icon=":material/leaderboard:",
            url_path="picks",
            path=_page_path(project_root, "app/ui/pages/12_Dashboard_Picks.py"),
            access_mode="safe",
        ),
        PageSpec(
            key="outlook",
            title="즉석 종목 전망",
            icon=":material/query_stats:",
            url_path="outlook",
            path=_page_path(project_root, "app/ui/pages/13_Dashboard_Outlook.py"),
            access_mode="safe",
        ),
        PageSpec(
            key="weekly_report",
            title="주간 보고",
            icon=":material/fact_check:",
            url_path="weekly-report",
            path=_page_path(project_root, "app/ui/pages/14_Dashboard_Weekly_Report.py"),
            access_mode="safe",
        ),
        PageSpec(
            key="ops_console",
            title="운영 콘솔",
            icon=":material/settings:",
            url_path="ops-console",
            path=_page_path(project_root, "app/ui/pages/01_Ops.py"),
        ),
        PageSpec(
            key="research_console",
            title="리서치 콘솔",
            icon=":material/science:",
            url_path="research-console",
            path=_page_path(project_root, "app/ui/pages/07_Intraday_Console.py"),
        ),
        PageSpec(
            key="docs",
            title="문서 / 도움말",
            icon=":material/menu_book:",
            url_path="docs-help",
            path=_page_path(project_root, "app/ui/pages/11_Docs_Help.py"),
            access_mode="safe",
        ),
    )


def safe_dashboard_page_keys(project_root: Path | None = None) -> frozenset[str]:
    root = project_root or Path(__file__).resolve().parents[2]
    return frozenset(spec.key for spec in page_specs(root) if spec.access_mode == "safe")


def dashboard_page_groups(
    project_root: Path | None = None,
) -> tuple[tuple[PageSpec, ...], tuple[PageSpec, ...]]:
    root = project_root or Path(__file__).resolve().parents[2]
    specs = page_specs(root)
    safe_specs = tuple(spec for spec in specs if spec.access_mode == "safe")
    restricted_specs = tuple(spec for spec in specs if spec.access_mode != "safe")
    return safe_specs, restricted_specs


def build_navigation_registry(
    project_root: Path,
    *,
    render_today_page: Callable[[], None],
    allowed_page_keys: set[str] | None = None,
):
    import streamlit as st

    registry: dict[str, object] = {}
    for spec in page_specs(project_root):
        if allowed_page_keys is not None and spec.key not in allowed_page_keys:
            continue
        if spec.callable_name is not None:
            registry[spec.key] = st.Page(
                render_today_page,
                title=spec.title,
                icon=spec.icon,
                url_path=spec.url_path,
            )
            continue

        assert spec.path is not None
        registry[spec.key] = st.Page(
            spec.path,
            title=spec.title,
            icon=spec.icon,
            url_path=spec.url_path,
        )
    return registry


def build_navigation_pages(
    project_root: Path,
    *,
    render_today_page: Callable[[], None],
    allowed_page_keys: set[str] | None = None,
):
    return list(
        build_navigation_registry(
            project_root,
            render_today_page=render_today_page,
            allowed_page_keys=allowed_page_keys,
        ).values()
    )


PAGE_SPECS = page_specs(Path(__file__).resolve().parents[2])
