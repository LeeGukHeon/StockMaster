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
    access_mode: str = "safe"


def _page_path(project_root: Path, relative_path: str) -> Path:
    return project_root / relative_path


def page_specs(project_root: Path) -> tuple[PageSpec, ...]:
    return (
        PageSpec(
            key="today",
            title="봇 전환 안내",
            icon=":material/smart_toy:",
            url_path="bot-guide",
            callable_name="render_today_page",
        ),
        PageSpec(
            key="docs",
            title="문서 / 도움말",
            icon=":material/menu_book:",
            url_path="docs-help",
            path=_page_path(project_root, "app/ui/pages/11_Docs_Help.py"),
        ),
    )


def safe_dashboard_page_keys(project_root: Path | None = None) -> frozenset[str]:
    root = project_root or Path(__file__).resolve().parents[2]
    return frozenset(spec.key for spec in page_specs(root))


def dashboard_page_groups(
    project_root: Path | None = None,
) -> tuple[tuple[PageSpec, ...], tuple[PageSpec, ...]]:
    root = project_root or Path(__file__).resolve().parents[2]
    specs = page_specs(root)
    return specs, ()


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
