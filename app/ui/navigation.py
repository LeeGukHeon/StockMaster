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
            title="오늘",
            icon=":material/home:",
            url_path="today",
            callable_name="render_today_page",
            access_mode="safe",
        ),
        PageSpec(
            key="market_pulse",
            title="시장 현황",
            icon=":material/monitoring:",
            url_path="market-pulse",
            path=_page_path(project_root, "app/ui/pages/04_Market_Pulse.py"),
        ),
        PageSpec(
            key="leaderboard",
            title="리더보드",
            icon=":material/leaderboard:",
            url_path="leaderboard",
            path=_page_path(project_root, "app/ui/pages/03_Leaderboard.py"),
        ),
        PageSpec(
            key="portfolio",
            title="추천 구성안",
            icon=":material/account_balance:",
            url_path="portfolio",
            path=_page_path(project_root, "app/ui/pages/08_Portfolio_Studio.py"),
        ),
        PageSpec(
            key="portfolio_evaluation",
            title="추천안 평가",
            icon=":material/analytics:",
            url_path="portfolio-evaluation",
            path=_page_path(project_root, "app/ui/pages/09_Portfolio_Evaluation.py"),
        ),
        PageSpec(
            key="intraday_console",
            title="장중 콘솔",
            icon=":material/timeline:",
            url_path="intraday-console",
            path=_page_path(project_root, "app/ui/pages/07_Intraday_Console.py"),
        ),
        PageSpec(
            key="evaluation",
            title="사후 평가",
            icon=":material/fact_check:",
            url_path="evaluation",
            path=_page_path(project_root, "app/ui/pages/06_Evaluation.py"),
        ),
        PageSpec(
            key="stock_workbench",
            title="종목 분석",
            icon=":material/query_stats:",
            url_path="stock-workbench",
            path=_page_path(project_root, "app/ui/pages/05_Stock_Workbench.py"),
        ),
        PageSpec(
            key="research_lab",
            title="리서치 랩",
            icon=":material/science:",
            url_path="research-lab",
            path=_page_path(project_root, "app/ui/pages/02_Placeholder_Research.py"),
        ),
        PageSpec(
            key="ops",
            title="운영",
            icon=":material/settings:",
            url_path="ops",
            path=_page_path(project_root, "app/ui/pages/01_Ops.py"),
        ),
        PageSpec(
            key="health_dashboard",
            title="헬스 대시보드",
            icon=":material/health_metrics:",
            url_path="health-dashboard",
            path=_page_path(project_root, "app/ui/pages/10_Health_Dashboard.py"),
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
