from __future__ import annotations

from pathlib import Path

import pytest

from app.ui.helpers import DashboardActivityState, SAFE_DASHBOARD_PAGE_KEYS, load_ui_page_context
from app.ui.navigation import page_specs
from tests._ticket003_support import build_test_settings


def test_safe_dashboard_page_keys_match_navigation_specs() -> None:
    project_root = Path(__file__).resolve().parents[2]
    expected = frozenset(
        spec.key for spec in page_specs(project_root) if spec.access_mode == "safe"
    )
    assert SAFE_DASHBOARD_PAGE_KEYS == expected


def test_load_ui_page_context_allows_safe_page_during_active_job(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    activity = DashboardActivityState(
        writer_active=True,
        lock_names=("scheduler_global_write",),
        running_job_names=("run_weekly_calibration_bundle",),
        source="test",
    )

    monkeypatch.setattr("app.ui.helpers.load_ui_base_settings", lambda project_root: settings)
    monkeypatch.setattr("app.ui.helpers.dashboard_activity_state", lambda value: activity)
    monkeypatch.setattr(
        "app.ui.helpers.load_ui_settings",
        lambda project_root: (_ for _ in ()).throw(AssertionError("safe pages should not bootstrap DuckDB")),
    )

    resolved_settings, resolved_activity = load_ui_page_context(
        Path("."),
        page_key="today",
        page_title="오늘",
    )

    assert resolved_settings is settings
    assert resolved_activity == activity


def test_load_ui_page_context_blocks_restricted_page_during_active_job(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    activity = DashboardActivityState(
        writer_active=True,
        lock_names=("scheduler_global_write",),
        running_job_names=("run_weekly_calibration_bundle",),
        source="test",
    )
    warnings: list[str] = []

    class StopCalled(RuntimeError):
        pass

    monkeypatch.setattr("app.ui.helpers.load_ui_base_settings", lambda project_root: settings)
    monkeypatch.setattr("app.ui.helpers.dashboard_activity_state", lambda value: activity)
    monkeypatch.setattr("app.ui.helpers.st.warning", warnings.append)
    monkeypatch.setattr(
        "app.ui.helpers.st.stop",
        lambda: (_ for _ in ()).throw(StopCalled()),
    )

    with pytest.raises(StopCalled):
        load_ui_page_context(
            Path("."),
            page_key="stock_workbench",
            page_title="종목 분석",
        )

    assert warnings
    assert "active_lock=" in warnings[0]
