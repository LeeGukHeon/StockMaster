from __future__ import annotations

from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from app.common.time import now_local
from app.settings import Settings
from app.ui.components import inject_app_styles, render_page_footer, render_story_stream, render_warning_banner
from app.ui.helpers import dashboard_activity_state, latest_recommendation_timeline_text, load_ui_base_settings
from app.ui.read_model import load_ui_read_model_frame, load_ui_read_model_manifest


def load_dashboard_v2_context(project_root: Path):
    settings = load_ui_base_settings(project_root)
    activity = dashboard_activity_state(settings)
    manifest = load_ui_read_model_manifest(settings)
    return settings, activity, manifest


def read_dashboard_frame(settings: Settings, dataset_name: str) -> pd.DataFrame:
    try:
        return load_ui_read_model_frame(settings, dataset_name)
    except Exception:
        return pd.DataFrame()


def read_dashboard_manifest(settings: Settings) -> dict[str, object]:
    try:
        payload = load_ui_read_model_manifest(settings)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def display_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def render_dashboard_v2_header(
    *,
    title: str,
    description: str,
    settings: Settings,
    activity,
    manifest: dict[str, object],
) -> None:
    inject_app_styles()
    as_of_date = display_text(manifest.get("as_of_date"))
    built_at = display_text(manifest.get("built_at"))
    scope = display_text(manifest.get("scope"), "core")
    writer_mode = "WRITE BUSY" if activity.writer_active else "READ READY"
    writer_tone = "warning" if activity.writer_active else "positive"
    st.markdown(
        (
            '<section class="sm-hero">'
            '<div class="sm-hero-copy">'
            '<div class="sm-hero-kicker">Dashboard v2</div>'
            f'<h1 class="sm-hero-title">{escape(title)}</h1>'
            f'<p class="sm-hero-body">{escape(description)}</p>'
            '<div class="sm-hero-badges">'
            f'<span class="sm-mini-pill sm-tone-{writer_tone}">{escape(writer_mode)}</span>'
            f'<span class="sm-mini-pill sm-tone-accent">READ STORE {escape(scope.upper())}</span>'
            '</div>'
            '</div>'
            '<div class="sm-hero-stats">'
            f'<div class="sm-hero-stat"><span>기준일</span><strong>{escape(as_of_date)}</strong></div>'
            f'<div class="sm-hero-stat"><span>스냅샷 시각</span><strong>{escape(built_at)}</strong></div>'
            f'<div class="sm-hero-stat"><span>시간대</span><strong>{escape(settings.app.timezone)}</strong></div>'
            '</div>'
            '</section>'
        ),
        unsafe_allow_html=True,
    )
    if activity.writer_active:
        render_warning_banner(
            "INFO",
            "지금은 배치/학습이 돌고 있어도 대시보드 v2는 read-store만 읽습니다. 숫자는 최신 스냅샷 기준입니다.",
        )


def render_dashboard_v2_empty(message: str) -> None:
    render_story_stream(
        title="데이터 준비 상태",
        summary="대시보드 v2는 read-store 스냅샷만 읽습니다.",
        items=[],
        empty_message=message,
    )


def dashboard_snapshot_note(manifest: dict[str, object]) -> str:
    built_at = display_text(manifest.get("built_at"))
    as_of_date = display_text(manifest.get("as_of_date"))
    return f"스냅샷 시각 {built_at} / 기준일 {as_of_date}"


def recommendation_timeline_note(settings: Settings) -> str:
    return latest_recommendation_timeline_text(settings)


def render_dashboard_v2_footer(settings: Settings, *, manifest: dict[str, object], page_name: str) -> None:
    built_at = display_text(manifest.get("built_at"))
    render_page_footer(
        settings,
        page_name=page_name,
        extra_items=[f"snapshot: {built_at}", f"refreshed_at: {now_local(settings.app.timezone).isoformat()}"],
    )
