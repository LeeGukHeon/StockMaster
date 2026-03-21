from __future__ import annotations

from html import escape
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from app.common.time import now_local
from app.settings import Settings
from app.ui.components import inject_app_styles, render_page_footer, render_story_stream, render_warning_banner
from app.ui.helpers import (
    dashboard_activity_state,
    format_ui_value,
    format_ui_date,
    format_ui_datetime,
    format_ui_number,
    format_ui_percent,
    latest_recommendation_timeline_text,
    load_ui_base_settings,
    translate_ui_token,
)
from app.ui.read_model import load_ui_read_model_frame, load_ui_read_model_manifest

REGIME_LABELS = {
    "risk_on": "상승 우위 장세",
    "risk_off": "방어 우위 장세",
    "neutral": "중립 장세",
    "unknown": "판단 보류",
}

DASHBOARD_DEFAULT_PICK_HORIZON = 5
_DASHBOARD_EXCLUDED_TARGET_SYMBOLS = {"__CASH__"}
_EXECUTION_MODE_PRIORITY = {"OPEN_ALL": 0, "TIMING_ASSISTED": 1}


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
    translated = translate_ui_token(text)
    return translated if translated not in {"", "-"} else fallback


def display_value(column: str, value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(format_ui_value(column, value)).strip()
    return text if text and text not in {"nan", "NaN", "NaT", "None"} else fallback


def display_token_list(raw_value: object, *, fallback: str = "-", max_items: int | None = None) -> str:
    if raw_value in (None, "", "[]"):
        return fallback

    tokens: list[str] = []
    if isinstance(raw_value, list):
        tokens = [str(item).strip() for item in raw_value if str(item).strip()]
    else:
        text = str(raw_value).strip()
        if not text:
            return fallback
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            tokens = [str(item).strip() for item in parsed if str(item).strip()]
        else:
            tokens = [part.strip() for part in text.split(",") if part.strip()]

    if max_items is not None:
        tokens = tokens[:max_items]
    if not tokens:
        return fallback
    return ", ".join(display_text(token) for token in tokens)


def display_number(value: object, *, decimals: int = 2, fallback: str = "-") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return fallback
    return format_ui_number(value, decimals=decimals)


def display_percent(
    value: object,
    *,
    decimals: int = 1,
    signed: bool = False,
    percent_points: bool = False,
    fallback: str = "-",
) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return fallback
    return format_ui_percent(
        value,
        decimals=decimals,
        signed=signed,
        percent_points=percent_points,
        missing=fallback,
    )


def display_bool(value: object, *, true_label: str = "예", false_label: str = "아니오", fallback: str = "-") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return fallback
    return true_label if bool(value) else false_label


def display_scope(scope: object) -> str:
    value = display_text(scope, "core").lower()
    return {
        "core": "핵심 스냅샷",
        "all": "전체 스냅샷",
        "stock_intraday": "종목·장중 스냅샷",
    }.get(value, value)


def display_market_mood(headline: object) -> str:
    value = display_text(headline, "-").lower()
    return REGIME_LABELS.get(value, display_text(headline))


def filter_dashboard_leaderboard(
    frame: pd.DataFrame,
    *,
    horizon: int = DASHBOARD_DEFAULT_PICK_HORIZON,
    market: str = "ALL",
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    filtered = frame.copy()
    if "horizon" in filtered.columns:
        horizon_values = pd.to_numeric(filtered["horizon"], errors="coerce")
        filtered = filtered.loc[horizon_values == int(horizon)].copy()
    if market != "ALL" and "market" in filtered.columns:
        filtered = filtered.loc[filtered["market"].astype(str).str.upper() == market].copy()
    return filtered


def filter_dashboard_target_book(frame: pd.DataFrame, *, market: str = "ALL") -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    filtered = frame.copy()
    if "included_flag" in filtered.columns:
        filtered = filtered.loc[filtered["included_flag"].fillna(False)].copy()
    if market != "ALL" and "market" in filtered.columns:
        filtered = filtered.loc[filtered["market"].astype(str).str.upper() == market].copy()
    if "symbol" in filtered.columns:
        filtered = filtered.loc[
            ~filtered["symbol"].astype(str).isin(_DASHBOARD_EXCLUDED_TARGET_SYMBOLS)
        ].copy()
    if "target_weight" in filtered.columns:
        target_weights = pd.to_numeric(filtered["target_weight"], errors="coerce").fillna(0.0)
        filtered = filtered.loc[target_weights > 0].copy()
    if filtered.empty:
        return filtered
    filtered = filtered.assign(_original_order=range(len(filtered)))
    if "execution_mode" in filtered.columns:
        filtered = filtered.assign(
            _execution_priority=filtered["execution_mode"]
            .astype(str)
            .map(_EXECUTION_MODE_PRIORITY)
            .fillna(99)
        )
    else:
        filtered = filtered.assign(_execution_priority=99)
    if "symbol" in filtered.columns:
        filtered = (
            filtered.sort_values(["symbol", "_execution_priority", "_original_order"])
            .drop_duplicates(subset=["symbol"], keep="first")
            .sort_values("_original_order")
            .copy()
        )
    return filtered.drop(columns=["_execution_priority", "_original_order"], errors="ignore")


def render_dashboard_v2_header(
    *,
    title: str,
    description: str,
    settings: Settings,
    activity,
    manifest: dict[str, object],
) -> None:
    inject_app_styles()
    as_of_date = format_ui_date(manifest.get("as_of_date"))
    built_at = format_ui_datetime(manifest.get("built_at"))
    scope_label = display_scope(manifest.get("scope"))
    writer_label = "작업 중 읽기 전용" if activity.writer_active else "읽기 준비 완료"
    writer_tone = "warning" if activity.writer_active else "positive"
    st.markdown(
        (
            '<section class="sm-hero">'
            '<div class="sm-hero-copy">'
            '<div class="sm-hero-kicker">Dashboard v2</div>'
            f'<h1 class="sm-hero-title">{escape(title)}</h1>'
            f'<p class="sm-hero-body">{escape(description)}</p>'
            '<div class="sm-hero-badges">'
            f'<span class="sm-mini-pill sm-tone-{writer_tone}">{escape(writer_label)}</span>'
            f'<span class="sm-mini-pill sm-tone-accent">{escape(scope_label)}</span>'
            '</div>'
            '</div>'
            '<div class="sm-hero-stats">'
            f'<div class="sm-hero-stat"><span>기준일</span><strong>{escape(as_of_date)}</strong></div>'
            f'<div class="sm-hero-stat"><span>스냅샷 생성</span><strong>{escape(built_at)}</strong></div>'
            f'<div class="sm-hero-stat"><span>시간대</span><strong>{escape(settings.app.timezone)}</strong></div>'
            '</div>'
            '</section>'
        ),
        unsafe_allow_html=True,
    )
    if activity.writer_active:
        render_warning_banner(
            "INFO",
            "학습이나 배치가 돌고 있어도 Dashboard v2는 읽기 전용 스냅샷만 읽습니다. 화면 수치는 최신 스냅샷 기준입니다.",
        )


def render_dashboard_v2_empty(message: str) -> None:
    render_story_stream(
        title="데이터 준비 상태",
        summary="Dashboard v2는 읽기 전용 스냅샷만 사용합니다.",
        items=[],
        empty_message=message,
    )


def dashboard_snapshot_note(manifest: dict[str, object]) -> str:
    return f"기준일 {format_ui_date(manifest.get('as_of_date'))} · 스냅샷 생성 {format_ui_datetime(manifest.get('built_at'))}"


def recommendation_timeline_note(settings: Settings) -> str:
    return latest_recommendation_timeline_text(settings)


def render_dashboard_v2_footer(settings: Settings, *, manifest: dict[str, object], page_name: str) -> None:
    built_at = format_ui_datetime(manifest.get("built_at"))
    render_page_footer(
        settings,
        page_name=page_name,
        extra_items=[f"스냅샷 생성: {built_at}", f"현재 시각: {now_local(settings.app.timezone).isoformat()}"],
    )
