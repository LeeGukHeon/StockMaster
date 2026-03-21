# ruff: noqa: E501, I001

from __future__ import annotations

import json
from html import escape
from pathlib import Path
from typing import Iterable

import streamlit as st

from app.settings import Settings
from app.ui.glossary import glossary_mapping
from app.ui.helpers import (
    format_ui_date,
    format_ui_datetime,
    format_ui_value,
    latest_app_snapshot_frame,
    latest_release_candidate_check_frame,
    latest_report_index_frame,
    latest_ui_freshness_frame,
    localize_frame,
)


STATUS_BADGE_META: dict[str, tuple[str, str]] = {
    "SUCCESS": ("정상", "#0f766e"),
    "PARTIAL_SUCCESS": ("부분 성공", "#0f766e"),
    "DEGRADED_SUCCESS": ("저하", "#b45309"),
    "SKIPPED": ("건너뜀", "#475569"),
    "BLOCKED": ("차단", "#b91c1c"),
    "FAILED": ("실패", "#b91c1c"),
    "WARNING": ("경고", "#b45309"),
    "CRITICAL": ("치명", "#b91c1c"),
    "OK": ("정상", "#0f766e"),
    "INFO": ("안내", "#1d4ed8"),
}


def inject_app_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --sm-bg: #07131a;
            --sm-bg-soft: #0b1820;
            --sm-panel: rgba(14, 26, 34, 0.88);
            --sm-panel-strong: rgba(16, 30, 39, 0.96);
            --sm-panel-alt: rgba(10, 21, 29, 0.92);
            --sm-border: rgba(121, 147, 163, 0.18);
            --sm-rule: rgba(121, 147, 163, 0.10);
            --sm-ink: #e8f1f5;
            --sm-muted: #97aab6;
            --sm-accent: #39d0b0;
            --sm-accent-soft: rgba(57, 208, 176, 0.14);
            --sm-highlight: #ffb84d;
            --sm-danger: #ff6b6b;
            --sm-shadow: 0 18px 42px rgba(0, 0, 0, 0.24);
        }
        .stApp {
            background:
                radial-gradient(circle at 12% 8%, rgba(57, 208, 176, 0.18), transparent 24%),
                radial-gradient(circle at 88% 0%, rgba(255, 184, 77, 0.16), transparent 22%),
                radial-gradient(circle at 50% 100%, rgba(37, 99, 235, 0.12), transparent 28%),
                linear-gradient(180deg, #07131a 0%, #08161f 42%, #091118 100%);
            color: var(--sm-ink);
        }
        html, body, [class*="css"]  {
            font-family: "SUIT Variable", "Pretendard Variable", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
        }
        .block-container {
            max-width: 1120px;
            padding-top: 0.85rem;
            padding-bottom: 1.6rem;
        }
        section[data-testid="stSidebar"] {
            background:
                linear-gradient(180deg, rgba(6, 16, 22, 0.98), rgba(10, 20, 28, 0.98)) !important;
            border-right: 1px solid var(--sm-border);
        }
        div[data-testid="stForm"] {
            border: 1px solid var(--sm-border);
            border-radius: 18px;
            padding: 0.85rem 0.9rem 0.35rem 0.9rem;
            background: var(--sm-panel);
            box-shadow: var(--sm-shadow);
        }
        h1, h2, h3 {
            color: var(--sm-ink);
            letter-spacing: -0.03em;
        }
        p, label, .stCaption, [data-testid="stMarkdownContainer"] p {
            color: var(--sm-muted);
        }
        .sm-badge-row {display:flex; flex-wrap:wrap; gap:0.45rem; margin:0.35rem 0 1.05rem 0;}
        .sm-badge {
            display:inline-flex; align-items:center; gap:0.4rem;
            padding:0.34rem 0.78rem; border-radius:999px; color:white;
            font-size:0.79rem; font-weight:700; box-shadow:0 10px 24px rgba(0,0,0,0.24);
        }
        .sm-banner {
            border-radius:22px; padding:1rem 1.05rem; margin:0.55rem 0 1rem 0;
            border:1px solid var(--sm-border);
            border-left:5px solid transparent;
            background:linear-gradient(145deg, rgba(18, 32, 42, 0.96), rgba(10, 22, 30, 0.92));
            box-shadow:var(--sm-shadow);
        }
        .sm-footer {
            margin-top:1.6rem; padding-top:1rem; border-top:1px solid var(--sm-border);
            color:var(--sm-muted); font-size:0.85rem;
        }
        .sm-card {
            border:1px solid var(--sm-border);
            border-left:4px solid var(--sm-accent);
            border-radius:20px;
            padding:0.95rem 1rem;
            background:var(--sm-panel);
            box-shadow:var(--sm-shadow);
            margin-bottom:0.75rem;
        }
        .sm-card h4 {margin:0 0 0.2rem 0; font-size:0.92rem; color:var(--sm-ink);}
        .sm-card p {margin:0; color:var(--sm-muted); line-height:1.55; font-size:0.9rem;}
        .sm-guide {
            border:1px solid rgba(57, 208, 176, 0.18);
            border-radius:20px;
            padding:0.95rem 1rem;
            background:linear-gradient(180deg, rgba(10, 28, 33, 0.92), rgba(9, 20, 28, 0.92));
            box-shadow:var(--sm-shadow);
            margin:0.2rem 0 0.8rem 0;
        }
        .sm-guide h4 {margin:0 0 0.25rem 0; font-size:0.94rem; color:var(--sm-ink);}
        .sm-guide p {margin:0 0 0.35rem 0; color:var(--sm-muted); line-height:1.54; font-size:0.9rem;}
        .sm-guide ul {margin:0.24rem 0 0 1.1rem; color:var(--sm-muted);}
        .sm-guide li {margin:0.18rem 0;}
        .sm-report-preview {
            border:1px solid var(--sm-border);
            border-radius:22px;
            padding:1rem 1.05rem;
            background:linear-gradient(180deg, rgba(17, 31, 41, 0.96), rgba(9, 20, 28, 0.94));
            box-shadow:var(--sm-shadow);
        }
        .sm-report-preview p,
        .sm-report-preview li {
            line-height:1.7;
            color: var(--sm-muted);
        }
        .sm-sheet {
            display:flex;
            flex-direction:column;
            gap:0.78rem;
            margin:0.55rem 0 1rem 0;
        }
        .sm-sheet-row {
            border:1px solid var(--sm-border);
            border-radius:20px;
            padding:0.9rem 0.95rem;
            background:var(--sm-panel);
            box-shadow:var(--sm-shadow);
        }
        .sm-sheet-head {
            display:flex;
            align-items:flex-start;
            justify-content:space-between;
            gap:0.45rem;
        }
        .sm-sheet-kicker {
            margin:0 0 0.18rem 0;
            color:#88a3a9;
            font-size:0.72rem;
            letter-spacing:0.08em;
            font-weight:800;
            text-transform:uppercase;
        }
        .sm-sheet-title {
            margin:0;
            font-size:0.98rem;
            font-weight:800;
            color:var(--sm-ink);
            letter-spacing:-0.02em;
        }
        .sm-sheet-secondary {
            margin-top:0.16rem;
            color:var(--sm-muted);
            font-size:0.84rem;
            line-height:1.48;
        }
        .sm-sheet-grid {
            display:grid;
            grid-template-columns:repeat(auto-fit, minmax(150px, 1fr));
            gap:0.35rem 0.65rem;
            margin-top:0.5rem;
        }
        .sm-sheet-item {
            padding:0.34rem 0;
            border-top:1px solid var(--sm-rule);
        }
        .sm-sheet-item:first-child {
            border-top:none;
        }
        .sm-sheet-label {
            font-size:0.73rem;
            font-weight:700;
            letter-spacing:0.04em;
            text-transform:uppercase;
            color:#88a3a9;
        }
        .sm-sheet-value {
            margin-top:0.16rem;
            color:var(--sm-ink);
            font-size:0.9rem;
            font-weight:700;
            line-height:1.42;
            word-break:break-word;
        }
        div[data-testid="stMetric"] {
            background:linear-gradient(180deg, rgba(17, 31, 41, 0.98), rgba(10, 23, 31, 0.94));
            border:1px solid var(--sm-border);
            border-radius:18px;
            padding:0.78rem 0.92rem;
            box-shadow:var(--sm-shadow);
        }
        div[data-testid="stMetricLabel"] {
            color:#90a4af;
            font-weight:700;
        }
        div[data-testid="stMetricValue"] {
            color:var(--sm-ink);
            letter-spacing:-0.03em;
        }
        div[data-testid="stMetricDelta"] svg {
            fill: var(--sm-accent);
        }
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div,
        div[data-testid="stDateInput"] > div > div,
        div[data-testid="stMultiSelect"] > div,
        div[data-testid="stTextInput"] > div > div {
            background:var(--sm-panel);
            border-radius:16px;
            border:1px solid var(--sm-border);
            box-shadow:var(--sm-shadow);
        }
        div[data-testid="stDataFrame"] {
            border:1px solid var(--sm-border);
            border-radius:20px;
            overflow:hidden;
            background:var(--sm-panel);
            box-shadow:var(--sm-shadow);
        }
        div[data-testid="stExpander"] {
            border:1px solid var(--sm-border);
            border-radius:20px;
            background:var(--sm-panel-alt);
            box-shadow:var(--sm-shadow);
        }
        .stTabs [role="tablist"] {
            display:flex;
            flex-wrap:wrap;
            gap:0.38rem;
            padding:0.3rem;
            border-radius:999px;
            background:rgba(18, 33, 43, 0.88);
            border:1px solid var(--sm-border);
            width:100%;
        }
        .stTabs [role="tab"] {
            flex:1 1 180px;
            border-radius:999px;
            padding:0.4rem 0.95rem;
            height:auto;
            background:transparent;
            color:#8fa2ae;
            justify-content:center;
        }
        .stTabs [aria-selected="true"] {
            background:linear-gradient(135deg, rgba(57, 208, 176, 0.94), rgba(28, 153, 211, 0.88));
            color:#07131a;
        }
        div[data-baseweb="button-group"] {
            width:100%;
        }
        div[data-baseweb="button-group"] > div {
            display:flex;
            flex-wrap:wrap;
            gap:0.38rem;
            width:100%;
        }
        div[data-baseweb="button-group"] button,
        button[kind="secondary"] {
            flex:1 1 140px;
            border-radius:999px !important;
            border:1px solid var(--sm-border) !important;
            background:var(--sm-panel) !important;
            color:var(--sm-ink) !important;
        }
        button[kind="primary"] {
            border-radius:999px !important;
            border:1px solid transparent !important;
            background:linear-gradient(135deg, rgba(57, 208, 176, 0.96), rgba(28, 153, 211, 0.92)) !important;
            color:#07131a !important;
        }
        div[data-testid="stPageLink"] a {
            width: 100%;
            border-radius: 16px;
            border: 1px solid var(--sm-border);
            background: var(--sm-panel);
            color: var(--sm-ink);
            padding: 0.85rem 0.95rem;
            box-shadow: var(--sm-shadow);
        }
        .sm-mini-pill {
            display:inline-flex;
            align-items:center;
            gap:0.25rem;
            border-radius:999px;
            padding:0.32rem 0.72rem;
            border:1px solid var(--sm-border);
            font-size:0.72rem;
            font-weight:800;
            letter-spacing:0.06em;
            text-transform:uppercase;
            color:var(--sm-ink);
            background:rgba(18, 33, 43, 0.96);
        }
        .sm-tone-positive {
            background:rgba(25, 195, 125, 0.14);
            color:#8ff0c3;
            border-color:rgba(25, 195, 125, 0.26);
        }
        .sm-tone-warning,
        .sm-tone-degradedsuccess {
            background:rgba(255, 184, 77, 0.16);
            color:#ffd08a;
            border-color:rgba(255, 184, 77, 0.28);
        }
        .sm-tone-failed,
        .sm-tone-critical,
        .sm-tone-blocked {
            background:rgba(255, 107, 107, 0.16);
            color:#ff9a9a;
            border-color:rgba(255, 107, 107, 0.28);
        }
        .sm-tone-info,
        .sm-tone-accent,
        .sm-tone-neutral {
            background:rgba(57, 208, 176, 0.10);
            color:#7fe7d0;
            border-color:rgba(57, 208, 176, 0.22);
        }
        .sm-hero {
            display:grid;
            grid-template-columns:1fr;
            gap:0.9rem;
            padding:1.05rem 1rem;
            margin:0.15rem 0 1rem 0;
            border-radius:28px;
            background:
                linear-gradient(135deg, rgba(15, 30, 39, 0.98), rgba(8, 18, 25, 0.96)),
                radial-gradient(circle at top right, rgba(57, 208, 176, 0.14), transparent 35%);
            border:1px solid var(--sm-border);
            box-shadow:var(--sm-shadow);
            overflow:hidden;
            position:relative;
        }
        .sm-hero::after {
            content:"";
            position:absolute;
            inset:auto -8% -35% auto;
            width:220px;
            height:220px;
            background:radial-gradient(circle, rgba(255, 184, 77, 0.14), transparent 65%);
            pointer-events:none;
        }
        .sm-hero-copy, .sm-hero-stats {
            position:relative;
            z-index:1;
        }
        .sm-hero-kicker {
            color:#8ff0c3;
            font-size:0.72rem;
            font-weight:800;
            letter-spacing:0.16em;
            text-transform:uppercase;
            margin-bottom:0.45rem;
        }
        .sm-hero-title {
            margin:0;
            font-size:clamp(1.8rem, 5vw, 3.2rem) !important;
            line-height:0.98;
            letter-spacing:-0.06em;
        }
        .sm-hero-body {
            margin:0.6rem 0 0 0;
            max-width:42rem;
            color:var(--sm-muted);
            line-height:1.7;
            font-size:0.98rem;
        }
        .sm-hero-badges {
            display:flex;
            flex-wrap:wrap;
            gap:0.45rem;
            margin-top:0.85rem;
        }
        .sm-hero-stats {
            display:grid;
            grid-template-columns:repeat(3, minmax(0, 1fr));
            gap:0.55rem;
        }
        .sm-hero-stat {
            padding:0.85rem 0.9rem;
            border-radius:18px;
            background:rgba(11, 24, 32, 0.86);
            border:1px solid var(--sm-border);
        }
        .sm-hero-stat span {
            display:block;
            color:#8fa2ae;
            font-size:0.75rem;
            text-transform:uppercase;
            letter-spacing:0.08em;
            font-weight:800;
        }
        .sm-hero-stat strong {
            display:block;
            margin-top:0.35rem;
            color:var(--sm-ink);
            font-size:1.08rem;
            letter-spacing:-0.04em;
        }
        .sm-stream {
            margin:0.85rem 0 0.95rem 0;
            padding:0.95rem 0.95rem 0.3rem 0.95rem;
            border-radius:24px;
            background:var(--sm-panel);
            border:1px solid var(--sm-border);
            box-shadow:var(--sm-shadow);
        }
        .sm-stream-head {
            margin-bottom:0.75rem;
        }
        .sm-stream-head h3 {
            margin:0;
            font-size:1.05rem;
            letter-spacing:-0.03em;
        }
        .sm-stream-head p {
            margin:0.32rem 0 0 0;
            color:var(--sm-muted);
            line-height:1.65;
        }
        .sm-story-list {
            display:grid;
            grid-template-columns:1fr;
            gap:0.7rem;
        }
        .sm-story {
            padding:0.9rem 0 0.95rem 0;
            border-top:1px solid var(--sm-rule);
        }
        .sm-story:first-child {
            border-top:none;
            padding-top:0.1rem;
        }
        .sm-story-top {
            display:flex;
            align-items:center;
            flex-wrap:wrap;
            gap:0.45rem;
            margin-bottom:0.45rem;
        }
        .sm-story-eyebrow {
            color:#88a3a9;
            font-size:0.72rem;
            font-weight:800;
            letter-spacing:0.12em;
            text-transform:uppercase;
        }
        .sm-story h4 {
            margin:0;
            color:var(--sm-ink);
            font-size:1rem;
            letter-spacing:-0.03em;
        }
        .sm-story p {
            margin:0.34rem 0 0 0;
            color:var(--sm-muted);
            line-height:1.7;
        }
        .sm-story-meta {
            margin-top:0.5rem;
            color:#88a3a9;
            font-size:0.84rem;
        }
        .sm-stream-empty {
            color:var(--sm-muted);
            padding:0.25rem 0 0.75rem 0;
        }
        @media (min-width: 960px) {
            .sm-hero {
                grid-template-columns:minmax(0, 1.8fr) minmax(280px, 0.95fr);
                align-items:end;
                padding:1.15rem 1.15rem 1.05rem 1.15rem;
            }
            .sm-story-list {
                grid-template-columns:repeat(2, minmax(0, 1fr));
                gap:0.85rem 1rem;
            }
        }
        @media (max-width: 900px) {
            .block-container {
                padding-left: 0.7rem;
                padding-right: 0.7rem;
            }
            [data-testid="column"] {
                min-width: 100% !important;
                flex: 1 1 100% !important;
            }
            .sm-hero {
                border-radius:24px;
                padding:0.95rem 0.9rem;
            }
            .sm-hero-stats {
                grid-template-columns:1fr;
            }
            .sm-badge {
                font-size:0.8rem;
                padding:0.28rem 0.62rem;
            }
            .sm-card,
            .sm-guide,
            .sm-report-preview,
            .sm-sheet-row,
            div[data-testid="stMetric"],
            div[data-testid="stDataFrame"] {
                border-radius:15px;
            }
            h1 {
                font-size: 1.7rem !important;
            }
            .sm-stream {
                border-radius:20px;
                padding:0.9rem 0.85rem 0.25rem 0.85rem;
            }
            .sm-sheet-head {
                flex-direction:column;
            }
            .sm-sheet-grid {
                grid-template-columns:1fr;
            }
            [data-testid="stTabs"] button {
                padding-left: 0.5rem !important;
                padding-right: 0.5rem !important;
            }
            div[data-testid="stForm"] {
                padding-left:0.75rem;
                padding-right:0.75rem;
            }
            div[data-baseweb="button-group"] button,
            button[kind="secondary"],
            button[kind="primary"] {
                width:100%;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _status_badge_html(status: str, *, label: str | None = None) -> str:
    normalized = str(status).upper()
    default_label, color = STATUS_BADGE_META.get(normalized, (normalized, "#334155"))
    display = label or default_label
    return f'<span class="sm-badge" style="background:{color};">{escape(str(display))}</span>'


def render_status_badges(items: Iterable[tuple[str, str]]) -> None:
    badge_html = "".join(_status_badge_html(status, label=label) for label, status in items)
    if badge_html:
        st.markdown(f'<div class="sm-badge-row">{badge_html}</div>', unsafe_allow_html=True)


def render_warning_banner(level: str, message: str) -> None:
    normalized = str(level).upper()
    display_label, color = STATUS_BADGE_META.get(normalized, (normalized, "#475569"))
    st.markdown(
        f'<div class="sm-banner" style="border-left-color:{color};"><strong>{display_label}</strong><br>{message}</div>',
        unsafe_allow_html=True,
    )


def render_narrative_card(title: str, body: str) -> None:
    st.markdown(
        f'<div class="sm-card"><h4>{escape(title)}</h4><p>{escape(body)}</p></div>',
        unsafe_allow_html=True,
    )


def render_story_stream(
    *,
    title: str,
    summary: str,
    items: list[dict[str, str]],
    empty_message: str,
) -> None:
    def _story_badge(label: str, tone: str) -> str:
        normalized = "".join(ch for ch in str(tone).lower() if ch.isalpha() or ch == "-") or "neutral"
        return f'<span class="sm-mini-pill sm-tone-{escape(normalized)}">{escape(str(label))}</span>'

    if not items:
        st.markdown(
            (
                '<section class="sm-stream">'
                f'<div class="sm-stream-head"><h3>{escape(title)}</h3><p>{escape(summary)}</p></div>'
                f'<div class="sm-stream-empty">{escape(empty_message)}</div>'
                '</section>'
            ),
            unsafe_allow_html=True,
        )
        return

    story_html: list[str] = []
    for item in items:
        eyebrow = item.get("eyebrow", "")
        heading = item.get("title", "-")
        body = item.get("body", "-")
        meta = item.get("meta", "")
        badge = item.get("badge", "")
        tone = item.get("tone", "neutral")
        badge_html = _story_badge(badge, tone) if badge else ""
        story_html.append(
            '<article class="sm-story">'
            '<div class="sm-story-top">'
            f"{badge_html}"
            f'<span class="sm-story-eyebrow">{escape(str(eyebrow))}</span>'
            '</div>'
            f'<h4>{escape(str(heading))}</h4>'
            f'<p>{escape(str(body))}</p>'
            f'<div class="sm-story-meta">{escape(str(meta))}</div>'
            '</article>'
        )

    st.markdown(
        (
            '<section class="sm-stream">'
            f'<div class="sm-stream-head"><h3>{escape(title)}</h3><p>{escape(summary)}</p></div>'
            f'<div class="sm-story-list">{"".join(story_html)}</div>'
            '</section>'
        ),
        unsafe_allow_html=True,
    )


def render_screen_guide(
    *,
    summary: str,
    bullets: list[str] | None = None,
    title: str = "이 화면은 이렇게 보세요",
) -> None:
    with st.expander(title, expanded=False):
        st.caption(summary)
        if bullets:
            for item in bullets:
                st.markdown(f"- {item}")


def render_report_preview(
    *,
    title: str,
    preview: str | None,
    empty_message: str = "미리 볼 보고서가 아직 없습니다.",
) -> None:
    if not preview:
        st.info(empty_message)
        return
    st.subheader(title)
    with st.container(border=True):
        st.markdown(preview)


def _display_value(value: object) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text if text and text not in {"nan", "NaN", "NaT", "None"} else "-"


def _existing_columns(frame, columns: Iterable[str]) -> list[str]:
    return [column for column in columns if column in frame.columns]


def _deduplicate_preserving_order(columns: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for column in columns:
        if column in seen:
            continue
        seen.add(column)
        ordered.append(column)
    return ordered


def _localized_display(frame, columns: list[str] | None = None):
    display = frame.copy() if not columns else frame[columns].copy()
    localized = localize_frame(display)
    label_map = dict(zip(display.columns, localized.columns, strict=False))
    return localized, label_map


def _row_badge_html(row: dict[str, object], label_map: dict[str, str], columns: Iterable[str]) -> str:
    for column in columns:
        if column not in label_map:
            continue
        value = _display_value(row.get(label_map[column]))
        normalized = value.upper()
        if normalized not in STATUS_BADGE_META:
            continue
        if not any(token in column.lower() for token in ("status", "severity", "warning_level", "decision")):
            continue
        return _status_badge_html(normalized, label=value)
    return ""


def render_data_sheet(
    frame,
    *,
    title: str,
    primary_column: str | None = None,
    secondary_columns: list[str] | None = None,
    detail_columns: list[str] | None = None,
    limit: int = 5,
    empty_message: str = "표시할 데이터가 없습니다.",
    show_table_expander: bool = True,
    table_expander_label: str = "원본 표 보기",
    caption: str | None = None,
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if frame.empty:
        st.info(empty_message)
        return

    secondary_columns = secondary_columns or []
    if primary_column is None:
        primary_column = str(frame.columns[0]) if len(frame.columns) > 0 else None

    if detail_columns is None:
        excluded = {primary_column, *secondary_columns}
        detail_columns = [column for column in frame.columns if column not in excluded]

    selected_columns = _existing_columns(
        frame,
        [column for column in [primary_column, *secondary_columns, *detail_columns] if column is not None],
    )
    selected_columns = _deduplicate_preserving_order(selected_columns)
    localized, label_map = _localized_display(frame, selected_columns or None)
    primary_label = label_map.get(primary_column) if primary_column else None

    if not primary_label:
        st.caption("모바일 요약용 기준 열을 찾지 못해 전체 표만 표시합니다.")
        st.dataframe(localized, width="stretch", hide_index=True)
        return

    rows = localized.head(limit).to_dict(orient="records")
    row_markup: list[str] = []
    for row in rows:
        title_value = _display_value(row.get(primary_label))
        secondary_values = [
            _display_value(row.get(label_map[column]))
            for column in secondary_columns
            if column in label_map
        ]
        secondary_values = [value for value in secondary_values if value != "-"]
        secondary_html = (
            f'<div class="sm-sheet-secondary">{escape(" · ".join(secondary_values))}</div>'
            if secondary_values
            else ""
        )

        detail_parts: list[str] = []
        for column in detail_columns:
            if column not in label_map:
                continue
            label = label_map[column]
            value = _display_value(row.get(label))
            detail_parts.append(
                f'<div class="sm-sheet-item"><div class="sm-sheet-label">{escape(str(label))}</div>'
                f'<div class="sm-sheet-value">{escape(str(value))}</div></div>'
            )
        detail_html = f'<div class="sm-sheet-grid">{"".join(detail_parts)}</div>' if detail_parts else ""
        badge_html = _row_badge_html(row, label_map, [*secondary_columns, *detail_columns])
        row_markup.append(
            '<section class="sm-sheet-row">'
            '<div class="sm-sheet-head">'
            f'<div><h4 class="sm-sheet-title">{escape(str(title_value))}</h4>{secondary_html}</div>'
            f"{badge_html}</div>{detail_html}</section>"
        )
    st.markdown(f'<div class="sm-sheet">{"".join(row_markup)}</div>', unsafe_allow_html=True)

    if show_table_expander:
        with st.expander(table_expander_label, expanded=False):
            st.dataframe(localized, width="stretch", hide_index=True)


def render_record_cards(
    frame,
    *,
    title: str,
    primary_column: str,
    secondary_columns: list[str] | None = None,
    detail_columns: list[str] | None = None,
    limit: int = 5,
    empty_message: str = "표시할 데이터가 없습니다.",
    show_table_expander: bool = True,
    table_expander_label: str = "원본 표 보기",
) -> None:
    render_data_sheet(
        frame,
        title=title,
        primary_column=primary_column,
        secondary_columns=secondary_columns,
        detail_columns=detail_columns,
        limit=limit,
        empty_message=empty_message,
        show_table_expander=show_table_expander,
        table_expander_label=table_expander_label,
    )


def _latest_snapshot_row(settings: Settings) -> dict[str, object] | None:
    frame = latest_app_snapshot_frame(settings)
    if frame.empty:
        return None
    return frame.iloc[0].to_dict()


def _policy_badges(snapshot: dict[str, object] | None) -> list[tuple[str, str]]:
    if snapshot is None:
        return []

    badges: list[tuple[str, str]] = [("현재 추천 모델", "INFO")]
    if snapshot.get("latest_daily_bundle_status"):
        badges.append(
            (
                f"일일 배치 {format_ui_value('status', snapshot['latest_daily_bundle_status'])}",
                str(snapshot["latest_daily_bundle_status"]),
            )
        )
    if snapshot.get("health_status"):
        badges.append(
            (
                f"운영 상태 {format_ui_value('health_status', snapshot['health_status'])}",
                str(snapshot["health_status"]),
            )
        )
    if snapshot.get("active_intraday_policy_id"):
        badges.append((f"장중 정책 {snapshot['active_intraday_policy_id']}", "INFO"))
    if snapshot.get("active_portfolio_policy_id"):
        badges.append((f"포트폴리오 정책 {snapshot['active_portfolio_policy_id']}", "INFO"))
    if snapshot.get("active_ops_policy_id"):
        badges.append((f"운영 정책 {snapshot['active_ops_policy_id']}", "INFO"))

    raw_meta = snapshot.get("active_meta_model_ids_json")
    if raw_meta:
        try:
            meta_items = json.loads(str(raw_meta))
        except json.JSONDecodeError:
            meta_items = []
        if meta_items:
            badges.append((f"메타 모델 {len(meta_items)}개", "INFO"))
    return badges


def render_page_header(
    settings: Settings,
    *,
    page_name: str,
    title: str,
    description: str,
) -> None:
    inject_app_styles()
    st.title(title)
    st.caption(description)

    snapshot = _latest_snapshot_row(settings)
    render_status_badges(_policy_badges(snapshot))

    freshness = latest_ui_freshness_frame(settings, page_name=page_name, limit=20)
    critical = freshness[freshness["warning_level"].astype(str).str.upper() == "CRITICAL"]
    warning = freshness[freshness["warning_level"].astype(str).str.upper() == "WARNING"]
    if not critical.empty:
        render_warning_banner(
            "CRITICAL",
            f"{page_name} 화면에 치명적인 지연 데이터가 있습니다. 숫자와 리포트 링크를 보수적으로 해석해야 합니다.",
        )
    elif not warning.empty:
        render_warning_banner(
            "WARNING",
            f"{page_name} 화면 일부 데이터가 경고 임계치를 넘었습니다. 최신 실행 이력과 신선도 상태를 함께 확인하세요.",
        )


def render_provenance_footer(
    settings: Settings,
    *,
    page_name: str,
    extra_items: list[str] | None = None,
) -> None:
    snapshot = _latest_snapshot_row(settings)
    pieces = [f"환경: {settings.app.env.upper()}", f"페이지: {page_name}"]
    if snapshot:
        if snapshot.get("as_of_date") is not None:
            pieces.append(f"기준일: {format_ui_date(snapshot.get('as_of_date'))}")
        if snapshot.get("snapshot_ts") is not None:
            pieces.append(f"업데이트: {format_ui_datetime(snapshot.get('snapshot_ts'))}")
        if snapshot.get("health_status"):
            pieces.append(f"상태: {format_ui_value('health_status', snapshot['health_status'])}")
    if extra_items:
        pieces.extend(extra_items)
    st.markdown(
        f'<div class="sm-footer">{" | ".join(str(piece) for piece in pieces)}</div>',
        unsafe_allow_html=True,
    )


def render_page_footer(
    settings: Settings,
    *,
    page_name: str,
    extra_items: list[str] | None = None,
) -> None:
    render_provenance_footer(settings, page_name=page_name, extra_items=extra_items)


def render_report_center(settings: Settings, *, limit: int = 12) -> None:
    reports = latest_report_index_frame(settings, limit=limit)
    if reports.empty:
        st.info("리포트 목록이 없습니다. `build_report_index.py`를 먼저 실행하세요.")
        return

    render_data_sheet(
        reports,
        title="리포트 센터",
        primary_column="report_type",
        secondary_columns=["status", "as_of_date"],
        detail_columns=["generated_ts", "published_flag", "dry_run_flag"],
        limit=limit,
        empty_message="리포트 목록이 없습니다. `build_report_index.py`를 먼저 실행하세요.",
        show_table_expander=True,
        table_expander_label="리포트 전체 표 보기",
        caption="모바일에서는 리포트 상태와 발행 여부를 먼저 읽고, 원본 표는 필요할 때만 펼칩니다.",
    )


def render_release_candidate_summary(settings: Settings, *, limit: int = 12) -> None:
    checks = latest_release_candidate_check_frame(settings, limit=limit)
    if checks.empty:
        st.info("릴리스 점검 결과가 없습니다. `validate_release_candidate.py`를 실행하세요.")
        return

    render_data_sheet(
        checks,
        title="릴리스 후보 점검",
        primary_column="check_name",
        secondary_columns=["status", "severity"],
        detail_columns=["check_ts", "recommended_action"],
        limit=limit,
        empty_message="릴리스 점검 결과가 없습니다. `validate_release_candidate.py`를 실행하세요.",
        show_table_expander=True,
        table_expander_label="릴리스 점검 전체 표 보기",
    )


def render_glossary_hint(term: str) -> None:
    entry = glossary_mapping().get(term)
    if entry is None:
        return
    st.caption(f"{entry.short_label}: {entry.definition}")


def render_top_actionable_badges(settings: Settings) -> None:
    snapshot = _latest_snapshot_row(settings)
    if not snapshot:
        return

    raw_value = snapshot.get("top_actionable_symbol_list_json")
    if not raw_value:
        return

    try:
        records = json.loads(str(raw_value))
    except json.JSONDecodeError:
        return

    badges = []
    for record in records[:5]:
        symbol = record.get("symbol", "N/A")
        grade = record.get("grade", "")
        badges.append((f"{symbol} {grade}".strip(), "INFO"))
    render_status_badges(badges)


def read_markdown(path: Path) -> str:
    if not path.exists():
        return f"> 문서를 찾을 수 없습니다: `{path}`"
    return path.read_text(encoding="utf-8")
