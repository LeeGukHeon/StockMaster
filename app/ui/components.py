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
            --sm-ink: #14231f;
            --sm-muted: #53625d;
            --sm-accent: #0f766e;
            --sm-accent-soft: rgba(15, 118, 110, 0.14);
            --sm-highlight: #b45309;
            --sm-paper: rgba(255, 252, 247, 0.88);
            --sm-panel: rgba(255, 255, 255, 0.72);
            --sm-border: rgba(20, 35, 31, 0.11);
            --sm-shadow: 0 18px 40px rgba(61, 42, 24, 0.08);
        }
        .stApp {
            background:
                radial-gradient(circle at 12% 10%, rgba(15, 118, 110, 0.16), transparent 28%),
                radial-gradient(circle at 88% 8%, rgba(180, 83, 9, 0.14), transparent 24%),
                linear-gradient(180deg, #f6f1e8 0%, #f1eadf 42%, #ece3d7 100%);
            color: var(--sm-ink);
        }
        html, body, [class*="css"]  {
            font-family: "SUIT Variable", "Pretendard Variable", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
        }
        .block-container {
            max-width: 1220px;
            padding-top: 1.2rem;
            padding-bottom: 1.4rem;
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
            font-size:0.82rem; font-weight:700; box-shadow:0 10px 24px rgba(20,35,31,0.12);
        }
        .sm-banner {
            border-radius:18px; padding:0.95rem 1.05rem; margin:0.45rem 0 1rem 0;
            border:1px solid rgba(20,35,31,0.08);
            border-left:6px solid transparent;
            background:linear-gradient(135deg, rgba(255,255,255,0.84), rgba(255,249,240,0.72));
            box-shadow: var(--sm-shadow);
        }
        .sm-footer {
            margin-top:1.35rem; padding-top:0.95rem; border-top:1px solid rgba(20,35,31,0.12);
            color:var(--sm-muted); font-size:0.85rem;
        }
        .sm-card {
            border:1px solid var(--sm-border); border-radius:22px; padding:1rem 1.08rem;
            background:linear-gradient(160deg, rgba(255,255,255,0.9), rgba(251,247,240,0.82));
            box-shadow: var(--sm-shadow); backdrop-filter: blur(12px); margin-bottom:1rem;
        }
        .sm-card h4 {margin:0 0 0.28rem 0; font-size:1.02rem; color:var(--sm-ink);}
        .sm-card p {margin:0; color:var(--sm-muted); line-height:1.68;}
        .sm-guide {
            border:1px solid rgba(15,118,110,0.16);
            border-radius:22px;
            padding:1.05rem 1.1rem 0.9rem 1.1rem;
            background:linear-gradient(145deg, rgba(235,247,245,0.9), rgba(255,251,245,0.9));
            box-shadow: var(--sm-shadow);
            margin:0.25rem 0 1rem 0;
        }
        .sm-guide h4 {margin:0 0 0.35rem 0; font-size:1.02rem; color:var(--sm-ink);}
        .sm-guide p {margin:0 0 0.45rem 0; color:var(--sm-muted); line-height:1.62;}
        .sm-guide ul {margin:0.24rem 0 0 1.1rem; color:var(--sm-muted);}
        .sm-guide li {margin:0.18rem 0;}
        .sm-report-preview {
            border:1px solid var(--sm-border);
            border-radius:22px;
            padding:1rem 1.05rem;
            background:linear-gradient(180deg, rgba(255,255,255,0.92), rgba(248,244,237,0.92));
            box-shadow: var(--sm-shadow);
        }
        .sm-report-preview p,
        .sm-report-preview li {
            line-height:1.7;
        }
        .sm-record-card {
            border:1px solid var(--sm-border);
            border-radius:22px;
            padding:1rem 1.05rem;
            margin-bottom:0.78rem;
            background:linear-gradient(160deg, rgba(255,255,255,0.94), rgba(249,245,238,0.84));
            box-shadow: var(--sm-shadow);
        }
        .sm-record-primary {
            font-size:1.02rem;
            font-weight:800;
            color:var(--sm-ink);
            letter-spacing:-0.02em;
        }
        .sm-record-secondary {
            margin-top:0.2rem;
            color:var(--sm-muted);
            font-size:0.86rem;
        }
        .sm-record-grid {
            display:grid;
            grid-template-columns:repeat(auto-fit, minmax(180px, 1fr));
            gap:0.62rem 0.8rem;
            margin-top:0.82rem;
        }
        .sm-record-item {
            padding:0.66rem 0.72rem;
            border-radius:16px;
            background:rgba(255,255,255,0.62);
            border:1px solid rgba(20,35,31,0.08);
        }
        .sm-record-label {
            font-size:0.74rem;
            font-weight:700;
            letter-spacing:0.02em;
            text-transform:uppercase;
            color:#6a7a75;
        }
        .sm-record-value {
            margin-top:0.18rem;
            color:var(--sm-ink);
            font-size:0.96rem;
            font-weight:700;
            line-height:1.42;
            word-break:break-word;
        }
        div[data-testid="stMetric"] {
            background:linear-gradient(160deg, rgba(255,255,255,0.92), rgba(248,243,235,0.86));
            border:1px solid var(--sm-border);
            border-radius:22px;
            padding:0.8rem 0.95rem;
            box-shadow: var(--sm-shadow);
        }
        div[data-testid="stMetricLabel"] {
            color:#61706b;
            font-weight:700;
        }
        div[data-testid="stMetricValue"] {
            color:var(--sm-ink);
            letter-spacing:-0.03em;
        }
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div,
        div[data-testid="stDateInput"] > div > div,
        div[data-testid="stMultiSelect"] > div,
        div[data-testid="stTextInput"] > div > div {
            background:rgba(255,255,255,0.72);
            border-radius:16px;
            border:1px solid rgba(20,35,31,0.12);
            box-shadow:0 10px 24px rgba(61,42,24,0.05);
        }
        div[data-testid="stDataFrame"] {
            border:1px solid var(--sm-border);
            border-radius:22px;
            overflow:hidden;
            background:rgba(255,255,255,0.74);
            box-shadow: var(--sm-shadow);
        }
        div[data-testid="stExpander"] {
            border:1px solid var(--sm-border);
            border-radius:20px;
            background:rgba(255,255,255,0.56);
            box-shadow: var(--sm-shadow);
        }
        .stTabs [role="tablist"] {
            gap:0.38rem;
            padding:0.3rem;
            border-radius:999px;
            background:rgba(255,255,255,0.48);
            border:1px solid rgba(20,35,31,0.08);
            width:fit-content;
        }
        .stTabs [role="tab"] {
            border-radius:999px;
            padding:0.4rem 0.95rem;
            height:auto;
            background:transparent;
            color:#4f5d58;
        }
        .stTabs [aria-selected="true"] {
            background:linear-gradient(135deg, rgba(15,118,110,0.92), rgba(22,163,74,0.88));
            color:white;
        }
        div[data-baseweb="button-group"] button,
        button[kind="secondary"] {
            border-radius:999px !important;
            border:1px solid rgba(20,35,31,0.1) !important;
            background:rgba(255,255,255,0.72) !important;
            color:var(--sm-ink) !important;
        }
        @media (max-width: 900px) {
            .block-container {
                padding-left: 0.8rem;
                padding-right: 0.8rem;
            }
            [data-testid="column"] {
                min-width: 100% !important;
                flex: 1 1 100% !important;
            }
            .sm-badge {
                font-size:0.8rem;
                padding:0.28rem 0.62rem;
            }
            .sm-card,
            .sm-guide,
            .sm-report-preview,
            .sm-record-card,
            div[data-testid="stMetric"],
            div[data-testid="stDataFrame"] {
                border-radius:16px;
                padding:0.85rem 0.9rem;
            }
            h1 {
                font-size: 1.8rem !important;
            }
            [data-testid="stDataFrame"] {
                font-size: 0.82rem;
            }
            [data-testid="stTabs"] button {
                padding-left: 0.5rem !important;
                padding-right: 0.5rem !important;
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
    return f'<span class="sm-badge" style="background:{color};">{display}</span>'


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


def render_screen_guide(
    *,
    summary: str,
    bullets: list[str] | None = None,
    title: str = "이 화면은 이렇게 보세요",
) -> None:
    bullet_html = ""
    if bullets:
        bullet_html = "<ul>" + "".join(f"<li>{item}</li>" for item in bullets) + "</ul>"
    st.markdown(
        f'<div class="sm-guide"><h4>{title}</h4><p>{summary}</p>{bullet_html}</div>',
        unsafe_allow_html=True,
    )


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
    st.subheader(title)
    if frame.empty:
        st.info(empty_message)
        return

    secondary_columns = secondary_columns or []
    detail_columns = detail_columns or []
    selected_columns = [
        column
        for column in [primary_column, *secondary_columns, *detail_columns]
        if column in frame.columns
    ]
    display = frame[selected_columns].copy()
    localized = localize_frame(display)
    localized_columns = list(localized.columns)
    label_map = dict(zip(selected_columns, localized_columns, strict=False))

    if primary_column not in label_map:
        st.dataframe(localized, width="stretch", hide_index=True)
        return

    rows = localized.head(limit).to_dict(orient="records")
    for row in rows:
        primary_value = _display_value(row.get(label_map[primary_column]))
        secondary_text = " · ".join(
            _display_value(row.get(label_map[column]))
            for column in secondary_columns
            if column in label_map
        )
        detail_parts: list[str] = []
        for column in detail_columns:
            if column not in label_map:
                continue
            label = label_map[column]
            value = _display_value(row.get(label))
            detail_parts.append(
                f'<div class="sm-record-item"><div class="sm-record-label">{escape(str(label))}</div>'
                f'<div class="sm-record-value">{escape(str(value))}</div></div>'
            )
        secondary_html = (
            f'<div class="sm-record-secondary">{escape(str(secondary_text))}</div>'
            if secondary_text
            else ""
        )
        detail_html = f'<div class="sm-record-grid">{"".join(detail_parts)}</div>' if detail_parts else ""
        st.markdown(
            (
                f'<div class="sm-record-card"><div class="sm-record-primary">{escape(str(primary_value))}</div>'
                f"{secondary_html}{detail_html}</div>"
            ),
            unsafe_allow_html=True,
        )

    if show_table_expander:
        with st.expander(table_expander_label, expanded=False):
            st.dataframe(localized, width="stretch", hide_index=True)


def _latest_snapshot_row(settings: Settings) -> dict[str, object] | None:
    frame = latest_app_snapshot_frame(settings)
    if frame.empty:
        return None
    return frame.iloc[0].to_dict()


def _policy_badges(snapshot: dict[str, object] | None) -> list[tuple[str, str]]:
    if snapshot is None:
        return []

    badges: list[tuple[str, str]] = [("선정 엔진 v2", "INFO")]
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

    display = reports[
        [
            "report_type",
            "as_of_date",
            "generated_ts",
            "status",
            "published_flag",
            "dry_run_flag",
        ]
    ].copy()
    st.dataframe(localize_frame(display), width="stretch", hide_index=True)


def render_release_candidate_summary(settings: Settings, *, limit: int = 12) -> None:
    checks = latest_release_candidate_check_frame(settings, limit=limit)
    if checks.empty:
        st.info("릴리스 점검 결과가 없습니다. `validate_release_candidate.py`를 실행하세요.")
        return

    display = checks[
        ["check_ts", "check_name", "status", "severity", "recommended_action"]
    ].copy()
    st.dataframe(localize_frame(display), width="stretch", hide_index=True)


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
