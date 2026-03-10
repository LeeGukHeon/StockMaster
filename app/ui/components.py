# ruff: noqa: E501, I001

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import streamlit as st

from app.settings import Settings
from app.ui.glossary import glossary_mapping
from app.ui.helpers import (
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
        .sm-badge-row {display:flex; flex-wrap:wrap; gap:0.45rem; margin:0.3rem 0 1rem 0;}
        .sm-badge {
            display:inline-flex; align-items:center; gap:0.4rem;
            padding:0.3rem 0.7rem; border-radius:999px; color:white;
            font-size:0.85rem; font-weight:600;
        }
        .sm-banner {
            border-radius:14px; padding:0.85rem 1rem; margin:0.35rem 0 1rem 0;
            border-left:6px solid transparent; background:#f8fafc;
        }
        .sm-footer {
            margin-top:1.25rem; padding-top:0.8rem; border-top:1px solid rgba(148,163,184,0.35);
            color:#475569; font-size:0.85rem;
        }
        .sm-card {
            border:1px solid rgba(148,163,184,0.25); border-radius:16px; padding:0.9rem 1rem;
            background:linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,250,252,0.96));
            margin-bottom:0.9rem;
        }
        .sm-card h4 {margin:0 0 0.25rem 0; font-size:1rem;}
        .sm-card p {margin:0; color:#475569;}
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
        f'<div class="sm-card"><h4>{title}</h4><p>{body}</p></div>',
        unsafe_allow_html=True,
    )


def _display_value(value: object) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text if text else "-"


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
        with st.container(border=True):
            st.markdown(f"**{primary_value}**")
            if secondary_text:
                st.caption(secondary_text)
            for column in detail_columns:
                if column not in label_map:
                    continue
                label = label_map[column]
                st.write(f"{label}: {_display_value(row.get(label))}")

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
        badges.append((f"일일 배치 {snapshot['latest_daily_bundle_status']}", str(snapshot["latest_daily_bundle_status"])))
    if snapshot.get("health_status"):
        badges.append((f"운영 상태 {snapshot['health_status']}", str(snapshot["health_status"])))
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
        pieces.append(f"스냅샷: {snapshot.get('snapshot_ts')}")
        if snapshot.get("latest_daily_bundle_run_id"):
            pieces.append(f"일일 배치: {snapshot['latest_daily_bundle_run_id']}")
        if snapshot.get("active_ops_policy_id"):
            pieces.append(f"운영 정책: {snapshot['active_ops_policy_id']}")
        if snapshot.get("active_portfolio_policy_id"):
            pieces.append(f"포트폴리오 정책: {snapshot['active_portfolio_policy_id']}")
        if snapshot.get("active_intraday_policy_id"):
            pieces.append(f"장중 정책: {snapshot['active_intraday_policy_id']}")
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
            "artifact_path",
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
