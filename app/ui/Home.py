# ruff: noqa: E402, E501

from __future__ import annotations

import json
import sys
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ui.components import (
    inject_app_styles,
    render_page_footer,
    render_report_preview,
    render_screen_guide,
    render_warning_banner,
)
from app.ui.helpers import (
    dashboard_activity_state,
    format_ui_date,
    format_ui_value,
    home_banner_freshness_levels,
    latest_alert_event_frame,
    latest_alpha_promotion_summary_frame,
    latest_app_snapshot_frame,
    latest_market_news_frame,
    latest_market_mood_summary,
    latest_portfolio_target_book_frame,
    latest_recommendation_timeline_text,
    latest_release_candidate_preview,
    latest_report_index_frame,
    latest_sector_outlook_frame,
    latest_ui_freshness_frame,
    leaderboard_frame,
    load_ui_base_settings,
    load_ui_page_context,
)
from app.ui.navigation import (
    build_navigation_registry,
    dashboard_page_groups,
    safe_dashboard_page_keys,
)


def _snapshot_row(settings):
    frame = latest_app_snapshot_frame(settings)
    if frame.empty:
        return None
    return frame.iloc[0]


def _display_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def _pill(label: str, tone: str = "neutral") -> str:
    normalized = "".join(ch for ch in tone.lower() if ch.isalpha() or ch == "-") or "neutral"
    return f'<span class="sm-mini-pill sm-tone-{escape(normalized)}">{escape(label)}</span>'


def _hero_badges(snapshot_row, activity) -> str:
    badges: list[str] = [
        _pill("WRITE BUSY" if activity.writer_active else "LIVE READY", "warning" if activity.writer_active else "positive")
    ]
    if snapshot_row is not None and snapshot_row.get("health_status"):
        badges.append(
            _pill(
                f"운영 {format_ui_value('health_status', snapshot_row.get('health_status'))}",
                str(snapshot_row.get("health_status", "neutral")).lower(),
            )
        )
    if snapshot_row is not None and snapshot_row.get("latest_daily_bundle_status"):
        badges.append(
            _pill(
                f"일일 배치 {format_ui_value('status', snapshot_row.get('latest_daily_bundle_status'))}",
                str(snapshot_row.get("latest_daily_bundle_status", "neutral")).lower(),
            )
        )
    active_symbols = []
    if snapshot_row is not None and snapshot_row.get("top_actionable_symbol_list_json"):
        try:
            active_symbols = json.loads(str(snapshot_row.get("top_actionable_symbol_list_json")))
        except json.JSONDecodeError:
            active_symbols = []
    for record in active_symbols[:2]:
        symbol = _display_text(record.get("symbol"))
        grade = _display_text(record.get("grade"), "")
        badges.append(_pill(f"{symbol} {grade}".strip(), "accent"))
    return "".join(badges)


def _render_dashboard_hero(snapshot_row, market_mood: dict[str, str], activity) -> None:
    as_of_date = format_ui_date(snapshot_row.get("as_of_date")) if snapshot_row is not None else "-"
    health_status = (
        format_ui_value("health_status", snapshot_row.get("health_status"))
        if snapshot_row is not None
        else "-"
    )
    warning_count = int(snapshot_row.get("warning_alert_count") or 0) if snapshot_row is not None else 0
    critical_count = int(snapshot_row.get("critical_alert_count") or 0) if snapshot_row is not None else 0
    hero_title = market_mood.get("headline", "운영 기준점")
    hero_detail = market_mood.get("detail", "")
    hero_mode = "읽기 전용 안전 모드" if activity.writer_active else "실시간 운영 대시보드"
    hero_body = hero_detail or (
        "추천 결과, 운영 상태, 최신 리포트를 카드나 표보다 세로 흐름에 가깝게 읽도록 다시 정리한 화면입니다."
    )
    st.markdown(
        (
            '<section class="sm-hero">'
            '<div class="sm-hero-copy">'
            f'<div class="sm-hero-kicker">{escape(hero_mode)}</div>'
            f'<h1 class="sm-hero-title">{escape(hero_title)}</h1>'
            f'<p class="sm-hero-body">{escape(hero_body)}</p>'
            f'<div class="sm-hero-badges">{_hero_badges(snapshot_row, activity)}</div>'
            '</div>'
            '<div class="sm-hero-stats">'
            f'<div class="sm-hero-stat"><span>기준일</span><strong>{escape(as_of_date)}</strong></div>'
            f'<div class="sm-hero-stat"><span>운영상태</span><strong>{escape(health_status)}</strong></div>'
            f'<div class="sm-hero-stat"><span>열린 경고</span><strong>{warning_count + critical_count}</strong></div>'
            '</div>'
            '</section>'
        ),
        unsafe_allow_html=True,
    )


def _quick_link(label: str, page_key: str, description: str) -> None:
    st.markdown(f"**{label}**")
    st.caption(description)
    page = NAVIGATION_REGISTRY.get(page_key)
    if page is None:
        st.caption("페이지 연결 정보를 찾지 못했습니다.")
        return
    st.page_link(page, label=f"{label} 열기", icon=":material/open_in_new:")


def _render_page_access_summary() -> None:
    safe_specs, restricted_specs = dashboard_page_groups(PROJECT_ROOT)
    allowed_col, blocked_col = st.columns(2)
    with allowed_col:
        st.markdown("**지금 열 수 있는 페이지**")
        for spec in safe_specs:
            st.markdown(f"- {spec.title}")
    with blocked_col:
        st.markdown("**작업 종료 후 열리는 페이지**")
        for spec in restricted_specs:
            st.markdown(f"- {spec.title}")


def _render_story_section(
    *,
    title: str,
    summary: str,
    items: list[dict[str, str]],
    empty_message: str,
) -> None:
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
        story_html.append(
            '<article class="sm-story">'
            '<div class="sm-story-top">'
            f'{_pill(badge, tone) if badge else ""}'
            f'<span class="sm-story-eyebrow">{escape(eyebrow)}</span>'
            '</div>'
            f'<h4>{escape(heading)}</h4>'
            f'<p>{escape(body)}</p>'
            f'<div class="sm-story-meta">{escape(meta)}</div>'
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


def _build_brief_items(
    snapshot_row,
    alerts: pd.DataFrame,
    freshness: pd.DataFrame,
    market_mood: dict[str, str],
    settings,
) -> list[dict[str, str]]:
    freshness_issue_count = int(freshness["stale_flag"].fillna(False).sum()) if not freshness.empty else 0
    return [
        {
            "eyebrow": "Market",
            "title": market_mood.get("headline", "시장 분위기"),
            "body": market_mood.get("detail", "최신 시장 맥락 요약을 불러오지 못했습니다."),
            "meta": market_mood.get("label", "-"),
            "badge": market_mood.get("label", "MOOD"),
            "tone": "positive" if "상승" in market_mood.get("headline", "") else "neutral",
        },
        {
            "eyebrow": "Recommendation",
            "title": "오늘 추천 해석",
            "body": latest_recommendation_timeline_text(settings),
            "meta": format_ui_date(snapshot_row.get("as_of_date")) if snapshot_row is not None else "-",
            "badge": "TIMELINE",
            "tone": "accent",
        },
        {
            "eyebrow": "Operations",
            "title": "운영 기준 요약",
            "body": _today_narrative(snapshot_row, alerts, freshness, market_mood),
            "meta": f"경고 {len(alerts)}건 · 최신성 이슈 {freshness_issue_count}건",
            "badge": "OPS",
            "tone": "warning" if not alerts.empty or freshness_issue_count else "positive",
        },
    ]


def _build_focus_items(
    selection_preview: pd.DataFrame,
    official_targets: pd.DataFrame,
    sector_outlook: pd.DataFrame,
) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in selection_preview.head(3).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Selection",
                "title": f"{_display_text(row.get('symbol'))} · {_display_text(row.get('company_name'))}",
                "body": f"{_display_text(row.get('industry'))} / 예상 초과수익률 {_display_text(row.get('expected_excess_return'))}",
                "meta": f"진입일 {_display_text(row.get('next_entry_trade_date'))} · 모델 {_display_text(row.get('model_spec_id'))}",
                "badge": _display_text(row.get("sector")),
                "tone": "positive",
            }
        )
    if not items:
        for row in official_targets.head(3).to_dict(orient="records"):
            items.append(
                {
                    "eyebrow": "Portfolio",
                    "title": f"{_display_text(row.get('symbol'))} · {_display_text(row.get('company_name'))}",
                    "body": f"{_display_text(row.get('action_plan_label'))} / 목표가 {_display_text(row.get('target_price'))}",
                    "meta": f"편입일 {_display_text(row.get('entry_trade_date'))} · horizon {_display_text(row.get('plan_horizon'))}",
                    "badge": _display_text(row.get("sector")),
                    "tone": "accent",
                }
            )
    if not items:
        for row in sector_outlook.head(3).to_dict(orient="records"):
            items.append(
                {
                    "eyebrow": "Sector",
                    "title": _display_text(row.get("outlook_label")),
                    "body": f"표본 {_display_text(row.get('symbol_count'))}개 · 상위 {_display_text(row.get('top10_count'))}개",
                    "meta": _display_text(row.get("sample_symbols")),
                    "badge": _display_text(row.get("broad_sector")),
                    "tone": "neutral",
                }
            )
    return items


def _build_model_items(alpha_promotion: pd.DataFrame) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in alpha_promotion.head(3).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": _display_text(row.get("summary_title"), "Alpha"),
                "title": f"{_display_text(row.get('active_model_label'))} vs {_display_text(row.get('comparison_model_label'))}",
                "body": f"{_display_text(row.get('decision_label'))} / gap {_display_text(row.get('promotion_gap'))}",
                "meta": f"표본 {_display_text(row.get('sample_count'))} · window {_display_text(row.get('window_end'))}",
                "badge": _display_text(row.get("decision_label"), "MODEL"),
                "tone": "neutral",
            }
        )
    return items


def _build_news_items(latest_news: pd.DataFrame) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in latest_news.head(4).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": _display_text(row.get("provider")),
                "title": _display_text(row.get("title")),
                "body": _display_text(row.get("linked_symbols")),
                "meta": f"{_display_text(row.get('published_at'))} · {_display_text(row.get('news_category'))}",
                "badge": "NEWS",
                "tone": "neutral",
            }
        )
    return items


def _build_ops_items(
    alerts: pd.DataFrame,
    freshness: pd.DataFrame,
    latest_reports: pd.DataFrame,
) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in alerts.head(2).to_dict(orient="records"):
        severity = _display_text(row.get("severity"))
        items.append(
            {
                "eyebrow": "Alert",
                "title": _display_text(row.get("message")),
                "body": f"{_display_text(row.get('component_name'))} · {severity}",
                "meta": _display_text(row.get("created_at")),
                "badge": severity,
                "tone": severity.lower(),
            }
        )
    for row in freshness.head(2).to_dict(orient="records"):
        level = _display_text(row.get("warning_level"))
        items.append(
            {
                "eyebrow": "Freshness",
                "title": _display_text(row.get("dataset_name")),
                "body": f"{_display_text(row.get('page_name'))} · 최신 시각 {_display_text(row.get('latest_available_ts'))}",
                "meta": f"stale={_display_text(row.get('stale_flag'))}",
                "badge": level,
                "tone": level.lower(),
            }
        )
    for row in latest_reports.head(2).to_dict(orient="records"):
        status = _display_text(row.get("status"))
        items.append(
            {
                "eyebrow": "Report",
                "title": _display_text(row.get("report_type")),
                "body": f"기준일 {_display_text(row.get('as_of_date'))} · 발행 {_display_text(row.get('published_flag'))}",
                "meta": _display_text(row.get("generated_ts")),
                "badge": status,
                "tone": status.lower(),
            }
        )
    return items


def _today_narrative(
    snapshot_row,
    alerts: pd.DataFrame,
    freshness: pd.DataFrame,
    market_mood: dict[str, str],
) -> str:
    if snapshot_row is None:
        return "앱 스냅샷이 아직 없어 기준일과 정책 상태를 요약하지 못했습니다."

    stale_count = int(freshness["stale_flag"].fillna(False).sum()) if not freshness.empty else 0
    critical_alert_count = int(snapshot_row.get("critical_alert_count") or 0)
    parts = [
        f"기준일은 {format_ui_date(snapshot_row.get('as_of_date'))}입니다.",
        f"시장 분위기는 {market_mood.get('headline', '-')} / {market_mood.get('label', '-')} 쪽으로 읽히고 있습니다.",
    ]
    if critical_alert_count > 0:
        parts.append(f"치명 경고가 {critical_alert_count}건 열려 있습니다.")
    elif not alerts.empty:
        parts.append(f"운영 경고가 {len(alerts)}건 남아 있습니다.")
    else:
        parts.append("현재 열린 치명 경고는 없습니다.")
    if stale_count > 0:
        parts.append(f"최신성 이슈가 {stale_count}건 있어 수치는 보수적으로 해석해야 합니다.")
    else:
        parts.append("최신성 경고는 운영 허용 범위 안입니다.")
    return " ".join(parts)


def render_today_page() -> None:
    settings, activity = load_ui_page_context(
        PROJECT_ROOT,
        page_key="today",
        page_title="오늘",
    )
    inject_app_styles()

    snapshot_row = _snapshot_row(settings)
    alerts = latest_alert_event_frame(settings, limit=10)
    freshness = latest_ui_freshness_frame(settings, limit=30)
    latest_reports = latest_report_index_frame(settings, limit=12, latest_only=True)
    market_mood = latest_market_mood_summary(settings)
    release_preview = latest_release_candidate_preview(settings)

    _render_dashboard_hero(snapshot_row, market_mood, activity)

    if activity.writer_active:
        render_warning_banner(
            "WARNING",
            "현재는 학습 또는 백필이 돌아가는 중이라 읽기 전용 안전 모드로 전환되었습니다. 추천 상세와 분석 탭은 작업 종료 후 다시 열립니다.",
        )
        _render_page_access_summary()
        render_screen_guide(
            summary="안전 모드에서는 메타데이터 기반 요약만 남기고, 무거운 분석 화면은 잠시 숨깁니다.",
            bullets=[
                "새벽이나 장중에도 홈에서 운영 상태와 최신 리포트는 바로 확인할 수 있습니다.",
                "추천·종목분석·리더보드는 작업 종료 후 자동으로 다시 열립니다.",
            ],
        )
        _render_story_section(
            title="지금 확인 가능한 브리프",
            summary="잠금과 상관없는 메타데이터 기반 정보만 세로 흐름으로 보여줍니다.",
            items=_build_brief_items(snapshot_row, alerts, freshness, market_mood, settings),
            empty_message="표시할 안전 모드 브리프가 없습니다.",
        )
        _render_story_section(
            title="운영 메타 스트림",
            summary="현재 작업 중에도 안전하게 읽을 수 있는 경고, 최신성, 리포트 상태입니다.",
            items=_build_ops_items(alerts, freshness, latest_reports),
            empty_message="운영 메타데이터가 아직 없습니다.",
        )
        if release_preview:
            with st.expander("최신 릴리즈 체크리스트 미리보기", expanded=False):
                render_report_preview(title="릴리즈 체크리스트", preview=release_preview)
        _quick_link("문서 / 도움말", "docs", "운영 가이드와 용어, 복구 문서를 확인합니다.")
        render_page_footer(settings, page_name="오늘", extra_items=["mode: safe-readonly"])
        return

    critical_freshness, warning_freshness = home_banner_freshness_levels(freshness)
    if snapshot_row is None:
        render_warning_banner(
            "CRITICAL",
            "앱 스냅샷이 아직 없습니다. 최신 기준일과 운영 상태를 만들기 위해 snapshot materialization이 먼저 필요합니다.",
        )
    elif not critical_freshness.empty:
        render_warning_banner(
            "CRITICAL",
            "일부 핵심 데이터셋 최신성이 임계치 밖입니다. 추천과 운영 숫자는 보수적으로 읽는 편이 안전합니다.",
        )
    elif not warning_freshness.empty:
        render_warning_banner(
            "WARNING",
            "일부 데이터셋 최신성이 경고 구간입니다. 최신 배치와 리포트 생성 시각을 함께 확인해 주세요.",
        )

    render_screen_guide(
        summary="모바일에서 한 손으로 읽을 수 있도록, 추천 포커스와 운영 상태를 세로 흐름 중심으로 재정리했습니다.",
        bullets=[
            "오늘 브리프에서 시장 분위기와 운영 기준을 먼저 읽습니다.",
            "추천 포커스에서 내일 진입 후보와 포트폴리오 방향을 확인합니다.",
            "운영 메타 스트림에서 경고, 최신성, 최신 리포트 상태를 마지막으로 점검합니다.",
        ],
    )

    selection_preview = leaderboard_frame(
        settings,
        horizon=5,
        limit=5,
        ranking_version=SELECTION_ENGINE_V2_VERSION,
    )
    sector_outlook = latest_sector_outlook_frame(
        settings,
        horizon=5,
        ranking_version=SELECTION_ENGINE_V2_VERSION,
        limit=3,
    )
    official_targets = latest_portfolio_target_book_frame(
        settings,
        execution_mode="OPEN_ALL",
        include_cash=False,
        included_only=True,
        limit=6,
    )
    alpha_promotion = latest_alpha_promotion_summary_frame(settings, limit=6)
    latest_news = latest_market_news_frame(settings, limit=6)

    _render_story_section(
        title="오늘 브리프",
        summary="시장 해석, 추천 설명, 운영 기준을 짧은 문장 흐름으로 정리했습니다.",
        items=_build_brief_items(snapshot_row, alerts, freshness, market_mood, settings),
        empty_message="오늘 브리프를 아직 만들지 못했습니다.",
    )
    _render_story_section(
        title="추천 포커스",
        summary="표 대신 진입 후보와 포트폴리오 포인트만 바로 읽히도록 압축했습니다.",
        items=_build_focus_items(selection_preview, official_targets, sector_outlook),
        empty_message="추천 후보 또는 포트폴리오 포커스가 아직 없습니다.",
    )
    _render_story_section(
        title="모델 브리프",
        summary="최근 알파 모델 비교 결과를 짧은 문장으로 보여줍니다.",
        items=_build_model_items(alpha_promotion),
        empty_message="최근 알파 모델 비교 결과가 없습니다.",
    )
    _render_story_section(
        title="운영 메타 스트림",
        summary="경고, 최신성, 최신 리포트 상태를 표 대신 스트림 형태로 배치했습니다.",
        items=_build_ops_items(alerts, freshness, latest_reports),
        empty_message="운영 메타데이터가 아직 없습니다.",
    )
    _render_story_section(
        title="시장 뉴스 라운지",
        summary="최신 뉴스와 연결 종목을 기사 카드 대신 스트림 형태로 배치했습니다.",
        items=_build_news_items(latest_news),
        empty_message="최신 뉴스가 아직 없습니다.",
    )

    st.subheader("빠른 이동")
    link_left, link_mid, link_right = st.columns(3)
    with link_left:
        _quick_link("리더보드", "leaderboard", "내일 진입 후보와 강한 흐름 후보를 바로 확인합니다.")
        _quick_link("추천 구성", "portfolio", "공식 포트폴리오 편입 방향과 목표 비중을 확인합니다.")
    with link_mid:
        _quick_link("장중 콘솔", "intraday_console", "장중 판단 흐름과 보정 레이어를 봅니다.")
        _quick_link("사후 평가", "evaluation", "최근 성과와 검증 결과를 확인합니다.")
    with link_right:
        _quick_link("운영", "ops", "배치, 경고, 리포트, 정책 상태를 봅니다.")
        _quick_link("문서 / 도움말", "docs", "운영 가이드와 용어, 복구 문서를 확인합니다.")

    if release_preview:
        with st.expander("최신 릴리즈 체크리스트 미리보기", expanded=False):
            render_report_preview(
                title="릴리즈 체크리스트",
                preview=release_preview,
            )

    render_page_footer(settings, page_name="오늘")


st.set_page_config(page_title="StockMaster", page_icon="SM", layout="wide")

NAV_SETTINGS = load_ui_base_settings(PROJECT_ROOT)
NAV_ACTIVITY = dashboard_activity_state(NAV_SETTINGS)
NAVIGATION_REGISTRY = build_navigation_registry(
    PROJECT_ROOT,
    render_today_page=render_today_page,
    allowed_page_keys=set(safe_dashboard_page_keys(PROJECT_ROOT)) if NAV_ACTIVITY.writer_active else None,
)

navigation = st.navigation(
    list(NAVIGATION_REGISTRY.values()),
    position="sidebar",
)
navigation.run()
