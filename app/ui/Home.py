# ruff: noqa: E402, E501

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ui.components import (
    inject_app_styles,
    render_narrative_card,
    render_page_footer,
    render_release_candidate_summary,
    render_report_center,
    render_status_badges,
    render_top_actionable_badges,
    render_warning_banner,
)
from app.ui.helpers import (
    latest_alert_event_frame,
    latest_app_snapshot_frame,
    latest_market_news_frame,
    latest_release_candidate_preview,
    latest_report_index_frame,
    latest_ui_freshness_frame,
    leaderboard_frame,
    load_ui_settings,
    localize_frame,
)
from app.ui.navigation import build_navigation_registry


def _snapshot_row(settings):
    frame = latest_app_snapshot_frame(settings)
    if frame.empty:
        return None
    return frame.iloc[0]


def _parse_json_list(raw_value: object) -> list[dict[str, object]]:
    if raw_value in (None, "", "[]"):
        return []
    try:
        parsed = json.loads(str(raw_value))
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _policy_badges(snapshot_row) -> list[tuple[str, str]]:
    if snapshot_row is None:
        return []
    badges: list[tuple[str, str]] = [("Selection v2", "INFO")]
    if snapshot_row.get("latest_daily_bundle_status"):
        badges.append((f"데일리 번들 {snapshot_row['latest_daily_bundle_status']}", str(snapshot_row["latest_daily_bundle_status"])))
    if snapshot_row.get("health_status"):
        badges.append((f"헬스 {snapshot_row['health_status']}", str(snapshot_row["health_status"])))
    if snapshot_row.get("active_intraday_policy_id"):
        badges.append((f"타이밍 정책 {snapshot_row['active_intraday_policy_id']}", "INFO"))
    if snapshot_row.get("active_portfolio_policy_id"):
        badges.append((f"포트폴리오 정책 {snapshot_row['active_portfolio_policy_id']}", "INFO"))
    if snapshot_row.get("active_ops_policy_id"):
        badges.append((f"운영 정책 {snapshot_row['active_ops_policy_id']}", "INFO"))
    meta_models = _parse_json_list(snapshot_row.get("active_meta_model_ids_json"))
    if meta_models:
        badges.append((f"메타 모델 {len(meta_models)}개", "INFO"))
    return badges


def _today_narrative(snapshot_row, alerts: pd.DataFrame, freshness: pd.DataFrame) -> str:
    if snapshot_row is None:
        return (
            "Current truth snapshot이 아직 없습니다. "
            "build_latest_app_snapshot, build_report_index, build_ui_freshness_snapshot을 먼저 실행해야 합니다."
        )
    stale_count = int(freshness["stale_flag"].fillna(False).sum()) if not freshness.empty else 0
    critical_alert_count = int(snapshot_row.get("critical_alert_count") or 0)
    regime = snapshot_row.get("market_regime_family") or "unknown"
    parts = [
        f"현재 기준일은 {snapshot_row.get('as_of_date') or 'unknown'} 입니다.",
        f"시장 regime은 {regime} 입니다.",
    ]
    if critical_alert_count > 0:
        parts.append(f"치명 알림 {critical_alert_count}건이 열려 있습니다.")
    elif not alerts.empty:
        parts.append(f"열린 알림 {len(alerts)}건이 있습니다.")
    else:
        parts.append("현재 열린 치명 알림은 없습니다.")
    if stale_count > 0:
        parts.append(f"stale dataset {stale_count}건이 있어 일부 숫자는 보수적으로 해석해야 합니다.")
    else:
        parts.append("핵심 화면의 freshness 경고는 현재 허용 범위 안입니다.")
    return " ".join(parts)


def _quick_link(label: str, page_key: str, description: str) -> None:
    st.markdown(f"**{label}**")
    st.caption(description)
    page = NAVIGATION_REGISTRY.get(page_key)
    if page is None:
        st.caption("페이지 연결 정보를 찾지 못했습니다.")
        return
    st.page_link(page, label=f"{label} 열기", icon=":material/open_in_new:")


def render_today_page() -> None:
    settings = load_ui_settings(PROJECT_ROOT)
    inject_app_styles()

    snapshot_row = _snapshot_row(settings)
    alerts = latest_alert_event_frame(settings, limit=10)
    freshness = latest_ui_freshness_frame(settings, limit=30)
    critical_freshness = freshness[freshness["warning_level"].astype(str).str.upper() == "CRITICAL"]
    warning_freshness = freshness[freshness["warning_level"].astype(str).str.upper() == "WARNING"]
    selection_preview = leaderboard_frame(
        settings,
        horizon=5,
        limit=12,
        ranking_version=SELECTION_ENGINE_V2_VERSION,
    )
    latest_reports = latest_report_index_frame(settings, limit=12, latest_only=True)
    latest_news = latest_market_news_frame(settings, limit=6)
    release_preview = latest_release_candidate_preview(settings)

    st.title("오늘")
    st.caption(
        "현재 truth snapshot, active policy, 핵심 알림, top actionable names, 최신 리포트와 freshness를 먼저 확인하는 시작 화면입니다."
    )

    if snapshot_row is None:
        render_warning_banner(
            "CRITICAL",
            "latest app snapshot이 없습니다. build_latest_app_snapshot / build_report_index / build_ui_freshness_snapshot을 먼저 실행하세요.",
        )
    elif not critical_freshness.empty:
        render_warning_banner(
            "CRITICAL",
            "오늘 화면 기준 critical stale dataset이 있습니다. 숫자와 리포트 링크를 보수적으로 해석해야 합니다.",
        )
    elif not warning_freshness.empty:
        render_warning_banner(
            "WARNING",
            "일부 데이터셋이 warning 임계치를 넘었습니다. 최신 run과 freshness 상태를 함께 확인하세요.",
        )

    render_status_badges(_policy_badges(snapshot_row))

    top_left, top_mid, top_right = st.columns(3)
    if snapshot_row is not None:
        top_left.metric("현재 기준일", str(snapshot_row.get("as_of_date") or "-"))
        top_mid.metric("최신 평가일", str(snapshot_row.get("latest_evaluation_date") or "-"))
        top_right.metric("최신 장중 세션", str(snapshot_row.get("latest_intraday_session_date") or "-"))
    else:
        top_left.metric("현재 기준일", "-")
        top_mid.metric("최신 평가일", "-")
        top_right.metric("최신 장중 세션", "-")

    bottom_left, bottom_mid, bottom_right = st.columns(3)
    if snapshot_row is not None:
        bottom_left.metric("치명 알림", int(snapshot_row.get("critical_alert_count") or 0))
        bottom_mid.metric("경고 알림", int(snapshot_row.get("warning_alert_count") or 0))
        bottom_right.metric("최신 리포트 번들", str(snapshot_row.get("latest_report_bundle_id") or "-"))
    else:
        bottom_left.metric("치명 알림", 0)
        bottom_mid.metric("경고 알림", 0)
        bottom_right.metric("최신 리포트 번들", "-")

    render_narrative_card("Current Truth Summary", _today_narrative(snapshot_row, alerts, freshness))

    link_left, link_mid, link_right = st.columns(3)
    with link_left:
        _quick_link("리더보드", "leaderboard", "오늘 바로 볼 종목 선별 결과와 위험 플래그를 확인합니다.")
        _quick_link("포트폴리오", "portfolio", "목표 보유, 리밸런스, 현금 비중과 제약 사유를 봅니다.")
    with link_mid:
        _quick_link("장중 콘솔", "intraday_console", "raw/adjusted/final action과 stale 경고를 확인합니다.")
        _quick_link("사후 평가", "evaluation", "D+1/D+5 성숙 결과와 calibration을 점검합니다.")
    with link_right:
        _quick_link("운영", "ops", "최근 run, 알림, 정책, 보고서 상태를 점검합니다.")
        _quick_link("문서 / 도움말", "docs", "용어집, 사용자 가이드, known limitations를 봅니다.")

    actionable_left, actionable_right = st.columns((2, 1))
    with actionable_left:
        st.subheader("Top Actionable Names")
        render_top_actionable_badges(settings)
        if selection_preview.empty:
            st.info("Selection v2 preview가 없습니다.")
        else:
            columns = [
                "symbol",
                "company_name",
                "grade",
                "final_selection_value",
                "expected_excess_return",
                "uncertainty_score",
                "disagreement_score",
                "implementation_penalty_score",
                "flow_score",
                "lower_band",
                "upper_band",
                "risks",
            ]
            display = selection_preview[[column for column in columns if column in selection_preview.columns]].copy()
            st.dataframe(localize_frame(display), width="stretch", hide_index=True)
    with actionable_right:
        st.subheader("Critical Alerts")
        if alerts.empty:
            st.success("열린 알림이 없습니다.")
        else:
            display = alerts[
                ["created_at", "alert_type", "severity", "component_name", "message", "status"]
            ].copy()
            st.dataframe(localize_frame(display), width="stretch", hide_index=True)

    report_left, report_right = st.columns((2, 1))
    with report_left:
        st.subheader("Canonical Report Center")
        render_report_center(settings, limit=12)
    with report_right:
        st.subheader("Freshness Watch")
        if freshness.empty:
            st.info("UI freshness snapshot이 없습니다.")
        else:
            display = freshness[
                [
                    "page_name",
                    "dataset_name",
                    "warning_level",
                    "stale_flag",
                    "latest_available_ts",
                ]
            ].copy()
            st.dataframe(localize_frame(display), width="stretch", hide_index=True)

    summary_left, summary_right = st.columns(2)
    with summary_left:
        st.subheader("Market Narrative")
        if latest_news.empty:
            st.info("시장 뉴스 메타데이터가 없습니다.")
        else:
            st.dataframe(localize_frame(latest_news), width="stretch", hide_index=True)
    with summary_right:
        st.subheader("Release Candidate Status")
        render_release_candidate_summary(settings, limit=8)
        if release_preview:
            with st.expander("최신 릴리즈 체크리스트 미리보기", expanded=False):
                st.code(release_preview)

    if not latest_reports.empty:
        st.subheader("Latest Reports")
        display = latest_reports[
            ["report_type", "as_of_date", "generated_ts", "status", "artifact_path"]
        ].copy()
        st.dataframe(localize_frame(display), width="stretch", hide_index=True)

    render_page_footer(settings, page_name="오늘")


st.set_page_config(page_title="StockMaster", page_icon="SM", layout="wide")

NAVIGATION_REGISTRY = build_navigation_registry(
    PROJECT_ROOT,
    render_today_page=render_today_page,
)

navigation = st.navigation(
    list(NAVIGATION_REGISTRY.values()),
    position="sidebar",
)
navigation.run()
