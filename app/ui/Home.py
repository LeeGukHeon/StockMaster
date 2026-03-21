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
    render_data_sheet,
    render_narrative_card,
    render_page_footer,
    render_report_preview,
    render_record_cards,
    render_release_candidate_summary,
    render_report_center,
    render_screen_guide,
    render_status_badges,
    render_top_actionable_badges,
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
    latest_sector_outlook_frame,
    latest_recommendation_timeline_text,
    latest_release_candidate_preview,
    latest_report_index_frame,
    latest_ui_freshness_frame,
    leaderboard_frame,
    load_ui_base_settings,
    load_ui_page_context,
    load_ui_settings,
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

    badges: list[tuple[str, str]] = [("현재 추천 모델", "INFO")]
    if snapshot_row.get("latest_daily_bundle_status"):
        badges.append(
            (
                f"일일 배치 {format_ui_value('status', snapshot_row['latest_daily_bundle_status'])}",
                str(snapshot_row["latest_daily_bundle_status"]),
            )
        )
    if snapshot_row.get("health_status"):
        badges.append(
            (
                f"운영 상태 {format_ui_value('health_status', snapshot_row['health_status'])}",
                str(snapshot_row["health_status"]),
            )
        )
    if snapshot_row.get("active_intraday_policy_id"):
        badges.append((f"장중 정책 {snapshot_row['active_intraday_policy_id']}", "INFO"))
    if snapshot_row.get("active_portfolio_policy_id"):
        badges.append((f"포트폴리오 정책 {snapshot_row['active_portfolio_policy_id']}", "INFO"))
    if snapshot_row.get("active_ops_policy_id"):
        badges.append((f"운영 정책 {snapshot_row['active_ops_policy_id']}", "INFO"))

    meta_models = _parse_json_list(snapshot_row.get("active_meta_model_ids_json"))
    if meta_models:
        badges.append((f"메타 모델 {len(meta_models)}개", "INFO"))
    return badges


def _today_narrative(snapshot_row, alerts: pd.DataFrame, freshness: pd.DataFrame, market_mood: dict[str, str]) -> str:
    if snapshot_row is None:
        return (
            "현재 기준 스냅샷이 아직 없습니다. "
            "build_latest_app_snapshot, build_report_index, build_ui_freshness_snapshot를 먼저 실행해야 합니다."
        )

    stale_count = int(freshness["stale_flag"].fillna(False).sum()) if not freshness.empty else 0
    critical_alert_count = int(snapshot_row.get("critical_alert_count") or 0)
    regime = format_ui_value("market_regime_family", snapshot_row.get("market_regime_family"))

    parts = [
        f"현재 기준일은 {format_ui_date(snapshot_row.get('as_of_date'))}입니다.",
        f"시장 분위기는 {market_mood.get('headline', regime)}이고, {market_mood.get('label', '-')} 기준입니다.",
    ]
    if critical_alert_count > 0:
        parts.append(f"치명 알림이 {critical_alert_count}건 열려 있습니다.")
    elif not alerts.empty:
        parts.append(f"열린 알림이 {len(alerts)}건 있습니다.")
    else:
        parts.append("현재 열린 치명 알림은 없습니다.")

    if stale_count > 0:
        parts.append(f"지연 데이터가 {stale_count}건 있어 일부 숫자는 보수적으로 해석해야 합니다.")
    else:
        parts.append("대시보드 신선도 경고는 현재 허용 범위 안에 있습니다.")
    return " ".join(parts)


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


def render_today_page() -> None:
    settings, activity = load_ui_page_context(
        PROJECT_ROOT,
        page_key="today",
        page_title="오늘",
    )
    inject_app_styles()
    if activity.writer_active:
        snapshot_row = _snapshot_row(settings)
        alerts = latest_alert_event_frame(settings, limit=10)
        freshness = latest_ui_freshness_frame(settings, limit=20)
        critical_freshness, warning_freshness = home_banner_freshness_levels(freshness)
        latest_reports = latest_report_index_frame(settings, limit=12, latest_only=True)
        release_preview = latest_release_candidate_preview(settings)

        st.title("오늘")
        st.caption("학습/백필 중에도 안전하게 볼 수 있는 읽기 전용 요약 화면입니다.")
        render_warning_banner(
            "WARNING",
            "현재 학습/백필 같은 쓰기 작업이 진행 중이라 일부 분석 화면은 잠시 잠겨 있습니다. "
            "이 화면은 메타데이터 기준 읽기 전용으로만 표시됩니다.",
        )
        render_screen_guide(
            summary="지금은 운영 작업과 충돌하지 않는 최소 요약만 보여줍니다.",
            bullets=[
                "상세 종목분석, 리더보드, 포트폴리오, 평가, 장중 콘솔은 작업 종료 후 다시 열 수 있습니다.",
                "작업 중에는 오늘, 문서 / 도움말 화면만 안전하게 볼 수 있습니다.",
            ],
        )
        _render_page_access_summary()
        render_status_badges(_policy_badges(snapshot_row))
        if snapshot_row is not None:
            top_left, top_mid, top_right = st.columns(3)
            top_left.metric("현재 기준일", format_ui_date(snapshot_row.get("as_of_date")))
            top_mid.metric("운영 상태", format_ui_value("health_status", snapshot_row.get("health_status")))
            top_right.metric("열린 알림", int(snapshot_row.get("warning_alert_count") or 0))
        if not critical_freshness.empty:
            render_warning_banner("CRITICAL", "일부 데이터셋 최신성이 임계치 바깥입니다.")
        elif not warning_freshness.empty:
            render_warning_banner("WARNING", "일부 데이터셋 최신성이 경고 구간입니다.")
        render_record_cards(
            alerts,
            title="중요 알림",
            primary_column="message",
            secondary_columns=["severity", "component_name"],
            detail_columns=["created_at", "alert_type", "status"],
            limit=6,
            empty_message="열린 알림이 없습니다.",
            table_expander_label="알림 전체 보기",
        )
        render_record_cards(
            freshness,
            title="최신성 점검",
            primary_column="dataset_name",
            secondary_columns=["warning_level", "page_name"],
            detail_columns=["latest_available_ts", "stale_flag"],
            limit=8,
            empty_message="최신성 스냅샷이 없습니다.",
            table_expander_label="최신성 전체 보기",
        )
        if not latest_reports.empty:
            render_data_sheet(
                latest_reports,
                title="최신 리포트",
                primary_column="report_type",
                secondary_columns=["status", "as_of_date"],
                detail_columns=["generated_ts", "published_flag"],
                limit=8,
                empty_message="최신 리포트가 없습니다.",
                table_expander_label="리포트 전체 보기",
            )
        if release_preview:
            with st.expander("최신 릴리즈 체크리스트 미리보기", expanded=False):
                render_report_preview(title="릴리즈 체크리스트", preview=release_preview)
        _quick_link("문서 / 도움말", "docs", "런북과 운영 문서를 확인합니다.")
        render_page_footer(settings, page_name="오늘")
        return

    snapshot_row = _snapshot_row(settings)
    alerts = latest_alert_event_frame(settings, limit=10)
    freshness = latest_ui_freshness_frame(settings, limit=30)
    critical_freshness, warning_freshness = home_banner_freshness_levels(freshness)
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
    latest_reports = latest_report_index_frame(settings, limit=12, latest_only=True)
    latest_news = latest_market_news_frame(settings, limit=6)
    market_mood = latest_market_mood_summary(settings)
    alpha_promotion = latest_alpha_promotion_summary_frame(settings, limit=6)
    release_preview = latest_release_candidate_preview(settings)

    st.title("오늘")
    st.caption("현재 기준 스냅샷, 활성 정책, 중요 알림, 주목 종목, 최신 리포트와 데이터 신선도를 먼저 확인하는 시작 화면입니다.")

    if snapshot_row is None:
        render_warning_banner(
            "CRITICAL",
            "현재 기준 스냅샷이 없습니다. build_latest_app_snapshot / build_report_index / build_ui_freshness_snapshot를 먼저 실행하세요.",
        )
    elif not critical_freshness.empty:
        render_warning_banner(
            "CRITICAL",
            "오늘 화면 기준으로 치명적인 지연 데이터가 있습니다. 숫자와 리포트 링크를 보수적으로 해석해야 합니다.",
        )
    elif not warning_freshness.empty:
        render_warning_banner(
            "WARNING",
            "일부 데이터셋이 경고 임계치를 넘었습니다. 최신 실행 이력과 데이터 신선도 상태를 함께 확인하세요.",
        )

    render_screen_guide(
        summary="처음 앱을 열었을 때 가장 먼저 보는 시작 화면입니다. 오늘 기준일, 열린 경고, 주목 종목, 최신 보고서를 빠르게 훑는 용도로 씁니다.",
        bullets=[
            "먼저 현재 기준일과 치명 알림 수를 확인하세요.",
            "다음으로 오늘의 주목 종목에서 선정일, 진입 예정일, 기준 종가, 참고 목표선/손절선을 같이 보세요.",
            "성과가 궁금하면 사후 평가, 운영 상태가 궁금하면 운영 화면으로 이동하면 됩니다.",
        ],
    )

    render_status_badges(_policy_badges(snapshot_row))

    top_left, top_mid, top_right = st.columns(3)
    if snapshot_row is not None:
        top_left.metric("현재 기준일", format_ui_date(snapshot_row.get("as_of_date")))
        top_mid.metric("최신 사후 평가", format_ui_date(snapshot_row.get("latest_evaluation_summary_date")))
        top_right.metric("현재 장 분위기", market_mood.get("headline", "-"))
    else:
        top_left.metric("현재 기준일", "-")
        top_mid.metric("최신 사후 평가", "-")
        top_right.metric("현재 장 분위기", market_mood.get("headline", "-"))

    bottom_left, bottom_mid, bottom_right = st.columns(3)
    if snapshot_row is not None:
        bottom_left.metric("치명 알림", int(snapshot_row.get("critical_alert_count") or 0))
        bottom_mid.metric("경고 알림", int(snapshot_row.get("warning_alert_count") or 0))
        bottom_right.metric("운영 상태", format_ui_value("health_status", snapshot_row.get("health_status")))
    else:
        bottom_left.metric("치명 알림", 0)
        bottom_mid.metric("경고 알림", 0)
        bottom_right.metric("운영 상태", "-")

    render_narrative_card("현재 장 분위기", f"{market_mood.get('headline', '-')} | {market_mood.get('label', '-')}. {market_mood.get('detail', '')}")
    render_narrative_card("현재 기준 요약", _today_narrative(snapshot_row, alerts, freshness, market_mood))
    render_narrative_card("오늘 추천 해석", latest_recommendation_timeline_text(settings))

    st.subheader("빠른 이동")
    link_left, link_mid, link_right = st.columns(3)
    with link_left:
        _quick_link("리더보드", "leaderboard", "오늘 바로 볼 종목 선별 결과와 위험 신호를 확인합니다.")
        _quick_link("추천 구성안", "portfolio", "공식 추천안, 비중 제안, 대기 종목과 제외 사유를 확인합니다.")
    with link_mid:
        _quick_link("장중 콘솔", "intraday_console", "처음 판단, 보정 후 판단, 최종 판단이 어떻게 달라졌는지 확인합니다.")
        _quick_link("사후 평가", "evaluation", "1거래일·5거래일 뒤 결과와 예측 범위 점검 상태를 확인합니다.")
    with link_right:
        _quick_link("운영", "ops", "최근 실행 이력, 알림, 정책, 리포트 상태를 점검합니다.")
        _quick_link("문서 / 도움말", "docs", "용어집, 사용자 가이드, 알려진 한계를 확인합니다.")

    home_view = st.segmented_control(
        "오늘 보기",
        options=["핵심 요약", "주목 종목", "리포트 / 상태"],
        default="핵심 요약",
    )

    if home_view == "핵심 요약":
        render_record_cards(
            alpha_promotion,
            title="알파 모델 비교 요약",
            primary_column="summary_title",
            secondary_columns=["active_model_label", "comparison_model_label"],
            detail_columns=[
                "decision_label",
                "active_top10_mean_excess_return",
                "comparison_top10_mean_excess_return",
                "promotion_gap",
                "sample_count",
                "window_end",
            ],
            limit=2,
            empty_message="아직 알파 모델 비교 기록이 없습니다.",
            table_expander_label="알파 모델 비교 원본 표 보기",
        )
        render_record_cards(
            alerts,
            title="중요 알림",
            primary_column="message",
            secondary_columns=["severity", "component_name"],
            detail_columns=["created_at", "alert_type", "status"],
            limit=5,
            empty_message="열린 알림이 없습니다.",
            table_expander_label="알림 원본 표 보기",
        )
        render_record_cards(
            freshness,
            title="신선도 점검",
            primary_column="dataset_name",
            secondary_columns=["warning_level", "page_name"],
            detail_columns=["latest_available_ts", "stale_flag"],
            limit=6,
            empty_message="화면 신선도 스냅샷이 없습니다.",
            table_expander_label="신선도 원본 표 보기",
        )
    elif home_view == "주목 종목":
        st.subheader("오늘의 주목 종목")
        st.caption("단일매수 상위 후보, 강세 예상 업종, 공식 추천안을 핵심만 나눠서 보여줍니다.")
        render_top_actionable_badges(settings)
        render_record_cards(
            sector_outlook,
            title="다음 거래일 강세 예상 업종",
            primary_column="outlook_label",
            secondary_columns=["broad_sector"],
            detail_columns=[
                "top10_count",
                "symbol_count",
                "avg_expected_excess_return",
                "sample_symbols",
            ],
            limit=3,
            empty_message="강세 예상 업종 데이터가 아직 없습니다.",
            table_expander_label="강세 예상 업종 원본 표 보기",
        )
        if not selection_preview.empty:
            render_record_cards(
                selection_preview,
                title="단일매수 상위 5종목",
                primary_column="symbol",
                secondary_columns=["company_name", "industry"],
                detail_columns=[
                    "sector",
                    "selection_date",
                    "next_entry_trade_date",
                    "selection_close_price",
                    "final_selection_value",
                    "expected_excess_return",
                    "flat_target_price",
                    "flat_upper_target_price",
                    "flat_stop_price",
                    "model_spec_id",
                    "risks",
                ],
                limit=5,
                empty_message="단일매수 상위 후보가 아직 없습니다.",
                table_expander_label="단일매수 상위 후보 원본 표 보기",
            )
        elif official_targets.empty:
            st.info("단일매수 후보와 공식 추천안이 아직 없습니다.")

        if not official_targets.empty:
            render_record_cards(
                official_targets,
                title="다음 거래일 공식 추천안",
                primary_column="symbol",
                secondary_columns=["company_name", "action_plan_label"],
                detail_columns=[
                    "sector",
                    "entry_trade_date",
                    "exit_trade_date",
                    "target_price",
                    "action_target_price",
                    "action_stretch_price",
                    "action_stop_price",
                    "plan_horizon",
                    "model_spec_id",
                ],
                limit=6,
                empty_message="공식 추천안이 아직 없습니다.",
                table_expander_label="공식 추천안 원본 표 보기",
            )
    else:
        report_left, report_right = st.columns((2, 1))
        with report_left:
            render_report_center(settings, limit=12)
        with report_right:
            render_record_cards(
                alerts,
                title="중요 알림",
                primary_column="message",
                secondary_columns=["severity", "component_name"],
                detail_columns=["created_at", "alert_type", "status"],
                limit=5,
                empty_message="열린 알림이 없습니다.",
                table_expander_label="알림 원본 표 보기",
            )
            render_record_cards(
                freshness,
                title="신선도 점검",
                primary_column="dataset_name",
                secondary_columns=["warning_level", "page_name"],
                detail_columns=["latest_available_ts", "stale_flag"],
                limit=6,
                empty_message="화면 신선도 스냅샷이 없습니다.",
                table_expander_label="신선도 원본 표 보기",
            )

        summary_left, summary_right = st.columns(2)
        with summary_left:
            render_record_cards(
                latest_news,
                title="시장 요약",
                primary_column="title",
                secondary_columns=["provider", "published_at"],
                detail_columns=["linked_symbols", "news_category"],
                limit=5,
                empty_message="시장 뉴스 메타데이터가 없습니다.",
                table_expander_label="시장 뉴스 원본 표 보기",
            )
        with summary_right:
            st.subheader("릴리스 점검 상태")
            render_release_candidate_summary(settings, limit=8)
            if release_preview:
                with st.expander("최신 릴리스 점검표 미리보기", expanded=False):
                    render_report_preview(
                        title="릴리스 체크 미리보기",
                        preview=release_preview,
                    )

        if not latest_reports.empty:
            render_data_sheet(
                latest_reports,
                title="최신 리포트",
                primary_column="report_type",
                secondary_columns=["status", "as_of_date"],
                detail_columns=["generated_ts", "published_flag"],
                limit=8,
                empty_message="최신 리포트가 없습니다.",
                table_expander_label="최신 리포트 전체 표 보기",
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
