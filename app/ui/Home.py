# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import render_story_stream
from app.ui.dashboard_v2 import (
    DASHBOARD_DEFAULT_PICK_HORIZON,
    dashboard_snapshot_note,
    display_number,
    display_percent,
    display_text,
    display_value,
    filter_dashboard_leaderboard,
    load_dashboard_v2_context,
    read_dashboard_frame,
    render_dashboard_v2_empty,
    render_dashboard_v2_footer,
    render_dashboard_v2_header,
    recommendation_timeline_note,
)
from app.ui.navigation import build_navigation_registry, safe_dashboard_page_keys


def _quick_link(label: str, page_key: str, description: str) -> None:
    st.markdown(f"**{label}**")
    st.caption(description)
    page = NAVIGATION_REGISTRY.get(page_key)
    if page is None:
        st.caption("페이지 연결 정보를 찾지 못했습니다.")
        return
    st.page_link(page, label=f"{label} 열기", icon=":material/open_in_new:")


def render_today_page() -> None:
    settings, activity, manifest = load_dashboard_v2_context(PROJECT_ROOT)
    leaderboard = read_dashboard_frame(settings, "leaderboard")
    summary_frame = read_dashboard_frame(settings, "stock_workbench_summary")
    live_frame = read_dashboard_frame(settings, "stock_workbench_live_recommendation")
    alpha_promotion = read_dashboard_frame(settings, "alpha_promotion_summary")
    policy_eval = read_dashboard_frame(settings, "intraday_policy_evaluation_latest")

    render_dashboard_v2_header(
        title="핵심만 보는 새 대시보드",
        description="추천 종목, 즉석 종목 전망, 주간 보고만 남기고 나머지는 콘솔로 분리한 새 시작 화면입니다.",
        settings=settings,
        activity=activity,
        manifest=manifest,
    )

    if leaderboard.empty and summary_frame.empty:
        render_dashboard_v2_empty("Dashboard v2용 스냅샷이 아직 준비되지 않았습니다.")
        render_dashboard_v2_footer(settings, manifest=manifest, page_name="대시보드")
        return

    filtered_leaderboard = filter_dashboard_leaderboard(
        leaderboard,
        horizon=DASHBOARD_DEFAULT_PICK_HORIZON,
    )

    picks_items = []
    for row in filtered_leaderboard.head(3).to_dict(orient="records"):
        picks_items.append(
            {
                "eyebrow": display_text(row.get("market")),
                "title": f"{display_text(row.get('symbol'))} · {display_text(row.get('company_name'))}",
                "body": (
                    f"등급 {display_text(row.get('grade'))} / "
                    f"예상 초과수익률 {display_percent(row.get('expected_excess_return'), signed=True)} / "
                    f"진입 예정일 {display_text(row.get('next_entry_trade_date'))}"
                ),
                "meta": f"선정 점수 {display_number(row.get('final_selection_value'))}",
                "badge": display_text(row.get("grade"), "추천"),
                "tone": "positive",
            }
        )

    outlook_items = []
    for row in live_frame.head(2).to_dict(orient="records"):
        outlook_items.append(
            {
                "eyebrow": "즉석 전망",
                "title": f"{display_text(row.get('symbol'))} · {display_text(row.get('company_name'))}",
                "body": (
                    f"D1 {display_text(row.get('live_d1_selection_v2_grade'))} / "
                    f"D5 {display_text(row.get('live_d5_selection_v2_grade'))} / "
                    f"예상 초과수익률 {display_percent(row.get('live_d5_expected_excess_return'), signed=True)}"
                ),
                "meta": f"기준가 {display_number(row.get('live_reference_price'))}",
                "badge": display_text(row.get("live_d5_selection_v2_grade"), "전망"),
                "tone": "positive",
            }
        )
    if not outlook_items:
        for row in summary_frame.head(2).to_dict(orient="records"):
            outlook_items.append(
                {
                    "eyebrow": "저장 스냅샷",
                    "title": f"{display_text(row.get('symbol'))} · {display_text(row.get('company_name'))}",
                    "body": (
                        f"5일 수익률 {display_percent(row.get('ret_5d'), signed=True)} / "
                        f"20일 수익률 {display_percent(row.get('ret_20d'), signed=True)} / "
                        f"D5 알파 기대수익 {display_percent(row.get('d5_alpha_expected_excess_return'), signed=True)}"
                    ),
                    "meta": f"최근 3일 뉴스 {display_number(row.get('news_count_3d'))}건",
                    "badge": display_text(row.get("d5_selection_v2_grade"), "스냅샷"),
                    "tone": "accent",
                }
            )

    weekly_items = []
    for row in alpha_promotion.head(2).to_dict(orient="records"):
        weekly_items.append(
            {
                "eyebrow": "알파 비교",
                "title": f"{display_text(row.get('active_model_label'))} vs {display_text(row.get('comparison_model_label'))}",
                "body": (
                    f"{display_text(row.get('decision_label'))} / "
                    f"격차 {display_percent(row.get('promotion_gap'), signed=True, percent_points=True)}"
                ),
                "meta": f"표본 {display_number(row.get('sample_count'))}개",
                "badge": display_text(row.get("decision_label"), "알파"),
                "tone": "neutral",
            }
        )
    for row in policy_eval.head(2).to_dict(orient="records"):
        weekly_items.append(
            {
                "eyebrow": "정책 평가",
                "title": display_value("template_id", row.get("template_id")),
                "body": (
                    f"목표 점수 {display_number(row.get('objective_score'))} / "
                    f"적중률 {display_percent(row.get('hit_rate'))} / "
                    f"평가 세션 {display_number(row.get('test_session_count'))}회"
                ),
                "meta": f"{display_value('scope_type', row.get('scope_type'))} · D+{display_text(row.get('horizon'))}",
                "badge": "주간 보고",
                "tone": "warning",
            }
        )

    render_story_stream(
        title="추천 종목 한눈에 보기",
        summary=recommendation_timeline_note(settings),
        items=picks_items,
        empty_message="추천 종목 데이터가 없습니다.",
    )
    render_story_stream(
        title="즉석 종목 전망",
        summary=dashboard_snapshot_note(manifest),
        items=outlook_items,
        empty_message="즉석 종목 전망 데이터가 없습니다.",
    )
    render_story_stream(
        title="주간 캘리브레이션 / 정책 보고",
        summary="이번 스냅샷에 담긴 모델·정책 변화만 간단히 보여줍니다.",
        items=weekly_items,
        empty_message="주간 보고 데이터가 없습니다.",
    )

    st.subheader("바로 가기")
    left, mid, right = st.columns(3)
    with left:
        _quick_link("추천 종목", "picks", "내일 바로 볼 상위 후보와 섹터 흐름을 봅니다.")
        _quick_link("즉석 종목 전망", "outlook", "종목 하나를 눌러 최신 스냅샷 전망을 봅니다.")
    with mid:
        _quick_link("주간 보고", "weekly_report", "주간 캘리브레이션과 정책 보고만 모아 봅니다.")
        _quick_link("운영 콘솔", "ops_console", "운영 로그와 세부 상태는 여기서 확인합니다.")
    with right:
        _quick_link("리서치 콘솔", "research_console", "장중 흐름과 연구 화면은 별도 콘솔로 봅니다.")
        _quick_link("문서 / 도움말", "docs", "설명서와 복구 문서를 확인합니다.")

    render_dashboard_v2_footer(settings, manifest=manifest, page_name="대시보드")


SETTINGS, ACTIVITY, _MANIFEST = load_dashboard_v2_context(PROJECT_ROOT)
NAVIGATION_REGISTRY = build_navigation_registry(
    PROJECT_ROOT,
    render_today_page=render_today_page,
    allowed_page_keys=set(safe_dashboard_page_keys(PROJECT_ROOT)) if ACTIVITY.writer_active else None,
)

st.set_page_config(page_title="StockMaster", page_icon="SM", layout="wide")
navigation = st.navigation(list(NAVIGATION_REGISTRY.values()), position="sidebar")
navigation.run()
