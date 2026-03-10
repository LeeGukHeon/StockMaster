# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_narrative_card,
    render_page_footer,
    render_page_header,
    render_record_cards,
)
from app.ui.helpers import (
    format_execution_mode_label,
    latest_portfolio_candidate_frame,
    latest_portfolio_constraint_frame,
    latest_portfolio_nav_frame,
    latest_portfolio_policy_registry_frame,
    latest_portfolio_rebalance_plan_frame,
    latest_portfolio_report_preview,
    latest_portfolio_target_book_frame,
    latest_portfolio_waitlist_frame,
    latest_recommendation_timeline_text,
    load_ui_settings,
)

settings = load_ui_settings(PROJECT_ROOT)

execution_mode = st.selectbox(
    "실행 모드",
    options=["OPEN_ALL", "TIMING_ASSISTED"],
    index=1,
    format_func=format_execution_mode_label,
)

active_policy = latest_portfolio_policy_registry_frame(settings, active_only=True, limit=10)
candidate_book = latest_portfolio_candidate_frame(settings, execution_mode=execution_mode, limit=30)
target_book = latest_portfolio_target_book_frame(settings, execution_mode=execution_mode, limit=30)
waitlist = latest_portfolio_waitlist_frame(settings, execution_mode=execution_mode, limit=20)
rebalance = latest_portfolio_rebalance_plan_frame(settings, execution_mode=execution_mode, limit=30)
constraints = latest_portfolio_constraint_frame(settings, limit=20)
nav_frame = latest_portfolio_nav_frame(settings, limit=20)
report_preview = latest_portfolio_report_preview(settings)

render_page_header(
    settings,
    page_name="포트폴리오",
    title="포트폴리오",
    description="오늘 추천을 실제 보유안으로 어떻게 옮길지, 목표 비중과 리밸런스 계획 중심으로 확인합니다.",
)

if active_policy.empty:
    render_narrative_card(
        "포트폴리오 요약",
        "활성 포트폴리오 정책은 없지만, 현재 실행 기준으로 후보와 목표 보유안을 미리 볼 수 있습니다.",
    )
else:
    row = active_policy.iloc[0]
    render_narrative_card(
        "포트폴리오 요약",
        (
            f"현재 활성 정책은 {row.get('active_portfolio_policy_id', '-')}이고, "
            f"실행 모드는 {format_execution_mode_label(execution_mode)}입니다. "
            "타이밍 보조는 신규 진입과 추가 매수에만 선택적으로 반영합니다."
        ),
    )

render_narrative_card("추천 기준 안내", latest_recommendation_timeline_text(settings))

render_record_cards(
    active_policy,
    title="활성 포트폴리오 정책",
    primary_column="active_portfolio_policy_id",
    secondary_columns=["display_name"],
    detail_columns=["promotion_type", "effective_from_date", "note"],
    limit=3,
    empty_message="활성 포트폴리오 정책이 아직 없습니다.",
    table_expander_label="포트폴리오 정책 원본 표 보기",
)

render_record_cards(
    target_book,
    title="목표 보유안",
    primary_column="symbol",
    secondary_columns=["company_name", "candidate_state"],
    detail_columns=[
        "target_rank",
        "target_weight",
        "target_notional",
        "target_shares",
        "gate_status",
    ],
    limit=8,
    empty_message="목표 보유안 데이터가 없습니다.",
    table_expander_label="목표 보유안 원본 표 보기",
)

render_record_cards(
    rebalance,
    title="리밸런스 계획",
    primary_column="symbol",
    secondary_columns=["rebalance_action", "gate_status"],
    detail_columns=["delta_shares", "cash_delta", "blocked_reason"],
    limit=8,
    empty_message="리밸런스 계획이 없습니다.",
    table_expander_label="리밸런스 원본 표 보기",
)

render_record_cards(
    candidate_book,
    title="후보 종목",
    primary_column="symbol",
    secondary_columns=["company_name", "candidate_state"],
    detail_columns=[
        "candidate_rank",
        "effective_alpha_long",
        "risk_scaled_conviction",
        "timing_action",
        "timing_gate_status",
    ],
    limit=8,
    empty_message="후보 종목 데이터가 없습니다.",
    table_expander_label="후보 종목 원본 표 보기",
)

render_record_cards(
    waitlist,
    title="대기 / 차단 종목",
    primary_column="symbol",
    secondary_columns=["company_name", "gate_status"],
    detail_columns=["waitlist_rank", "blocked_flag", "blocked_reason"],
    limit=8,
    empty_message="대기 또는 차단 종목이 없습니다.",
    table_expander_label="대기 / 차단 원본 표 보기",
)

render_record_cards(
    constraints,
    title="제약 이벤트",
    primary_column="constraint_type",
    secondary_columns=["severity"],
    detail_columns=["affected_symbol_count", "message"],
    limit=8,
    empty_message="기록된 제약 이벤트가 없습니다.",
    table_expander_label="제약 이벤트 원본 표 보기",
)

render_record_cards(
    nav_frame,
    title="최근 순자산가치 요약",
    primary_column="snapshot_date",
    secondary_columns=["execution_mode"],
    detail_columns=["nav", "cash_weight", "holding_count", "turnover"],
    limit=6,
    empty_message="순자산가치 스냅샷이 없습니다.",
    table_expander_label="NAV 원본 표 보기",
)

if report_preview:
    with st.expander("최신 포트폴리오 리포트 미리보기", expanded=False):
        st.code(report_preview)

render_page_footer(
    settings,
    page_name="포트폴리오",
    extra_items=[f"실행 모드: {format_execution_mode_label(execution_mode)}"],
)
