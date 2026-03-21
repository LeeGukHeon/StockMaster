# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import (
    render_page_footer,
    render_page_header,
    render_record_cards,
    render_report_preview,
    render_screen_guide,
    render_story_stream,
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
    load_ui_page_context,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="portfolio",
    page_title="추천 구성",
)


def _display_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def _build_official_items(active_policy: pd.DataFrame, target_book: pd.DataFrame, rebalance: pd.DataFrame, execution_mode: str) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    if not active_policy.empty:
        row = active_policy.iloc[0]
        items.append(
            {
                "eyebrow": "Policy",
                "title": _display_text(row.get("active_portfolio_policy_id")),
                "body": f"실행 방식 {format_execution_mode_label(execution_mode)} / {_display_text(row.get('display_name'))}",
                "meta": f"promotion {_display_text(row.get('promotion_type'))} · {_display_text(row.get('effective_from_date'))}",
                "badge": "ACTIVE",
                "tone": "positive",
            }
        )
    for row in target_book.head(3).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Target Book",
                "title": f"{_display_text(row.get('symbol'))} · {_display_text(row.get('company_name'))}",
                "body": (
                    f"{_display_text(row.get('action_plan_label'))} / "
                    f"비중 {_display_text(row.get('target_weight'))} / "
                    f"목표가 {_display_text(row.get('target_price'))}"
                ),
                "meta": f"진입일 {_display_text(row.get('entry_trade_date'))} · gate {_display_text(row.get('gate_status'))}",
                "badge": _display_text(row.get("market")),
                "tone": "accent",
            }
        )
    for row in rebalance.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Rebalance",
                "title": f"{_display_text(row.get('symbol'))} · {_display_text(row.get('rebalance_action'))}",
                "body": f"delta shares {_display_text(row.get('delta_shares'))} / cash {_display_text(row.get('cash_delta'))}",
                "meta": _display_text(row.get("blocked_reason")),
                "badge": _display_text(row.get("gate_status"), "PLAN"),
                "tone": "warning",
            }
        )
    return items


def _build_queue_items(candidate_book: pd.DataFrame, waitlist: pd.DataFrame, constraints: pd.DataFrame) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in candidate_book.head(3).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Candidate",
                "title": f"{_display_text(row.get('symbol'))} · {_display_text(row.get('company_name'))}",
                "body": (
                    f"state {_display_text(row.get('candidate_state'))} / "
                    f"conviction {_display_text(row.get('risk_scaled_conviction'))}"
                ),
                "meta": f"timing {_display_text(row.get('timing_action'))} · gate {_display_text(row.get('timing_gate_status'))}",
                "badge": _display_text(row.get("candidate_rank"), "QUEUE"),
                "tone": "neutral",
            }
        )
    for row in waitlist.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Waitlist",
                "title": f"{_display_text(row.get('symbol'))} · {_display_text(row.get('company_name'))}",
                "body": f"gate {_display_text(row.get('gate_status'))} / blocked {_display_text(row.get('blocked_flag'))}",
                "meta": _display_text(row.get("blocked_reason")),
                "badge": _display_text(row.get("waitlist_rank"), "WAIT"),
                "tone": "warning",
            }
        )
    for row in constraints.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Constraint",
                "title": _display_text(row.get("constraint_type")),
                "body": _display_text(row.get("message")),
                "meta": f"severity {_display_text(row.get('severity'))} · affected {_display_text(row.get('affected_symbol_count'))}",
                "badge": _display_text(row.get("severity")),
                "tone": str(row.get("severity", "warning")).lower(),
            }
        )
    return items


def _build_outcome_items(nav_frame: pd.DataFrame) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in nav_frame.head(4).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "NAV",
                "title": f"{_display_text(row.get('snapshot_date'))} · {_display_text(row.get('execution_mode'))}",
                "body": (
                    f"NAV {_display_text(row.get('nav'))} / "
                    f"cash {_display_text(row.get('cash_weight'))} / "
                    f"holding {_display_text(row.get('holding_count'))}"
                ),
                "meta": f"turnover {_display_text(row.get('turnover'))}",
                "badge": "RESULT",
                "tone": "neutral",
            }
        )
    return items


render_page_header(
    settings,
    page_name="추천 구성",
    title="추천 구성",
    description="다음 거래일 포트폴리오를 표보다 먼저 서술형 브리프로 읽고, 필요할 때만 원본 책자를 펼치는 구조입니다.",
)
render_screen_guide(
    summary="공식 편입안, 후보/대기열, 결과 흐름을 따로 읽되 기본 화면은 이야기형 요약으로 정리했습니다.",
    bullets=[
        "공식 편입안에서 target book과 rebalance 계획을 먼저 읽습니다.",
        "후보/대기열에서 왜 아직 못 들어왔는지 gate와 blocked reason을 봅니다.",
        "결과에서는 최근 NAV 흐름과 리포트만 요약해서 읽습니다.",
    ],
)

execution_mode = st.segmented_control(
    "실행 방식",
    options=["OPEN_ALL", "TIMING_ASSISTED"],
    default="TIMING_ASSISTED",
    format_func=format_execution_mode_label,
)

active_policy = latest_portfolio_policy_registry_frame(settings, active_only=True, limit=10)
candidate_book = latest_portfolio_candidate_frame(settings, execution_mode=execution_mode, limit=30)
target_book = latest_portfolio_target_book_frame(
    settings,
    execution_mode=execution_mode,
    included_only=True,
    limit=30,
)
waitlist = latest_portfolio_waitlist_frame(settings, execution_mode=execution_mode, limit=20)
rebalance = latest_portfolio_rebalance_plan_frame(settings, execution_mode=execution_mode, limit=30)
constraints = latest_portfolio_constraint_frame(settings, limit=20)
nav_frame = latest_portfolio_nav_frame(settings, limit=20)
report_preview = latest_portfolio_report_preview(settings)

render_story_stream(
    title="추천 기준 브리프",
    summary="selection 결과가 어떻게 target book으로 이어지는지 한 문단으로 읽습니다.",
    items=[
        {
            "eyebrow": "Guide",
            "title": "추천 구성 읽는 법",
            "body": latest_recommendation_timeline_text(settings),
            "meta": format_execution_mode_label(execution_mode),
            "badge": "GUIDE",
            "tone": "accent",
        }
    ],
    empty_message="추천 기준 브리프가 없습니다.",
)

tab = st.segmented_control(
    "구성 보기",
    options=["공식 편입안", "후보/대기열", "결과"],
    default="공식 편입안",
)

if tab == "공식 편입안":
    render_story_stream(
        title="공식 편입안",
        summary="active portfolio policy와 target book, rebalance 계획을 바로 읽습니다.",
        items=_build_official_items(active_policy, target_book, rebalance, execution_mode),
        empty_message="공식 편입안 데이터가 없습니다.",
    )
    with st.expander("공식 편입안 원본 보기", expanded=False):
        render_record_cards(
            active_policy,
            title="active portfolio policy 원본",
            primary_column="active_portfolio_policy_id",
            secondary_columns=["display_name"],
            detail_columns=["promotion_type", "effective_from_date", "note"],
            limit=3,
            empty_message="active portfolio policy 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            target_book,
            title="target book 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "action_plan_label"],
            detail_columns=["target_rank", "target_weight", "target_price", "gate_status", "model_spec_id"],
            limit=8,
            empty_message="target book 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            rebalance,
            title="rebalance 원본",
            primary_column="symbol",
            secondary_columns=["rebalance_action", "gate_status"],
            detail_columns=["delta_shares", "cash_delta", "blocked_reason"],
            limit=8,
            empty_message="rebalance 원본이 없습니다.",
            show_table_expander=False,
        )

elif tab == "후보/대기열":
    render_story_stream(
        title="후보/대기열",
        summary="왜 아직 후보인지, 왜 보류됐는지, 어떤 제약이 걸렸는지를 먼저 읽습니다.",
        items=_build_queue_items(candidate_book, waitlist, constraints),
        empty_message="후보/대기열 데이터가 없습니다.",
    )
    with st.expander("후보/대기열 원본 보기", expanded=False):
        render_record_cards(
            candidate_book,
            title="candidate 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "candidate_state"],
            detail_columns=["candidate_rank", "effective_alpha_long", "risk_scaled_conviction", "timing_gate_status"],
            limit=8,
            empty_message="candidate 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            waitlist,
            title="waitlist 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "gate_status"],
            detail_columns=["waitlist_rank", "blocked_flag", "blocked_reason"],
            limit=8,
            empty_message="waitlist 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            constraints,
            title="constraint 원본",
            primary_column="constraint_type",
            secondary_columns=["severity"],
            detail_columns=["affected_symbol_count", "message"],
            limit=8,
            empty_message="constraint 원본이 없습니다.",
            show_table_expander=False,
        )

else:
    render_story_stream(
        title="결과 흐름",
        summary="최근 NAV와 실행 모드 흐름을 요약해서 먼저 보여줍니다.",
        items=_build_outcome_items(nav_frame),
        empty_message="결과 흐름 데이터가 없습니다.",
    )
    if report_preview:
        with st.expander("최신 포트폴리오 리포트 미리보기", expanded=False):
            render_report_preview(
                title="포트폴리오 리포트",
                preview=report_preview,
            )
    with st.expander("결과 원본 보기", expanded=False):
        render_record_cards(
            nav_frame,
            title="NAV 원본",
            primary_column="snapshot_date",
            secondary_columns=["execution_mode"],
            detail_columns=["nav", "cash_weight", "holding_count", "turnover"],
            limit=8,
            empty_message="NAV 원본이 없습니다.",
            show_table_expander=False,
        )

render_page_footer(
    settings,
    page_name="추천 구성",
    extra_items=[f"실행 방식: {format_execution_mode_label(execution_mode)}"],
)
