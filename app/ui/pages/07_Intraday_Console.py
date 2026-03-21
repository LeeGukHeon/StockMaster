# ruff: noqa: E402

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
    render_warning_banner,
)
from app.ui.helpers import (
    intraday_console_adjusted_decision_frame,
    intraday_console_candidate_frame,
    intraday_console_decision_frame,
    intraday_console_market_context_frame,
    intraday_console_signal_frame,
    intraday_console_strategy_trace_frame,
    intraday_console_timing_frame,
    intraday_console_tuned_action_frame,
    latest_intraday_active_policy_frame,
    latest_intraday_checkpoint_health_frame,
    latest_intraday_console_basis_summary,
    latest_intraday_decision_lineage_frame,
    latest_intraday_meta_active_model_frame,
    latest_intraday_meta_decision_frame,
    latest_intraday_meta_prediction_frame,
    latest_intraday_policy_recommendation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_postmortem_preview,
    latest_intraday_research_capability_frame,
    latest_intraday_status_frame,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_summary_report_preview,
    load_ui_base_settings,
    load_ui_page_context,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="intraday_console",
    page_title="장중 콘솔",
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


def _safe_int(value: object) -> int:
    if value is None or pd.isna(value):
        return 0
    return int(float(value))


@st.cache_data(ttl=30, show_spinner=False)
def _load_intraday_console_section(project_root_str: str, section: str) -> dict[str, object]:
    local_settings = load_ui_base_settings(Path(project_root_str))
    if section == "개요":
        return {
            "status_frame": latest_intraday_status_frame(local_settings),
            "basis_summary": latest_intraday_console_basis_summary(local_settings),
            "capability_frame": latest_intraday_research_capability_frame(local_settings, limit=20),
            "checkpoint_health": latest_intraday_checkpoint_health_frame(local_settings),
            "market_context": intraday_console_market_context_frame(local_settings, limit=12),
            "active_policy_frame": latest_intraday_active_policy_frame(local_settings, limit=20),
            "active_meta_model_frame": latest_intraday_meta_active_model_frame(local_settings, limit=20),
            "recommendation_frame": latest_intraday_policy_recommendation_frame(local_settings, limit=20),
        }
    if section == "판단 흐름":
        return {
            "candidate_frame": intraday_console_candidate_frame(local_settings, limit=20),
            "signal_frame": intraday_console_signal_frame(local_settings, limit=20),
            "decision_frame": intraday_console_decision_frame(local_settings, limit=20),
            "adjusted_decision_frame": intraday_console_adjusted_decision_frame(local_settings, limit=20),
            "meta_prediction_frame": latest_intraday_meta_prediction_frame(local_settings, limit=20),
            "meta_decision_frame": latest_intraday_meta_decision_frame(local_settings, limit=20),
            "tuned_decision_frame": intraday_console_tuned_action_frame(local_settings, limit=20),
        }
    if section == "결과 비교":
        return {
            "timing_frame": intraday_console_timing_frame(local_settings, limit=20),
            "strategy_trace_frame": intraday_console_strategy_trace_frame(local_settings, limit=20),
            "same_exit_frame": latest_intraday_strategy_comparison_frame(local_settings, limit=12),
            "lineage_frame": latest_intraday_decision_lineage_frame(local_settings, limit=20),
        }
    return {
        "capability_frame": latest_intraday_research_capability_frame(local_settings, limit=20),
        "active_policy_frame": latest_intraday_active_policy_frame(local_settings, limit=20),
        "active_meta_model_frame": latest_intraday_meta_active_model_frame(local_settings, limit=20),
        "recommendation_frame": latest_intraday_policy_recommendation_frame(local_settings, limit=20),
        "tuned_decision_frame": intraday_console_tuned_action_frame(local_settings, limit=20),
        "summary_preview": latest_intraday_summary_report_preview(local_settings),
        "postmortem_preview": latest_intraday_postmortem_preview(local_settings),
        "policy_preview": latest_intraday_policy_report_preview(local_settings),
    }


def _build_overview_items(payload: dict[str, object]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    basis_summary = payload["basis_summary"]
    items.append(
        {
            "eyebrow": "Basis",
            "title": _display_text(basis_summary.get("headline")),
            "body": _display_text(basis_summary.get("detail")),
            "meta": _display_text(basis_summary.get("label")),
            "badge": _display_text(basis_summary.get("mode"), "BASIS"),
            "tone": "warning" if basis_summary.get("mode") in {"historical", "stale"} else "positive",
        }
    )
    status_frame = payload["status_frame"]
    if isinstance(status_frame, pd.DataFrame) and not status_frame.empty:
        row = status_frame.iloc[0]
        items.append(
            {
                "eyebrow": "Status",
                "title": f"후보 {_safe_int(row.get('candidate_symbols'))}개 · 최종 액션 {_safe_int(row.get('final_action_symbols'))}개",
                "body": f"신호 {_safe_int(row.get('signal_symbols'))} / raw {_safe_int(row.get('raw_decision_symbols'))} / adjusted {_safe_int(row.get('adjusted_symbols'))}",
                "meta": f"bar {_display_text(row.get('avg_bar_latency_ms'))}ms · quote {_display_text(row.get('avg_quote_latency_ms'))}ms",
                "badge": "LIVE",
                "tone": "accent",
            }
        )
    for row in payload["checkpoint_health"].head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Checkpoint",
                "title": _display_text(row.get("checkpoint_time")),
                "body": f"candidate {_safe_int(row.get('candidate_symbols'))} / final {_safe_int(row.get('final_action_symbols'))}",
                "meta": f"raw {_safe_int(row.get('raw_decision_symbols'))} · adjusted {_safe_int(row.get('adjusted_symbols'))}",
                "badge": _display_text(row.get("status"), "CHECK"),
                "tone": str(row.get("status", "neutral")).lower(),
            }
        )
    return items


def _build_flow_items(payload: dict[str, object]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for frame_key, eyebrow, title_col, body_fields, badge_key in (
        ("candidate_frame", "Candidate", "symbol", ["grade", "expected_excess_return"], "candidate_rank"),
        ("signal_frame", "Signal", "symbol", ["signal_quality_score", "timing_adjustment_score"], "checkpoint_time"),
        ("decision_frame", "Rule", "symbol", ["action", "action_score"], "checkpoint_time"),
        ("adjusted_decision_frame", "Adjusted", "symbol", ["adjusted_action", "market_regime_family"], "checkpoint_time"),
        ("meta_prediction_frame", "Meta", "symbol", ["predicted_class", "confidence_margin"], "checkpoint_time"),
        ("tuned_decision_frame", "Final", "symbol", ["tuned_action", "final_action"], "checkpoint_time"),
    ):
        frame = payload[frame_key]
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            row = frame.iloc[0]
            items.append(
                {
                    "eyebrow": eyebrow,
                    "title": _display_text(row.get(title_col)),
                    "body": " / ".join(_display_text(row.get(field)) for field in body_fields),
                    "meta": f"horizon {_display_text(row.get('horizon'))}",
                    "badge": _display_text(row.get(badge_key), eyebrow.upper()),
                    "tone": "neutral",
                }
            )
    return items


def _build_result_items(payload: dict[str, object]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in payload["timing_frame"].head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Timing",
                "title": _display_text(row.get("symbol")),
                "body": f"checkpoint {_display_text(row.get('selected_checkpoint_time'))} / edge {_display_text(row.get('timing_edge_bps'))}",
                "meta": f"status {_display_text(row.get('outcome_status'))}",
                "badge": "TIMING",
                "tone": "accent",
            }
        )
    for row in payload["same_exit_frame"].head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Strategy",
                "title": _display_text(row.get("strategy_id")),
                "body": f"mean excess {_display_text(row.get('mean_realized_excess_return'))} / execution {_display_text(row.get('execution_rate'))}",
                "meta": f"horizon {_display_text(row.get('horizon'))}",
                "badge": "TRACE",
                "tone": "warning",
            }
        )
    for row in payload["lineage_frame"].head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Lineage",
                "title": _display_text(row.get("symbol")),
                "body": f"raw {_display_text(row.get('raw_action'))} / adjusted {_display_text(row.get('adjusted_action'))} / final {_display_text(row.get('final_action'))}",
                "meta": _display_text(row.get("checkpoint_time")),
                "badge": "FLOW",
                "tone": "neutral",
            }
        )
    return items


def _build_policy_items(payload: dict[str, object]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in payload["capability_frame"].head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Capability",
                "title": _display_text(row.get("feature_slug")),
                "body": f"dependency {_display_text(row.get('dependency_ready_flag'))} / report {_display_text(row.get('report_available_flag'))}",
                "meta": _display_text(row.get("rollout_mode")),
                "badge": "RESEARCH",
                "tone": "neutral",
            }
        )
    for row in payload["active_policy_frame"].head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Policy",
                "title": _display_text(row.get("template_id")),
                "body": f"{_display_text(row.get('scope_type'))} / objective {_display_text(row.get('objective_score'))}",
                "meta": f"D+{_display_text(row.get('horizon'))}",
                "badge": "POLICY",
                "tone": "accent",
            }
        )
    for row in payload["active_meta_model_frame"].head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Meta Model",
                "title": _display_text(row.get("model_version")),
                "body": f"{_display_text(row.get('panel_name'))} / D+{_display_text(row.get('horizon'))}",
                "meta": _display_text(row.get("effective_from_date")),
                "badge": "META",
                "tone": "warning",
            }
        )
    return items


render_page_header(
    settings,
    page_name="장중 콘솔",
    title="장중 콘솔",
    description="장중 보조 판단의 흐름을 표보다 먼저 브리프로 읽고, 원본은 필요할 때만 펼쳐보는 콘솔입니다.",
)
render_screen_guide(
    summary="개요, 판단 흐름, 결과 비교, 정책/리포트를 나눠서 기본은 한 줄 브리프로 정리했습니다.",
    bullets=[
        "개요에서 오늘 basis와 checkpoint 상태를 먼저 봅니다.",
        "판단 흐름에서 candidate → signal → final action을 최신 건 기준으로 읽습니다.",
        "결과 비교와 정책/리포트는 필요한 원본만 expander에서 확인합니다.",
    ],
)
render_warning_banner(
    "INFO",
    "이 화면은 해석용 콘솔입니다. 최종 액션은 참고 정보이며 자동 주문으로 이어지지 않습니다.",
)

section = st.segmented_control(
    "콘솔 보기",
    options=["개요", "판단 흐름", "결과 비교", "정책/리포트"],
    default="개요",
)

with st.spinner("장중 콘솔 데이터를 불러오는 중..."):
    payload = _load_intraday_console_section(PROJECT_ROOT.as_posix(), section)

if section == "개요":
    render_story_stream(
        title="장중 개요 브리프",
        summary="basis, 상태, checkpoint 흐름을 한 번에 읽습니다.",
        items=_build_overview_items(payload),
        empty_message="장중 개요 브리프 데이터가 없습니다.",
    )
    with st.expander("개요 원본 보기", expanded=False):
        render_record_cards(
            payload["checkpoint_health"],
            title="checkpoint health 원본",
            primary_column="checkpoint_time",
            secondary_columns=["status"],
            detail_columns=["candidate_symbols", "raw_decision_symbols", "adjusted_symbols", "final_action_symbols"],
            limit=8,
            empty_message="checkpoint health 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            payload["market_context"],
            title="market context 원본",
            primary_column="checkpoint_time",
            secondary_columns=["market_session_state"],
            detail_columns=["prior_daily_regime_state", "market_breadth_ratio", "candidate_mean_signal_quality"],
            limit=8,
            empty_message="market context 원본이 없습니다.",
            show_table_expander=False,
        )

elif section == "판단 흐름":
    render_story_stream(
        title="판단 흐름 브리프",
        summary="candidate에서 final action까지 가장 최신 건 중심으로 압축했습니다.",
        items=_build_flow_items(payload),
        empty_message="판단 흐름 브리프 데이터가 없습니다.",
    )
    with st.expander("판단 흐름 원본 보기", expanded=False):
        for frame, title, primary, secondary, details in (
            (payload["candidate_frame"], "candidate 원본", "symbol", ["company_name", "grade"], ["horizon", "candidate_rank", "final_selection_value"]),
            (payload["signal_frame"], "signal 원본", "symbol", ["checkpoint_time"], ["signal_quality_score", "timing_adjustment_score", "risk_friction_score"]),
            (payload["decision_frame"], "decision 원본", "symbol", ["action"], ["checkpoint_time", "horizon", "action_score"]),
            (payload["adjusted_decision_frame"], "adjusted 원본", "symbol", ["adjusted_action"], ["checkpoint_time", "market_regime_family", "raw_action"]),
            (payload["meta_prediction_frame"], "meta prediction 원본", "symbol", ["predicted_class"], ["checkpoint_time", "predicted_class_probability", "confidence_margin"]),
            (payload["tuned_decision_frame"], "final action 원본", "symbol", ["final_action"], ["checkpoint_time", "raw_action", "adjusted_action", "tuned_action"]),
        ):
            render_record_cards(
                frame,
                title=title,
                primary_column=primary,
                secondary_columns=secondary,
                detail_columns=details,
                limit=6,
                empty_message=f"{title}이 없습니다.",
                show_table_expander=False,
            )

elif section == "결과 비교":
    render_story_stream(
        title="결과 비교 브리프",
        summary="timing, strategy trace, lineage를 최신 건 위주로 요약했습니다.",
        items=_build_result_items(payload),
        empty_message="결과 비교 브리프 데이터가 없습니다.",
    )
    with st.expander("결과 비교 원본 보기", expanded=False):
        render_record_cards(
            payload["timing_frame"],
            title="timing 원본",
            primary_column="symbol",
            secondary_columns=["session_date", "selected_checkpoint_time"],
            detail_columns=["selected_action", "timing_edge_bps", "outcome_status"],
            limit=8,
            empty_message="timing 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            payload["same_exit_frame"],
            title="strategy comparison 원본",
            primary_column="strategy_id",
            secondary_columns=["horizon"],
            detail_columns=["executed_count", "execution_rate", "mean_realized_excess_return"],
            limit=8,
            empty_message="strategy comparison 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            payload["lineage_frame"],
            title="lineage 원본",
            primary_column="symbol",
            secondary_columns=["selection_date"],
            detail_columns=["checkpoint_time", "raw_action", "adjusted_action", "final_action"],
            limit=8,
            empty_message="lineage 원본이 없습니다.",
            show_table_expander=False,
        )

else:
    render_story_stream(
        title="정책 / 리포트 브리프",
        summary="research capability, active policy, meta model 상태를 짧게 읽습니다.",
        items=_build_policy_items(payload),
        empty_message="정책 / 리포트 브리프 데이터가 없습니다.",
    )
    if payload.get("summary_preview"):
        with st.expander("intraday summary 미리보기", expanded=False):
            render_report_preview(title="intraday summary", preview=payload["summary_preview"])
    if payload.get("policy_preview"):
        with st.expander("intraday policy report 미리보기", expanded=False):
            render_report_preview(title="intraday policy report", preview=payload["policy_preview"])
    if payload.get("postmortem_preview"):
        with st.expander("intraday postmortem 미리보기", expanded=False):
            render_report_preview(title="intraday postmortem", preview=payload["postmortem_preview"])
    with st.expander("정책 / 리포트 원본 보기", expanded=False):
        render_record_cards(
            payload["capability_frame"],
            title="capability 원본",
            primary_column="feature_slug",
            secondary_columns=["rollout_mode"],
            detail_columns=["dependency_ready_flag", "report_available_flag", "last_skip_reason"],
            limit=8,
            empty_message="capability 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            payload["active_policy_frame"],
            title="active policy 원본",
            primary_column="template_id",
            secondary_columns=["scope_type"],
            detail_columns=["horizon", "scope_key", "effective_from_date", "note"],
            limit=8,
            empty_message="active policy 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            payload["active_meta_model_frame"],
            title="active meta model 원본",
            primary_column="model_version",
            secondary_columns=["panel_name"],
            detail_columns=["horizon", "effective_from_date", "note"],
            limit=8,
            empty_message="active meta model 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            payload["recommendation_frame"],
            title="policy recommendation 원본",
            primary_column="recommended_policy_template",
            secondary_columns=["scope_type"],
            detail_columns=["horizon", "objective_score"],
            limit=8,
            empty_message="policy recommendation 원본이 없습니다.",
            show_table_expander=False,
        )

render_page_footer(settings, page_name="장중 콘솔")
