# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.selection.engine_v2 import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
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
    available_evaluation_dates,
    evaluation_outcomes_frame,
    format_ranking_version_label,
    format_ui_date,
    latest_alpha_promotion_summary_frame,
    latest_calibration_diagnostic_frame,
    latest_evaluation_comparison_frame,
    latest_evaluation_summary_frame,
    latest_intraday_meta_overlay_comparison_frame,
    latest_intraday_policy_ablation_frame,
    latest_intraday_policy_evaluation_frame,
    latest_intraday_postmortem_preview,
    latest_intraday_research_capability_frame,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_timing_calibration_frame,
    latest_postmortem_preview,
    latest_selection_engine_comparison_frame,
    load_ui_page_context,
)

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="evaluation",
    page_title="사후 평가",
)
evaluation_dates = available_evaluation_dates(settings)

latest_summary = latest_evaluation_summary_frame(settings, limit=30)
latest_comparison = latest_evaluation_comparison_frame(settings)
latest_selection_v2_comparison = latest_selection_engine_comparison_frame(settings)
latest_alpha_promotion = latest_alpha_promotion_summary_frame(settings, limit=10)
latest_calibration = latest_calibration_diagnostic_frame(settings, limit=30)
intraday_capability = latest_intraday_research_capability_frame(settings, limit=20)
intraday_strategy_comparison = latest_intraday_strategy_comparison_frame(settings, limit=30)
intraday_regime_matrix = latest_intraday_strategy_comparison_frame(
    settings,
    comparison_scope="regime_family",
    limit=30,
)
intraday_timing_calibration = latest_intraday_timing_calibration_frame(settings, limit=30)
policy_walkforward = latest_intraday_policy_evaluation_frame(settings, split_name="test", limit=30)
policy_ablation = latest_intraday_policy_ablation_frame(settings, limit=30)
meta_overlay = latest_intraday_meta_overlay_comparison_frame(settings, limit=30)
meta_regime_breakdown = latest_intraday_meta_overlay_comparison_frame(
    settings,
    metric_scope="regime",
    limit=30,
)
meta_checkpoint_breakdown = latest_intraday_meta_overlay_comparison_frame(
    settings,
    metric_scope="checkpoint",
    limit=30,
)
postmortem_preview = latest_postmortem_preview(settings)
intraday_postmortem_preview = latest_intraday_postmortem_preview(settings)


def _display_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def _build_model_eval_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in latest_summary.head(3).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Summary",
                "title": _display_text(row.get("summary_name")),
                "body": f"{_display_text(row.get('window_type'))} / value {_display_text(row.get('summary_value'))}",
                "meta": f"horizon {_display_text(row.get('horizon'))}",
                "badge": "SUMMARY",
                "tone": "neutral",
            }
        )
    for row in latest_comparison.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Selection vs Explanatory",
                "title": f"{_display_text(row.get('metric_name'))} · D+{_display_text(row.get('horizon'))}",
                "body": (
                    f"selection {_display_text(row.get('selection_v2_avg_excess'))} / "
                    f"explanatory {_display_text(row.get('explanatory_avg_excess'))}"
                ),
                "meta": f"gap {_display_text(row.get('avg_excess_gap'))}",
                "badge": "COMPARE",
                "tone": "accent",
            }
        )
    for row in latest_alpha_promotion.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Alpha",
                "title": f"{_display_text(row.get('active_model_label'))} vs {_display_text(row.get('comparison_model_label'))}",
                "body": f"{_display_text(row.get('decision_label'))} / gap {_display_text(row.get('promotion_gap'))}",
                "meta": f"sample {_display_text(row.get('sample_count'))} · p {_display_text(row.get('p_value'))}",
                "badge": _display_text(row.get("decision_label"), "ALPHA"),
                "tone": "neutral",
            }
        )
    return items


def _build_calibration_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in latest_calibration.head(3).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Calibration",
                "title": _display_text(row.get("diagnostic_name")),
                "body": _display_text(row.get("diagnostic_value")),
                "meta": _display_text(row.get("window_end_date")),
                "badge": f"D+{_display_text(row.get('horizon'))}",
                "tone": "accent",
            }
        )
    for row in latest_selection_v2_comparison.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Engine",
                "title": _display_text(row.get("metric_name")),
                "body": f"current {_display_text(row.get('current_value'))} / prior {_display_text(row.get('prior_value'))}",
                "meta": _display_text(row.get("window_type")),
                "badge": "ENGINE",
                "tone": "neutral",
            }
        )
    return items


def _build_intraday_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in intraday_capability.head(2).to_dict(orient="records"):
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
    for row in intraday_strategy_comparison.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Strategy",
                "title": _display_text(row.get("strategy_id")),
                "body": f"mean excess {_display_text(row.get('mean_realized_excess_return'))} / execution {_display_text(row.get('execution_rate'))}",
                "meta": f"horizon {_display_text(row.get('horizon'))}",
                "badge": "TRACE",
                "tone": "accent",
            }
        )
    for row in intraday_timing_calibration.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Timing",
                "title": _display_text(row.get("grouping_value"), "overall"),
                "body": f"hit {_display_text(row.get('hit_rate'))} / edge {_display_text(row.get('mean_timing_edge_vs_open_bps'))}",
                "meta": f"horizon {_display_text(row.get('horizon'))}",
                "badge": "TIMING",
                "tone": "warning",
            }
        )
    return items


def _build_policy_items() -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in policy_walkforward.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Walkforward",
                "title": _display_text(row.get("policy_template")),
                "body": f"objective {_display_text(row.get('objective_score'))} / test sessions {_display_text(row.get('test_session_count'))}",
                "meta": f"{_display_text(row.get('scope_type'))} · D+{_display_text(row.get('horizon'))}",
                "badge": "POLICY",
                "tone": "neutral",
            }
        )
    for row in policy_ablation.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Ablation",
                "title": _display_text(row.get("ablation_name"), "ablation"),
                "body": f"value {_display_text(row.get('metric_value'))}",
                "meta": f"{_display_text(row.get('metric_name'))} · D+{_display_text(row.get('horizon'))}",
                "badge": "ABLATION",
                "tone": "warning",
            }
        )
    for row in meta_overlay.head(2).to_dict(orient="records"):
        items.append(
            {
                "eyebrow": "Meta Overlay",
                "title": _display_text(row.get("metric_name")),
                "body": f"policy {_display_text(row.get('policy_only_value'))} / meta {_display_text(row.get('meta_overlay_value'))}",
                "meta": f"D+{_display_text(row.get('horizon'))}",
                "badge": "META",
                "tone": "accent",
            }
        )
    return items


render_page_header(
    settings,
    page_name="사후 평가",
    title="사후 평가",
    description="사후 성과와 calibration, intraday 비교를 표보다 먼저 브리프로 읽는 모바일 우선 평가 화면입니다.",
)
render_screen_guide(
    summary="사후 성과, calibration, intraday 연구 결과를 각각 스트림으로 나눠 읽도록 정리했습니다.",
    bullets=[
        "모델 평가 브리프에서 성과 요약과 alpha 비교를 먼저 봅니다.",
        "calibration 브리프에서 밴드와 엔진 변화만 추려 읽습니다.",
        "intraday와 policy 비교는 research 지표로만 해석합니다.",
    ],
)
render_warning_banner(
    "INFO",
    "장중 비교와 메타 오버레이는 연구용 평가 결과이며, 자동 주문이나 실거래 반영과는 연결되지 않습니다.",
)

if not evaluation_dates:
    st.info("아직 평가 결과가 없습니다. 평가 배치를 먼저 실행해 주세요.")
else:
    selected_date = st.selectbox("평가일", options=evaluation_dates, index=0, format_func=format_ui_date)
    horizon = st.selectbox("기간", options=[1, 5], index=1, format_func=lambda value: f"D+{value}")
    ranking_version = st.selectbox(
        "순위 버전",
        options=[
            SELECTION_ENGINE_V2_VERSION,
            SELECTION_ENGINE_VERSION,
            EXPLANATORY_RANKING_VERSION,
        ],
        index=0,
        format_func=format_ranking_version_label,
    )
    limit = st.slider("표시 수", min_value=10, max_value=100, value=25, step=5)
    outcomes = evaluation_outcomes_frame(
        settings,
        evaluation_date=selected_date,
        horizon=horizon,
        ranking_version=ranking_version,
        limit=limit,
    )

    render_story_stream(
        title="모델 평가 브리프",
        summary="최신 성과 요약과 selection/explanatory 비교, alpha promotion 결과를 한 흐름으로 읽습니다.",
        items=_build_model_eval_items(),
        empty_message="모델 평가 브리프 데이터가 없습니다.",
    )
    render_story_stream(
        title="Calibration 브리프",
        summary="밴드 보정과 selection engine 비교를 짧게 묶었습니다.",
        items=_build_calibration_items(),
        empty_message="calibration 브리프 데이터가 없습니다.",
    )
    render_story_stream(
        title="Intraday 브리프",
        summary="capability, strategy 비교, timing calibration의 최근 상태를 요약합니다.",
        items=_build_intraday_items(),
        empty_message="intraday 브리프 데이터가 없습니다.",
    )
    render_story_stream(
        title="Policy / Meta 비교",
        summary="walkforward, ablation, meta overlay 차이를 연구 지표 기준으로 읽습니다.",
        items=_build_policy_items(),
        empty_message="policy/meta 비교 데이터가 없습니다.",
    )

    with st.expander("평가 원본 보기", expanded=False):
        render_record_cards(
            outcomes,
            title="평가 결과 원본",
            primary_column="symbol",
            secondary_columns=["company_name", "outcome_status"],
            detail_columns=["selection_date", "horizon", "realized_excess_return", "band_status"],
            limit=10,
            empty_message="평가 결과 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            latest_summary,
            title="평가 요약 원본",
            primary_column="summary_name",
            secondary_columns=["window_type"],
            detail_columns=["summary_value", "horizon"],
            limit=10,
            empty_message="평가 요약 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            latest_calibration,
            title="calibration 원본",
            primary_column="diagnostic_name",
            secondary_columns=["horizon"],
            detail_columns=["diagnostic_value", "window_end_date"],
            limit=10,
            empty_message="calibration 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            policy_walkforward,
            title="policy walkforward 원본",
            primary_column="policy_template",
            secondary_columns=["scope_type", "horizon"],
            detail_columns=["objective_score", "test_session_count", "manual_review_required_flag"],
            limit=10,
            empty_message="policy walkforward 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            meta_regime_breakdown,
            title="meta regime 원본",
            primary_column="metric_name",
            secondary_columns=["horizon"],
            detail_columns=["policy_only_value", "meta_overlay_value"],
            limit=8,
            empty_message="meta regime 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            meta_checkpoint_breakdown,
            title="meta checkpoint 원본",
            primary_column="metric_name",
            secondary_columns=["horizon"],
            detail_columns=["policy_only_value", "meta_overlay_value"],
            limit=8,
            empty_message="meta checkpoint 원본이 없습니다.",
            show_table_expander=False,
        )

    if postmortem_preview:
        with st.expander("최신 postmortem 미리보기", expanded=False):
            render_report_preview(title="postmortem", preview=postmortem_preview)
    if intraday_postmortem_preview:
        with st.expander("최신 intraday postmortem 미리보기", expanded=False):
            render_report_preview(title="intraday postmortem", preview=intraday_postmortem_preview)

render_page_footer(settings, page_name="사후 평가")
