# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import render_record_cards, render_screen_guide, render_story_stream
from app.ui.dashboard_v2 import (
    dashboard_snapshot_note,
    display_text,
    load_dashboard_v2_context,
    read_dashboard_frame,
    render_dashboard_v2_empty,
    render_dashboard_v2_footer,
    render_dashboard_v2_header,
)

settings, activity, manifest = load_dashboard_v2_context(PROJECT_ROOT)
evaluation_summary = read_dashboard_frame(settings, "evaluation_summary_latest")
evaluation_comparison = read_dashboard_frame(settings, "evaluation_comparison_latest")
alpha_promotion = read_dashboard_frame(settings, "alpha_promotion_summary")
calibration = read_dashboard_frame(settings, "calibration_diagnostic_latest")
policy_eval = read_dashboard_frame(settings, "intraday_policy_evaluation_latest")


render_dashboard_v2_header(
    title="주간 캘리브레이션 / 정책 보고",
    description="주간 성과, 알파 비교, 캘리브레이션 상태만 남긴 얇은 보고 화면입니다.",
    settings=settings,
    activity=activity,
    manifest=manifest,
)
render_screen_guide(
    summary="주간 보고는 실험 로그 대신 성과 변화와 정책 상태만 보여줍니다.",
    bullets=[
        "성과 요약에서 window별 평균 초과수익과 hit rate를 읽습니다.",
        "알파 비교에서 active와 challenger 차이를 확인합니다.",
        "캘리브레이션 / 정책 섹션에서 밴드와 walkforward 상태를 봅니다.",
    ],
)

if evaluation_summary.empty and alpha_promotion.empty and calibration.empty:
    render_dashboard_v2_empty("주간 보고용 read-store가 아직 준비되지 않았습니다.")
else:
    summary_items: list[dict[str, str]] = []
    for row in evaluation_summary.head(4).to_dict(orient="records"):
        summary_items.append(
            {
                "eyebrow": "Summary",
                "title": f"{display_text(row.get('window_type'))} · D+{display_text(row.get('horizon'))}",
                "body": (
                    f"평균 초과수익 {display_text(row.get('mean_realized_excess_return'))} / "
                    f"hit {display_text(row.get('hit_rate'))}"
                ),
                "meta": f"평가수 {display_text(row.get('count_evaluated'))} · 기준일 {display_text(row.get('summary_date'))}",
                "badge": display_text(row.get("ranking_version"), "SUMMARY"),
                "tone": "neutral",
            }
        )
    for row in evaluation_comparison.head(2).to_dict(orient="records"):
        summary_items.append(
            {
                "eyebrow": "Compare",
                "title": f"{display_text(row.get('window_type'))} · D+{display_text(row.get('horizon'))}",
                "body": (
                    f"selection {display_text(row.get('selection_avg_excess'))} / "
                    f"explanatory {display_text(row.get('explanatory_avg_excess'))}"
                ),
                "meta": f"gap {display_text(row.get('avg_excess_gap'))}",
                "badge": "COMPARE",
                "tone": "accent",
            }
        )

    alpha_items: list[dict[str, str]] = []
    for row in alpha_promotion.head(4).to_dict(orient="records"):
        alpha_items.append(
            {
                "eyebrow": display_text(row.get("summary_title"), "Alpha"),
                "title": f"{display_text(row.get('active_model_label'))} vs {display_text(row.get('comparison_model_label'))}",
                "body": (
                    f"{display_text(row.get('decision_label'))} / "
                    f"active {display_text(row.get('active_top10_mean_excess_return'))} / "
                    f"challenger {display_text(row.get('comparison_top10_mean_excess_return'))}"
                ),
                "meta": f"sample {display_text(row.get('sample_count'))} · gap {display_text(row.get('promotion_gap'))}",
                "badge": display_text(row.get("decision_label"), "ALPHA"),
                "tone": "neutral",
            }
        )

    calibration_items: list[dict[str, str]] = []
    for row in calibration.head(4).to_dict(orient="records"):
        calibration_items.append(
            {
                "eyebrow": "Calibration",
                "title": f"D+{display_text(row.get('horizon'))} · {display_text(row.get('bin_type'))}",
                "body": (
                    f"expected {display_text(row.get('expected_median'))} / "
                    f"observed {display_text(row.get('observed_mean'))} / "
                    f"coverage {display_text(row.get('coverage_rate'))}"
                ),
                "meta": f"quality {display_text(row.get('quality_flag'))} · window {display_text(row.get('diagnostic_date'))}",
                "badge": "CAL",
                "tone": "warning",
            }
        )
    for row in policy_eval.head(3).to_dict(orient="records"):
        calibration_items.append(
            {
                "eyebrow": "Policy",
                "title": display_text(row.get("template_id")),
                "body": (
                    f"objective {display_text(row.get('objective_score'))} / "
                    f"test {display_text(row.get('test_session_count'))} / "
                    f"hit {display_text(row.get('hit_rate'))}"
                ),
                "meta": f"{display_text(row.get('scope_type'))} · D+{display_text(row.get('horizon'))}",
                "badge": "WF",
                "tone": "accent",
            }
        )

    render_story_stream(
        title="성과 브리프",
        summary=dashboard_snapshot_note(manifest),
        items=summary_items,
        empty_message="성과 브리프 데이터가 없습니다.",
    )
    render_story_stream(
        title="알파 비교",
        summary="active 모델과 challenger 차이를 주간 기준으로 읽습니다.",
        items=alpha_items,
        empty_message="알파 비교 데이터가 없습니다.",
    )
    render_story_stream(
        title="캘리브레이션 / 정책",
        summary="밴드와 정책 walkforward 상태를 같은 흐름으로 보여줍니다.",
        items=calibration_items,
        empty_message="캘리브레이션 / 정책 데이터가 없습니다.",
    )

    with st.expander("원본 데이터 보기", expanded=False):
        render_record_cards(
            evaluation_summary,
            title="평가 요약 원본",
            primary_column="window_type",
            secondary_columns=["horizon", "ranking_version"],
            detail_columns=["mean_realized_excess_return", "hit_rate", "count_evaluated"],
            limit=10,
            empty_message="평가 요약 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            alpha_promotion,
            title="알파 비교 원본",
            primary_column="summary_title",
            secondary_columns=["active_model_label", "comparison_model_label"],
            detail_columns=["decision_label", "promotion_gap", "sample_count", "window_end"],
            limit=10,
            empty_message="알파 비교 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            calibration,
            title="캘리브레이션 원본",
            primary_column="diagnostic_date",
            secondary_columns=["horizon", "bin_type"],
            detail_columns=["expected_median", "observed_mean", "coverage_rate", "quality_flag"],
            limit=10,
            empty_message="캘리브레이션 원본이 없습니다.",
            show_table_expander=False,
        )
        render_record_cards(
            policy_eval,
            title="정책 평가 원본",
            primary_column="template_id",
            secondary_columns=["scope_type", "horizon"],
            detail_columns=["objective_score", "test_session_count", "hit_rate"],
            limit=10,
            empty_message="정책 평가 원본이 없습니다.",
            show_table_expander=False,
        )

render_dashboard_v2_footer(settings, manifest=manifest, page_name="주간 보고")
