# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
from app.intraday.meta_common import ENTER_PANEL, WAIT_PANEL
from app.intraday.meta_training import freeze_intraday_active_meta_model
from app.intraday.policy import freeze_intraday_active_policy
from app.ui.components import (
    render_data_sheet,
    render_page_footer,
    render_page_header,
    render_report_preview,
    render_screen_guide,
    render_warning_banner,
)
from app.ui.helpers import (
    format_ui_date,
    format_ui_delta,
    format_ui_number,
    format_ui_percent,
    format_ui_value,
    intraday_meta_calibration_frame,
    intraday_meta_confusion_matrix_frame,
    intraday_meta_feature_importance_frame,
    is_ui_missing_value,
    latest_alpha_training_candidate_frame,
    latest_intraday_active_policy_frame,
    latest_intraday_meta_active_model_frame,
    latest_intraday_meta_apply_compare_frame,
    latest_intraday_meta_overlay_comparison_frame,
    latest_intraday_meta_training_frame,
    latest_intraday_policy_ablation_frame,
    latest_intraday_policy_apply_compare_frame,
    latest_intraday_policy_evaluation_frame,
    latest_intraday_policy_experiment_frame,
    latest_intraday_policy_publish_status_frame,
    latest_intraday_policy_recommendation_frame,
    latest_intraday_policy_report_preview,
    latest_intraday_policy_rollback_frame,
    latest_intraday_research_capability_frame,
    latest_job_runs_frame,
    latest_model_training_summary_frame,
    load_ui_page_context,
)

POLICY_TEMPLATE_LABELS = {
    "BASE_DEFAULT": "기본형",
    "DEFENSIVE_LIGHT": "방어형(약)",
    "DEFENSIVE_STRONG": "방어형(강)",
    "RISK_ON_LIGHT": "상승장 적극형",
    "GAP_GUARD_STRICT": "갭 추격 억제형",
    "FRICTION_GUARD_STRICT": "체결 부담 억제형",
}


def _safe_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text and text not in {"nan", "NaN", "NaT", "None"} else fallback


def _template_label(template_id: object) -> str:
    template = _safe_text(template_id)
    return POLICY_TEMPLATE_LABELS.get(template, template)


def _policy_scope_text(row) -> str:
    horizon = _safe_text(row.get("horizon"))
    scope_type = format_ui_value("scope_type", row.get("scope_type"))
    scope_key = _safe_text(row.get("scope_key"))
    checkpoint = _safe_text(row.get("checkpoint_time"))
    if checkpoint != "-":
        return f"{horizon}거래일 · {scope_type} · {scope_key} · {checkpoint}"
    return f"{horizon}거래일 · {scope_type} · {scope_key}"


def _panel_label(panel_name: object) -> str:
    panel = _safe_text(panel_name)
    if panel == ENTER_PANEL:
        return "진입 판단"
    if panel == WAIT_PANEL:
        return "대기 판단"
    return panel


def _metric_delta(value: object, *, percent_points: bool = False) -> str | None:
    formatted = (
        format_ui_percent(value, signed=True, percent_points=True)
        if percent_points
        else format_ui_delta(value)
    )
    return None if formatted == "-" else formatted


def _render_metric(
    column,
    label: str,
    value: str,
    *,
    delta: str | None = None,
    delta_color: str = "normal",
) -> None:
    kwargs = {"label": label, "value": value}
    if delta is not None:
        kwargs["delta"] = delta
        kwargs["delta_color"] = delta_color
    column.metric(**kwargs)


def _policy_compare_notice(row_dict: dict[str, object]) -> tuple[str, str]:
    has_active_policy = not is_ui_missing_value(row_dict.get("active_policy_candidate_id")) or not is_ui_missing_value(
        row_dict.get("active_template_id")
    )
    has_candidate_metrics = any(
        not is_ui_missing_value(row_dict.get(column))
        for column in (
            "after_objective_score",
            "after_mean_excess_return",
            "after_hit_rate",
            "after_execution_rate",
        )
    )
    if not has_active_policy:
        return ("info", "현재 운영 중인 기준 정책이 없어 변화량 대신 새 후보 수치만 표시합니다.")
    if not has_candidate_metrics:
        return ("warning", "이번 주 후보의 검증 수치가 없어 정책 점수를 비교할 수 없습니다.")
    if bool(row_dict.get("manual_review_required_flag")):
        return ("warning", "이 후보는 자동 반영 대상이 아니며 수동 검토가 필요합니다.")
    return ("success", "현재 운영 정책과 이번 주 후보를 같은 기준으로 비교할 수 있습니다.")


def _meta_compare_notice(row_dict: dict[str, object]) -> tuple[str, str]:
    has_active_model = not is_ui_missing_value(row_dict.get("active_meta_model_id")) or not is_ui_missing_value(
        row_dict.get("active_training_run_id")
    )
    has_candidate_metrics = any(
        not is_ui_missing_value(row_dict.get(column))
        for column in ("after_macro_f1", "after_log_loss", "macro_f1_delta", "log_loss_delta")
    )
    fallback_reason = _safe_text(row_dict.get("fallback_reason"))
    validation_sessions = row_dict.get("validation_session_count")
    if bool(row_dict.get("fallback_flag")):
        if fallback_reason != "-":
            return ("warning", f"이 후보는 대체 경로로 생성되었습니다. 사유: {fallback_reason}")
        return ("warning", "이 후보는 대체 경로로 생성되어 바로 반영하기 어렵습니다.")
    if not has_active_model:
        return ("info", "현재 운영 중인 메타 모델이 없어 변화량 대신 새 후보 수치만 표시합니다.")
    if not has_candidate_metrics:
        if not is_ui_missing_value(validation_sessions) and int(validation_sessions or 0) == 0:
            return ("warning", "검증 세션이 없어 메타 모델 점수를 비교할 수 없습니다.")
        return ("warning", "이번 주 후보의 검증 수치가 없어 메타 모델 점수를 비교할 수 없습니다.")
    return ("success", "현재 운영 모델과 이번 주 후보를 같은 기준으로 비교할 수 있습니다.")


def _review_text(flag: object) -> str:
    return "예, 한 번 더 확인 필요" if bool(flag) else "아니오, 바로 반영 가능"


def _render_policy_change_summary() -> None:
    st.markdown("### 정책 교체를 이렇게 보세요")
    if weekly_calibration_runs.empty:
        st.error("최근 주말 보정 실행 기록이 없습니다.")
        return

    latest_run = weekly_calibration_runs.iloc[0]
    st.write(
        f"- 최근 주말 보정 실행: {format_ui_date(latest_run.get('as_of_date'))} / 결과: "
        f"{format_ui_value('status', latest_run.get('status'))}"
    )
    notes = _safe_text(latest_run.get("notes"))
    if notes != "-":
        st.caption(notes)

    if policy_apply_compare.empty:
        st.warning("주말 보정 결과는 있지만, 지금 화면에 비교할 다음 정책 후보가 없습니다.")
        return

    for index, row in enumerate(policy_apply_compare.head(2).itertuples(index=False), start=1):
        row_dict = row._asdict()
        st.markdown(f"#### 비교 {index}. {_policy_scope_text(row_dict)}")
        left, right = st.columns(2)
        with left:
            st.write("지금 운영 중")
            st.write(f"- 정책 기준: {_template_label(row_dict.get('active_template_id'))}")
            st.write(f"- 반영 시작일: {format_ui_date(row_dict.get('effective_from_date'))}")
            st.write(f"- 원본 추천일: {format_ui_date(row_dict.get('source_recommendation_date'))}")
        with right:
            st.write("이번 주 새 후보")
            st.write(f"- 정책 기준: {_template_label(row_dict.get('recommended_template_id'))}")
            st.write(f"- 추천일: {format_ui_date(row_dict.get('recommendation_date'))}")
            st.write(f"- 사람 확인 필요: {_review_text(row_dict.get('manual_review_required_flag'))}")

        notice_level, notice_text = _policy_compare_notice(row_dict)
        getattr(st, notice_level, st.info)(notice_text)

        metric_a, metric_b, metric_c, metric_d = st.columns(4)
        _render_metric(
            metric_a,
            "추천 목표 점수",
            format_ui_number(row_dict.get("after_objective_score")),
            delta=_metric_delta(row_dict.get("objective_score_delta")),
        )
        _render_metric(
            metric_b,
            "추천 평균 초과수익률",
            format_ui_percent(row_dict.get("after_mean_excess_return")),
            delta=_metric_delta(row_dict.get("mean_excess_return_delta"), percent_points=True),
        )
        _render_metric(
            metric_c,
            "추천 적중률",
            format_ui_percent(row_dict.get("after_hit_rate")),
            delta=_metric_delta(row_dict.get("hit_rate_delta"), percent_points=True),
        )
        _render_metric(
            metric_d,
            "추천 실행률",
            format_ui_percent(row_dict.get("after_execution_rate")),
            delta=_metric_delta(row_dict.get("execution_rate_delta"), percent_points=True),
        )

    st.success("바꿔도 괜찮다고 판단되면 바로 아래 `정책 바로 반영` 버튼을 누르시면 됩니다.")


def _render_meta_change_summary() -> None:
    st.markdown("### 메타 모델 교체를 이렇게 보세요")
    if weekly_training_runs.empty:
        st.error("최근 주말 학습 실행 기록이 없습니다.")
        return

    latest_run = weekly_training_runs.iloc[0]
    st.write(
        f"- 최근 주말 학습 실행: {format_ui_date(latest_run.get('as_of_date'))} / 결과: "
        f"{format_ui_value('status', latest_run.get('status'))}"
    )
    notes = _safe_text(latest_run.get("notes"))
    if notes != "-":
        st.caption(notes)

    if meta_apply_compare.empty:
        st.warning("주말 학습 결과는 있지만, 지금 화면에 비교할 메타 모델 후보가 없습니다.")
        return

    for index, row in enumerate(meta_apply_compare.head(4).itertuples(index=False), start=1):
        row_dict = row._asdict()
        st.markdown(f"#### 비교 {index}. {_panel_label(row_dict.get('panel_name'))} / {row_dict.get('horizon')}거래일")
        left, right = st.columns(2)
        with left:
            st.write("지금 운영 중")
            st.write(f"- 운영 모델 ID: {_safe_text(row_dict.get('active_meta_model_id'))}")
            st.write(f"- 반영 시작일: {format_ui_date(row_dict.get('effective_from_date'))}")
            st.write(f"- 학습 실행 ID: {_safe_text(row_dict.get('active_training_run_id'))}")
        with right:
            st.write("이번 주 새 후보")
            st.write(f"- 학습 종료일: {format_ui_date(row_dict.get('train_end_date'))}")
            st.write(f"- 검증 세션 수: {_safe_text(format_ui_number(row_dict.get('validation_session_count')))}")
            st.write(f"- 대체 계산 사용: {_review_text(row_dict.get('fallback_flag'))}")

        notice_level, notice_text = _meta_compare_notice(row_dict)
        getattr(st, notice_level, st.info)(notice_text)

        metric_a, metric_b, metric_c = st.columns(3)
        _render_metric(
            metric_a,
            "후보 종합 분류 점수",
            format_ui_number(row_dict.get("after_macro_f1")),
            delta=_metric_delta(row_dict.get("macro_f1_delta")),
        )
        _render_metric(
            metric_b,
            "후보 로그 손실",
            format_ui_number(row_dict.get("after_log_loss")),
            delta=_metric_delta(row_dict.get("log_loss_delta")),
            delta_color="inverse",
        )
        _render_metric(
            metric_c,
            "학습에 쓴 특징 수",
            format_ui_number(row_dict.get("feature_count")),
        )

    st.success("바꿔도 괜찮다고 판단되면 바로 아래 `메타 모델 바로 반영` 버튼을 누르시면 됩니다.")

settings, _activity = load_ui_page_context(
    PROJECT_ROOT,
    page_key="research_lab",
    page_title="리서치 랩",
)

recent_runs = latest_job_runs_frame(settings, limit=60)
weekly_calibration_runs = (
    recent_runs.loc[recent_runs["job_name"].astype(str).eq("run_weekly_calibration_bundle")].copy()
    if not recent_runs.empty
    else recent_runs
)
weekly_training_runs = (
    recent_runs.loc[recent_runs["job_name"].astype(str).eq("run_weekly_training_bundle")].copy()
    if not recent_runs.empty
    else recent_runs
)

alpha_training_summary = latest_model_training_summary_frame(settings)
alpha_training_candidates = latest_alpha_training_candidate_frame(settings, limit=20)
meta_training_summary = latest_intraday_meta_training_frame(settings, limit=20)
intraday_capability = latest_intraday_research_capability_frame(settings, limit=12)
policy_experiments = latest_intraday_policy_experiment_frame(settings, limit=20)
policy_calibration = latest_intraday_policy_evaluation_frame(settings, split_name="validation", limit=20)
policy_walkforward = latest_intraday_policy_evaluation_frame(settings, split_name="test", limit=20)
policy_ablation = latest_intraday_policy_ablation_frame(settings, limit=20)
policy_recommendation = latest_intraday_policy_recommendation_frame(settings, limit=20)
policy_rollbacks = latest_intraday_policy_rollback_frame(settings, limit=12)
policy_publish_status = latest_intraday_policy_publish_status_frame(settings, limit=12)
policy_report_preview = latest_intraday_policy_report_preview(settings)
active_policy = latest_intraday_active_policy_frame(settings, limit=12)
policy_apply_compare = latest_intraday_policy_apply_compare_frame(settings, limit=12)
active_meta_models = latest_intraday_meta_active_model_frame(settings, limit=12)
meta_apply_compare = latest_intraday_meta_apply_compare_frame(settings, limit=12)
meta_overlay = latest_intraday_meta_overlay_comparison_frame(settings, limit=20)
meta_regime_breakdown = latest_intraday_meta_overlay_comparison_frame(
    settings,
    metric_scope="regime",
    limit=20,
)

render_page_header(
    settings,
    page_name="리서치",
    title="리서치",
    description="주간 학습 결과 비교와 반영 버튼을 맨 위에 두고, 정책·메타 분석은 탭으로 나눠 보는 연구 화면입니다.",
)
render_screen_guide(
    summary="주간 학습 결과를 실제로 반영할 때 헤매지 않도록 첫 탭을 바로 반영 화면으로 바꿨습니다.",
    bullets=[
        "주간 반영 탭에서 현재 운영값과 다음 후보를 비교하고 바로 승인합니다.",
        "정책 보기 탭에서는 정책 실험과 기간별 재검증을 확인합니다.",
        "메타 보기 탭에서는 메타 보정 결과와 국면별 차이를 확인합니다.",
        "학습 요약 탭에서는 최근 학습 이력과 연구 리포트를 한 번에 봅니다.",
    ],
)
render_warning_banner(
    "INFO",
    "자동 반영은 없습니다. 아래 주간 반영 탭에서 비교하고 직접 승인해야만 운영값이 바뀝니다.",
)

action_tab, policy_tab, meta_tab, summary_tab = st.tabs(
    ["주간 반영", "정책 보기", "메타 보기", "학습 요약"]
)

with action_tab:
    st.subheader("이번 주 결과 반영")
    st.caption("정책과 메타 모델 모두 `지금 운영 중인 값`과 `이번 주 새 후보`를 먼저 읽고, 괜찮으면 바로 아래 버튼으로 반영하세요.")

    policy_action_tab, meta_action_tab = st.tabs(["정책 교체", "메타 모델 교체"])

    with policy_action_tab:
        _render_policy_change_summary()
        with st.expander("정책 수치를 자세히 보기", expanded=False):
            render_data_sheet(
                weekly_calibration_runs,
                title="최근 주말 보정 실행 결과",
                primary_column="started_at",
                secondary_columns=["status", "as_of_date"],
                detail_columns=["notes", "run_id"],
                limit=2,
                empty_message="최근 주말 보정 실행 기록이 없습니다.",
                show_table_expander=False,
            )
            render_data_sheet(
                policy_recommendation,
                title="주말 보정에서 나온 추천 정책",
                primary_column="policy_candidate_id",
                secondary_columns=["template_id", "recommendation_date"],
                detail_columns=[
                    "horizon",
                    "scope_type",
                    "scope_key",
                    "objective_score",
                    "mean_realized_excess_return",
                    "hit_rate",
                    "execution_rate",
                    "manual_review_required_flag",
                ],
                limit=6,
                empty_message="주말 보정 추천 결과가 없습니다.",
                show_table_expander=False,
            )
            render_data_sheet(
                active_policy,
                title="현재 운영 정책",
                primary_column="policy_candidate_id",
                secondary_columns=["template_id", "effective_from_date"],
                detail_columns=["horizon", "scope_type", "scope_key", "checkpoint_time", "note"],
                limit=6,
                empty_message="현재 운영 정책 기록이 없습니다.",
                show_table_expander=False,
            )
            render_data_sheet(
                policy_apply_compare,
                title="현재 정책과 다음 후보 비교",
                primary_column="recommended_policy_candidate_id",
                secondary_columns=["recommended_template_id", "recommendation_date"],
                detail_columns=[
                    "horizon",
                    "scope_type",
                    "scope_key",
                    "active_template_id",
                    "before_objective_score",
                    "after_objective_score",
                    "objective_score_delta",
                    "mean_excess_return_delta",
                    "hit_rate_delta",
                    "execution_rate_delta",
                    "manual_review_required_flag",
                ],
                limit=6,
                empty_message="이번 주에 반영할 정책 후보가 없습니다.",
                show_table_expander=False,
            )

        with st.form("apply_intraday_policy_form", clear_on_submit=False):
            policy_note = st.text_input(
                "정책 반영 메모",
                value="주간 보정 결과를 확인하고 운영 정책을 교체함",
            )
            policy_confirm = st.checkbox("비교 내용을 확인했고, 현재 운영 정책을 이 후보로 바꾸는 데 동의합니다.")
            policy_submit = st.form_submit_button("정책 바로 반영")
            if policy_submit:
                if not policy_confirm:
                    st.warning("먼저 비교 내용을 확인하고 동의 체크를 해주세요.")
                elif policy_apply_compare.empty:
                    st.warning("반영할 정책 후보가 없습니다.")
                else:
                    recommendation_dates = (
                        policy_apply_compare["recommendation_date"].dropna().astype(str).tolist()
                        if "recommendation_date" in policy_apply_compare.columns
                        else []
                    )
                    effective_date = (
                        max(recommendation_dates)
                        if recommendation_dates
                        else today_local(settings.app.timezone).isoformat()
                    )
                    policy_result = freeze_intraday_active_policy(
                        settings,
                        as_of_date=date.fromisoformat(effective_date),
                        promotion_type="MANUAL_FREEZE",
                        source="scheduler_latest_recommendation",
                        note=policy_note,
                    )
                    st.success(
                        f"정책 반영을 완료했습니다. run_id={policy_result.run_id} rows={policy_result.row_count}"
                    )

    with meta_action_tab:
        _render_meta_change_summary()
        with st.expander("메타 모델 수치를 자세히 보기", expanded=False):
            render_data_sheet(
                weekly_training_runs,
                title="최근 주말 학습 실행 결과",
                primary_column="started_at",
                secondary_columns=["status", "as_of_date"],
                detail_columns=["notes", "run_id"],
                limit=2,
                empty_message="최근 주말 학습 실행 기록이 없습니다.",
                show_table_expander=False,
            )
            render_data_sheet(
                meta_training_summary,
                title="주말 학습에서 나온 메타 후보",
                primary_column="training_run_id",
                secondary_columns=["panel_name", "horizon"],
                detail_columns=[
                    "train_end_date",
                    "validation_row_count",
                    "validation_session_count",
                    "feature_count",
                    "fallback_flag",
                    "fallback_reason",
                ],
                limit=6,
                empty_message="주말 학습 메타 후보가 없습니다.",
                show_table_expander=False,
            )
            render_data_sheet(
                active_meta_models,
                title="현재 운영 메타 모델",
                primary_column="active_meta_model_id",
                secondary_columns=["panel_name", "horizon"],
                detail_columns=[
                    "training_run_id",
                    "promotion_type",
                    "effective_from_date",
                    "fallback_flag",
                    "fallback_reason",
                ],
                limit=6,
                empty_message="현재 운영 메타 모델이 없습니다.",
                show_table_expander=False,
            )
            render_data_sheet(
                meta_apply_compare,
                title="현재 메타 모델과 다음 후보 비교",
                primary_column="candidate_training_run_id",
                secondary_columns=["panel_name", "horizon"],
                detail_columns=[
                    "active_meta_model_id",
                    "active_training_run_id",
                    "train_end_date",
                    "before_macro_f1",
                    "after_macro_f1",
                    "macro_f1_delta",
                    "before_log_loss",
                    "after_log_loss",
                    "log_loss_delta",
                    "validation_session_count",
                    "feature_count",
                    "fallback_flag",
                    "fallback_reason",
                ],
                limit=6,
                empty_message="이번 주에 반영할 메타 모델 후보가 없습니다.",
                show_table_expander=False,
            )

        with st.form("apply_intraday_meta_form", clear_on_submit=False):
            meta_note = st.text_input(
                "메타 모델 반영 메모",
                value="주간 학습 결과를 확인하고 메타 모델을 교체함",
            )
            meta_confirm = st.checkbox("비교 내용을 확인했고, 현재 메타 모델을 이 후보로 바꾸는 데 동의합니다.")
            meta_submit = st.form_submit_button("메타 모델 바로 반영")
            if meta_submit:
                if not meta_confirm:
                    st.warning("먼저 비교 내용을 확인하고 동의 체크를 해주세요.")
                elif meta_apply_compare.empty:
                    st.warning("반영할 메타 모델 후보가 없습니다.")
                else:
                    candidate_dates = (
                        meta_apply_compare["train_end_date"].dropna().astype(str).tolist()
                        if "train_end_date" in meta_apply_compare.columns
                        else []
                    )
                    effective_date = (
                        max(candidate_dates)
                        if candidate_dates
                        else today_local(settings.app.timezone).isoformat()
                    )
                    meta_result = freeze_intraday_active_meta_model(
                        settings,
                        as_of_date=date.fromisoformat(effective_date),
                        source="scheduler_latest_training_candidate",
                        note=meta_note,
                        horizons=[1, 5],
                    )
                    st.success(
                        f"메타 모델 반영을 완료했습니다. run_id={meta_result.run_id} rows={meta_result.row_count}"
                    )

with policy_tab:
    render_data_sheet(
        policy_recommendation,
        title="이번 주 정책 추천",
        primary_column="policy_candidate_id",
        secondary_columns=["template_id", "recommendation_date"],
        detail_columns=[
            "horizon",
            "scope_type",
            "scope_key",
            "objective_score",
            "mean_realized_excess_return",
            "hit_rate",
            "execution_rate",
            "manual_review_required_flag",
        ],
        limit=6,
        empty_message="정책 추천 결과가 없습니다.",
    )
    render_data_sheet(
        policy_experiments,
        title="정책 실험 기록",
        primary_column="experiment_name",
        secondary_columns=["experiment_type", "created_at"],
        detail_columns=[
            "horizon",
            "candidate_count",
            "selected_policy_candidate_id",
            "fallback_used_flag",
            "status",
        ],
        limit=6,
        empty_message="정책 실험 결과가 없습니다.",
    )
    render_data_sheet(
        policy_walkforward,
        title="기간별 재검증",
        primary_column="template_id",
        secondary_columns=["split_name", "horizon"],
        detail_columns=[
            "scope_type",
            "scope_key",
            "objective_score",
            "mean_realized_excess_return",
            "hit_rate",
            "execution_rate",
            "manual_review_required_flag",
        ],
        limit=6,
        empty_message="기간별 재검증 결과가 없습니다.",
    )
    with st.expander("정책 발행 이력과 자세한 검증 보기", expanded=False):
        render_data_sheet(
            policy_publish_status,
            title="정책 발행 상태",
            primary_column="run_type",
            secondary_columns=["status"],
            detail_columns=["started_at", "finished_at", "notes"],
            limit=6,
            empty_message="정책 발행 상태가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            policy_rollbacks,
            title="정책 되돌리기 이력",
            primary_column="policy_candidate_id",
            secondary_columns=["promotion_type", "effective_from_date"],
            detail_columns=["horizon", "scope_type", "scope_key", "rollback_of_active_policy_id", "note"],
            limit=6,
            empty_message="정책 되돌리기 이력이 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            policy_calibration,
            title="검증용 세부 점수",
            primary_column="template_id",
            secondary_columns=["split_name", "horizon"],
            detail_columns=[
                "scope_type",
                "scope_key",
                "objective_score",
                "mean_realized_excess_return",
                "hit_rate",
                "execution_rate",
                "manual_review_required_flag",
            ],
            limit=6,
            empty_message="검증용 세부 점수가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            policy_ablation,
            title="항목 제거 실험",
            primary_column="ablation_name",
            secondary_columns=["horizon", "ablation_date"],
            detail_columns=[
                "mean_realized_excess_return_delta",
                "hit_rate_delta",
                "execution_rate_delta",
                "objective_score_delta",
            ],
            limit=6,
            empty_message="항목 제거 실험 결과가 없습니다.",
            show_table_expander=False,
        )

with meta_tab:
    meta_horizon = st.selectbox("메타 모델 기간", options=[1, 5], index=0, format_func=lambda value: f"{value}거래일")
    meta_panel = st.selectbox(
        "메타 모델 구분",
        options=[ENTER_PANEL, WAIT_PANEL],
        index=0,
        format_func=lambda value: "진입 판단" if value == ENTER_PANEL else "대기 판단",
    )
    meta_calibration = intraday_meta_calibration_frame(settings, horizon=meta_horizon, panel_name=meta_panel)
    meta_confusion = intraday_meta_confusion_matrix_frame(settings, horizon=meta_horizon, panel_name=meta_panel)
    meta_feature_importance = intraday_meta_feature_importance_frame(
        settings,
        horizon=meta_horizon,
        panel_name=meta_panel,
        limit=20,
    )
    render_data_sheet(
        meta_overlay,
        title="메타 보정 결과",
        limit=6,
        empty_message="메타 보정 비교 결과가 없습니다.",
    )
    render_data_sheet(
        meta_regime_breakdown,
        title="국면별 차이",
        limit=6,
        empty_message="국면별 차이 결과가 없습니다.",
    )
    render_data_sheet(
        meta_training_summary,
        title="메타 모델 학습 이력",
        primary_column="training_run_id",
        secondary_columns=["panel_name", "horizon"],
        detail_columns=[
            "train_end_date",
            "validation_row_count",
            "validation_session_count",
            "feature_count",
            "fallback_flag",
            "fallback_reason",
        ],
        limit=6,
        empty_message="메타 모델 학습 이력이 없습니다.",
    )
    with st.expander("메타 진단 자세히 보기", expanded=False):
        render_data_sheet(
            meta_calibration,
            title="메타 보정 세부 결과",
            limit=6,
            empty_message="메타 보정 결과가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            meta_confusion,
            title="혼동 행렬",
            limit=6,
            empty_message="혼동 행렬 결과가 없습니다.",
            show_table_expander=False,
        )
        render_data_sheet(
            meta_feature_importance,
            title="중요하게 본 특징",
            limit=6,
            empty_message="특징 중요도 결과가 없습니다.",
            show_table_expander=False,
        )

with summary_tab:
    render_data_sheet(
        intraday_capability,
        title="장중 리서치 준비 상태",
        limit=6,
        empty_message="장중 리서치 기능 상태가 없습니다.",
    )
    render_data_sheet(
        alpha_training_summary,
        title="최근 알파 학습 상태 요약",
        primary_column="horizon",
        secondary_columns=["train_end_date"],
        detail_columns=["train_row_count", "validation_row_count", "fallback_flag", "fallback_reason"],
        limit=6,
        empty_message="최근 알파 학습 상태가 없습니다.",
    )
    render_data_sheet(
        alpha_training_candidates,
        title="최근 알파 모델 학습 후보",
        primary_column="model_spec_id",
        secondary_columns=["horizon", "train_end_date"],
        detail_columns=[
            "training_run_id",
            "model_version",
            "validation_row_count",
            "fallback_flag",
            "fallback_reason",
        ],
        limit=6,
        empty_message="최근 알파 모델 학습 후보가 없습니다.",
    )
    render_data_sheet(
        meta_training_summary,
        title="최근 메타 모델 학습",
        primary_column="training_run_id",
        secondary_columns=["panel_name", "horizon"],
        detail_columns=[
            "train_end_date",
            "validation_row_count",
            "validation_session_count",
            "feature_count",
        ],
        limit=6,
        empty_message="최근 메타 모델 학습 이력이 없습니다.",
    )
    if policy_report_preview:
        with st.expander("최신 장중 정책 연구 리포트 미리보기", expanded=False):
            render_report_preview(
                title="장중 정책 연구 리포트 미리보기",
                preview=policy_report_preview,
            )

render_page_footer(settings, page_name="리서치")
