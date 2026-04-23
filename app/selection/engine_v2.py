from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import (
    D5_PRIMARY_FOCUS_MODEL_SPEC_ID,
    D5_PRIMARY_OUTPUT_CONTRACT_ROLES,
    SELECTION_ENGINE_VERSION,
    get_alpha_model_spec,
)
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.inference import materialize_alpha_predictions_v1
from app.ml.registry import load_active_alpha_model
from app.ranking.explanatory_score import (
    _component_score,
    _feature_inverse_rank,
    _feature_rank,
    _load_regime_map,
    upsert_ranking,
)
from app.ranking.grade_assignment import assign_grades
from app.ranking.reason_tags import build_eligibility_notes, build_reason_tags, build_risk_flags
from app.selection.engine_v1 import _apply_selection_engine_v1
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

SELECTION_V2_WEIGHTS = {
    1: {
        "alpha_core_score": 40,
        "relative_alpha_score": 14,
        "flow_persistence_score": 8,
        "flow_score": 10,
        "trend_momentum_score": 4,
        "news_catalyst_score": 3,
        "news_drift_score": 7,
        "quality_score": 4,
        "regime_fit_score": 6,
        "risk_penalty_score": -6,
        "uncertainty_score": -10,
        "disagreement_score": -8,
        "implementation_penalty_score": -8,
        "crowding_penalty_score": -14,
        "fallback_penalty": -4,
    },
    5: {
        "alpha_core_score": 38,
        "relative_alpha_score": 16,
        "flow_persistence_score": 10,
        "flow_score": 10,
        "trend_momentum_score": 5,
        "quality_score": 7,
        "value_safety_score": 8,
        "news_catalyst_score": 2,
        "news_drift_score": 4,
        "regime_fit_score": 6,
        "risk_penalty_score": -6,
        "uncertainty_score": -10,
        "disagreement_score": -10,
        "implementation_penalty_score": -8,
        "crowding_penalty_score": -12,
        "fallback_penalty": -4,
    },
}

SELECTION_V2_TOP5_FOCUS_WEIGHTS = {
    1: {
        "alpha_core_score": 52,
        "relative_alpha_score": 10,
        "flow_persistence_score": 8,
        "flow_score": 8,
        "trend_momentum_score": 2,
        "news_catalyst_score": 2,
        "news_drift_score": 5,
        "quality_score": 3,
        "regime_fit_score": 4,
        "risk_penalty_score": -4,
        "uncertainty_score": -6,
        "disagreement_score": -5,
        "implementation_penalty_score": -5,
        "crowding_penalty_score": -8,
        "fallback_penalty": -3,
    },
    5: {
        "alpha_core_score": 50,
        "relative_alpha_score": 12,
        "flow_persistence_score": 8,
        "flow_score": 8,
        "trend_momentum_score": 2,
        "quality_score": 5,
        "value_safety_score": 5,
        "news_catalyst_score": 1,
        "news_drift_score": 3,
        "regime_fit_score": 4,
        "risk_penalty_score": -4,
        "uncertainty_score": -6,
        "disagreement_score": -6,
        "implementation_penalty_score": -5,
        "crowding_penalty_score": -8,
        "fallback_penalty": -3,
    },
}

SELECTION_V2_D5_PRIMARY_WEIGHTS = {
    5: {
        "alpha_core_score": 44,
        "output_contract_support_score": 6,
        "relative_alpha_score": 18,
        "flow_persistence_score": 12,
        "flow_score": 8,
        "trend_momentum_score": 3,
        "quality_score": 7,
        "value_safety_score": 8,
        "news_catalyst_score": 1,
        "news_drift_score": 2,
        "regime_fit_score": 6,
        "risk_penalty_score": -5,
        "uncertainty_score": -5,
        "disagreement_score": -2,
        "implementation_penalty_score": -5,
        "crowding_penalty_score": -7,
        "late_entry_penalty_score": -7,
        "fallback_penalty": -3,
    },
}

SELECTION_V2_TOPBUCKET_WEIGHTS = {
    1: {
        "alpha_core_score": 46,
        "relative_alpha_score": 12,
        "flow_persistence_score": 9,
        "flow_score": 8,
        "trend_momentum_score": 2,
        "news_catalyst_score": 2,
        "news_drift_score": 6,
        "quality_score": 4,
        "regime_fit_score": 5,
        "risk_penalty_score": -4,
        "uncertainty_score": -5,
        "disagreement_score": -4,
        "implementation_penalty_score": -4,
        "crowding_penalty_score": -7,
        "fallback_penalty": -3,
    },
    5: {
        "alpha_core_score": 44,
        "relative_alpha_score": 12,
        "flow_persistence_score": 8,
        "flow_score": 8,
        "trend_momentum_score": 3,
        "quality_score": 6,
        "value_safety_score": 6,
        "news_catalyst_score": 2,
        "news_drift_score": 4,
        "regime_fit_score": 5,
        "risk_penalty_score": -4,
        "uncertainty_score": -5,
        "disagreement_score": -5,
        "implementation_penalty_score": -4,
        "crowding_penalty_score": -7,
        "fallback_penalty": -3,
    },
}

D5_RAW_PRESERVATION_PRIORITY_COUNT = 5


def _resolve_selection_weights(
    *,
    horizon: int,
    model_spec_id: str | None,
    target_variant: str | None,
) -> dict[str, float]:
    if (
        model_spec_id == D5_PRIMARY_FOCUS_MODEL_SPEC_ID
        and int(horizon) in SELECTION_V2_D5_PRIMARY_WEIGHTS
    ):
        return dict(SELECTION_V2_D5_PRIMARY_WEIGHTS[int(horizon)])
    if target_variant == "top5_binary":
        return dict(SELECTION_V2_TOP5_FOCUS_WEIGHTS[int(horizon)])
    if target_variant == "top20_weighted":
        return dict(SELECTION_V2_TOPBUCKET_WEIGHTS[int(horizon)])
    return dict(SELECTION_V2_WEIGHTS[int(horizon)])


@dataclass(slots=True)
class SelectionEngineV2Result:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str
    ranking_version: str


SELECTION_V2_RANKING_OUTPUT_COLUMNS: tuple[str, ...] = (
    "run_id",
    "as_of_date",
    "symbol",
    "horizon",
    "final_selection_value",
    "final_selection_rank_pct",
    "grade",
    "report_candidate_flag",
    "explanatory_score_json",
    "top_reason_tags_json",
    "risk_flags_json",
    "eligible_flag",
    "eligibility_notes_json",
    "regime_state",
    "ranking_version",
    "created_at",
)


def _resolve_model_spec_context(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    model_spec_ids = {
        str(value)
        for value in pd.Series(frame.get("model_spec_id")).dropna().astype(str).tolist()
    }
    if len(model_spec_ids) != 1:
        return None, None
    model_spec_id = next(iter(model_spec_ids))
    try:
        target_variant = get_alpha_model_spec(model_spec_id).target_variant
    except KeyError:
        target_variant = None
    return model_spec_id, target_variant


def _resolve_report_candidate_limit(
    *,
    model_spec_id: str | None,
    target_variant: str | None,
    horizon: int,
) -> int | None:
    if target_variant == "top5_binary":
        return 5
    if model_spec_id == "alpha_topbucket_h1_rolling_120_v1" and int(horizon) == 1:
        return 5
    if target_variant == "top20_weighted":
        return 10
    return None


def _select_report_candidate_mask(
    scored: pd.DataFrame,
    *,
    model_spec_id: str | None,
    target_variant: str | None,
    horizon: int,
) -> pd.Series:
    candidate_limit = _resolve_report_candidate_limit(
        model_spec_id=model_spec_id,
        target_variant=target_variant,
        horizon=horizon,
    )
    if candidate_limit is None:
        eligible_mask = scored["eligible_flag"].fillna(False).astype(bool)
        return eligible_mask & scored["final_selection_rank_pct"].fillna(0.0).ge(0.85)
    candidate_mask = pd.Series(False, index=scored.index)
    top_candidate_indices = (
        scored.sort_values(["final_selection_value", "symbol"], ascending=[False, True])
        .head(candidate_limit)
        .index
    )
    candidate_mask.loc[top_candidate_indices] = True
    return candidate_mask


def _augment_reason_tags(row: pd.Series, tags: list[str]) -> list[str]:
    values = []
    if row.get("relative_alpha_score", 0) >= 60:
        values.append("residual_strength_improving")
    if row.get("flow_persistence_score", 0) >= 60:
        values.append("flow_persistence_supportive")
    if row.get("news_drift_score", 0) >= 60:
        values.append("news_drift_underreacted")
    if row.get("crowding_penalty_score", 100) <= 45:
        values.append("crowding_risk_low")
    if row.get("raw_preservation_guardrail_applied", False):
        values.append("raw_alpha_leader_preserved")
    values.extend(tags)
    if pd.notna(row.get("expected_excess_return")) and float(row["expected_excess_return"]) > 0:
        values.append("ml_alpha_supportive")
    if bool(row.get("fallback_flag")):
        values.append("prediction_fallback_used")
    deduped = []
    for item in values:
        if item not in deduped:
            deduped.append(item)
    return deduped[:3]


def _augment_risk_flags(row: pd.Series, flags: list[str]) -> list[str]:
    values = list(flags)
    if pd.to_numeric(row.get("uncertainty_score"), errors="coerce") >= 70:
        values.append("model_uncertainty_high")
    if pd.to_numeric(row.get("disagreement_score"), errors="coerce") >= 70:
        values.append("model_disagreement_high")
    if bool(row.get("fallback_flag")):
        values.append("prediction_fallback")
    return sorted(set(values))


def _alpha_core_rank_score(frame: pd.DataFrame) -> pd.Series:
    expected = pd.to_numeric(frame["expected_excess_return"], errors="coerce")
    if expected.notna().sum() <= 1:
        return pd.Series(50.0, index=frame.index)
    return expected.rank(method="average", pct=True).mul(100.0).fillna(50.0)


def _alpha_core_magnitude_score(frame: pd.DataFrame) -> pd.Series:
    expected = pd.to_numeric(frame["expected_excess_return"], errors="coerce")
    if expected.notna().sum() <= 1:
        return pd.Series(50.0, index=frame.index)
    positive_expected = expected.clip(lower=0.0)
    positive_max = positive_expected.max(skipna=True)
    if pd.notna(positive_max) and float(positive_max) > 0.0:
        return positive_expected.div(float(positive_max)).mul(100.0).fillna(0.0)
    minimum = expected.min(skipna=True)
    maximum = expected.max(skipna=True)
    if pd.isna(minimum) or pd.isna(maximum) or float(maximum) <= float(minimum):
        return pd.Series(50.0, index=frame.index)
    return (
        expected.sub(float(minimum))
        .div(float(maximum) - float(minimum))
        .mul(100.0)
        .clip(lower=0.0, upper=100.0)
        .fillna(50.0)
    )


def _alpha_core_score(
    frame: pd.DataFrame,
    *,
    d5_primary_focus: bool = False,
) -> pd.Series:
    rank_component = _alpha_core_rank_score(frame)
    if not d5_primary_focus:
        return rank_component
    magnitude_component = _alpha_core_magnitude_score(frame)
    return (
        rank_component.mul(0.55).add(magnitude_component.mul(0.45))
    ).clip(lower=0.0, upper=100.0)


def _output_contract_rank_score(frame: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name not in frame.columns:
        return pd.Series(0.5, index=frame.index)
    values = pd.to_numeric(frame.get(column_name), errors="coerce")
    if values.notna().sum() <= 1:
        return pd.Series(0.5, index=frame.index)
    return values.rank(method="average", pct=True).fillna(0.5)


def _compute_output_contract_support_score(frame: pd.DataFrame) -> pd.Series:
    return _component_score(
        _output_contract_rank_score(frame, "lower_band"),
        _output_contract_rank_score(frame, "median_band"),
    )


def _compute_relative_alpha_score(frame: pd.DataFrame, *, horizon: int) -> pd.Series:
    if horizon == 1:
        return _component_score(
            _feature_rank(frame, "residual_ret_3d"),
            _feature_rank(frame, "residual_ret_5d"),
            _feature_rank(frame, "residual_ret_10d"),
        )
    return _component_score(
        _feature_rank(frame, "residual_ret_5d"),
        _feature_rank(frame, "residual_ret_10d"),
        _feature_rank(frame, "drawdown_20d"),
    )


def _compute_flow_persistence_score(frame: pd.DataFrame) -> pd.Series:
    return _component_score(
        _feature_rank(frame, "foreign_flow_persistence_5d"),
        _feature_rank(frame, "institution_flow_persistence_5d"),
        _feature_inverse_rank(frame, "flow_disagreement_score"),
    )


def _compute_news_drift_score(frame: pd.DataFrame) -> pd.Series:
    return _component_score(
        _feature_rank(frame, "news_drift_persistence_score"),
        _feature_inverse_rank(frame, "news_burst_share_1d"),
        _feature_rank(frame, "distinct_publishers_3d"),
    )


def _compute_crowding_penalty_score(frame: pd.DataFrame, *, horizon: int) -> pd.Series:
    if horizon == 1:
        return _component_score(
            _feature_rank(frame, "ret_5d"),
            _feature_rank(frame, "dist_from_20d_high"),
            _feature_rank(frame, "turnover_z_5_20"),
            _feature_rank(frame, "news_burst_share_1d"),
        )
    return _component_score(
        _feature_rank(frame, "ret_10d"),
        _feature_rank(frame, "dist_from_20d_high"),
        _feature_rank(frame, "turnover_burst_persistence_5d"),
        _feature_rank(frame, "news_burst_share_1d"),
    )


def _compute_late_entry_penalty_score(frame: pd.DataFrame) -> pd.Series:
    crowding = pd.to_numeric(frame.get("crowding_penalty_score"), errors="coerce").fillna(50.0)
    alpha_core_source = frame.get("alpha_core_score")
    if alpha_core_source is None:
        alpha_core = pd.Series(50.0, index=frame.index)
    else:
        alpha_core = pd.to_numeric(alpha_core_source, errors="coerce").fillna(50.0)
    relative_alpha = pd.to_numeric(frame.get("relative_alpha_score"), errors="coerce").fillna(50.0)
    flow_persistence = pd.to_numeric(
        frame.get("flow_persistence_score"),
        errors="coerce",
    ).fillna(50.0)
    news_drift = pd.to_numeric(frame.get("news_drift_score"), errors="coerce").fillna(50.0)
    weakness_score = (
        alpha_core.rsub(58.0).clip(lower=0.0).div(58.0).mul(0.40)
        + relative_alpha.rsub(55.0).clip(lower=0.0).div(55.0).mul(0.25)
        + flow_persistence.rsub(55.0).clip(lower=0.0).div(55.0).mul(0.20)
        + news_drift.rsub(52.0).clip(lower=0.0).div(52.0).mul(0.15)
    ).clip(lower=0.0, upper=1.0)
    weak_signal_share = (
        alpha_core.lt(58.0).astype(float)
        + relative_alpha.lt(55.0).astype(float)
        + flow_persistence.lt(55.0).astype(float)
        + news_drift.lt(52.0).astype(float)
    ).div(4.0)
    alpha_relief = (
        alpha_core.sub(75.0).clip(lower=0.0).div(25.0).mul(0.45)
        + relative_alpha.sub(65.0).clip(lower=0.0).div(35.0).mul(0.25)
        + flow_persistence.sub(60.0).clip(lower=0.0).div(40.0).mul(0.15)
        + news_drift.sub(60.0).clip(lower=0.0).div(40.0).mul(0.15)
    ).clip(lower=0.0, upper=0.80)
    crowding_gate = crowding.sub(55.0).clip(lower=0.0).div(45.0).clip(lower=0.0, upper=1.0)
    return (
        crowding_gate.mul(100.0)
        * weakness_score
        * weak_signal_share.pow(1.5)
        * (1.0 - alpha_relief)
    ).clip(lower=0.0, upper=100.0)


def _top_selection_indices(scored: pd.DataFrame, *, limit: int) -> pd.Index:
    return (
        scored.sort_values(["final_selection_value", "symbol"], ascending=[False, True])
        .head(int(limit))
        .index
    )


def _compute_d5_raw_preservation_blocker_mask(scored: pd.DataFrame) -> pd.Series:
    eligible_flag = scored.get("eligible_flag")
    if eligible_flag is None:
        eligible = pd.Series(False, index=scored.index)
    else:
        eligible = eligible_flag.fillna(False).astype(bool)
    critical_risk = pd.to_numeric(
        scored.get("critical_risk_flag"),
        errors="coerce",
    ).fillna(0.0).astype(bool)
    fallback_flag = pd.to_numeric(
        scored.get("fallback_flag"),
        errors="coerce",
    ).fillna(0.0).astype(bool)
    uncertainty = pd.to_numeric(scored.get("uncertainty_score"), errors="coerce").fillna(0.0)
    return (
        ~eligible
        | critical_risk
        | (fallback_flag & uncertainty.ge(75.0))
        | uncertainty.ge(85.0)
    )


def _apply_d5_raw_preservation_guardrail(scored: pd.DataFrame) -> pd.DataFrame:
    guarded = scored.copy()
    guarded["raw_top5_candidate_flag"] = False
    guarded["raw_preservation_bonus"] = 0.0
    guarded["raw_preservation_guardrail_applied"] = False
    guarded["raw_preservation_blocker_flag"] = False

    if guarded.empty:
        return guarded

    raw_top_indices = (
        guarded.assign(
            _expected_excess_return=pd.to_numeric(
                guarded["expected_excess_return"],
                errors="coerce",
            )
        )
        .sort_values(
            ["_expected_excess_return", "symbol"],
            ascending=[False, True],
            na_position="last",
        )
        .head(5)
        .index
    )
    guarded.loc[raw_top_indices, "raw_top5_candidate_flag"] = True
    blocker_mask = _compute_d5_raw_preservation_blocker_mask(guarded)
    guarded["raw_preservation_blocker_flag"] = blocker_mask

    preservable_raw_indices = [
        index for index in raw_top_indices if not bool(blocker_mask.loc[index])
    ]
    if not preservable_raw_indices:
        return guarded

    priority_preservable_indices = preservable_raw_indices[
        :D5_RAW_PRESERVATION_PRIORITY_COUNT
    ]
    priority_preservable_set = set(priority_preservable_indices)

    current_top_indices = _top_selection_indices(guarded, limit=5)
    replaceable_top_indices = [
        top_index
        for top_index in current_top_indices
        if top_index not in priority_preservable_set
    ]
    if replaceable_top_indices:
        reserve_anchor = max(
            float(guarded.loc[index, "final_selection_value"])
            for index in replaceable_top_indices
        )

        for reverse_offset, index in enumerate(reversed(priority_preservable_indices), start=1):
            current_value = float(guarded.loc[index, "final_selection_value"])
            target_value = min(100.0, reserve_anchor + (0.01 * reverse_offset))
            bonus = max(0.0, target_value - current_value)
            if bonus <= 0.0:
                continue
            guarded.loc[index, "final_selection_value"] = min(100.0, current_value + bonus)
            guarded.loc[index, "raw_preservation_bonus"] = bonus

    final_top_indices = set(_top_selection_indices(guarded, limit=5))
    applied_indices = final_top_indices.intersection(priority_preservable_set)
    guarded.loc[list(applied_indices), "raw_preservation_guardrail_applied"] = True
    guarded["final_selection_rank_pct"] = guarded["final_selection_value"].rank(
        method="average",
        pct=True,
    )
    return guarded


def _attach_regime_context(
    feature_matrix: pd.DataFrame,
    *,
    regime_map: dict[str, dict[str, object]],
) -> pd.DataFrame:
    working = feature_matrix.copy()
    working["regime_state"] = working["market"].map(
        lambda value: regime_map.get(str(value).upper(), regime_map["KR_ALL"])["regime_state"]
    )
    working["regime_score"] = working["market"].map(
        lambda value: regime_map.get(str(value).upper(), regime_map["KR_ALL"])["regime_score"]
    )
    return working


def _score_selection_engine_v2_frame(
    base: pd.DataFrame,
    prediction_frame: pd.DataFrame,
    *,
    horizon: int,
    settings: Settings,
) -> pd.DataFrame:
    scored = base.merge(prediction_frame, on="symbol", how="left")
    model_spec_id, target_variant = _resolve_model_spec_context(scored)
    d5_primary_focus = (
        model_spec_id == D5_PRIMARY_FOCUS_MODEL_SPEC_ID and int(horizon) == 5
    )
    scored["alpha_core_rank_component_score"] = _alpha_core_rank_score(scored)
    scored["alpha_core_magnitude_component_score"] = (
        _alpha_core_magnitude_score(scored)
        if d5_primary_focus
        else pd.Series(float("nan"), index=scored.index, dtype="float64")
    )
    scored["alpha_core_score"] = _alpha_core_score(scored, d5_primary_focus=d5_primary_focus)
    if d5_primary_focus:
        scored["output_contract_support_score"] = _compute_output_contract_support_score(scored)
    else:
        scored["output_contract_support_score"] = pd.NA
    scored["relative_alpha_score"] = _compute_relative_alpha_score(scored, horizon=horizon)
    scored["flow_persistence_score"] = _compute_flow_persistence_score(scored)
    scored["news_drift_score"] = _compute_news_drift_score(scored)
    scored["crowding_penalty_score"] = _compute_crowding_penalty_score(scored, horizon=horizon)
    scored["uncertainty_score"] = pd.to_numeric(
        scored["uncertainty_score"],
        errors="coerce",
    ).fillna(pd.to_numeric(scored["uncertainty_proxy_score"], errors="coerce"))
    scored["disagreement_score"] = pd.to_numeric(
        scored["disagreement_score"],
        errors="coerce",
    )
    disagreement_fill = (
        scored["disagreement_score"].rank(pct=True).mul(100.0)
        if scored["disagreement_score"].notna().any()
        else pd.Series(pd.NA, index=scored.index)
    )
    scored["disagreement_score"] = scored["disagreement_score"].fillna(disagreement_fill)
    scored["fallback_flag"] = scored["fallback_flag"].fillna(False).astype(bool)
    scored["fallback_reason"] = scored["fallback_reason"].fillna("")
    weights = _resolve_selection_weights(
        horizon=int(horizon),
        model_spec_id=model_spec_id,
        target_variant=target_variant,
    )
    if d5_primary_focus:
        scored["late_entry_penalty_score"] = _compute_late_entry_penalty_score(scored)
    else:
        scored["late_entry_penalty_score"] = pd.NA
    alpha_positive_components = {key: value for key, value in weights.items() if value > 0}
    positive_score = sum(
        pd.to_numeric(scored[name], errors="coerce").fillna(50.0) * weight
        for name, weight in alpha_positive_components.items()
    ) / sum(alpha_positive_components.values())
    risk_penalty = (
        pd.to_numeric(scored["risk_penalty_score"], errors="coerce").fillna(50.0)
        * abs(weights["risk_penalty_score"])
        / 100.0
    )
    uncertainty_penalty = (
        pd.to_numeric(scored["uncertainty_score"], errors="coerce").fillna(50.0)
        * abs(weights["uncertainty_score"])
        * settings.model.uncertainty_lambda
        / 100.0
    )
    disagreement_penalty = (
        pd.to_numeric(scored["disagreement_score"], errors="coerce").fillna(50.0)
        * abs(weights["disagreement_score"])
        * settings.model.disagreement_eta
        / 100.0
    )
    implementation_penalty = (
        pd.to_numeric(scored["implementation_penalty_score"], errors="coerce").fillna(50.0)
        * abs(weights["implementation_penalty_score"])
        * settings.model.implementation_kappa
        / 100.0
    )
    crowding_penalty = (
        pd.to_numeric(scored["crowding_penalty_score"], errors="coerce").fillna(50.0)
        * abs(weights["crowding_penalty_score"])
        / 100.0
    )
    late_entry_penalty = (
        pd.to_numeric(scored["late_entry_penalty_score"], errors="coerce").fillna(0.0)
        * abs(weights.get("late_entry_penalty_score", 0.0))
        / 100.0
    )
    fallback_penalty = scored["fallback_flag"].astype(float) * abs(weights["fallback_penalty"])
    scored["final_selection_value"] = (
        positive_score
        - risk_penalty
        - uncertainty_penalty
        - disagreement_penalty
        - implementation_penalty
        - crowding_penalty
        - late_entry_penalty
        - fallback_penalty
    ).clip(lower=0.0, upper=100.0)
    scored["final_selection_rank_pct"] = scored["final_selection_value"].rank(
        method="average",
        pct=True,
    )

    risk_flags = scored.apply(build_risk_flags, axis=1)
    risk_flags = pd.Series(
        [
            _augment_risk_flags(row, values)
            for (_, row), values in zip(scored.iterrows(), risk_flags, strict=False)
        ],
        index=scored.index,
    )
    scored["critical_risk_flag"] = risk_flags.map(
        lambda values: any(
            flag
            in {
                "high_realized_volatility",
                "large_recent_drawdown",
                "model_uncertainty_high",
            }
            for flag in values
        )
    )
    scored["grade"] = assign_grades(scored)
    if d5_primary_focus:
        scored = _apply_d5_raw_preservation_guardrail(scored)
    else:
        scored["raw_top5_candidate_flag"] = False
        scored["raw_preservation_bonus"] = 0.0
        scored["raw_preservation_guardrail_applied"] = False
        scored["raw_preservation_blocker_flag"] = False
    scored["report_candidate_flag"] = _select_report_candidate_mask(
        scored,
        model_spec_id=model_spec_id,
        target_variant=target_variant,
        horizon=int(horizon),
    )
    scored["risk_flags_json"] = risk_flags.map(
        lambda values: json.dumps(values, ensure_ascii=False)
    )
    scored["top_reason_tags_json"] = scored.apply(
        lambda row: json.dumps(
            _augment_reason_tags(row, build_reason_tags(row)),
            ensure_ascii=False,
        ),
        axis=1,
    )
    scored["eligibility_notes_json"] = scored.apply(
        lambda row: build_eligibility_notes(
            row,
            risk_flags=json.loads(str(row["risk_flags_json"])),
        ),
        axis=1,
    )
    weight_payload = dict(weights)
    output_contract_roles = (
        dict(D5_PRIMARY_OUTPUT_CONTRACT_ROLES) if d5_primary_focus else None
    )
    scored["explanatory_score_json"] = scored.apply(
        lambda row,
        weight_payload=weight_payload,
        output_contract_roles=output_contract_roles: json.dumps(
            {
                "alpha_core_score": float(row["alpha_core_score"]),
                "output_contract_support_score": None
                if pd.isna(row["output_contract_support_score"])
                else float(row["output_contract_support_score"]),
                "alpha_core_rank_component_score": float(row["alpha_core_rank_component_score"]),
                "alpha_core_magnitude_component_score": None
                if pd.isna(row["alpha_core_magnitude_component_score"])
                else float(row["alpha_core_magnitude_component_score"]),
                "relative_alpha_score": float(row["relative_alpha_score"]),
                "expected_excess_return": None
                if pd.isna(row["expected_excess_return"])
                else float(row["expected_excess_return"]),
                "flow_score": float(row["flow_score"]),
                "flow_persistence_score": float(row["flow_persistence_score"]),
                "trend_momentum_score": float(row["trend_momentum_score"]),
                "news_drift_score": float(row["news_drift_score"]),
                "crowding_penalty_score": float(row["crowding_penalty_score"]),
                "late_entry_penalty_score": None
                if pd.isna(row["late_entry_penalty_score"])
                else float(row["late_entry_penalty_score"]),
                "quality_score": float(row["quality_score"]),
                "value_safety_score": float(row["value_safety_score"]),
                "regime_fit_score": float(row["regime_fit_score"]),
                "risk_penalty_score": float(row["risk_penalty_score"]),
                "uncertainty_score": None
                if pd.isna(row["uncertainty_score"])
                else float(row["uncertainty_score"]),
                "disagreement_score": None
                if pd.isna(row["disagreement_score"])
                else float(row["disagreement_score"]),
                "implementation_penalty_score": float(row["implementation_penalty_score"]),
                "fallback_flag": bool(row["fallback_flag"]),
                "fallback_reason": row["fallback_reason"] or None,
                "raw_top5_candidate_flag": bool(row["raw_top5_candidate_flag"]),
                "raw_preservation_bonus": float(row["raw_preservation_bonus"]),
                "raw_preservation_guardrail_applied": bool(
                    row["raw_preservation_guardrail_applied"]
                ),
                "raw_preservation_blocker_flag": bool(row["raw_preservation_blocker_flag"]),
                "prediction_version": row.get("prediction_version"),
                "score_version": SELECTION_ENGINE_VERSION,
                "score_type": "selection_engine_v2",
                "active_weights": weight_payload,
                "output_contract_roles": output_contract_roles,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        axis=1,
    )
    return scored


def build_selection_engine_v2_rankings(
    *,
    feature_matrix: pd.DataFrame,
    as_of_date: date,
    horizons: list[int],
    regime_map: dict[str, dict[str, object]],
    prediction_frames_by_horizon: dict[int, pd.DataFrame],
    run_id: str,
    settings: Settings,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    output_columns: tuple[str, ...] = SELECTION_V2_RANKING_OUTPUT_COLUMNS,
) -> list[pd.DataFrame]:
    feature_with_regime = _attach_regime_context(feature_matrix, regime_map=regime_map)
    ranking_frames: list[pd.DataFrame] = []
    for horizon in horizons:
        base = _apply_selection_engine_v1(feature_with_regime, horizon=horizon, settings=settings)
        prediction_frame = prediction_frames_by_horizon.get(int(horizon), pd.DataFrame())
        if (
            not prediction_frame.empty
            and "model_spec_id" in prediction_frame.columns
            and prediction_frame["model_spec_id"].dropna().nunique() > 1
        ):
            for _model_spec_id, spec_prediction_frame in prediction_frame.groupby(
                "model_spec_id",
                dropna=False,
            ):
                scored = _score_selection_engine_v2_frame(
                    base,
                    spec_prediction_frame.copy(),
                    horizon=int(horizon),
                    settings=settings,
                )
                scored["run_id"] = run_id
                scored["as_of_date"] = as_of_date
                scored["ranking_version"] = ranking_version
                scored["created_at"] = pd.Timestamp.utcnow()
                scored["horizon"] = int(horizon)
                for column in output_columns:
                    if column not in scored.columns:
                        scored[column] = pd.NA
                ranking_frames.append(scored.loc[:, list(output_columns)].copy())
        else:
            scored = _score_selection_engine_v2_frame(
                base,
                prediction_frame,
                horizon=int(horizon),
                settings=settings,
            )
            scored["run_id"] = run_id
            scored["as_of_date"] = as_of_date
            scored["ranking_version"] = ranking_version
            scored["created_at"] = pd.Timestamp.utcnow()
            scored["horizon"] = int(horizon)
            for column in output_columns:
                if column not in scored.columns:
                    scored[column] = pd.NA
            ranking_frames.append(scored.loc[:, list(output_columns)].copy())
    return ranking_frames


def _load_predictions(connection, *, as_of_date: date, horizon: int) -> pd.DataFrame:
    active_model = load_active_alpha_model(
        connection,
        as_of_date=as_of_date,
        horizon=int(horizon),
    )
    parameters: list[object] = [
        as_of_date,
        int(horizon),
        ALPHA_PREDICTION_VERSION,
        SELECTION_ENGINE_VERSION,
    ]
    active_filter = ""
    if active_model is not None and active_model.get("active_alpha_model_id") not in (None, ""):
        active_filter = "AND active_alpha_model_id = ?"
        parameters.append(str(active_model["active_alpha_model_id"]))
    return connection.execute(
        f"""
        SELECT
            symbol,
            expected_excess_return,
            lower_band,
            median_band,
            upper_band,
            uncertainty_score,
            disagreement_score,
            fallback_flag,
            fallback_reason,
            prediction_version,
            model_spec_id,
            active_alpha_model_id,
            member_count,
            ensemble_weight_json,
            source_notes_json
        FROM fact_prediction
        WHERE as_of_date = ?
          AND horizon = ?
          AND prediction_version = ?
          AND ranking_version = ?
          {active_filter}
        """,
        parameters,
    ).fetchdf()


def materialize_selection_engine_v2(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
    ensure_predictions: bool = True,
) -> SelectionEngineV2Result:
    ensure_storage_layout(settings)
    if ensure_predictions:
        materialize_alpha_predictions_v1(
            settings,
            as_of_date=as_of_date,
            horizons=horizons,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
        )

    with activate_run_context(
        "materialize_selection_engine_v2", as_of_date=as_of_date
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_feature_snapshot",
                    "fact_prediction",
                    "fact_market_regime_snapshot",
                ],
                notes=(
                    "Materialize selection engine v2. "
                    f"as_of_date={as_of_date.isoformat()} horizons={horizons}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                from app.features.feature_store import load_feature_matrix

                feature_matrix = load_feature_matrix(
                    connection,
                    as_of_date=as_of_date,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                )
                if feature_matrix.empty:
                    raise RuntimeError(
                        "Feature snapshot is missing for selection engine v2. "
                        "Run scripts/build_feature_store.py first."
                    )
                regime_map = _load_regime_map(connection, as_of_date=as_of_date)
                prediction_frames_by_horizon: dict[int, pd.DataFrame] = {}
                artifact_paths: list[str] = []
                for horizon in horizons:
                    prediction_frames_by_horizon[int(horizon)] = _load_predictions(
                        connection,
                        as_of_date=as_of_date,
                        horizon=int(horizon),
                    )
                ranking_frames = build_selection_engine_v2_rankings(
                    feature_matrix=feature_matrix,
                    as_of_date=as_of_date,
                    horizons=horizons,
                    regime_map=regime_map,
                    prediction_frames_by_horizon=prediction_frames_by_horizon,
                    run_id=run_context.run_id,
                    settings=settings,
                )
                for ranking_output in ranking_frames:
                    horizon = int(ranking_output["horizon"].iloc[0])
                    artifact_paths.append(
                        str(
                            write_parquet(
                                ranking_output,
                                base_dir=settings.paths.curated_dir,
                                dataset="ranking",
                                partitions={
                                    "as_of_date": as_of_date.isoformat(),
                                    "horizon": str(int(horizon)),
                                    "ranking_version": SELECTION_ENGINE_VERSION,
                                },
                                filename="selection_engine_v2.parquet",
                            )
                        )
                    )

                combined = pd.concat(ranking_frames, ignore_index=True)
                if force:
                    connection.execute(
                        """
                        DELETE FROM fact_ranking
                        WHERE as_of_date = ?
                          AND ranking_version = ?
                        """,
                        [as_of_date, SELECTION_ENGINE_VERSION],
                    )
                upsert_ranking(connection, combined)
                notes = (
                    "Selection engine v2 materialized. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(combined)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=ALPHA_PREDICTION_VERSION,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return SelectionEngineV2Result(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(combined),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Selection engine v2 materialization failed.",
                    error_message=str(exc),
                    model_version=ALPHA_PREDICTION_VERSION,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
