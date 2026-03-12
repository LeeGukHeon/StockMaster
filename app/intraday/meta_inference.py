from __future__ import annotations

# ruff: noqa: E501
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from app.common.artifacts import resolve_artifact_path
from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.ml.registry import load_model_artifact
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

from .meta_common import (
    ENTER_PANEL,
    HARD_GUARD_ACTIONS,
    INTRADAY_META_MODEL_DOMAIN,
    INTRADAY_META_MODEL_VERSION,
    KEEP_CLASS_BY_PANEL,
    PANEL_ACTION_MAP,
    WAIT_PANEL,
    IntradayMetaDecisionResult,
    IntradayMetaEvaluationResult,
    IntradayMetaPredictionResult,
    feature_frame_with_dummies,
    json_or_none,
    upsert_intraday_meta_decision,
    upsert_intraday_meta_prediction,
    upsert_meta_metric_summary,
)
from .meta_dataset import (
    assemble_intraday_meta_dataset_frame,
    ensure_intraday_meta_label_inputs,
)
from .policy import apply_active_intraday_policy_frame


def _panel_name_for_action(action: object) -> str | None:
    if action == "ENTER_NOW":
        return ENTER_PANEL
    if action == "WAIT_RECHECK":
        return WAIT_PANEL
    return None


def _load_active_meta_model_registry(
    connection,
    *,
    as_of_date: date,
    horizons: list[int],
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_active_meta_model
        WHERE effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
          AND active_flag = TRUE
          AND horizon IN ({placeholders})
        ORDER BY effective_from_date DESC, updated_at DESC
        """,
        [as_of_date, as_of_date, *horizons],
    ).fetchdf()


def _load_training_artifact_rows(connection, *, training_run_ids: list[str]) -> pd.DataFrame:
    if not training_run_ids:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in training_run_ids)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_model_training_run
        WHERE training_run_id IN ({placeholders})
          AND model_domain = ?
          AND model_version = ?
        """,
        [*training_run_ids, INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION],
    ).fetchdf()


def _apply_probability_calibration(
    probability_frame: pd.DataFrame,
    *,
    calibration_params: dict[str, dict[str, float] | None],
) -> pd.DataFrame:
    calibrated = probability_frame.copy()
    for class_label, params in calibration_params.items():
        if params is None or class_label not in calibrated.columns:
            continue
        logit = params["coef"] * calibrated[class_label] + params["intercept"]
        calibrated[class_label] = 1.0 / (1.0 + np.exp(-logit))
    row_sums = calibrated.sum(axis=1).replace(0.0, 1.0)
    return calibrated.div(row_sums, axis=0)


def _action_from_prediction(panel_name: str, predicted_class: str, tuned_action: str) -> str:
    if panel_name == ENTER_PANEL:
        mapping = {
            "KEEP_ENTER": "ENTER_NOW",
            "DOWNGRADE_WAIT": "WAIT_RECHECK",
            "DOWNGRADE_AVOID": "AVOID_TODAY",
        }
    elif panel_name == WAIT_PANEL:
        mapping = {
            "KEEP_WAIT": "WAIT_RECHECK",
            "UPGRADE_ENTER": "ENTER_NOW",
            "DOWNGRADE_AVOID": "AVOID_TODAY",
        }
    else:
        return tuned_action
    return mapping.get(predicted_class, tuned_action)


def _policy_action_outcome(row: pd.Series, action: object) -> tuple[float | None, float | None]:
    if action == "ENTER_NOW":
        return (
            None if pd.isna(row.get("current_realized_excess_return")) else float(row["current_realized_excess_return"]),
            None if pd.isna(row.get("current_timing_edge_vs_open_bps")) else float(row["current_timing_edge_vs_open_bps"]),
        )
    if action == "WAIT_RECHECK":
        later_excess = row.get("later_enter_realized_excess_return")
        later_edge = row.get("later_enter_timing_edge_vs_open_bps")
        if pd.notna(later_excess):
            return float(later_excess), None if pd.isna(later_edge) else float(later_edge)
    no_entry = row.get("no_entry_realized_excess_return")
    return None if pd.isna(no_entry) else float(no_entry), 0.0


def materialize_intraday_meta_predictions(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    symbol: str | None = None,
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayMetaPredictionResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "materialize_intraday_meta_predictions",
        as_of_date=session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=session_date,
                input_sources=[
                    "fact_intraday_active_policy",
                    "fact_intraday_active_meta_model",
                    "fact_model_training_run",
                ],
                notes=f"Materialize intraday meta predictions for {session_date.isoformat()}",
                ranking_version=ranking_version,
            )
            try:
                tuned_frame = apply_active_intraday_policy_frame(
                    settings,
                    session_date=session_date,
                    horizons=horizons,
                    symbol=symbol,
                    connection=connection,
                )
                active_registry = _load_active_meta_model_registry(
                    connection,
                    as_of_date=session_date,
                    horizons=horizons,
                )
                artifact_rows = _load_training_artifact_rows(
                    connection,
                    training_run_ids=active_registry["training_run_id"].astype(str).tolist(),
                )
                registry_map: dict[tuple[int, str], pd.Series] = {}
                for _, row in active_registry.iterrows():
                    key = (int(row["horizon"]), str(row["panel_name"]))
                    if key not in registry_map:
                        registry_map[key] = row
                artifact_map = {
                    str(row["training_run_id"]): row
                    for _, row in artifact_rows.iterrows()
                }
                prediction_rows: list[dict[str, object]] = []
                created_at = now_local(settings.app.timezone)
                for horizon in horizons:
                    horizon_frame = tuned_frame.loc[tuned_frame["horizon"] == int(horizon)].copy()
                    if horizon_frame.empty:
                        continue
                    for panel_name in (ENTER_PANEL, WAIT_PANEL):
                        group = horizon_frame.loc[
                            horizon_frame["tuned_action"] == PANEL_ACTION_MAP[panel_name]
                        ].copy()
                        if group.empty:
                            continue
                        active_row = registry_map.get((int(horizon), panel_name))
                        if active_row is None:
                            for row in group.itertuples(index=False):
                                prediction_rows.append(
                                    {
                                        "run_id": run_context.run_id,
                                        "session_date": session_date,
                                        "symbol": row.symbol,
                                        "horizon": int(row.horizon),
                                        "checkpoint_time": row.checkpoint_time,
                                        "ranking_version": ranking_version,
                                        "panel_name": panel_name,
                                        "tuned_action": row.tuned_action,
                                        "active_policy_candidate_id": row.active_policy_candidate_id,
                                        "active_meta_model_id": None,
                                        "training_run_id": None,
                                        "model_version": INTRADAY_META_MODEL_VERSION,
                                        "predicted_class": KEEP_CLASS_BY_PANEL[panel_name],
                                        "predicted_class_probability": None,
                                        "confidence_margin": None,
                                        "uncertainty_score": None,
                                        "disagreement_score": None,
                                        "class_probability_json": None,
                                        "fallback_flag": True,
                                        "fallback_reason": "active_meta_model_missing",
                                        "source_notes_json": json_or_none({"panel_name": panel_name}),
                                        "created_at": created_at,
                                    }
                                )
                            continue
                        artifact_row = artifact_map.get(str(active_row["training_run_id"]))
                        resolved_artifact_path = (
                            None
                            if artifact_row is None
                            else resolve_artifact_path(settings, artifact_row["artifact_uri"])
                        )
                        if artifact_row is None or resolved_artifact_path is None:
                            for row in group.itertuples(index=False):
                                prediction_rows.append(
                                    {
                                        "run_id": run_context.run_id,
                                        "session_date": session_date,
                                        "symbol": row.symbol,
                                        "horizon": int(row.horizon),
                                        "checkpoint_time": row.checkpoint_time,
                                        "ranking_version": ranking_version,
                                        "panel_name": panel_name,
                                        "tuned_action": row.tuned_action,
                                        "active_policy_candidate_id": row.active_policy_candidate_id,
                                        "active_meta_model_id": active_row["active_meta_model_id"],
                                        "training_run_id": active_row["training_run_id"],
                                        "model_version": INTRADAY_META_MODEL_VERSION,
                                        "predicted_class": KEEP_CLASS_BY_PANEL[panel_name],
                                        "predicted_class_probability": None,
                                        "confidence_margin": None,
                                        "uncertainty_score": None,
                                        "disagreement_score": None,
                                        "class_probability_json": None,
                                        "fallback_flag": True,
                                        "fallback_reason": "training_artifact_missing",
                                        "source_notes_json": json_or_none({"panel_name": panel_name}),
                                        "created_at": created_at,
                                    }
                                )
                            continue
                        payload = load_model_artifact(resolved_artifact_path)
                        features = feature_frame_with_dummies(
                            group,
                            feature_columns=list(payload["feature_columns"]),
                        )
                        weighted_probs: list[pd.DataFrame] = []
                        member_top_probs: dict[str, np.ndarray] = {}
                        for member_name, model in payload["member_models"].items():
                            member_prob = pd.DataFrame(
                                model.predict_proba(features),
                                columns=[str(item) for item in model.classes_],
                                index=group.index,
                            ).reindex(columns=list(payload["class_labels"]), fill_value=0.0)
                            weighted_probs.append(
                                member_prob * float(payload["ensemble_weights"].get(member_name, 0.0))
                            )
                            member_top_probs[member_name] = member_prob.to_numpy()
                        ensemble_prob = sum(weighted_probs)
                        ensemble_prob = _apply_probability_calibration(
                            ensemble_prob,
                            calibration_params=payload.get("calibration_params", {}),
                        )
                        predicted_class = ensemble_prob.idxmax(axis=1)
                        sorted_probabilities = np.sort(ensemble_prob.to_numpy(), axis=1)
                        max_prob = sorted_probabilities[:, -1]
                        second_prob = (
                            sorted_probabilities[:, -2]
                            if ensemble_prob.shape[1] > 1
                            else np.zeros(len(ensemble_prob))
                        )
                        disagreement_scores: list[float] = []
                        for index, class_label in enumerate(predicted_class):
                            class_index = list(payload["class_labels"]).index(class_label)
                            member_values = [
                                matrix[index, class_index]
                                for matrix in member_top_probs.values()
                            ]
                            disagreement_scores.append(float(np.std(member_values) * 100.0))
                        for local_index, row in enumerate(group.itertuples(index=False)):
                            class_probability_map = {
                                class_label: float(ensemble_prob.iloc[local_index][class_label])
                                for class_label in payload["class_labels"]
                            }
                            prediction_rows.append(
                                {
                                    "run_id": run_context.run_id,
                                    "session_date": session_date,
                                    "symbol": row.symbol,
                                    "horizon": int(row.horizon),
                                    "checkpoint_time": row.checkpoint_time,
                                    "ranking_version": ranking_version,
                                    "panel_name": panel_name,
                                    "tuned_action": row.tuned_action,
                                    "active_policy_candidate_id": row.active_policy_candidate_id,
                                    "active_meta_model_id": active_row["active_meta_model_id"],
                                    "training_run_id": active_row["training_run_id"],
                                    "model_version": INTRADAY_META_MODEL_VERSION,
                                    "predicted_class": predicted_class.iloc[local_index],
                                    "predicted_class_probability": float(max_prob[local_index]),
                                    "confidence_margin": float(max_prob[local_index] - second_prob[local_index]),
                                    "uncertainty_score": float((1.0 - max_prob[local_index]) * 100.0),
                                    "disagreement_score": disagreement_scores[local_index],
                                    "class_probability_json": json_or_none(class_probability_map),
                                    "fallback_flag": False,
                                    "fallback_reason": None,
                                    "source_notes_json": json_or_none(
                                        {
                                            "training_run_id": active_row["training_run_id"],
                                            "active_meta_model_id": active_row["active_meta_model_id"],
                                        }
                                    ),
                                    "created_at": created_at,
                                }
                            )
                for row in tuned_frame.itertuples(index=False):
                    if _panel_name_for_action(row.tuned_action) is not None:
                        continue
                    prediction_rows.append(
                        {
                            "run_id": run_context.run_id,
                            "session_date": session_date,
                            "symbol": row.symbol,
                            "horizon": int(row.horizon),
                            "checkpoint_time": row.checkpoint_time,
                            "ranking_version": ranking_version,
                            "panel_name": None,
                            "tuned_action": row.tuned_action,
                            "active_policy_candidate_id": row.active_policy_candidate_id,
                            "active_meta_model_id": None,
                            "training_run_id": None,
                            "model_version": INTRADAY_META_MODEL_VERSION,
                            "predicted_class": None,
                            "predicted_class_probability": None,
                            "confidence_margin": None,
                            "uncertainty_score": None,
                            "disagreement_score": None,
                            "class_probability_json": None,
                            "fallback_flag": True,
                            "fallback_reason": "hard_guard_action",
                            "source_notes_json": json_or_none({"tuned_action": row.tuned_action}),
                            "created_at": created_at,
                        }
                    )
                prediction_frame = pd.DataFrame(prediction_rows)
                if not prediction_frame.empty:
                    upsert_intraday_meta_prediction(connection, prediction_frame)
                notes = (
                    "Intraday meta predictions materialized. "
                    f"rows={len(prediction_rows)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=created_at,
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=ranking_version,
                )
                return IntradayMetaPredictionResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    row_count=len(prediction_rows),
                    artifact_paths=[],
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday meta predictions failed.",
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise


def materialize_intraday_final_actions(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    symbol: str | None = None,
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayMetaDecisionResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "materialize_intraday_final_actions",
        as_of_date=session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=session_date,
                input_sources=[
                    "fact_intraday_meta_prediction",
                    "fact_intraday_active_meta_model",
                    "fact_intraday_active_policy",
                ],
                notes=f"Materialize intraday final actions for {session_date.isoformat()}",
                ranking_version=ranking_version,
            )
            try:
                tuned_frame = apply_active_intraday_policy_frame(
                    settings,
                    session_date=session_date,
                    horizons=horizons,
                    symbol=symbol,
                    connection=connection,
                )
                prediction_frame = connection.execute(
                    """
                    SELECT *
                    FROM fact_intraday_meta_prediction
                    WHERE session_date = ?
                      AND ranking_version = ?
                    """,
                    [session_date, ranking_version],
                ).fetchdf()
                active_registry = _load_active_meta_model_registry(
                    connection,
                    as_of_date=session_date,
                    horizons=horizons,
                )
                threshold_payloads: dict[tuple[int, str], dict[str, object]] = {}
                for _, row in active_registry.iterrows():
                    key = (int(row["horizon"]), str(row["panel_name"]))
                    if key in threshold_payloads:
                        continue
                    threshold_payloads[key] = (
                        json.loads(str(row["threshold_payload_json"]))
                        if pd.notna(row["threshold_payload_json"])
                        else {}
                    )
                prediction_map = {
                    (row["symbol"], int(row["horizon"]), row["checkpoint_time"]): row
                    for _, row in prediction_frame.iterrows()
                }
                decision_rows: list[dict[str, object]] = []
                created_at = now_local(settings.app.timezone)
                for row in tuned_frame.itertuples(index=False):
                    panel_name = _panel_name_for_action(row.tuned_action)
                    hard_guard = row.tuned_action in HARD_GUARD_ACTIONS or panel_name is None
                    prediction_row = prediction_map.get((row.symbol, int(row.horizon), row.checkpoint_time))
                    final_action = row.tuned_action
                    predicted_class = None
                    predicted_probability = None
                    confidence_margin = None
                    uncertainty_score = None
                    disagreement_score = None
                    active_meta_model_id = None
                    training_run_id = None
                    override_applied = False
                    override_type = None
                    fallback_flag = True
                    fallback_reason = "hard_guard_action" if hard_guard else "prediction_missing"
                    decision_reason_codes: list[str] = []
                    risk_flags: list[str] = []
                    if prediction_row is not None:
                        predicted_class = prediction_row["predicted_class"]
                        predicted_probability = prediction_row["predicted_class_probability"]
                        confidence_margin = prediction_row["confidence_margin"]
                        uncertainty_score = prediction_row["uncertainty_score"]
                        disagreement_score = prediction_row["disagreement_score"]
                        active_meta_model_id = prediction_row["active_meta_model_id"]
                        training_run_id = prediction_row["training_run_id"]
                        fallback_flag = bool(prediction_row["fallback_flag"]) if pd.notna(prediction_row["fallback_flag"]) else False
                        fallback_reason = prediction_row["fallback_reason"]
                    if hard_guard:
                        decision_reason_codes.append("hard_guard_keep")
                    elif prediction_row is None or pd.isna(prediction_row["predicted_class"]):
                        decision_reason_codes.append("prediction_missing_keep")
                    else:
                        threshold_payload = threshold_payloads.get((int(row.horizon), panel_name), {})
                        minimum_confidence = float(threshold_payload.get("minimum_confidence", 0.55))
                        minimum_margin = float(threshold_payload.get("minimum_margin", 0.08))
                        uncertainty_ceiling = float(threshold_payload.get("uncertainty_ceiling", 55.0))
                        disagreement_ceiling = float(threshold_payload.get("disagreement_ceiling", 18.0))
                        class_thresholds = threshold_payload.get("class_thresholds", {}).get(panel_name, {})
                        predicted_probability = float(predicted_probability or 0.0)
                        confidence_margin = float(confidence_margin or 0.0)
                        uncertainty_score = float(uncertainty_score or 0.0)
                        disagreement_score = float(disagreement_score or 0.0)
                        if predicted_probability < minimum_confidence:
                            fallback_flag = True
                            fallback_reason = "low_confidence"
                            decision_reason_codes.append("low_confidence_keep")
                            risk_flags.append("LOW_CONFIDENCE")
                        elif confidence_margin < minimum_margin:
                            fallback_flag = True
                            fallback_reason = "low_margin"
                            decision_reason_codes.append("low_margin_keep")
                            risk_flags.append("LOW_MARGIN")
                        elif uncertainty_score > uncertainty_ceiling:
                            fallback_flag = True
                            fallback_reason = "high_uncertainty"
                            decision_reason_codes.append("high_uncertainty_keep")
                            risk_flags.append("HIGH_UNCERTAINTY")
                        elif disagreement_score > disagreement_ceiling:
                            fallback_flag = True
                            fallback_reason = "high_disagreement"
                            decision_reason_codes.append("high_disagreement_keep")
                            risk_flags.append("HIGH_DISAGREEMENT")
                        else:
                            override_class_threshold = float(
                                class_thresholds.get(str(predicted_class), minimum_confidence)
                            )
                            if predicted_class == KEEP_CLASS_BY_PANEL[panel_name]:
                                fallback_flag = False
                                fallback_reason = None
                                decision_reason_codes.append("keep_policy_class")
                            elif predicted_probability < override_class_threshold:
                                fallback_flag = True
                                fallback_reason = "below_class_threshold"
                                decision_reason_codes.append("below_class_threshold_keep")
                            else:
                                final_action = _action_from_prediction(
                                    panel_name,
                                    str(predicted_class),
                                    str(row.tuned_action),
                                )
                                override_applied = final_action != row.tuned_action
                                override_type = str(predicted_class) if override_applied else None
                                fallback_flag = False
                                fallback_reason = None
                                decision_reason_codes.append("meta_override_applied" if override_applied else "meta_keep")
                    decision_rows.append(
                        {
                            "run_id": run_context.run_id,
                            "session_date": session_date,
                            "symbol": row.symbol,
                            "horizon": int(row.horizon),
                            "checkpoint_time": row.checkpoint_time,
                            "ranking_version": ranking_version,
                            "raw_action": row.raw_action,
                            "adjusted_action": row.adjusted_action,
                            "tuned_action": row.tuned_action,
                            "final_action": final_action,
                            "panel_name": panel_name,
                            "predicted_class": predicted_class,
                            "predicted_class_probability": predicted_probability,
                            "confidence_margin": confidence_margin,
                            "uncertainty_score": uncertainty_score,
                            "disagreement_score": disagreement_score,
                            "active_policy_candidate_id": row.active_policy_candidate_id,
                            "active_meta_model_id": active_meta_model_id,
                            "active_meta_training_run_id": training_run_id,
                            "hard_guard_block_flag": hard_guard,
                            "override_applied_flag": override_applied,
                            "override_type": override_type,
                            "fallback_flag": fallback_flag,
                            "fallback_reason": fallback_reason,
                            "decision_reason_codes_json": json_or_none(decision_reason_codes),
                            "risk_flags_json": json_or_none(risk_flags),
                            "source_notes_json": json_or_none({"policy_trace": row.policy_trace}),
                            "created_at": created_at,
                        }
                    )
                decision_frame = pd.DataFrame(decision_rows)
                if not decision_frame.empty:
                    upsert_intraday_meta_decision(connection, decision_frame)
                notes = (
                    "Intraday final actions materialized. "
                    f"rows={len(decision_rows)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=created_at,
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=ranking_version,
                )
                return IntradayMetaDecisionResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    row_count=len(decision_rows),
                    artifact_paths=[],
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday final actions failed.",
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise


def evaluate_intraday_meta_models(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayMetaEvaluationResult:
    ensure_storage_layout(settings)
    ensure_intraday_meta_label_inputs(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
        ranking_version=ranking_version,
    )
    with activate_run_context(
        "evaluate_intraday_meta_models",
        as_of_date=end_session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=["fact_intraday_meta_decision", "fact_model_metric_summary"],
                notes=(
                    "Evaluate intraday meta overlay. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                ),
                ranking_version=ranking_version,
            )
            try:
                dataset = assemble_intraday_meta_dataset_frame(
                    settings,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                    connection=connection,
                )
                decisions = connection.execute(
                    """
                    SELECT *
                    FROM fact_intraday_meta_decision
                    WHERE session_date BETWEEN ? AND ?
                      AND ranking_version = ?
                    """,
                    [start_session_date, end_session_date, ranking_version],
                ).fetchdf()
                if not dataset.empty:
                    dataset["session_date"] = pd.to_datetime(dataset["session_date"]).dt.date
                    dataset["symbol"] = dataset["symbol"].astype(str).str.zfill(6)
                    dataset["horizon"] = pd.to_numeric(dataset["horizon"], errors="coerce").astype(int)
                    dataset["checkpoint_time"] = dataset["checkpoint_time"].astype(str)
                if not decisions.empty:
                    decisions["session_date"] = pd.to_datetime(decisions["session_date"]).dt.date
                    decisions["symbol"] = decisions["symbol"].astype(str).str.zfill(6)
                    decisions["horizon"] = (
                        pd.to_numeric(decisions["horizon"], errors="coerce").astype(int)
                    )
                    decisions["checkpoint_time"] = decisions["checkpoint_time"].astype(str)
                if dataset.empty or decisions.empty:
                    notes = "No intraday meta overlay rows were available for evaluation."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=ranking_version,
                    )
                    return IntradayMetaEvaluationResult(
                        run_id=run_context.run_id,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                merged = dataset.merge(
                    decisions,
                    on=["session_date", "symbol", "horizon", "checkpoint_time"],
                    how="inner",
                    suffixes=("_dataset", "_decision"),
                )
                if merged.empty:
                    notes = "No joined intraday meta overlay rows were available for evaluation."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=ranking_version,
                    )
                    return IntradayMetaEvaluationResult(
                        run_id=run_context.run_id,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                overlay_rows: list[dict[str, object]] = []
                for row in merged.to_dict(orient="records"):
                    row_series = pd.Series(row)
                    tuned_action = row_series.get("tuned_action_dataset", row_series.get("tuned_action"))
                    policy_excess, policy_edge = _policy_action_outcome(row_series, tuned_action)
                    meta_excess, meta_edge = _policy_action_outcome(row_series, row_series["final_action"])
                    overlay_rows.append(
                        {
                            "session_date": row_series["session_date"],
                            "horizon": int(row_series["horizon"]),
                            "panel_name": row_series["panel_name_dataset"],
                            "training_run_id": row_series.get("active_meta_training_run_id"),
                            "market_regime_family": row_series.get("market_regime_family"),
                            "checkpoint_time": row_series.get("checkpoint_time"),
                            "policy_excess": policy_excess,
                            "meta_excess": meta_excess,
                            "lift_excess": None if policy_excess is None or meta_excess is None else meta_excess - policy_excess,
                            "policy_edge_bps": policy_edge,
                            "meta_edge_bps": meta_edge,
                            "edge_delta_bps": None if policy_edge is None or meta_edge is None else meta_edge - policy_edge,
                            "override_applied_flag": bool(row_series.get("override_applied_flag")),
                            "fallback_flag": bool(row_series.get("fallback_flag")),
                            "predicted_class": row_series.get("predicted_class"),
                            "target_class": row_series.get("target_class"),
                            "final_action": row_series.get("final_action"),
                            "tuned_action": tuned_action,
                        }
                    )
                overlay_frame = pd.DataFrame(overlay_rows)
                metric_rows: list[dict[str, object]] = []
                created_at = pd.Timestamp.utcnow()
                for (horizon, panel_name), group in overlay_frame.groupby(["horizon", "panel_name"], dropna=False):
                    training_run_id = (
                        group["training_run_id"].dropna().astype(str).iloc[0]
                        if not group["training_run_id"].dropna().empty
                        else f"{run_context.run_id}-h{int(horizon)}-{panel_name}"
                    )
                    overall_metrics = {
                        "policy_only_mean_excess_return": float(group["policy_excess"].dropna().mean()) if not group["policy_excess"].dropna().empty else None,
                        "meta_overlay_mean_excess_return": float(group["meta_excess"].dropna().mean()) if not group["meta_excess"].dropna().empty else None,
                        "same_exit_lift_mean_excess_return": float(group["lift_excess"].dropna().mean()) if not group["lift_excess"].dropna().empty else None,
                        "same_exit_lift_mean_timing_edge_bps": float(group["edge_delta_bps"].dropna().mean()) if not group["edge_delta_bps"].dropna().empty else None,
                        "override_rate": float(group["override_applied_flag"].mean()),
                        "fallback_rate": float(group["fallback_flag"].mean()),
                    }
                    upgrade_mask = group["predicted_class"] == "UPGRADE_ENTER"
                    downgrade_mask = group["predicted_class"].isin(["DOWNGRADE_WAIT", "DOWNGRADE_AVOID"])
                    overall_metrics["upgrade_precision"] = (
                        float((group.loc[upgrade_mask, "lift_excess"] > 0).mean()) if upgrade_mask.any() else None
                    )
                    overall_metrics["downgrade_precision"] = (
                        float((group.loc[downgrade_mask, "lift_excess"] > 0).mean()) if downgrade_mask.any() else None
                    )
                    overall_metrics["saved_loss_rate"] = float(
                        (
                            (group["policy_excess"] < 0)
                            & (group["meta_excess"] >= group["policy_excess"])
                        ).mean()
                    )
                    overall_metrics["missed_winner_rate"] = float(
                        (
                            (group["policy_excess"] > 0)
                            & (group["meta_excess"] < group["policy_excess"])
                        ).mean()
                    )
                    for metric_name, metric_value in overall_metrics.items():
                        metric_rows.append(
                            {
                                "training_run_id": training_run_id,
                                "model_domain": INTRADAY_META_MODEL_DOMAIN,
                                "model_version": INTRADAY_META_MODEL_VERSION,
                                "horizon": int(horizon),
                                "panel_name": panel_name,
                                "member_name": "overlay",
                                "split_name": "evaluation",
                                "metric_scope": "overlay",
                                "class_label": None,
                                "comparison_key": "overall",
                                "metric_name": metric_name,
                                "metric_value": metric_value,
                                "sample_count": int(len(group)),
                                "created_at": created_at,
                            }
                        )
                    for scope_column in ("market_regime_family", "checkpoint_time"):
                        for scope_value, scoped in group.groupby(scope_column):
                            if pd.isna(scope_value):
                                continue
                            metric_rows.append(
                                {
                                    "training_run_id": training_run_id,
                                    "model_domain": INTRADAY_META_MODEL_DOMAIN,
                                    "model_version": INTRADAY_META_MODEL_VERSION,
                                    "horizon": int(horizon),
                                    "panel_name": panel_name,
                                    "member_name": "overlay",
                                    "split_name": "evaluation",
                                    "metric_scope": scope_column,
                                    "class_label": None,
                                    "comparison_key": str(scope_value),
                                    "metric_name": "same_exit_lift_mean_excess_return",
                                    "metric_value": float(scoped["lift_excess"].dropna().mean()) if not scoped["lift_excess"].dropna().empty else None,
                                    "sample_count": int(len(scoped)),
                                    "created_at": created_at,
                                }
                            )
                if metric_rows:
                    upsert_meta_metric_summary(connection, pd.DataFrame(metric_rows))
                notes = (
                    "Intraday meta overlay evaluation completed. "
                    f"rows={len(overlay_frame)} metrics={len(metric_rows)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=ranking_version,
                )
                return IntradayMetaEvaluationResult(
                    run_id=run_context.run_id,
                    row_count=len(overlay_frame),
                    artifact_paths=[],
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday meta overlay evaluation failed.",
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
