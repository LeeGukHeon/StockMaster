from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np
import pandas as pd

INTRADAY_META_MODEL_VERSION = "intraday_meta_v1"
INTRADAY_META_MODEL_DOMAIN = "intraday_meta"
INTRADAY_META_DATASET_VERSION = "intraday_meta_ds_v1"
INTRADAY_META_PREDICTION_VERSION = "intraday_meta_prediction_v1"
INTRADAY_META_OVERLAY_STRATEGY_ID = "SEL_V2_TIMING_META_OVERLAY_FIRST_ENTER"

ENTER_PANEL = "ENTER_PANEL"
WAIT_PANEL = "WAIT_PANEL"
META_PANELS: tuple[str, ...] = (ENTER_PANEL, WAIT_PANEL)
PANEL_ACTION_MAP: dict[str, str] = {
    ENTER_PANEL: "ENTER_NOW",
    WAIT_PANEL: "WAIT_RECHECK",
}
PANEL_CLASSES: dict[str, tuple[str, ...]] = {
    ENTER_PANEL: ("KEEP_ENTER", "DOWNGRADE_WAIT", "DOWNGRADE_AVOID"),
    WAIT_PANEL: ("KEEP_WAIT", "UPGRADE_ENTER", "DOWNGRADE_AVOID"),
}
KEEP_CLASS_BY_PANEL: dict[str, str] = {
    ENTER_PANEL: "KEEP_ENTER",
    WAIT_PANEL: "KEEP_WAIT",
}
META_MEMBER_NAMES: tuple[str, ...] = ("logreg", "hist_gbm", "extra_trees")
META_LABEL_THRESHOLDS: dict[str, float] = {
    "enter_vs_wait_delta_bps": 20.0,
    "wait_vs_enter_delta_bps": 20.0,
    "avoid_vs_enter_delta_bps": 35.0,
    "avoid_vs_wait_delta_bps": 30.0,
    "min_effective_trade_outcome_bps": 10.0,
    "label_noise_buffer_bps": 5.0,
}
DEFAULT_THRESHOLD_PAYLOAD: dict[str, object] = {
    "minimum_confidence": 0.55,
    "minimum_margin": 0.08,
    "class_thresholds": {
        ENTER_PANEL: {"DOWNGRADE_WAIT": 0.58, "DOWNGRADE_AVOID": 0.62},
        WAIT_PANEL: {"UPGRADE_ENTER": 0.58, "DOWNGRADE_AVOID": 0.62},
    },
    "uncertainty_ceiling": 55.0,
    "disagreement_ceiling": 18.0,
    "threshold_version": "intraday_meta_threshold_v1",
}
HARD_GUARD_ACTIONS: tuple[str, ...] = ("AVOID_TODAY", "DATA_INSUFFICIENT")

META_NUMERIC_FEATURES: tuple[str, ...] = (
    "candidate_rank",
    "final_selection_value",
    "final_selection_rank_pct",
    "expected_excess_return",
    "uncertainty_score",
    "disagreement_score",
    "raw_timing_score",
    "adjusted_timing_score",
    "tuned_score",
    "gap_opening_quality_score",
    "micro_trend_score",
    "relative_activity_score",
    "orderbook_score",
    "execution_strength_score",
    "risk_friction_score",
    "signal_quality_score",
    "spread_bps",
    "imbalance_ratio",
    "execution_strength",
    "activity_ratio",
    "market_breadth_ratio",
    "market_shock_proxy",
    "candidate_mean_signal_quality",
    "advancers_count",
    "decliners_count",
    "bar_coverage_ratio",
    "trade_coverage_ratio",
    "quote_coverage_ratio",
)
META_CATEGORICAL_FEATURES: tuple[str, ...] = (
    "market",
    "checkpoint_time",
    "raw_action",
    "adjusted_action",
    "tuned_action",
    "market_regime_family",
    "adjustment_profile",
    "signal_quality_flag",
    "selection_confidence_bucket",
    "policy_trace",
    "active_policy_template_id",
    "active_policy_scope_type",
    "trade_summary_status",
    "quote_status",
    "data_quality_flag",
)


@dataclass(slots=True)
class IntradayMetaDatasetResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayMetaDatasetValidationResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayMetaTrainingResult:
    run_id: str
    train_end_date: object
    training_run_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayMetaWalkforwardResult:
    run_id: str
    split_count: int
    training_run_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayMetaThresholdCalibrationResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayMetaEvaluationResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayMetaPredictionResult:
    run_id: str
    session_date: object
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayMetaDecisionResult:
    run_id: str
    session_date: object
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayActiveMetaModelResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


META_DATASET_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(
        (
            "session_date",
            "selection_date",
            "symbol",
            "market",
            "horizon",
            "checkpoint_time",
            "panel_name",
            "active_policy_action",
            "raw_action",
            "adjusted_action",
            "tuned_action",
            "target_class",
            "target_reason",
            "label_available_flag",
            "current_realized_excess_return",
            "current_timing_edge_vs_open_bps",
            "later_enter_realized_excess_return",
            "later_enter_timing_edge_vs_open_bps",
            "no_entry_realized_excess_return",
            "active_policy_candidate_id",
            "active_policy_template_id",
            "active_policy_scope_type",
            "active_policy_scope_key",
            "policy_trace",
            "fallback_used_flag",
            "market_regime_family",
            "adjustment_profile",
            "signal_quality_flag",
            "selection_confidence_bucket",
            "trade_summary_status",
            "quote_status",
            "data_quality_flag",
            *META_NUMERIC_FEATURES,
            *META_CATEGORICAL_FEATURES,
        )
    )
)

META_PREDICTION_COLUMNS: tuple[str, ...] = (
    "run_id",
    "session_date",
    "symbol",
    "horizon",
    "checkpoint_time",
    "ranking_version",
    "panel_name",
    "tuned_action",
    "active_policy_candidate_id",
    "active_meta_model_id",
    "training_run_id",
    "model_version",
    "predicted_class",
    "predicted_class_probability",
    "confidence_margin",
    "uncertainty_score",
    "disagreement_score",
    "class_probability_json",
    "fallback_flag",
    "fallback_reason",
    "source_notes_json",
    "created_at",
)

META_DECISION_COLUMNS: tuple[str, ...] = (
    "run_id",
    "session_date",
    "symbol",
    "horizon",
    "checkpoint_time",
    "ranking_version",
    "raw_action",
    "adjusted_action",
    "tuned_action",
    "final_action",
    "panel_name",
    "predicted_class",
    "predicted_class_probability",
    "confidence_margin",
    "uncertainty_score",
    "disagreement_score",
    "active_policy_candidate_id",
    "active_meta_model_id",
    "active_meta_training_run_id",
    "hard_guard_block_flag",
    "override_applied_flag",
    "override_type",
    "fallback_flag",
    "fallback_reason",
    "decision_reason_codes_json",
    "risk_flags_json",
    "source_notes_json",
    "created_at",
)

ACTIVE_META_MODEL_COLUMNS: tuple[str, ...] = (
    "active_meta_model_id",
    "horizon",
    "panel_name",
    "training_run_id",
    "model_version",
    "source_type",
    "promotion_type",
    "threshold_payload_json",
    "calibration_summary_json",
    "effective_from_date",
    "effective_to_date",
    "active_flag",
    "rollback_of_active_meta_model_id",
    "note",
    "created_at",
    "updated_at",
)


def ordered_frame(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    return frame.reindex(columns=list(columns)).copy()


def json_or_none(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def safe_float(value: object) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def default_threshold_payload(panel_name: str) -> dict[str, object]:
    payload = json.loads(json.dumps(DEFAULT_THRESHOLD_PAYLOAD))
    payload["panel_name"] = panel_name
    payload["calibrated_at"] = None
    return payload


def feature_frame_with_dummies(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str] | None = None,
) -> pd.DataFrame:
    working = frame.copy()
    for column in META_NUMERIC_FEATURES:
        if column not in working.columns:
            working[column] = pd.NA
        working[column] = pd.to_numeric(working[column], errors="coerce")
    for column in META_CATEGORICAL_FEATURES:
        if column not in working.columns:
            working[column] = "missing"
        working[column] = working[column].astype("string").fillna("missing")
    features = pd.concat(
        [
            working[list(META_NUMERIC_FEATURES)],
            pd.get_dummies(working[list(META_CATEGORICAL_FEATURES)], dummy_na=False),
        ],
        axis=1,
    )
    features = features.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    if feature_columns is None:
        return features
    return features.reindex(columns=feature_columns, fill_value=0.0)


def upsert_intraday_meta_prediction(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    stage = ordered_frame(frame, META_PREDICTION_COLUMNS)
    connection.register("intraday_meta_prediction_stage", stage)
    connection.execute(
        """
        DELETE FROM fact_intraday_meta_prediction
        WHERE (
            session_date,
            symbol,
            horizon,
            checkpoint_time,
            ranking_version
        ) IN (
            SELECT
                session_date,
                symbol,
                horizon,
                checkpoint_time,
                ranking_version
            FROM intraday_meta_prediction_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_meta_prediction
        SELECT * FROM intraday_meta_prediction_stage
        """
    )
    connection.unregister("intraday_meta_prediction_stage")


def upsert_intraday_meta_decision(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    stage = ordered_frame(frame, META_DECISION_COLUMNS)
    connection.register("intraday_meta_decision_stage", stage)
    connection.execute(
        """
        DELETE FROM fact_intraday_meta_decision
        WHERE (
            session_date,
            symbol,
            horizon,
            checkpoint_time,
            ranking_version
        ) IN (
            SELECT
                session_date,
                symbol,
                horizon,
                checkpoint_time,
                ranking_version
            FROM intraday_meta_decision_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_meta_decision
        SELECT * FROM intraday_meta_decision_stage
        """
    )
    connection.unregister("intraday_meta_decision_stage")


def upsert_intraday_active_meta_model(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    stage = ordered_frame(frame, ACTIVE_META_MODEL_COLUMNS)
    connection.register("intraday_active_meta_model_stage", stage)
    connection.execute(
        """
        DELETE FROM fact_intraday_active_meta_model
        WHERE active_meta_model_id IN (
            SELECT active_meta_model_id
            FROM intraday_active_meta_model_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_active_meta_model
        SELECT * FROM intraday_active_meta_model_stage
        """
    )
    connection.unregister("intraday_active_meta_model_stage")


def upsert_meta_training_runs(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_meta_training_run_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_model_training_run
        WHERE training_run_id IN (
            SELECT training_run_id
            FROM intraday_meta_training_run_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_model_training_run (
            training_run_id,
            run_id,
            model_domain,
            model_version,
            horizon,
            panel_name,
            train_end_date,
            training_window_start,
            training_window_end,
            validation_window_start,
            validation_window_end,
            train_row_count,
            validation_row_count,
            train_session_count,
            validation_session_count,
            feature_count,
            ensemble_weight_json,
            model_family_json,
            threshold_payload_json,
            diagnostic_artifact_uri,
            metadata_json,
            fallback_flag,
            fallback_reason,
            artifact_uri,
            notes,
            status,
            created_at
        )
        SELECT
            training_run_id,
            run_id,
            model_domain,
            model_version,
            horizon,
            panel_name,
            train_end_date,
            training_window_start,
            training_window_end,
            validation_window_start,
            validation_window_end,
            train_row_count,
            validation_row_count,
            train_session_count,
            validation_session_count,
            feature_count,
            ensemble_weight_json,
            model_family_json,
            threshold_payload_json,
            diagnostic_artifact_uri,
            metadata_json,
            fallback_flag,
            fallback_reason,
            artifact_uri,
            notes,
            status,
            created_at
        FROM intraday_meta_training_run_stage
        """
    )
    connection.unregister("intraday_meta_training_run_stage")


def upsert_meta_metric_summary(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    working = frame.copy()
    for column_name, default_value in {
        "model_domain": "default",
        "panel_name": "all",
        "metric_scope": "all",
        "class_label": "all",
        "comparison_key": "all",
    }.items():
        if column_name not in working.columns:
            working[column_name] = default_value
        working[column_name] = working[column_name].fillna(default_value)
    connection.register("intraday_meta_metric_stage", working)
    connection.execute(
        """
        DELETE FROM fact_model_metric_summary
        WHERE (
            training_run_id,
            member_name,
            split_name,
            metric_scope,
            class_label,
            comparison_key,
            metric_name
        ) IN (
            SELECT
                training_run_id,
                member_name,
                split_name,
                metric_scope,
                class_label,
                comparison_key,
                metric_name
            FROM intraday_meta_metric_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_model_metric_summary (
            training_run_id,
            model_domain,
            model_version,
            horizon,
            panel_name,
            member_name,
            split_name,
            metric_scope,
            class_label,
            comparison_key,
            metric_name,
            metric_value,
            sample_count,
            created_at
        )
        SELECT
            training_run_id,
            model_domain,
            model_version,
            horizon,
            panel_name,
            member_name,
            split_name,
            metric_scope,
            class_label,
            comparison_key,
            metric_name,
            metric_value,
            sample_count,
            created_at
        FROM intraday_meta_metric_stage
        """
    )
    connection.unregister("intraday_meta_metric_stage")
