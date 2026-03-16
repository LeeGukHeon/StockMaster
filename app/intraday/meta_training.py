from __future__ import annotations

# ruff: noqa: E501
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, log_loss, precision_recall_fscore_support

from app.common.artifacts import resolve_artifact_path
from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.ml.registry import load_model_artifact, write_model_artifact
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

from .meta_common import (
    DEFAULT_THRESHOLD_PAYLOAD,
    ENTER_PANEL,
    INTRADAY_META_MODEL_DOMAIN,
    INTRADAY_META_MODEL_VERSION,
    KEEP_CLASS_BY_PANEL,
    META_MEMBER_NAMES,
    PANEL_CLASSES,
    WAIT_PANEL,
    IntradayActiveMetaModelResult,
    IntradayMetaThresholdCalibrationResult,
    IntradayMetaTrainingResult,
    IntradayMetaWalkforwardResult,
    default_threshold_payload,
    feature_frame_with_dummies,
    json_or_none,
    upsert_intraday_active_meta_model,
    upsert_meta_metric_summary,
    upsert_meta_training_runs,
)
from .meta_dataset import (
    assemble_intraday_meta_dataset_frame,
    ensure_intraday_meta_label_inputs,
)
from .promotion_common import resolve_alpha_lineage_status, write_promotion_decision_artifact

MIN_CLASS_COUNT_FOR_LIVE_MODEL = 2


@dataclass(slots=True)
class IntradayMetaPromotionResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class _PanelTrainingArtifacts:
    training_row: dict[str, object]
    metric_rows: list[dict[str, object]]
    artifact_path: Path | None
    diagnostic_path: Path | None
    validation_frame: pd.DataFrame
    notes: str


def _panel_frame(dataset: pd.DataFrame, *, horizon: int, panel_name: str) -> pd.DataFrame:
    if dataset.empty:
        return dataset.head(0).copy()
    frame = dataset.loc[
        (dataset["horizon"] == int(horizon)) & (dataset["panel_name"] == panel_name)
    ].copy()
    if frame.empty:
        return frame
    frame["session_date"] = pd.to_datetime(frame["session_date"]).dt.date
    return frame.sort_values(["session_date", "symbol", "checkpoint_time"]).reset_index(drop=True)


def _session_split(
    session_dates: list[date],
    *,
    validation_sessions: int,
) -> tuple[list[date], list[date], bool, str | None]:
    ordered = sorted(dict.fromkeys(session_dates))
    if not ordered:
        return [], [], True, "no_sessions"
    if len(ordered) == 1:
        return ordered, ordered, True, "single_session"
    validation_count = min(max(1, int(validation_sessions)), max(1, len(ordered) // 3))
    train_dates = ordered[:-validation_count]
    validation_dates = ordered[-validation_count:]
    fallback_reasons: list[str] = []
    if not train_dates:
        train_dates = ordered[:-1]
        validation_dates = ordered[-1:]
        fallback_reasons.append("narrow_history_split")
    if len(train_dates) < 3:
        fallback_reasons.append("short_train_history")
    return train_dates, validation_dates, bool(fallback_reasons), ",".join(fallback_reasons) or None


def _walkforward_splits(
    session_dates: list[date],
    *,
    mode: str,
    train_sessions: int,
    validation_sessions: int,
    test_sessions: int,
    step_sessions: int,
) -> list[dict[str, Any]]:
    ordered = sorted(dict.fromkeys(session_dates))
    if not ordered:
        return []
    min_total = max(3, int(train_sessions) + int(validation_sessions) + int(test_sessions))
    if len(ordered) < min_total:
        if len(ordered) < 3:
            return []
        train_end = max(1, len(ordered) - 2)
        validation_end = max(train_end + 1, len(ordered) - 1)
        return [
            {
                "split_index": 0,
                "train_dates": ordered[:train_end],
                "validation_dates": ordered[train_end:validation_end],
                "test_dates": ordered[validation_end:],
                "fallback_used": True,
                "fallback_reason": "compact_history_split",
            }
        ]

    splits: list[dict[str, Any]] = []
    cursor = int(train_sessions) + int(validation_sessions)
    split_index = 0
    while cursor + int(test_sessions) <= len(ordered):
        train_start = 0 if mode == "anchored" else max(0, cursor - int(train_sessions))
        splits.append(
            {
                "split_index": split_index,
                "train_dates": ordered[train_start : cursor - int(validation_sessions)],
                "validation_dates": ordered[cursor - int(validation_sessions) : cursor],
                "test_dates": ordered[cursor : cursor + int(test_sessions)],
                "fallback_used": False,
                "fallback_reason": None,
            }
        )
        cursor += max(1, int(step_sessions))
        split_index += 1
    return splits


def _build_member_estimators(train_frame: pd.DataFrame) -> dict[str, Any]:
    if train_frame["target_class"].nunique(dropna=True) < MIN_CLASS_COUNT_FOR_LIVE_MODEL:
        return {
            member_name: DummyClassifier(strategy="most_frequent")
            for member_name in META_MEMBER_NAMES
        }
    return {
        "logreg": LogisticRegression(
            max_iter=4000,
            random_state=42,
            class_weight="balanced",
        ),
        "hist_gbm": HistGradientBoostingClassifier(
            max_depth=4,
            learning_rate=0.05,
            max_iter=200,
            min_samples_leaf=12,
            random_state=42,
        ),
        "extra_trees": ExtraTreesClassifier(
            n_estimators=300,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1,
            class_weight="balanced_subsample",
        ),
    }


def _predict_proba_aligned(
    estimator: Any,
    features: pd.DataFrame,
    *,
    class_labels: list[str],
) -> pd.DataFrame:
    raw = estimator.predict_proba(features)
    raw_frame = pd.DataFrame(raw, columns=[str(item) for item in estimator.classes_], index=features.index)
    return raw_frame.reindex(columns=class_labels, fill_value=0.0)


def _metric_rows(
    *,
    training_run_id: str,
    horizon: int,
    panel_name: str,
    member_name: str,
    split_name: str,
    actual: pd.Series,
    predicted: pd.Series,
    probability_frame: pd.DataFrame,
) -> list[dict[str, object]]:
    actual_series = actual.astype("string")
    predicted_series = predicted.astype("string")
    classes = sorted(dict.fromkeys(actual_series.dropna().tolist() + predicted_series.dropna().tolist()))
    sample_count = int(len(actual_series))
    if sample_count == 0:
        return []
    try:
        log_loss_value = float(
            log_loss(
                actual_series,
                probability_frame.reindex(columns=classes, fill_value=0.0),
                labels=classes,
            )
        )
    except ValueError:
        log_loss_value = None
    created_at = pd.Timestamp.utcnow()
    metric_rows: list[dict[str, object]] = [
        {
            "training_run_id": training_run_id,
            "model_domain": INTRADAY_META_MODEL_DOMAIN,
            "model_version": INTRADAY_META_MODEL_VERSION,
            "horizon": int(horizon),
            "panel_name": panel_name,
            "member_name": member_name,
            "split_name": split_name,
            "metric_scope": "panel",
            "class_label": None,
            "comparison_key": None,
            "metric_name": "macro_f1",
            "metric_value": float(
                f1_score(actual_series, predicted_series, average="macro", zero_division=0)
            ),
            "sample_count": sample_count,
            "created_at": created_at,
        },
        {
            "training_run_id": training_run_id,
            "model_domain": INTRADAY_META_MODEL_DOMAIN,
            "model_version": INTRADAY_META_MODEL_VERSION,
            "horizon": int(horizon),
            "panel_name": panel_name,
            "member_name": member_name,
            "split_name": split_name,
            "metric_scope": "panel",
            "class_label": None,
            "comparison_key": None,
            "metric_name": "log_loss",
            "metric_value": log_loss_value,
            "sample_count": sample_count,
            "created_at": created_at,
        },
        {
            "training_run_id": training_run_id,
            "model_domain": INTRADAY_META_MODEL_DOMAIN,
            "model_version": INTRADAY_META_MODEL_VERSION,
            "horizon": int(horizon),
            "panel_name": panel_name,
            "member_name": member_name,
            "split_name": split_name,
            "metric_scope": "panel",
            "class_label": None,
            "comparison_key": None,
            "metric_name": "override_rate",
            "metric_value": float(
                (actual_series != KEEP_CLASS_BY_PANEL.get(panel_name, "")).mean()
            ),
            "sample_count": sample_count,
            "created_at": created_at,
        },
    ]
    precision, recall, f1_values, support = precision_recall_fscore_support(
        actual_series,
        predicted_series,
        labels=classes,
        zero_division=0,
    )
    for class_label, precision_value, recall_value, f1_value, support_value in zip(
        classes,
        precision,
        recall,
        f1_values,
        support,
        strict=False,
    ):
        for metric_name, metric_value in {
            "precision": float(precision_value),
            "recall": float(recall_value),
            "f1": float(f1_value),
        }.items():
            metric_rows.append(
                {
                    "training_run_id": training_run_id,
                    "model_domain": INTRADAY_META_MODEL_DOMAIN,
                    "model_version": INTRADAY_META_MODEL_VERSION,
                    "horizon": int(horizon),
                    "panel_name": panel_name,
                    "member_name": member_name,
                    "split_name": split_name,
                    "metric_scope": "class",
                    "class_label": class_label,
                    "comparison_key": None,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "sample_count": int(support_value),
                    "created_at": created_at,
                }
            )
    return metric_rows


def _normalise_weights(validation_metrics: dict[str, dict[str, float | None]]) -> dict[str, float]:
    raw_weights: dict[str, float] = {}
    for member_name, metrics in validation_metrics.items():
        macro_f1 = metrics.get("macro_f1")
        log_loss_value = metrics.get("log_loss")
        if macro_f1 is None:
            raw_weights[member_name] = 0.0
            continue
        base = max(float(macro_f1), 0.01)
        if log_loss_value is not None:
            base /= max(float(log_loss_value), 0.1)
        raw_weights[member_name] = base
    total = sum(value for value in raw_weights.values() if value > 0)
    if total <= 0:
        equal_weight = 1.0 / len(raw_weights) if raw_weights else 0.0
        return {name: equal_weight for name in raw_weights}
    return {name: value / total for name, value in raw_weights.items()}


def _fit_sigmoid_calibrator(probabilities: pd.Series, actual_mask: pd.Series) -> dict[str, float] | None:
    clean = pd.DataFrame(
        {
            "probability": pd.to_numeric(probabilities, errors="coerce"),
            "label": actual_mask.astype(int),
        }
    ).dropna()
    if clean["label"].nunique(dropna=True) < 2 or len(clean) < 8:
        return None
    calibrator = LogisticRegression(max_iter=1000, random_state=42)
    calibrator.fit(clean[["probability"]], clean["label"])
    return {
        "coef": float(calibrator.coef_[0][0]),
        "intercept": float(calibrator.intercept_[0]),
    }


def _apply_sigmoid_calibration(
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


def _threshold_payload(
    *,
    panel_name: str,
    validation_frame: pd.DataFrame,
    calibration_params: dict[str, dict[str, float] | None],
) -> dict[str, object]:
    payload = default_threshold_payload(panel_name)
    payload["calibrated_at"] = pd.Timestamp.utcnow().isoformat()
    payload["calibration_method"] = "sigmoid_per_class_v1"
    payload["class_calibration"] = calibration_params
    if validation_frame.empty:
        return payload
    payload["minimum_confidence"] = max(
        float(DEFAULT_THRESHOLD_PAYLOAD["minimum_confidence"]),
        round(float(validation_frame["predicted_class_probability"].quantile(0.35)), 4),
    )
    payload["minimum_margin"] = max(
        float(DEFAULT_THRESHOLD_PAYLOAD["minimum_margin"]),
        round(float(validation_frame["confidence_margin"].quantile(0.35)), 4),
    )
    class_thresholds = payload["class_thresholds"].get(panel_name, {}).copy()
    for class_label in PANEL_CLASSES[panel_name]:
        if class_label == KEEP_CLASS_BY_PANEL[panel_name]:
            continue
        target_rows = validation_frame.loc[validation_frame["target_class"] == class_label]
        if target_rows.empty:
            continue
        threshold = round(float(target_rows[f"prob_{class_label}"].quantile(0.35)), 4)
        class_thresholds[class_label] = max(float(class_thresholds.get(class_label, 0.55)), threshold)
    payload["class_thresholds"][panel_name] = class_thresholds
    if not validation_frame["uncertainty_score"].dropna().empty:
        payload["uncertainty_ceiling"] = max(
            float(DEFAULT_THRESHOLD_PAYLOAD["uncertainty_ceiling"]),
            round(float(validation_frame["uncertainty_score"].median()), 4),
        )
    if not validation_frame["disagreement_score"].dropna().empty:
        payload["disagreement_ceiling"] = max(
            float(DEFAULT_THRESHOLD_PAYLOAD["disagreement_ceiling"]),
            round(float(validation_frame["disagreement_score"].median()), 4),
        )
    return payload


def _feature_importance_payload(
    *,
    feature_columns: list[str],
    member_models: dict[str, Any],
    ensemble_weights: dict[str, float],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    aggregated: dict[str, float] = {}
    for member_name, model in member_models.items():
        values: np.ndarray | None = None
        if hasattr(model, "feature_importances_"):
            values = np.asarray(model.feature_importances_, dtype=float)
        elif hasattr(model, "coef_"):
            coef = np.asarray(model.coef_, dtype=float)
            values = np.abs(coef).mean(axis=0)
        if values is None or len(values) != len(feature_columns):
            continue
        member_weight = float(ensemble_weights.get(member_name, 0.0))
        for feature_name, importance in zip(feature_columns, values, strict=False):
            score = float(importance)
            aggregated[feature_name] = aggregated.get(feature_name, 0.0) + score * member_weight
            rows.append({"member_name": member_name, "feature_name": feature_name, "importance": score})
    rows.extend(
        {
            "member_name": "ensemble",
            "feature_name": feature_name,
            "importance": score,
        }
        for feature_name, score in sorted(
            aggregated.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:40]
        )
    return rows


def _fit_panel_model(
    dataset: pd.DataFrame,
    *,
    run_id: str,
    train_end_date: date,
    horizon: int,
    panel_name: str,
    train_dates: list[date],
    validation_dates: list[date],
    artifact_root: Path,
    notes_suffix: str,
) -> _PanelTrainingArtifacts:
    panel_frame = _panel_frame(dataset, horizon=horizon, panel_name=panel_name)
    if "session_date" not in panel_frame.columns:
        panel_frame = pd.DataFrame(columns=["session_date", "target_class"])
    training_run_id = f"{run_id}-h{int(horizon)}-{panel_name.lower()}"
    created_at = pd.Timestamp.utcnow()
    train_frame = panel_frame.loc[panel_frame["session_date"].isin(train_dates)].copy()
    validation_frame = panel_frame.loc[panel_frame["session_date"].isin(validation_dates)].copy()
    fallback_reason = None
    feature_columns: list[str] = []
    validation_prediction_frame = pd.DataFrame()
    metric_rows: list[dict[str, object]] = []
    artifact_path: Path | None = None
    diagnostic_path: Path | None = None
    ensemble_weights = {member_name: 1.0 / len(META_MEMBER_NAMES) for member_name in META_MEMBER_NAMES}
    threshold_payload = default_threshold_payload(panel_name)
    calibration_params = {class_label: None for class_label in PANEL_CLASSES[panel_name]}
    member_models: dict[str, Any] = {}
    if train_frame.empty:
        fallback_reason = "empty_training_frame"
    elif train_frame["target_class"].nunique(dropna=True) < MIN_CLASS_COUNT_FOR_LIVE_MODEL:
        fallback_reason = "single_class_training"

    if not train_frame.empty:
        train_features = feature_frame_with_dummies(train_frame)
        feature_columns = train_features.columns.tolist()
        train_target = train_frame["target_class"].astype(str)
        validation_features = feature_frame_with_dummies(
            validation_frame,
            feature_columns=feature_columns,
        )
        validation_target = (
            validation_frame["target_class"].astype(str)
            if not validation_frame.empty
            else pd.Series(dtype="string")
        )
        validation_summary: dict[str, dict[str, float | None]] = {}
        member_probabilities: dict[str, pd.DataFrame] = {}
        for member_name, estimator in _build_member_estimators(train_frame).items():
            estimator.fit(train_features, train_target)
            member_models[member_name] = estimator
            train_prob = _predict_proba_aligned(
                estimator,
                train_features,
                class_labels=list(PANEL_CLASSES[panel_name]),
            )
            metric_rows.extend(
                _metric_rows(
                    training_run_id=training_run_id,
                    horizon=horizon,
                    panel_name=panel_name,
                    member_name=member_name,
                    split_name="train",
                    actual=train_target,
                    predicted=train_prob.idxmax(axis=1),
                    probability_frame=train_prob,
                )
            )
            if not validation_frame.empty:
                validation_prob = _predict_proba_aligned(
                    estimator,
                    validation_features,
                    class_labels=list(PANEL_CLASSES[panel_name]),
                )
                member_probabilities[member_name] = validation_prob
                validation_metric_rows = _metric_rows(
                    training_run_id=training_run_id,
                    horizon=horizon,
                    panel_name=panel_name,
                    member_name=member_name,
                    split_name="validation",
                    actual=validation_target,
                    predicted=validation_prob.idxmax(axis=1),
                    probability_frame=validation_prob,
                )
                metric_rows.extend(validation_metric_rows)
                validation_summary[member_name] = {
                    row["metric_name"]: row["metric_value"]
                    for row in validation_metric_rows
                    if row["metric_scope"] == "panel"
                }
        if member_models and not validation_frame.empty:
            ensemble_weights = _normalise_weights(validation_summary)
            ensemble_prob = sum(
                member_probabilities[member_name] * float(ensemble_weights.get(member_name, 0.0))
                for member_name in member_probabilities
            )
            ensemble_prob = ensemble_prob.reindex(columns=list(PANEL_CLASSES[panel_name]), fill_value=0.0)
            for class_label in PANEL_CLASSES[panel_name]:
                calibration_params[class_label] = _fit_sigmoid_calibrator(
                    ensemble_prob[class_label],
                    validation_target == class_label,
                )
            calibrated_prob = _apply_sigmoid_calibration(
                ensemble_prob,
                calibration_params=calibration_params,
            )
            predicted_class = calibrated_prob.idxmax(axis=1)
            sorted_probabilities = np.sort(calibrated_prob.to_numpy(), axis=1)
            max_prob = sorted_probabilities[:, -1]
            second_prob = (
                sorted_probabilities[:, -2]
                if calibrated_prob.shape[1] > 1
                else np.zeros(len(calibrated_prob))
            )
            disagreement_values: list[float] = []
            for row_index, class_label in zip(calibrated_prob.index, predicted_class, strict=False):
                member_values = [
                    float(member_probabilities[member_name].loc[row_index, class_label])
                    for member_name in member_probabilities
                ]
                disagreement_values.append(float(np.std(member_values) * 100.0))
            validation_prediction_frame = validation_frame[
                ["session_date", "symbol", "horizon", "checkpoint_time", "panel_name", "target_class"]
            ].copy()
            for class_label in PANEL_CLASSES[panel_name]:
                validation_prediction_frame[f"prob_{class_label}"] = calibrated_prob[class_label].to_numpy()
            validation_prediction_frame["predicted_class"] = predicted_class.to_numpy()
            validation_prediction_frame["predicted_class_probability"] = max_prob
            validation_prediction_frame["confidence_margin"] = max_prob - second_prob
            validation_prediction_frame["uncertainty_score"] = (1.0 - max_prob) * 100.0
            validation_prediction_frame["disagreement_score"] = disagreement_values
            threshold_payload = _threshold_payload(
                panel_name=panel_name,
                validation_frame=validation_prediction_frame,
                calibration_params=calibration_params,
            )
            metric_rows.extend(
                _metric_rows(
                    training_run_id=training_run_id,
                    horizon=horizon,
                    panel_name=panel_name,
                    member_name="ensemble",
                    split_name="validation",
                    actual=validation_prediction_frame["target_class"],
                    predicted=validation_prediction_frame["predicted_class"],
                    probability_frame=calibrated_prob,
                )
            )

    diagnostics_payload = {
        "panel_name": panel_name,
        "horizon": int(horizon),
        "threshold_payload": threshold_payload,
        "feature_importance": _feature_importance_payload(
            feature_columns=feature_columns,
            member_models=member_models,
            ensemble_weights=ensemble_weights,
        ),
        "validation_class_counts": (
            validation_prediction_frame["target_class"].value_counts().to_dict()
            if not validation_prediction_frame.empty
            else {}
        ),
    }
    if member_models:
        artifact_dir = artifact_root / training_run_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = write_model_artifact(
            artifact_dir / "model.pkl",
            {
                "training_run_id": training_run_id,
                "model_domain": INTRADAY_META_MODEL_DOMAIN,
                "model_version": INTRADAY_META_MODEL_VERSION,
                "panel_name": panel_name,
                "horizon": int(horizon),
                "feature_columns": feature_columns,
                "class_labels": list(PANEL_CLASSES[panel_name]),
                "member_models": member_models,
                "ensemble_weights": ensemble_weights,
                "threshold_payload": threshold_payload,
                "calibration_params": calibration_params,
                "validation_prediction_frame": validation_prediction_frame,
                "feature_importance": diagnostics_payload["feature_importance"],
            },
        )
        diagnostic_path = artifact_dir / "diagnostics.json"
        diagnostic_path.write_text(
            json.dumps(diagnostics_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    notes = (
        f"{panel_name} horizon={int(horizon)} "
        f"train_rows={len(train_frame)} validation_rows={len(validation_frame)} "
        f"fallback={fallback_reason or 'none'} {notes_suffix}"
    ).strip()
    training_row = {
        "training_run_id": training_run_id,
        "run_id": run_id,
        "model_domain": INTRADAY_META_MODEL_DOMAIN,
        "model_version": INTRADAY_META_MODEL_VERSION,
        "horizon": int(horizon),
        "panel_name": panel_name,
        "train_end_date": train_end_date,
        "training_window_start": min(train_dates) if train_dates else None,
        "training_window_end": max(train_dates) if train_dates else None,
        "validation_window_start": min(validation_dates) if validation_dates else None,
        "validation_window_end": max(validation_dates) if validation_dates else None,
        "train_row_count": int(len(train_frame)),
        "validation_row_count": int(len(validation_frame)),
        "train_session_count": len(dict.fromkeys(train_dates)),
        "validation_session_count": len(dict.fromkeys(validation_dates)),
        "feature_count": int(len(feature_columns)),
        "ensemble_weight_json": json_or_none(ensemble_weights),
        "model_family_json": json_or_none(
            {
                "member_names": list(META_MEMBER_NAMES),
                "class_labels": list(PANEL_CLASSES[panel_name]),
                "panel_name": panel_name,
            }
        ),
        "threshold_payload_json": json_or_none(threshold_payload),
        "diagnostic_artifact_uri": str(diagnostic_path) if diagnostic_path else None,
        "metadata_json": json_or_none({"calibration_params": calibration_params}),
        "fallback_flag": fallback_reason is not None,
        "fallback_reason": fallback_reason,
        "artifact_uri": str(artifact_path) if artifact_path else None,
        "notes": notes,
        "status": "success",
        "created_at": created_at,
    }
    return _PanelTrainingArtifacts(
        training_row=training_row,
        metric_rows=metric_rows,
        artifact_path=artifact_path,
        diagnostic_path=diagnostic_path,
        validation_frame=validation_prediction_frame,
        notes=notes,
    )


def train_intraday_meta_models(
    settings: Settings,
    *,
    train_end_date: date,
    horizons: list[int],
    start_session_date: date | None = None,
    validation_sessions: int = 10,
) -> IntradayMetaTrainingResult:
    ensure_storage_layout(settings)
    effective_start = start_session_date or date(2026, 1, 1)
    ensure_intraday_meta_label_inputs(
        settings,
        start_session_date=effective_start,
        end_session_date=train_end_date,
        horizons=horizons,
        ranking_version=SELECTION_ENGINE_VERSION,
    )
    with activate_run_context(
        "train_intraday_meta_models",
        as_of_date=train_end_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=train_end_date,
                input_sources=[
                    "fact_intraday_adjusted_entry_decision",
                    "fact_intraday_timing_outcome",
                    "fact_intraday_active_policy",
                ],
                notes=(
                    "Train intraday meta-models. "
                    f"range={effective_start.isoformat()}..{train_end_date.isoformat()}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                dataset = assemble_intraday_meta_dataset_frame(
                    settings,
                    start_session_date=effective_start,
                    end_session_date=train_end_date,
                    horizons=horizons,
                    ranking_version=SELECTION_ENGINE_VERSION,
                    connection=connection,
                )
                training_rows: list[dict[str, object]] = []
                metric_rows: list[dict[str, object]] = []
                artifact_paths: list[str] = []
                artifact_root = settings.paths.artifacts_dir / "intraday_meta_models" / run_context.run_id
                for horizon in horizons:
                    for panel_name in (ENTER_PANEL, WAIT_PANEL):
                        panel_frame = _panel_frame(dataset, horizon=horizon, panel_name=panel_name)
                        session_date_values = (
                            panel_frame["session_date"].tolist()
                            if not panel_frame.empty and "session_date" in panel_frame.columns
                            else []
                        )
                        train_dates, validation_dates, fallback_used, fallback_reason = _session_split(
                            session_date_values,
                            validation_sessions=validation_sessions,
                        )
                        artifacts = _fit_panel_model(
                            dataset,
                            run_id=run_context.run_id,
                            train_end_date=train_end_date,
                            horizon=horizon,
                            panel_name=panel_name,
                            train_dates=train_dates,
                            validation_dates=validation_dates,
                            artifact_root=artifact_root,
                            notes_suffix=f"split_fallback={fallback_used}:{fallback_reason}",
                        )
                        training_rows.append(artifacts.training_row)
                        metric_rows.extend(artifacts.metric_rows)
                        if artifacts.artifact_path:
                            artifact_paths.append(str(artifacts.artifact_path))
                        if artifacts.diagnostic_path:
                            artifact_paths.append(str(artifacts.diagnostic_path))
                if training_rows:
                    upsert_meta_training_runs(connection, pd.DataFrame(training_rows))
                if metric_rows:
                    upsert_meta_metric_summary(connection, pd.DataFrame(metric_rows))
                notes = (
                    "Intraday meta-model training completed. "
                    f"training_runs={len(training_rows)} metrics={len(metric_rows)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayMetaTrainingResult(
                    run_id=run_context.run_id,
                    train_end_date=train_end_date,
                    training_run_count=len(training_rows),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday meta-model training failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def _load_latest_meta_training_rows(
    connection,
    *,
    as_of_date: date,
    horizons: list[int],
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM vw_latest_model_training_run
        WHERE model_domain = ?
          AND model_version = ?
          AND train_end_date <= ?
          AND horizon IN ({placeholders})
        ORDER BY train_end_date DESC, horizon, panel_name
        """,
        [INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION, as_of_date, *horizons],
    ).fetchdf()


def calibrate_intraday_meta_thresholds(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
) -> IntradayMetaThresholdCalibrationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "calibrate_intraday_meta_thresholds",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_model_training_run", "fact_model_metric_summary"],
                notes=f"Calibrate intraday meta-model thresholds for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                training_rows = _load_latest_meta_training_rows(
                    connection,
                    as_of_date=as_of_date,
                    horizons=horizons,
                )
                updated_rows: list[dict[str, object]] = []
                artifact_paths: list[str] = []
                for row in training_rows.itertuples(index=False):
                    if not row.artifact_uri:
                        continue
                    resolved_artifact_path = resolve_artifact_path(settings, row.artifact_uri)
                    if resolved_artifact_path is None:
                        continue
                    payload = load_model_artifact(resolved_artifact_path)
                    validation_frame = payload.get("validation_prediction_frame")
                    if not isinstance(validation_frame, pd.DataFrame):
                        continue
                    calibration_params = payload.get("calibration_params", {})
                    threshold_payload = _threshold_payload(
                        panel_name=str(row.panel_name),
                        validation_frame=validation_frame,
                        calibration_params=calibration_params,
                    )
                    payload["threshold_payload"] = threshold_payload
                    artifact_path = write_model_artifact(resolved_artifact_path, payload)
                    updated_rows.append(
                        {
                            **row._asdict(),
                            "threshold_payload_json": json_or_none(threshold_payload),
                            "metadata_json": json_or_none(
                                {
                                    "calibrated_by_run_id": run_context.run_id,
                                    "threshold_version": threshold_payload.get("threshold_version"),
                                }
                            ),
                        }
                    )
                    artifact_paths.append(str(artifact_path))
                if updated_rows:
                    upsert_meta_training_runs(connection, pd.DataFrame(updated_rows))
                notes = (
                    "Intraday meta threshold calibration completed. "
                    f"updated_rows={len(updated_rows)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayMetaThresholdCalibrationResult(
                    run_id=run_context.run_id,
                    row_count=len(updated_rows),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday meta threshold calibration failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def run_intraday_meta_walkforward(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    mode: str,
    train_sessions: int,
    validation_sessions: int,
    test_sessions: int,
    step_sessions: int,
    horizons: list[int],
) -> IntradayMetaWalkforwardResult:
    ensure_storage_layout(settings)
    effective_mode = (
        "ANCHORED_WALKFORWARD" if str(mode).lower().startswith("anchor") else "ROLLING_WALKFORWARD"
    )
    ensure_intraday_meta_label_inputs(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
        ranking_version=SELECTION_ENGINE_VERSION,
    )
    with activate_run_context(
        "run_intraday_meta_walkforward",
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
                input_sources=[
                    "fact_intraday_adjusted_entry_decision",
                    "fact_intraday_timing_outcome",
                    "fact_intraday_active_policy",
                ],
                notes=(
                    "Run intraday meta-model walk-forward. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()} mode={effective_mode}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                dataset = assemble_intraday_meta_dataset_frame(
                    settings,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    ranking_version=SELECTION_ENGINE_VERSION,
                    connection=connection,
                )
                session_dates = (
                    pd.to_datetime(dataset["session_date"]).dt.date.tolist() if not dataset.empty else []
                )
                splits = _walkforward_splits(
                    session_dates,
                    mode="anchored" if effective_mode == "ANCHORED_WALKFORWARD" else "rolling",
                    train_sessions=train_sessions,
                    validation_sessions=validation_sessions,
                    test_sessions=test_sessions,
                    step_sessions=step_sessions,
                )
                training_rows: list[dict[str, object]] = []
                metric_rows: list[dict[str, object]] = []
                artifact_paths: list[str] = []
                artifact_root = settings.paths.artifacts_dir / "intraday_meta_walkforward" / run_context.run_id
                for split in splits:
                    for horizon in horizons:
                        for panel_name in (ENTER_PANEL, WAIT_PANEL):
                            artifacts = _fit_panel_model(
                                dataset,
                                run_id=f"{run_context.run_id}-split{int(split['split_index'])}",
                                train_end_date=max(split["train_dates"]) if split["train_dates"] else end_session_date,
                                horizon=horizon,
                                panel_name=panel_name,
                                train_dates=split["train_dates"],
                                validation_dates=split["validation_dates"],
                                artifact_root=artifact_root,
                                notes_suffix=(
                                    f"walkforward_mode={effective_mode} split={split['split_index']} "
                                    f"fallback={split['fallback_used']}:{split['fallback_reason']}"
                                ),
                            )
                            training_rows.append(artifacts.training_row)
                            metric_rows.extend(artifacts.metric_rows)
                            if artifacts.artifact_path:
                                artifact_paths.append(str(artifacts.artifact_path))
                            if artifacts.diagnostic_path:
                                artifact_paths.append(str(artifacts.diagnostic_path))
                            if split["test_dates"] and artifacts.artifact_path:
                                payload = load_model_artifact(artifacts.artifact_path)
                                test_frame = _panel_frame(dataset, horizon=horizon, panel_name=panel_name)
                                test_frame = test_frame.loc[test_frame["session_date"].isin(split["test_dates"])].copy()
                                if not test_frame.empty:
                                    test_features = feature_frame_with_dummies(
                                        test_frame,
                                        feature_columns=list(payload["feature_columns"]),
                                    )
                                    weighted_probs: list[pd.DataFrame] = []
                                    for member_name, model in payload["member_models"].items():
                                        member_prob = _predict_proba_aligned(
                                            model,
                                            test_features,
                                            class_labels=list(payload["class_labels"]),
                                        )
                                        metric_rows.extend(
                                            _metric_rows(
                                                training_run_id=str(artifacts.training_row["training_run_id"]),
                                                horizon=horizon,
                                                panel_name=panel_name,
                                                member_name=member_name,
                                                split_name="test",
                                                actual=test_frame["target_class"],
                                                predicted=member_prob.idxmax(axis=1),
                                                probability_frame=member_prob,
                                            )
                                        )
                                        weighted_probs.append(
                                            member_prob * float(payload["ensemble_weights"].get(member_name, 0.0))
                                        )
                                    if weighted_probs:
                                        ensemble_prob = sum(weighted_probs)
                                        ensemble_prob = _apply_sigmoid_calibration(
                                            ensemble_prob,
                                            calibration_params=payload.get("calibration_params", {}),
                                        )
                                        metric_rows.extend(
                                            _metric_rows(
                                                training_run_id=str(artifacts.training_row["training_run_id"]),
                                                horizon=horizon,
                                                panel_name=panel_name,
                                                member_name="ensemble",
                                                split_name="test",
                                                actual=test_frame["target_class"],
                                                predicted=ensemble_prob.idxmax(axis=1),
                                                probability_frame=ensemble_prob,
                                            )
                                        )
                if training_rows:
                    upsert_meta_training_runs(connection, pd.DataFrame(training_rows))
                if metric_rows:
                    upsert_meta_metric_summary(connection, pd.DataFrame(metric_rows))
                notes = (
                    "Intraday meta walk-forward completed. "
                    f"splits={len(splits)} training_runs={len(training_rows)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayMetaWalkforwardResult(
                    run_id=run_context.run_id,
                    split_count=len(splits),
                    training_run_count=len(training_rows),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday meta walk-forward failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def _select_training_rows_for_freeze(
    connection,
    *,
    as_of_date: date,
    horizons: list[int] | None = None,
) -> pd.DataFrame:
    horizon_filter = ""
    parameters: list[object] = [INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION, as_of_date]
    if horizons:
        placeholders = ",".join("?" for _ in horizons)
        horizon_filter = f" AND horizon IN ({placeholders})"
        parameters.extend(horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM vw_latest_model_training_run
        WHERE model_domain = ?
          AND model_version = ?
          AND train_end_date <= ?
          AND status = 'success'
          {horizon_filter}
        ORDER BY horizon, panel_name, train_end_date DESC
        """,
        parameters,
    ).fetchdf()


def _meta_activation_signature_rows(frame: pd.DataFrame) -> list[tuple[object, ...]]:
    if frame.empty:
        return []
    signatures = [
        (
            int(row.horizon),
            str(row.panel_name),
            str(row.training_run_id),
            "" if pd.isna(getattr(row, "threshold_payload_json", None)) else str(row.threshold_payload_json),
            ""
            if pd.isna(getattr(row, "calibration_summary_json", None))
            else str(row.calibration_summary_json),
        )
        for row in frame.itertuples(index=False)
    ]
    return sorted(signatures)


def _current_active_meta_rows(
    connection,
    *,
    as_of_date: date,
    horizons: list[int] | None,
) -> pd.DataFrame:
    horizon_filter = ""
    parameters: list[object] = [as_of_date, as_of_date]
    if horizons:
        placeholders = ",".join("?" for _ in horizons)
        horizon_filter = f" AND horizon IN ({placeholders})"
        parameters.extend(horizons)
    return connection.execute(
        f"""
        SELECT
            horizon,
            panel_name,
            training_run_id,
            threshold_payload_json,
            calibration_summary_json
        FROM fact_intraday_active_meta_model
        WHERE active_flag = TRUE
          AND effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
          {horizon_filter}
        ORDER BY horizon, panel_name
        """,
        parameters,
    ).fetchdf()


def _load_meta_test_metric_map(
    connection,
    *,
    training_run_ids: list[str],
    metric_name: str = "macro_f1",
) -> dict[tuple[str, str], float]:
    if not training_run_ids:
        return {}
    placeholders = ",".join("?" for _ in training_run_ids)
    frame = connection.execute(
        f"""
        SELECT
            training_run_id,
            panel_name,
            AVG(metric_value) AS metric_value
        FROM fact_model_metric_summary
        WHERE training_run_id IN ({placeholders})
          AND model_domain = ?
          AND model_version = ?
          AND member_name = 'ensemble'
          AND split_name = 'test'
          AND metric_name = ?
        GROUP BY training_run_id, panel_name
        """,
        [
            *training_run_ids,
            INTRADAY_META_MODEL_DOMAIN,
            INTRADAY_META_MODEL_VERSION,
            metric_name,
        ],
    ).fetchdf()
    if frame.empty:
        return {}
    return {
        (str(row.training_run_id), str(row.panel_name)): float(row.metric_value)
        for row in frame.itertuples(index=False)
        if pd.notna(row.metric_value)
    }


def freeze_intraday_active_meta_model(
    settings: Settings,
    *,
    as_of_date: date,
    source: str,
    note: str | None = None,
    horizons: list[int] | None = None,
    promotion_type: str = "MANUAL_FREEZE",
) -> IntradayActiveMetaModelResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "freeze_intraday_active_meta_model",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_model_training_run", "fact_intraday_active_meta_model"],
                notes=f"Freeze intraday active meta-models for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                training_rows = _select_training_rows_for_freeze(
                    connection,
                    as_of_date=as_of_date,
                    horizons=horizons,
                )
                if training_rows.empty:
                    notes = (
                        "Intraday active meta-model freeze was a no-op. "
                        "No latest training rows were available."
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return IntradayActiveMetaModelResult(
                        run_id=run_context.run_id,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                selected_signature_frame = training_rows.copy()
                selected_signature_frame["calibration_summary_json"] = selected_signature_frame[
                    "metadata_json"
                ]
                current_active_rows = _current_active_meta_rows(
                    connection,
                    as_of_date=as_of_date,
                    horizons=horizons,
                )
                if _meta_activation_signature_rows(
                    current_active_rows
                ) == _meta_activation_signature_rows(selected_signature_frame):
                    notes = (
                        "Intraday active meta-model freeze was a no-op. "
                        "Latest trained meta-models are already active."
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return IntradayActiveMetaModelResult(
                        run_id=run_context.run_id,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                now_ts = now_local(settings.app.timezone)
                frozen_rows: list[dict[str, object]] = []
                for row in training_rows.itertuples(index=False):
                    connection.execute(
                        """
                        UPDATE fact_intraday_active_meta_model
                        SET effective_to_date = ?, active_flag = FALSE, updated_at = ?
                        WHERE horizon = ?
                          AND panel_name = ?
                          AND effective_from_date <= ?
                          AND (effective_to_date IS NULL OR effective_to_date >= ?)
                          AND active_flag = TRUE
                        """,
                        [
                            as_of_date - timedelta(days=1),
                            now_ts,
                            int(row.horizon),
                            str(row.panel_name),
                            as_of_date,
                            as_of_date,
                        ],
                    )
                    frozen_rows.append(
                        {
                            "active_meta_model_id": f"{run_context.run_id}-{int(row.horizon)}-{row.panel_name}",
                            "horizon": int(row.horizon),
                            "panel_name": str(row.panel_name),
                            "training_run_id": str(row.training_run_id),
                            "model_version": str(row.model_version),
                            "source_type": source,
                            "promotion_type": promotion_type,
                            "threshold_payload_json": row.threshold_payload_json,
                            "calibration_summary_json": row.metadata_json,
                            "effective_from_date": as_of_date,
                            "effective_to_date": None,
                            "active_flag": True,
                            "rollback_of_active_meta_model_id": None,
                            "note": note,
                            "created_at": now_ts,
                            "updated_at": now_ts,
                        }
                    )
                if frozen_rows:
                    upsert_intraday_active_meta_model(connection, pd.DataFrame(frozen_rows))
                notes = (
                    "Intraday active meta-model freeze completed. "
                    f"rows={len(frozen_rows)} source={source}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_ts,
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayActiveMetaModelResult(
                    run_id=run_context.run_id,
                    row_count=len(frozen_rows),
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
                    notes="Intraday active meta-model freeze failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def run_intraday_meta_auto_promotion(
    settings: Settings,
    *,
    as_of_date: date,
    source: str,
    note: str | None = None,
    horizons: list[int] | None = None,
) -> IntradayMetaPromotionResult:
    ensure_storage_layout(settings)
    target_horizons = sorted({int(value) for value in (horizons or [1, 5])})
    with activate_run_context(
        "run_intraday_meta_auto_promotion",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=[
                    "fact_model_training_run",
                    "fact_model_metric_summary",
                    "fact_intraday_active_meta_model",
                    "fact_alpha_active_model",
                ],
                notes=f"Run intraday meta auto-promotion for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                training_rows = _select_training_rows_for_freeze(
                    connection,
                    as_of_date=as_of_date,
                    horizons=target_horizons,
                )
                artifact_paths: list[str] = []
                if training_rows.empty:
                    notes = (
                        "Intraday meta auto-promotion was a no-op. "
                        "No latest training rows were available."
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return IntradayMetaPromotionResult(
                        run_id=run_context.run_id,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                alpha_status = resolve_alpha_lineage_status(
                    connection,
                    as_of_date=as_of_date,
                    horizons=target_horizons,
                )
                current_active_rows = _current_active_meta_rows(
                    connection,
                    as_of_date=as_of_date,
                    horizons=target_horizons,
                )
                candidate_metric_map = _load_meta_test_metric_map(
                    connection,
                    training_run_ids=training_rows["training_run_id"].astype(str).tolist(),
                )
                current_metric_map = _load_meta_test_metric_map(
                    connection,
                    training_run_ids=current_active_rows["training_run_id"].astype(str).tolist()
                    if not current_active_rows.empty
                    else [],
                )
                current_by_horizon_panel = {
                    (int(row.horizon), str(row.panel_name)): row
                    for row in current_active_rows.itertuples(index=False)
                }
                promote_horizons: list[int] = []
                decision_by_horizon: dict[int, dict[str, object]] = {}
                for horizon, horizon_rows in training_rows.groupby("horizon", sort=True):
                    horizon_int = int(horizon)
                    if horizon_int in alpha_status.blocked_horizons:
                        decision_by_horizon[horizon_int] = {
                            "decision": "BLOCKED_ALPHA_STABILIZING",
                            "reason": "alpha_stabilization_window",
                        }
                        continue
                    eligible = True
                    strictly_better = False
                    identical = True
                    panel_payloads: list[dict[str, object]] = []
                    for row in horizon_rows.itertuples(index=False):
                        panel_key = (horizon_int, str(row.panel_name))
                        current_row = current_by_horizon_panel.get(panel_key)
                        candidate_metric = candidate_metric_map.get(
                            (str(row.training_run_id), str(row.panel_name))
                        )
                        current_metric = (
                            current_metric_map.get(
                                (
                                    str(current_row.training_run_id),
                                    str(current_row.panel_name),
                                )
                            )
                            if current_row is not None
                            else None
                        )
                        same_training = bool(
                            current_row is not None
                            and str(current_row.training_run_id) == str(row.training_run_id)
                        )
                        identical = identical and same_training
                        if candidate_metric is None:
                            eligible = False
                        elif current_metric is not None and candidate_metric < current_metric:
                            eligible = False
                        elif current_metric is None or candidate_metric > current_metric:
                            strictly_better = True
                        panel_payloads.append(
                            {
                                "panel_name": str(row.panel_name),
                                "candidate_training_run_id": str(row.training_run_id),
                                "candidate_macro_f1": candidate_metric,
                                "current_training_run_id": (
                                    None
                                    if current_row is None
                                    else str(current_row.training_run_id)
                                ),
                                "current_macro_f1": current_metric,
                                "same_training_run": same_training,
                            }
                        )
                    if identical:
                        decision_by_horizon[horizon_int] = {
                            "decision": "NO_PROMOTION_ALREADY_ACTIVE",
                            "panels": panel_payloads,
                        }
                    elif eligible and strictly_better:
                        promote_horizons.append(horizon_int)
                        decision_by_horizon[horizon_int] = {
                            "decision": "PROMOTE",
                            "panels": panel_payloads,
                        }
                    else:
                        decision_by_horizon[horizon_int] = {
                            "decision": "NO_PROMOTION_OOS_GATE",
                            "panels": panel_payloads,
                        }
                if not promote_horizons:
                    notes = (
                        "Intraday meta auto-promotion was a no-op. "
                        "No horizon satisfied the OOS promotion gate."
                    )
                    row_count = 0
                else:
                    freeze_result = freeze_intraday_active_meta_model(
                        settings,
                        as_of_date=as_of_date,
                        source=source,
                        note=note,
                        horizons=promote_horizons,
                        promotion_type="AUTO_PROMOTION",
                    )
                    row_count = int(freeze_result.row_count)
                    notes = freeze_result.notes
                payload = {
                    "as_of_date": as_of_date.isoformat(),
                    "promote_horizons": promote_horizons,
                    "alpha_lineage": alpha_status.detail_by_horizon,
                    "decision_by_horizon": decision_by_horizon,
                }
                artifact_paths.append(
                    write_promotion_decision_artifact(
                        settings,
                        dataset="intraday_meta_promotion",
                        run_id=run_context.run_id,
                        filename="meta_promotion_decision.json",
                        payload=payload,
                    )
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayMetaPromotionResult(
                    run_id=run_context.run_id,
                    row_count=row_count,
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday meta auto-promotion failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def rollback_intraday_active_meta_model(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    note: str | None = None,
) -> IntradayActiveMetaModelResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "rollback_intraday_active_meta_model",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_intraday_active_meta_model"],
                notes=f"Rollback intraday active meta-models for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                current_rows = connection.execute(
                    f"""
                    SELECT *
                    FROM fact_intraday_active_meta_model
                    WHERE horizon IN ({placeholders})
                      AND effective_from_date <= ?
                      AND (effective_to_date IS NULL OR effective_to_date >= ?)
                      AND active_flag = TRUE
                    ORDER BY horizon, panel_name, effective_from_date DESC
                    """,
                    [*horizons, as_of_date, as_of_date],
                ).fetchdf()
                now_ts = now_local(settings.app.timezone)
                rollback_rows: list[dict[str, object]] = []
                for current in current_rows.itertuples(index=False):
                    previous = connection.execute(
                        """
                        SELECT *
                        FROM fact_intraday_active_meta_model
                        WHERE horizon = ?
                          AND panel_name = ?
                          AND effective_from_date < ?
                        ORDER BY effective_from_date DESC, updated_at DESC
                        LIMIT 1
                        """,
                        [int(current.horizon), str(current.panel_name), as_of_date],
                    ).fetchdf()
                    connection.execute(
                        """
                        UPDATE fact_intraday_active_meta_model
                        SET effective_to_date = ?, active_flag = FALSE, updated_at = ?
                        WHERE active_meta_model_id = ?
                        """,
                        [as_of_date - timedelta(days=1), now_ts, str(current.active_meta_model_id)],
                    )
                    if previous.empty:
                        continue
                    restored = previous.iloc[0]
                    rollback_rows.append(
                        {
                            "active_meta_model_id": f"{run_context.run_id}-{int(current.horizon)}-{current.panel_name}",
                            "horizon": int(restored["horizon"]),
                            "panel_name": str(restored["panel_name"]),
                            "training_run_id": str(restored["training_run_id"]),
                            "model_version": str(restored["model_version"]),
                            "source_type": "rollback_restore",
                            "promotion_type": "ROLLBACK_RESTORE",
                            "threshold_payload_json": restored["threshold_payload_json"],
                            "calibration_summary_json": restored["calibration_summary_json"],
                            "effective_from_date": as_of_date,
                            "effective_to_date": None,
                            "active_flag": True,
                            "rollback_of_active_meta_model_id": str(current.active_meta_model_id),
                            "note": note,
                            "created_at": now_ts,
                            "updated_at": now_ts,
                        }
                    )
                if rollback_rows:
                    upsert_intraday_active_meta_model(connection, pd.DataFrame(rollback_rows))
                notes = (
                    "Intraday active meta-model rollback completed. "
                    f"rows={len(rollback_rows)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_ts,
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayActiveMetaModelResult(
                    run_id=run_context.run_id,
                    row_count=len(rollback_rows),
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
                    notes="Intraday active meta-model rollback failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
