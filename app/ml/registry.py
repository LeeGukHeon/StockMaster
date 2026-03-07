from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.common.paths import ensure_directory


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def write_model_artifact(path: Path, payload: dict[str, Any]) -> Path:
    ensure_directory(path.parent)
    with path.open("wb") as handle:
        pickle.dump(payload, handle)
    return path


def load_model_artifact(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        payload = pickle.load(handle)
    if not isinstance(payload, dict):
        raise TypeError("Model artifact payload must be a dictionary.")
    return payload


def upsert_model_training_runs(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    if frame.empty:
        return
    connection.register("model_training_run_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_model_training_run
        WHERE training_run_id IN (
            SELECT training_run_id
            FROM model_training_run_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_model_training_run (
            training_run_id,
            run_id,
            model_version,
            horizon,
            train_end_date,
            training_window_start,
            training_window_end,
            validation_window_start,
            validation_window_end,
            train_row_count,
            validation_row_count,
            feature_count,
            ensemble_weight_json,
            model_family_json,
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
            model_version,
            horizon,
            train_end_date,
            training_window_start,
            training_window_end,
            validation_window_start,
            validation_window_end,
            train_row_count,
            validation_row_count,
            feature_count,
            ensemble_weight_json,
            model_family_json,
            fallback_flag,
            fallback_reason,
            artifact_uri,
            notes,
            status,
            created_at
        FROM model_training_run_stage
        """
    )
    connection.unregister("model_training_run_stage")


def upsert_model_member_predictions(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    if frame.empty:
        return
    connection.register("model_member_prediction_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_model_member_prediction
        WHERE (
            training_run_id,
            as_of_date,
            symbol,
            horizon,
            prediction_role,
            member_name
        ) IN (
            SELECT
                training_run_id,
                as_of_date,
                symbol,
                horizon,
                prediction_role,
                member_name
            FROM model_member_prediction_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_model_member_prediction (
            training_run_id,
            as_of_date,
            symbol,
            horizon,
            model_version,
            prediction_role,
            member_name,
            predicted_excess_return,
            actual_excess_return,
            residual,
            fallback_flag,
            fallback_reason,
            created_at
        )
        SELECT
            training_run_id,
            as_of_date,
            symbol,
            horizon,
            model_version,
            prediction_role,
            member_name,
            predicted_excess_return,
            actual_excess_return,
            residual,
            fallback_flag,
            fallback_reason,
            created_at
        FROM model_member_prediction_stage
        """
    )
    connection.unregister("model_member_prediction_stage")


def upsert_model_metric_summary(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    if frame.empty:
        return
    connection.register("model_metric_summary_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_model_metric_summary
        WHERE (
            training_run_id,
            member_name,
            split_name,
            metric_name
        ) IN (
            SELECT
                training_run_id,
                member_name,
                split_name,
                metric_name
            FROM model_metric_summary_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_model_metric_summary (
            training_run_id,
            model_version,
            horizon,
            member_name,
            split_name,
            metric_name,
            metric_value,
            sample_count,
            created_at
        )
        SELECT
            training_run_id,
            model_version,
            horizon,
            member_name,
            split_name,
            metric_name,
            metric_value,
            sample_count,
            created_at
        FROM model_metric_summary_stage
        """
    )
    connection.unregister("model_metric_summary_stage")


def load_latest_training_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    horizon: int,
    model_version: str,
    train_end_date: object,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT
            training_run_id,
            run_id,
            model_version,
            horizon,
            train_end_date,
            training_window_start,
            training_window_end,
            validation_window_start,
            validation_window_end,
            train_row_count,
            validation_row_count,
            feature_count,
            ensemble_weight_json,
            model_family_json,
            fallback_flag,
            fallback_reason,
            artifact_uri,
            notes,
            status,
            created_at
        FROM fact_model_training_run
        WHERE horizon = ?
          AND model_version = ?
          AND train_end_date <= ?
          AND status = 'success'
        ORDER BY train_end_date DESC, created_at DESC
        LIMIT 1
        """,
        [horizon, model_version, train_end_date],
    ).fetchone()
    if row is None:
        return None
    keys = [
        "training_run_id",
        "run_id",
        "model_version",
        "horizon",
        "train_end_date",
        "training_window_start",
        "training_window_end",
        "validation_window_start",
        "validation_window_end",
        "train_row_count",
        "validation_row_count",
        "feature_count",
        "ensemble_weight_json",
        "model_family_json",
        "fallback_flag",
        "fallback_reason",
        "artifact_uri",
        "notes",
        "status",
        "created_at",
    ]
    payload = dict(zip(keys, row, strict=True))
    for column in ("ensemble_weight_json", "model_family_json"):
        if payload[column]:
            payload[column] = json.loads(str(payload[column]))
    return payload
