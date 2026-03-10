from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.common.paths import ensure_directory
from app.ml.constants import MODEL_DOMAIN


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _prepare_stage_frame(
    frame: pd.DataFrame,
    *,
    defaults: dict[str, Any],
) -> pd.DataFrame:
    prepared = frame.copy()
    for column, default_value in defaults.items():
        if column not in prepared.columns:
            prepared[column] = default_value
    return prepared


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
    stage = _prepare_stage_frame(
        frame,
        defaults={
            "model_domain": None,
            "model_spec_id": None,
            "estimation_scheme": None,
            "rolling_window_days": None,
            "panel_name": None,
            "train_session_count": None,
            "validation_session_count": None,
            "threshold_payload_json": None,
            "diagnostic_artifact_uri": None,
            "metadata_json": None,
        },
    )
    connection.register("model_training_run_stage", stage)
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
            model_domain,
            model_version,
            model_spec_id,
            estimation_scheme,
            rolling_window_days,
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
            model_spec_id,
            estimation_scheme,
            rolling_window_days,
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
        FROM model_training_run_stage
        """
    )
    connection.unregister("model_training_run_stage")


def upsert_alpha_model_specs(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    if frame.empty:
        return
    stage = _prepare_stage_frame(
        frame,
        defaults={
            "model_domain": MODEL_DOMAIN,
            "rolling_window_days": None,
            "feature_version": None,
            "label_version": None,
            "selection_engine_version": None,
            "spec_payload_json": None,
            "active_candidate_flag": True,
            "created_at": pd.Timestamp.utcnow(),
            "updated_at": pd.Timestamp.utcnow(),
        },
    )
    connection.register("alpha_model_spec_stage", stage)
    connection.execute(
        """
        INSERT OR REPLACE INTO dim_alpha_model_spec (
            model_spec_id,
            model_domain,
            model_version,
            estimation_scheme,
            rolling_window_days,
            feature_version,
            label_version,
            selection_engine_version,
            spec_payload_json,
            active_candidate_flag,
            created_at,
            updated_at
        )
        SELECT
            model_spec_id,
            model_domain,
            model_version,
            estimation_scheme,
            rolling_window_days,
            feature_version,
            label_version,
            selection_engine_version,
            spec_payload_json,
            active_candidate_flag,
            created_at,
            updated_at
        FROM alpha_model_spec_stage
        """
    )
    connection.unregister("alpha_model_spec_stage")


def load_alpha_model_specs(
    connection: duckdb.DuckDBPyConnection,
    *,
    model_domain: str | None = None,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    where_clauses = ["WHERE 1 = 1"]
    parameters: list[Any] = []
    if model_domain is not None:
        where_clauses.append("AND model_domain = ?")
        parameters.append(model_domain)
    if active_only:
        where_clauses.append("AND active_candidate_flag = TRUE")
    rows = connection.execute(
        """
        SELECT
            model_spec_id,
            model_domain,
            model_version,
            estimation_scheme,
            rolling_window_days,
            feature_version,
            label_version,
            selection_engine_version,
            spec_payload_json,
            active_candidate_flag,
            created_at,
            updated_at
        FROM dim_alpha_model_spec
        """
        + "\n".join(where_clauses)
        + "\nORDER BY model_spec_id",
        parameters,
    ).fetchall()
    keys = [
        "model_spec_id",
        "model_domain",
        "model_version",
        "estimation_scheme",
        "rolling_window_days",
        "feature_version",
        "label_version",
        "selection_engine_version",
        "spec_payload_json",
        "active_candidate_flag",
        "created_at",
        "updated_at",
    ]
    return [
        _decode_json_columns(dict(zip(keys, row, strict=True)), ("spec_payload_json",))
        for row in rows
    ]


def upsert_alpha_active_models(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    if frame.empty:
        return
    stage = _prepare_stage_frame(
        frame,
        defaults={
            "promotion_report_json": None,
            "effective_to_date": None,
            "rollback_of_active_alpha_model_id": None,
            "note": None,
        },
    )
    connection.register("alpha_active_model_stage", stage)
    connection.execute(
        """
        DELETE FROM fact_alpha_active_model
        WHERE active_alpha_model_id IN (
            SELECT active_alpha_model_id
            FROM alpha_active_model_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_alpha_active_model (
            active_alpha_model_id,
            horizon,
            model_spec_id,
            training_run_id,
            model_version,
            source_type,
            promotion_type,
            promotion_report_json,
            effective_from_date,
            effective_to_date,
            active_flag,
            rollback_of_active_alpha_model_id,
            note,
            created_at,
            updated_at
        )
        SELECT
            active_alpha_model_id,
            horizon,
            model_spec_id,
            training_run_id,
            model_version,
            source_type,
            promotion_type,
            promotion_report_json,
            effective_from_date,
            effective_to_date,
            active_flag,
            rollback_of_active_alpha_model_id,
            note,
            created_at,
            updated_at
        FROM alpha_active_model_stage
        """
    )
    connection.unregister("alpha_active_model_stage")


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


def _decode_json_columns(payload: dict[str, Any], columns: tuple[str, ...]) -> dict[str, Any]:
    decoded = payload.copy()
    for column in columns:
        value = decoded.get(column)
        if value:
            decoded[column] = json.loads(str(value))
    return decoded


def _deserialize_training_run_row(row: Any) -> dict[str, Any]:
    keys = [
        "training_run_id",
        "run_id",
        "model_domain",
        "model_version",
        "model_spec_id",
        "estimation_scheme",
        "rolling_window_days",
        "horizon",
        "panel_name",
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
        "threshold_payload_json",
        "diagnostic_artifact_uri",
        "metadata_json",
        "fallback_flag",
        "fallback_reason",
        "artifact_uri",
        "notes",
        "status",
        "created_at",
    ]
    payload = dict(zip(keys, row, strict=True))
    return _decode_json_columns(
        payload,
        ("ensemble_weight_json", "model_family_json", "threshold_payload_json", "metadata_json"),
    )


def _training_run_select_sql(*, extra_where: str = "") -> str:
    return f"""
        SELECT
            training_run_id,
            run_id,
            model_domain,
            model_version,
            model_spec_id,
            estimation_scheme,
            rolling_window_days,
            horizon,
            panel_name,
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
            threshold_payload_json,
            diagnostic_artifact_uri,
            metadata_json,
            fallback_flag,
            fallback_reason,
            artifact_uri,
            notes,
            status,
            created_at
        FROM fact_model_training_run
        WHERE 1 = 1
        {extra_where}
    """


def load_latest_training_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    horizon: int,
    model_version: str,
    train_end_date: object,
    model_domain: str | None = None,
    model_spec_id: str | None = None,
) -> dict[str, Any] | None:
    where_clauses = [
        "AND horizon = ?",
        "AND model_version = ?",
        "AND train_end_date <= ?",
        "AND status = 'success'",
    ]
    parameters: list[Any] = [horizon, model_version, train_end_date]
    if model_domain is not None:
        where_clauses.append("AND COALESCE(model_domain, ?) = ?")
        parameters.extend([model_domain, model_domain])
    if model_spec_id is not None:
        where_clauses.append("AND COALESCE(model_spec_id, ?) = ?")
        parameters.extend([model_spec_id, model_spec_id])
    query = (
        _training_run_select_sql(extra_where="\n        ".join(where_clauses))
        + "\n        ORDER BY train_end_date DESC, created_at DESC\n        LIMIT 1\n    "
    )
    row = connection.execute(query, parameters).fetchone()
    if row is None:
        return None
    return _deserialize_training_run_row(row)


def load_training_run_by_id(
    connection: duckdb.DuckDBPyConnection,
    *,
    training_run_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        _training_run_select_sql(extra_where="AND training_run_id = ?") + "\n        LIMIT 1\n    ",
        [training_run_id],
    ).fetchone()
    if row is None:
        return None
    return _deserialize_training_run_row(row)


def load_active_alpha_model(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date: object,
    horizon: int,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT
            active_alpha_model_id,
            horizon,
            model_spec_id,
            training_run_id,
            model_version,
            source_type,
            promotion_type,
            promotion_report_json,
            effective_from_date,
            effective_to_date,
            active_flag,
            rollback_of_active_alpha_model_id,
            note,
            created_at,
            updated_at
        FROM fact_alpha_active_model
        WHERE horizon = ?
          AND effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
          AND active_flag = TRUE
        ORDER BY effective_from_date DESC, created_at DESC
        LIMIT 1
        """,
        [horizon, as_of_date, as_of_date],
    ).fetchone()
    if row is None:
        return None
    keys = [
        "active_alpha_model_id",
        "horizon",
        "model_spec_id",
        "training_run_id",
        "model_version",
        "source_type",
        "promotion_type",
        "promotion_report_json",
        "effective_from_date",
        "effective_to_date",
        "active_flag",
        "rollback_of_active_alpha_model_id",
        "note",
        "created_at",
        "updated_at",
    ]
    payload = dict(zip(keys, row, strict=True))
    return _decode_json_columns(payload, ("promotion_report_json",))
