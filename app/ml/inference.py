from __future__ import annotations

import gc
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from app.common.artifacts import resolve_artifact_path
from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.feature_store import (
    build_feature_store,
    feature_snapshot_has_required_quality_features,
    load_feature_matrix,
)
from app.ml.constants import (
    MODEL_DOMAIN,
    MODEL_SPEC_ID,
    MODEL_VERSION,
    PREDICTION_VERSION,
    SELECTION_ENGINE_VERSION,
)
from app.ml.dataset import TRAINING_FEATURE_COLUMNS, augment_market_regime_features
from app.ml.registry import (
    load_active_alpha_model,
    load_latest_training_run,
    load_model_artifact,
    load_training_run_by_id,
    upsert_model_member_predictions,
)
from app.selection.calibration import PREDICTION_VERSION as PROXY_PREDICTION_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet


@dataclass(slots=True)
class AlphaPredictionMaterializationResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str
    prediction_version: str


PREDICTION_OUTPUT_COLUMNS: tuple[str, ...] = (
    "run_id",
    "as_of_date",
    "symbol",
    "horizon",
    "market",
    "ranking_version",
    "prediction_version",
    "expected_excess_return",
    "lower_band",
    "median_band",
    "upper_band",
    "calibration_start_date",
    "calibration_end_date",
    "calibration_bucket",
    "calibration_sample_size",
    "model_version",
    "training_run_id",
    "model_spec_id",
    "active_alpha_model_id",
    "uncertainty_score",
    "disagreement_score",
    "fallback_flag",
    "fallback_reason",
    "member_count",
    "ensemble_weight_json",
    "source_notes_json",
    "created_at",
)

D1_LEAD_PREDICTION_SQUASH_SCALE = 0.03
D1_LEAD_PREDICTION_SQUASH_AMPLITUDE = 0.03
D1_LEAD_EXPECTED_RETURN_CAP = 0.05


def _apply_d1_lead_prediction_shape_control(
    values: pd.Series | np.ndarray,
) -> pd.Series:
    working = pd.Series(values, copy=True, dtype="float64")
    squashed = D1_LEAD_PREDICTION_SQUASH_AMPLITUDE * np.tanh(
        working / D1_LEAD_PREDICTION_SQUASH_SCALE
    )
    return pd.Series(
        np.clip(
            squashed.to_numpy(dtype="float64", copy=False),
            -D1_LEAD_EXPECTED_RETURN_CAP,
            D1_LEAD_EXPECTED_RETURN_CAP,
        ),
        index=working.index,
        dtype="float64",
    )


def _normalise_prediction_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.reindex(columns=PREDICTION_OUTPUT_COLUMNS).copy()
    float_columns = (
        "expected_excess_return",
        "lower_band",
        "median_band",
        "upper_band",
        "uncertainty_score",
        "disagreement_score",
    )
    integer_columns = ("horizon", "calibration_sample_size", "member_count")
    string_columns = (
        "run_id",
        "symbol",
        "market",
        "ranking_version",
        "prediction_version",
        "calibration_bucket",
        "model_version",
        "training_run_id",
        "model_spec_id",
        "active_alpha_model_id",
        "fallback_reason",
        "ensemble_weight_json",
        "source_notes_json",
    )
    for column in float_columns:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    for column in integer_columns:
        working[column] = pd.to_numeric(working[column], errors="coerce").astype("Int64")
    working["fallback_flag"] = working["fallback_flag"].astype("boolean")
    for column in string_columns:
        working[column] = working[column].where(working[column].notna(), None)
    return working


def _prepare_prediction_frame_for_parquet(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    working = frame.copy()
    for column in ("as_of_date", "calibration_start_date", "calibration_end_date", "created_at"):
        if column in working.columns:
            working[column] = pd.to_datetime(working[column], errors="coerce")
    return working


def upsert_predictions(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    stage = frame.copy()
    for column in ("training_run_id", "model_spec_id", "active_alpha_model_id"):
        if column not in stage.columns:
            stage[column] = None
    connection.register("alpha_prediction_stage", stage)
    connection.execute(
        """
        DELETE FROM fact_prediction
        WHERE (as_of_date, symbol, horizon, prediction_version) IN (
            SELECT as_of_date, symbol, horizon, prediction_version
            FROM alpha_prediction_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_prediction (
            run_id,
            as_of_date,
            symbol,
            horizon,
            market,
            ranking_version,
            prediction_version,
            expected_excess_return,
            lower_band,
            median_band,
            upper_band,
            calibration_start_date,
            calibration_end_date,
            calibration_bucket,
            calibration_sample_size,
            model_version,
            training_run_id,
            model_spec_id,
            active_alpha_model_id,
            uncertainty_score,
            disagreement_score,
            fallback_flag,
            fallback_reason,
            member_count,
            ensemble_weight_json,
            source_notes_json,
            created_at
        )
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            market,
            ranking_version,
            prediction_version,
            expected_excess_return,
            lower_band,
            median_band,
            upper_band,
            calibration_start_date,
            calibration_end_date,
            calibration_bucket,
            calibration_sample_size,
            model_version,
            training_run_id,
            model_spec_id,
            active_alpha_model_id,
            uncertainty_score,
            disagreement_score,
            fallback_flag,
            fallback_reason,
            member_count,
            ensemble_weight_json,
            source_notes_json,
            created_at
        FROM alpha_prediction_stage
        """
    )
    connection.unregister("alpha_prediction_stage")


def _ensure_feature_snapshot(settings: Settings, *, as_of_date: date) -> None:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_feature_snapshot
            WHERE as_of_date = ?
            """,
            [as_of_date],
        ).fetchone()
        has_required_quality_features = feature_snapshot_has_required_quality_features(
            connection,
            as_of_date=as_of_date,
        )
    if row is None or int(row[0] or 0) == 0:
        build_feature_store(settings, as_of_date=as_of_date, cutoff_time="17:30")
    elif not has_required_quality_features:
        build_feature_store(settings, as_of_date=as_of_date, cutoff_time="17:30", force=True)


def _bucket_from_calibration(
    calibration_rows: list[dict[str, Any]], predicted_value: float
) -> dict[str, Any]:
    if not calibration_rows:
        return {
            "bucket": "missing",
            "prediction_lower": None,
            "prediction_upper": None,
            "residual_q25": 0.0,
            "residual_median": 0.0,
            "residual_q75": 0.0,
            "expected_abs_error": 0.0,
            "sample_count": 0,
        }
    non_global = [row for row in calibration_rows if row.get("bucket") != "global"]
    if not non_global:
        return calibration_rows[0]
    ordered = sorted(non_global, key=lambda row: float(row.get("prediction_lower") or -1e18))
    for row in ordered:
        lower = row.get("prediction_lower")
        upper = row.get("prediction_upper")
        lower_ok = lower is None or predicted_value >= float(lower)
        upper_ok = upper is None or predicted_value <= float(upper)
        if lower_ok and upper_ok:
            return row
    if predicted_value < float(ordered[0].get("prediction_lower") or 0):
        return ordered[0]
    return ordered[-1]


def _prediction_from_proxy(
    connection,
    *,
    run_id: str,
    as_of_date: date,
    horizon: int,
) -> pd.DataFrame:
    frame = connection.execute(
        """
        SELECT
            ? AS run_id,
            proxy.as_of_date,
            proxy.symbol,
            proxy.horizon,
            proxy.market,
            ? AS ranking_version,
            ? AS prediction_version,
            proxy.expected_excess_return,
            proxy.lower_band,
            proxy.median_band,
            proxy.upper_band,
            proxy.calibration_start_date,
            proxy.calibration_end_date,
            proxy.calibration_bucket,
            proxy.calibration_sample_size,
            ? AS model_version,
            NULL::VARCHAR AS training_run_id,
            NULL::VARCHAR AS model_spec_id,
            NULL::VARCHAR AS active_alpha_model_id,
            NULL::DOUBLE AS uncertainty_score,
            NULL::DOUBLE AS disagreement_score,
            TRUE AS fallback_flag,
            'use_proxy_prediction_band_v1' AS fallback_reason,
            0::BIGINT AS member_count,
            '{}' AS ensemble_weight_json,
            proxy.source_notes_json,
            now() AS created_at
        FROM fact_prediction AS proxy
        WHERE proxy.as_of_date = ?
          AND proxy.horizon = ?
          AND proxy.prediction_version = ?
          AND proxy.ranking_version = 'selection_engine_v1'
        """,
        [
            run_id,
            SELECTION_ENGINE_VERSION,
            PREDICTION_VERSION,
            MODEL_VERSION,
            as_of_date,
            horizon,
            PROXY_PREDICTION_VERSION,
        ],
    ).fetchdf()
    return frame


def _resolve_training_run_for_inference(
    connection,
    *,
    as_of_date: date,
    horizon: int,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    active_model = load_active_alpha_model(
        connection,
        as_of_date=as_of_date,
        horizon=horizon,
    )
    if active_model is not None:
        training_run = load_training_run_by_id(
            connection,
            training_run_id=str(active_model["training_run_id"]),
        )
        if training_run is not None:
            return training_run, active_model, "active_registry"
        active_model = None
    training_run = load_latest_training_run(
        connection,
        horizon=horizon,
        model_version=MODEL_VERSION,
        train_end_date=as_of_date,
        model_domain=MODEL_DOMAIN,
        model_spec_id=MODEL_SPEC_ID,
    )
    if training_run is not None:
        return training_run, active_model, "legacy_latest_successful"
    return None, active_model, "missing"


def build_prediction_frame_from_training_run(
    *,
    run_id: str,
    as_of_date: date,
    horizon: int,
    feature_frame: pd.DataFrame,
    training_run: dict[str, Any],
    training_run_source: str,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    prediction_version: str = PREDICTION_VERSION,
    active_alpha_model_id: str | None = None,
    persist_member_predictions: bool = True,
) -> tuple[pd.DataFrame, list[pd.DataFrame]]:
    if not training_run.get("artifact_uri"):
        return pd.DataFrame(columns=PREDICTION_OUTPUT_COLUMNS), []

    artifact_payload = load_model_artifact(Path(str(training_run["artifact_uri"])))
    feature_columns = list(artifact_payload.get("feature_columns", list(TRAINING_FEATURE_COLUMNS)))
    inference_features = feature_frame.reindex(columns=feature_columns)
    X_inference = inference_features.apply(
        pd.to_numeric,
        errors="coerce",
    )
    base_index = feature_frame.index
    base_symbols = feature_frame["symbol"]
    base_markets = feature_frame["market"]
    member_predictions: dict[str, pd.Series] = {}
    for member_name in artifact_payload.get("member_order", []):
        model = artifact_payload["members"].get(member_name)
        if model is None:
            continue
        member_predictions[member_name] = pd.Series(
            model.predict(X_inference),
            index=base_index,
            dtype="float64",
        )

    if not member_predictions:
        return pd.DataFrame(columns=PREDICTION_OUTPUT_COLUMNS), []

    ensemble_weights = {
        str(key): float(value)
        for key, value in artifact_payload.get("ensemble_weights", {}).items()
        if key in member_predictions
    }
    if not ensemble_weights:
        equal_weight = 1.0 / len(member_predictions)
        ensemble_weights = {member_name: equal_weight for member_name in member_predictions}
    ensemble_prediction = sum(
        member_predictions[member_name] * weight
        for member_name, weight in ensemble_weights.items()
    )
    if len(member_predictions) == 1:
        disagreement_raw = pd.Series(0.0, index=base_index, dtype="float64")
    else:
        sum_values = np.zeros(len(base_index), dtype="float64")
        sum_squares = np.zeros(len(base_index), dtype="float64")
        for member_prediction in member_predictions.values():
            values = member_prediction.to_numpy(dtype="float64", copy=False)
            sum_values += values
            sum_squares += values * values
        mean_values = sum_values / float(len(member_predictions))
        variance = np.maximum(
            (sum_squares / float(len(member_predictions))) - np.square(mean_values),
            0.0,
        )
        disagreement_raw = pd.Series(np.sqrt(variance), index=base_index, dtype="float64")
    disagreement_score = disagreement_raw.rank(pct=True).mul(100.0)
    model_spec_id = (
        training_run.get("model_spec_id")
        or artifact_payload.get("model_spec_id")
        or MODEL_SPEC_ID
    )
    calibration_rows = artifact_payload.get("calibration", [])
    raw_expected_excess_return = pd.Series(
        ensemble_prediction,
        index=base_index,
        dtype="float64",
    )
    expected_excess_return = raw_expected_excess_return.copy()
    if str(model_spec_id) == "alpha_lead_d1_v1" and int(horizon) == 1:
        expected_excess_return = _apply_d1_lead_prediction_shape_control(
            raw_expected_excess_return
        )
    bucket_records = [
        _bucket_from_calibration(calibration_rows, float(value))
        for value in raw_expected_excess_return
    ]
    band_frame = pd.DataFrame(bucket_records)
    residual_q25 = pd.to_numeric(band_frame["residual_q25"], errors="coerce").fillna(0.0)
    residual_median = pd.to_numeric(band_frame["residual_median"], errors="coerce").fillna(0.0)
    residual_q75 = pd.to_numeric(band_frame["residual_q75"], errors="coerce").fillna(0.0)
    expected_abs_error = pd.to_numeric(
        band_frame["expected_abs_error"], errors="coerce"
    ).fillna(0.0)
    lower_band = pd.Series(
        expected_excess_return + residual_q25,
        index=base_index,
        dtype="float64",
    )
    median_band = pd.Series(
        expected_excess_return + residual_median,
        index=base_index,
        dtype="float64",
    )
    upper_band = pd.Series(
        expected_excess_return + residual_q75,
        index=base_index,
        dtype="float64",
    )
    uncertainty_raw = expected_abs_error
    result_frame = pd.DataFrame(
        {
            "run_id": run_id,
            "as_of_date": as_of_date,
            "symbol": base_symbols,
            "horizon": int(horizon),
            "market": base_markets,
            "ranking_version": ranking_version,
            "prediction_version": prediction_version,
            "expected_excess_return": expected_excess_return,
            "lower_band": lower_band,
            "median_band": median_band,
            "upper_band": upper_band,
            "calibration_start_date": training_run.get("validation_window_start"),
            "calibration_end_date": training_run.get("validation_window_end"),
            "calibration_bucket": band_frame["bucket"],
            "calibration_sample_size": band_frame["sample_count"],
            "model_version": MODEL_VERSION,
            "training_run_id": training_run["training_run_id"],
            "model_spec_id": model_spec_id,
            "active_alpha_model_id": active_alpha_model_id,
            "uncertainty_score": uncertainty_raw,
            "disagreement_score": disagreement_score,
            "fallback_flag": bool(artifact_payload.get("fallback_flag", False)),
            "fallback_reason": artifact_payload.get("fallback_reason"),
            "member_count": len(member_predictions),
            "ensemble_weight_json": json.dumps(
                ensemble_weights,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "source_notes_json": [
                json.dumps(
                    {
                        "training_run_id": training_run["training_run_id"],
                        "artifact_uri": training_run["artifact_uri"],
                        "model_spec_id": model_spec_id,
                        "active_alpha_model_id": active_alpha_model_id,
                        "training_run_source": training_run_source,
                        "fallback_flag": bool(artifact_payload.get("fallback_flag", False)),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            ]
            * len(feature_frame),
            "created_at": pd.Timestamp.utcnow(),
        }
    )
    if result_frame["uncertainty_score"].notna().any():
        result_frame["uncertainty_score"] = (
            result_frame["uncertainty_score"].rank(pct=True).mul(100.0)
        )

    member_prediction_frames: list[pd.DataFrame] = []
    if persist_member_predictions:
        prediction_created_at = pd.Timestamp.utcnow()
        for member_name, values in member_predictions.items():
            member_prediction_frames.append(
                pd.DataFrame(
                    {
                        "training_run_id": training_run["training_run_id"],
                        "as_of_date": as_of_date,
                        "symbol": base_symbols,
                        "horizon": int(horizon),
                        "model_version": MODEL_VERSION,
                        "prediction_role": "inference",
                        "member_name": member_name,
                        "predicted_excess_return": values,
                        "actual_excess_return": pd.NA,
                        "residual": pd.NA,
                        "fallback_flag": bool(artifact_payload.get("fallback_flag", False)),
                        "fallback_reason": artifact_payload.get("fallback_reason"),
                        "created_at": prediction_created_at,
                    }
                )
            )
        member_prediction_frames.append(
            pd.DataFrame(
                {
                    "training_run_id": training_run["training_run_id"],
                    "as_of_date": as_of_date,
                    "symbol": base_symbols,
                    "horizon": int(horizon),
                    "model_version": MODEL_VERSION,
                    "prediction_role": "inference",
                    "member_name": "ensemble",
                    "predicted_excess_return": expected_excess_return,
                    "actual_excess_return": pd.NA,
                    "residual": pd.NA,
                    "fallback_flag": bool(artifact_payload.get("fallback_flag", False)),
                    "fallback_reason": artifact_payload.get("fallback_reason"),
                    "created_at": prediction_created_at,
                }
            )
        )

    del inference_features, X_inference, artifact_payload
    return _normalise_prediction_frame(result_frame), member_prediction_frames


def materialize_alpha_predictions_v1(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
) -> AlphaPredictionMaterializationResult:
    ensure_storage_layout(settings)
    _ensure_feature_snapshot(settings, as_of_date=as_of_date)

    with activate_run_context(
        "materialize_alpha_predictions_v1", as_of_date=as_of_date
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
                    "fact_market_regime_snapshot",
                    "fact_model_training_run",
                    "fact_alpha_active_model",
                ],
                notes=(
                    "Materialize alpha model v1 predictions. "
                    f"as_of_date={as_of_date.isoformat()} horizons={horizons}"
                ),
            )
            try:
                feature_frame = load_feature_matrix(
                    connection,
                    as_of_date=as_of_date,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                    include_rank_features=False,
                    include_zscore_features=False,
                )
                feature_frame = augment_market_regime_features(connection, feature_frame)
                if feature_frame.empty:
                    raise RuntimeError(
                        "Feature matrix is missing for alpha inference. "
                        "Run scripts/build_feature_store.py first."
                    )

                prediction_frames: list[pd.DataFrame] = []
                member_prediction_frames: list[pd.DataFrame] = []
                artifact_paths: list[str] = []
                for horizon in horizons:
                    (
                        selected_training_run,
                        active_alpha_model,
                        training_run_source,
                    ) = _resolve_training_run_for_inference(
                        connection,
                        as_of_date=as_of_date,
                        horizon=int(horizon),
                    )
                    if selected_training_run is None:
                        proxy_frame = _prediction_from_proxy(
                            connection,
                            run_id=run_context.run_id,
                            as_of_date=as_of_date,
                            horizon=int(horizon),
                        )
                        if not proxy_frame.empty:
                            prediction_frames.append(_normalise_prediction_frame(proxy_frame))
                            continue

                    if selected_training_run is None or not selected_training_run.get(
                        "artifact_uri"
                    ):
                        fallback_frame = feature_frame[["as_of_date", "symbol", "market"]].copy()
                        fallback_frame["run_id"] = run_context.run_id
                        fallback_frame["horizon"] = int(horizon)
                        fallback_frame["ranking_version"] = SELECTION_ENGINE_VERSION
                        fallback_frame["prediction_version"] = PREDICTION_VERSION
                        fallback_frame["expected_excess_return"] = pd.NA
                        fallback_frame["lower_band"] = pd.NA
                        fallback_frame["median_band"] = pd.NA
                        fallback_frame["upper_band"] = pd.NA
                        fallback_frame["calibration_start_date"] = pd.NA
                        fallback_frame["calibration_end_date"] = pd.NA
                        fallback_frame["calibration_bucket"] = pd.NA
                        fallback_frame["calibration_sample_size"] = pd.NA
                        fallback_frame["model_version"] = MODEL_VERSION
                        fallback_frame["training_run_id"] = pd.NA
                        fallback_frame["model_spec_id"] = pd.NA
                        fallback_frame["active_alpha_model_id"] = pd.NA
                        fallback_frame["uncertainty_score"] = pd.NA
                        fallback_frame["disagreement_score"] = pd.NA
                        fallback_frame["fallback_flag"] = True
                        fallback_frame["fallback_reason"] = "no_model_or_proxy_available"
                        fallback_frame["member_count"] = 0
                        fallback_frame["ensemble_weight_json"] = "{}"
                        fallback_frame["source_notes_json"] = json.dumps(
                            {
                                "note": (
                                    "No trained alpha artifact or proxy prediction was available."
                                )
                            },
                            ensure_ascii=False,
                        )
                        fallback_frame["created_at"] = pd.Timestamp.utcnow()
                        prediction_frames.append(
                            _normalise_prediction_frame(
                                fallback_frame[
                                    [
                                        "run_id",
                                        "as_of_date",
                                        "symbol",
                                        "horizon",
                                        "market",
                                        "ranking_version",
                                        "prediction_version",
                                        "expected_excess_return",
                                        "lower_band",
                                        "median_band",
                                        "upper_band",
                                        "calibration_start_date",
                                        "calibration_end_date",
                                        "calibration_bucket",
                                        "calibration_sample_size",
                                        "model_version",
                                        "training_run_id",
                                        "model_spec_id",
                                        "active_alpha_model_id",
                                        "uncertainty_score",
                                        "disagreement_score",
                                        "fallback_flag",
                                        "fallback_reason",
                                        "member_count",
                                        "ensemble_weight_json",
                                        "source_notes_json",
                                        "created_at",
                                    ]
                                ].copy()
                            )
                        )
                        continue

                    resolved_artifact_path = resolve_artifact_path(
                        settings,
                        selected_training_run.get("artifact_uri"),
                    )
                    if resolved_artifact_path is None:
                        proxy_frame = _prediction_from_proxy(
                            connection,
                            run_id=run_context.run_id,
                            as_of_date=as_of_date,
                            horizon=int(horizon),
                        )
                        if not proxy_frame.empty:
                            prediction_frames.append(_normalise_prediction_frame(proxy_frame))
                        continue
                    resolved_training_run = dict(selected_training_run)
                    resolved_training_run["artifact_uri"] = str(resolved_artifact_path)

                    result_frame, member_frames = build_prediction_frame_from_training_run(
                        run_id=run_context.run_id,
                        as_of_date=as_of_date,
                        horizon=int(horizon),
                        feature_frame=feature_frame,
                        training_run=resolved_training_run,
                        training_run_source=training_run_source,
                        active_alpha_model_id=(
                            active_alpha_model.get("active_alpha_model_id")
                            if active_alpha_model is not None
                            else None
                        ),
                    )
                    if result_frame.empty:
                        proxy_frame = _prediction_from_proxy(
                            connection,
                            run_id=run_context.run_id,
                            as_of_date=as_of_date,
                            horizon=int(horizon),
                        )
                        if not proxy_frame.empty:
                            prediction_frames.append(_normalise_prediction_frame(proxy_frame))
                        continue
                    prediction_frames.append(result_frame)
                    member_prediction_frames.extend(member_frames)
                    del result_frame, member_frames, resolved_training_run
                    gc.collect()

                non_empty_prediction_frames = [
                    frame for frame in prediction_frames if not frame.empty
                ]
                combined_predictions = (
                    pd.concat(non_empty_prediction_frames, ignore_index=True)
                    if non_empty_prediction_frames
                    else pd.DataFrame(columns=PREDICTION_OUTPUT_COLUMNS)
                )
                upsert_predictions(connection, combined_predictions)
                if member_prediction_frames:
                    upsert_model_member_predictions(
                        connection,
                        pd.concat(member_prediction_frames, ignore_index=True),
                    )

                if not combined_predictions.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                _prepare_prediction_frame_for_parquet(combined_predictions),
                                base_dir=settings.paths.curated_dir,
                                dataset="prediction",
                                partitions={
                                    "as_of_date": as_of_date.isoformat(),
                                    "prediction_version": PREDICTION_VERSION,
                                },
                                filename="alpha_prediction_v1.parquet",
                            )
                        )
                    )
                notes = (
                    "Alpha predictions materialized. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(combined_predictions)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=MODEL_VERSION,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return AlphaPredictionMaterializationResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(combined_predictions),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    prediction_version=PREDICTION_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Alpha prediction materialization failed.",
                    error_message=str(exc),
                    model_version=MODEL_VERSION,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
