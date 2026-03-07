from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION, materialize_selection_engine_v1
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

PREDICTION_VERSION = "proxy_prediction_band_v1"


@dataclass(slots=True)
class ProxyPredictionCalibrationResult:
    run_id: str
    as_of_date: date
    row_count: int
    calibration_row_count: int
    artifact_paths: list[str]
    notes: str
    prediction_version: str


def _bucketize(rank_pct: pd.Series) -> pd.Series:
    return (
        (
            pd.to_numeric(rank_pct, errors="coerce")
            .clip(lower=0.0, upper=0.9999)
            .mul(10)
            .fillna(0)
            .astype(int)
        )
        + 1
    ).map(lambda value: f"decile_{value:02d}")


def upsert_predictions(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("prediction_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_prediction
        WHERE (as_of_date, symbol, horizon, prediction_version) IN (
            SELECT as_of_date, symbol, horizon, prediction_version
            FROM prediction_stage
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
            disagreement_score,
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
            disagreement_score,
            source_notes_json,
            created_at
        FROM prediction_stage
        """
    )
    connection.unregister("prediction_stage")


def _ensure_selection_history(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
) -> list[date]:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        candidate_rows = connection.execute(
            """
            SELECT DISTINCT feature.as_of_date
            FROM fact_feature_snapshot AS feature
            JOIN fact_market_regime_snapshot AS regime
              ON feature.as_of_date = regime.as_of_date
             AND regime.market_scope = 'KR_ALL'
            WHERE feature.as_of_date BETWEEN ? AND ?
            ORDER BY feature.as_of_date
            """,
            [start_date, end_date],
        ).fetchall()
        missing_dates: list[date] = []
        expected_horizon_count = len(horizons)
        for row in candidate_rows:
            as_of_date = pd.Timestamp(row[0]).date()
            existing_count = connection.execute(
                """
                SELECT COUNT(DISTINCT horizon)
                FROM fact_ranking
                WHERE as_of_date = ?
                  AND ranking_version = ?
                """,
                [as_of_date, SELECTION_ENGINE_VERSION],
            ).fetchone()[0]
            if int(existing_count or 0) < expected_horizon_count:
                missing_dates.append(as_of_date)

    for as_of_date in missing_dates:
        materialize_selection_engine_v1(
            settings,
            as_of_date=as_of_date,
            horizons=horizons,
        )
    return missing_dates


def calibrate_proxy_prediction_bands(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
) -> ProxyPredictionCalibrationResult:
    ensure_storage_layout(settings)
    auto_built_dates = _ensure_selection_history(
        settings,
        start_date=start_date,
        end_date=end_date,
        horizons=horizons,
    )

    with activate_run_context(
        "calibrate_proxy_prediction_bands",
        as_of_date=end_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_ranking", "fact_forward_return_label", "dim_symbol"],
                notes=(
                    "Calibrate proxy prediction bands from selection engine history. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()} horizons={horizons}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                horizon_placeholders = ",".join("?" for _ in horizons)
                historical = connection.execute(
                    f"""
                    SELECT
                        ranking.as_of_date,
                        ranking.symbol,
                        ranking.horizon,
                        symbol.market,
                        ranking.final_selection_rank_pct,
                        label.excess_forward_return
                    FROM fact_ranking AS ranking
                    JOIN fact_forward_return_label AS label
                      ON ranking.as_of_date = label.as_of_date
                     AND ranking.symbol = label.symbol
                     AND ranking.horizon = label.horizon
                    JOIN dim_symbol AS symbol
                      ON ranking.symbol = symbol.symbol
                    WHERE ranking.as_of_date BETWEEN ? AND ?
                      AND ranking.horizon IN ({horizon_placeholders})
                      AND ranking.ranking_version = ?
                      AND label.label_available_flag
                    """,
                    [start_date, end_date, *horizons, SELECTION_ENGINE_VERSION],
                ).fetchdf()
                if historical.empty:
                    raise RuntimeError(
                        "No overlapping selection-engine rows and forward labels were available "
                        "for proxy calibration."
                    )

                historical["calibration_bucket"] = _bucketize(
                    historical["final_selection_rank_pct"]
                )
                market_calibration = (
                    historical.groupby(["horizon", "market", "calibration_bucket"], as_index=False)
                    .agg(
                        calibration_sample_size=("symbol", "count"),
                        expected_excess_return=("excess_forward_return", "mean"),
                        lower_band=("excess_forward_return", lambda values: values.quantile(0.25)),
                        median_band=("excess_forward_return", "median"),
                        upper_band=("excess_forward_return", lambda values: values.quantile(0.75)),
                    )
                )
                global_calibration = (
                    historical.groupby(["horizon", "calibration_bucket"], as_index=False)
                    .agg(
                        calibration_sample_size=("symbol", "count"),
                        expected_excess_return=("excess_forward_return", "mean"),
                        lower_band=("excess_forward_return", lambda values: values.quantile(0.25)),
                        median_band=("excess_forward_return", "median"),
                        upper_band=("excess_forward_return", lambda values: values.quantile(0.75)),
                    )
                    .assign(market="KR_ALL")
                )
                calibration_frame = pd.concat(
                    [market_calibration, global_calibration],
                    ignore_index=True,
                )

                latest_selection_date = connection.execute(
                    """
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = ?
                      AND as_of_date <= ?
                    """,
                    [SELECTION_ENGINE_VERSION, end_date],
                ).fetchone()[0]
                if latest_selection_date is None:
                    raise RuntimeError(
                        "No selection engine rows exist at or before the calibration end date."
                    )

                current_selection = connection.execute(
                    f"""
                    SELECT
                        ranking.as_of_date,
                        ranking.symbol,
                        ranking.horizon,
                        ranking.final_selection_rank_pct,
                        symbol.market
                    FROM fact_ranking AS ranking
                    JOIN dim_symbol AS symbol
                      ON ranking.symbol = symbol.symbol
                    WHERE ranking.as_of_date = ?
                      AND ranking.horizon IN ({horizon_placeholders})
                      AND ranking.ranking_version = ?
                    """,
                    [latest_selection_date, *horizons, SELECTION_ENGINE_VERSION],
                ).fetchdf()
                current_selection["calibration_bucket"] = _bucketize(
                    current_selection["final_selection_rank_pct"]
                )

                merged = current_selection.merge(
                    market_calibration,
                    on=["horizon", "market", "calibration_bucket"],
                    how="left",
                )
                missing_mask = merged["expected_excess_return"].isna()
                if missing_mask.any():
                    merged.loc[missing_mask, [
                        "calibration_sample_size",
                        "expected_excess_return",
                        "lower_band",
                        "median_band",
                        "upper_band",
                    ]] = (
                        merged.loc[missing_mask, ["horizon", "calibration_bucket"]]
                        .merge(
                            global_calibration.drop(columns=["market"]),
                            on=["horizon", "calibration_bucket"],
                            how="left",
                        )[
                            [
                                "calibration_sample_size",
                                "expected_excess_return",
                                "lower_band",
                                "median_band",
                                "upper_band",
                            ]
                        ]
                        .to_numpy()
                    )

                predictions = merged.copy()
                predictions["run_id"] = run_context.run_id
                predictions["ranking_version"] = SELECTION_ENGINE_VERSION
                predictions["prediction_version"] = PREDICTION_VERSION
                predictions["calibration_start_date"] = start_date
                predictions["calibration_end_date"] = end_date
                predictions["disagreement_score"] = pd.NA
                predictions["source_notes_json"] = predictions.apply(
                    lambda row: json.dumps(
                        {
                            "calibration_bucket": row["calibration_bucket"],
                            "proxy_prediction": True,
                            "note": "Bands are calibrated historical proxies, not ML forecasts.",
                        },
                        ensure_ascii=False,
                    ),
                    axis=1,
                )
                predictions["created_at"] = pd.Timestamp.utcnow()
                predictions = predictions[
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
                        "disagreement_score",
                        "source_notes_json",
                        "created_at",
                    ]
                ].copy()
                upsert_predictions(connection, predictions)

                artifact_paths = [
                    str(
                        write_parquet(
                            calibration_frame,
                            base_dir=settings.paths.artifacts_dir,
                            dataset="calibration/proxy_prediction",
                            partitions={
                                "start_date": start_date.isoformat(),
                                "end_date": end_date.isoformat(),
                            },
                            filename="calibration_summary.parquet",
                        )
                    ),
                    str(
                        write_parquet(
                            predictions,
                            base_dir=settings.paths.curated_dir,
                            dataset="prediction",
                            partitions={"as_of_date": str(latest_selection_date)},
                            filename="proxy_prediction_band.parquet",
                        )
                    ),
                ]

                notes = (
                    "Proxy prediction calibration completed. "
                    f"selection_date={latest_selection_date}, predictions={len(predictions)}, "
                    f"calibration_rows={len(calibration_frame)}, "
                    f"auto_built_selection_dates={len(auto_built_dates)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=PREDICTION_VERSION,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return ProxyPredictionCalibrationResult(
                    run_id=run_context.run_id,
                    as_of_date=pd.Timestamp(latest_selection_date).date(),
                    row_count=len(predictions),
                    calibration_row_count=len(calibration_frame),
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
                    notes=(
                        "Proxy prediction calibration failed. "
                        f"range={start_date.isoformat()}..{end_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    model_version=PREDICTION_VERSION,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
