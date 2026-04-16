from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.artifacts import resolve_artifact_path
from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.feature_store import build_feature_store, load_feature_matrix
from app.ml.constants import DEFAULT_ALPHA_MODEL_SPEC, MODEL_DOMAIN, MODEL_VERSION
from app.ml.inference import build_prediction_frame_from_training_run
from app.ml.registry import load_alpha_model_specs, load_latest_training_run
from app.selection.engine_v2 import build_selection_engine_v2_rankings
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

SHADOW_PREDICTION_COLUMNS: tuple[str, ...] = (
    "run_id",
    "selection_date",
    "symbol",
    "horizon",
    "model_spec_id",
    "training_run_id",
    "expected_excess_return",
    "lower_band",
    "median_band",
    "upper_band",
    "uncertainty_score",
    "disagreement_score",
    "fallback_flag",
    "fallback_reason",
    "created_at",
)

SHADOW_RANKING_OUTPUT_COLUMNS: tuple[str, ...] = (
    "run_id",
    "as_of_date",
    "symbol",
    "horizon",
    "model_spec_id",
    "training_run_id",
    "final_selection_value",
    "final_selection_rank_pct",
    "grade",
    "report_candidate_flag",
    "eligible_flag",
    "created_at",
)


@dataclass(slots=True)
class AlphaShadowMaterializationResult:
    run_id: str
    as_of_date: date
    prediction_row_count: int
    ranking_row_count: int
    artifact_paths: list[str]
    notes: str


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
    if row is None or int(row[0] or 0) == 0:
        build_feature_store(settings, as_of_date=as_of_date, cutoff_time="17:30")


def _load_regime_map(connection, *, as_of_date: date) -> dict[str, dict[str, object]]:
    frame = connection.execute(
        """
        SELECT market_scope, regime_state, regime_score
        FROM fact_market_regime_snapshot
        WHERE as_of_date = ?
        """,
        [as_of_date],
    ).fetchdf()
    if frame.empty:
        raise RuntimeError(
            "Market regime snapshot is missing for alpha shadow materialization. "
            "Run scripts/build_market_regime_snapshot.py first."
        )
    return {
        str(row.market_scope): {
            "regime_state": row.regime_state,
            "regime_score": row.regime_score,
        }
        for row in frame.itertuples(index=False)
    }


def _load_candidate_specs(connection) -> list[dict[str, object]]:
    specs = load_alpha_model_specs(
        connection,
        model_domain=MODEL_DOMAIN,
        active_only=True,
    )
    if specs:
        return specs
    return [
        {
            "model_spec_id": DEFAULT_ALPHA_MODEL_SPEC.model_spec_id,
            "estimation_scheme": DEFAULT_ALPHA_MODEL_SPEC.estimation_scheme,
            "rolling_window_days": DEFAULT_ALPHA_MODEL_SPEC.rolling_window_days,
        }
    ]


def upsert_alpha_shadow_predictions(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("alpha_shadow_prediction_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_alpha_shadow_prediction
        WHERE (selection_date, symbol, horizon, model_spec_id) IN (
            SELECT selection_date, symbol, horizon, model_spec_id
            FROM alpha_shadow_prediction_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_alpha_shadow_prediction (
            run_id,
            selection_date,
            symbol,
            horizon,
            model_spec_id,
            training_run_id,
            expected_excess_return,
            lower_band,
            median_band,
            upper_band,
            uncertainty_score,
            disagreement_score,
            fallback_flag,
            fallback_reason,
            created_at
        )
        SELECT
            run_id,
            selection_date,
            symbol,
            horizon,
            model_spec_id,
            training_run_id,
            expected_excess_return,
            lower_band,
            median_band,
            upper_band,
            uncertainty_score,
            disagreement_score,
            fallback_flag,
            fallback_reason,
            created_at
        FROM alpha_shadow_prediction_stage
        """
    )
    connection.unregister("alpha_shadow_prediction_stage")


def upsert_alpha_shadow_rankings(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("alpha_shadow_ranking_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_alpha_shadow_ranking
        WHERE (selection_date, symbol, horizon, model_spec_id) IN (
            SELECT selection_date, symbol, horizon, model_spec_id
            FROM alpha_shadow_ranking_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_alpha_shadow_ranking (
            run_id,
            selection_date,
            symbol,
            horizon,
            model_spec_id,
            training_run_id,
            final_selection_value,
            selection_percentile,
            grade,
            report_candidate_flag,
            eligible_flag,
            created_at
        )
        SELECT
            run_id,
            selection_date,
            symbol,
            horizon,
            model_spec_id,
            training_run_id,
            final_selection_value,
            selection_percentile,
            grade,
            report_candidate_flag,
            eligible_flag,
            created_at
        FROM alpha_shadow_ranking_stage
        """
    )
    connection.unregister("alpha_shadow_ranking_stage")


def materialize_alpha_shadow_candidates(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
) -> AlphaShadowMaterializationResult:
    ensure_storage_layout(settings)
    _ensure_feature_snapshot(settings, as_of_date=as_of_date)

    with activate_run_context(
        "materialize_alpha_shadow_candidates",
        as_of_date=as_of_date,
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
                    "dim_alpha_model_spec",
                ],
                notes=(
                    "Materialize alpha shadow candidate predictions and rankings. "
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
                )
                if feature_frame.empty:
                    raise RuntimeError(
                        "Feature matrix is missing for alpha shadow materialization."
                    )

                regime_map = _load_regime_map(connection, as_of_date=as_of_date)
                prediction_frames_by_horizon: dict[int, list[pd.DataFrame]] = {
                    int(horizon): [] for horizon in horizons
                }
                spec_count = 0
                for spec in _load_candidate_specs(connection):
                    model_spec_id = str(spec["model_spec_id"])
                    spec_count += 1
                    for horizon in horizons:
                        training_run = load_latest_training_run(
                            connection,
                            horizon=int(horizon),
                            model_version=str(spec.get("model_version") or MODEL_VERSION),
                            train_end_date=as_of_date,
                            model_domain=MODEL_DOMAIN,
                            model_spec_id=model_spec_id,
                        )
                        if training_run is None or not training_run.get("artifact_uri"):
                            continue
                        resolved_artifact_path = resolve_artifact_path(
                            settings,
                            training_run.get("artifact_uri"),
                        )
                        if resolved_artifact_path is None:
                            continue
                        resolved_training_run = dict(training_run)
                        resolved_training_run["artifact_uri"] = str(resolved_artifact_path)
                        prediction_frame, _ = build_prediction_frame_from_training_run(
                            run_id=run_context.run_id,
                            as_of_date=as_of_date,
                            horizon=int(horizon),
                            feature_frame=feature_frame,
                            training_run=resolved_training_run,
                            training_run_source="shadow_candidate",
                            active_alpha_model_id=None,
                            persist_member_predictions=False,
                        )
                        if prediction_frame.empty:
                            continue
                        prediction_frames_by_horizon[int(horizon)].append(prediction_frame)

                combined_prediction_frames = [
                    pd.concat(frames, ignore_index=True)
                    for frames in prediction_frames_by_horizon.values()
                    if frames
                ]
                combined_predictions = (
                    pd.concat(combined_prediction_frames, ignore_index=True)
                    if combined_prediction_frames
                    else pd.DataFrame(columns=list(SHADOW_PREDICTION_COLUMNS))
                )
                if not combined_predictions.empty:
                    combined_predictions = combined_predictions.rename(
                        columns={"as_of_date": "selection_date"}
                    )
                    combined_predictions = combined_predictions.loc[
                        :, list(SHADOW_PREDICTION_COLUMNS)
                    ].copy()
                upsert_alpha_shadow_predictions(connection, combined_predictions)

                ranking_frames: list[pd.DataFrame] = []
                for horizon, prediction_frames in prediction_frames_by_horizon.items():
                    if not prediction_frames:
                        continue
                    by_horizon = {int(horizon): pd.concat(prediction_frames, ignore_index=True)}
                    ranking_frames.extend(
                        build_selection_engine_v2_rankings(
                            feature_matrix=feature_frame,
                            as_of_date=as_of_date,
                            horizons=[int(horizon)],
                            regime_map=regime_map,
                            prediction_frames_by_horizon=by_horizon,
                            run_id=run_context.run_id,
                            settings=settings,
                            ranking_version="alpha_shadow_selection_engine_v2",
                            output_columns=SHADOW_RANKING_OUTPUT_COLUMNS,
                        )
                    )
                combined_rankings = (
                    pd.concat(ranking_frames, ignore_index=True)
                    if ranking_frames
                    else pd.DataFrame(columns=list(SHADOW_RANKING_OUTPUT_COLUMNS))
                )
                if not combined_rankings.empty:
                    combined_rankings = combined_rankings.rename(
                        columns={
                            "as_of_date": "selection_date",
                            "final_selection_rank_pct": "selection_percentile",
                        }
                    )
                upsert_alpha_shadow_rankings(connection, combined_rankings)

                artifact_paths: list[str] = []
                if not combined_predictions.empty:
                    for (
                        selection_date,
                        horizon,
                        model_spec_id,
                    ), partition_frame in combined_predictions.groupby(
                        ["selection_date", "horizon", "model_spec_id"],
                        sort=True,
                    ):
                        artifact_paths.append(
                            str(
                                write_parquet(
                                    partition_frame,
                                    base_dir=settings.paths.curated_dir,
                                    dataset="alpha_shadow/prediction",
                                    partitions={
                                        "selection_date": pd.Timestamp(selection_date)
                                        .date()
                                        .isoformat(),
                                        "horizon": str(int(horizon)),
                                        "model_spec_id": str(model_spec_id),
                                    },
                                    filename="alpha_shadow_prediction.parquet",
                                )
                            )
                        )
                if not combined_rankings.empty:
                    for (
                        selection_date,
                        horizon,
                        model_spec_id,
                    ), partition_frame in combined_rankings.groupby(
                        ["selection_date", "horizon", "model_spec_id"],
                        sort=True,
                    ):
                        artifact_paths.append(
                            str(
                                write_parquet(
                                    partition_frame,
                                    base_dir=settings.paths.curated_dir,
                                    dataset="alpha_shadow/ranking",
                                    partitions={
                                        "selection_date": pd.Timestamp(selection_date)
                                        .date()
                                        .isoformat(),
                                        "horizon": str(int(horizon)),
                                        "model_spec_id": str(model_spec_id),
                                    },
                                    filename="alpha_shadow_ranking.parquet",
                                )
                            )
                        )

                notes = (
                    "Alpha shadow candidates materialized. "
                    f"as_of_date={as_of_date.isoformat()} "
                    f"prediction_rows={len(combined_predictions)} "
                    f"ranking_rows={len(combined_rankings)} "
                    f"candidate_specs={spec_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return AlphaShadowMaterializationResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    prediction_row_count=len(combined_predictions),
                    ranking_row_count=len(combined_rankings),
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
                    notes="Alpha shadow candidate materialization failed.",
                    error_message=str(exc),
                )
                raise
