from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.constants import FEATURE_NAMES
from app.features.feature_store import build_feature_store
from app.labels.forward_returns import build_forward_labels
from app.ml.constants import MODEL_DATASET_VERSION
from app.pipelines._helpers import load_symbol_frame
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

MARKET_FEATURE_COLUMNS: tuple[str, ...] = ("market_is_kospi", "market_is_kosdaq")
TRAINING_FEATURE_COLUMNS: tuple[str, ...] = FEATURE_NAMES + MARKET_FEATURE_COLUMNS


@dataclass(slots=True)
class ModelTrainingDatasetResult:
    run_id: str
    train_end_date: date
    row_count: int
    date_count: int
    artifact_paths: list[str]
    notes: str
    dataset_version: str


def _resolve_label_start_date(connection, *, train_end_date: date) -> date:
    row = connection.execute(
        """
        SELECT MIN(trading_date)
        FROM fact_daily_ohlcv
        WHERE trading_date <= ?
        """,
        [train_end_date],
    ).fetchone()
    if row is None or row[0] is None:
        return train_end_date
    return pd.Timestamp(row[0]).date()


def _resolve_candidate_dates(
    connection,
    *,
    train_end_date: date,
    horizons: list[int],
    symbols: list[str] | None,
    limit_symbols: int | None,
    market: str,
) -> list[date]:
    symbol_frame = load_symbol_frame(
        connection,
        symbols=symbols,
        market=market,
        limit_symbols=limit_symbols,
        as_of_date=train_end_date,
    )
    if symbol_frame.empty:
        return []
    connection.register("model_training_symbol_stage", symbol_frame[["symbol"]])
    try:
        placeholders = ",".join("?" for _ in horizons)
        rows = connection.execute(
            f"""
            SELECT DISTINCT as_of_date
            FROM fact_forward_return_label
            WHERE as_of_date <= ?
              AND horizon IN ({placeholders})
              AND label_available_flag
              AND symbol IN (SELECT symbol FROM model_training_symbol_stage)
            ORDER BY as_of_date
            """,
            [train_end_date, *horizons],
        ).fetchall()
    finally:
        connection.unregister("model_training_symbol_stage")
    return [pd.Timestamp(row[0]).date() for row in rows]


def _ensure_feature_snapshots(
    settings: Settings,
    *,
    candidate_dates: list[date],
    symbols: list[str] | None,
    limit_symbols: int | None,
    market: str,
) -> list[date]:
    if not candidate_dates:
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        rows = connection.execute(
            """
            SELECT DISTINCT as_of_date
            FROM fact_feature_snapshot
            WHERE as_of_date BETWEEN ? AND ?
            """,
            [min(candidate_dates), max(candidate_dates)],
        ).fetchall()
    existing_dates = {pd.Timestamp(row[0]).date() for row in rows}
    missing_dates = [value for value in candidate_dates if value not in existing_dates]
    for missing_date in missing_dates:
        build_feature_store(
            settings,
            as_of_date=missing_date,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
        )
    return missing_dates


def _load_dataset_frame(
    connection,
    *,
    train_end_date: date,
    horizons: list[int],
    symbols: list[str] | None,
    limit_symbols: int | None,
    market: str,
) -> pd.DataFrame:
    symbol_frame = load_symbol_frame(
        connection,
        symbols=symbols,
        market=market,
        limit_symbols=limit_symbols,
        as_of_date=train_end_date,
    )
    if symbol_frame.empty:
        return pd.DataFrame()

    connection.register("model_training_symbol_stage", symbol_frame[["symbol"]])
    try:
        horizon_placeholders = ",".join("?" for _ in horizons)
        label_rows = connection.execute(
            f"""
            SELECT
                as_of_date,
                symbol,
                horizon,
                excess_forward_return
            FROM fact_forward_return_label
            WHERE as_of_date <= ?
              AND horizon IN ({horizon_placeholders})
              AND label_available_flag
              AND symbol IN (SELECT symbol FROM model_training_symbol_stage)
            ORDER BY as_of_date, symbol, horizon
            """,
            [train_end_date, *horizons],
        ).fetchdf()
        if label_rows.empty:
            return pd.DataFrame()

        feature_rows = connection.execute(
            f"""
            SELECT
                snapshot.as_of_date,
                snapshot.symbol,
                snapshot.feature_name,
                snapshot.feature_value
            FROM fact_feature_snapshot AS snapshot
            WHERE snapshot.as_of_date IN (
                SELECT DISTINCT as_of_date
                FROM fact_forward_return_label
                WHERE as_of_date <= ?
                  AND horizon IN ({horizon_placeholders})
                  AND label_available_flag
                  AND symbol IN (SELECT symbol FROM model_training_symbol_stage)
            )
              AND snapshot.symbol IN (SELECT symbol FROM model_training_symbol_stage)
            ORDER BY snapshot.as_of_date, snapshot.symbol, snapshot.feature_name
            """,
            [train_end_date, *horizons],
        ).fetchdf()
    finally:
        connection.unregister("model_training_symbol_stage")

    if feature_rows.empty:
        return pd.DataFrame()

    feature_matrix = feature_rows.pivot(
        index=["as_of_date", "symbol"],
        columns="feature_name",
        values="feature_value",
    ).reset_index()
    label_matrix = (
        label_rows.assign(
            target_name=label_rows["horizon"].map(lambda value: f"target_h{int(value)}")
        )
        .pivot(
            index=["as_of_date", "symbol"],
            columns="target_name",
            values="excess_forward_return",
        )
        .reset_index()
    )
    dataset = feature_matrix.merge(label_matrix, on=["as_of_date", "symbol"], how="inner")
    dataset = dataset.merge(
        symbol_frame[["symbol", "company_name", "market"]],
        on="symbol",
        how="left",
    )
    dataset["market_is_kospi"] = dataset["market"].eq("KOSPI").astype(float)
    dataset["market_is_kosdaq"] = dataset["market"].eq("KOSDAQ").astype(float)
    dataset["as_of_date"] = pd.to_datetime(dataset["as_of_date"]).dt.date
    for feature_name in FEATURE_NAMES:
        if feature_name not in dataset.columns:
            dataset[feature_name] = pd.NA
    for target_name in [f"target_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    ordered_columns = [
        "as_of_date",
        "symbol",
        "company_name",
        "market",
        *TRAINING_FEATURE_COLUMNS,
        *[f"target_h{int(horizon)}" for horizon in horizons],
    ]
    return dataset[ordered_columns].sort_values(["as_of_date", "symbol"]).reset_index(drop=True)


def build_model_training_dataset(
    settings: Settings,
    *,
    train_end_date: date,
    horizons: list[int],
    min_train_days: int,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
) -> ModelTrainingDatasetResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "build_model_training_dataset", as_of_date=train_end_date
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
                    "fact_forward_return_label",
                    "dim_symbol",
                ],
                notes=(
                    "Assemble supervised alpha-model training dataset. "
                    f"train_end_date={train_end_date.isoformat()} horizons={horizons} "
                    f"min_train_days={min_train_days}"
                ),
                git_commit=None,
            )
            try:
                label_start_date = _resolve_label_start_date(
                    connection, train_end_date=train_end_date
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Model training dataset build failed.",
                    error_message=str(exc),
                    model_version=MODEL_DATASET_VERSION,
                )
                raise

        build_forward_labels(
            settings,
            start_date=label_start_date,
            end_date=train_end_date,
            horizons=horizons,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
        )

        with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
            bootstrap_core_tables(connection)
            candidate_dates = _resolve_candidate_dates(
                connection,
                train_end_date=train_end_date,
                horizons=horizons,
                symbols=symbols,
                limit_symbols=limit_symbols,
                market=market,
            )

        missing_dates = _ensure_feature_snapshots(
            settings,
            candidate_dates=candidate_dates,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
        )

        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            try:
                dataset_frame = _load_dataset_frame(
                    connection,
                    train_end_date=train_end_date,
                    horizons=horizons,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                )
                if dataset_frame.empty:
                    notes = (
                        "No overlapping feature snapshots and forward labels were available "
                        f"for train_end_date={train_end_date.isoformat()}."
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        model_version=MODEL_DATASET_VERSION,
                    )
                    return ModelTrainingDatasetResult(
                        run_id=run_context.run_id,
                        train_end_date=train_end_date,
                        row_count=0,
                        date_count=0,
                        artifact_paths=[],
                        notes=notes,
                        dataset_version=MODEL_DATASET_VERSION,
                    )

                artifact_paths = [
                    str(
                        write_parquet(
                            dataset_frame,
                            base_dir=settings.paths.curated_dir,
                            dataset="model/training_dataset",
                            partitions={"train_end_date": train_end_date.isoformat()},
                            filename="alpha_training_dataset.parquet",
                        )
                    )
                ]
                per_horizon_summary = pd.DataFrame(
                    [
                        {
                            "horizon": int(horizon),
                            "row_count": int(
                                dataset_frame[f"target_h{int(horizon)}"].notna().sum()
                            ),
                            "date_count": int(
                                dataset_frame.loc[
                                    dataset_frame[f"target_h{int(horizon)}"].notna(),
                                    "as_of_date",
                                ].nunique()
                            ),
                        }
                        for horizon in horizons
                    ]
                )
                artifact_paths.append(
                    str(
                        write_parquet(
                            per_horizon_summary,
                            base_dir=settings.paths.artifacts_dir,
                            dataset="model/training_dataset_summary",
                            partitions={"train_end_date": train_end_date.isoformat()},
                            filename="alpha_training_dataset_summary.parquet",
                        )
                    )
                )

                available_days = int(dataset_frame["as_of_date"].nunique())
                fallback_note = ""
                if available_days < min_train_days:
                    fallback_note = (
                        f" available_train_days={available_days} below requested "
                        f"min_train_days={min_train_days}; training should use fallback policy."
                    )
                if missing_dates:
                    fallback_note = (
                        f"{fallback_note} auto_materialized_feature_dates={len(missing_dates)}."
                    ).strip()
                notes = (
                    "Model training dataset assembled. "
                    f"rows={len(dataset_frame)} dates={available_days}.{fallback_note}"
                ).strip()
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=MODEL_DATASET_VERSION,
                )
                return ModelTrainingDatasetResult(
                    run_id=run_context.run_id,
                    train_end_date=train_end_date,
                    row_count=len(dataset_frame),
                    date_count=available_days,
                    artifact_paths=artifact_paths,
                    notes=notes,
                    dataset_version=MODEL_DATASET_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Model training dataset build failed.",
                    error_message=str(exc),
                    model_version=MODEL_DATASET_VERSION,
                )
                raise


def load_training_dataset(
    connection,
    *,
    train_end_date: date,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
) -> pd.DataFrame:
    return _load_dataset_frame(
        connection,
        train_end_date=train_end_date,
        horizons=horizons,
        symbols=symbols,
        limit_symbols=limit_symbols,
        market=market,
    )
