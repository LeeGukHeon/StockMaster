from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.labels.forward_returns import LABEL_VERSION, build_forward_labels
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet


@dataclass(slots=True)
class AlphaShadowSelectionOutcomeResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
    row_count: int
    matured_row_count: int
    pending_row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class AlphaShadowEvaluationSummaryResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def _resolve_target_selection_dates(
    selection_date: date | None,
    start_selection_date: date | None,
    end_selection_date: date | None,
) -> tuple[date, date]:
    if selection_date is not None:
        return selection_date, selection_date
    if start_selection_date is None or end_selection_date is None:
        raise ValueError(
            "Either selection_date or both start_selection_date/end_selection_date are required."
        )
    if start_selection_date > end_selection_date:
        raise ValueError("start_selection_date must be on or before end_selection_date.")
    return start_selection_date, end_selection_date


def _derive_outcome_status(label_available: object, exclusion_reason: object) -> str:
    if pd.notna(label_available) and bool(label_available):
        return "matured"
    if exclusion_reason in {
        "insufficient_future_trading_days",
        "missing_entry_day_ohlcv",
        "missing_exit_day_ohlcv",
    }:
        return "pending"
    return "unavailable"


def _mean_or_none(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _correlation_or_none(left: pd.Series, right: pd.Series) -> float | None:
    pair = pd.DataFrame(
        {
            "left": pd.to_numeric(left, errors="coerce"),
            "right": pd.to_numeric(right, errors="coerce"),
        }
    ).dropna()
    if len(pair) < 2:
        return None
    value = pair["left"].corr(pair["right"])
    if pd.isna(value):
        return None
    return float(value)


def _load_shadow_candidate_rows(
    connection,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    model_spec_ids: list[str] | None,
) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    params: list[object] = [start_selection_date, end_selection_date, *horizons]
    model_spec_clause = ""
    if model_spec_ids:
        model_spec_placeholders = ",".join("?" for _ in model_spec_ids)
        model_spec_clause = f"AND model_spec_id IN ({model_spec_placeholders})"
        params.extend(model_spec_ids)
    return connection.execute(
        f"""
        SELECT selection_date, symbol, horizon, model_spec_id
        FROM fact_alpha_shadow_ranking
        WHERE selection_date BETWEEN ? AND ?
          AND horizon IN ({horizon_placeholders})
          {model_spec_clause}
        ORDER BY selection_date, horizon, model_spec_id, symbol
        """,
        params,
    ).fetchdf()


def upsert_alpha_shadow_selection_outcomes(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("alpha_shadow_selection_outcome_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_alpha_shadow_selection_outcome
        WHERE (selection_date, symbol, horizon, model_spec_id) IN (
            SELECT selection_date, symbol, horizon, model_spec_id
            FROM alpha_shadow_selection_outcome_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_alpha_shadow_selection_outcome (
            selection_date,
            evaluation_date,
            symbol,
            market,
            horizon,
            model_spec_id,
            training_run_id,
            selection_percentile,
            report_candidate_flag,
            grade,
            eligible_flag,
            final_selection_value,
            expected_excess_return_at_selection,
            lower_band_at_selection,
            median_band_at_selection,
            upper_band_at_selection,
            uncertainty_score_at_selection,
            disagreement_score_at_selection,
            realized_excess_return,
            prediction_error,
            outcome_status,
            source_label_version,
            evaluation_run_id,
            created_at,
            updated_at
        )
        SELECT
            selection_date,
            evaluation_date,
            symbol,
            market,
            horizon,
            model_spec_id,
            training_run_id,
            selection_percentile,
            report_candidate_flag,
            grade,
            eligible_flag,
            final_selection_value,
            expected_excess_return_at_selection,
            lower_band_at_selection,
            median_band_at_selection,
            upper_band_at_selection,
            uncertainty_score_at_selection,
            disagreement_score_at_selection,
            realized_excess_return,
            prediction_error,
            outcome_status,
            source_label_version,
            evaluation_run_id,
            created_at,
            updated_at
        FROM alpha_shadow_selection_outcome_stage
        """
    )
    connection.unregister("alpha_shadow_selection_outcome_stage")


def materialize_alpha_shadow_selection_outcomes(
    settings: Settings,
    *,
    selection_date: date | None = None,
    start_selection_date: date | None = None,
    end_selection_date: date | None = None,
    horizons: list[int],
    model_spec_ids: list[str] | None = None,
    symbols: list[str] | None = None,
    market: str = "ALL",
) -> AlphaShadowSelectionOutcomeResult:
    ensure_storage_layout(settings)
    start_dt, end_dt = _resolve_target_selection_dates(
        selection_date,
        start_selection_date,
        end_selection_date,
    )
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        candidate_rows = _load_shadow_candidate_rows(
            connection,
            start_selection_date=start_dt,
            end_selection_date=end_dt,
            horizons=horizons,
            model_spec_ids=model_spec_ids,
        )
    if symbols:
        requested = {symbol.zfill(6) for symbol in symbols}
        candidate_rows = candidate_rows.loc[
            candidate_rows["symbol"].astype(str).str.zfill(6).isin(requested)
        ].copy()
    target_symbols = sorted(
        candidate_rows["symbol"].astype(str).str.zfill(6).drop_duplicates().tolist()
    )
    if target_symbols:
        build_forward_labels(
            settings,
            start_date=start_dt,
            end_date=end_dt,
            horizons=horizons,
            symbols=target_symbols,
            market=market,
        )

    with activate_run_context(
        "materialize_alpha_shadow_selection_outcomes",
        as_of_date=end_dt,
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
                    "fact_alpha_shadow_ranking",
                    "fact_alpha_shadow_prediction",
                    "fact_forward_return_label",
                    "dim_symbol",
                ],
                notes=(
                    "Freeze alpha shadow recommendation snapshots against realized outcomes. "
                    f"range={start_dt.isoformat()}..{end_dt.isoformat()}"
                ),
            )
            try:
                if candidate_rows.empty:
                    notes = (
                        "No alpha shadow ranking rows were available for outcome materialization. "
                        f"range={start_dt.isoformat()}..{end_dt.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return AlphaShadowSelectionOutcomeResult(
                        run_id=run_context.run_id,
                        start_selection_date=start_dt,
                        end_selection_date=end_dt,
                        row_count=0,
                        matured_row_count=0,
                        pending_row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                horizon_placeholders = ",".join("?" for _ in horizons)
                params: list[object] = [start_dt, end_dt, *horizons]
                model_spec_clause = ""
                if model_spec_ids:
                    model_spec_placeholders = ",".join("?" for _ in model_spec_ids)
                    model_spec_clause = f"AND ranking.model_spec_id IN ({model_spec_placeholders})"
                    params.extend(model_spec_ids)
                symbol_clause = ""
                if target_symbols:
                    symbol_placeholders = ",".join("?" for _ in target_symbols)
                    symbol_clause = f"AND ranking.symbol IN ({symbol_placeholders})"
                    params.extend(target_symbols)
                joined = connection.execute(
                    f"""
                    SELECT
                        ranking.selection_date,
                        label.exit_date AS evaluation_date,
                        ranking.symbol,
                        symbol_meta.market,
                        ranking.horizon,
                        ranking.model_spec_id,
                        ranking.training_run_id,
                        ranking.selection_percentile,
                        ranking.report_candidate_flag,
                        ranking.grade,
                        ranking.eligible_flag,
                        ranking.final_selection_value,
                        prediction.expected_excess_return AS expected_excess_return_at_selection,
                        prediction.lower_band AS lower_band_at_selection,
                        prediction.median_band AS median_band_at_selection,
                        prediction.upper_band AS upper_band_at_selection,
                        prediction.uncertainty_score AS uncertainty_score_at_selection,
                        prediction.disagreement_score AS disagreement_score_at_selection,
                        label.excess_forward_return AS realized_excess_return,
                        label.label_available_flag,
                        label.exclusion_reason
                    FROM fact_alpha_shadow_ranking AS ranking
                    JOIN dim_symbol AS symbol_meta
                      ON ranking.symbol = symbol_meta.symbol
                    LEFT JOIN fact_alpha_shadow_prediction AS prediction
                      ON ranking.selection_date = prediction.selection_date
                     AND ranking.symbol = prediction.symbol
                     AND ranking.horizon = prediction.horizon
                     AND ranking.model_spec_id = prediction.model_spec_id
                    LEFT JOIN fact_forward_return_label AS label
                      ON ranking.selection_date = label.as_of_date
                     AND ranking.symbol = label.symbol
                     AND ranking.horizon = label.horizon
                    WHERE ranking.selection_date BETWEEN ? AND ?
                      AND ranking.horizon IN ({horizon_placeholders})
                      {model_spec_clause}
                      {symbol_clause}
                    ORDER BY
                        ranking.selection_date,
                        ranking.horizon,
                        ranking.model_spec_id,
                        ranking.symbol
                    """,
                    params,
                ).fetchdf()
                if joined.empty:
                    notes = (
                        "No alpha shadow joined rows were available for outcome materialization. "
                        f"range={start_dt.isoformat()}..{end_dt.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return AlphaShadowSelectionOutcomeResult(
                        run_id=run_context.run_id,
                        start_selection_date=start_dt,
                        end_selection_date=end_dt,
                        row_count=0,
                        matured_row_count=0,
                        pending_row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                joined["prediction_error"] = (
                    joined["realized_excess_return"] - joined["expected_excess_return_at_selection"]
                )
                joined["outcome_status"] = joined.apply(
                    lambda row: _derive_outcome_status(
                        row["label_available_flag"],
                        row["exclusion_reason"],
                    ),
                    axis=1,
                )
                joined["source_label_version"] = LABEL_VERSION
                joined["evaluation_run_id"] = run_context.run_id
                joined["created_at"] = pd.Timestamp.utcnow()
                joined["updated_at"] = joined["created_at"]
                outcome_frame = joined[
                    [
                        "selection_date",
                        "evaluation_date",
                        "symbol",
                        "market",
                        "horizon",
                        "model_spec_id",
                        "training_run_id",
                        "selection_percentile",
                        "report_candidate_flag",
                        "grade",
                        "eligible_flag",
                        "final_selection_value",
                        "expected_excess_return_at_selection",
                        "lower_band_at_selection",
                        "median_band_at_selection",
                        "upper_band_at_selection",
                        "uncertainty_score_at_selection",
                        "disagreement_score_at_selection",
                        "realized_excess_return",
                        "prediction_error",
                        "outcome_status",
                        "source_label_version",
                        "evaluation_run_id",
                        "created_at",
                        "updated_at",
                    ]
                ].copy()
                upsert_alpha_shadow_selection_outcomes(connection, outcome_frame)

                artifact_paths: list[str] = []
                for (
                    selection_dt,
                    horizon,
                    model_spec_id,
                ), partition_frame in outcome_frame.groupby(
                    ["selection_date", "horizon", "model_spec_id"],
                    sort=True,
                ):
                    artifact_paths.append(
                        str(
                            write_parquet(
                                partition_frame,
                                base_dir=settings.paths.curated_dir,
                                dataset="alpha_shadow/selection_outcomes",
                                partitions={
                                    "selection_date": pd.Timestamp(selection_dt).date().isoformat(),
                                    "horizon": str(int(horizon)),
                                    "model_spec_id": str(model_spec_id),
                                },
                                filename="alpha_shadow_selection_outcomes.parquet",
                            )
                        )
                    )

                matured_row_count = int(outcome_frame["outcome_status"].eq("matured").sum())
                pending_row_count = int(outcome_frame["outcome_status"].eq("pending").sum())
                notes = (
                    "Alpha shadow selection outcomes materialized. "
                    f"range={start_dt.isoformat()}..{end_dt.isoformat()} "
                    f"rows={len(outcome_frame)} matured={matured_row_count} "
                    f"pending={pending_row_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return AlphaShadowSelectionOutcomeResult(
                    run_id=run_context.run_id,
                    start_selection_date=start_dt,
                    end_selection_date=end_dt,
                    row_count=len(outcome_frame),
                    matured_row_count=matured_row_count,
                    pending_row_count=pending_row_count,
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
                    notes="Alpha shadow selection outcome materialization failed.",
                    error_message=str(exc),
                )
                raise


def _segment_frames(frame: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    ordered = frame.sort_values(
        ["selection_date", "final_selection_value", "symbol"],
        ascending=[True, False, True],
    )
    return [
        ("all", ordered.copy()),
        (
            "top5",
            ordered.groupby("selection_date", sort=True, group_keys=False).head(5).copy(),
        ),
        (
            "top10",
            ordered.groupby("selection_date", sort=True, group_keys=False).head(10).copy(),
        ),
        (
            "top20",
            ordered.groupby("selection_date", sort=True, group_keys=False).head(20).copy(),
        ),
        (
            "report_candidates",
            ordered.loc[ordered["report_candidate_flag"].fillna(False).astype(bool)].copy(),
        ),
    ]


def _build_summary_row(
    frame: pd.DataFrame,
    *,
    summary_date: date,
    window_type: str,
    window_start: date,
    window_end: date,
    horizon: int,
    model_spec_id: str,
    segment_value: str,
    run_id: str,
) -> dict[str, object]:
    matured = frame.loc[frame["outcome_status"] == "matured"].copy()
    point_loss = pd.to_numeric(matured["prediction_error"], errors="coerce").pow(2)
    return {
        "summary_date": summary_date,
        "window_type": window_type,
        "window_start": window_start,
        "window_end": window_end,
        "horizon": int(horizon),
        "model_spec_id": model_spec_id,
        "segment_value": segment_value,
        "count_evaluated": int(len(matured)),
        "mean_realized_excess_return": _mean_or_none(matured["realized_excess_return"]),
        "mean_point_loss": _mean_or_none(point_loss),
        "rank_ic": _correlation_or_none(
            matured["selection_percentile"],
            matured["realized_excess_return"],
        ),
        "evaluation_run_id": run_id,
        "created_at": pd.Timestamp.utcnow(),
    }


def upsert_alpha_shadow_evaluation_summary(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("alpha_shadow_evaluation_summary_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_alpha_shadow_evaluation_summary
        WHERE (summary_date, window_type, horizon, model_spec_id, segment_value) IN (
            SELECT summary_date, window_type, horizon, model_spec_id, segment_value
            FROM alpha_shadow_evaluation_summary_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_alpha_shadow_evaluation_summary (
            summary_date,
            window_type,
            window_start,
            window_end,
            horizon,
            model_spec_id,
            segment_value,
            count_evaluated,
            mean_realized_excess_return,
            mean_point_loss,
            rank_ic,
            evaluation_run_id,
            created_at
        )
        SELECT
            summary_date,
            window_type,
            window_start,
            window_end,
            horizon,
            model_spec_id,
            segment_value,
            count_evaluated,
            mean_realized_excess_return,
            mean_point_loss,
            rank_ic,
            evaluation_run_id,
            created_at
        FROM alpha_shadow_evaluation_summary_stage
        """
    )
    connection.unregister("alpha_shadow_evaluation_summary_stage")


def materialize_alpha_shadow_evaluation_summary(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    model_spec_ids: list[str] | None = None,
    rolling_windows: list[int] | None = None,
) -> AlphaShadowEvaluationSummaryResult:
    ensure_storage_layout(settings)
    rolling_windows = list(rolling_windows or [20, 60])
    materialize_alpha_shadow_selection_outcomes(
        settings,
        start_selection_date=start_selection_date,
        end_selection_date=end_selection_date,
        horizons=horizons,
        model_spec_ids=model_spec_ids,
    )

    with activate_run_context(
        "materialize_alpha_shadow_evaluation_summary",
        as_of_date=end_selection_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_alpha_shadow_selection_outcome"],
                notes=(
                    "Aggregate alpha shadow self-backtest summary. "
                    f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                ),
            )
            try:
                horizon_placeholders = ",".join("?" for _ in horizons)
                params: list[object] = [start_selection_date, end_selection_date, *horizons]
                model_spec_clause = ""
                if model_spec_ids:
                    model_spec_placeholders = ",".join("?" for _ in model_spec_ids)
                    model_spec_clause = f"AND model_spec_id IN ({model_spec_placeholders})"
                    params.extend(model_spec_ids)
                outcomes = connection.execute(
                    f"""
                    SELECT *
                    FROM fact_alpha_shadow_selection_outcome
                    WHERE selection_date BETWEEN ? AND ?
                      AND horizon IN ({horizon_placeholders})
                      {model_spec_clause}
                    ORDER BY selection_date, horizon, model_spec_id, symbol
                    """,
                    params,
                ).fetchdf()
                if outcomes.empty:
                    notes = (
                        "No alpha shadow outcomes were available for summary materialization. "
                        f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return AlphaShadowEvaluationSummaryResult(
                        run_id=run_context.run_id,
                        start_selection_date=start_selection_date,
                        end_selection_date=end_selection_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                summary_rows: list[dict[str, object]] = []
                for (horizon, model_spec_id), subset in outcomes.groupby(
                    ["horizon", "model_spec_id"],
                    sort=True,
                ):
                    subset = subset.copy()
                    for segment_value, segment_frame in _segment_frames(subset):
                        summary_rows.append(
                            _build_summary_row(
                                segment_frame,
                                summary_date=end_selection_date,
                                window_type="cohort",
                                window_start=start_selection_date,
                                window_end=end_selection_date,
                                horizon=int(horizon),
                                model_spec_id=str(model_spec_id),
                                segment_value=segment_value,
                                run_id=run_context.run_id,
                            )
                        )
                    matured_dates = sorted(
                        {
                            pd.Timestamp(value).date()
                            for value in subset.loc[
                                subset["outcome_status"] == "matured",
                                "selection_date",
                            ].tolist()
                        }
                    )
                    for window in rolling_windows:
                        if not matured_dates:
                            continue
                        trailing_dates = matured_dates[-int(window) :]
                        selection_dates = pd.to_datetime(subset["selection_date"]).dt.date
                        window_frame = subset.loc[
                            selection_dates.isin(trailing_dates)
                        ].copy()
                        window_start = min(trailing_dates)
                        window_end = max(trailing_dates)
                        for segment_value, segment_frame in _segment_frames(window_frame):
                            summary_rows.append(
                                _build_summary_row(
                                    segment_frame,
                                    summary_date=end_selection_date,
                                    window_type=f"rolling_{int(window)}",
                                    window_start=window_start,
                                    window_end=window_end,
                                    horizon=int(horizon),
                                    model_spec_id=str(model_spec_id),
                                    segment_value=segment_value,
                                    run_id=run_context.run_id,
                                )
                            )

                summary_frame = pd.DataFrame(summary_rows)
                upsert_alpha_shadow_evaluation_summary(connection, summary_frame)

                artifact_paths: list[str] = []
                if not summary_frame.empty:
                    for (
                        horizon,
                        model_spec_id,
                        window_type,
                    ), partition_frame in summary_frame.groupby(
                        ["horizon", "model_spec_id", "window_type"],
                        sort=True,
                    ):
                        artifact_paths.append(
                            str(
                                write_parquet(
                                    partition_frame,
                                    base_dir=settings.paths.curated_dir,
                                    dataset="alpha_shadow/evaluation_summary",
                                    partitions={
                                        "summary_date": end_selection_date.isoformat(),
                                        "horizon": str(int(horizon)),
                                        "model_spec_id": str(model_spec_id),
                                        "window_type": str(window_type),
                                    },
                                    filename="alpha_shadow_evaluation_summary.parquet",
                                )
                            )
                        )
                notes = (
                    "Alpha shadow evaluation summary materialized. "
                    f"summary_date={end_selection_date.isoformat()} rows={len(summary_frame)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return AlphaShadowEvaluationSummaryResult(
                    run_id=run_context.run_id,
                    start_selection_date=start_selection_date,
                    end_selection_date=end_selection_date,
                    row_count=len(summary_frame),
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
                    notes="Alpha shadow evaluation summary materialization failed.",
                    error_message=str(exc),
                )
                raise
