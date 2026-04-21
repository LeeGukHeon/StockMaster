from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.feature_store import load_feature_matrix
from app.labels.forward_returns import LABEL_VERSION, build_forward_labels
from app.ml.constants import D5_PRIMARY_BUCKET_SEGMENTS, get_alpha_model_spec
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


@dataclass(slots=True)
class AlphaShadowSelectionGapScorecardResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class AlphaShadowCoverageWindow:
    horizon: int
    requested_start_selection_date: date
    requested_end_selection_date: date
    candidate_min_selection_date: date | None
    candidate_max_selection_date: date | None
    stored_label_max_selection_date: date | None
    stored_matured_max_selection_date: date | None
    db_only_max_selection_date: date | None
    effective_start_selection_date: date | None
    effective_end_selection_date: date | None


BUCKET_FEATURE_COLUMNS: tuple[str, ...] = (
    "ret_10d",
    "dist_from_20d_high",
    "drawdown_20d",
    "residual_ret_5d",
    "turnover_z_5_20",
    "news_burst_share_1d",
    "realized_vol_20d",
)


def _load_bucket_feature_frame(
    connection,
    *,
    selection_dates: list[date],
    symbols: list[str],
) -> pd.DataFrame:
    bucket_frames: list[pd.DataFrame] = []
    for selection_date in selection_dates:
        feature_matrix = load_feature_matrix(
            connection,
            as_of_date=selection_date,
            symbols=symbols,
            include_rank_features=False,
            include_zscore_features=False,
        )
        if feature_matrix.empty:
            continue
        available_columns = [
            column for column in BUCKET_FEATURE_COLUMNS if column in feature_matrix.columns
        ]
        if not available_columns:
            continue
        bucket_frame = feature_matrix.loc[:, ["symbol", *available_columns]].copy()
        bucket_frame["selection_date"] = selection_date
        bucket_frames.append(bucket_frame)
    if not bucket_frames:
        return pd.DataFrame(columns=["selection_date", "symbol", *BUCKET_FEATURE_COLUMNS])
    combined = pd.concat(bucket_frames, ignore_index=True)
    combined["symbol"] = combined["symbol"].astype(str).str.zfill(6)
    return combined


def _attach_bucket_features(connection, frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    selection_dates = sorted(
        {
            pd.Timestamp(value).date()
            for value in pd.Series(frame["selection_date"]).dropna().tolist()
        }
    )
    symbols = sorted(frame["symbol"].astype(str).str.zfill(6).drop_duplicates().tolist())
    if not selection_dates or not symbols:
        return frame
    bucket_features = _load_bucket_feature_frame(
        connection,
        selection_dates=selection_dates,
        symbols=symbols,
    )
    if bucket_features.empty:
        return frame
    working = frame.copy()
    working["selection_date"] = pd.to_datetime(working["selection_date"]).dt.date
    working["symbol"] = working["symbol"].astype(str).str.zfill(6)
    return working.merge(
        bucket_features,
        on=["selection_date", "symbol"],
        how="left",
    )


def _bucket_masks(frame: pd.DataFrame) -> dict[str, pd.Series]:
    def _series(column_name: str) -> pd.Series:
        if column_name not in frame.columns:
            return pd.Series(float("nan"), index=frame.index, dtype="float64")
        return pd.to_numeric(frame[column_name], errors="coerce")

    ret_10d = _series("ret_10d")
    dist_from_20d_high = _series("dist_from_20d_high")
    drawdown_20d = _series("drawdown_20d")
    residual_ret_5d = _series("residual_ret_5d")
    turnover_z_5_20 = _series("turnover_z_5_20")
    news_burst_share_1d = _series("news_burst_share_1d")
    realized_vol_20d = _series("realized_vol_20d")
    return {
        "bucket_continuation": ret_10d.gt(0.03) & dist_from_20d_high.le(0.15),
        "bucket_reversal_recovery": drawdown_20d.le(-0.08) & residual_ret_5d.gt(0.0),
        "bucket_crowded_risk": (
            turnover_z_5_20.ge(1.5)
            | news_burst_share_1d.ge(0.30)
            | realized_vol_20d.ge(0.05)
        ),
    }


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


def _latest_selection_date_supported_by_current_market_data(
    connection,
    *,
    horizon: int,
) -> date | None:
    latest_market_row = connection.execute(
        """
        SELECT MAX(trading_date)
        FROM fact_daily_ohlcv
        """
    ).fetchone()
    if latest_market_row is None or latest_market_row[0] is None:
        return None
    latest_market_date = pd.Timestamp(latest_market_row[0]).date()
    trading_days = (
        connection.execute(
            """
            SELECT trading_date
            FROM dim_trading_calendar
            WHERE is_trading_day
              AND trading_date <= ?
            ORDER BY trading_date
            """,
            [latest_market_date],
        )
        .fetchdf()["trading_date"]
        .tolist()
    )
    trading_days = [pd.Timestamp(value).date() for value in trading_days]
    offset = int(horizon) + 1
    if len(trading_days) < offset:
        return None
    return trading_days[-offset]


def resolve_alpha_shadow_db_only_windows(
    connection,
    *,
    requested_start_selection_date: date,
    requested_end_selection_date: date,
    horizons: list[int],
    model_spec_ids: list[str] | None = None,
) -> list[AlphaShadowCoverageWindow]:
    if requested_start_selection_date > requested_end_selection_date:
        raise ValueError("requested_start_selection_date must be on or before requested_end_selection_date.")

    model_spec_clause = ""
    model_spec_params: list[object] = []
    if model_spec_ids:
        placeholders = ",".join("?" for _ in model_spec_ids)
        model_spec_clause = f"AND model_spec_id IN ({placeholders})"
        model_spec_params.extend(model_spec_ids)

    windows: list[AlphaShadowCoverageWindow] = []
    for horizon in sorted({int(value) for value in horizons}):
        candidate_row = connection.execute(
            f"""
            SELECT MIN(selection_date), MAX(selection_date)
            FROM fact_alpha_shadow_ranking
            WHERE horizon = ?
              {model_spec_clause}
            """,
            [horizon, *model_spec_params],
        ).fetchone()
        candidate_min = (
            pd.Timestamp(candidate_row[0]).date()
            if candidate_row is not None and candidate_row[0] is not None
            else None
        )
        candidate_max = (
            pd.Timestamp(candidate_row[1]).date()
            if candidate_row is not None and candidate_row[1] is not None
            else None
        )
        stored_label_row = connection.execute(
            """
            SELECT MAX(as_of_date)
            FROM fact_forward_return_label
            WHERE horizon = ?
              AND label_available_flag
            """,
            [horizon],
        ).fetchone()
        stored_label_max = (
            pd.Timestamp(stored_label_row[0]).date()
            if stored_label_row is not None and stored_label_row[0] is not None
            else None
        )
        stored_matured_row = connection.execute(
            f"""
            SELECT MAX(selection_date)
            FROM fact_alpha_shadow_selection_outcome
            WHERE horizon = ?
              AND outcome_status = 'matured'
              {model_spec_clause}
            """,
            [horizon, *model_spec_params],
        ).fetchone()
        stored_matured_max = (
            pd.Timestamp(stored_matured_row[0]).date()
            if stored_matured_row is not None and stored_matured_row[0] is not None
            else None
        )
        market_supported_max = _latest_selection_date_supported_by_current_market_data(
            connection,
            horizon=horizon,
        )

        db_only_max = None
        if candidate_max is not None and market_supported_max is not None:
            db_only_max = min(candidate_max, market_supported_max, requested_end_selection_date)

        effective_start = None
        effective_end = None
        if candidate_min is not None and db_only_max is not None:
            effective_start = max(requested_start_selection_date, candidate_min)
            if effective_start <= db_only_max:
                effective_end = db_only_max
            else:
                effective_start = None
                effective_end = None

        windows.append(
            AlphaShadowCoverageWindow(
                horizon=horizon,
                requested_start_selection_date=requested_start_selection_date,
                requested_end_selection_date=requested_end_selection_date,
                candidate_min_selection_date=candidate_min,
                candidate_max_selection_date=candidate_max,
                stored_label_max_selection_date=stored_label_max,
                stored_matured_max_selection_date=stored_matured_max,
                db_only_max_selection_date=db_only_max,
                effective_start_selection_date=effective_start,
                effective_end_selection_date=effective_end,
            )
        )
    return windows


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


def _supports_point_loss(model_spec_id: object) -> bool:
    if model_spec_id in (None, ""):
        return True
    try:
        return get_alpha_model_spec(str(model_spec_id)).target_variant == "excess_return"
    except KeyError:
        return True


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


def _resolve_report_candidate_limit(model_spec_id: object, *, horizon: int) -> int | None:
    if model_spec_id in (None, ""):
        return None
    try:
        target_variant = get_alpha_model_spec(str(model_spec_id)).target_variant
    except KeyError:
        return None
    if target_variant == "top5_binary":
        return 5
    if str(model_spec_id) == "alpha_topbucket_h1_rolling_120_v1" and int(horizon) == 1:
        return 5
    if target_variant == "top20_weighted":
        return 10
    return None


def _extract_report_candidate_frame(
    frame: pd.DataFrame,
    *,
    horizon: int,
    model_spec_id: str,
) -> pd.DataFrame:
    ordered = frame.sort_values(
        ["selection_date", "final_selection_value", "symbol"],
        ascending=[True, False, True],
    )
    candidate_limit = _resolve_report_candidate_limit(model_spec_id, horizon=horizon)
    if candidate_limit is not None:
        return ordered.groupby("selection_date", sort=True, group_keys=False).head(candidate_limit).copy()
    return ordered.loc[ordered["report_candidate_flag"].fillna(False).astype(bool)].copy()


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

                joined["prediction_error"] = pd.Series(float("nan"), index=joined.index, dtype="float64")
                point_loss_supported = joined["model_spec_id"].map(_supports_point_loss)
                joined.loc[point_loss_supported, "prediction_error"] = (
                    joined.loc[point_loss_supported, "realized_excess_return"]
                    - joined.loc[point_loss_supported, "expected_excess_return_at_selection"]
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


def _segment_frames(
    frame: pd.DataFrame,
    *,
    horizon: int,
    model_spec_id: str,
) -> list[tuple[str, pd.DataFrame]]:
    ordered = frame.sort_values(
        ["selection_date", "final_selection_value", "symbol"],
        ascending=[True, False, True],
    )
    report_candidates = _extract_report_candidate_frame(
        ordered,
        horizon=horizon,
        model_spec_id=model_spec_id,
    )
    segments: list[tuple[str, pd.DataFrame]] = [
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
            report_candidates,
        ),
    ]
    if int(horizon) == 5:
        bucket_masks = _bucket_masks(ordered)
        for segment_value in D5_PRIMARY_BUCKET_SEGMENTS:
            bucket_mask = bucket_masks.get(segment_value)
            if bucket_mask is None:
                continue
            segments.append((segment_value, ordered.loc[bucket_mask].copy()))
    return segments


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
    matured_selection_date_count = int(
        pd.to_datetime(matured.get("selection_date"), errors="coerce")
        .dt.date.dropna()
        .nunique()
    )
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
        "matured_selection_date_count": matured_selection_date_count,
        "mean_realized_excess_return": _mean_or_none(matured["realized_excess_return"]),
        "mean_point_loss": _mean_or_none(point_loss),
        "rank_ic": _correlation_or_none(
            matured["selection_percentile"],
            matured["realized_excess_return"],
        ),
        "evaluation_run_id": run_id,
        "created_at": pd.Timestamp.utcnow(),
    }


RAW_TOP5_SOURCE_LABEL = (
    "fact_alpha_shadow_prediction.expected_excess_return_at_selection desc within "
    "(selection_date, horizon, model_spec_id)"
)
TOP5_HIT_RATE_FORMULA_LABEL = (
    "share of matured rows in segment where realized_excess_return > 0.0"
)
D1_EXTREME_EXPECTED_RETURN_THRESHOLD = 0.05


def _hit_rate_or_none(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.gt(0.0).mean())


def _raw_top5_frame(frame: pd.DataFrame) -> pd.DataFrame:
    ordered = frame.sort_values(
        ["selection_date", "expected_excess_return_at_selection", "symbol"],
        ascending=[True, False, True],
    )
    return ordered.groupby("selection_date", sort=True, group_keys=False).head(5).copy()


def _compute_top5_overlap_metrics(
    raw_top5: pd.DataFrame,
    selected_top5: pd.DataFrame,
) -> tuple[float | None, float | None, float | None]:
    overlap_values: list[float] = []
    pred_only_returns: list[float] = []
    sel_only_returns: list[float] = []
    selection_dates = sorted(
        {
            pd.Timestamp(value).date()
            for value in pd.concat(
                [raw_top5["selection_date"], selected_top5["selection_date"]],
                ignore_index=True,
            ).tolist()
        }
    )
    for selection_date in selection_dates:
        raw_frame = raw_top5.loc[
            pd.to_datetime(raw_top5["selection_date"]).dt.date == selection_date
        ].copy()
        selected_frame = selected_top5.loc[
            pd.to_datetime(selected_top5["selection_date"]).dt.date == selection_date
        ].copy()
        raw_symbols = set(raw_frame["symbol"].astype(str))
        selected_symbols = set(selected_frame["symbol"].astype(str))
        denominator = max(len(raw_symbols), len(selected_symbols), 1)
        overlap_values.append(len(raw_symbols & selected_symbols) / float(denominator))
        if raw_symbols - selected_symbols:
            pred_only_returns.extend(
                pd.to_numeric(
                    raw_frame.loc[
                        raw_frame["symbol"].astype(str).isin(raw_symbols - selected_symbols),
                        "realized_excess_return",
                    ],
                    errors="coerce",
                )
                .dropna()
                .tolist()
            )
        if selected_symbols - raw_symbols:
            sel_only_returns.extend(
                pd.to_numeric(
                    selected_frame.loc[
                        selected_frame["symbol"].astype(str).isin(selected_symbols - raw_symbols),
                        "realized_excess_return",
                    ],
                    errors="coerce",
                )
                .dropna()
                .tolist()
            )
    overlap = None if not overlap_values else float(pd.Series(overlap_values).mean())
    pred_only_mean = None if not pred_only_returns else float(pd.Series(pred_only_returns).mean())
    sel_only_mean = None if not sel_only_returns else float(pd.Series(sel_only_returns).mean())
    return overlap, pred_only_mean, sel_only_mean


def _expected_return_concentration_metrics(
    frame: pd.DataFrame,
) -> tuple[float | None, float | None, float | None, int]:
    values = pd.to_numeric(
        frame.get("expected_excess_return_at_selection"),
        errors="coerce",
    ).dropna()
    if values.empty:
        return None, None, None, 0
    ordered = values.sort_values(ascending=False).reset_index(drop=True)
    top1 = float(ordered.iloc[0])
    positive_sum = float(ordered.clip(lower=0.0).sum())
    share = None if positive_sum <= 0.0 else float(top1 / positive_sum)
    median_value = float(ordered.median())
    spread = float(top1 - median_value)
    worst_realized = _mean_or_none(
        pd.to_numeric(frame.get("realized_excess_return"), errors="coerce").nsmallest(1)
    )
    extreme_count = int(ordered.ge(D1_EXTREME_EXPECTED_RETURN_THRESHOLD).sum())
    return worst_realized, share, spread, extreme_count


def _build_selection_gap_row(
    frame: pd.DataFrame,
    *,
    summary_date: date,
    window_name: str,
    window_start: date,
    window_end: date,
    horizon: int,
    model_spec_id: str,
    required_selection_date_count: int,
    insufficient_history_flag: bool,
    run_id: str,
) -> dict[str, object]:
    matured = frame.loc[frame["outcome_status"] == "matured"].copy()
    raw_top5 = _raw_top5_frame(matured)
    selected_top5 = (
        matured.sort_values(
            ["selection_date", "final_selection_value", "symbol"],
            ascending=[True, False, True],
        )
        .groupby("selection_date", sort=True, group_keys=False)
        .head(5)
        .copy()
    )
    report_candidates = _extract_report_candidate_frame(
        matured,
        horizon=int(horizon),
        model_spec_id=str(model_spec_id),
    )
    overlap, pred_only_mean, sel_only_mean = _compute_top5_overlap_metrics(
        raw_top5,
        selected_top5,
    )
    (
        raw_worst_realized,
        raw_top1_share,
        raw_top1_spread,
        raw_extreme_count,
    ) = _expected_return_concentration_metrics(raw_top5)
    (
        selected_worst_realized,
        selected_top1_share,
        selected_top1_spread,
        selected_extreme_count,
    ) = _expected_return_concentration_metrics(selected_top5)
    raw_mean = _mean_or_none(raw_top5["realized_excess_return"])
    selected_mean = _mean_or_none(selected_top5["realized_excess_return"])
    return {
        "summary_date": summary_date,
        "window_name": window_name,
        "window_start": window_start,
        "window_end": window_end,
        "horizon": int(horizon),
        "model_spec_id": str(model_spec_id),
        "segment_name": "top5",
        "matured_selection_date_count": int(
            matured["selection_date"].dropna().map(lambda value: pd.Timestamp(value).date()).nunique()
        ),
        "required_selection_date_count": int(required_selection_date_count),
        "insufficient_history_flag": bool(insufficient_history_flag),
        "raw_top5_source": RAW_TOP5_SOURCE_LABEL,
        "hit_rate_formula": TOP5_HIT_RATE_FORMULA_LABEL,
        "raw_top5_mean_realized_excess_return": raw_mean,
        "selected_top5_mean_realized_excess_return": selected_mean,
        "report_candidates_mean_realized_excess_return": _mean_or_none(
            report_candidates["realized_excess_return"]
        ),
        "raw_top5_hit_rate": _hit_rate_or_none(raw_top5["realized_excess_return"]),
        "selected_top5_hit_rate": _hit_rate_or_none(selected_top5["realized_excess_return"]),
        "report_candidates_hit_rate": _hit_rate_or_none(
            report_candidates["realized_excess_return"]
        ),
        "top5_overlap": overlap,
        "pred_only_top5_mean_realized_excess_return": pred_only_mean,
        "sel_only_top5_mean_realized_excess_return": sel_only_mean,
        "raw_top5_worst_realized_excess_return": raw_worst_realized,
        "selected_top5_worst_realized_excess_return": selected_worst_realized,
        "raw_top5_top1_expected_return_share": raw_top1_share,
        "selected_top5_top1_expected_return_share": selected_top1_share,
        "raw_top5_top1_minus_median_expected_return": raw_top1_spread,
        "selected_top5_top1_minus_median_expected_return": selected_top1_spread,
        "extreme_expected_return_threshold": float(D1_EXTREME_EXPECTED_RETURN_THRESHOLD),
        "raw_top5_extreme_expected_return_count": raw_extreme_count,
        "selected_top5_extreme_expected_return_count": selected_extreme_count,
        "drag_vs_raw_top5": (
            None
            if raw_mean is None or selected_mean is None
            else float(selected_mean - raw_mean)
        ),
        "evaluation_run_id": run_id,
        "created_at": pd.Timestamp.utcnow(),
    }


def _build_insufficient_selection_gap_row(
    *,
    summary_date: date,
    window_name: str,
    window_start: date,
    window_end: date,
    horizon: int,
    model_spec_id: str,
    matured_selection_date_count: int,
    required_selection_date_count: int,
    run_id: str,
) -> dict[str, object]:
    return {
        "summary_date": summary_date,
        "window_name": window_name,
        "window_start": window_start,
        "window_end": window_end,
        "horizon": int(horizon),
        "model_spec_id": str(model_spec_id),
        "segment_name": "top5",
        "matured_selection_date_count": int(matured_selection_date_count),
        "required_selection_date_count": int(required_selection_date_count),
        "insufficient_history_flag": True,
        "raw_top5_source": RAW_TOP5_SOURCE_LABEL,
        "hit_rate_formula": TOP5_HIT_RATE_FORMULA_LABEL,
        "raw_top5_mean_realized_excess_return": None,
        "selected_top5_mean_realized_excess_return": None,
        "report_candidates_mean_realized_excess_return": None,
        "raw_top5_hit_rate": None,
        "selected_top5_hit_rate": None,
        "report_candidates_hit_rate": None,
        "top5_overlap": None,
        "pred_only_top5_mean_realized_excess_return": None,
        "sel_only_top5_mean_realized_excess_return": None,
        "raw_top5_worst_realized_excess_return": None,
        "selected_top5_worst_realized_excess_return": None,
        "raw_top5_top1_expected_return_share": None,
        "selected_top5_top1_expected_return_share": None,
        "raw_top5_top1_minus_median_expected_return": None,
        "selected_top5_top1_minus_median_expected_return": None,
        "extreme_expected_return_threshold": float(D1_EXTREME_EXPECTED_RETURN_THRESHOLD),
        "raw_top5_extreme_expected_return_count": 0,
        "selected_top5_extreme_expected_return_count": 0,
        "drag_vs_raw_top5": None,
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
            matured_selection_date_count,
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
            matured_selection_date_count,
            mean_realized_excess_return,
            mean_point_loss,
            rank_ic,
            evaluation_run_id,
            created_at
        FROM alpha_shadow_evaluation_summary_stage
        """
    )
    connection.unregister("alpha_shadow_evaluation_summary_stage")


def upsert_alpha_shadow_selection_gap_scorecard(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("alpha_shadow_selection_gap_scorecard_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_alpha_shadow_selection_gap_scorecard
        WHERE (
            summary_date,
            window_name,
            horizon,
            model_spec_id,
            segment_name
        ) IN (
            SELECT
                summary_date,
                window_name,
                horizon,
                model_spec_id,
                segment_name
            FROM alpha_shadow_selection_gap_scorecard_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_alpha_shadow_selection_gap_scorecard (
            summary_date,
            window_name,
            window_start,
            window_end,
            horizon,
            model_spec_id,
            segment_name,
            matured_selection_date_count,
            required_selection_date_count,
            insufficient_history_flag,
            raw_top5_source,
            hit_rate_formula,
            raw_top5_mean_realized_excess_return,
            selected_top5_mean_realized_excess_return,
            report_candidates_mean_realized_excess_return,
            raw_top5_hit_rate,
            selected_top5_hit_rate,
            report_candidates_hit_rate,
            top5_overlap,
            pred_only_top5_mean_realized_excess_return,
            sel_only_top5_mean_realized_excess_return,
            raw_top5_worst_realized_excess_return,
            selected_top5_worst_realized_excess_return,
            raw_top5_top1_expected_return_share,
            selected_top5_top1_expected_return_share,
            raw_top5_top1_minus_median_expected_return,
            selected_top5_top1_minus_median_expected_return,
            extreme_expected_return_threshold,
            raw_top5_extreme_expected_return_count,
            selected_top5_extreme_expected_return_count,
            drag_vs_raw_top5,
            evaluation_run_id,
            created_at
        )
        SELECT
            summary_date,
            window_name,
            window_start,
            window_end,
            horizon,
            model_spec_id,
            segment_name,
            matured_selection_date_count,
            required_selection_date_count,
            insufficient_history_flag,
            raw_top5_source,
            hit_rate_formula,
            raw_top5_mean_realized_excess_return,
            selected_top5_mean_realized_excess_return,
            report_candidates_mean_realized_excess_return,
            raw_top5_hit_rate,
            selected_top5_hit_rate,
            report_candidates_hit_rate,
            top5_overlap,
            pred_only_top5_mean_realized_excess_return,
            sel_only_top5_mean_realized_excess_return,
            raw_top5_worst_realized_excess_return,
            selected_top5_worst_realized_excess_return,
            raw_top5_top1_expected_return_share,
            selected_top5_top1_expected_return_share,
            raw_top5_top1_minus_median_expected_return,
            selected_top5_top1_minus_median_expected_return,
            extreme_expected_return_threshold,
            raw_top5_extreme_expected_return_count,
            selected_top5_extreme_expected_return_count,
            drag_vs_raw_top5,
            evaluation_run_id,
            created_at
        FROM alpha_shadow_selection_gap_scorecard_stage
        """
    )
    connection.unregister("alpha_shadow_selection_gap_scorecard_stage")


def materialize_alpha_shadow_evaluation_summary(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    model_spec_ids: list[str] | None = None,
    rolling_windows: list[int] | None = None,
    ensure_shadow_selection_outcomes: bool = True,
) -> AlphaShadowEvaluationSummaryResult:
    ensure_storage_layout(settings)
    rolling_windows = list(rolling_windows or [20, 60])
    if ensure_shadow_selection_outcomes:
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

                outcomes = _attach_bucket_features(connection, outcomes)
                summary_rows: list[dict[str, object]] = []
                for (horizon, model_spec_id), subset in outcomes.groupby(
                    ["horizon", "model_spec_id"],
                    sort=True,
                ):
                    subset = subset.copy()
                    for segment_value, segment_frame in _segment_frames(
                        subset,
                        horizon=int(horizon),
                        model_spec_id=str(model_spec_id),
                    ):
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
                        for segment_value, segment_frame in _segment_frames(
                            window_frame,
                            horizon=int(horizon),
                            model_spec_id=str(model_spec_id),
                        ):
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


def materialize_alpha_shadow_selection_gap_scorecard(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    model_spec_ids: list[str] | None = None,
    rolling_windows: list[int] | None = None,
    ensure_shadow_selection_outcomes: bool = True,
) -> AlphaShadowSelectionGapScorecardResult:
    ensure_storage_layout(settings)
    rolling_windows = list(rolling_windows or [20, 60])
    if ensure_shadow_selection_outcomes:
        materialize_alpha_shadow_selection_outcomes(
            settings,
            start_selection_date=start_selection_date,
            end_selection_date=end_selection_date,
            horizons=horizons,
            model_spec_ids=model_spec_ids,
        )

    with activate_run_context(
        "materialize_alpha_shadow_selection_gap_scorecard",
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
                    "Materialize alpha shadow selection-gap scorecard. "
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
                        "No alpha shadow outcomes were available for selection-gap scorecard materialization. "
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
                    return AlphaShadowSelectionGapScorecardResult(
                        run_id=run_context.run_id,
                        start_selection_date=start_selection_date,
                        end_selection_date=end_selection_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                scorecard_rows: list[dict[str, object]] = []
                for (horizon, model_spec_id), subset in outcomes.groupby(
                    ["horizon", "model_spec_id"],
                    sort=True,
                ):
                    subset = subset.copy()
                    matured_dates = sorted(
                        {
                            pd.Timestamp(value).date()
                            for value in subset.loc[
                                subset["outcome_status"] == "matured",
                                "selection_date",
                            ].tolist()
                        }
                    )
                    if matured_dates:
                        scorecard_rows.append(
                            _build_selection_gap_row(
                                subset,
                                summary_date=end_selection_date,
                                window_name="cohort",
                                window_start=min(matured_dates),
                                window_end=max(matured_dates),
                                horizon=int(horizon),
                                model_spec_id=str(model_spec_id),
                                required_selection_date_count=len(matured_dates),
                                insufficient_history_flag=False,
                                run_id=run_context.run_id,
                            )
                        )
                    for window in rolling_windows:
                        window_name = f"rolling_{int(window)}"
                        if len(matured_dates) < int(window):
                            scorecard_rows.append(
                                _build_insufficient_selection_gap_row(
                                    summary_date=end_selection_date,
                                    window_name=window_name,
                                    window_start=min(matured_dates) if matured_dates else start_selection_date,
                                    window_end=max(matured_dates) if matured_dates else end_selection_date,
                                    horizon=int(horizon),
                                    model_spec_id=str(model_spec_id),
                                    matured_selection_date_count=len(matured_dates),
                                    required_selection_date_count=int(window),
                                    run_id=run_context.run_id,
                                )
                            )
                            continue
                        trailing_dates = matured_dates[-int(window) :]
                        selection_dates = pd.to_datetime(subset["selection_date"]).dt.date
                        window_frame = subset.loc[selection_dates.isin(trailing_dates)].copy()
                        scorecard_rows.append(
                            _build_selection_gap_row(
                                window_frame,
                                summary_date=end_selection_date,
                                window_name=window_name,
                                window_start=min(trailing_dates),
                                window_end=max(trailing_dates),
                                horizon=int(horizon),
                                model_spec_id=str(model_spec_id),
                                required_selection_date_count=int(window),
                                insufficient_history_flag=False,
                                run_id=run_context.run_id,
                            )
                        )

                scorecard_frame = pd.DataFrame(scorecard_rows)
                upsert_alpha_shadow_selection_gap_scorecard(connection, scorecard_frame)

                artifact_paths: list[str] = []
                if not scorecard_frame.empty:
                    for (
                        horizon,
                        model_spec_id,
                        window_name,
                    ), partition_frame in scorecard_frame.groupby(
                        ["horizon", "model_spec_id", "window_name"],
                        sort=True,
                    ):
                        artifact_paths.append(
                            str(
                                write_parquet(
                                    partition_frame,
                                    base_dir=settings.paths.curated_dir,
                                    dataset="alpha_shadow/selection_gap_scorecard",
                                    partitions={
                                        "summary_date": end_selection_date.isoformat(),
                                        "horizon": str(int(horizon)),
                                        "model_spec_id": str(model_spec_id),
                                        "window_name": str(window_name),
                                    },
                                    filename="alpha_shadow_selection_gap_scorecard.parquet",
                                )
                            )
                        )
                notes = (
                    "Alpha shadow selection-gap scorecard materialized. "
                    f"summary_date={end_selection_date.isoformat()} rows={len(scorecard_frame)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return AlphaShadowSelectionGapScorecardResult(
                    run_id=run_context.run_id,
                    start_selection_date=start_selection_date,
                    end_selection_date=end_selection_date,
                    row_count=len(scorecard_frame),
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
                    notes="Alpha shadow selection-gap scorecard materialization failed.",
                    error_message=str(exc),
                )
                raise
