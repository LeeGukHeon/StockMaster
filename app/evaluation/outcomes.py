from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.labels.forward_returns import LABEL_VERSION, build_forward_labels
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ranking.explanatory_score import (
    RANKING_VERSION as EXPLANATORY_RANKING_VERSION,
)
from app.ranking.explanatory_score import (
    materialize_explanatory_ranking,
)
from app.selection.calibration import PREDICTION_VERSION, calibrate_proxy_prediction_bands
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION, materialize_selection_engine_v1
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

DEFAULT_RANKING_VERSIONS = (
    SELECTION_ENGINE_VERSION,
    EXPLANATORY_RANKING_VERSION,
)
PREDICTION_LOOKBACK_DAYS = 60


@dataclass(slots=True)
class SelectionOutcomeMaterializationResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
    row_count: int
    matured_row_count: int
    pending_row_count: int
    artifact_paths: list[str]
    notes: str


def _normalize_ranking_versions(
    ranking_versions: list[str] | None,
) -> list[str]:
    if ranking_versions:
        values = [str(value) for value in ranking_versions if str(value)]
        return list(dict.fromkeys(values))
    return list(DEFAULT_RANKING_VERSIONS)


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


def _resolve_selection_dates_with_features(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
) -> list[date]:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        rows = connection.execute(
            """
            SELECT DISTINCT feature.as_of_date
            FROM fact_feature_snapshot AS feature
            JOIN fact_market_regime_snapshot AS regime
              ON feature.as_of_date = regime.as_of_date
             AND regime.market_scope = 'KR_ALL'
            WHERE feature.as_of_date BETWEEN ? AND ?
            ORDER BY feature.as_of_date
            """,
            [start_selection_date, end_selection_date],
        ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows]


def _resolve_prediction_lookback_start(
    settings: Settings,
    *,
    end_date: date,
    trading_days: int = PREDICTION_LOOKBACK_DAYS,
) -> date:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            """
            SELECT MIN(trading_date)
            FROM (
                SELECT trading_date
                FROM dim_trading_calendar
                WHERE trading_date <= ?
                  AND is_trading_day
                ORDER BY trading_date DESC
                LIMIT ?
            )
            """,
            [end_date, trading_days],
        ).fetchone()
    if row is None or row[0] is None:
        return end_date
    return pd.Timestamp(row[0]).date()


def _ensure_ranking_history(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    ranking_versions: list[str],
    symbols: list[str] | None,
    limit_symbols: int | None,
    market: str,
) -> list[date]:
    target_dates = _resolve_selection_dates_with_features(
        settings,
        start_selection_date=start_selection_date,
        end_selection_date=end_selection_date,
    )
    if not target_dates:
        return []

    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        missing_by_version: dict[str, list[date]] = {version: [] for version in ranking_versions}
        expected_horizon_count = len(horizons)
        for selection_dt in target_dates:
            for ranking_version in ranking_versions:
                existing_count = connection.execute(
                    """
                    SELECT COUNT(DISTINCT horizon)
                    FROM fact_ranking
                    WHERE as_of_date = ?
                      AND ranking_version = ?
                    """,
                    [selection_dt, ranking_version],
                ).fetchone()[0]
                if int(existing_count or 0) < expected_horizon_count:
                    missing_by_version[ranking_version].append(selection_dt)

    built_dates: list[date] = []
    for selection_dt in missing_by_version.get(EXPLANATORY_RANKING_VERSION, []):
        materialize_explanatory_ranking(
            settings,
            as_of_date=selection_dt,
            horizons=horizons,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
        )
        built_dates.append(selection_dt)
    for selection_dt in missing_by_version.get(SELECTION_ENGINE_VERSION, []):
        materialize_selection_engine_v1(
            settings,
            as_of_date=selection_dt,
            horizons=horizons,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
        )
        built_dates.append(selection_dt)
    for selection_dt in missing_by_version.get(SELECTION_ENGINE_V2_VERSION, []):
        from app.selection.engine_v2 import materialize_selection_engine_v2

        materialize_selection_engine_v2(
            settings,
            as_of_date=selection_dt,
            horizons=horizons,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
        )
        built_dates.append(selection_dt)
    return sorted(set(built_dates))


def _load_ranking_candidates(
    connection,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    ranking_versions: list[str],
    limit_symbols: int | None,
) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    version_placeholders = ",".join("?" for _ in ranking_versions)
    params: list[object] = [
        start_selection_date,
        end_selection_date,
        *horizons,
        *ranking_versions,
    ]
    limit_clause = ""
    if limit_symbols is not None and limit_symbols > 0:
        limit_clause = (
            "QUALIFY ROW_NUMBER() OVER ("
            "PARTITION BY as_of_date, horizon, ranking_version "
            "ORDER BY final_selection_value DESC, symbol"
            f") <= {int(limit_symbols)}"
        )
    return connection.execute(
        f"""
        SELECT
            as_of_date AS selection_date,
            symbol,
            horizon,
            ranking_version
        FROM fact_ranking
        WHERE as_of_date BETWEEN ? AND ?
          AND horizon IN ({horizon_placeholders})
          AND ranking_version IN ({version_placeholders})
        {limit_clause}
        ORDER BY as_of_date, ranking_version, horizon, symbol
        """,
        params,
    ).fetchdf()


def _ensure_prediction_history(
    settings: Settings,
    *,
    selection_dates: list[date],
    horizons: list[int],
) -> list[date]:
    if not selection_dates:
        return []

    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        missing_dates: list[date] = []
        expected_horizon_count = len(horizons)
        for selection_dt in selection_dates:
            existing_count = connection.execute(
                """
                SELECT COUNT(DISTINCT horizon)
                FROM fact_prediction
                WHERE as_of_date = ?
                  AND prediction_version = ?
                  AND ranking_version = ?
                """,
                [
                    selection_dt,
                    PREDICTION_VERSION,
                    SELECTION_ENGINE_VERSION,
                ],
            ).fetchone()[0]
            if int(existing_count or 0) < expected_horizon_count:
                missing_dates.append(selection_dt)

    built_dates: list[date] = []
    for selection_dt in missing_dates:
        lookback_start = _resolve_prediction_lookback_start(settings, end_date=selection_dt)
        try:
            calibrate_proxy_prediction_bands(
                settings,
                start_date=lookback_start,
                end_date=selection_dt,
                horizons=horizons,
            )
        except RuntimeError:
            continue
        built_dates.append(selection_dt)
    return built_dates


def _parse_score_payload(value: object) -> dict[str, object]:
    if value in {None, ""}:
        return {}
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


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


def upsert_selection_outcomes(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("selection_outcome_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_selection_outcome
        WHERE (selection_date, symbol, horizon, ranking_version) IN (
            SELECT selection_date, symbol, horizon, ranking_version
            FROM selection_outcome_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_selection_outcome (
            selection_date,
            evaluation_date,
            symbol,
            market,
            horizon,
            ranking_version,
            selection_engine_version,
            grade,
            grade_detail,
            report_candidate_flag,
            eligible_flag,
            final_selection_value,
            selection_percentile,
            expected_excess_return_at_selection,
            lower_band_at_selection,
            median_band_at_selection,
            upper_band_at_selection,
            uncertainty_score_at_selection,
            disagreement_score_at_selection,
            implementation_penalty_at_selection,
            fallback_flag_at_selection,
            fallback_reason_at_selection,
            prediction_version_at_selection,
            regime_label_at_selection,
            top_reason_tags_json,
            risk_flags_json,
            entry_trade_date,
            exit_trade_date,
            realized_return,
            realized_excess_return,
            prediction_error,
            direction_hit_flag,
            raw_positive_flag,
            band_available_flag,
            band_status,
            in_band_flag,
            above_upper_flag,
            below_lower_flag,
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
            ranking_version,
            selection_engine_version,
            grade,
            grade_detail,
            report_candidate_flag,
            eligible_flag,
            final_selection_value,
            selection_percentile,
            expected_excess_return_at_selection,
            lower_band_at_selection,
            median_band_at_selection,
            upper_band_at_selection,
            uncertainty_score_at_selection,
            disagreement_score_at_selection,
            implementation_penalty_at_selection,
            fallback_flag_at_selection,
            fallback_reason_at_selection,
            prediction_version_at_selection,
            regime_label_at_selection,
            top_reason_tags_json,
            risk_flags_json,
            entry_trade_date,
            exit_trade_date,
            realized_return,
            realized_excess_return,
            prediction_error,
            direction_hit_flag,
            raw_positive_flag,
            band_available_flag,
            band_status,
            in_band_flag,
            above_upper_flag,
            below_lower_flag,
            outcome_status,
            source_label_version,
            evaluation_run_id,
            created_at,
            updated_at
        FROM selection_outcome_stage
        """
    )
    connection.unregister("selection_outcome_stage")


def materialize_selection_outcomes(
    settings: Settings,
    *,
    selection_date: date | None = None,
    start_selection_date: date | None = None,
    end_selection_date: date | None = None,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    ranking_versions: list[str] | None = None,
) -> SelectionOutcomeMaterializationResult:
    ensure_storage_layout(settings)
    ranking_versions = _normalize_ranking_versions(ranking_versions)
    start_dt, end_dt = _resolve_target_selection_dates(
        selection_date,
        start_selection_date,
        end_selection_date,
    )
    _ensure_ranking_history(
        settings,
        start_selection_date=start_dt,
        end_selection_date=end_dt,
        horizons=horizons,
        ranking_versions=ranking_versions,
        symbols=symbols,
        limit_symbols=limit_symbols,
        market=market,
    )

    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        candidate_rows = _load_ranking_candidates(
            connection,
            start_selection_date=start_dt,
            end_selection_date=end_dt,
            horizons=horizons,
            ranking_versions=ranking_versions,
            limit_symbols=limit_symbols,
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
        label_start_date = start_dt
        selection_prediction_dates = sorted(
            candidate_rows.loc[
                candidate_rows["ranking_version"] == SELECTION_ENGINE_VERSION,
                "selection_date",
            ]
            .dropna()
            .map(lambda value: pd.Timestamp(value).date())
            .unique()
            .tolist()
        )
        if selection_prediction_dates:
            label_start_date = min(
                label_start_date,
                _resolve_prediction_lookback_start(
                    settings,
                    end_date=max(selection_prediction_dates),
                ),
            )
        build_forward_labels(
            settings,
            start_date=label_start_date,
            end_date=end_dt,
            horizons=horizons,
            symbols=target_symbols,
            market=market,
        )
        _ensure_prediction_history(
            settings,
            selection_dates=selection_prediction_dates,
            horizons=horizons,
        )

    with activate_run_context(
        "materialize_selection_outcomes",
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
                    "fact_ranking",
                    "fact_prediction",
                    "fact_forward_return_label",
                    "dim_symbol",
                ],
                notes=(
                    "Freeze ranking/prediction snapshots against realized outcomes. "
                    f"range={start_dt.isoformat()}..{end_dt.isoformat()} horizons={horizons}"
                ),
                ranking_version=",".join(ranking_versions),
            )
            try:
                if candidate_rows.empty:
                    notes = (
                        "No ranking rows were available for selection outcome materialization. "
                        f"range={start_dt.isoformat()}..{end_dt.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=",".join(ranking_versions),
                    )
                    return SelectionOutcomeMaterializationResult(
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
                version_placeholders = ",".join("?" for _ in ranking_versions)
                params: list[object] = [
                    SELECTION_ENGINE_VERSION,
                    PREDICTION_VERSION,
                    SELECTION_ENGINE_V2_VERSION,
                    ALPHA_PREDICTION_VERSION,
                    start_dt,
                    end_dt,
                    *horizons,
                    *ranking_versions,
                ]
                symbol_clause = ""
                if target_symbols:
                    symbol_placeholders = ",".join("?" for _ in target_symbols)
                    symbol_clause = f" AND ranking.symbol IN ({symbol_placeholders})"
                    params.extend(target_symbols)
                limit_clause = ""
                if limit_symbols is not None and limit_symbols > 0:
                    limit_clause = (
                        "QUALIFY ROW_NUMBER() OVER ("
                        "PARTITION BY ranking.as_of_date, ranking.horizon, ranking.ranking_version "
                        "ORDER BY ranking.final_selection_value DESC, ranking.symbol"
                        f") <= {int(limit_symbols)}"
                    )

                joined = connection.execute(
                    f"""
                    SELECT
                        ranking.as_of_date AS selection_date,
                        label.exit_date AS evaluation_date,
                        ranking.symbol,
                        symbol_meta.market,
                        ranking.horizon,
                        ranking.ranking_version,
                        ranking.grade,
                        ranking.eligible_flag,
                        ranking.final_selection_value,
                        ranking.final_selection_rank_pct AS selection_percentile,
                        ranking.explanatory_score_json,
                        ranking.top_reason_tags_json,
                        ranking.risk_flags_json,
                        ranking.eligibility_notes_json,
                        ranking.regime_state,
                        prediction.prediction_version AS prediction_version_at_selection,
                        prediction.expected_excess_return AS expected_excess_return_at_selection,
                        prediction.lower_band AS lower_band_at_selection,
                        prediction.median_band AS median_band_at_selection,
                        prediction.upper_band AS upper_band_at_selection,
                        prediction.uncertainty_score AS uncertainty_score_at_selection,
                        prediction.disagreement_score AS disagreement_score_at_selection,
                        prediction.fallback_flag AS fallback_flag_at_selection,
                        prediction.fallback_reason AS fallback_reason_at_selection,
                        label.entry_date AS entry_trade_date,
                        label.exit_date AS exit_trade_date,
                        label.gross_forward_return AS realized_return,
                        label.excess_forward_return AS realized_excess_return,
                        label.label_available_flag,
                        label.exclusion_reason
                    FROM fact_ranking AS ranking
                    JOIN dim_symbol AS symbol_meta
                      ON ranking.symbol = symbol_meta.symbol
                    LEFT JOIN fact_prediction AS prediction
                      ON ranking.as_of_date = prediction.as_of_date
                     AND ranking.symbol = prediction.symbol
                     AND ranking.horizon = prediction.horizon
                     AND ranking.ranking_version = prediction.ranking_version
                     AND prediction.prediction_version = CASE
                        WHEN ranking.ranking_version = ? THEN ?
                        WHEN ranking.ranking_version = ? THEN ?
                        ELSE NULL
                     END
                    LEFT JOIN fact_forward_return_label AS label
                      ON ranking.as_of_date = label.as_of_date
                     AND ranking.symbol = label.symbol
                     AND ranking.horizon = label.horizon
                    WHERE ranking.as_of_date BETWEEN ? AND ?
                      AND ranking.horizon IN ({horizon_placeholders})
                      AND ranking.ranking_version IN ({version_placeholders})
                      {symbol_clause}
                    {limit_clause}
                    ORDER BY
                        ranking.as_of_date,
                        ranking.ranking_version,
                        ranking.horizon,
                        ranking.symbol
                    """,
                    params,
                ).fetchdf()
                if joined.empty:
                    notes = (
                        "No joined ranking/label rows were available for selection outcomes. "
                        f"range={start_dt.isoformat()}..{end_dt.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=",".join(ranking_versions),
                    )
                    return SelectionOutcomeMaterializationResult(
                        run_id=run_context.run_id,
                        start_selection_date=start_dt,
                        end_selection_date=end_dt,
                        row_count=0,
                        matured_row_count=0,
                        pending_row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                score_payloads = joined["explanatory_score_json"].map(_parse_score_payload)
                joined["selection_engine_version"] = joined["ranking_version"]
                joined["grade_detail"] = joined["eligibility_notes_json"]
                joined["report_candidate_flag"] = joined["eligible_flag"].fillna(False).astype(
                    bool
                ) & joined["selection_percentile"].fillna(0.0).ge(0.85)
                missing_uncertainty = joined["uncertainty_score_at_selection"].isna()
                joined.loc[missing_uncertainty, "uncertainty_score_at_selection"] = (
                    score_payloads.loc[missing_uncertainty].map(
                        lambda payload: payload.get("uncertainty_proxy_score")
                    )
                )
                joined["implementation_penalty_at_selection"] = score_payloads.map(
                    lambda payload: payload.get("implementation_penalty_score")
                )
                joined["regime_label_at_selection"] = joined["regime_state"]
                joined["prediction_error"] = (
                    joined["realized_excess_return"] - joined["expected_excess_return_at_selection"]
                )
                expected_positive = joined["expected_excess_return_at_selection"].gt(0)
                realized_positive = joined["realized_excess_return"].gt(0)
                joined["direction_hit_flag"] = (
                    joined["expected_excess_return_at_selection"].notna()
                    & joined["realized_excess_return"].notna()
                    & (expected_positive == realized_positive)
                )
                joined["direction_hit_flag"] = joined["direction_hit_flag"].where(
                    joined["expected_excess_return_at_selection"].notna()
                    & joined["realized_excess_return"].notna(),
                    pd.NA,
                )
                joined["raw_positive_flag"] = joined["realized_return"].gt(0)
                joined["raw_positive_flag"] = joined["raw_positive_flag"].where(
                    joined["realized_return"].notna(),
                    pd.NA,
                )
                joined["band_available_flag"] = (
                    joined["lower_band_at_selection"].notna()
                    & joined["upper_band_at_selection"].notna()
                )
                joined["in_band_flag"] = (
                    joined["band_available_flag"]
                    & joined["realized_excess_return"].notna()
                    & joined["realized_excess_return"].ge(joined["lower_band_at_selection"])
                    & joined["realized_excess_return"].le(joined["upper_band_at_selection"])
                )
                joined["above_upper_flag"] = (
                    joined["band_available_flag"]
                    & joined["realized_excess_return"].notna()
                    & joined["realized_excess_return"].gt(joined["upper_band_at_selection"])
                )
                joined["below_lower_flag"] = (
                    joined["band_available_flag"]
                    & joined["realized_excess_return"].notna()
                    & joined["realized_excess_return"].lt(joined["lower_band_at_selection"])
                )
                joined["outcome_status"] = joined.apply(
                    lambda row: _derive_outcome_status(
                        row["label_available_flag"],
                        row["exclusion_reason"],
                    ),
                    axis=1,
                )
                joined["band_status"] = "band_missing"
                joined.loc[joined["outcome_status"] == "pending", "band_status"] = "label_pending"
                joined.loc[
                    joined["outcome_status"] == "unavailable",
                    "band_status",
                ] = "label_unavailable"
                joined.loc[joined["in_band_flag"], "band_status"] = "in_band"
                joined.loc[joined["above_upper_flag"], "band_status"] = "above_upper"
                joined.loc[joined["below_lower_flag"], "band_status"] = "below_lower"
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
                        "ranking_version",
                        "selection_engine_version",
                        "grade",
                        "grade_detail",
                        "report_candidate_flag",
                        "eligible_flag",
                        "final_selection_value",
                        "selection_percentile",
                        "expected_excess_return_at_selection",
                        "lower_band_at_selection",
                        "median_band_at_selection",
                        "upper_band_at_selection",
                        "uncertainty_score_at_selection",
                        "disagreement_score_at_selection",
                        "implementation_penalty_at_selection",
                        "fallback_flag_at_selection",
                        "fallback_reason_at_selection",
                        "prediction_version_at_selection",
                        "regime_label_at_selection",
                        "top_reason_tags_json",
                        "risk_flags_json",
                        "entry_trade_date",
                        "exit_trade_date",
                        "realized_return",
                        "realized_excess_return",
                        "prediction_error",
                        "direction_hit_flag",
                        "raw_positive_flag",
                        "band_available_flag",
                        "band_status",
                        "in_band_flag",
                        "above_upper_flag",
                        "below_lower_flag",
                        "outcome_status",
                        "source_label_version",
                        "evaluation_run_id",
                        "created_at",
                        "updated_at",
                    ]
                ].copy()
                upsert_selection_outcomes(connection, outcome_frame)

                artifact_paths: list[str] = []
                for (
                    selection_dt,
                    ranking_version,
                    horizon,
                ), partition_frame in outcome_frame.groupby(
                    ["selection_date", "ranking_version", "horizon"],
                    sort=True,
                ):
                    artifact_paths.append(
                        str(
                            write_parquet(
                                partition_frame,
                                base_dir=settings.paths.curated_dir,
                                dataset="evaluation/selection_outcomes",
                                partitions={
                                    "selection_date": pd.Timestamp(selection_dt).date().isoformat(),
                                    "ranking_version": str(ranking_version),
                                    "horizon": str(int(horizon)),
                                },
                                filename="selection_outcomes.parquet",
                            )
                        )
                    )

                matured_row_count = int(outcome_frame["outcome_status"].eq("matured").sum())
                pending_row_count = int(outcome_frame["outcome_status"].eq("pending").sum())
                notes = (
                    "Selection outcomes materialized. "
                    f"range={start_dt.isoformat()}..{end_dt.isoformat()}, "
                    f"rows={len(outcome_frame)}, matured={matured_row_count}, "
                    f"pending={pending_row_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=",".join(ranking_versions),
                )
                return SelectionOutcomeMaterializationResult(
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
                    notes=(
                        "Selection outcome materialization failed. "
                        f"range={start_dt.isoformat()}..{end_dt.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=",".join(ranking_versions),
                )
                raise
