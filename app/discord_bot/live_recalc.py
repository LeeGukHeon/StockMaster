from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date

import duckdb
import pandas as pd

from app.common.artifacts import resolve_artifact_path
from app.features.builders.flow_features import build_flow_feature_frame
from app.features.builders.fundamentals_features import build_fundamentals_feature_frame
from app.features.builders.liquidity_features import build_liquidity_feature_frame
from app.features.builders.news_features import build_news_feature_frame
from app.features.builders.price_features import build_price_feature_frame
from app.features.builders.quality_features import build_data_quality_feature_frame
from app.features.feature_store import (
    _load_feature_symbol_frame,
    _load_investor_flow_history,
    _load_latest_fundamentals,
    _load_ohlcv_history,
    _load_recent_news,
    _register_symbol_stage,
    _unregister_symbol_stage,
    load_feature_matrix,
)
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ml.inference import (
    _resolve_training_run_for_inference,
    build_prediction_frame_from_training_run,
)
from app.ml.shadow import _load_regime_map
from app.selection.engine_v2 import build_selection_engine_v2_rankings
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables


@dataclass(frozen=True, slots=True)
class LiveRecalcResult:
    frame: pd.DataFrame
    mode: str
    note: str | None = None


def _is_read_only_conflict(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "different configuration",
            "could not set lock on file",
            "conflicting lock is held",
        )
    )


@contextmanager
def _direct_read_only_connection(settings: Settings):
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        bootstrap_core_tables(connection)
        yield connection
    finally:
        connection.close()


def _latest_workbench_as_of_date(connection) -> date | None:
    row = connection.execute(
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = ?
        """,
        [SELECTION_ENGINE_V2_VERSION],
    ).fetchone()
    if row is not None and row[0] is not None:
        return pd.Timestamp(row[0]).date()
    row = connection.execute("SELECT MAX(as_of_date) FROM fact_feature_snapshot").fetchone()
    if row is None or row[0] is None:
        return None
    return pd.Timestamp(row[0]).date()


def _build_workbench_live_feature_row(
    connection,
    *,
    as_of_date: date,
    symbol: str,
) -> pd.DataFrame:
    symbol_frame = _load_feature_symbol_frame(
        connection,
        as_of_date=as_of_date,
        symbols=[symbol],
        limit_symbols=None,
        market="ALL",
    )
    if symbol_frame.empty:
        return pd.DataFrame()

    _register_symbol_stage(connection, symbol_frame)
    try:
        ohlcv_history = _load_ohlcv_history(connection, as_of_date=as_of_date)
        latest_fundamentals = _load_latest_fundamentals(connection, as_of_date=as_of_date)
        investor_flow_history = _load_investor_flow_history(connection, as_of_date=as_of_date)
        recent_news = _load_recent_news(connection, as_of_date=as_of_date)
    finally:
        _unregister_symbol_stage(connection)

    latest_price_dates = (
        ohlcv_history.groupby("symbol", as_index=False)["trading_date"].max()
        if not ohlcv_history.empty
        else pd.DataFrame(columns=["symbol", "trading_date"])
    ).rename(columns={"trading_date": "latest_price_date"})
    latest_close = (
        ohlcv_history.loc[
            pd.to_datetime(ohlcv_history["trading_date"]).dt.date == as_of_date,
            ["symbol", "close", "market_cap"],
        ]
        if not ohlcv_history.empty
        else pd.DataFrame(columns=["symbol", "close", "market_cap"])
    )

    feature_matrix = (
        symbol_frame[["symbol", "company_name", "market"]]
        .merge(latest_price_dates, on="symbol", how="left")
        .merge(latest_close, on="symbol", how="left")
        .merge(build_price_feature_frame(ohlcv_history, as_of_date=as_of_date), on="symbol", how="left")
        .merge(
            build_liquidity_feature_frame(ohlcv_history, as_of_date=as_of_date),
            on="symbol",
            how="left",
        )
        .merge(
            build_fundamentals_feature_frame(latest_fundamentals, as_of_date=as_of_date),
            on="symbol",
            how="left",
        )
        .merge(
            build_flow_feature_frame(
                investor_flow_history,
                ohlcv_history=ohlcv_history,
                as_of_date=as_of_date,
            ),
            on="symbol",
            how="left",
        )
        .merge(build_news_feature_frame(recent_news, as_of_date=as_of_date), on="symbol", how="left")
    )

    feature_matrix["earnings_yield_proxy"] = feature_matrix["net_income_latest"] / feature_matrix[
        "market_cap"
    ].replace(0, pd.NA)
    feature_matrix["value_proxy_available_flag"] = (
        feature_matrix[
            [
                "earnings_yield_proxy",
                "low_debt_preference_proxy",
                "profitability_support_proxy",
            ]
        ]
        .notna()
        .any(axis=1)
        .astype(float)
    )
    feature_matrix["liquidity_rank_pct"] = 1.0
    quality_features = build_data_quality_feature_frame(feature_matrix, as_of_date=as_of_date)
    feature_matrix = feature_matrix.merge(
        quality_features,
        on="symbol",
        how="left",
        suffixes=("", "_dup"),
    )
    feature_matrix = feature_matrix.drop(
        columns=[column for column in feature_matrix.columns if column.endswith("_dup")]
    )
    feature_matrix.insert(0, "as_of_date", as_of_date)
    return feature_matrix


def _workbench_latest_reference_price(
    connection,
    *,
    symbol: str,
    as_of_date: date,
) -> tuple[date | None, float | None]:
    row = connection.execute(
        """
        SELECT trading_date, close
        FROM fact_daily_ohlcv
        WHERE symbol = ?
          AND trading_date <= ?
        ORDER BY trading_date DESC
        LIMIT 1
        """,
        [symbol, as_of_date],
    ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        return None, None
    return pd.Timestamp(row[0]).date(), float(row[1])


def _load_live_prediction_row(
    settings: Settings,
    connection,
    *,
    feature_row: pd.DataFrame,
    as_of_date: date,
    horizon: int,
) -> pd.DataFrame:
    training_run, active_alpha_model, training_run_source = _resolve_training_run_for_inference(
        connection,
        as_of_date=as_of_date,
        horizon=int(horizon),
    )
    if training_run is None or not training_run.get("artifact_uri"):
        return pd.DataFrame()
    resolved_artifact_path = resolve_artifact_path(settings, training_run.get("artifact_uri"))
    if resolved_artifact_path is None:
        return pd.DataFrame()
    resolved_training_run = dict(training_run)
    resolved_training_run["artifact_uri"] = str(resolved_artifact_path)
    result_frame, _ = build_prediction_frame_from_training_run(
        run_id="discord-live-analysis",
        as_of_date=as_of_date,
        horizon=int(horizon),
        feature_frame=feature_row,
        training_run=resolved_training_run,
        training_run_source=training_run_source,
        active_alpha_model_id=(
            active_alpha_model.get("active_alpha_model_id")
            if active_alpha_model is not None
            else None
        ),
        persist_member_predictions=False,
    )
    return result_frame


def compute_live_stock_recommendation(
    settings: Settings,
    *,
    symbol: str,
) -> LiveRecalcResult:
    if not settings.paths.duckdb_path.exists():
        return LiveRecalcResult(pd.DataFrame(), mode="unavailable", note="분석 DB가 없습니다.")

    normalized_symbol = str(symbol).zfill(6)
    try:
        with _direct_read_only_connection(settings) as connection:
            as_of_date = _latest_workbench_as_of_date(connection)
            if as_of_date is None:
                return LiveRecalcResult(pd.DataFrame(), mode="missing", note="기준일이 없습니다.")

            feature_context = load_feature_matrix(connection, as_of_date=as_of_date, market="ALL")
            if feature_context.empty:
                return LiveRecalcResult(pd.DataFrame(), mode="missing", note="특징값 스냅샷이 없습니다.")

            live_feature_row = _build_workbench_live_feature_row(
                connection,
                as_of_date=as_of_date,
                symbol=normalized_symbol,
            )
            if live_feature_row.empty:
                return LiveRecalcResult(pd.DataFrame(), mode="missing", note="종목 특징값을 만들지 못했습니다.")

            feature_context = feature_context.loc[
                feature_context["symbol"].astype(str).ne(normalized_symbol)
            ].copy()
            feature_matrix = pd.concat([feature_context, live_feature_row], ignore_index=True)

            prediction_frames_by_horizon: dict[int, pd.DataFrame] = {}
            live_prediction_rows: dict[int, pd.DataFrame] = {}
            for horizon in (1, 5):
                stored_prediction_frame = connection.execute(
                    """
                    SELECT
                        symbol,
                        expected_excess_return,
                        lower_band,
                        median_band,
                        upper_band,
                        uncertainty_score,
                        disagreement_score,
                        fallback_flag,
                        fallback_reason,
                        prediction_version,
                        member_count,
                        ensemble_weight_json,
                        source_notes_json
                    FROM fact_prediction
                    WHERE as_of_date = ?
                      AND horizon = ?
                      AND prediction_version = ?
                      AND ranking_version = ?
                    """,
                    [as_of_date, horizon, ALPHA_PREDICTION_VERSION, SELECTION_ENGINE_V2_VERSION],
                ).fetchdf()
                live_prediction_row = _load_live_prediction_row(
                    settings,
                    connection,
                    feature_row=live_feature_row,
                    as_of_date=as_of_date,
                    horizon=horizon,
                )
                live_prediction_rows[horizon] = live_prediction_row
                if not live_prediction_row.empty:
                    stored_prediction_frame = stored_prediction_frame.loc[
                        stored_prediction_frame["symbol"].astype(str).ne(normalized_symbol)
                    ].copy()
                    stored_prediction_frame = pd.concat(
                        [
                            stored_prediction_frame,
                            live_prediction_row[
                                [
                                    "symbol",
                                    "expected_excess_return",
                                    "lower_band",
                                    "median_band",
                                    "upper_band",
                                    "uncertainty_score",
                                    "disagreement_score",
                                    "fallback_flag",
                                    "fallback_reason",
                                    "prediction_version",
                                    "member_count",
                                    "ensemble_weight_json",
                                    "source_notes_json",
                                ]
                            ],
                        ],
                        ignore_index=True,
                    )
                prediction_frames_by_horizon[horizon] = stored_prediction_frame

            ranking_frames = build_selection_engine_v2_rankings(
                feature_matrix=feature_matrix,
                as_of_date=as_of_date,
                horizons=[1, 5],
                regime_map=_load_regime_map(connection, as_of_date=as_of_date),
                prediction_frames_by_horizon=prediction_frames_by_horizon,
                run_id="discord-live-analysis",
                settings=settings,
            )

            ranking_by_horizon: dict[int, pd.Series] = {}
            for frame in ranking_frames:
                symbol_row = frame.loc[frame["symbol"].astype(str) == normalized_symbol]
                if symbol_row.empty:
                    continue
                ranking_by_horizon[int(symbol_row["horizon"].iloc[0])] = symbol_row.iloc[0]
            if not ranking_by_horizon:
                return LiveRecalcResult(pd.DataFrame(), mode="missing", note="실시간 순위를 만들지 못했습니다.")

            reference_date, reference_price = _workbench_latest_reference_price(
                connection,
                symbol=normalized_symbol,
                as_of_date=as_of_date,
            )
            d5_prediction_row = None
            if 5 in live_prediction_rows and not live_prediction_rows[5].empty:
                d5_prediction_row = live_prediction_rows[5].iloc[0]
            expected = (
                None
                if d5_prediction_row is None or pd.isna(d5_prediction_row.get("expected_excess_return"))
                else float(d5_prediction_row["expected_excess_return"])
            )
            upper = (
                None
                if d5_prediction_row is None or pd.isna(d5_prediction_row.get("upper_band"))
                else float(d5_prediction_row["upper_band"])
            )
            lower = (
                None
                if d5_prediction_row is None or pd.isna(d5_prediction_row.get("lower_band"))
                else float(d5_prediction_row["lower_band"])
            )
            result = pd.DataFrame(
                [
                    {
                        "symbol": normalized_symbol,
                        "company_name": live_feature_row.iloc[0].get("company_name"),
                        "market": live_feature_row.iloc[0].get("market"),
                        "live_as_of_date": as_of_date,
                        "live_reference_date": reference_date,
                        "live_reference_price": reference_price,
                        "live_d1_selection_v2_value": ranking_by_horizon.get(1, {}).get("final_selection_value"),
                        "live_d1_selection_v2_grade": ranking_by_horizon.get(1, {}).get("grade"),
                        "live_d1_eligible_flag": ranking_by_horizon.get(1, {}).get("eligible_flag"),
                        "live_d1_report_candidate_flag": ranking_by_horizon.get(1, {}).get("report_candidate_flag"),
                        "live_d5_selection_v2_value": ranking_by_horizon.get(5, {}).get("final_selection_value"),
                        "live_d5_selection_v2_grade": ranking_by_horizon.get(5, {}).get("grade"),
                        "live_d5_eligible_flag": ranking_by_horizon.get(5, {}).get("eligible_flag"),
                        "live_d5_report_candidate_flag": ranking_by_horizon.get(5, {}).get("report_candidate_flag"),
                        "live_d5_expected_excess_return": expected,
                        "live_d5_target_price": None
                        if reference_price is None or expected is None
                        else reference_price * (1.0 + expected),
                        "live_d5_upper_target_price": None
                        if reference_price is None or upper is None
                        else reference_price * (1.0 + upper),
                        "live_d5_stop_price": None
                        if reference_price is None or lower is None
                        else reference_price * (1.0 + lower),
                    }
                ]
            )
            return LiveRecalcResult(result, mode="live")
    except (duckdb.ConnectionException, duckdb.IOException) as exc:
        if _is_read_only_conflict(exc):
            return LiveRecalcResult(
                pd.DataFrame(),
                mode="busy",
                note="배치가 DB를 점유 중이라 실시간 재계산 대신 최신 안정 스냅샷으로 안내합니다.",
            )
        raise
