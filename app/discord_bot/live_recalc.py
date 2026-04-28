from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, time

import duckdb
import pandas as pd

from app.common.artifacts import resolve_artifact_path
from app.common.time import now_local
from app.features.builders.flow_features import build_flow_feature_frame
from app.features.builders.fundamentals_features import build_fundamentals_feature_frame
from app.features.builders.liquidity_features import build_liquidity_feature_frame
from app.features.builders.news_features import build_news_feature_frame
from app.features.builders.price_features import build_price_feature_frame
from app.features.builders.quality_features import build_data_quality_feature_frame
from app.features.constants import FEATURE_NAMES
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
from app.features.normalization import compute_group_rank_pct
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ml.inference import (
    _resolve_training_run_for_inference,
    build_prediction_frame_from_training_run,
)
from app.ml.shadow import _load_regime_map
from app.recommendation.buyability import buyability_priority_score
from app.recommendation.judgement import classify_recommendation, load_score_band_evidence
from app.selection.engine_v2 import build_selection_engine_v2_rankings
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables


@dataclass(frozen=True, slots=True)
class LiveRecalcResult:
    frame: pd.DataFrame
    mode: str
    note: str | None = None


MARKET_OPEN_TIME = time(9, 0)
MARKET_CLOSE_TIME = time(15, 30)


def _json_list(value: object) -> list[str]:
    if value in (None, "", "-"):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip()]


def _json_dict(value: object) -> dict[str, object]:
    if value in (None, "", "-"):
        return {}
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _derive_invalidation_conditions(risk_flags: list[str]) -> list[str]:
    conditions: list[str] = []
    for risk_flag in risk_flags:
        if risk_flag == "high_realized_volatility":
            conditions.append("변동성이 더 커지면 선행 신호보다 잡음이 커질 수 있습니다.")
        elif risk_flag == "large_recent_drawdown":
            conditions.append(
                "최근 급락이 이어지면 반등 가설보다 추세 훼손 가능성을 우선 봐야 합니다."
            )
        elif risk_flag in {"prediction_error_bucket_high", "model_uncertainty_high"}:
            conditions.append("고예측 구간은 과거 오차가 커서 분할·관찰 기준으로만 봅니다.")
        elif risk_flag == "model_disagreement_high":
            conditions.append("앙상블 판단 차이가 커서 단독 신호로 과신하지 않습니다.")
        elif risk_flag == "model_joint_instability_high":
            conditions.append("고예측 오차와 모델 이견이 겹치면 매수 후보에서 제외합니다.")
        elif risk_flag == "crowding_risk":
            conditions.append("거래 과열이 심해지면 D+1 선행 우위가 빠르게 약해질 수 있습니다.")
    if not conditions:
        conditions.append("거래대금이 식거나 가격 추세가 꺾이면 즉시 재평가합니다.")
    return conditions[:3]


def build_live_analysis_payload(
    snapshot_payload: dict[str, object],
    live_result: LiveRecalcResult,
    *,
    quote_timestamp_or_basis: str,
    news_basis: str,
) -> dict[str, object]:
    live_row = live_result.frame.iloc[0] if not live_result.frame.empty else None
    source_precedence = ["live_recalc", "snapshot", "quote"] if live_row is not None else [
        "snapshot",
        "quote",
    ]
    degradation_mode = (
        "none" if live_result.mode == "live" and live_row is not None else str(live_result.mode)
    )
    d1_grade = (
        live_row.get("live_d1_selection_v2_grade")
        if live_row is not None
        else snapshot_payload.get("d1_grade")
    )
    d5_grade = (
        live_row.get("live_d5_selection_v2_grade")
        if live_row is not None
        else snapshot_payload.get("d5_grade")
    )
    d5_expected = (
        live_row.get("live_d5_expected_excess_return")
        if live_row is not None
        else snapshot_payload.get("d5_expected_excess_return")
    )
    d1_reasons = _json_list(
        None if live_row is None else live_row.get("live_d1_top_reason_tags_json")
    )
    d5_reasons = _json_list(
        None if live_row is None else live_row.get("live_d5_top_reason_tags_json")
    ) or _json_list(snapshot_payload.get("d5_reason_tags"))
    risk_flags = list(
        dict.fromkeys(
            _json_list(None if live_row is None else live_row.get("live_d1_risk_flags_json"))
            + (
                _json_list(None if live_row is None else live_row.get("live_d5_risk_flags_json"))
                or _json_list(snapshot_payload.get("risk_flags"))
            )
        )
    )
    d1_explanatory = _json_dict(
        None if live_row is None else live_row.get("live_d1_explanatory_score_json")
    )
    d5_explanatory = _json_dict(
        None if live_row is None else live_row.get("live_d5_explanatory_score_json")
    )
    why_reasons = d1_reasons[:1] + d5_reasons[:1]
    if why_reasons:
        why_now = " · ".join(why_reasons)
    elif live_result.note:
        why_now = str(live_result.note)
    else:
        why_now = "실시간 선행 신호는 제한적이며 최신 안정 스냅샷을 우선 참고합니다."
    signal_decomposition = {
        "price": {
            "d1_trend_momentum_score": d1_explanatory.get("trend_momentum_score"),
            "d5_trend_momentum_score": d5_explanatory.get("trend_momentum_score"),
            "d1_relative_alpha_score": d1_explanatory.get("relative_alpha_score"),
            "d5_relative_alpha_score": d5_explanatory.get("relative_alpha_score"),
        },
        "flow": {
            "d1_flow_score": d1_explanatory.get("flow_score"),
            "d5_flow_score": d5_explanatory.get("flow_score"),
            "d5_flow_persistence_score": d5_explanatory.get("flow_persistence_score"),
        },
        "crowding_risk": {
            "d1_crowding_penalty_score": d1_explanatory.get("crowding_penalty_score"),
            "d5_crowding_penalty_score": d5_explanatory.get("crowding_penalty_score"),
            "d5_risk_penalty_score": d5_explanatory.get("risk_penalty_score"),
        },
    }
    return {
        "mode": live_result.mode,
        "note": live_result.note,
        "source_precedence": source_precedence,
        "degradation_mode": degradation_mode,
        "snapshot_reused_flag": live_row is None,
        "d1_grade": d1_grade,
        "d5_grade": d5_grade,
        "d5_expected_excess_return": d5_expected,
        "d5_final_selection_value": (
            None if live_row is None else live_row.get("live_d5_selection_v2_value")
        )
        or snapshot_payload.get("d5_final_selection_value"),
        "d5_buyability_priority_score": (
            None if live_row is None else live_row.get("live_d5_buyability_priority_score")
        )
        or snapshot_payload.get("buyability_priority_score"),
        "d5_judgement_label": (
            None if live_row is None else live_row.get("live_d5_judgement_label")
        )
        or snapshot_payload.get("d5_judgement_label"),
        "d5_judgement_summary": (
            None if live_row is None else live_row.get("live_d5_judgement_summary")
        )
        or snapshot_payload.get("d5_judgement_summary"),
        "ret_5d": snapshot_payload.get("ret_5d"),
        "d1_head_spec_id": (None if live_row is None else live_row.get("live_d1_model_spec_id"))
        or snapshot_payload.get("d1_model_spec_id"),
        "d5_head_spec_id": (None if live_row is None else live_row.get("live_d5_model_spec_id"))
        or snapshot_payload.get("d5_model_spec_id"),
        "d1_active_alpha_model_id": (
            None if live_row is None else live_row.get("live_d1_active_alpha_model_id")
        )
        or snapshot_payload.get("d1_active_alpha_model_id"),
        "d5_active_alpha_model_id": (
            None if live_row is None else live_row.get("live_d5_active_alpha_model_id")
        )
        or snapshot_payload.get("d5_active_alpha_model_id"),
        "why_now": why_now,
        "signal_decomposition": signal_decomposition,
        "risk_flags": risk_flags,
        "invalidation_conditions": _derive_invalidation_conditions(risk_flags),
        "quote_timestamp_or_basis": quote_timestamp_or_basis,
        "news_basis": news_basis,
        "d1_reason_tags": d1_reasons,
        "d5_reason_tags": d5_reasons,
    }


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


def _is_regular_trading_session(settings: Settings, connection, *, as_of_date: date) -> bool:
    now_ts = now_local(settings.app.timezone)
    if now_ts.date() != as_of_date:
        return False
    calendar_row = connection.execute(
        """
        SELECT is_trading_day
        FROM dim_trading_calendar
        WHERE trading_date = ?
        """,
        [now_ts.date()],
    ).fetchone()
    if calendar_row is not None and not bool(calendar_row[0]):
        return False
    return MARKET_OPEN_TIME <= now_ts.time() <= MARKET_CLOSE_TIME


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
        .merge(
            build_price_feature_frame(ohlcv_history, as_of_date=as_of_date), on="symbol", how="left"
        )
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
        .merge(
            build_news_feature_frame(recent_news, as_of_date=as_of_date), on="symbol", how="left"
        )
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


def _refresh_live_rank_features(feature_matrix: pd.DataFrame) -> pd.DataFrame:
    if feature_matrix.empty or "market" not in feature_matrix.columns:
        return feature_matrix
    refreshed = feature_matrix.copy()
    for feature_name in FEATURE_NAMES:
        if feature_name not in refreshed.columns:
            continue
        refreshed[f"{feature_name}_rank_pct"] = compute_group_rank_pct(
            refreshed,
            column=feature_name,
        )
    return refreshed


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
            if not _is_regular_trading_session(settings, connection, as_of_date=as_of_date):
                return LiveRecalcResult(
                    pd.DataFrame(),
                    mode="closed",
                    note="비거래시간이라 장마감 추천 기준으로 안내합니다.",
                )

            feature_context = load_feature_matrix(connection, as_of_date=as_of_date, market="ALL")
            if feature_context.empty:
                return LiveRecalcResult(
                    pd.DataFrame(), mode="missing", note="특징값 스냅샷이 없습니다."
                )

            live_feature_row = _build_workbench_live_feature_row(
                connection,
                as_of_date=as_of_date,
                symbol=normalized_symbol,
            )
            if live_feature_row.empty:
                return LiveRecalcResult(
                    pd.DataFrame(), mode="missing", note="종목 특징값을 만들지 못했습니다."
                )

            feature_context = feature_context.loc[
                feature_context["symbol"].astype(str).ne(normalized_symbol)
            ].copy()
            feature_matrix = pd.concat([feature_context, live_feature_row], ignore_index=True)
            feature_matrix = _refresh_live_rank_features(feature_matrix)

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
                if int(frame["horizon"].iloc[0]) == 5:
                    frame = frame.copy()
                    frame["d5_selection_rank"] = (
                        pd.to_numeric(
                            frame["final_selection_value"],
                            errors="coerce",
                        )
                        .rank(ascending=False, method="first")
                        .astype("Int64")
                    )
                symbol_row = frame.loc[frame["symbol"].astype(str) == normalized_symbol]
                if symbol_row.empty:
                    continue
                ranking_by_horizon[int(symbol_row["horizon"].iloc[0])] = symbol_row.iloc[0]
            if not ranking_by_horizon:
                return LiveRecalcResult(
                    pd.DataFrame(), mode="missing", note="실시간 순위를 만들지 못했습니다."
                )

            reference_date, reference_price = _workbench_latest_reference_price(
                connection,
                symbol=normalized_symbol,
                as_of_date=as_of_date,
            )
            d1_prediction_row = None
            if 1 in live_prediction_rows and not live_prediction_rows[1].empty:
                d1_prediction_row = live_prediction_rows[1].iloc[0]
            d5_prediction_row = None
            if 5 in live_prediction_rows and not live_prediction_rows[5].empty:
                d5_prediction_row = live_prediction_rows[5].iloc[0]
            d5_risk_flags = _json_list(ranking_by_horizon.get(5, {}).get("risk_flags_json"))
            d5_evidence = load_score_band_evidence(
                connection,
                horizon=5,
                ranking_version=SELECTION_ENGINE_V2_VERSION,
            )
            d5_buyability_priority_score = (
                None
                if d5_prediction_row is None
                else buyability_priority_score(
                    expected_excess_return=d5_prediction_row.get("expected_excess_return"),
                    uncertainty_score=ranking_by_horizon.get(5, {}).get("uncertainty_score"),
                    disagreement_score=ranking_by_horizon.get(5, {}).get("disagreement_score"),
                )
            )
            d5_judgement = classify_recommendation(
                final_selection_value=ranking_by_horizon.get(5, {}).get("final_selection_value"),
                expected_excess_return=None
                if d5_prediction_row is None
                else d5_prediction_row.get("expected_excess_return"),
                risk_flags=d5_risk_flags,
                evidence_by_band=d5_evidence,
                candidate_selected=bool(
                    ranking_by_horizon.get(5, {}).get("report_candidate_flag", False)
                ),
                candidate_rank=ranking_by_horizon.get(5, {}).get("d5_selection_rank"),
                buyability_priority_score=d5_buyability_priority_score,
            )
            expected = (
                None
                if d5_prediction_row is None
                or pd.isna(d5_prediction_row.get("expected_excess_return"))
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
                        "live_d1_selection_v2_value": ranking_by_horizon.get(1, {}).get(
                            "final_selection_value"
                        ),
                        "live_d1_selection_v2_grade": ranking_by_horizon.get(1, {}).get("grade"),
                        "live_d1_eligible_flag": ranking_by_horizon.get(1, {}).get("eligible_flag"),
                        "live_d1_report_candidate_flag": ranking_by_horizon.get(1, {}).get(
                            "report_candidate_flag"
                        ),
                        "live_d1_model_spec_id": None
                        if d1_prediction_row is None
                        else d1_prediction_row.get("model_spec_id"),
                        "live_d1_active_alpha_model_id": None
                        if d1_prediction_row is None
                        else d1_prediction_row.get("active_alpha_model_id"),
                        "live_d1_top_reason_tags_json": ranking_by_horizon.get(1, {}).get(
                            "top_reason_tags_json"
                        ),
                        "live_d1_risk_flags_json": ranking_by_horizon.get(1, {}).get(
                            "risk_flags_json"
                        ),
                        "live_d1_explanatory_score_json": ranking_by_horizon.get(1, {}).get(
                            "explanatory_score_json"
                        ),
                        "live_d5_selection_v2_value": ranking_by_horizon.get(5, {}).get(
                            "final_selection_value"
                        ),
                        "live_d5_selection_v2_grade": ranking_by_horizon.get(5, {}).get("grade"),
                        "live_d5_eligible_flag": ranking_by_horizon.get(5, {}).get("eligible_flag"),
                        "live_d5_report_candidate_flag": ranking_by_horizon.get(5, {}).get(
                            "report_candidate_flag"
                        ),
                        "live_d5_model_spec_id": None
                        if d5_prediction_row is None
                        else d5_prediction_row.get("model_spec_id"),
                        "live_d5_active_alpha_model_id": None
                        if d5_prediction_row is None
                        else d5_prediction_row.get("active_alpha_model_id"),
                        "live_d5_top_reason_tags_json": ranking_by_horizon.get(5, {}).get(
                            "top_reason_tags_json"
                        ),
                        "live_d5_risk_flags_json": ranking_by_horizon.get(5, {}).get(
                            "risk_flags_json"
                        ),
                        "live_d5_explanatory_score_json": ranking_by_horizon.get(5, {}).get(
                            "explanatory_score_json"
                        ),
                        "live_d5_expected_excess_return": expected,
                        "live_d5_buyability_priority_score": d5_buyability_priority_score,
                        "live_d5_judgement_label": d5_judgement.label,
                        "live_d5_judgement_summary": d5_judgement.summary,
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
