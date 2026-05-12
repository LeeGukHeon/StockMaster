from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.reference.industry_grouping import add_industry_group_columns
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

SWING_3_5D_VERSION = "stockmaster_3_5d_swing_v2_ml"
SWING_3_5D_OUTPUT_DIR = "swing_3_5d"


@dataclass(frozen=True, slots=True)
class Swing35DConfig:
    price_min_krw: float = 1_000.0
    market_cap_min_krw: float = 100_000_000_000.0
    avg_turnover_20_min_krw: float = 2_000_000_000.0
    median_turnover_20_min_krw: float = 1_500_000_000.0
    avg_volume_20_min: float = 50_000.0
    min_history_days: int = 120
    rule_score_min: float = 70.0
    recommendation_threshold: float = 70.0
    high_confidence_threshold: float = 80.0
    min_ml_probability: float = 0.50
    high_confidence_ml_probability: float = 0.58
    weak_market_threshold_add: float = 0.0
    ml_weight: float = 0.35
    rule_weight: float = 0.40
    entry_weight: float = 0.25
    sector_weight: float = 0.10
    max_reversal_ratio: float = 0.20
    max_candidates_strong: int = 15
    max_candidates_neutral: int = 8
    max_candidates_weak: int = 3


@dataclass(slots=True)
class Swing35DMaterializationResult:
    run_id: str
    as_of_date: date
    row_count: int
    candidate_count: int
    artifact_paths: list[str]
    notes: str


def _safe_numeric(series: pd.Series | object, *, index: pd.Index | None = None) -> pd.Series:
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce")
    if index is not None:
        return pd.Series(series, index=index, dtype="float64")
    return pd.to_numeric(pd.Series([series]), errors="coerce")


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return _safe_numeric(numerator) / _safe_numeric(denominator).replace(0, np.nan)


def _clip_score(series: pd.Series, maximum: float) -> pd.Series:
    return _safe_numeric(series).fillna(0.0).clip(lower=0.0, upper=float(maximum))


def _bool_column(frame: pd.DataFrame, column: str, *, default: bool = False) -> pd.Series:
    values = frame.get(column)
    if values is None:
        return pd.Series(default, index=frame.index, dtype=bool)
    return values.astype("boolean").fillna(default).astype(bool)


def _rsi(close: pd.Series, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0).rolling(window, min_periods=window).mean()
    loss = (-delta.clip(upper=0.0)).rolling(window, min_periods=window).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(100.0).where(loss.ne(0), 100.0)


def _rolling_percentile_of_last(series: pd.Series, window: int) -> pd.Series:
    def _percentile(values) -> float:
        current = values[-1]
        if pd.isna(current):
            return float("nan")
        valid = pd.Series(values).dropna()
        if valid.empty:
            return float("nan")
        return float((valid <= current).mean())

    return series.rolling(window, min_periods=min(20, window)).apply(_percentile, raw=True)


def _count_consecutive_up(close: pd.Series) -> pd.Series:
    up = close.diff().gt(0)
    counts: list[int] = []
    current = 0
    for value in up.tolist():
        current = current + 1 if bool(value) else 0
        counts.append(current)
    return pd.Series(counts, index=close.index, dtype="float64")


def _compute_symbol_features(history: pd.DataFrame, *, as_of_date: date) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame()
    working = history.sort_values(["symbol", "trading_date"]).copy()
    working["symbol"] = working["symbol"].astype(str).str.zfill(6)
    working["trading_date"] = pd.to_datetime(working["trading_date"]).dt.date
    for column in ("open", "high", "low", "close", "volume", "turnover_value", "market_cap"):
        working[column] = _safe_numeric(working[column])
    working["turnover_effective"] = working["turnover_value"].fillna(
        working["close"] * working["volume"]
    )
    group = working.groupby("symbol", group_keys=False)
    working["history_days"] = group.cumcount() + 1

    for window in (5, 20, 60, 120):
        working[f"ma{window}"] = group["close"].transform(
            lambda series, w=window: series.rolling(w, min_periods=w).mean()
        )
        working[f"dist_ma{window}"] = working["close"] / working[f"ma{window}"] - 1.0
    working["ma20_slope_5"] = working["ma20"] / group["ma20"].shift(5) - 1.0
    working["ma60_slope_20"] = working["ma60"] / group["ma60"].shift(20) - 1.0
    working["ma120_slope_20"] = working["ma120"] / group["ma120"].shift(20) - 1.0
    working["ma5_prev"] = group["ma5"].shift(1)
    working["ma20_prev"] = group["ma20"].shift(1)
    working["ma5_cross_ma20_up"] = (
        working["ma5"].gt(working["ma20"]) & working["ma5_prev"].le(working["ma20_prev"])
    )
    working["ma_compression_5_20_60"] = (
        working[["ma5", "ma20", "ma60"]].max(axis=1)
        - working[["ma5", "ma20", "ma60"]].min(axis=1)
    ) / working["close"].replace(0, np.nan)

    for window in (1, 3, 5, 10, 20):
        working[f"ret{window}"] = group["close"].pct_change(periods=window)

    working["median_volume_20"] = group["volume"].transform(
        lambda series: series.rolling(20, min_periods=20).median()
    )
    working["median_volume_60"] = group["volume"].transform(
        lambda series: series.rolling(60, min_periods=60).median()
    )
    working["avg_volume_20"] = group["volume"].transform(
        lambda series: series.rolling(20, min_periods=20).mean()
    )
    working["avg_turnover_20"] = group["turnover_effective"].transform(
        lambda series: series.rolling(20, min_periods=20).mean()
    )
    working["median_turnover_20"] = group["turnover_effective"].transform(
        lambda series: series.rolling(20, min_periods=20).median()
    )
    working["vol_rel20"] = working["volume"] / working["median_volume_20"].replace(0, np.nan)
    working["vol_rel60"] = working["volume"] / working["median_volume_60"].replace(0, np.nan)
    working["turnover_rel20"] = working["turnover_effective"] / working[
        "median_turnover_20"
    ].replace(0, np.nan)
    working["avg_volume_prev5"] = group["volume"].transform(
        lambda series: series.shift(1).rolling(5, min_periods=5).mean()
    )
    working["volume_dry_up_then_expand"] = (
        working["avg_volume_prev5"].le(working["median_volume_20"] * 0.80)
        & working["volume"].ge(working["median_volume_20"] * 1.50)
    )
    log_volume = working["volume"].where(working["volume"].gt(0)).apply(
        lambda value: np.nan if pd.isna(value) else np.log(float(value))
    )
    working["vol_z20"] = (
        log_volume
        - log_volume.groupby(working["symbol"]).transform(
            lambda series: series.rolling(20, min_periods=20).mean()
        )
    ) / log_volume.groupby(working["symbol"]).transform(
        lambda series: series.rolling(20, min_periods=20).std(ddof=0)
    ).replace(0, np.nan)

    candle_range = (working["high"] - working["low"]).replace(0, np.nan)
    working["close_loc"] = ((working["close"] - working["low"]) / candle_range).fillna(0.5)
    working["upper_wick_ratio"] = (
        (working["high"] - working[["open", "close"]].max(axis=1)) / candle_range
    ).fillna(0.0)
    working["lower_wick_ratio"] = (
        (working[["open", "close"]].min(axis=1) - working["low"]) / candle_range
    ).fillna(0.0)
    working["body_ratio"] = ((working["close"] - working["open"]).abs() / candle_range).fillna(0.0)

    working["high_20_prev"] = group["high"].transform(
        lambda series: series.shift(1).rolling(20, min_periods=20).max()
    )
    working["low_20_prev"] = group["low"].transform(
        lambda series: series.shift(1).rolling(20, min_periods=20).min()
    )
    working["high_10"] = group["high"].transform(
        lambda series: series.rolling(10, min_periods=10).max()
    )
    working["resistance_20"] = group["high"].transform(
        lambda series: series.rolling(20, min_periods=20).max()
    )
    working["resistance_60"] = group["high"].transform(
        lambda series: series.rolling(60, min_periods=60).max()
    )
    working["drawdown_from_high_10"] = working["close"] / working["high_10"] - 1.0
    working["box_width_20"] = (
        working["high_20_prev"] / working["low_20_prev"].replace(0, np.nan) - 1.0
    )

    bb_mid = working["ma20"]
    bb_std = group["close"].transform(lambda series: series.rolling(20, min_periods=20).std(ddof=0))
    working["bb_width"] = (4.0 * bb_std) / bb_mid.replace(0, np.nan)
    working["bb_width_rank_120"] = group["bb_width"].transform(
        lambda series: _rolling_percentile_of_last(series, 120)
    )

    prev_close = group["close"].shift(1)
    true_range = pd.concat(
        [
            (working["high"] - working["low"]).abs(),
            (working["high"] - prev_close).abs(),
            (working["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    working["atr14"] = true_range.groupby(working["symbol"]).transform(
        lambda series: series.rolling(14, min_periods=14).mean()
    )
    working["atr_pct"] = working["atr14"] / working["close"].replace(0, np.nan)
    working["rsi14"] = group["close"].transform(lambda series: _rsi(series, 14))
    working["rsi5"] = group["close"].transform(lambda series: _rsi(series, 5))
    working["consecutive_up_days"] = group["close"].transform(_count_consecutive_up)

    latest = working.loc[working["trading_date"] == as_of_date].copy()
    return latest.reset_index(drop=True)


def _load_swing_inputs(
    connection,
    *,
    as_of_date: date,
    symbols: list[str] | None,
    limit_symbols: int | None,
    market: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    symbol_filter = ""
    params: list[object] = [as_of_date]
    if symbols:
        normalized = [str(symbol).zfill(6) for symbol in symbols]
        placeholders = ", ".join("?" for _ in normalized)
        symbol_filter = f" AND symbol.symbol IN ({placeholders})"
        params.extend(normalized)
    market_filter = ""
    if market.upper() != "ALL":
        market_filter = " AND UPPER(symbol.market) = ?"
        params.append(market.upper())
    symbol_frame = connection.execute(
        f"""
        SELECT
            symbol.symbol,
            symbol.company_name,
            symbol.market,
            symbol.sector,
            symbol.industry,
            symbol.sector_code,
            symbol.industry_code,
            symbol.listing_date,
            symbol.is_common_stock,
            symbol.is_preferred_stock,
            symbol.is_etf,
            symbol.is_etn,
            symbol.is_spac,
            symbol.is_reit,
            symbol.is_delisted,
            symbol.is_trading_halt,
            symbol.is_management_issue
        FROM dim_symbol AS symbol
        WHERE symbol.market IN ('KOSPI', 'KOSDAQ')
          AND COALESCE(symbol.is_common_stock, FALSE)
          AND NOT COALESCE(symbol.is_preferred_stock, FALSE)
          AND NOT COALESCE(symbol.is_etf, FALSE)
          AND NOT COALESCE(symbol.is_etn, FALSE)
          AND NOT COALESCE(symbol.is_spac, FALSE)
          AND NOT COALESCE(symbol.is_reit, FALSE)
          AND NOT COALESCE(symbol.is_delisted, FALSE)
          AND NOT COALESCE(symbol.is_trading_halt, FALSE)
          AND (
              symbol.listing_date IS NULL
              OR symbol.listing_date <= ?
          )
          {symbol_filter}
          {market_filter}
        ORDER BY symbol.symbol
        """,
        params,
    ).fetchdf()
    if limit_symbols is not None and int(limit_symbols) > 0:
        symbol_frame = symbol_frame.head(int(limit_symbols)).copy()
    if symbol_frame.empty:
        return symbol_frame, pd.DataFrame(), pd.DataFrame()
    symbol_frame["symbol"] = symbol_frame["symbol"].astype(str).str.zfill(6)
    connection.register("swing_symbols", symbol_frame[["symbol"]].drop_duplicates())
    try:
        ohlcv = connection.execute(
            """
            SELECT
                trading_date,
                symbol,
                open,
                high,
                low,
                close,
                volume,
                turnover_value,
                market_cap
            FROM fact_daily_ohlcv
            WHERE symbol IN (SELECT symbol FROM swing_symbols)
              AND trading_date <= ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY trading_date DESC, ingested_at DESC
            ) <= 180
            ORDER BY symbol, trading_date
            """,
            [as_of_date],
        ).fetchdf()
        fundamentals = connection.execute(
            """
            SELECT *
            FROM fact_fundamentals_snapshot
            WHERE symbol IN (SELECT symbol FROM swing_symbols)
              AND as_of_date <= ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY as_of_date DESC, disclosed_at DESC NULLS LAST, ingested_at DESC
            ) = 1
            """,
            [as_of_date],
        ).fetchdf()
    finally:
        try:
            connection.unregister("swing_symbols")
        except Exception:
            pass
    return symbol_frame, ohlcv, fundamentals


def _market_regime_map(connection, *, as_of_date: date) -> dict[str, str]:
    frame = connection.execute(
        """
        SELECT market_scope, regime_state
        FROM fact_market_regime_snapshot
        WHERE as_of_date <= ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY market_scope
            ORDER BY as_of_date DESC, created_at DESC
        ) = 1
        """,
        [as_of_date],
    ).fetchdf()
    mapping = {"KR_ALL": "neutral", "KOSPI": "neutral", "KOSDAQ": "neutral"}
    for row in frame.itertuples(index=False):
        state = str(getattr(row, "regime_state", "") or "").lower()
        if state in {"risk_on", "euphoria"}:
            mapped = "strong"
        elif state in {"panic", "risk_off"}:
            mapped = "weak"
        else:
            mapped = "neutral"
        mapping[str(getattr(row, "market_scope", "") or "").upper()] = mapped
    return mapping


def _prediction_score_frame(prediction_frame: pd.DataFrame) -> pd.DataFrame:
    if prediction_frame.empty or "symbol" not in prediction_frame.columns:
        return pd.DataFrame(
            columns=[
                "symbol",
                "ml_score_scaled",
                "ml_probability_target_first",
                "expected_excess_return",
            ]
        )
    frame = prediction_frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    expected = _safe_numeric(frame.get("expected_excess_return"))
    probability_source = None
    for column in (
        "ml_probability_target_first",
        "p_target_first",
        "target_first_probability",
    ):
        if column in frame.columns:
            probability_source = _safe_numeric(frame[column])
            break
    if probability_source is not None and probability_source.notna().any():
        frame["ml_probability_target_first"] = probability_source.clip(0.0, 1.0)
        frame["ml_score_scaled"] = (
            (frame["ml_probability_target_first"] - 0.40).div(0.30).mul(100.0)
        ).clip(0.0, 100.0)
    elif expected.notna().any():
        frame["ml_score_scaled"] = expected.rank(method="average", pct=True).mul(100.0)
        frame["ml_probability_target_first"] = (
            0.40 + frame["ml_score_scaled"].div(100.0).mul(0.30)
        ).clip(0.0, 1.0)
    else:
        frame["ml_score_scaled"] = 50.0
        frame["ml_probability_target_first"] = 0.55
    return frame[
        [
            "symbol",
            "ml_score_scaled",
            "ml_probability_target_first",
            "expected_excess_return",
        ]
    ].drop_duplicates(
        "symbol",
        keep="last",
    )


def _financial_pass(frame: pd.DataFrame) -> pd.Series:
    equity = _safe_numeric(frame.get("equity"), index=frame.index)
    debt_ratio = _safe_numeric(frame.get("debt_ratio"), index=frame.index)
    revenue = _safe_numeric(frame.get("revenue"), index=frame.index)
    debt_pass = debt_ratio.isna() | debt_ratio.le(300.0)
    revenue_pass = revenue.isna() | revenue.ge(50_000_000_000.0)
    return (equity.isna() | equity.gt(0)) & debt_pass & revenue_pass


def _rowwise_min_positive(frame: pd.DataFrame) -> pd.Series:
    numeric = frame.apply(pd.to_numeric, errors="coerce")
    return numeric.where(numeric.gt(0)).min(axis=1)


def _rowwise_max_below(frame: pd.DataFrame, current: pd.Series) -> pd.Series:
    numeric = frame.apply(pd.to_numeric, errors="coerce")
    below = numeric.where(numeric.lt(current), np.nan)
    return below.max(axis=1)


def _entry_status_series(
    *,
    current_price: pd.Series,
    signal_close: pd.Series,
    invalidation_price: pd.Series,
) -> pd.Series:
    move = current_price / signal_close.replace(0, np.nan) - 1.0
    status = pd.Series("VALID", index=current_price.index, dtype="object")
    status.loc[move.ge(0.03)] = "WATCH_CAUTION"
    status.loc[move.ge(0.04)] = "CHASE_RISK"
    status.loc[move.ge(0.05)] = "TARGET_ZONE_REACHED"
    status.loc[move.ge(0.08)] = "EXTENDED"
    status.loc[current_price.lt(invalidation_price)] = "INVALIDATED"
    return status


def _entry_score(
    *,
    entry_status: pd.Series,
    upside_to_resistance: pd.Series,
    reward_risk_ratio: pd.Series,
) -> pd.Series:
    score = pd.Series(70.0, index=entry_status.index)
    score = score + entry_status.eq("VALID").astype(float).mul(10.0)
    score = score - entry_status.eq("CHASE_RISK").astype(float).mul(15.0)
    score = score - entry_status.eq("TARGET_ZONE_REACHED").astype(float).mul(30.0)
    score = score - entry_status.eq("EXTENDED").astype(float).mul(40.0)
    score = score - upside_to_resistance.lt(0.03).astype(float).mul(20.0)
    score = score - (
        upside_to_resistance.ge(0.03) & upside_to_resistance.lt(0.05)
    ).astype(float).mul(10.0)
    score = score - reward_risk_ratio.lt(1.2).astype(float).mul(30.0)
    score = score - (
        reward_risk_ratio.ge(1.2) & reward_risk_ratio.lt(1.5)
    ).astype(float).mul(15.0)
    score = score + reward_risk_ratio.ge(1.5).astype(float).mul(10.0)
    score.loc[entry_status.eq("INVALIDATED")] = 0.0
    return score.clip(0.0, 100.0)


def _score_rows(frame: pd.DataFrame, *, config: Swing35DConfig) -> pd.DataFrame:
    scored = frame.copy()
    for column in (
        "close",
        "current_price",
        "open",
        "high",
        "low",
        "market_cap",
        "avg_turnover_20",
        "median_turnover_20",
        "turnover_effective",
        "avg_volume_20",
        "ret1",
        "ret5",
        "ret10",
        "ret20",
        "dist_ma20",
        "dist_ma60",
        "vol_rel20",
        "turnover_rel20",
        "vol_z20",
        "rsi14",
        "rsi5",
        "atr_pct",
        "ma20_slope_5",
        "ma60_slope_20",
        "ma120_slope_20",
        "drawdown_from_high_10",
        "close_loc",
        "body_ratio",
        "upper_wick_ratio",
        "high_20_prev",
        "low_20_prev",
        "resistance_20",
        "resistance_60",
        "ma5",
        "ma20",
        "ma60",
        "ma120",
        "reward_risk_ratio",
        "risk_distance",
        "upside_to_resistance",
        "expected_excess_return",
    ):
        if column in scored.columns:
            scored[column] = _safe_numeric(scored[column])
    if "turnover_effective" not in scored.columns:
        scored["turnover_effective"] = scored["avg_turnover_20"]

    liquidity_pass = (
        scored["close"].ge(config.price_min_krw)
        & (
            scored["market_cap"].isna()
            | scored["market_cap"].ge(config.market_cap_min_krw)
        )
        & scored["avg_turnover_20"].ge(config.avg_turnover_20_min_krw)
        & scored["median_turnover_20"].ge(config.median_turnover_20_min_krw)
        & scored["turnover_effective"].ge(config.avg_turnover_20_min_krw)
        & scored["avg_volume_20"].ge(config.avg_volume_20_min)
    )
    financial_pass = _financial_pass(scored)
    market_risk_pass = ~_bool_column(scored, "is_management_issue")
    history_pass = scored["history_days"].ge(config.min_history_days)
    volume_dry_up_then_expand = _bool_column(scored, "volume_dry_up_then_expand")
    ma5_cross_ma20_up = _bool_column(scored, "ma5_cross_ma20_up")

    heavy_down_volume = (
        scored["close"].lt(scored["open"])
        & scored["ret1"].le(-0.03)
        & scored["vol_rel20"].ge(1.5)
        & scored["close"].lt(scored["ma20"])
    )
    upper_wick_distribution = (
        scored["vol_rel20"].ge(2.0)
        & scored["upper_wick_ratio"].ge(0.45)
        & scored["close_loc"].le(0.55)
    )
    return_overheat = scored["ret5"].gt(0.15) | scored["ret10"].gt(0.25) | scored["ret20"].gt(0.25)
    ma_overheat = scored["dist_ma20"].gt(0.10) | scored["dist_ma60"].gt(0.20)
    volume_overheat = scored["vol_rel20"].gt(5.0) & scored["ret1"].gt(0.08)
    chase_risk = (
        scored["ret1"].gt(0.08)
        & scored["vol_rel20"].gt(4.0)
        & scored["dist_ma20"].gt(0.08)
    )

    common_pass = (
        market_risk_pass
        & history_pass
        & liquidity_pass
        & financial_pass
        & ~return_overheat.fillna(True)
        & ~ma_overheat.fillna(True)
        & ~heavy_down_volume.fillna(True)
        & ~upper_wick_distribution.fillna(True)
        & ~volume_overheat.fillna(True)
        & ~chase_risk.fillna(False)
    )

    pullback = (
        scored["close"].gt(scored["ma20"] * 0.99)
        & scored["close"].le(scored["ma20"] * 1.06)
        & scored["ma20_slope_5"].gt(0.0)
        & scored["ma60_slope_20"].ge(-0.005)
        & scored["ret5"].le(0.10)
        & scored["ret20"].le(0.25)
        & scored["close"].gt(scored["ma5"])
        & scored["rsi14"].between(45, 65, inclusive="both")
        & scored["drawdown_from_high_10"].between(-0.12, -0.03, inclusive="both")
        & scored["vol_rel20"].between(1.2, 2.5, inclusive="both")
        & scored["turnover_rel20"].ge(1.2)
        & scored["vol_z20"].ge(0.5)
        & scored["close_loc"].ge(0.65)
        & scored["body_ratio"].ge(0.30)
        & scored["upper_wick_ratio"].le(0.30)
    )
    box_breakout = (
        scored["box_width_20"].between(0.05, 0.18, inclusive="both")
        & scored["ma_compression_5_20_60"].le(0.08)
        & scored["bb_width_rank_120"].le(0.40)
        & scored["close"].gt(scored["high_20_prev"] * 1.005)
        & scored["vol_rel20"].between(1.7, 4.0, inclusive="both")
        & scored["turnover_rel20"].ge(1.5)
        & scored["vol_z20"].ge(1.0)
        & scored["close_loc"].ge(0.70)
        & scored["upper_wick_ratio"].le(0.30)
        & scored["body_ratio"].ge(0.35)
        & scored["ret5"].le(0.12)
        & scored["ret20"].le(0.25)
        & scored["dist_ma20"].le(0.10)
        & scored["rsi14"].le(70)
    )
    reversal = (
        scored["close"].gt(scored["ma20"])
        & ma5_cross_ma20_up
        & scored["ma20_slope_5"].gt(-0.003)
        & scored["vol_rel20"].between(1.5, 3.5, inclusive="both")
        & scored["rsi14"].between(40, 60, inclusive="both")
        & scored["close_loc"].ge(0.65)
        & scored["upper_wick_ratio"].le(0.30)
    )
    full_bear_alignment = (
        scored["ma5"].lt(scored["ma20"])
        & scored["ma20"].lt(scored["ma60"])
        & scored["ma60"].lt(scored["ma120"])
    )
    all_slopes_negative = (
        scored["ma20_slope_5"].lt(0)
        & scored["ma60_slope_20"].lt(0)
        & scored["ma120_slope_20"].lt(0)
    )
    reversal = reversal & ~(full_bear_alignment & all_slopes_negative)
    recovery_breakout = (
        scored["close"].gt(scored["ma20"])
        & scored["close"].gt(scored["ma120"])
        & scored["close"].gt(scored["high_20_prev"] * 1.005)
        & scored["ma60_slope_20"].ge(-0.01)
        & scored["vol_rel20"].between(1.5, 4.0, inclusive="both")
        & scored["close_loc"].ge(0.65)
        & scored["upper_wick_ratio"].le(0.35)
    )

    scored["swing_pattern"] = pd.NA
    scored.loc[reversal, "swing_pattern"] = "reversal_recovery"
    scored.loc[recovery_breakout, "swing_pattern"] = "recovery_breakout"
    scored.loc[box_breakout, "swing_pattern"] = "box_breakout"
    scored.loc[pullback, "swing_pattern"] = "pullback"
    pattern_pass = scored["swing_pattern"].notna()

    scored["breakout_level"] = scored["high_20_prev"]
    resistance_candidates = pd.DataFrame(
        {
            "resistance_20": scored["resistance_20"].where(
                scored["resistance_20"].gt(scored["close"])
            ),
            "resistance_60": scored["resistance_60"].where(
                scored["resistance_60"].gt(scored["close"])
            ),
            "recent_pivot_high": scored["high_20_prev"].where(
                scored["high_20_prev"].gt(scored["close"])
            ),
        },
        index=scored.index,
    )
    scored["resistance_line"] = _rowwise_min_positive(resistance_candidates).fillna(
        scored["close"] * 1.08
    )
    support_candidates = pd.DataFrame(
        {
            "ma20": scored["ma20"],
            "recent_pivot_low": scored["low_20_prev"],
            "breakout_level": scored["breakout_level"] * 0.98,
        },
        index=scored.index,
    )
    scored["nearest_support"] = _rowwise_max_below(support_candidates, scored["close"]).fillna(
        scored["ma20"] * 0.985
    )
    invalidation_candidates = pd.DataFrame(
        {
            "signal_low": scored["low"],
            "ma20_floor": scored["ma20"] * 0.985,
            "breakout_floor": scored["breakout_level"] * 0.98,
        },
        index=scored.index,
    )
    scored["risk_line"] = _rowwise_min_positive(invalidation_candidates).fillna(
        scored["ma20"] * 0.985
    )
    scored["invalidation_price"] = scored["risk_line"]
    scored["signal_close"] = scored["close"]
    if "current_price" not in scored.columns:
        scored["current_price"] = scored["signal_close"]
    scored["current_price"] = scored["current_price"].fillna(scored["signal_close"])
    scored["price_move_from_signal"] = (
        scored["current_price"] / scored["signal_close"].replace(0, np.nan) - 1.0
    )
    scored["risk_distance"] = (
        scored["current_price"] / _safe_numeric(scored["risk_line"]).replace(0, np.nan) - 1.0
    )
    scored["upside_to_resistance"] = (
        scored["resistance_line"] / scored["current_price"].replace(0, np.nan) - 1.0
    )
    scored["reward_risk_ratio"] = scored["upside_to_resistance"] / scored[
        "risk_distance"
    ].replace(0, np.nan)
    scored["entry_status"] = _entry_status_series(
        current_price=scored["current_price"],
        signal_close=scored["signal_close"],
        invalidation_price=scored["invalidation_price"],
    )
    scored["entry_score"] = _entry_score(
        entry_status=scored["entry_status"],
        upside_to_resistance=scored["upside_to_resistance"],
        reward_risk_ratio=scored["reward_risk_ratio"],
    )

    risk_reward_pass = scored["risk_distance"].le(0.05) & scored["reward_risk_ratio"].ge(1.2)
    scored["swing_common_pass"] = common_pass
    scored["swing_pattern_pass"] = pattern_pass
    scored["swing_candidate_pass"] = common_pass & pattern_pass & risk_reward_pass.fillna(False)

    chart_score = (
        scored["close"].gt(scored["ma20"]).astype(float).mul(5)
        + scored["ma20_slope_5"].gt(0).astype(float).mul(5)
        + scored["ma60_slope_20"].ge(0).astype(float).mul(5)
        + (scored["ma60_slope_20"].between(-0.005, 0, inclusive="left")).astype(float).mul(3)
        + (
            scored["ma5"].gt(scored["ma20"])
            | scored["ma5"].gt(scored["ma5_prev"])
        )
        .astype(float)
        .mul(5)
        + scored["dist_ma20"].between(-0.01, 0.06, inclusive="both").astype(float).mul(5)
        + pullback.astype(float).mul(3)
        + box_breakout.astype(float).mul(2)
        + recovery_breakout.astype(float).mul(2)
        - reversal.astype(float).mul(2)
    )
    volume_score = (
        scored["vol_rel20"].between(1.3, 3.5, inclusive="both").astype(float).mul(6)
        + scored["vol_rel20"].between(1.1, 1.3, inclusive="left").astype(float).mul(3)
        + scored["turnover_rel20"].ge(1.3).astype(float).mul(4)
        + volume_dry_up_then_expand.astype(float).mul(5)
        + scored["vol_z20"].between(0.8, 2.8, inclusive="both").astype(float).mul(3)
        + (scored["close"].gt(scored["open"]) & scored["vol_rel20"].ge(1.3)).astype(float).mul(2)
        - (scored["vol_rel20"].gt(4.0) & scored["ret1"].gt(0.08)).astype(float).mul(5)
        - upper_wick_distribution.astype(float).mul(8)
    )
    overheat_score = (
        scored["ret5"].le(0.10).astype(float).mul(5)
        + scored["ret20"].le(0.25).astype(float).mul(5)
        + scored["dist_ma20"].le(0.08).astype(float).mul(3)
        + scored["rsi14"].le(70).astype(float).mul(2)
        - scored["ret5"].gt(0.12).astype(float).mul(4)
        - scored["dist_ma20"].gt(0.10).astype(float).mul(5)
        - scored["rsi14"].gt(72).astype(float).mul(5)
    )
    regime_score = (
        scored["market_regime"].eq("strong").astype(float).mul(4)
        + scored["market_regime"].eq("neutral").astype(float).mul(2)
        + scored["sector_ret5"].gt(scored["market_ret5"]).astype(float).mul(2)
        + scored["sector_ret20"].gt(scored["market_ret20"]).astype(float).mul(2)
        + scored["sector_rank_20"].le(0.30).astype(float).mul(2)
        - scored["market_regime"].eq("weak").astype(float).mul(3)
    )
    rr_score = (
        scored["risk_distance"].le(0.04).astype(float).mul(4)
        + scored["risk_distance"].between(0.04, 0.05, inclusive="right").astype(float).mul(2)
        + scored["upside_to_resistance"].ge(0.05).astype(float).mul(3)
        + scored["upside_to_resistance"].between(0.04, 0.05, inclusive="left").astype(float).mul(2)
        + scored["reward_risk_ratio"].ge(2.0).astype(float).mul(3)
        + scored["reward_risk_ratio"].between(1.5, 2.0, inclusive="left").astype(float).mul(2)
    )
    candle_score = (
        scored["close_loc"].ge(0.65).astype(float).mul(4)
        + scored["body_ratio"].ge(0.35).astype(float).mul(3)
        + scored["upper_wick_ratio"].le(0.30).astype(float).mul(3)
        - scored["upper_wick_ratio"].ge(0.45).astype(float).mul(5)
        - scored["close_loc"].le(0.50).astype(float).mul(3)
    )
    debt_ratio = _safe_numeric(scored.get("debt_ratio"))
    financial_score = (
        _safe_numeric(scored.get("equity")).gt(0).astype(float).mul(2)
        + debt_ratio.le(100).astype(float).mul(2)
        + debt_ratio.between(100, 200, inclusive="right").astype(float).mul(1)
        + _safe_numeric(scored.get("operating_income")).gt(0).astype(float).mul(2)
        + _safe_numeric(scored.get("net_income")).gt(0).astype(float).mul(2)
        + _safe_numeric(scored.get("revenue")).ge(30_000_000_000).astype(float).mul(2)
    )

    scored["swing_chart_score"] = _clip_score(chart_score, 25)
    scored["swing_volume_score"] = _clip_score(volume_score, 20)
    scored["swing_overheat_score"] = _clip_score(overheat_score, 15)
    scored["swing_regime_score"] = _clip_score(regime_score, 10)
    scored["swing_risk_reward_score"] = _clip_score(rr_score, 10)
    scored["swing_candle_score"] = _clip_score(candle_score, 10)
    scored["swing_financial_score"] = _clip_score(financial_score, 10)
    scored["swing_rule_score"] = (
        scored["swing_chart_score"]
        + scored["swing_volume_score"]
        + scored["swing_overheat_score"]
        + scored["swing_regime_score"]
        + scored["swing_risk_reward_score"]
        + scored["swing_candle_score"]
        + scored["swing_financial_score"]
    ).clip(lower=0.0, upper=100.0)
    scored["ml_score_scaled"] = _safe_numeric(scored.get("ml_score_scaled")).fillna(50.0)
    if "ml_probability_target_first" not in scored.columns:
        scored["ml_probability_target_first"] = (
            0.40 + scored["ml_score_scaled"].div(100.0).mul(0.30)
        )
    scored["ml_probability_target_first"] = _safe_numeric(
        scored["ml_probability_target_first"]
    ).fillna(0.55).clip(0.0, 1.0)
    scored["ml_probability_score"] = (
        (scored["ml_probability_target_first"] - 0.40).div(0.30).mul(100.0)
    ).clip(0.0, 100.0)
    scored["sector_score_scaled"] = scored["swing_regime_score"].div(10.0).mul(100.0)
    expected_reward = scored["upside_to_resistance"].clip(lower=0.0, upper=0.08)
    expected_risk = scored["risk_distance"].clip(lower=0.0, upper=0.05)
    volatility_penalty = (scored["atr_pct"] - 0.08).clip(lower=0.0).mul(0.5)
    overheat_penalty = (scored["dist_ma20"] - 0.08).clip(lower=0.0).mul(0.8)
    scored["expected_utility"] = (
        scored["ml_probability_target_first"] * expected_reward
        - (1.0 - scored["ml_probability_target_first"]) * expected_risk
        - 0.003
        - volatility_penalty.fillna(0.0)
        - overheat_penalty.fillna(0.0)
    )
    scored["swing_signal_score"] = scored["swing_rule_score"]
    scored["swing_hybrid_score"] = (
        config.rule_weight * scored["swing_signal_score"]
        + config.ml_weight * scored["ml_probability_score"]
        + config.entry_weight * scored["entry_score"]
    ).clip(lower=0.0, upper=100.0)
    threshold = config.recommendation_threshold + scored["market_regime"].eq("weak").astype(
        float
    ).mul(config.weak_market_threshold_add)
    operating_candidate = (
        common_pass
        & pattern_pass
        & risk_reward_pass.fillna(False)
        & scored["swing_rule_score"].ge(config.rule_score_min)
        & scored["ml_probability_target_first"].ge(config.min_ml_probability)
        & scored["entry_status"].isin(["VALID", "WATCH_CAUTION"])
        & scored["swing_hybrid_score"].ge(threshold)
    )
    high_confidence = (
        operating_candidate
        & scored["entry_status"].eq("VALID")
        & scored["swing_rule_score"].ge(80.0)
        & scored["ml_probability_target_first"].ge(config.high_confidence_ml_probability)
        & scored["entry_score"].ge(70.0)
        & scored["swing_hybrid_score"].ge(config.high_confidence_threshold)
        & scored["reward_risk_ratio"].ge(1.5)
        & scored["upside_to_resistance"].ge(0.04)
    )
    final_status = pd.Series("WATCHLIST", index=scored.index, dtype="object")
    final_status.loc[~common_pass.fillna(False)] = "REJECTED"
    final_status.loc[scored["entry_status"].eq("INVALIDATED")] = "INVALIDATED"
    final_status.loc[scored["entry_status"].eq("EXTENDED")] = "EXTENDED"
    final_status.loc[scored["entry_status"].eq("TARGET_ZONE_REACHED")] = "TARGET_ZONE_REACHED"
    final_status.loc[scored["entry_status"].eq("CHASE_RISK")] = "CHASE_RISK"
    final_status.loc[operating_candidate] = "CANDIDATE"
    final_status.loc[high_confidence] = "HIGH_CONFIDENCE"
    scored["swing_final_status"] = final_status
    scored["swing_recommendation_pass"] = scored["swing_final_status"].isin(
        ["HIGH_CONFIDENCE", "CANDIDATE"]
    )
    scored["swing_candidate_pass"] = scored["swing_recommendation_pass"]
    scored["swing_grade"] = pd.cut(
        scored["swing_hybrid_score"],
        bins=[-0.01, 69.999, 74.999, 79.999, 84.999, 100.0],
        labels=["제외", "C", "B", "B+", "A"],
    ).astype(str)

    reasons: list[list[str]] = []
    risks: list[list[str]] = []
    for row in scored.itertuples(index=False):
        row_reasons: list[str] = []
        row_risks: list[str] = []
        pattern = getattr(row, "swing_pattern", None)
        pattern_text = "" if pd.isna(pattern) else str(pattern)
        if pattern_text == "pullback":
            row_reasons.append("swing_pullback_pattern")
        elif pattern_text == "box_breakout":
            row_reasons.append("swing_box_breakout_pattern")
        elif pattern_text == "recovery_breakout":
            row_reasons.append("swing_recovery_breakout_pattern")
        elif pattern_text == "reversal_recovery":
            row_reasons.append("swing_reversal_recovery_pattern")
        if getattr(row, "vol_rel20", 0) >= 1.3:
            row_reasons.append("swing_volume_expansion")
        if getattr(row, "close_loc", 0) >= 0.65:
            row_reasons.append("swing_strong_close")
        if getattr(row, "reward_risk_ratio", 0) >= 1.5:
            row_reasons.append("swing_reward_risk_ok")
        if not bool(getattr(row, "swing_common_pass", False)):
            row_risks.append("swing_common_filter_failed")
        if not bool(getattr(row, "swing_pattern_pass", False)):
            row_risks.append("swing_no_valid_pattern")
        if bool(getattr(row, "upper_wick_ratio", 0) >= 0.45):
            row_risks.append("swing_upper_wick_distribution")
        if bool(getattr(row, "ret5", 0) > 0.12):
            row_risks.append("swing_recent_overheat")
        if bool(getattr(row, "risk_distance", 999) > 0.05):
            row_risks.append("swing_stop_distance_wide")
        if bool(getattr(row, "upside_to_resistance", 999) < 0.03):
            row_risks.append("swing_near_resistance")
        if bool(getattr(row, "reward_risk_ratio", 999) < 1.2):
            row_risks.append("swing_low_reward_risk")
        status_text = str(getattr(row, "entry_status", "") or "")
        if status_text == "CHASE_RISK":
            row_risks.append("swing_chase_risk")
        elif status_text == "TARGET_ZONE_REACHED":
            row_risks.append("swing_target_zone_reached")
        elif status_text == "EXTENDED":
            row_risks.append("swing_extended")
        elif status_text == "INVALIDATED":
            row_risks.append("swing_invalidated")
        equity_value = getattr(row, "equity", None)
        if pd.isna(equity_value):
            row_risks.append("swing_financial_context_missing")
        reasons.append(row_reasons[:4])
        risks.append(row_risks[:4])
    scored["swing_reason_tags"] = reasons
    scored["swing_risk_flags"] = risks
    return scored


def build_swing_3_5d_frame(
    connection,
    *,
    as_of_date: date,
    settings: Settings,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    prediction_frame: pd.DataFrame | None = None,
    config: Swing35DConfig | None = None,
) -> pd.DataFrame:
    del settings
    config = config or Swing35DConfig()
    symbol_frame, ohlcv, fundamentals = _load_swing_inputs(
        connection,
        as_of_date=as_of_date,
        symbols=symbols,
        limit_symbols=limit_symbols,
        market=market,
    )
    if symbol_frame.empty or ohlcv.empty:
        return pd.DataFrame()
    features = _compute_symbol_features(ohlcv, as_of_date=as_of_date)
    if features.empty:
        return pd.DataFrame()
    fundamentals = fundamentals.copy()
    if not fundamentals.empty:
        fundamentals["symbol"] = fundamentals["symbol"].astype(str).str.zfill(6)
    frame = (
        symbol_frame.merge(features, on="symbol", how="left")
        .merge(fundamentals, on="symbol", how="left", suffixes=("", "_fund"))
        .merge(
            _prediction_score_frame(
                prediction_frame if prediction_frame is not None else pd.DataFrame()
            ),
            on="symbol",
            how="left",
        )
    )
    frame = add_industry_group_columns(
        frame,
        key_column="swing_industry_group_key",
        label_column="swing_industry_group_label",
        type_column="swing_industry_group_type",
    )
    regime_map = _market_regime_map(connection, as_of_date=as_of_date)
    frame["market_regime"] = frame["market"].map(
        lambda value: regime_map.get(
            str(value).upper(),
            regime_map.get("KR_ALL", "neutral"),
        )
    )
    frame["market_ret5"] = frame.groupby("market")["ret5"].transform("median")
    frame["market_ret20"] = frame.groupby("market")["ret20"].transform("median")
    frame["sector_ret5"] = frame.groupby("swing_industry_group_key")["ret5"].transform("median")
    frame["sector_ret20"] = frame.groupby("swing_industry_group_key")["ret20"].transform("median")
    sector_strength = (
        frame[["swing_industry_group_key", "sector_ret20"]]
        .drop_duplicates()
        .assign(
            sector_rank_20=lambda data: data["sector_ret20"].rank(
                ascending=False,
                method="average",
                pct=True,
            )
        )
    )
    frame = frame.merge(
        sector_strength[["swing_industry_group_key", "sector_rank_20"]],
        on="swing_industry_group_key",
        how="left",
    )
    scored = _score_rows(frame, config=config)
    return scored


def apply_swing_3_5d_overlay(
    scored: pd.DataFrame,
    swing_frame: pd.DataFrame | None,
    *,
    horizon: int,
) -> pd.DataFrame:
    if int(horizon) != 5 or swing_frame is None or swing_frame.empty:
        return scored
    overlay_columns = [
        "symbol",
        "swing_pattern",
        "swing_signal_score",
        "swing_rule_score",
        "swing_hybrid_score",
        "swing_final_status",
        "swing_recommendation_pass",
        "swing_candidate_pass",
        "swing_grade",
        "swing_reason_tags",
        "swing_risk_flags",
        "swing_chart_score",
        "swing_volume_score",
        "swing_overheat_score",
        "swing_regime_score",
        "swing_risk_reward_score",
        "swing_candle_score",
        "swing_financial_score",
        "ml_probability_target_first",
        "ml_probability_score",
        "expected_utility",
        "entry_score",
        "entry_status",
        "signal_close",
        "current_price",
        "price_move_from_signal",
        "breakout_level",
        "risk_line",
        "nearest_support",
        "invalidation_price",
        "resistance_line",
        "risk_distance",
        "upside_to_resistance",
        "reward_risk_ratio",
        "vol_rel20",
        "turnover_rel20",
        "dist_ma20",
        "rsi14",
        "close_loc",
        "upper_wick_ratio",
        "swing_industry_group_key",
        "swing_industry_group_label",
        "swing_industry_group_type",
        "market_regime",
    ]
    available = [column for column in overlay_columns if column in swing_frame.columns]
    merged = scored.merge(
        swing_frame[available].drop_duplicates("symbol", keep="last"),
        on="symbol",
        how="left",
    )
    recommendation_pass = merged["swing_recommendation_pass"].fillna(False).astype(bool)
    candidate_pass = merged["swing_candidate_pass"].fillna(False).astype(bool)
    swing_score = _safe_numeric(merged.get("swing_hybrid_score")).fillna(0.0)
    merged["final_selection_value"] = swing_score.where(candidate_pass, 0.0).clip(0.0, 100.0)
    merged["eligible_flag"] = recommendation_pass
    merged["final_selection_rank_pct"] = merged["final_selection_value"].rank(
        method="average",
        pct=True,
    )
    merged["swing_3_5d_overlay_applied"] = True
    return merged


def swing_explanatory_payload(row: pd.Series) -> dict[str, object] | None:
    if not bool(row.get("swing_3_5d_overlay_applied", False)):
        return None
    return {
        "methodology_version": SWING_3_5D_VERSION,
        "pattern": None if pd.isna(row.get("swing_pattern")) else row.get("swing_pattern"),
        "signal_score": None
        if pd.isna(row.get("swing_signal_score"))
        else float(row.get("swing_signal_score")),
        "rule_score": None
        if pd.isna(row.get("swing_rule_score"))
        else float(row.get("swing_rule_score")),
        "hybrid_score": None
        if pd.isna(row.get("swing_hybrid_score"))
        else float(row.get("swing_hybrid_score")),
        "final_score": None
        if pd.isna(row.get("swing_hybrid_score"))
        else float(row.get("swing_hybrid_score")),
        "final_status": None
        if pd.isna(row.get("swing_final_status"))
        else row.get("swing_final_status"),
        "entry_status": None if pd.isna(row.get("entry_status")) else row.get("entry_status"),
        "entry_score": None
        if pd.isna(row.get("entry_score"))
        else float(row.get("entry_score")),
        "signal_close": None
        if pd.isna(row.get("signal_close"))
        else float(row.get("signal_close")),
        "current_price": None
        if pd.isna(row.get("current_price"))
        else float(row.get("current_price")),
        "price_move_from_signal": None
        if pd.isna(row.get("price_move_from_signal"))
        else float(row.get("price_move_from_signal")),
        "ml_probability_target_first": None
        if pd.isna(row.get("ml_probability_target_first"))
        else float(row.get("ml_probability_target_first")),
        "ml_probability_score": None
        if pd.isna(row.get("ml_probability_score"))
        else float(row.get("ml_probability_score")),
        "expected_utility": None
        if pd.isna(row.get("expected_utility"))
        else float(row.get("expected_utility")),
        "recommendation_pass": bool(row.get("swing_recommendation_pass", False)),
        "candidate_pass": bool(row.get("swing_candidate_pass", False)),
        "component_scores": {
            "chart": None
            if pd.isna(row.get("swing_chart_score"))
            else float(row.get("swing_chart_score")),
            "volume": None
            if pd.isna(row.get("swing_volume_score"))
            else float(row.get("swing_volume_score")),
            "overheat": None
            if pd.isna(row.get("swing_overheat_score"))
            else float(row.get("swing_overheat_score")),
            "regime": None
            if pd.isna(row.get("swing_regime_score"))
            else float(row.get("swing_regime_score")),
            "risk_reward": None
            if pd.isna(row.get("swing_risk_reward_score"))
            else float(row.get("swing_risk_reward_score")),
            "candle": None
            if pd.isna(row.get("swing_candle_score"))
            else float(row.get("swing_candle_score")),
            "financial": None
            if pd.isna(row.get("swing_financial_score"))
            else float(row.get("swing_financial_score")),
        },
        "breakout_level": None
        if pd.isna(row.get("breakout_level"))
        else float(row.get("breakout_level")),
        "nearest_support": None
        if pd.isna(row.get("nearest_support"))
        else float(row.get("nearest_support")),
        "invalidation_price": None
        if pd.isna(row.get("invalidation_price"))
        else float(row.get("invalidation_price")),
        "risk_line": None if pd.isna(row.get("risk_line")) else float(row.get("risk_line")),
        "resistance_line": None
        if pd.isna(row.get("resistance_line"))
        else float(row.get("resistance_line")),
        "risk_distance": None
        if pd.isna(row.get("risk_distance"))
        else float(row.get("risk_distance")),
        "upside_to_resistance": None
        if pd.isna(row.get("upside_to_resistance"))
        else float(row.get("upside_to_resistance")),
        "reward_risk_ratio": None
        if pd.isna(row.get("reward_risk_ratio"))
        else float(row.get("reward_risk_ratio")),
        "volume_rel20": None if pd.isna(row.get("vol_rel20")) else float(row.get("vol_rel20")),
        "turnover_rel20": None
        if pd.isna(row.get("turnover_rel20"))
        else float(row.get("turnover_rel20")),
        "dist_ma20": None if pd.isna(row.get("dist_ma20")) else float(row.get("dist_ma20")),
        "rsi14": None if pd.isna(row.get("rsi14")) else float(row.get("rsi14")),
        "close_loc": None if pd.isna(row.get("close_loc")) else float(row.get("close_loc")),
        "upper_wick_ratio": None
        if pd.isna(row.get("upper_wick_ratio"))
        else float(row.get("upper_wick_ratio")),
        "industry_group": {
            "key": row.get("swing_industry_group_key"),
            "label": row.get("swing_industry_group_label"),
            "type": row.get("swing_industry_group_type"),
        },
    }


def swing_reason_tags(row: pd.Series) -> list[str]:
    values = row.get("swing_reason_tags")
    return values if isinstance(values, list) else []


def swing_risk_flags(row: pd.Series) -> list[str]:
    values = row.get("swing_risk_flags")
    return values if isinstance(values, list) else []


def materialize_swing_3_5d_snapshot(
    settings: Settings,
    *,
    as_of_date: date,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    prediction_frame: pd.DataFrame | None = None,
) -> Swing35DMaterializationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "materialize_swing_3_5d_snapshot",
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
                input_sources=["fact_daily_ohlcv", "dim_symbol", "fact_fundamentals_snapshot"],
                notes=f"Materialize 3~5D swing methodology snapshot for {as_of_date.isoformat()}",
            )
            try:
                frame = build_swing_3_5d_frame(
                    connection,
                    as_of_date=as_of_date,
                    settings=settings,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                    prediction_frame=prediction_frame,
                )
                artifact_paths: list[str] = []
                if not frame.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                frame,
                                base_dir=settings.paths.curated_dir,
                                dataset=SWING_3_5D_OUTPUT_DIR,
                                partitions={"as_of_date": as_of_date.isoformat()},
                                filename="swing_3_5d_snapshot.parquet",
                            )
                        )
                    )
                candidate_count = (
                    int(frame["swing_recommendation_pass"].fillna(False).astype(bool).sum())
                    if "swing_recommendation_pass" in frame.columns
                    else 0
                )
                notes = (
                    f"3~5D swing snapshot materialized. as_of_date={as_of_date.isoformat()} "
                    f"rows={len(frame)} candidates={candidate_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SWING_3_5D_VERSION,
                )
                return Swing35DMaterializationResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(frame),
                    candidate_count=candidate_count,
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
                    notes="3~5D swing snapshot materialization failed.",
                    error_message=str(exc),
                    ranking_version=SWING_3_5D_VERSION,
                )
                raise


def dumps_list(values: list[str]) -> str:
    return json.dumps(values, ensure_ascii=False)
