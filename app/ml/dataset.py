from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from glob import glob

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.constants import FEATURE_NAMES
from app.features.feature_store import (
    build_feature_store,
    feature_snapshot_has_required_quality_features,
)
from app.labels.forward_returns import build_forward_labels
from app.ml.constants import MARKET_REGIME_FEATURE_COLUMNS, MODEL_DATASET_VERSION
from app.pipelines._helpers import load_symbol_frame
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

MARKET_FEATURE_COLUMNS: tuple[str, ...] = ("market_is_kospi", "market_is_kosdaq")
TRAINING_FEATURE_COLUMNS: tuple[str, ...] = (
    FEATURE_NAMES + MARKET_FEATURE_COLUMNS + MARKET_REGIME_FEATURE_COLUMNS
)
DEFAULT_TRAINING_LABEL_MAX_REBUILD_DAYS = 10
FORWARD_LABEL_PARQUET_ENV = "STOCKMASTER_FORWARD_LABEL_PARQUET"
PATH_LABEL_OVERLAY_PARQUET_ENV = "STOCKMASTER_PATH_LABEL_OVERLAY_PARQUET"
REGIME_STATE_FEATURE_MAP: dict[str, str] = {
    "panic": "market_regime_panic_flag",
    "risk_off": "market_regime_risk_off_flag",
    "neutral": "market_regime_neutral_flag",
    "risk_on": "market_regime_risk_on_flag",
    "euphoria": "market_regime_euphoria_flag",
}
REGIME_NUMERIC_FEATURE_MAP: dict[str, str] = {
    "regime_score": "market_regime_score",
    "breadth_up_ratio": "market_breadth_up_ratio",
    "breadth_down_ratio": "market_breadth_down_ratio",
    "median_symbol_return_1d": "market_median_symbol_return_1d",
    "median_symbol_return_5d": "market_median_symbol_return_5d",
    "market_realized_vol_20d": "market_realized_vol_20d",
    "turnover_burst_z": "market_turnover_burst_z",
    "new_high_ratio_20d": "market_new_high_ratio_20d",
    "new_low_ratio_20d": "market_new_low_ratio_20d",
}


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
    external_label_source = _register_external_forward_label_view(connection)
    label_source_sql = _forward_label_source_sql(external_label_source)
    connection.register("model_training_symbol_stage", symbol_frame[["symbol"]])
    try:
        placeholders = ",".join("?" for _ in horizons)
        rows = connection.execute(
            f"""
            SELECT DISTINCT as_of_date
            FROM (
                {label_source_sql}
            )
            WHERE as_of_date <= ?
              AND horizon IN ({placeholders})
              AND label_available_flag
              AND exit_date <= ?
              AND symbol IN (SELECT symbol FROM model_training_symbol_stage)
            ORDER BY as_of_date
            """,
            [train_end_date, *horizons, train_end_date],
        ).fetchall()
    finally:
        connection.unregister("model_training_symbol_stage")
    return [pd.Timestamp(row[0]).date() for row in rows]


def _training_label_max_rebuild_days() -> int:
    raw_value = os.environ.get("STOCKMASTER_TRAINING_LABEL_MAX_REBUILD_DAYS")
    if raw_value is None or raw_value.strip() == "":
        return DEFAULT_TRAINING_LABEL_MAX_REBUILD_DAYS
    return max(0, int(raw_value))


def _external_forward_label_paths() -> list[str]:
    label_path = os.environ.get(FORWARD_LABEL_PARQUET_ENV)
    if not label_path:
        return []
    matched_paths = glob(label_path)
    if not matched_paths:
        raise RuntimeError(
            f"{FORWARD_LABEL_PARQUET_ENV} did not match any parquet files: {label_path}"
        )
    return matched_paths


def _resolve_missing_label_dates(
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
    if _external_forward_label_paths():
        return []

    trading_days = [
        pd.Timestamp(row[0]).date()
        for row in connection.execute(
            """
            SELECT trading_date
            FROM dim_trading_calendar
            WHERE is_trading_day
              AND trading_date <= ?
            ORDER BY trading_date
            """,
            [train_end_date],
        ).fetchall()
    ]
    if not trading_days:
        return []
    trading_day_index = {value: index for index, value in enumerate(trading_days)}
    candidate_dates: set[date] = set()
    for trading_date in trading_days:
        date_index = trading_day_index[trading_date]
        if all(date_index + int(horizon) < len(trading_days) for horizon in horizons):
            candidate_dates.add(trading_date)
    if not candidate_dates:
        return []

    external_label_source = _register_external_forward_label_view(connection)
    label_source_sql = _forward_label_source_sql(external_label_source)
    connection.register("model_training_label_symbol_stage", symbol_frame[["symbol"]])
    try:
        horizon_placeholders = ",".join("?" for _ in horizons)
        existing_rows = connection.execute(
            f"""
            SELECT as_of_date, COUNT(DISTINCT horizon) AS horizon_count
            FROM (
                {label_source_sql}
            )
            WHERE as_of_date <= ?
              AND horizon IN ({horizon_placeholders})
              AND label_available_flag
              AND exit_date <= ?
              AND symbol IN (SELECT symbol FROM model_training_label_symbol_stage)
            GROUP BY as_of_date
            """,
            [train_end_date, *horizons, train_end_date],
        ).fetchall()
    finally:
        connection.unregister("model_training_label_symbol_stage")
    complete_dates = {
        pd.Timestamp(row[0]).date() for row in existing_rows if int(row[1] or 0) >= len(horizons)
    }
    return sorted(candidate_dates.difference(complete_dates))


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
        valid_dates = {
            value
            for value in existing_dates
            if feature_snapshot_has_required_quality_features(connection, as_of_date=value)
        }
    missing_dates = [value for value in candidate_dates if value not in existing_dates]
    invalid_dates = [
        value
        for value in candidate_dates
        if value in existing_dates and value not in valid_dates
    ]
    for missing_date in missing_dates:
        build_feature_store(
            settings,
            as_of_date=missing_date,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
            cutoff_time="17:30",
        )
    for invalid_date in invalid_dates:
        build_feature_store(
            settings,
            as_of_date=invalid_date,
            symbols=symbols,
            limit_symbols=limit_symbols,
            market=market,
            cutoff_time="17:30",
            force=True,
        )
    return [*missing_dates, *invalid_dates]


def _market_scope_for_row(market: object) -> str:
    market_value = str(market or "").upper()
    if market_value in {"KOSPI", "KOSDAQ"}:
        return market_value
    return "KR_ALL"


def _empty_market_regime_features(frame: pd.DataFrame) -> pd.DataFrame:
    augmented = frame.copy()
    for feature_name in MARKET_REGIME_FEATURE_COLUMNS:
        if feature_name not in augmented.columns:
            augmented[feature_name] = pd.NA
    augmented["market_regime_coverage_flag"] = 0.0
    for feature_name in REGIME_STATE_FEATURE_MAP.values():
        augmented[feature_name] = 0.0
    return augmented


def augment_market_regime_features(connection, frame: pd.DataFrame) -> pd.DataFrame:
    """Attach point-in-time market regime features to a training/inference frame.

    Regime rows are market-level, so each symbol row uses its own KOSPI/KOSDAQ scope when
    available and falls back to KR_ALL for broad-market coverage. The join is strictly
    same-date and therefore does not use future information.
    """
    if frame.empty:
        return _empty_market_regime_features(frame)
    if "as_of_date" not in frame.columns or "market" not in frame.columns:
        return _empty_market_regime_features(frame)

    augmented = frame.copy()
    augmented["as_of_date"] = pd.to_datetime(augmented["as_of_date"]).dt.date
    dates = sorted(date_value for date_value in augmented["as_of_date"].dropna().unique())
    if not dates:
        return _empty_market_regime_features(augmented)

    placeholders = ",".join("?" for _ in dates)
    regime = connection.execute(
        f"""
        SELECT
            as_of_date,
            market_scope,
            breadth_up_ratio,
            breadth_down_ratio,
            median_symbol_return_1d,
            median_symbol_return_5d,
            market_realized_vol_20d,
            turnover_burst_z,
            new_high_ratio_20d,
            new_low_ratio_20d,
            regime_state,
            regime_score
        FROM fact_market_regime_snapshot
        WHERE as_of_date IN ({placeholders})
        """,
        dates,
    ).fetchdf()
    if regime.empty:
        return _empty_market_regime_features(augmented)

    regime["as_of_date"] = pd.to_datetime(regime["as_of_date"]).dt.date
    regime["market_scope"] = regime["market_scope"].astype(str).str.upper()
    preferred = augmented[["as_of_date", "market"]].copy()
    preferred["__row_id"] = augmented.index
    preferred["market_scope"] = preferred["market"].map(_market_scope_for_row)

    preferred_regime = preferred.merge(
        regime,
        on=["as_of_date", "market_scope"],
        how="left",
    ).set_index("__row_id")
    fallback_regime = (
        preferred[["__row_id", "as_of_date"]]
        .merge(
            regime.loc[regime["market_scope"].eq("KR_ALL")],
            on="as_of_date",
            how="left",
        )
        .set_index("__row_id")
    )

    matched = preferred_regime["regime_state"].notna()
    fallback_matched = fallback_regime["regime_state"].notna()
    selected_state = preferred_regime["regime_state"].where(
        matched,
        fallback_regime["regime_state"],
    )

    for source_column, feature_name in REGIME_NUMERIC_FEATURE_MAP.items():
        selected = preferred_regime[source_column].where(matched, fallback_regime[source_column])
        augmented[feature_name] = pd.to_numeric(selected.reindex(augmented.index), errors="coerce")

    selected_state = selected_state.reindex(augmented.index).fillna("").astype(str).str.lower()
    for state_name, feature_name in REGIME_STATE_FEATURE_MAP.items():
        augmented[feature_name] = selected_state.eq(state_name).astype(float)
    augmented["market_regime_coverage_flag"] = (matched | fallback_matched).reindex(
        augmented.index,
        fill_value=False,
    ).astype(float)
    return augmented


def _buyable_candidate_scores(label_rows: pd.DataFrame) -> pd.Series:
    """Return an outlier-capped, downside-aware target for actually buyable D5 names."""
    if label_rows.empty:
        return pd.Series(dtype="float64")

    scores = pd.Series(0.0, index=label_rows.index, dtype="float64")
    grouped = label_rows.groupby(["as_of_date", "horizon", "market"], sort=False)
    for _, group in grouped:
        returns = pd.to_numeric(group["excess_forward_return"], errors="coerce")
        valid = returns.dropna()
        if valid.empty:
            continue
        lower = max(float(valid.quantile(0.05)), -0.10)
        upper = min(float(valid.quantile(0.95)), 0.12)
        if upper <= lower:
            lower = float(valid.min())
            upper = float(valid.max())
        clipped = returns.clip(lower=lower, upper=upper)
        if upper > lower:
            scaled_return = clipped.sub(lower).div(upper - lower).clip(0.0, 1.0)
        else:
            scaled_return = pd.Series(0.5, index=group.index, dtype="float64")
        robust_rank = clipped.rank(method="average", pct=True).fillna(0.0)
        positive = returns.gt(0.0).astype(float)
        severe_loss_threshold = -0.035 if int(group["horizon"].iloc[0]) <= 1 else -0.07
        severe_loss = returns.le(severe_loss_threshold)
        raw = (
            robust_rank.mul(0.45)
            .add(positive.mul(0.35))
            .add(scaled_return.mul(0.20))
            .clip(0.0, 1.0)
        )
        raw = raw.where(returns.gt(0.0), raw.mul(0.35))
        raw = raw.where(~severe_loss, raw.mul(0.15))
        raw = raw.where(returns.le(upper), raw.clip(upper=0.90))
        scores.loc[group.index] = raw.fillna(0.0)
    return scores


def _numeric_feature_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _date_market_rank(
    frame: pd.DataFrame,
    column: str,
    *,
    ascending: bool = True,
) -> pd.Series:
    values = _numeric_feature_series(frame, column)
    return values.groupby([frame["as_of_date"], frame["market"]]).rank(
        method="average",
        pct=True,
        ascending=ascending,
    )


def _practical_excess_return_targets(
    feature_label_frame: pd.DataFrame,
    *,
    horizon: int,
) -> pd.Series:
    """Return an excess-return-unit target penalized by point-in-time buyability risk.

    This target keeps the downstream prediction unit as excess return while reducing the
    training reward for names that only worked as high-risk/low-buyability outliers.
    Risk inputs are same-date features only; no news or intraday fields are used.
    """

    target_column = f"target_h{int(horizon)}"
    if feature_label_frame.empty or target_column not in feature_label_frame.columns:
        return pd.Series(dtype="float64")

    working = feature_label_frame.copy()
    returns = pd.to_numeric(working[target_column], errors="coerce")
    adjusted = pd.Series(0.0, index=working.index, dtype="float64")

    for _, group in working.groupby(["as_of_date", "market"], sort=False):
        group_returns = returns.reindex(group.index)
        valid = group_returns.dropna()
        if valid.empty:
            continue
        lower = max(float(valid.quantile(0.05)), -0.10)
        upper = min(float(valid.quantile(0.95)), 0.12)
        if upper <= lower:
            lower = float(valid.min())
            upper = float(valid.max())
        adjusted.loc[group.index] = group_returns.clip(lower=lower, upper=upper).fillna(0.0)

    liquidity_rank = _numeric_feature_series(working, "liquidity_rank_pct")
    adv_rank = _date_market_rank(working, "adv_20", ascending=True)
    vol_rank = _date_market_rank(working, "realized_vol_20d", ascending=True)
    drawdown_rank = _date_market_rank(working, "drawdown_20d", ascending=True)
    max_loss_rank = _date_market_rank(working, "max_loss_20d", ascending=True)
    missing_count = _numeric_feature_series(working, "missing_key_feature_count").fillna(99.0)
    data_confidence = _numeric_feature_series(working, "data_confidence_score").fillna(0.0)
    stale_price = _numeric_feature_series(working, "stale_price_flag").fillna(1.0)

    thin_liquidity = liquidity_rank.le(0.10).fillna(False) | adv_rank.le(0.10).fillna(False)
    high_volatility = vol_rank.ge(0.90).fillna(False)
    large_drawdown = drawdown_rank.le(0.10).fillna(False) | max_loss_rank.le(0.10).fillna(False)
    data_missing = missing_count.ge(2.0) | data_confidence.lt(60.0) | stale_price.gt(0.0)

    positive_penalty = pd.Series(1.0, index=working.index, dtype="float64")
    positive_penalty = positive_penalty.where(~thin_liquidity, positive_penalty.mul(0.35))
    positive_penalty = positive_penalty.where(~high_volatility, positive_penalty.mul(0.55))
    positive_penalty = positive_penalty.where(~large_drawdown, positive_penalty.mul(0.55))
    positive_penalty = positive_penalty.where(~data_missing, positive_penalty.mul(0.50))
    risk_count = (
        thin_liquidity.astype(float)
        + high_volatility.astype(float)
        + large_drawdown.astype(float)
        + data_missing.astype(float)
    )
    negative_multiplier = 1.0 + risk_count.mul(0.15)

    return adjusted.where(adjusted <= 0.0, adjusted.mul(positive_penalty)).where(
        adjusted > 0.0,
        adjusted.mul(negative_multiplier),
    )


def _practical_excess_return_v2_targets(
    feature_label_frame: pd.DataFrame,
    *,
    horizon: int,
) -> pd.Series:
    """Path-aware return-unit D5 target for the v2 practical-selection experiment.

    A fixed D+5 close target does not match how the product is used: users can take
    profit before the vertical time barrier. When available, train v2 on the
    conservative +5% take-profit / -3% stop-loss path label while preserving the
    existing practical risk haircut. Older databases without path columns fall back
    to the endpoint target.
    """
    path_column = f"path_excess_tp5_sl3_h{int(horizon)}"
    endpoint_column = f"target_h{int(horizon)}"
    if path_column not in feature_label_frame.columns:
        return _practical_excess_return_targets(feature_label_frame, horizon=int(horizon))
    working = feature_label_frame.copy()
    path_target = pd.to_numeric(working[path_column], errors="coerce")
    if path_target.notna().sum() == 0:
        return _practical_excess_return_targets(feature_label_frame, horizon=int(horizon))
    if endpoint_column not in working.columns:
        working[endpoint_column] = path_target
    else:
        working[endpoint_column] = path_target.combine_first(
            pd.to_numeric(working[endpoint_column], errors="coerce")
        )
    return _practical_excess_return_targets(working, horizon=int(horizon))


def _stable_practical_excess_return_targets(
    feature_label_frame: pd.DataFrame,
    *,
    horizon: int,
) -> pd.Series:
    """Return a tighter D5 utility target for stable, actually-buyable baskets.

    This target is intentionally more conservative than
    ``_practical_excess_return_targets``. It still uses only point-in-time features,
    but it caps one-off winners more tightly and amplifies losses when the same-date
    buyability profile was already fragile. The goal is to train a shadow D5 model
    for stable top3/top5 basket selection rather than realized-return outlier chasing.
    """

    target_column = f"target_h{int(horizon)}"
    if feature_label_frame.empty or target_column not in feature_label_frame.columns:
        return pd.Series(dtype="float64")

    working = feature_label_frame.copy()
    returns = pd.to_numeric(working[target_column], errors="coerce")
    adjusted = pd.Series(0.0, index=working.index, dtype="float64")

    for _, group in working.groupby(["as_of_date", "market"], sort=False):
        group_returns = returns.reindex(group.index)
        valid = group_returns.dropna()
        if valid.empty:
            continue
        lower = max(float(valid.quantile(0.08)), -0.08)
        upper = min(float(valid.quantile(0.92)), 0.10)
        if upper <= lower:
            lower = float(valid.min())
            upper = float(valid.max())
        adjusted.loc[group.index] = group_returns.clip(lower=lower, upper=upper).fillna(0.0)

    liquidity_rank = _numeric_feature_series(working, "liquidity_rank_pct")
    adv_rank = _date_market_rank(working, "adv_20", ascending=True)
    vol_rank = _date_market_rank(working, "realized_vol_20d", ascending=True)
    day_range_rank = _date_market_rank(working, "hl_range_1d", ascending=True)
    drawdown_rank = _date_market_rank(working, "drawdown_20d", ascending=True)
    max_loss_rank = _date_market_rank(working, "max_loss_20d", ascending=True)
    crowding_rank = _date_market_rank(working, "dist_from_20d_high", ascending=True)
    turnover_burst_rank = _date_market_rank(working, "volume_ratio_1d_vs_20d", ascending=True)
    missing_count = _numeric_feature_series(working, "missing_key_feature_count").fillna(99.0)
    data_confidence = _numeric_feature_series(working, "data_confidence_score").fillna(0.0)
    stale_price = _numeric_feature_series(working, "stale_price_flag").fillna(1.0)

    thin_liquidity = liquidity_rank.le(0.12).fillna(False) | adv_rank.le(0.12).fillna(False)
    high_volatility = vol_rank.ge(0.88).fillna(False) | day_range_rank.ge(0.90).fillna(False)
    large_drawdown = drawdown_rank.le(0.12).fillna(False) | max_loss_rank.le(0.12).fillna(False)
    late_crowding = crowding_rank.ge(0.90).fillna(False) & turnover_burst_rank.ge(
        0.85
    ).fillna(False)
    data_missing = missing_count.ge(2.0) | data_confidence.lt(65.0) | stale_price.gt(0.0)

    positive_penalty = pd.Series(1.0, index=working.index, dtype="float64")
    positive_penalty = positive_penalty.where(~thin_liquidity, positive_penalty.mul(0.25))
    positive_penalty = positive_penalty.where(~high_volatility, positive_penalty.mul(0.45))
    positive_penalty = positive_penalty.where(~large_drawdown, positive_penalty.mul(0.45))
    positive_penalty = positive_penalty.where(~late_crowding, positive_penalty.mul(0.60))
    positive_penalty = positive_penalty.where(~data_missing, positive_penalty.mul(0.35))

    risk_count = (
        thin_liquidity.astype(float)
        + high_volatility.astype(float)
        + large_drawdown.astype(float)
        + late_crowding.astype(float)
        + data_missing.astype(float)
    )
    severe_loss_threshold = -0.035 if int(horizon) <= 1 else -0.06
    realized_severe_loss = returns.le(severe_loss_threshold).fillna(False)
    negative_multiplier = 1.0 + risk_count.mul(0.20) + realized_severe_loss.astype(float).mul(0.30)

    return adjusted.where(adjusted <= 0.0, adjusted.mul(positive_penalty)).where(
        adjusted > 0.0,
        adjusted.mul(negative_multiplier),
    )


def _robust_buyable_excess_return_targets(
    feature_label_frame: pd.DataFrame,
    *,
    horizon: int,
) -> pd.Series:
    """Return a conservative D5 target for stable, actually-buyable candidates.

    The target remains in excess-return units so model outputs keep the same business
    interpretation, but it deliberately avoids rewarding one-off spikes that were
    fragile at the close. All penalties use only same-date features; the future
    return is used only as the supervised label.
    """

    target_column = f"target_h{int(horizon)}"
    if feature_label_frame.empty or target_column not in feature_label_frame.columns:
        return pd.Series(dtype="float64")

    working = feature_label_frame.copy()
    returns = pd.to_numeric(working[target_column], errors="coerce")
    adjusted = pd.Series(0.0, index=working.index, dtype="float64")

    for _, group in working.groupby(["as_of_date", "market"], sort=False):
        group_returns = returns.reindex(group.index)
        valid = group_returns.dropna()
        if valid.empty:
            continue
        lower = max(float(valid.quantile(0.10)), -0.06)
        upper = min(float(valid.quantile(0.85)), 0.06)
        if upper <= lower:
            lower = float(valid.min())
            upper = float(valid.max())
        adjusted.loc[group.index] = group_returns.clip(lower=lower, upper=upper).fillna(0.0)

    liquidity_rank = _numeric_feature_series(working, "liquidity_rank_pct")
    adv_rank = _date_market_rank(working, "adv_20", ascending=True)
    vol_rank = _date_market_rank(working, "realized_vol_20d", ascending=True)
    day_range_rank = _date_market_rank(working, "hl_range_1d", ascending=True)
    drawdown_rank = _date_market_rank(working, "drawdown_20d", ascending=True)
    max_loss_rank = _date_market_rank(working, "max_loss_20d", ascending=True)
    crowding_rank = _date_market_rank(working, "dist_from_20d_high", ascending=True)
    turnover_burst_rank = _date_market_rank(working, "volume_ratio_1d_vs_20d", ascending=True)
    missing_count = _numeric_feature_series(working, "missing_key_feature_count").fillna(99.0)
    data_confidence = _numeric_feature_series(working, "data_confidence_score").fillna(0.0)
    stale_price = _numeric_feature_series(working, "stale_price_flag").fillna(1.0)
    regime_coverage = _numeric_feature_series(working, "market_regime_coverage_flag").fillna(0.0)
    panic_regime = _numeric_feature_series(working, "market_regime_panic_flag").fillna(0.0)
    risk_off_regime = _numeric_feature_series(working, "market_regime_risk_off_flag").fillna(0.0)
    breadth_up = _numeric_feature_series(working, "market_breadth_up_ratio")
    breadth_down = _numeric_feature_series(working, "market_breadth_down_ratio")

    thin_liquidity = liquidity_rank.le(0.18).fillna(False) | adv_rank.le(0.18).fillna(False)
    high_volatility = vol_rank.ge(0.82).fillna(False) | day_range_rank.ge(0.85).fillna(False)
    large_drawdown = drawdown_rank.le(0.15).fillna(False) | max_loss_rank.le(0.15).fillna(False)
    late_crowding = crowding_rank.ge(0.88).fillna(False) & turnover_burst_rank.ge(
        0.80
    ).fillna(False)
    data_missing = missing_count.ge(2.0) | data_confidence.lt(70.0) | stale_price.gt(0.0)
    weak_regime = regime_coverage.gt(0.5) & (
        panic_regime.gt(0.5)
        | risk_off_regime.gt(0.5)
        | breadth_up.lt(0.35).fillna(False)
        | breadth_down.gt(0.55).fillna(False)
    )

    positive_penalty = pd.Series(1.0, index=working.index, dtype="float64")
    positive_penalty = positive_penalty.where(~thin_liquidity, positive_penalty.mul(0.20))
    positive_penalty = positive_penalty.where(~high_volatility, positive_penalty.mul(0.35))
    positive_penalty = positive_penalty.where(~large_drawdown, positive_penalty.mul(0.35))
    positive_penalty = positive_penalty.where(~late_crowding, positive_penalty.mul(0.50))
    positive_penalty = positive_penalty.where(~data_missing, positive_penalty.mul(0.25))
    positive_penalty = positive_penalty.where(~weak_regime, positive_penalty.mul(0.60))

    risk_count = (
        thin_liquidity.astype(float)
        + high_volatility.astype(float)
        + large_drawdown.astype(float)
        + late_crowding.astype(float)
        + data_missing.astype(float)
        + weak_regime.astype(float)
    )
    severe_loss_threshold = -0.035 if int(horizon) <= 1 else -0.045
    realized_severe_loss = returns.le(severe_loss_threshold).fillna(False)
    negative_multiplier = (
        1.0
        + risk_count.mul(0.30)
        + realized_severe_loss.astype(float).mul(0.50)
        + weak_regime.astype(float).mul(0.15)
    )

    positive_adjusted = adjusted.mul(positive_penalty)
    hard_blocker = thin_liquidity | large_drawdown | data_missing
    positive_adjusted = positive_adjusted.where(
        ~(positive_adjusted.gt(0.0) & hard_blocker),
        positive_adjusted.clip(upper=0.002),
    )
    return positive_adjusted.where(adjusted > 0.0, adjusted.mul(negative_multiplier))


def _register_external_forward_label_view(connection) -> bool:
    matched_paths = _external_forward_label_paths()
    if not matched_paths:
        return False
    label_frame = connection.execute(
        """
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            market,
            entry_date,
            exit_date,
            excess_forward_return,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            label_available_flag,
            created_at
        FROM read_parquet(?)
        """,
        [matched_paths],
    ).fetchdf()
    connection.register("external_forward_label_frame", label_frame)
    connection.execute(
        """
        CREATE OR REPLACE TEMP VIEW external_forward_label AS
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            market,
            entry_date,
            exit_date,
            excess_forward_return,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            label_available_flag,
            created_at
        FROM external_forward_label_frame
        """
    )
    return True


def _forward_label_source_sql(include_external: bool) -> str:
    base_source = """
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            market,
            entry_date,
            exit_date,
            excess_forward_return,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            label_available_flag,
            created_at,
            0 AS source_priority
        FROM fact_forward_return_label
    """
    if not include_external:
        return base_source
    return (
        """
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            market,
            entry_date,
            exit_date,
            excess_forward_return,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            label_available_flag,
            created_at,
            1 AS source_priority
        FROM external_forward_label
        UNION ALL
        """
        + base_source
    )


def _register_external_path_overlay_view(connection) -> bool:
    overlay_path = os.environ.get(PATH_LABEL_OVERLAY_PARQUET_ENV)
    if not overlay_path:
        return False
    matched_paths = glob(overlay_path)
    if not matched_paths:
        raise RuntimeError(
            f"{PATH_LABEL_OVERLAY_PARQUET_ENV} did not match any parquet files: {overlay_path}"
        )
    overlay_frame = connection.execute(
        """
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            created_at
        FROM read_parquet(?)
        """,
        [matched_paths],
    ).fetchdf()
    connection.register("external_forward_path_label_frame", overlay_frame)
    connection.execute(
        """
        CREATE OR REPLACE TEMP VIEW external_forward_path_label AS
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            created_at
        FROM external_forward_path_label_frame
        """
    )
    return True


def _path_overlay_source_sql(include_external: bool) -> str:
    base_source = """
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            created_at,
            0 AS source_priority
        FROM fact_forward_return_path_label
    """
    if not include_external:
        return base_source
    return (
        """
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            created_at,
            1 AS source_priority
        FROM external_forward_path_label
        UNION ALL
        """
        + base_source
    )


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

    external_label_source = _register_external_forward_label_view(connection)
    label_source_sql = _forward_label_source_sql(external_label_source)
    external_overlay_source = _register_external_path_overlay_view(connection)
    path_overlay_source_sql = _path_overlay_source_sql(external_overlay_source)

    connection.register("model_training_symbol_stage", symbol_frame[["symbol"]])
    try:
        horizon_placeholders = ",".join("?" for _ in horizons)
        label_rows = connection.execute(
            f"""
            SELECT
                label.as_of_date,
                label.symbol,
                label.market,
                label.horizon,
                label.excess_forward_return,
                COALESCE(
                    path.path_excess_return_tp3_sl3_conservative,
                    label.path_excess_return_tp3_sl3_conservative
                ) AS path_excess_return_tp3_sl3_conservative,
                COALESCE(
                    path.path_excess_return_tp5_sl3_conservative,
                    label.path_excess_return_tp5_sl3_conservative
                ) AS path_excess_return_tp5_sl3_conservative
            FROM (
                SELECT *
                FROM (
                    {label_source_sql}
                )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY as_of_date, symbol, horizon
                    ORDER BY source_priority DESC, created_at DESC, run_id DESC
                ) = 1
            ) AS label
            LEFT JOIN (
                SELECT
                    as_of_date,
                    symbol,
                    horizon,
                    path_excess_return_tp3_sl3_conservative,
                    path_excess_return_tp5_sl3_conservative,
                    created_at,
                    run_id,
                    source_priority
                FROM (
                    {path_overlay_source_sql}
                )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY as_of_date, symbol, horizon
                    ORDER BY source_priority DESC, created_at DESC, run_id DESC
                ) = 1
            ) AS path
              ON label.as_of_date = path.as_of_date
             AND label.symbol = path.symbol
             AND label.horizon = path.horizon
            WHERE label.as_of_date <= ?
              AND label.horizon IN ({horizon_placeholders})
              AND label.label_available_flag
              AND label.exit_date <= ?
              AND label.symbol IN (SELECT symbol FROM model_training_symbol_stage)
            ORDER BY label.as_of_date, label.symbol, label.horizon
            """,
            [train_end_date, *horizons, train_end_date],
        ).fetchdf()
        if label_rows.empty:
            return pd.DataFrame()

        label_rows["target_rank"] = (
            label_rows.groupby(["as_of_date", "horizon", "market"])["excess_forward_return"]
            .rank(method="average", pct=True)
        )
        label_rows["target_top5"] = 0.0
        top5_indices = (
            label_rows.sort_values(
                ["as_of_date", "horizon", "excess_forward_return", "symbol"],
                ascending=[True, True, False, True],
            )
            .groupby(["as_of_date", "horizon"], sort=True, group_keys=False)
            .head(5)
            .index
        )
        label_rows.loc[top5_indices, "target_top5"] = 1.0
        label_rows["target_topbucket"] = 0.0
        ordered_rows = label_rows.sort_values(
            ["as_of_date", "horizon", "excess_forward_return", "symbol"],
            ascending=[True, True, False, True],
        )
        top20_indices = (
            ordered_rows.groupby(["as_of_date", "horizon"], sort=True, group_keys=False)
            .head(20)
            .index
        )
        top10_indices = (
            ordered_rows.groupby(["as_of_date", "horizon"], sort=True, group_keys=False)
            .head(10)
            .index
        )
        label_rows.loc[top20_indices, "target_topbucket"] = 0.25
        label_rows.loc[top10_indices, "target_topbucket"] = 0.5
        label_rows.loc[top5_indices, "target_topbucket"] = 1.0
        label_rows["target_buyable"] = _buyable_candidate_scores(label_rows)

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
                FROM (
                    {label_source_sql}
                )
                WHERE as_of_date <= ?
                  AND horizon IN ({horizon_placeholders})
                  AND label_available_flag
                  AND exit_date <= ?
                  AND symbol IN (SELECT symbol FROM model_training_symbol_stage)
            )
              AND snapshot.symbol IN (SELECT symbol FROM model_training_symbol_stage)
            ORDER BY snapshot.as_of_date, snapshot.symbol, snapshot.feature_name
            """,
            [train_end_date, *horizons, train_end_date],
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
    rank_label_matrix = (
        label_rows.assign(
            target_name=label_rows["horizon"].map(lambda value: f"target_rank_h{int(value)}")
        )
        .pivot(
            index=["as_of_date", "symbol"],
            columns="target_name",
            values="target_rank",
        )
        .reset_index()
    )
    top5_label_matrix = (
        label_rows.assign(
            target_name=label_rows["horizon"].map(lambda value: f"target_top5_h{int(value)}")
        )
        .pivot(
            index=["as_of_date", "symbol"],
            columns="target_name",
            values="target_top5",
        )
        .reset_index()
    )
    topbucket_label_matrix = (
        label_rows.assign(
            target_name=label_rows["horizon"].map(lambda value: f"target_topbucket_h{int(value)}")
        )
        .pivot(
            index=["as_of_date", "symbol"],
            columns="target_name",
            values="target_topbucket",
        )
        .reset_index()
    )
    buyable_label_matrix = (
        label_rows.assign(
            target_name=label_rows["horizon"].map(lambda value: f"target_buyable_h{int(value)}")
        )
        .pivot(
            index=["as_of_date", "symbol"],
            columns="target_name",
            values="target_buyable",
        )
        .reset_index()
    )
    path_tp5_sl3_label_matrix = (
        label_rows.assign(
            target_name=label_rows["horizon"].map(
                lambda value: f"path_excess_tp5_sl3_h{int(value)}"
            )
        )
        .pivot(
            index=["as_of_date", "symbol"],
            columns="target_name",
            values="path_excess_return_tp5_sl3_conservative",
        )
        .reset_index()
    )
    dataset = feature_matrix.merge(label_matrix, on=["as_of_date", "symbol"], how="inner")
    dataset = dataset.merge(rank_label_matrix, on=["as_of_date", "symbol"], how="left")
    dataset = dataset.merge(top5_label_matrix, on=["as_of_date", "symbol"], how="left")
    dataset = dataset.merge(topbucket_label_matrix, on=["as_of_date", "symbol"], how="left")
    dataset = dataset.merge(buyable_label_matrix, on=["as_of_date", "symbol"], how="left")
    dataset = dataset.merge(path_tp5_sl3_label_matrix, on=["as_of_date", "symbol"], how="left")
    dataset = dataset.merge(
        symbol_frame[["symbol", "company_name", "market"]],
        on="symbol",
        how="left",
    )
    dataset["market_is_kospi"] = dataset["market"].eq("KOSPI").astype(float)
    dataset["market_is_kosdaq"] = dataset["market"].eq("KOSDAQ").astype(float)
    dataset = augment_market_regime_features(connection, dataset)
    for horizon in horizons:
        dataset[f"target_practical_excess_h{int(horizon)}"] = _practical_excess_return_targets(
            dataset,
            horizon=int(horizon),
        )
        dataset[f"target_practical_excess_v2_h{int(horizon)}"] = (
            _practical_excess_return_v2_targets(
                dataset,
                horizon=int(horizon),
            )
        )
        dataset[f"target_stable_practical_excess_h{int(horizon)}"] = (
            _stable_practical_excess_return_targets(
                dataset,
                horizon=int(horizon),
            )
        )
        dataset[f"target_robust_buyable_excess_h{int(horizon)}"] = (
            _robust_buyable_excess_return_targets(
                dataset,
                horizon=int(horizon),
            )
        )
    dataset["as_of_date"] = pd.to_datetime(dataset["as_of_date"]).dt.date
    for feature_name in FEATURE_NAMES:
        if feature_name not in dataset.columns:
            dataset[feature_name] = pd.NA
    for target_name in [f"target_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    for target_name in [f"target_rank_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    for target_name in [f"target_top5_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    for target_name in [f"target_topbucket_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    for target_name in [f"target_buyable_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    for target_name in [f"target_practical_excess_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    for target_name in [f"target_practical_excess_v2_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    for target_name in [
        f"target_stable_practical_excess_h{int(horizon)}" for horizon in horizons
    ]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    for target_name in [f"target_robust_buyable_excess_h{int(horizon)}" for horizon in horizons]:
        if target_name not in dataset.columns:
            dataset[target_name] = pd.NA
    ordered_columns = [
        "as_of_date",
        "symbol",
        "company_name",
        "market",
        *TRAINING_FEATURE_COLUMNS,
        *[f"target_h{int(horizon)}" for horizon in horizons],
        *[f"target_rank_h{int(horizon)}" for horizon in horizons],
        *[f"target_top5_h{int(horizon)}" for horizon in horizons],
        *[f"target_topbucket_h{int(horizon)}" for horizon in horizons],
        *[f"target_buyable_h{int(horizon)}" for horizon in horizons],
        *[f"target_practical_excess_h{int(horizon)}" for horizon in horizons],
        *[f"target_practical_excess_v2_h{int(horizon)}" for horizon in horizons],
        *[f"target_stable_practical_excess_h{int(horizon)}" for horizon in horizons],
        *[f"target_robust_buyable_excess_h{int(horizon)}" for horizon in horizons],
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
                    "fact_forward_return_path_label",
                    "fact_market_regime_snapshot",
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
                missing_label_dates = _resolve_missing_label_dates(
                    connection,
                    train_end_date=train_end_date,
                    horizons=horizons,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
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

        max_rebuild_days = _training_label_max_rebuild_days()
        if len(missing_label_dates) > max_rebuild_days:
            raise RuntimeError(
                "Training label rebuild blocked because too many missing label dates would "
                "force a large online DuckDB rebuild. "
                f"missing_dates={len(missing_label_dates)} "
                f"max_rebuild_days={max_rebuild_days}. "
                "Run the offline path/base label maintenance rebuild first, or increase "
                "STOCKMASTER_TRAINING_LABEL_MAX_REBUILD_DAYS explicitly for a controlled run."
            )
        if missing_label_dates:
            build_forward_labels(
                settings,
                start_date=min(missing_label_dates),
                end_date=max(missing_label_dates),
                horizons=horizons,
                symbols=symbols,
                limit_symbols=limit_symbols,
                market=market,
                chunk_trading_days=1,
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
