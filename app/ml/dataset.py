from __future__ import annotations

from dataclasses import dataclass
from datetime import date

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
              AND exit_date <= ?
              AND symbol IN (SELECT symbol FROM model_training_symbol_stage)
            ORDER BY as_of_date
            """,
            [train_end_date, *horizons, train_end_date],
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
    data_missing = missing_count.ge(2.0) | data_confidence.lt(0.60) | stale_price.gt(0.0)

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
    """Return-unit D5 target with gentler winner haircuts and stronger loss ranking.

    V1 improved blocker exposure but weakened the alpha signal by multiplying several
    positive-return risk haircuts together. V2 keeps the same point-in-time inputs, but
    applies an additive risk budget so buyable winners remain distinguishable while risky
    losers are still pushed down.
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
    data_missing = missing_count.ge(2.0) | data_confidence.lt(0.60) | stale_price.gt(0.0)

    risk_budget = (
        thin_liquidity.astype(float).mul(0.12)
        + high_volatility.astype(float).mul(0.09)
        + large_drawdown.astype(float).mul(0.09)
        + data_missing.astype(float).mul(0.14)
    ).clip(lower=0.0, upper=0.42)
    positive_multiplier = (1.0 - risk_budget).clip(lower=0.58, upper=1.0)
    negative_multiplier = (1.0 + risk_budget.mul(0.75)).clip(lower=1.0, upper=1.32)

    return adjusted.where(adjusted <= 0.0, adjusted.mul(positive_multiplier)).where(
        adjusted > 0.0,
        adjusted.mul(negative_multiplier),
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

    connection.register("model_training_symbol_stage", symbol_frame[["symbol"]])
    try:
        horizon_placeholders = ",".join("?" for _ in horizons)
        label_rows = connection.execute(
            f"""
            SELECT
                as_of_date,
                symbol,
                market,
                horizon,
                excess_forward_return
            FROM fact_forward_return_label
            WHERE as_of_date <= ?
              AND horizon IN ({horizon_placeholders})
              AND label_available_flag
              AND exit_date <= ?
              AND symbol IN (SELECT symbol FROM model_training_symbol_stage)
            ORDER BY as_of_date, symbol, horizon
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
                FROM fact_forward_return_label
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
    dataset = feature_matrix.merge(label_matrix, on=["as_of_date", "symbol"], how="inner")
    dataset = dataset.merge(rank_label_matrix, on=["as_of_date", "symbol"], how="left")
    dataset = dataset.merge(top5_label_matrix, on=["as_of_date", "symbol"], how="left")
    dataset = dataset.merge(topbucket_label_matrix, on=["as_of_date", "symbol"], how="left")
    dataset = dataset.merge(buyable_label_matrix, on=["as_of_date", "symbol"], how="left")
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
