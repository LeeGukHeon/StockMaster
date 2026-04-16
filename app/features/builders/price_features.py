from __future__ import annotations

import pandas as pd


def build_price_feature_frame(
    ohlcv_history: pd.DataFrame,
    *,
    as_of_date,
) -> pd.DataFrame:
    if ohlcv_history.empty:
        return pd.DataFrame(columns=["symbol"])

    history = ohlcv_history.sort_values(["symbol", "trading_date"]).copy()
    history["trading_date"] = pd.to_datetime(history["trading_date"]).dt.date
    group = history.groupby("symbol", group_keys=False)

    history["prev_close"] = group["close"].shift(1)
    history["ret_1d"] = history["close"] / history["prev_close"] - 1.0
    for window in (3, 5, 10, 20, 60):
        history[f"ret_{window}d"] = group["close"].pct_change(periods=window)

    for window in (3, 5, 10):
        history[f"market_ret_{window}d_median"] = history.groupby(
            ["trading_date", "market"], dropna=False
        )[f"ret_{window}d"].transform("median")
        history[f"residual_ret_{window}d"] = (
            history[f"ret_{window}d"] - history[f"market_ret_{window}d_median"]
        )

    for window in (5, 20, 60):
        history[f"ma_{window}"] = group["close"].transform(
            lambda series, w=window: series.rolling(w, min_periods=w).mean()
        )

    history["ma5_over_ma20"] = history["ma_5"] / history["ma_20"] - 1.0
    history["ma20_over_ma60"] = history["ma_20"] / history["ma_60"] - 1.0

    history["rolling_high_20"] = group["high"].transform(
        lambda series: series.rolling(20, min_periods=20).max()
    )
    history["rolling_high_60"] = group["high"].transform(
        lambda series: series.rolling(60, min_periods=60).max()
    )
    history["rolling_low_20"] = group["low"].transform(
        lambda series: series.rolling(20, min_periods=20).min()
    )
    history["rolling_close_high_20"] = group["close"].transform(
        lambda series: series.rolling(20, min_periods=20).max()
    )
    history["rolling_close_high_60"] = group["close"].transform(
        lambda series: series.rolling(60, min_periods=60).max()
    )

    history["dist_from_20d_high"] = history["close"] / history["rolling_high_20"] - 1.0
    history["dist_from_60d_high"] = history["close"] / history["rolling_high_60"] - 1.0
    history["dist_from_20d_low"] = history["close"] / history["rolling_low_20"] - 1.0

    range_denom = (history["high"] - history["low"]).replace(0, pd.NA)
    close_pos_in_day_range = pd.to_numeric(
        (history["close"] - history["low"]) / range_denom,
        errors="coerce",
    )
    history["close_pos_in_day_range"] = close_pos_in_day_range.fillna(0.5)
    history["up_day_count_5d"] = group["ret_1d"].transform(
        lambda series: series.gt(0).rolling(5, min_periods=5).sum()
    )
    history["up_day_count_20d"] = group["ret_1d"].transform(
        lambda series: series.gt(0).rolling(20, min_periods=20).sum()
    )
    history["drawdown_20d"] = history["close"] / history["rolling_close_high_20"] - 1.0
    history["drawdown_60d"] = history["close"] / history["rolling_close_high_60"] - 1.0

    history["realized_vol_5d"] = group["ret_1d"].transform(
        lambda series: series.rolling(5, min_periods=5).std(ddof=0)
    )
    history["realized_vol_10d"] = group["ret_1d"].transform(
        lambda series: series.rolling(10, min_periods=10).std(ddof=0)
    )
    history["realized_vol_20d"] = group["ret_1d"].transform(
        lambda series: series.rolling(20, min_periods=20).std(ddof=0)
    )
    history["hl_range_1d"] = history["high"] / history["low"].replace(0, pd.NA) - 1.0
    history["gap_open_1d"] = history["open"] / history["prev_close"] - 1.0
    history["gap_abs_avg_5d"] = group["gap_open_1d"].transform(
        lambda series: series.abs().rolling(5, min_periods=5).mean()
    )
    history["gap_abs_avg_20d"] = group["gap_open_1d"].transform(
        lambda series: series.abs().rolling(20, min_periods=20).mean()
    )
    history["max_loss_5d"] = group["ret_1d"].transform(
        lambda series: series.rolling(5, min_periods=5).min()
    )
    history["max_loss_20d"] = group["ret_1d"].transform(
        lambda series: series.rolling(20, min_periods=20).min()
    )

    latest = history.loc[history["trading_date"] == as_of_date].copy()
    if latest.empty:
        return pd.DataFrame(columns=["symbol"])
    return latest[
        [
            "symbol",
            "ret_1d",
            "ret_3d",
            "ret_5d",
            "ret_10d",
            "residual_ret_3d",
            "residual_ret_5d",
            "residual_ret_10d",
            "ret_20d",
            "ret_60d",
            "ma_5",
            "ma_20",
            "ma_60",
            "ma5_over_ma20",
            "ma20_over_ma60",
            "dist_from_20d_high",
            "dist_from_60d_high",
            "dist_from_20d_low",
            "close_pos_in_day_range",
            "up_day_count_5d",
            "up_day_count_20d",
            "drawdown_20d",
            "drawdown_60d",
            "realized_vol_5d",
            "realized_vol_10d",
            "realized_vol_20d",
            "hl_range_1d",
            "gap_open_1d",
            "gap_abs_avg_5d",
            "gap_abs_avg_20d",
            "max_loss_5d",
            "max_loss_20d",
        ]
    ].reset_index(drop=True)
