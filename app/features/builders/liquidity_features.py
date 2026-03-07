from __future__ import annotations

import pandas as pd


def build_liquidity_feature_frame(
    ohlcv_history: pd.DataFrame,
    *,
    as_of_date,
) -> pd.DataFrame:
    if ohlcv_history.empty:
        return pd.DataFrame(columns=["symbol"])

    history = ohlcv_history.sort_values(["symbol", "trading_date"]).copy()
    history["trading_date"] = pd.to_datetime(history["trading_date"]).dt.date
    history["turnover_effective"] = history["turnover_value"].fillna(
        history["close"] * history["volume"]
    )
    group = history.groupby("symbol", group_keys=False)

    history["volume_ma_20"] = group["volume"].transform(
        lambda series: series.rolling(20, min_periods=20).mean()
    )
    history["volume_ratio_1d_vs_20d"] = history["volume"] / history["volume_ma_20"]

    history["turnover_value_1d"] = history["turnover_effective"]
    history["turnover_value_ma_5"] = group["turnover_effective"].transform(
        lambda series: series.rolling(5, min_periods=5).mean()
    )
    history["turnover_value_ma_20"] = group["turnover_effective"].transform(
        lambda series: series.rolling(20, min_periods=20).mean()
    )
    history["turnover_std_20"] = group["turnover_effective"].transform(
        lambda series: series.rolling(20, min_periods=20).std(ddof=0)
    )
    history["turnover_z_5_20"] = (
        history["turnover_effective"] - history["turnover_value_ma_20"]
    ) / history["turnover_std_20"].replace(0, pd.NA)
    history["adv_20"] = history["turnover_value_ma_20"]
    history["adv_60"] = group["turnover_effective"].transform(
        lambda series: series.rolling(60, min_periods=60).mean()
    )

    latest = history.loc[history["trading_date"] == as_of_date].copy()
    if latest.empty:
        return pd.DataFrame(columns=["symbol"])
    return latest[
        [
            "symbol",
            "volume_ratio_1d_vs_20d",
            "turnover_value_1d",
            "turnover_value_ma_5",
            "turnover_value_ma_20",
            "turnover_z_5_20",
            "adv_20",
            "adv_60",
        ]
    ].reset_index(drop=True)
