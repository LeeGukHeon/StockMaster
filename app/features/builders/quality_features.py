from __future__ import annotations

from datetime import date

import pandas as pd

from app.features.constants import CORE_FEATURES_FOR_MISSINGNESS


def build_data_quality_feature_frame(
    feature_frame: pd.DataFrame,
    *,
    as_of_date: date,
) -> pd.DataFrame:
    if feature_frame.empty:
        return pd.DataFrame(columns=["symbol"])

    frame = feature_frame.copy()
    if "fundamental_coverage_flag" not in frame.columns:
        frame["fundamental_coverage_flag"] = 0.0
    if "news_coverage_flag" not in frame.columns:
        frame["news_coverage_flag"] = 0.0
    if "close" not in frame.columns:
        frame["close"] = pd.NA
    if "latest_price_date" not in frame.columns:
        frame["latest_price_date"] = pd.NaT
    for feature_name in CORE_FEATURES_FOR_MISSINGNESS:
        if feature_name not in frame.columns:
            frame[feature_name] = pd.NA

    frame["has_daily_ohlcv_flag"] = frame["close"].notna().astype(float)
    frame["has_fundamentals_flag"] = pd.to_numeric(
        frame["fundamental_coverage_flag"], errors="coerce"
    ).fillna(0.0)
    frame["has_news_flag"] = pd.to_numeric(
        frame["news_coverage_flag"], errors="coerce"
    ).fillna(0.0)
    frame["stale_price_flag"] = (
        pd.to_datetime(frame["latest_price_date"]).dt.date.ne(as_of_date)
        | frame["latest_price_date"].isna()
    ).astype(float)
    frame["missing_key_feature_count"] = (
        frame[list(CORE_FEATURES_FOR_MISSINGNESS)].isna().sum(axis=1)
    )

    coverage_ratio = 1.0 - (
        frame["missing_key_feature_count"] / max(len(CORE_FEATURES_FOR_MISSINGNESS), 1)
    )
    score = (
        frame["has_daily_ohlcv_flag"] * 40.0
        + frame["has_fundamentals_flag"] * 25.0
        + frame["has_news_flag"] * 10.0
        + (1.0 - frame["stale_price_flag"]) * 10.0
        + coverage_ratio.clip(lower=0.0) * 15.0
    )
    frame["data_confidence_score"] = score.clip(lower=0.0, upper=100.0)

    return frame[
        [
            "symbol",
            "has_daily_ohlcv_flag",
            "has_fundamentals_flag",
            "has_news_flag",
            "stale_price_flag",
            "missing_key_feature_count",
            "data_confidence_score",
        ]
    ].copy()
