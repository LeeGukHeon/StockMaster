from __future__ import annotations

import json

import pandas as pd

from app.features.constants import CORE_FEATURES_FOR_MISSINGNESS, PRICE_COVERAGE_FEATURES


def _row_value_present(row: pd.Series, feature_name: str) -> bool:
    return feature_name in row and pd.notna(row.get(feature_name))


def _price_core_present(row: pd.Series) -> bool:
    return all(_row_value_present(row, feature_name) for feature_name in PRICE_COVERAGE_FEATURES)


def _effective_has_daily_ohlcv(row: pd.Series) -> bool:
    return bool(row.get("has_daily_ohlcv_flag", 0) >= 1 or _price_core_present(row))


def _effective_stale_price(row: pd.Series) -> bool:
    latest_price_date = row.get("latest_price_date")
    as_of_date = row.get("as_of_date")
    if pd.notna(latest_price_date) and pd.notna(as_of_date):
        return pd.Timestamp(latest_price_date).date() != pd.Timestamp(as_of_date).date()
    if _price_core_present(row):
        return False
    return bool(row.get("stale_price_flag", 0) >= 1)


def _effective_missing_key_feature_count(row: pd.Series) -> int:
    return sum(
        1
        for feature_name in CORE_FEATURES_FOR_MISSINGNESS
        if not _row_value_present(row, feature_name)
    )


def _effective_data_confidence_score(row: pd.Series) -> float:
    missing_count = _effective_missing_key_feature_count(row)
    coverage_ratio = 1.0 - (missing_count / max(len(CORE_FEATURES_FOR_MISSINGNESS), 1))
    has_fundamentals = bool(
        row.get("has_fundamentals_flag", row.get("fundamental_coverage_flag", 0)) >= 1
        or row.get("fundamental_coverage_flag", 0) >= 1
    )
    score = (
        (45.0 if _effective_has_daily_ohlcv(row) else 0.0)
        + (25.0 if has_fundamentals else 0.0)
        + (0.0 if _effective_stale_price(row) else 15.0)
        + max(coverage_ratio, 0.0) * 15.0
    )
    return max(0.0, min(score, 100.0))


def build_reason_tags(row: pd.Series) -> list[str]:
    tags: list[str] = []
    if row.get("trend_momentum_score", 0) >= 65 and row.get("crowding_penalty_score", 0) < 65:
        tags.append("short_term_momentum_strong")
    if (
        pd.notna(row.get("dist_from_20d_high"))
        and row.get("dist_from_20d_high", -1) >= -0.05
        and row.get("crowding_penalty_score", 0) < 70
    ):
        tags.append("breakout_near_20d_high")
    if (
        row.get("turnover_participation_score", 0) >= 65
        and row.get("crowding_penalty_score", 0) < 70
    ):
        tags.append("turnover_surge")
    if row.get("quality_score", 0) >= 60:
        tags.append("quality_metrics_supportive")
    if pd.notna(row.get("drawdown_20d")) and row.get("drawdown_20d", -1) >= -0.08:
        tags.append("low_drawdown_relative")
    return tags[:3]


def build_risk_flags(row: pd.Series) -> list[str]:
    flags: list[str] = []
    if row.get("realized_vol_20d_rank_pct", 0) >= 0.85:
        flags.append("high_realized_volatility")
    if pd.notna(row.get("drawdown_20d")) and row.get("drawdown_20d", 0) <= -0.15:
        flags.append("large_recent_drawdown")
    if row.get("fundamental_coverage_flag", 0) < 1:
        flags.append("weak_fundamental_coverage")
    if row.get("adv_20_rank_pct", 0) <= 0.15:
        flags.append("thin_liquidity")
    if _effective_missing_key_feature_count(row) >= 4 or _effective_data_confidence_score(row) < 55:
        flags.append("data_missingness_high")
    return flags


def build_eligibility_notes(row: pd.Series, *, risk_flags: list[str]) -> str:
    notes: list[str] = []
    if not _effective_has_daily_ohlcv(row):
        notes.append("missing_price")
    if _effective_stale_price(row):
        notes.append("stale_price")
    if row.get("adv_20", 0) < 50_000_000:
        notes.append("adv20_below_threshold")
    if _effective_missing_key_feature_count(row) >= 5:
        notes.append("feature_missingness_high")
    notes.extend(risk_flags[:2])
    return json.dumps(sorted(set(notes)), ensure_ascii=False)
