from __future__ import annotations

import json

import pandas as pd


def build_reason_tags(row: pd.Series) -> list[str]:
    tags: list[str] = []
    if row.get("trend_momentum_score", 0) >= 65 and row.get("crowding_penalty_score", 0) < 65:
        tags.append("short_term_momentum_strong")
    if pd.notna(row.get("dist_from_20d_high")) and row.get("dist_from_20d_high", -1) >= -0.05 and row.get("crowding_penalty_score", 0) < 70:
        tags.append("breakout_near_20d_high")
    if row.get("turnover_participation_score", 0) >= 65 and row.get("crowding_penalty_score", 0) < 70:
        tags.append("turnover_surge")
    if row.get("fresh_news_flag", 0) >= 1 or row.get("news_catalyst_score", 0) >= 65:
        tags.append("fresh_news_catalyst")
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
    if row.get("has_news_flag", 0) >= 1 and row.get("news_link_confidence_score", 1.0) < 0.5:
        flags.append("news_link_low_confidence")
    if row.get("missing_key_feature_count", 0) >= 4 or row.get("data_confidence_score", 100) < 55:
        flags.append("data_missingness_high")
    return flags


def build_eligibility_notes(row: pd.Series, *, risk_flags: list[str]) -> str:
    notes: list[str] = []
    if row.get("has_daily_ohlcv_flag", 0) < 1:
        notes.append("missing_price")
    if row.get("stale_price_flag", 0) >= 1:
        notes.append("stale_price")
    if row.get("adv_20", 0) < 50_000_000:
        notes.append("adv20_below_threshold")
    if row.get("missing_key_feature_count", 0) >= 5:
        notes.append("feature_missingness_high")
    notes.extend(risk_flags[:2])
    return json.dumps(sorted(set(notes)), ensure_ascii=False)
