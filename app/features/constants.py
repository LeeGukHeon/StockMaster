from __future__ import annotations

from dataclasses import dataclass

FEATURE_VERSION = "feature_store_v2"


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    name: str
    group: str


FEATURE_SPECS: tuple[FeatureSpec, ...] = (
    FeatureSpec("ret_1d", "price_trend"),
    FeatureSpec("ret_3d", "price_trend"),
    FeatureSpec("ret_5d", "price_trend"),
    FeatureSpec("ret_10d", "price_trend"),
    FeatureSpec("ret_20d", "price_trend"),
    FeatureSpec("ret_60d", "price_trend"),
    FeatureSpec("ma_5", "price_trend"),
    FeatureSpec("ma_20", "price_trend"),
    FeatureSpec("ma_60", "price_trend"),
    FeatureSpec("ma5_over_ma20", "price_trend"),
    FeatureSpec("ma20_over_ma60", "price_trend"),
    FeatureSpec("dist_from_20d_high", "price_trend"),
    FeatureSpec("dist_from_60d_high", "price_trend"),
    FeatureSpec("dist_from_20d_low", "price_trend"),
    FeatureSpec("close_pos_in_day_range", "price_trend"),
    FeatureSpec("up_day_count_5d", "price_trend"),
    FeatureSpec("up_day_count_20d", "price_trend"),
    FeatureSpec("drawdown_20d", "price_trend"),
    FeatureSpec("drawdown_60d", "price_trend"),
    FeatureSpec("realized_vol_5d", "volatility_risk"),
    FeatureSpec("realized_vol_10d", "volatility_risk"),
    FeatureSpec("realized_vol_20d", "volatility_risk"),
    FeatureSpec("hl_range_1d", "volatility_risk"),
    FeatureSpec("gap_open_1d", "volatility_risk"),
    FeatureSpec("gap_abs_avg_5d", "volatility_risk"),
    FeatureSpec("gap_abs_avg_20d", "volatility_risk"),
    FeatureSpec("max_loss_5d", "volatility_risk"),
    FeatureSpec("max_loss_20d", "volatility_risk"),
    FeatureSpec("volume_ratio_1d_vs_20d", "liquidity_turnover"),
    FeatureSpec("turnover_value_1d", "liquidity_turnover"),
    FeatureSpec("turnover_value_ma_5", "liquidity_turnover"),
    FeatureSpec("turnover_value_ma_20", "liquidity_turnover"),
    FeatureSpec("turnover_z_5_20", "liquidity_turnover"),
    FeatureSpec("adv_20", "liquidity_turnover"),
    FeatureSpec("adv_60", "liquidity_turnover"),
    FeatureSpec("liquidity_rank_pct", "liquidity_turnover"),
    FeatureSpec("revenue_latest", "fundamentals_quality"),
    FeatureSpec("operating_income_latest", "fundamentals_quality"),
    FeatureSpec("net_income_latest", "fundamentals_quality"),
    FeatureSpec("roe_latest", "fundamentals_quality"),
    FeatureSpec("debt_ratio_latest", "fundamentals_quality"),
    FeatureSpec("operating_margin_latest", "fundamentals_quality"),
    FeatureSpec("net_margin_latest", "fundamentals_quality"),
    FeatureSpec("net_income_positive_flag", "fundamentals_quality"),
    FeatureSpec("operating_income_positive_flag", "fundamentals_quality"),
    FeatureSpec("days_since_latest_report", "fundamentals_quality"),
    FeatureSpec("fundamental_coverage_flag", "fundamentals_quality"),
    FeatureSpec("earnings_yield_proxy", "value_safety"),
    FeatureSpec("low_debt_preference_proxy", "value_safety"),
    FeatureSpec("profitability_support_proxy", "value_safety"),
    FeatureSpec("value_proxy_available_flag", "value_safety"),
    FeatureSpec("news_count_1d", "news_catalyst"),
    FeatureSpec("news_count_3d", "news_catalyst"),
    FeatureSpec("news_count_5d", "news_catalyst"),
    FeatureSpec("distinct_publishers_3d", "news_catalyst"),
    FeatureSpec("latest_news_age_hours", "news_catalyst"),
    FeatureSpec("fresh_news_flag", "news_catalyst"),
    FeatureSpec("positive_catalyst_count_3d", "news_catalyst"),
    FeatureSpec("negative_catalyst_count_3d", "news_catalyst"),
    FeatureSpec("news_link_confidence_score", "news_catalyst"),
    FeatureSpec("news_coverage_flag", "news_catalyst"),
    FeatureSpec("foreign_net_value_ratio_1d", "investor_flow"),
    FeatureSpec("foreign_net_value_ratio_5d", "investor_flow"),
    FeatureSpec("institution_net_value_ratio_5d", "investor_flow"),
    FeatureSpec("individual_net_value_ratio_5d", "investor_flow"),
    FeatureSpec("smart_money_flow_ratio_5d", "investor_flow"),
    FeatureSpec("smart_money_flow_ratio_20d", "investor_flow"),
    FeatureSpec("flow_alignment_score", "investor_flow"),
    FeatureSpec("flow_coverage_flag", "investor_flow"),
    FeatureSpec("has_daily_ohlcv_flag", "data_quality"),
    FeatureSpec("has_fundamentals_flag", "data_quality"),
    FeatureSpec("has_news_flag", "data_quality"),
    FeatureSpec("stale_price_flag", "data_quality"),
    FeatureSpec("missing_key_feature_count", "data_quality"),
    FeatureSpec("data_confidence_score", "data_quality"),
)

FEATURE_NAMES: tuple[str, ...] = tuple(spec.name for spec in FEATURE_SPECS)
FEATURE_GROUP_BY_NAME: dict[str, str] = {spec.name: spec.group for spec in FEATURE_SPECS}
CORE_FEATURES_FOR_MISSINGNESS: tuple[str, ...] = (
    "ret_3d",
    "ret_5d",
    "realized_vol_20d",
    "adv_20",
    "roe_latest",
    "debt_ratio_latest",
    "news_count_3d",
)
