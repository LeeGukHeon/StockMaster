from __future__ import annotations

from dataclasses import dataclass

from app.features.constants import FEATURE_GROUP_BY_NAME, FEATURE_NAMES


@dataclass(frozen=True, slots=True)
class AlphaModelSpec:
    model_spec_id: str
    estimation_scheme: str
    rolling_window_days: int | None = None
    active_candidate_flag: bool = True
    lifecycle_role: str = "active_candidate"
    lifecycle_fallback_flag: bool = False
    feature_groups: tuple[str, ...] | None = None
    member_names: tuple[str, ...] | None = None
    target_variant: str = "excess_return"
    training_target_variant: str | None = None
    validation_primary_metric_name: str | None = None
    promotion_primary_loss_name: str | None = None
    allowed_horizons: tuple[int, ...] | None = None


MODEL_DATASET_VERSION = "alpha_training_dataset_v1"
MODEL_DOMAIN = "alpha"
MODEL_VERSION = "alpha_model_v1"
MODEL_SPEC_ID = "alpha_recursive_expanding_v1"
ESTIMATION_SCHEME = "recursive"
ROLLING_WINDOW_DAYS: int | None = None
PREDICTION_VERSION = "alpha_prediction_v1"
SELECTION_ENGINE_VERSION = "selection_engine_v2"
D5_PRIMARY_FOCUS_MODEL_SPEC_ID = "alpha_swing_d5_v2"
D5_BUYABLE_MODEL_SPEC_ID = "alpha_buyable_d5_v1"
D5_PRACTICAL_MODEL_SPEC_ID = "alpha_practical_d5_v1"
D5_PRIMARY_BUCKET_SEGMENTS: tuple[str, ...] = (
    "bucket_continuation",
    "bucket_reversal_recovery",
    "bucket_crowded_risk",
)
D5_PRIMARY_OUTPUT_CONTRACT_ROLES: dict[str, str] = {
    "expected_excess_return": "primary_ranking",
    "uncertainty_score": "soft_penalty",
    "disagreement_score": "soft_penalty",
    "crowding_penalty_score": "soft_penalty",
    "risk_penalty_score": "soft_penalty",
    "late_entry_penalty_score": "soft_penalty",
    "lower_band": "diagnostic_only",
    "median_band": "diagnostic_only",
    "upper_band": "diagnostic_only",
    "report_candidate_flag": "compatibility_only",
}
D5_PRIMARY_DRAG_BASELINE_BY_WINDOW: dict[str, float] = {
    "cohort": -0.117986,
    "rolling_20": -0.117986,
}
D5_PRIMARY_DRAG_IMPROVEMENT_TARGET = 0.03
D5_PRIMARY_SELECTED_TOP5_FLOOR = 0.006493
D5_CHECKPOINT_PROMOTION_MIN_SAMPLE_COUNT = 10
D5_CHECKPOINT_PROMOTION_MIN_SELECTED_TOP5_UPLIFT = 0.001
D5_CHECKPOINT_PROMOTION_MAX_DRAG_DETERIORATION = 0.01
D5_CHECKPOINT_PROMOTION_MAX_HIT_RATE_DETERIORATION = 0.05
D5_CHECKPOINT_PROMOTION_MAX_WORST_CASE_DETERIORATION = 0.02
D5_PRIMARY_H5_BASELINE_MODEL_SPEC_ID = MODEL_SPEC_ID
D5_PRIMARY_COMPARATOR_PAIRS: tuple[tuple[int, str], ...] = (
    (5, D5_PRIMARY_H5_BASELINE_MODEL_SPEC_ID),
    (5, MODEL_SPEC_ID),
    (1, MODEL_SPEC_ID),
    (1, "alpha_topbucket_h1_rolling_120_v1"),
)

DEFAULT_ALPHA_MODEL_SPEC = AlphaModelSpec(
    model_spec_id=MODEL_SPEC_ID,
    estimation_scheme=ESTIMATION_SCHEME,
    rolling_window_days=ROLLING_WINDOW_DAYS,
    active_candidate_flag=False,
    lifecycle_role="baseline_only",
    lifecycle_fallback_flag=True,
)
CHALLENGER_ALPHA_MODEL_SPECS: tuple[AlphaModelSpec, ...] = (
    AlphaModelSpec(
        model_spec_id="alpha_rolling_120_v1",
        estimation_scheme="rolling",
        rolling_window_days=120,
        active_candidate_flag=False,
        lifecycle_role="inactive_candidate",
        feature_groups=(
            "price_trend",
            "volatility_risk",
            "liquidity_turnover",
            "investor_flow",
            "news_catalyst",
            "data_quality",
        ),
        member_names=("hist_gbm", "extra_trees"),
    ),
    AlphaModelSpec(
        model_spec_id="alpha_rolling_250_v1",
        estimation_scheme="rolling",
        rolling_window_days=250,
        active_candidate_flag=False,
        lifecycle_role="inactive_candidate",
        feature_groups=(
            "price_trend",
            "fundamentals_quality",
            "value_safety",
            "news_catalyst",
            "data_quality",
        ),
        member_names=("elasticnet", "hist_gbm"),
    ),
    AlphaModelSpec(
        model_spec_id="alpha_rank_rolling_120_v1",
        estimation_scheme="rolling",
        rolling_window_days=120,
        active_candidate_flag=False,
        lifecycle_role="baseline_only",
        feature_groups=(
            "price_trend",
            "volatility_risk",
            "liquidity_turnover",
            "investor_flow",
            "news_catalyst",
            "data_quality",
        ),
        member_names=("hist_gbm", "extra_trees"),
        target_variant="top5_binary",
        allowed_horizons=(5,),
    ),
    AlphaModelSpec(
        model_spec_id="alpha_topbucket_h1_rolling_120_v1",
        estimation_scheme="rolling",
        rolling_window_days=120,
        active_candidate_flag=False,
        lifecycle_role="inactive_candidate",
        feature_groups=(
            "price_trend",
            "volatility_risk",
            "liquidity_turnover",
            "investor_flow",
            "news_catalyst",
            "data_quality",
        ),
        member_names=("hist_gbm", "extra_trees"),
        target_variant="top20_weighted",
        training_target_variant="top5_binary",
        validation_primary_metric_name="top5_mean_excess_return",
        promotion_primary_loss_name="loss_top5",
        allowed_horizons=(1,),
    ),
    AlphaModelSpec(
        model_spec_id="alpha_lead_d1_v1",
        estimation_scheme="rolling",
        rolling_window_days=120,
        active_candidate_flag=True,
        lifecycle_role="active_candidate",
        feature_groups=(
            "price_trend",
            "liquidity_turnover",
            "investor_flow",
            "news_catalyst",
            "data_quality",
        ),
        member_names=("hist_gbm", "extra_trees"),
        target_variant="top5_binary",
        training_target_variant="top5_binary",
        validation_primary_metric_name="top5_mean_excess_return",
        promotion_primary_loss_name="loss_top5",
        allowed_horizons=(1,),
    ),
    AlphaModelSpec(
        model_spec_id=D5_PRIMARY_FOCUS_MODEL_SPEC_ID,
        estimation_scheme="rolling",
        rolling_window_days=250,
        active_candidate_flag=True,
        lifecycle_role="active_candidate",
        feature_groups=(
            "price_trend",
            "volatility_risk",
            "liquidity_turnover",
            "investor_flow",
            "news_catalyst",
            "fundamentals_quality",
            "value_safety",
            "data_quality",
        ),
        member_names=("elasticnet", "hist_gbm"),
        target_variant="top5_binary",
        training_target_variant="top5_binary",
        validation_primary_metric_name="top5_mean_excess_return",
        promotion_primary_loss_name="loss_top5",
        allowed_horizons=(5,),
    ),
    AlphaModelSpec(
        model_spec_id=D5_BUYABLE_MODEL_SPEC_ID,
        estimation_scheme="rolling",
        rolling_window_days=250,
        active_candidate_flag=False,
        lifecycle_role="experimental_candidate",
        feature_groups=(
            "price_trend",
            "volatility_risk",
            "liquidity_turnover",
            "investor_flow",
            "fundamentals_quality",
            "value_safety",
            "market_regime",
            "data_quality",
        ),
        member_names=("elasticnet", "hist_gbm"),
        target_variant="buyable_top5",
        training_target_variant="buyable_top5",
        validation_primary_metric_name="top5_mean_excess_return",
        promotion_primary_loss_name="loss_top5",
        allowed_horizons=(5,),
    ),
    AlphaModelSpec(
        model_spec_id=D5_PRACTICAL_MODEL_SPEC_ID,
        estimation_scheme="rolling",
        rolling_window_days=250,
        active_candidate_flag=False,
        lifecycle_role="experimental_candidate",
        feature_groups=(
            "price_trend",
            "volatility_risk",
            "liquidity_turnover",
            "investor_flow",
            "fundamentals_quality",
            "value_safety",
            "market_regime",
            "data_quality",
        ),
        member_names=("elasticnet", "hist_gbm"),
        target_variant="practical_excess_return",
        training_target_variant="practical_excess_return",
        validation_primary_metric_name="top5_mean_excess_return",
        promotion_primary_loss_name="loss_top5",
        allowed_horizons=(5,),
    ),
)
ALPHA_CANDIDATE_MODEL_SPECS: tuple[AlphaModelSpec, ...] = (
    DEFAULT_ALPHA_MODEL_SPEC,
    *CHALLENGER_ALPHA_MODEL_SPECS,
)
DEFAULT_TRAIN_ALPHA_CANDIDATE_MODEL_SPECS: tuple[AlphaModelSpec, ...] = tuple(
    spec
    for spec in CHALLENGER_ALPHA_MODEL_SPECS
    if spec.active_candidate_flag and spec.model_spec_id != D5_PRIMARY_FOCUS_MODEL_SPEC_ID
)

MARKET_REGIME_FEATURE_COLUMNS: tuple[str, ...] = (
    "market_regime_score",
    "market_breadth_up_ratio",
    "market_breadth_down_ratio",
    "market_median_symbol_return_1d",
    "market_median_symbol_return_5d",
    "market_realized_vol_20d",
    "market_turnover_burst_z",
    "market_new_high_ratio_20d",
    "market_new_low_ratio_20d",
    "market_regime_panic_flag",
    "market_regime_risk_off_flag",
    "market_regime_neutral_flag",
    "market_regime_risk_on_flag",
    "market_regime_euphoria_flag",
    "market_regime_coverage_flag",
)
DERIVED_FEATURE_GROUP_COLUMNS: dict[str, tuple[str, ...]] = {
    "market_regime": MARKET_REGIME_FEATURE_COLUMNS,
}

MODEL_MEMBER_NAMES: tuple[str, ...] = (
    "elasticnet",
    "hist_gbm",
    "extra_trees",
)

MIN_TRAIN_ROWS = 120
MIN_VALIDATION_ROWS = 20
CALIBRATION_BIN_COUNT = 5
PROMOTION_LOOKBACK_SELECTION_DATES = 60
MCS_ALPHA = 0.10
MCS_BOOTSTRAP_REPS = 500
MCS_BLOCK_LENGTH = 5


def resolve_feature_columns_for_spec(
    model_spec: AlphaModelSpec,
    *,
    include_market_features: bool = True,
) -> tuple[str, ...]:
    if not model_spec.feature_groups:
        columns = list(FEATURE_NAMES)
    else:
        allowed_groups = set(model_spec.feature_groups)
        columns = [
            feature_name
            for feature_name in FEATURE_NAMES
            if FEATURE_GROUP_BY_NAME.get(feature_name) in allowed_groups
        ]
        for feature_group in model_spec.feature_groups:
            columns.extend(DERIVED_FEATURE_GROUP_COLUMNS.get(feature_group, ()))
    if include_market_features:
        columns.extend(["market_is_kospi", "market_is_kosdaq"])
    return tuple(dict.fromkeys(columns))


def resolve_member_names_for_spec(model_spec: AlphaModelSpec) -> tuple[str, ...]:
    return tuple(model_spec.member_names or MODEL_MEMBER_NAMES)


def get_alpha_model_spec(model_spec_id: str) -> AlphaModelSpec:
    for spec in ALPHA_CANDIDATE_MODEL_SPECS:
        if spec.model_spec_id == model_spec_id:
            return spec
    raise KeyError(f"Unknown alpha model spec: {model_spec_id}")


def resolve_training_target_variant_for_spec(model_spec: AlphaModelSpec) -> str:
    return str(model_spec.training_target_variant or model_spec.target_variant)


def resolve_target_column_for_spec(model_spec: AlphaModelSpec, *, horizon: int) -> str:
    target_variant = resolve_training_target_variant_for_spec(model_spec)
    if target_variant == "rank_pct":
        return f"target_rank_h{int(horizon)}"
    if target_variant == "top5_binary":
        return f"target_top5_h{int(horizon)}"
    if target_variant == "top20_weighted":
        return f"target_topbucket_h{int(horizon)}"
    if target_variant == "buyable_top5":
        return f"target_buyable_h{int(horizon)}"
    if target_variant == "practical_excess_return":
        return f"target_practical_excess_h{int(horizon)}"
    return f"target_h{int(horizon)}"


def resolve_validation_primary_metric_for_spec(
    model_spec: AlphaModelSpec,
    *,
    horizon: int,
) -> str:
    if model_spec.validation_primary_metric_name:
        return str(model_spec.validation_primary_metric_name)
    training_target_variant = resolve_training_target_variant_for_spec(model_spec)
    if training_target_variant in {"top5_binary", "practical_excess_return"}:
        return "top5_mean_excess_return"
    return "top10_mean_excess_return"


def resolve_promotion_primary_loss_for_spec(
    model_spec: AlphaModelSpec,
    *,
    horizon: int,
) -> str:
    if model_spec.promotion_primary_loss_name:
        return str(model_spec.promotion_primary_loss_name)
    primary_metric_name = resolve_validation_primary_metric_for_spec(
        model_spec,
        horizon=horizon,
    )
    if primary_metric_name == "top5_mean_excess_return":
        return "loss_top5"
    return "loss_top10"


def supports_horizon_for_spec(model_spec: AlphaModelSpec, *, horizon: int) -> bool:
    if model_spec.allowed_horizons is None:
        return True
    return int(horizon) in {int(value) for value in model_spec.allowed_horizons}
