from __future__ import annotations

from dataclasses import dataclass

from app.features.constants import FEATURE_GROUP_BY_NAME, FEATURE_NAMES

@dataclass(frozen=True, slots=True)
class AlphaModelSpec:
    model_spec_id: str
    estimation_scheme: str
    rolling_window_days: int | None = None
    active_candidate_flag: bool = True
    feature_groups: tuple[str, ...] | None = None
    member_names: tuple[str, ...] | None = None


MODEL_DATASET_VERSION = "alpha_training_dataset_v1"
MODEL_DOMAIN = "alpha"
MODEL_VERSION = "alpha_model_v1"
MODEL_SPEC_ID = "alpha_recursive_expanding_v1"
ESTIMATION_SCHEME = "recursive"
ROLLING_WINDOW_DAYS: int | None = None
PREDICTION_VERSION = "alpha_prediction_v1"
SELECTION_ENGINE_VERSION = "selection_engine_v2"

DEFAULT_ALPHA_MODEL_SPEC = AlphaModelSpec(
    model_spec_id=MODEL_SPEC_ID,
    estimation_scheme=ESTIMATION_SCHEME,
    rolling_window_days=ROLLING_WINDOW_DAYS,
)
CHALLENGER_ALPHA_MODEL_SPECS: tuple[AlphaModelSpec, ...] = (
    AlphaModelSpec(
        model_spec_id="alpha_rolling_120_v1",
        estimation_scheme="rolling",
        rolling_window_days=120,
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
        feature_groups=(
            "price_trend",
            "fundamentals_quality",
            "value_safety",
            "news_catalyst",
            "data_quality",
        ),
        member_names=("elasticnet", "hist_gbm"),
    ),
)
ALPHA_CANDIDATE_MODEL_SPECS: tuple[AlphaModelSpec, ...] = (
    DEFAULT_ALPHA_MODEL_SPEC,
    *CHALLENGER_ALPHA_MODEL_SPECS,
)

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
