from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AlphaModelSpec:
    model_spec_id: str
    estimation_scheme: str
    rolling_window_days: int | None = None
    active_candidate_flag: bool = True


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
    ),
    AlphaModelSpec(
        model_spec_id="alpha_rolling_250_v1",
        estimation_scheme="rolling",
        rolling_window_days=250,
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


def get_alpha_model_spec(model_spec_id: str) -> AlphaModelSpec:
    for spec in ALPHA_CANDIDATE_MODEL_SPECS:
        if spec.model_spec_id == model_spec_id:
            return spec
    raise KeyError(f"Unknown alpha model spec: {model_spec_id}")
