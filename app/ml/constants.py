from __future__ import annotations

MODEL_DATASET_VERSION = "alpha_training_dataset_v1"
MODEL_VERSION = "alpha_model_v1"
PREDICTION_VERSION = "alpha_prediction_v1"
SELECTION_ENGINE_VERSION = "selection_engine_v2"

MODEL_MEMBER_NAMES: tuple[str, ...] = (
    "elasticnet",
    "hist_gbm",
    "extra_trees",
)

MIN_TRAIN_ROWS = 120
MIN_VALIDATION_ROWS = 20
CALIBRATION_BIN_COUNT = 5
