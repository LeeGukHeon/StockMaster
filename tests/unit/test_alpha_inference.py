from __future__ import annotations

import pandas as pd

from app.ml.inference import (
    _apply_d1_lead_prediction_shape_control,
    _bucket_from_calibration,
)


def test_apply_d1_lead_prediction_shape_control_squashes_and_caps() -> None:
    values = pd.Series([-0.20, -0.03, 0.0, 0.03, 0.20], dtype="float64")

    transformed = _apply_d1_lead_prediction_shape_control(values)

    assert transformed.iloc[2] == 0.0
    assert transformed.iloc[0] >= -0.05
    assert transformed.iloc[-1] <= 0.05
    assert transformed.iloc[-1] > 0.029
    assert transformed.iloc[0] < transformed.iloc[1] < transformed.iloc[2] < transformed.iloc[3] < transformed.iloc[4]
    assert transformed.iloc[-1] < values.iloc[-1]


def test_bucket_lookup_stays_on_pre_transform_predictions() -> None:
    rows = [
        {"bucket": "bucket_01", "prediction_lower": 0.0, "prediction_upper": 0.1},
        {"bucket": "bucket_02", "prediction_lower": 0.1, "prediction_upper": 0.2},
    ]

    raw_value = 0.12
    transformed_value = float(_apply_d1_lead_prediction_shape_control([raw_value]).iloc[0])

    assert transformed_value < 0.05
    assert _bucket_from_calibration(rows, raw_value)["bucket"] == "bucket_02"
    assert _bucket_from_calibration(rows, transformed_value)["bucket"] == "bucket_01"
