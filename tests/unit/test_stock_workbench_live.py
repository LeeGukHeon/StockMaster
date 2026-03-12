from __future__ import annotations

from datetime import date

import pandas as pd

from app.features.feature_store import build_feature_store
from app.regime.snapshot import build_market_regime_snapshot
from app.ui.helpers import stock_workbench_live_recommendation_frame
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_stock_workbench_live_recommendation_frame_returns_on_demand_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    build_feature_store(settings, as_of_date=date(2026, 3, 11), limit_symbols=4)
    build_market_regime_snapshot(settings, as_of_date=date(2026, 3, 11))

    monkeypatch.setattr(
        "app.ui.helpers._resolve_training_run_for_inference",
        lambda connection, *, as_of_date, horizon: (
            {
                "training_run_id": f"seed-h{horizon}",
                "artifact_uri": "dummy.pkl",
                "model_spec_id": "alpha_recursive_expanding_v1",
                "validation_window_start": date(2026, 3, 1),
                "validation_window_end": date(2026, 3, 10),
            },
            {"active_alpha_model_id": f"active-h{horizon}"},
            "active_registry",
        ),
    )

    def _fake_build_prediction_frame_from_training_run(
        *,
        run_id,
        as_of_date,
        horizon,
        feature_frame,
        training_run,
        training_run_source,
        active_alpha_model_id,
        persist_member_predictions,
    ):
        return (
            pd.DataFrame(
                [
                    {
                        "run_id": run_id,
                        "as_of_date": as_of_date,
                        "symbol": feature_frame.iloc[0]["symbol"],
                        "horizon": horizon,
                        "market": feature_frame.iloc[0]["market"],
                        "ranking_version": "selection_engine_v2",
                        "prediction_version": "alpha_prediction_v1",
                        "expected_excess_return": 0.05 if horizon == 5 else 0.02,
                        "lower_band": -0.03,
                        "median_band": 0.01,
                        "upper_band": 0.09,
                        "calibration_start_date": date(2026, 3, 1),
                        "calibration_end_date": date(2026, 3, 10),
                        "calibration_bucket": "bucket_01",
                        "calibration_sample_size": 20,
                        "model_version": "alpha_model_v1",
                        "training_run_id": training_run["training_run_id"],
                        "model_spec_id": training_run["model_spec_id"],
                        "active_alpha_model_id": active_alpha_model_id,
                        "uncertainty_score": 10.0,
                        "disagreement_score": 5.0,
                        "fallback_flag": False,
                        "fallback_reason": None,
                        "member_count": 3,
                        "ensemble_weight_json": "{}",
                        "source_notes_json": "{}",
                        "created_at": pd.Timestamp.utcnow(),
                    }
                ]
            ),
            [],
        )

    monkeypatch.setattr(
        "app.ui.helpers.build_prediction_frame_from_training_run",
        _fake_build_prediction_frame_from_training_run,
    )

    frame = stock_workbench_live_recommendation_frame(settings, symbol="005930")

    assert not frame.empty
    assert frame.iloc[0]["symbol"] == "005930"
    assert frame.iloc[0]["live_d5_selection_v2_grade"] is not None
    assert frame.iloc[0]["live_reference_price"] > 0
    assert frame.iloc[0]["live_d5_target_price"] > frame.iloc[0]["live_reference_price"]
