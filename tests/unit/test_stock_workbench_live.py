from __future__ import annotations

from datetime import date

import pandas as pd

from app.common.artifacts import resolve_artifact_path
from app.discord_bot.live_recalc import (
    _refresh_live_rank_features,
    compute_live_stock_recommendation,
)
from app.features.feature_store import build_feature_store
from app.regime.snapshot import build_market_regime_snapshot
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_compute_live_stock_recommendation_returns_on_demand_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    build_feature_store(settings, as_of_date=date(2026, 3, 11), limit_symbols=4)
    build_market_regime_snapshot(settings, as_of_date=date(2026, 3, 11))
    dummy_artifact = settings.paths.artifacts_dir / "models" / "dummy.pkl"
    dummy_artifact.parent.mkdir(parents=True, exist_ok=True)
    dummy_artifact.write_bytes(b"artifact")

    monkeypatch.setattr(
        "app.discord_bot.live_recalc._resolve_training_run_for_inference",
        lambda connection, *, as_of_date, horizon: (
            {
                "training_run_id": f"seed-h{horizon}",
                "artifact_uri": str(dummy_artifact),
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
        "app.discord_bot.live_recalc.build_prediction_frame_from_training_run",
        _fake_build_prediction_frame_from_training_run,
    )

    result = compute_live_stock_recommendation(settings, symbol="005930")
    frame = result.frame

    assert not frame.empty
    assert frame.iloc[0]["symbol"] == "005930"
    assert frame.iloc[0]["live_d5_selection_v2_grade"] is not None
    assert frame.iloc[0]["live_d1_model_spec_id"] == "alpha_recursive_expanding_v1"
    assert frame.iloc[0]["live_d5_active_alpha_model_id"] == "active-h5"
    assert frame.iloc[0]["live_d5_top_reason_tags_json"] is not None
    assert frame.iloc[0]["live_reference_price"] > 0
    assert frame.iloc[0]["live_d5_target_price"] > frame.iloc[0]["live_reference_price"]


def test_resolve_artifact_path_maps_legacy_artifact_root(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    runtime_artifact = settings.paths.artifacts_dir / "models" / "alpha_model_v1.pkl"
    runtime_artifact.parent.mkdir(parents=True, exist_ok=True)
    runtime_artifact.write_bytes(b"artifact")

    legacy_artifact = (
        settings.paths.project_root / "data" / "artifacts" / "models" / "alpha_model_v1.pkl"
    )

    resolved = resolve_artifact_path(settings, str(legacy_artifact))

    assert resolved == runtime_artifact.resolve()


def test_refresh_live_rank_features_recomputes_live_row_rank_pct() -> None:
    frame = pd.DataFrame(
        [
            {"symbol": "000001", "market": "KOSPI", "ret_5d": 0.01},
            {"symbol": "000002", "market": "KOSPI", "ret_5d": 0.03},
            {"symbol": "005930", "market": "KOSPI", "ret_5d": 0.10},
        ]
    )

    refreshed = _refresh_live_rank_features(frame)
    live_rank = refreshed.loc[refreshed["symbol"] == "005930", "ret_5d_rank_pct"].iloc[0]

    assert live_rank == 1.0
