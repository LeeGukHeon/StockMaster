from __future__ import annotations

from datetime import date

import pytest

from app.labels.forward_returns import build_forward_labels
from app.ml.constants import MODEL_SPEC_ID
from app.ml.indicator_product import (
    _analysis_model_spec_ids_for_bundle,
    inspect_alpha_indicator_product_readiness,
    run_alpha_indicator_product_bundle,
)
from tests._ticket003_support import seed_ticket003_data, seed_ticket004_flow_data, seed_ticket005_selection_history
from tests._ticket003_support import build_test_settings


def test_indicator_product_bundle_surfaces_missing_ohlcv_dates(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)

    monkeypatch.setattr(
        "app.ml.indicator_product.train_alpha_candidate_models",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError(
                "Feature store cannot build a market-wide snapshot because same-day OHLCV "
                "is missing for trading date 2026-02-27."
            )
        ),
    )
    monkeypatch.setattr(
        "app.ml.indicator_product._detect_missing_snapshot_dates",
        lambda *args, **kwargs: [date(2026, 2, 27)],
    )

    with pytest.raises(RuntimeError, match="Missing feature-snapshot source dates for bundle: 2026-02-27"):
        run_alpha_indicator_product_bundle(
            settings,
            train_end_date=date(2026, 3, 6),
            as_of_date=date(2026, 3, 6),
            shadow_start_selection_date=date(2026, 3, 6),
            shadow_end_selection_date=date(2026, 3, 6),
            horizons=[1, 5],
            model_spec_ids=["alpha_lead_d1_v1", "alpha_swing_d5_v1"],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
            market="ALL",
            rolling_windows=[20, 60],
        )


def test_indicator_product_readiness_surfaces_spec_runnability(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings, limit_symbols=4)
    build_forward_labels(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
        market="ALL",
    )

    readiness = inspect_alpha_indicator_product_readiness(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        model_spec_ids=["alpha_lead_d1_v1", "alpha_swing_d5_v1"],
        limit_symbols=4,
        market="ALL",
    )

    assert readiness.missing_snapshot_dates == []
    assert readiness.available_label_rows_by_horizon[1] > 0
    assert readiness.available_label_rows_by_horizon[5] > 0
    spec_map = {row.model_spec_id: row for row in readiness.specs}
    assert spec_map["alpha_lead_d1_v1"].runnable_horizons == [1]
    assert spec_map["alpha_swing_d5_v1"].runnable_horizons == [5]


def test_analysis_model_spec_ids_include_h5_comparator_for_d5_only_bundle() -> None:
    assert _analysis_model_spec_ids_for_bundle(
        model_spec_ids=["alpha_swing_d5_v2"],
        horizons=[5],
    ) == [
        MODEL_SPEC_ID,
        "alpha_swing_d5_v2",
    ]
