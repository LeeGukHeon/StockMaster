from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from app.ml.constants import (
    CHALLENGER_ALPHA_MODEL_SPECS,
    DEFAULT_ALPHA_MODEL_SPEC,
    MARKET_REGIME_FEATURE_COLUMNS,
    get_alpha_model_spec,
    resolve_feature_columns_for_spec,
    resolve_member_names_for_spec,
    resolve_target_column_for_spec,
    supports_horizon_for_spec,
)
from app.ml.training import (
    _metric_rows,
    _normalise_weights,
    _train_single_horizon,
    build_alpha_model_spec_registry_frame,
)


def test_challenger_specs_use_distinct_feature_profiles() -> None:
    challenger_columns = [
        set(resolve_feature_columns_for_spec(spec))
        for spec in CHALLENGER_ALPHA_MODEL_SPECS
    ]

    assert len(challenger_columns) >= 3
    unique_profiles = {frozenset(columns) for columns in challenger_columns}
    assert len(unique_profiles) >= 2


def test_challenger_specs_use_distinct_member_sets() -> None:
    challenger_members = [
        resolve_member_names_for_spec(spec)
        for spec in CHALLENGER_ALPHA_MODEL_SPECS
    ]

    assert len(challenger_members) >= 3
    assert len({tuple(members) for members in challenger_members}) >= 2


def test_default_spec_keeps_full_feature_space_and_members() -> None:
    feature_columns = resolve_feature_columns_for_spec(DEFAULT_ALPHA_MODEL_SPEC)
    member_names = resolve_member_names_for_spec(DEFAULT_ALPHA_MODEL_SPEC)

    assert "market_is_kospi" in feature_columns
    assert "market_is_kosdaq" in feature_columns
    assert "elasticnet" in member_names
    assert "hist_gbm" in member_names
    assert "extra_trees" in member_names


def test_target_column_resolution_supports_split_h1_h5_specs() -> None:
    assert resolve_target_column_for_spec(DEFAULT_ALPHA_MODEL_SPEC, horizon=1) == "target_h1"
    assert (
        resolve_target_column_for_spec(get_alpha_model_spec("alpha_rank_rolling_120_v1"), horizon=5)
        == "target_top5_h5"
    )
    assert (
        resolve_target_column_for_spec(
            get_alpha_model_spec("alpha_topbucket_h1_rolling_120_v1"),
            horizon=1,
        )
        == "target_top5_h1"
    )
    assert (
        resolve_target_column_for_spec(get_alpha_model_spec("alpha_buyable_d5_v1"), horizon=5)
        == "target_buyable_h5"
    )
    assert (
        resolve_target_column_for_spec(get_alpha_model_spec("alpha_practical_d5_v1"), horizon=5)
        == "target_practical_excess_h5"
    )


def test_split_specs_remain_candidate_enabled_and_horizon_bound() -> None:
    h5_spec = get_alpha_model_spec("alpha_rank_rolling_120_v1")
    h1_spec = get_alpha_model_spec("alpha_topbucket_h1_rolling_120_v1")
    d1_spec = get_alpha_model_spec("alpha_lead_d1_v1")
    d5_focus_spec = get_alpha_model_spec("alpha_swing_d5_v2")
    d5_buyable_spec = get_alpha_model_spec("alpha_buyable_d5_v1")
    d5_practical_spec = get_alpha_model_spec("alpha_practical_d5_v1")
    assert h5_spec.active_candidate_flag is False
    assert h1_spec.active_candidate_flag is False
    assert d1_spec.active_candidate_flag is True
    assert d5_focus_spec.active_candidate_flag is True
    assert d5_buyable_spec.active_candidate_flag is False
    assert d5_practical_spec.active_candidate_flag is False
    assert supports_horizon_for_spec(h5_spec, horizon=5) is True
    assert supports_horizon_for_spec(h5_spec, horizon=1) is False
    assert supports_horizon_for_spec(h1_spec, horizon=1) is True
    assert supports_horizon_for_spec(h1_spec, horizon=5) is False
    assert supports_horizon_for_spec(d1_spec, horizon=1) is True
    assert supports_horizon_for_spec(d1_spec, horizon=5) is False
    assert supports_horizon_for_spec(d5_focus_spec, horizon=5) is True
    assert supports_horizon_for_spec(d5_focus_spec, horizon=1) is False
    assert supports_horizon_for_spec(d5_buyable_spec, horizon=5) is True
    assert supports_horizon_for_spec(d5_buyable_spec, horizon=1) is False
    assert supports_horizon_for_spec(d5_practical_spec, horizon=5) is True
    assert supports_horizon_for_spec(d5_practical_spec, horizon=1) is False


def test_registry_frame_matches_operational_specs() -> None:
    frame_ids = build_alpha_model_spec_registry_frame()["model_spec_id"].tolist()

    assert frame_ids == [
        "alpha_recursive_expanding_v1",
        "alpha_rolling_120_v1",
        "alpha_rolling_250_v1",
        "alpha_rank_rolling_120_v1",
        "alpha_topbucket_h1_rolling_120_v1",
        "alpha_lead_d1_v1",
        "alpha_swing_d5_v2",
        "alpha_buyable_d5_v1",
        "alpha_practical_d5_v1",
    ]


def test_d5_focus_spec_matches_frozen_contract() -> None:
    spec = get_alpha_model_spec("alpha_swing_d5_v2")

    assert spec.estimation_scheme == "rolling"
    assert spec.rolling_window_days == 250
    assert spec.member_names == ("elasticnet", "hist_gbm")
    assert spec.feature_groups == (
        "price_trend",
        "volatility_risk",
        "liquidity_turnover",
        "investor_flow",
        "news_catalyst",
        "fundamentals_quality",
        "value_safety",
        "data_quality",
    )
    assert spec.target_variant == "top5_binary"
    assert spec.training_target_variant == "top5_binary"
    assert spec.validation_primary_metric_name == "top5_mean_excess_return"
    assert spec.promotion_primary_loss_name == "loss_top5"
    assert spec.allowed_horizons == (5,)


def test_alpha_swing_d5_v2_matches_frozen_contract() -> None:
    spec = get_alpha_model_spec("alpha_swing_d5_v2")

    assert spec.estimation_scheme == "rolling"
    assert spec.rolling_window_days == 250
    assert spec.active_candidate_flag is True
    assert spec.lifecycle_role == "active_candidate"
    assert spec.lifecycle_fallback_flag is False
    assert spec.feature_groups == (
        "price_trend",
        "volatility_risk",
        "liquidity_turnover",
        "investor_flow",
        "news_catalyst",
        "fundamentals_quality",
        "value_safety",
        "data_quality",
    )
    assert spec.member_names == ("elasticnet", "hist_gbm")
    assert spec.target_variant == "top5_binary"
    assert spec.training_target_variant == "top5_binary"
    assert spec.validation_primary_metric_name == "top5_mean_excess_return"
    assert spec.promotion_primary_loss_name == "loss_top5"
    assert spec.allowed_horizons == (5,)
    assert resolve_target_column_for_spec(spec, horizon=5) == "target_top5_h5"
    assert supports_horizon_for_spec(spec, horizon=5) is True
    assert supports_horizon_for_spec(spec, horizon=1) is False


def test_alpha_buyable_d5_v1_is_experimental_and_uses_buyable_target() -> None:
    spec = get_alpha_model_spec("alpha_buyable_d5_v1")

    assert spec.estimation_scheme == "rolling"
    assert spec.rolling_window_days == 250
    assert spec.active_candidate_flag is False
    assert spec.lifecycle_role == "experimental_candidate"
    assert spec.feature_groups == (
        "price_trend",
        "volatility_risk",
        "liquidity_turnover",
        "investor_flow",
        "fundamentals_quality",
        "value_safety",
        "market_regime",
        "data_quality",
    )
    feature_columns = resolve_feature_columns_for_spec(spec)
    assert "news_count_1d" not in feature_columns
    assert "news_drift_persistence_score" not in feature_columns
    for feature_name in MARKET_REGIME_FEATURE_COLUMNS:
        assert feature_name in feature_columns
    assert spec.member_names == ("elasticnet", "hist_gbm")
    assert spec.target_variant == "buyable_top5"
    assert spec.training_target_variant == "buyable_top5"
    assert spec.allowed_horizons == (5,)
    assert resolve_target_column_for_spec(spec, horizon=5) == "target_buyable_h5"



def test_alpha_practical_d5_v1_is_experimental_news_free_and_return_unit() -> None:
    spec = get_alpha_model_spec("alpha_practical_d5_v1")

    assert spec.estimation_scheme == "rolling"
    assert spec.rolling_window_days == 250
    assert spec.active_candidate_flag is False
    assert spec.lifecycle_role == "experimental_candidate"
    assert spec.feature_groups == (
        "price_trend",
        "volatility_risk",
        "liquidity_turnover",
        "investor_flow",
        "fundamentals_quality",
        "value_safety",
        "market_regime",
        "data_quality",
    )
    feature_columns = resolve_feature_columns_for_spec(spec)
    assert "news_count_1d" not in feature_columns
    assert "news_link_confidence_score" not in feature_columns
    for feature_name in MARKET_REGIME_FEATURE_COLUMNS:
        assert feature_name in feature_columns
    assert spec.member_names == ("elasticnet", "hist_gbm")
    assert spec.target_variant == "practical_excess_return"
    assert spec.training_target_variant == "practical_excess_return"
    assert resolve_target_column_for_spec(spec, horizon=5) == "target_practical_excess_h5"
    assert supports_horizon_for_spec(spec, horizon=5) is True
    assert supports_horizon_for_spec(spec, horizon=1) is False

def test_normalise_weights_prioritizes_topk_returns_over_small_mae_gap() -> None:
    weights = _normalise_weights(
        {
            "steady": {
                "mae": 0.0200,
                "corr": 0.06,
                "rank_ic": 0.03,
                "top10_mean_excess_return": -0.0005,
                "top20_mean_excess_return": 0.0001,
            },
            "alpha": {
                "mae": 0.0208,
                "corr": 0.05,
                "rank_ic": 0.05,
                "top10_mean_excess_return": 0.0025,
                "top20_mean_excess_return": 0.0015,
            },
        }
    )

    assert weights["alpha"] > weights["steady"]


def test_normalise_weights_uses_rank_metrics_as_tiebreakers() -> None:
    weights = _normalise_weights(
        {
            "member_a": {
                "mae": 0.025,
                "corr": 0.03,
                "rank_ic": 0.01,
                "top10_mean_excess_return": 0.0010,
                "top20_mean_excess_return": 0.0008,
            },
            "member_b": {
                "mae": 0.025,
                "corr": 0.07,
                "rank_ic": 0.06,
                "top10_mean_excess_return": 0.0010,
                "top20_mean_excess_return": 0.0008,
            },
        }
    )

    assert weights["member_b"] > weights["member_a"]


def test_normalise_weights_falls_back_to_equal_when_all_metrics_missing() -> None:
    weights = _normalise_weights(
        {
            "member_a": {"mae": None, "corr": None},
            "member_b": {"mae": None, "corr": None},
        }
    )

    assert weights == {"member_a": 0.5, "member_b": 0.5}


def test_metric_rows_topk_is_cohort_averaged_by_date() -> None:
    actual = pd.Series(([0.0] * 10) + [10.0] + ([1.0] * 10) + [11.0])
    predicted = pd.Series(
        [
            200,
            199,
            198,
            197,
            196,
            195,
            194,
            193,
            192,
            191,
            1,
            100,
            99,
            98,
            97,
            96,
            95,
            94,
            93,
            92,
            91,
            2,
        ]
    )
    as_of_dates = pd.Series(
        [pd.Timestamp("2026-03-01").date()] * 11
        + [pd.Timestamp("2026-03-02").date()] * 11
    )
    rows = _metric_rows(
        training_run_id="run-1",
        horizon=1,
        member_name="ensemble",
        split_name="validation",
        actual=actual,
        predicted=predicted,
        as_of_dates=as_of_dates,
    )
    payload = {row["metric_name"]: row["metric_value"] for row in rows}

    assert payload["top10_mean_excess_return"] == 0.5
    assert payload["top20_mean_excess_return"] == 1.4090909090909092
    assert payload["rank_ic"] == -0.5


def test_train_single_horizon_empty_dataset_keeps_spec_metadata(tmp_path) -> None:
    model_spec = CHALLENGER_ALPHA_MODEL_SPECS[0]
    row, member_predictions, metric_frame, artifact_path = _train_single_horizon(
        pd.DataFrame(columns=["as_of_date", "symbol", "target_h1"]),
        run_id="run-empty",
        train_end_date=date(2026, 4, 8),
        horizon=1,
        min_train_days=5,
        validation_days=2,
        artifact_root=Path(tmp_path),
        model_spec=model_spec,
    )

    model_family = json.loads(row["model_family_json"])

    assert row["model_spec_id"] == model_spec.model_spec_id
    assert row["feature_count"] == len(resolve_feature_columns_for_spec(model_spec))
    assert model_family["members"] == list(resolve_member_names_for_spec(model_spec))
    assert model_family["feature_groups"] == list(model_spec.feature_groups or ())
    assert row["fallback_reason"] == "empty_dataset"
    assert member_predictions.empty
    assert metric_frame.empty
    assert artifact_path is None
