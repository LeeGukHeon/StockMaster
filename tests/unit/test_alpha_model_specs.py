from __future__ import annotations

from app.ml.constants import (
    CHALLENGER_ALPHA_MODEL_SPECS,
    DEFAULT_ALPHA_MODEL_SPEC,
    resolve_feature_columns_for_spec,
    resolve_member_names_for_spec,
)
from app.ml.training import _normalise_weights


def test_challenger_specs_use_distinct_feature_profiles() -> None:
    challenger_columns = [set(resolve_feature_columns_for_spec(spec)) for spec in CHALLENGER_ALPHA_MODEL_SPECS]

    assert len(challenger_columns) == 2
    assert challenger_columns[0] != challenger_columns[1]
    assert len(challenger_columns[0] - challenger_columns[1]) > 0
    assert len(challenger_columns[1] - challenger_columns[0]) > 0


def test_challenger_specs_use_distinct_member_sets() -> None:
    challenger_members = [resolve_member_names_for_spec(spec) for spec in CHALLENGER_ALPHA_MODEL_SPECS]

    assert len(challenger_members) == 2
    assert challenger_members[0] != challenger_members[1]
    assert set(challenger_members[0]) != set(challenger_members[1])


def test_default_spec_keeps_full_feature_space_and_members() -> None:
    feature_columns = resolve_feature_columns_for_spec(DEFAULT_ALPHA_MODEL_SPEC)
    member_names = resolve_member_names_for_spec(DEFAULT_ALPHA_MODEL_SPEC)

    assert "market_is_kospi" in feature_columns
    assert "market_is_kosdaq" in feature_columns
    assert "elasticnet" in member_names
    assert "hist_gbm" in member_names
    assert "extra_trees" in member_names


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
