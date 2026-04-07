from __future__ import annotations

from app.ml.constants import (
    CHALLENGER_ALPHA_MODEL_SPECS,
    DEFAULT_ALPHA_MODEL_SPEC,
    resolve_feature_columns_for_spec,
    resolve_member_names_for_spec,
)


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
