from __future__ import annotations

from datetime import date

import pandas as pd

from app.ml.ltr_training import (
    D5_LTR_CONTRACT,
    add_query_group_key,
    add_stable_d5_utility_relevance,
    build_temporal_folds,
    group_sizes,
    prepare_ltr_frame,
)


def _ltr_frame() -> pd.DataFrame:
    rows = []
    values = [0.30, 0.25, 0.20, 0.15, 0.10, 0.09, 0.08, 0.07, 0.06, 0.05, 0.04]
    values += [0.03] * 10
    values += [0.02, 0.01, 0.0, -0.01, None]
    for index, value in enumerate(values, start=1):
        rows.append(
            {
                "as_of_date": date(2026, 4, 1),
                "symbol": f"{index:06d}",
                "company_name": f"name{index}",
                "market": "KOSDAQ",
                "feature_a": float(index),
                "target_h5": value or 0.0,
                "target_stable_practical_excess_h5": value,
            }
        )
    return pd.DataFrame(rows)


def test_query_group_key_uses_as_of_date_horizon_market() -> None:
    keyed = add_query_group_key(_ltr_frame().head(1), horizon=5)

    assert keyed.loc[0, "query_group_key"] == "2026-04-01|h5|KOSDAQ"
    assert keyed.loc[0, "horizon"] == 5


def test_stable_utility_relevance_bins_match_contract() -> None:
    keyed = add_query_group_key(_ltr_frame(), horizon=5)
    labeled = add_stable_d5_utility_relevance(keyed)
    relevance = labeled.sort_values(
        ["target_stable_practical_excess_h5", "symbol"],
        ascending=[False, True],
    )["stable_d5_utility_relevance"].tolist()

    assert relevance[:5] == [4, 4, 4, 4, 4]
    assert relevance[5:10] == [3, 3, 3, 3, 3]
    assert relevance[10:20] == [2] * 10
    assert relevance[20:23] == [1, 1, 1]
    missing_relevance = labeled.loc[
        labeled["target_stable_practical_excess_h5"].isna(),
        "stable_d5_utility_relevance",
    ].iloc[0]
    zero_relevance = labeled.loc[
        labeled["target_stable_practical_excess_h5"].eq(0.0),
        "stable_d5_utility_relevance",
    ].iloc[0]
    assert missing_relevance == 0
    assert zero_relevance == 0


def test_prepare_ltr_frame_preserves_rank_score_only_contract() -> None:
    prepared, features = prepare_ltr_frame(_ltr_frame(), horizon=5, feature_columns=["feature_a"])

    assert features == ["feature_a"]
    assert "expected_excess_return" not in prepared.columns
    assert D5_LTR_CONTRACT["score_semantics"] == "relative_rank_score_only"
    assert group_sizes(prepared) == [len(prepared)]


def test_temporal_folds_apply_purge_before_validation() -> None:
    dates = pd.date_range("2026-01-01", periods=40, freq="D").date
    folds = build_temporal_folds(
        dates,
        fold_count=2,
        purge_days=5,
        embargo_days=5,
        min_train_dates=20,
    )

    assert folds
    for fold in folds:
        assert (fold.validation_start_date - fold.train_end_date).days > 5
        assert fold.purge_days == 5
        assert fold.embargo_days == 5
