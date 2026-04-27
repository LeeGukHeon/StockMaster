from __future__ import annotations

from datetime import date

import pandas as pd

from app.ml.ltr_training import (
    D5_LTR_CANDIDATE_POOLS,
    D5_LTR_CONTRACT,
    add_query_group_key,
    add_stable_d5_utility_relevance,
    build_temporal_folds,
    group_sizes,
    prepare_ltr_frame,
    stable_buyable_candidate_pool_mask,
    topn_by_rank_score,
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


def test_prepare_ltr_frame_accepts_alternate_target_and_relevance_label() -> None:
    frame = _ltr_frame().assign(target_buyable_h5=lambda value: value["target_h5"].clip(lower=0.0))

    prepared, _ = prepare_ltr_frame(
        frame,
        horizon=5,
        feature_columns=["feature_a"],
        target_column="target_buyable_h5",
        relevance_column="buyable_d5_relevance",
    )

    assert "target_buyable_h5" in prepared.columns
    assert "target_stable_practical_excess_h5" in prepared.columns
    assert "buyable_d5_relevance" in prepared.columns
    assert prepared["buyable_d5_relevance"].max() == 4


def test_stable_buyable_candidate_pool_filters_hard_blockers() -> None:
    frame = pd.DataFrame(
        [
            {
                "as_of_date": date(2026, 4, 1),
                "symbol": "000001",
                "company_name": "pass",
                "market": "KOSDAQ",
                "feature_a": 1.0,
                "target_h5": 0.05,
                "target_stable_practical_excess_h5": 0.05,
                "liquidity_rank_pct": 0.50,
                "adv_20": 100_000_000.0,
                "realized_vol_20d": 0.02,
                "hl_range_1d": 0.02,
                "drawdown_20d": -0.01,
                "max_loss_20d": -0.01,
                "dist_from_20d_high": -0.10,
                "volume_ratio_1d_vs_20d": 1.0,
                "missing_key_feature_count": 0,
                "data_confidence_score": 100.0,
                "stale_price_flag": 0.0,
            },
            {
                "as_of_date": date(2026, 4, 1),
                "symbol": "000002",
                "company_name": "thin",
                "market": "KOSDAQ",
                "feature_a": 2.0,
                "target_h5": 0.10,
                "target_stable_practical_excess_h5": 0.10,
                "liquidity_rank_pct": 0.05,
                "adv_20": 1_000.0,
                "realized_vol_20d": 0.02,
                "hl_range_1d": 0.02,
                "drawdown_20d": 0.0,
                "max_loss_20d": 0.0,
                "dist_from_20d_high": -0.10,
                "volume_ratio_1d_vs_20d": 1.0,
                "missing_key_feature_count": 0,
                "data_confidence_score": 100.0,
                "stale_price_flag": 0.0,
            },
            {
                "as_of_date": date(2026, 4, 1),
                "symbol": "000003",
                "company_name": "stale",
                "market": "KOSDAQ",
                "feature_a": 3.0,
                "target_h5": 0.20,
                "target_stable_practical_excess_h5": 0.20,
                "liquidity_rank_pct": 0.60,
                "adv_20": 90_000_000.0,
                "realized_vol_20d": 0.02,
                "hl_range_1d": 0.02,
                "drawdown_20d": 0.0,
                "max_loss_20d": 0.0,
                "dist_from_20d_high": -0.10,
                "volume_ratio_1d_vs_20d": 1.0,
                "missing_key_feature_count": 0,
                "data_confidence_score": 100.0,
                "stale_price_flag": 1.0,
            },
        ]
    )

    keyed = add_query_group_key(frame, horizon=5)
    mask = stable_buyable_candidate_pool_mask(keyed, candidate_pool="stable_buyable_v1")
    prepared, _ = prepare_ltr_frame(
        frame,
        horizon=5,
        feature_columns=["feature_a"],
        candidate_pool="stable_buyable_v1",
    )

    assert set(D5_LTR_CANDIDATE_POOLS) == {"full", "stable_buyable_v1", "stable_buyable_strict"}
    assert mask.tolist() == [True, False, False]
    assert prepared["symbol"].tolist() == ["000001"]


def test_topn_by_rank_score_defaults_to_query_aligned_top5() -> None:
    predictions = pd.DataFrame(
        [
            {
                "as_of_date": date(2026, 4, 1),
                "symbol": "000001",
                "market": "KOSPI",
                "query_group_key": "2026-04-01|h5|KOSPI",
                "rank_score": 0.9,
                "target_stable_practical_excess_h5": 0.01,
                "target_h5": 0.02,
            },
            {
                "as_of_date": date(2026, 4, 1),
                "symbol": "000002",
                "market": "KOSDAQ",
                "query_group_key": "2026-04-01|h5|KOSDAQ",
                "rank_score": 0.8,
                "target_stable_practical_excess_h5": 0.02,
                "target_h5": 0.03,
            },
            {
                "as_of_date": date(2026, 4, 1),
                "symbol": "000003",
                "market": "KOSPI",
                "query_group_key": "2026-04-01|h5|KOSPI",
                "rank_score": 0.1,
                "target_stable_practical_excess_h5": -0.02,
                "target_h5": -0.01,
            },
        ]
    )

    by_market = topn_by_rank_score(predictions, top_ns=[2], horizon=5)
    daily = topn_by_rank_score(
        predictions,
        top_ns=[2],
        horizon=5,
        portfolio_group_key="as_of_date",
        portfolio_score_mode="query_rank_pct",
    )

    assert len(by_market) == 2
    assert len(daily) == 1
    assert daily.loc[0, "market"] == "ALL"
    assert daily.loc[0, "symbols"] == "000001,000002"


def test_daily_topn_rejects_raw_cross_query_rank_scores() -> None:
    predictions = pd.DataFrame(
        [
            {
                "as_of_date": date(2026, 4, 1),
                "symbol": "000001",
                "market": "KOSPI",
                "query_group_key": "2026-04-01|h5|KOSPI",
                "rank_score": 0.9,
                "target_stable_practical_excess_h5": 0.01,
                "target_h5": 0.02,
            }
        ]
    )

    try:
        topn_by_rank_score(predictions, top_ns=[1], horizon=5, portfolio_group_key="as_of_date")
    except ValueError as exc:
        assert "query-relative" in str(exc)
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("daily raw rank-score pooling should be rejected")


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
