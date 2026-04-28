from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from scripts.evaluate_d5_ltr_adaptive_market_gate import (
    GateSpec,
    evaluate_gate,
    load_query_group_top5,
    summarize_split_daily,
)


def _query_group_frame() -> pd.DataFrame:
    rows = []
    start = date(2026, 3, 20)
    for index in range(20):
        as_of_date = start + timedelta(days=index)
        rows.append(
            {
                "as_of_date": as_of_date,
                "market": "KOSPI",
                "top_n": 5,
                "n_names": 5,
                "avg_stable_utility": 0.01,
                "avg_excess_return": 0.012,
                "hit_stable_utility": 0.8,
                "symbols": "000001,000002,000003,000004,000005",
            }
        )
        rows.append(
            {
                "as_of_date": as_of_date,
                "market": "KOSDAQ",
                "top_n": 5,
                "n_names": 5,
                "avg_stable_utility": -0.02,
                "avg_excess_return": -0.018,
                "hit_stable_utility": 0.2,
                "symbols": "100001,100002,100003,100004,100005",
            }
        )
    return pd.DataFrame(rows)


def test_load_query_group_top5_filters_top5_and_normalizes_dates(tmp_path: Path) -> None:
    path = tmp_path / "top5.csv"
    frame = pd.concat(
        [
            _query_group_frame(),
            _query_group_frame().head(1).assign(top_n=3),
        ],
        ignore_index=True,
    )
    frame.to_csv(path, index=False)

    loaded = load_query_group_top5(path)

    assert loaded["top_n"].eq(5).all()
    assert loaded["as_of_date"].iloc[0] == date(2026, 3, 20)


def test_adaptive_gate_uses_prior_market_history_only() -> None:
    daily, summary = evaluate_gate(
        _query_group_frame(),
        GateSpec(
            lookback_days=5,
            min_history=3,
            min_trailing_mean=0.0,
            min_trailing_hit=0.5,
            max_markets=1,
        ),
    )

    # First three dates have no prior history and stay in cash.
    assert daily.head(3)["selected_markets"].tolist() == ["CASH", "CASH", "CASH"]
    assert set(daily.iloc[3:]["selected_markets"]) == {"KOSPI"}
    assert summary.loc[0, "avg_active_stable_utility"] == 0.01


def test_split_summary_keeps_tune_and_holdout_separate() -> None:
    daily, _ = evaluate_gate(
        _query_group_frame(),
        GateSpec(
            lookback_days=5,
            min_history=3,
            min_trailing_mean=0.0,
            min_trailing_hit=0.5,
            max_markets=1,
        ),
    )
    split = summarize_split_daily(
        daily,
        tune_end_date=date(2026, 3, 31),
        holdout_start_date=date(2026, 4, 1),
    )

    assert set(split["split"]) == {"tune", "holdout"}
    holdout = split.loc[split["split"].eq("holdout")].iloc[0]
    assert holdout["coverage"] == 1.0
    assert holdout["avg_daily_stable_utility_cash0"] == 0.01
