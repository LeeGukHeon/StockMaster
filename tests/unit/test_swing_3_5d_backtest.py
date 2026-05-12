from __future__ import annotations

from datetime import date

import pandas as pd

from scripts.backtest_swing_3_5d_methodology import _attach_topk_and_outcomes


def test_backtest_barrier_counts_take_profit_before_timeout() -> None:
    calendar = [date(2026, 1, day) for day in range(1, 8)]
    selections = pd.DataFrame(
        [
            {
                "strategy": "swing_hybrid",
                "as_of_date": date(2026, 1, 1),
                "symbol": "000001",
                "method_score": 90.0,
            }
        ]
    )
    prices = pd.DataFrame(
        {
            "trading_date": calendar,
            "symbol": ["000001"] * len(calendar),
            "open": [100, 100, 100, 100, 100, 100, 100],
            "high": [100, 103, 106, 101, 101, 101, 101],
            "low": [100, 99, 99, 99, 99, 99, 99],
            "close": [100, 102, 105, 100, 100, 100, 100],
        }
    )
    price_lookup = {"000001": prices}

    outcomes = _attach_topk_and_outcomes(
        selections,
        top_k_values=[1],
        calendar=calendar,
        price_lookup=price_lookup,
    )

    assert outcomes.loc[0, "outcome"] == "take_profit"
    assert outcomes.loc[0, "realized_return"] == 0.05


def test_backtest_barrier_uses_conservative_stop_on_same_day_ambiguity() -> None:
    calendar = [date(2026, 1, day) for day in range(1, 8)]
    selections = pd.DataFrame(
        [
            {
                "strategy": "swing_hybrid",
                "as_of_date": date(2026, 1, 1),
                "symbol": "000001",
                "method_score": 90.0,
            }
        ]
    )
    prices = pd.DataFrame(
        {
            "trading_date": calendar,
            "symbol": ["000001"] * len(calendar),
            "open": [100, 100, 100, 100, 100, 100, 100],
            "high": [100, 106, 101, 101, 101, 101, 101],
            "low": [100, 96, 99, 99, 99, 99, 99],
            "close": [100, 100, 100, 100, 100, 100, 100],
        }
    )

    outcomes = _attach_topk_and_outcomes(
        selections,
        top_k_values=[1],
        calendar=calendar,
        price_lookup={"000001": prices},
    )

    assert outcomes.loc[0, "outcome"] == "ambiguous_stop_first"
    assert outcomes.loc[0, "realized_return"] == -0.03
