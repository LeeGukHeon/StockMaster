from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from scripts.summarize_d5_ltr_shadow_variants import pbo_style_summary, summarize_variants


def _variant_frame() -> pd.DataFrame:
    rows = []
    start = date(2026, 1, 1)
    for day in range(24):
        as_of_date = start + timedelta(days=day)
        for variant, value in {
            "stable": 0.01 if day < 12 else -0.01,
            "strict": 0.004,
            "full": -0.002 if day < 12 else 0.012,
        }.items():
            rows.append(
                {
                    "variant": variant,
                    "as_of_date": as_of_date,
                    "top_n": 5,
                    "avg_stable_utility": value,
                    "avg_excess_return": value + 0.001,
                    "hit_stable_utility": float(value > 0),
                }
            )
    return pd.DataFrame(rows)


def test_summarize_variants_reports_downside_and_edge_share() -> None:
    summary = summarize_variants(_variant_frame())
    strict = summary.loc[summary["variant"].eq("strict")].iloc[0]

    assert strict["dates"] == 24
    assert strict["avg_stable_utility"] == 0.004
    assert strict["hit_stable_utility"] == 1.0
    assert 0.0 < strict["max_positive_edge_share"] < 1.0


def test_pbo_style_summary_flags_lower_half_oos_rate() -> None:
    summary = pbo_style_summary(_variant_frame(), block_count=4, min_common_dates=12)
    row = summary.iloc[0]

    assert row["status"] == "ok"
    assert row["combination_count"] > 0
    assert 0.0 <= row["pbo_lower_half_rate"] <= 1.0
    assert row["variant_count"] == 3
