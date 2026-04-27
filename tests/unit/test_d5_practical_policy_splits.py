from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from scripts.evaluate_d5_practical_policy_splits import (
    CURRENT_POLICY_SPECS,
    _outer_fold_summary,
    _passes_gate,
    _policy_frame,
    _resolve_outcome_paths,
    _resolve_policy_specs,
    _select_top_by_date,
    _summarise,
    _top1_wide,
)


def _frame() -> pd.DataFrame:
    rows = []
    for as_of_date in pd.date_range("2026-03-30", periods=4, freq="D"):
        for symbol, realized, expected in [
            ("000001", 0.03, 0.02),
            ("000002", -0.01, 0.01),
        ]:
            rows.append(
                {
                    "as_of_date": as_of_date.date(),
                    "symbol": symbol,
                    "risk_flag_list": [],
                    "base_buyability_blocker": False,
                    "high_model_disagreement": False,
                    "expected_excess_return": expected,
                    "uncertainty_score": 1.0,
                    "disagreement_score": 1.0,
                    "buyability_priority_score": expected * 100 - 0.06,
                    "final_selection_value": 50.0,
                    "excess_forward_return": realized,
                }
            )
    return pd.DataFrame(rows)


def test_current_policy_set_is_baseline_only() -> None:
    specs = _resolve_policy_specs("current")

    assert specs == CURRENT_POLICY_SPECS
    assert [spec.policy_id for spec in specs] == [
        "active_current",
        "practical_v1_current",
        "practical_v2_current",
    ]


def test_outcome_path_resolution_supports_legacy_and_keyed_inputs() -> None:
    args = SimpleNamespace(
        outcome=[("practical_v2", Path("v2.csv"))],
        active_outcomes=Path("active.csv"),
        practical_outcomes=Path("v1.csv"),
    )

    paths = _resolve_outcome_paths(args)

    assert paths["active"] == Path("active.csv")
    assert paths["practical_v1"] == Path("v1.csv")
    assert paths["practical"] == Path("v1.csv")
    assert paths["practical_v2"] == Path("v2.csv")


def test_top1_wide_keeps_each_policy_date_independently() -> None:
    selected = pd.DataFrame(
        {
            "policy_id": ["A", "A", "B", "B"],
            "split": ["holdout"] * 4,
            "as_of_date": [date(2026, 4, 1)] * 4,
            "symbol": ["000001", "000002", "000003", "000004"],
            "buyability_priority_score": [1.0, 0.5, 0.8, 0.7],
            "excess_forward_return": [0.01, 0.02, 0.03, 0.04],
        }
    )

    wide = _top1_wide(selected)

    assert wide.loc[0, "A"] == 0.01
    assert wide.loc[0, "B"] == 0.03


def test_outer_fold_summary_uses_temporal_blocks() -> None:
    candidates = _policy_frame(_frame(), CURRENT_POLICY_SPECS[0])
    candidates["split"] = "holdout"
    folds = _outer_fold_summary(candidates, top_ns=[1], outer_fold_size=2)

    assert folds["split"].tolist() == ["outer_fold_01", "outer_fold_02"]
    assert folds["prior_date_count"].tolist() == [0, 2]
    assert folds["dates"].tolist() == [2, 2]


def test_gate_fails_when_holdout_edge_is_concentrated() -> None:
    summary = pd.DataFrame(
        [
            {
                "policy_id": "active_current",
                "split": "holdout",
                "top_n": 1,
                "dates": 2,
                "avg": 0.01,
                "median": 0.01,
                "p10": -0.01,
                "hit": 0.5,
                "blocker_rate": 0.0,
                "high_disagreement_rate": 0.0,
                "max_positive_edge_share": 0.40,
            },
            {
                "policy_id": "candidate",
                "split": "holdout",
                "top_n": 1,
                "dates": 2,
                "avg": 0.02,
                "median": 0.02,
                "p10": -0.005,
                "hit": 0.5,
                "blocker_rate": 0.0,
                "high_disagreement_rate": 0.0,
                "max_positive_edge_share": 0.90,
            },
            {
                "policy_id": "active_current",
                "split": "tune",
                "top_n": 1,
                "dates": 2,
                "avg": 0.01,
                "median": 0.01,
                "p10": -0.01,
                "hit": 0.5,
                "blocker_rate": 0.0,
                "high_disagreement_rate": 0.0,
                "max_positive_edge_share": 0.40,
            },
            {
                "policy_id": "candidate",
                "split": "tune",
                "top_n": 1,
                "dates": 2,
                "avg": 0.02,
                "median": 0.02,
                "p10": -0.005,
                "hit": 0.5,
                "blocker_rate": 0.0,
                "high_disagreement_rate": 0.0,
                "max_positive_edge_share": 0.90,
            },
        ]
    )

    passed, reasons = _passes_gate(
        summary,
        policy_id="candidate",
        baseline_policy_id="active_current",
        min_coverage_ratio=0.7,
        min_median_ratio=0.8,
        max_high_disagreement_rate=0.1,
        max_single_date_edge_share=0.4,
    )

    assert not passed
    assert "holdout_positive_edge_concentration_above_floor" in reasons


def test_summary_includes_positive_edge_concentration() -> None:
    candidates = _policy_frame(_frame(), CURRENT_POLICY_SPECS[0])
    candidates["split"] = "holdout"
    summary = _summarise(candidates, top_ns=[1])
    all_row = summary.loc[summary["split"].eq("all")].iloc[0]

    assert all_row["max_positive_edge_share"] == 0.25
    assert all_row["top_positive_edge_date"] == "2026-03-30"
