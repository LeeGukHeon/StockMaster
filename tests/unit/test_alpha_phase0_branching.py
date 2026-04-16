from __future__ import annotations

import pandas as pd

from app.audit.alpha_phase0 import decide_branch


def test_decide_branch_routes_to_path_c_on_hard_fail():
    pit_checks = pd.DataFrame(
        [
            {
                "check_name": "news_after_cutoff_same_day_rows",
                "hard_fail_flag": True,
                "status": "fail",
            }
        ]
    )
    metrics = pd.DataFrame()

    result = decide_branch(pit_checks=pit_checks, decomposition_metrics=metrics, top_k=10)

    assert result["branch_recommendation"] == "C"
    assert result["pit_status"] == "fail"


def test_decide_branch_routes_to_path_a_when_selection_amplifies_chasing():
    pit_checks = pd.DataFrame(
        [{"check_name": "ok", "hard_fail_flag": False, "status": "pass"}]
    )
    metrics = pd.DataFrame(
        [
            {
                "scorer_variant": "raw_model",
                "cohort": "top10",
                "avg_recent_5d_return": 0.01,
                "avg_recent_10d_return": 0.01,
                "avg_distance_to_20d_high": -0.02,
                "avg_turnover_zscore": 0.2,
                "avg_news_density_3d": 0.5,
            },
            {
                "scorer_variant": "selection_v2",
                "cohort": "top10",
                "avg_recent_5d_return": 0.08,
                "avg_recent_10d_return": 0.06,
                "avg_distance_to_20d_high": -0.005,
                "avg_turnover_zscore": 2.0,
                "avg_news_density_3d": 5.0,
            },
        ]
    )

    result = decide_branch(pit_checks=pit_checks, decomposition_metrics=metrics, top_k=10)

    assert result["branch_recommendation"] == "A"
    assert "selection" in " ".join(result["decision_reasons"])


def test_decide_branch_routes_to_path_b_when_raw_model_is_already_chasing():
    pit_checks = pd.DataFrame(
        [{"check_name": "ok", "hard_fail_flag": False, "status": "pass"}]
    )
    metrics = pd.DataFrame(
        [
            {
                "scorer_variant": "raw_model",
                "cohort": "top10",
                "avg_recent_5d_return": 0.10,
                "avg_recent_10d_return": 0.10,
                "avg_distance_to_20d_high": -0.001,
                "avg_turnover_zscore": 2.5,
                "avg_news_density_3d": 6.0,
            },
            {
                "scorer_variant": "selection_v2",
                "cohort": "top10",
                "avg_recent_5d_return": 0.11,
                "avg_recent_10d_return": 0.11,
                "avg_distance_to_20d_high": -0.001,
                "avg_turnover_zscore": 2.6,
                "avg_news_density_3d": 6.2,
            },
        ]
    )

    result = decide_branch(pit_checks=pit_checks, decomposition_metrics=metrics, top_k=10)

    assert result["branch_recommendation"] == "B"
    assert result["pit_status"] == "pass"
