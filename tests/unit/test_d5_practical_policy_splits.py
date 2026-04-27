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


def test_active_only_policy_set_has_no_challengers() -> None:
    specs = _resolve_policy_specs("active_only")

    assert [spec.policy_id for spec in specs] == ["active_current"]


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


def test_gate_fails_when_top5_holdout_edge_is_concentrated() -> None:
    portfolio_summary = pd.DataFrame(
        [
            {
                "policy_id": policy_id,
                "split": "holdout",
                "top_n": top_n,
                "dates": 4,
                "avg_net": 0.02,
                "median_net": 0.02,
                "p10_net": 0.01,
                "hit_net": 1.0,
                "max_drawdown_net": 0.0,
                "max_positive_edge_share": edge_share,
                "max_sector_concentration": 0.4,
            }
            for policy_id, top_n, edge_share in [
                ("active_current", 5, 0.40),
                ("active_current", 3, 0.40),
                ("active_current", 1, 0.40),
                ("candidate", 5, 0.90),
                ("candidate", 3, 0.40),
                ("candidate", 1, 0.40),
            ]
        ]
    )

    gate = _passes_gate(
        portfolio_summary,
        policy_id="candidate",
        baseline_policy_id="active_current",
        min_coverage_ratio=0.7,
        min_median_ratio=0.8,
        max_high_disagreement_rate=0.1,
        max_single_date_edge_share=0.4,
    )

    assert gate.gate_decision == "proceed_ltr_shadow_baseline_failed"
    assert not gate.passed
    assert gate.research_continues
    assert "top5_positive_edge_concentration_above_floor" in gate.fail_reasons


def test_summary_includes_positive_edge_concentration() -> None:
    candidates = _policy_frame(_frame(), CURRENT_POLICY_SPECS[0])
    candidates["split"] = "holdout"
    summary = _summarise(candidates, top_ns=[1])
    all_row = summary.loc[summary["split"].eq("all")].iloc[0]

    assert all_row["max_positive_edge_share"] == 0.25
    assert all_row["top_positive_edge_date"] == "2026-03-30"


def test_portfolio_summary_uses_equal_weight_net_returns() -> None:
    from scripts.evaluate_d5_practical_policy_splits import _portfolio_by_date, _portfolio_summary

    selected = pd.DataFrame(
        {
            "policy_id": ["active_current", "active_current", "active_current", "active_current"],
            "split": ["holdout", "holdout", "holdout", "holdout"],
            "as_of_date": [date(2026, 4, 1), date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 2)],
            "symbol": ["000001", "000002", "000001", "000002"],
            "buyability_priority_score": [2.0, 1.0, 2.0, 1.0],
            "excess_forward_return": [0.03, 0.01, -0.01, 0.02],
            "market": ["KOSPI", "KOSDAQ", "KOSPI", "KOSPI"],
        }
    )

    daily = _portfolio_by_date(selected, top_ns=[2], transaction_cost_bps=10.0)
    summary = _portfolio_summary(
        daily,
        bootstrap_reps=0,
        bootstrap_block_size=2,
        bootstrap_seed=1,
    )

    assert daily["portfolio_net_excess_return"].round(3).tolist() == [0.019, 0.004]
    row = summary.iloc[0]
    assert round(row["avg_net"], 4) == 0.0115
    assert round(row["cumulative_net"], 6) == round((1.019 * 1.004) - 1.0, 6)
    assert row["max_market_concentration"] == 1.0


def test_candidate_count_guard_rejects_large_policy_sets(tmp_path: Path) -> None:
    from scripts.evaluate_d5_practical_policy_splits import run

    args = SimpleNamespace(
        output_dir=tmp_path,
        policy_set="abc",
        baseline_policy_id="active_current",
        max_candidate_policy_count=2,
        outcome=[],
        active_outcomes=None,
        practical_outcomes=None,
    )

    try:
        run(args)
    except RuntimeError as exc:
        assert "Candidate policy count exceeds guardrail" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("guard should reject abc policy set by default")


def test_gate_uses_top5_primary_and_top1_diagnostic_only() -> None:
    rows = []
    for policy_id in ["active_current", "candidate"]:
        for top_n in [3, 5]:
            rows.append(
                {
                    "policy_id": policy_id,
                    "split": "holdout",
                    "top_n": top_n,
                    "dates": 10,
                    "avg_net": 0.02,
                    "median_net": 0.02,
                    "p10_net": 0.01,
                    "hit_net": 0.8,
                    "max_drawdown_net": -0.01,
                    "max_positive_edge_share": 0.20,
                    "max_sector_concentration": 0.4,
                }
            )
    rows.extend(
        [
            {
                "policy_id": "active_current",
                "split": "holdout",
                "top_n": 1,
                "dates": 10,
                "avg_net": 0.03,
                "median_net": 0.03,
                "p10_net": 0.01,
                "hit_net": 0.9,
                "max_drawdown_net": 0.0,
                "max_positive_edge_share": 0.20,
                "max_sector_concentration": 0.2,
            },
            {
                "policy_id": "candidate",
                "split": "holdout",
                "top_n": 1,
                "dates": 10,
                "avg_net": -0.01,
                "median_net": -0.01,
                "p10_net": -0.03,
                "hit_net": 0.2,
                "max_drawdown_net": -0.1,
                "max_positive_edge_share": 0.20,
                "max_sector_concentration": 0.2,
            },
        ]
    )

    gate = _passes_gate(
        pd.DataFrame(rows),
        policy_id="candidate",
        baseline_policy_id="active_current",
        min_coverage_ratio=0.7,
        min_median_ratio=0.8,
        max_high_disagreement_rate=0.1,
        max_single_date_edge_share=0.4,
    )

    assert gate.gate_decision == "proceed_ltr_shadow_with_warnings"
    assert not gate.passed
    assert gate.research_continues
    assert gate.fail_reasons == []
    assert "top1_negative" in gate.warning_reasons
    assert "top1_avg_net_below_baseline" in gate.warning_reasons


def test_gate_passes_when_top5_and_top3_clear_thresholds() -> None:
    rows = []
    for policy_id in ["active_current", "candidate"]:
        for top_n in [1, 3, 5]:
            rows.append(
                {
                    "policy_id": policy_id,
                    "split": "holdout",
                    "top_n": top_n,
                    "dates": 10,
                    "avg_net": 0.02,
                    "median_net": 0.02,
                    "p10_net": 0.01,
                    "hit_net": 0.8,
                    "max_drawdown_net": -0.01,
                    "max_positive_edge_share": 0.20,
                    "max_sector_concentration": 0.4,
                }
            )

    gate = _passes_gate(
        pd.DataFrame(rows),
        policy_id="candidate",
        baseline_policy_id="active_current",
        min_coverage_ratio=0.7,
        min_median_ratio=0.8,
        max_high_disagreement_rate=0.1,
        max_single_date_edge_share=0.4,
    )

    payload = gate.as_dict()
    assert gate.gate_decision == "pass_to_ltr"
    assert gate.passed
    assert gate.research_continues
    assert payload["primary_top_n"] == 5
    assert payload["objective_hierarchy"]["diagnostic"] == "top1"



def test_safety_fail_stops_lane_even_when_metrics_pass() -> None:
    rows = []
    for policy_id in ["active_current", "candidate"]:
        for top_n in [1, 3, 5]:
            rows.append(
                {
                    "policy_id": policy_id,
                    "split": "holdout",
                    "top_n": top_n,
                    "dates": 10,
                    "avg_net": 0.02,
                    "median_net": 0.02,
                    "p10_net": 0.01,
                    "hit_net": 0.8,
                    "max_drawdown_net": -0.01,
                    "max_positive_edge_share": 0.20,
                    "max_sector_concentration": 0.4,
                }
            )

    gate = _passes_gate(
        pd.DataFrame(rows),
        policy_id="candidate",
        baseline_policy_id="active_current",
        min_coverage_ratio=0.7,
        min_median_ratio=0.8,
        max_high_disagreement_rate=0.1,
        max_single_date_edge_share=0.4,
        safety_fail_reasons=["missing_repo_sha"],
    )

    assert gate.gate_decision == "stop_lane"
    assert not gate.passed
    assert not gate.research_continues
    assert gate.blocking_fail_reasons == ["missing_repo_sha"]


def test_run_manifest_includes_server_sha_and_basket_gate_contract(
    tmp_path: Path, monkeypatch
) -> None:
    from scripts.evaluate_d5_practical_policy_splits import run

    rows = []
    for as_of_date in pd.date_range("2026-03-25", periods=12, freq="D"):
        for index in range(6):
            rows.append(
                {
                    "as_of_date": as_of_date.date().isoformat(),
                    "symbol": f"{index + 1:06d}",
                    "risk_flags_json": "[]",
                    "expected_excess_return": 0.03 - (index * 0.001),
                    "uncertainty_score": 1.0,
                    "disagreement_score": 1.0,
                    "final_selection_value": 80.0,
                    "excess_forward_return": 0.01 if index < 5 else -0.01,
                    "market": "KOSDAQ",
                    "sector": f"sector-{index % 2}",
                }
            )
    active_path = tmp_path / "active.csv"
    stable_path = tmp_path / "stable.csv"
    pd.DataFrame(rows).to_csv(active_path, index=False)
    pd.DataFrame(rows).to_csv(stable_path, index=False)
    monkeypatch.setenv("SERVER_SHA", "server-sha-test")

    run(
        SimpleNamespace(
            output_dir=tmp_path / "out",
            policy_set="stable",
            baseline_policy_id="active_current",
            max_candidate_policy_count=2,
            outcome=[("active", active_path), ("stable", stable_path)],
            active_outcomes=None,
            practical_outcomes=None,
            tune_end_date=date(2026, 3, 31),
            holdout_start_date=date(2026, 4, 1),
            top_ns=[1, 3, 5],
            outer_fold_size=5,
            contaminated_window=[],
            min_coverage_ratio=0.70,
            min_median_ratio=0.80,
            max_high_disagreement_rate=0.10,
            max_single_date_edge_share=0.40,
            transaction_cost_bps=30.0,
            bootstrap_reps=0,
            bootstrap_block_size=5,
            bootstrap_seed=20260428,
        )
    )

    import json

    manifest = json.loads((tmp_path / "out" / "manifest.json").read_text())
    gate = manifest["gates"][0]
    assert manifest["server_sha"] == "server-sha-test"
    assert manifest["read_only"] is True
    assert manifest["db_read_only"] is True
    assert manifest["artifact_only"] is True
    assert manifest["promotion_disabled"] is True
    assert gate["primary_top_n"] == 5
    assert gate["secondary_top_n"] == 3
    assert gate["diagnostic_top_n"] == 1
    assert gate["research_continues"] is True
    assert gate["promotion_eligible"] is False
