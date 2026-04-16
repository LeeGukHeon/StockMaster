from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from app.audit.alpha_phase0 import run_alpha_phase0_audit
from tests._ticket003_support import build_test_settings


def test_alpha_phase0_audit_writes_expected_artifacts(tmp_path, monkeypatch):
    settings = build_test_settings(tmp_path)

    monkeypatch.setattr(
        "app.audit.alpha_phase0.run_pit_checks",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "check_name": "news_after_cutoff_same_day_rows",
                    "severity": "critical",
                    "status": "pass",
                    "violation_count": 0,
                    "hard_fail_flag": False,
                    "details": "ok",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "app.audit.alpha_phase0.compute_decomposition_metrics",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "selection_date": date(2026, 3, 6),
                    "horizon": 1,
                    "scorer_variant": "raw_model",
                    "cohort": "top10",
                    "symbol_count": 10,
                    "rank_ic": 0.02,
                    "mean_realized_excess_return": 0.01,
                    "hit_rate": 0.5,
                    "top10_mean_excess_return": 0.01,
                    "top20_mean_excess_return": 0.01,
                    "avg_recent_1d_return": 0.01,
                    "avg_recent_3d_return": 0.01,
                    "avg_recent_5d_return": 0.01,
                    "avg_recent_10d_return": 0.01,
                    "avg_distance_to_20d_high": -0.02,
                    "avg_turnover_zscore": 0.1,
                    "avg_news_density_3d": 0.2,
                    "sector_concentration": 0.2,
                    "liquidity_tail_exposure": 0.0,
                    "overlap_with_selection_v2": 0.5,
                },
                {
                    "selection_date": date(2026, 3, 6),
                    "horizon": 1,
                    "scorer_variant": "selection_v2",
                    "cohort": "top10",
                    "symbol_count": 10,
                    "rank_ic": 0.02,
                    "mean_realized_excess_return": 0.01,
                    "hit_rate": 0.5,
                    "top10_mean_excess_return": 0.01,
                    "top20_mean_excess_return": 0.01,
                    "avg_recent_1d_return": 0.08,
                    "avg_recent_3d_return": 0.08,
                    "avg_recent_5d_return": 0.08,
                    "avg_recent_10d_return": 0.06,
                    "avg_distance_to_20d_high": -0.005,
                    "avg_turnover_zscore": 1.8,
                    "avg_news_density_3d": 4.2,
                    "sector_concentration": 0.4,
                    "liquidity_tail_exposure": 0.1,
                    "overlap_with_selection_v2": 1.0,
                },
            ]
        ),
    )

    result = run_alpha_phase0_audit(
        settings,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 6),
        horizons=[1, 5],
    )

    assert result.branch_recommendation == "A"
    assert len(result.artifact_paths) == 4
    for artifact in result.artifact_paths:
        assert Path(artifact).exists()

    branch_payload = json.loads(Path(result.artifact_paths[2]).read_text(encoding="utf-8"))
    assert branch_payload["branch_recommendation"] == "A"
    assert branch_payload["pit_status"] == "pass"


def test_alpha_phase0_audit_forces_path_c_on_synthetic_pit_failure(tmp_path, monkeypatch):
    settings = build_test_settings(tmp_path)

    monkeypatch.setattr(
        "app.audit.alpha_phase0.run_pit_checks",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "check_name": "news_after_cutoff_same_day_rows",
                    "severity": "critical",
                    "status": "fail",
                    "violation_count": 3,
                    "hard_fail_flag": True,
                    "details": "late same-day news exists",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "app.audit.alpha_phase0.compute_decomposition_metrics",
        lambda *args, **kwargs: pd.DataFrame(),
    )

    result = run_alpha_phase0_audit(
        settings,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 6),
        horizons=[1, 5],
    )

    assert result.branch_recommendation == "C"
    assert result.pit_status == "fail"
