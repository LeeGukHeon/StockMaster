from __future__ import annotations

import pandas as pd

from scripts.replay_current_d5_policy import ReplayConfig, replay_policy


def _row(
    selection_date: str,
    symbol: str,
    score: float,
    expected: float,
    realized: float,
    *,
    uncertainty: float = 10.0,
    disagreement: float = 10.0,
    sector: str = "A",
) -> dict[str, object]:
    return {
        "selection_date": selection_date,
        "evaluation_date": "2026-03-10",
        "symbol": symbol,
        "company_name": symbol,
        "market": "KOSPI",
        "sector": sector,
        "industry": sector,
        "horizon": 5,
        "ranking_version": "selection_engine_v2",
        "grade": "A",
        "eligible_flag": True,
        "final_selection_value": score,
        "selection_percentile": 0.99,
        "expected_excess_return": expected,
        "uncertainty_score": uncertainty,
        "disagreement_score": disagreement,
        "fallback_flag": False,
        "risk_flags_json": "[]",
        "top_reason_tags_json": "[]",
        "model_spec_id": "alpha_practical_d5_v2",
        "active_alpha_model_id": "active",
        "realized_return": realized,
        "realized_excess_return": realized,
        "direction_hit_flag": realized > 0,
        "outcome_status": "matured",
    }


def _config() -> ReplayConfig:
    return ReplayConfig(
        start_date=None,
        end_date=None,
        horizon=5,
        ranking_version="selection_engine_v2",
        top_limit=5,
        max_per_sector=2,
        evidence_mode="none",
        evidence_lookback_dates=120,
        min_matured_dates=1,
    )


def test_replay_policy_excludes_overconfident_rank1_and_keeps_buyable_rank2() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-03-02", "R1", 90.0, 0.01, -0.03, uncertainty=90, disagreement=90),
            _row("2026-03-02", "R2", 70.0, 0.01, 0.04),
            _row("2026-03-02", "R3", 60.0, 0.002, 0.01),
        ]
    )

    rows, daily = replay_policy(frame, config=_config())

    assert rows["symbol"].tolist() == ["R2", "R3"]
    assert rows.set_index("symbol").loc["R2", "judgement_label"] == "매수해볼 가치 있음"
    assert rows.set_index("symbol").loc["R3", "judgement_label"] == "관찰 우선"
    assert daily.loc[0, "actionable_symbols"] == "R2"
    assert daily.loc[0, "actionable_avg_excess_return_cash0"] == 0.04


def test_replay_policy_uses_cash_zero_when_no_candidates() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-03-02", "N1", 40.0, -0.01, -0.05),
        ]
    )

    rows, daily = replay_policy(frame, config=_config())

    assert rows.empty
    assert daily.loc[0, "selected_count"] == 0
    assert daily.loc[0, "selected_avg_excess_return_cash0"] == 0.0
