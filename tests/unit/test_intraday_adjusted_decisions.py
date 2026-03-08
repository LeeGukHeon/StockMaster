from __future__ import annotations

import pandas as pd

from app.intraday.adjusted_decisions import _decide_adjusted_action


def test_adjusted_decision_does_not_upgrade_data_insufficient() -> None:
    action, reasons, eligible = _decide_adjusted_action(
        pd.Series(
            {
                "raw_action": "DATA_INSUFFICIENT",
                "adjustment_profile": "SELECTIVE_RISK_ON",
                "signal_quality_flag": "high",
                "adjusted_timing_score": 99.0,
                "adjustment_reason_codes_json": "[]",
                "eligible_to_execute_flag": True,
            }
        )
    )

    assert action == "DATA_INSUFFICIENT"
    assert "raw_data_insufficient_locked" in reasons
    assert eligible is False


def test_adjusted_decision_preserves_avoid_today() -> None:
    action, reasons, eligible = _decide_adjusted_action(
        pd.Series(
            {
                "raw_action": "AVOID_TODAY",
                "adjustment_profile": "HEALTHY_TREND",
                "signal_quality_flag": "high",
                "adjusted_timing_score": 99.0,
                "adjustment_reason_codes_json": "[]",
                "eligible_to_execute_flag": True,
            }
        )
    )

    assert action == "AVOID_TODAY"
    assert "raw_avoid_preserved" in reasons
    assert eligible is False
