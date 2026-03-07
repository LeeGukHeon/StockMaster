from __future__ import annotations

from app.regime.classifier import classify_regime


def test_classify_regime_panic_branch():
    result = classify_regime(
        breadth_up_ratio=0.2,
        median_symbol_return_1d=-0.02,
        median_symbol_return_5d=-0.05,
        market_realized_vol_20d=0.04,
        turnover_burst_z=0.3,
        new_high_ratio_20d=0.01,
        new_low_ratio_20d=0.2,
    )

    assert result.regime_state == "panic"
    assert result.rule_tag == "breadth_volatility_breakdown"


def test_classify_regime_euphoria_branch():
    result = classify_regime(
        breadth_up_ratio=0.85,
        median_symbol_return_1d=0.018,
        median_symbol_return_5d=0.055,
        market_realized_vol_20d=0.02,
        turnover_burst_z=1.5,
        new_high_ratio_20d=0.22,
        new_low_ratio_20d=0.01,
    )

    assert result.regime_state == "euphoria"
    assert result.rule_tag == "breadth_breakout_with_turnover"
