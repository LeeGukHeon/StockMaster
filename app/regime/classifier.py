from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RegimeClassification:
    regime_state: str
    regime_score: float
    rule_tag: str


def classify_regime(
    *,
    breadth_up_ratio: float | None,
    median_symbol_return_1d: float | None,
    median_symbol_return_5d: float | None,
    market_realized_vol_20d: float | None,
    turnover_burst_z: float | None,
    new_high_ratio_20d: float | None,
    new_low_ratio_20d: float | None,
) -> RegimeClassification:
    breadth = breadth_up_ratio if breadth_up_ratio is not None else 0.5
    ret1 = median_symbol_return_1d if median_symbol_return_1d is not None else 0.0
    ret5 = median_symbol_return_5d if median_symbol_return_5d is not None else 0.0
    vol = market_realized_vol_20d if market_realized_vol_20d is not None else 0.02
    turnover = turnover_burst_z if turnover_burst_z is not None else 0.0
    new_high = new_high_ratio_20d if new_high_ratio_20d is not None else 0.0
    new_low = new_low_ratio_20d if new_low_ratio_20d is not None else 0.0

    if breadth <= 0.25 and vol >= 0.035 and (ret1 <= -0.015 or new_low >= 0.12):
        return RegimeClassification("panic", 12.5, "breadth_volatility_breakdown")
    if breadth <= 0.40 and (vol >= 0.028 or ret5 <= -0.03):
        return RegimeClassification("risk_off", 30.0, "weak_breadth_high_vol")
    if breadth >= 0.78 and turnover >= 1.0 and new_high >= 0.18:
        return RegimeClassification("euphoria", 92.5, "breadth_breakout_with_turnover")
    if breadth >= 0.60 and ret5 >= 0.02 and new_high >= new_low:
        return RegimeClassification("risk_on", 72.5, "breadth_trend_supportive")
    return RegimeClassification("neutral", 52.5, "mixed_reading")
