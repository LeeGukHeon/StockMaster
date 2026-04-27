from __future__ import annotations

import math
from collections.abc import Iterable

from app.ranking.risk_taxonomy import has_buyability_blocking_risk

BUYABILITY_EXPECTED_RETURN_WEIGHT = 100.0
BUYABILITY_UNCERTAINTY_PENALTY = 0.04
BUYABILITY_DISAGREEMENT_PENALTY = 0.02
BUYABILITY_MIN_EXPECTED_EXCESS_RETURN = 0.0005
BUYABILITY_MIN_FINAL_SELECTION_VALUE = 20.0
BUYABILITY_MIN_PRIORITY_SCORE = -1.0


def _finite_float(value: object, *, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def buyability_priority_score(
    *,
    expected_excess_return: object,
    uncertainty_score: object = None,
    disagreement_score: object = None,
) -> float:
    """Rank D5 buyable candidates by PIT-tested expected return net of model risk."""

    expected = _finite_float(expected_excess_return)
    uncertainty = _finite_float(uncertainty_score)
    disagreement = _finite_float(disagreement_score)
    return (
        expected * BUYABILITY_EXPECTED_RETURN_WEIGHT
        - uncertainty * BUYABILITY_UNCERTAINTY_PENALTY
        - disagreement * BUYABILITY_DISAGREEMENT_PENALTY
    )


def has_buyability_blocker(risk_flags: Iterable[object] | None) -> bool:
    return has_buyability_blocking_risk(risk_flags)
