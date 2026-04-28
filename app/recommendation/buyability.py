from __future__ import annotations

import math
from collections.abc import Iterable

from app.ranking.risk_taxonomy import (
    MODEL_DISAGREEMENT_HIGH_FLAG,
    MODEL_JOINT_INSTABILITY_HIGH_FLAG,
    PREDICTION_ERROR_BUCKET_HIGH_FLAG,
    has_buyability_blocking_risk,
    risk_flag_set,
)

BUYABILITY_EXPECTED_RETURN_WEIGHT = 100.0
BUYABILITY_UNCERTAINTY_PENALTY = 0.04
BUYABILITY_DISAGREEMENT_PENALTY = 0.02
BUYABILITY_MIN_EXPECTED_EXCESS_RETURN = 0.005
BUYABILITY_MIN_FINAL_SELECTION_VALUE = 20.0
BUYABILITY_MIN_PRIORITY_SCORE = -3.0
BUYABILITY_MIN_DISPLAY_PRIORITY_SCORE = -1.0
D5_POLICY_PREFERRED_RANK_START = 2
D5_POLICY_PREFERRED_RANK_END = 6
D5_POLICY_FILL_RANK_END = 10
D5_POLICY_RANK1_UNCERTAINTY_CEILING = 75.0
D5_POLICY_RANK1_DISAGREEMENT_CEILING = 75.0


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


def d5_rank1_passes_overconfidence_guard(
    *,
    risk_flags: Iterable[object] | None,
    fallback_flag: object = None,
    uncertainty_score: object = None,
    disagreement_score: object = None,
) -> bool:
    """Return whether active D5 rank1 is safe enough to keep in the buyable basket."""

    flags = risk_flag_set(risk_flags)
    if has_buyability_blocking_risk(flags):
        return False
    if bool(fallback_flag):
        return False
    if flags & {
        MODEL_DISAGREEMENT_HIGH_FLAG,
        MODEL_JOINT_INSTABILITY_HIGH_FLAG,
        PREDICTION_ERROR_BUCKET_HIGH_FLAG,
    }:
        return False
    uncertainty = _finite_float(uncertainty_score)
    disagreement = _finite_float(disagreement_score)
    return (
        uncertainty < D5_POLICY_RANK1_UNCERTAINTY_CEILING
        and disagreement < D5_POLICY_RANK1_DISAGREEMENT_CEILING
    )


def d5_buyability_policy_bucket(
    *,
    selection_rank: object,
    expected_excess_return: object,
    final_selection_value: object,
    risk_flags: Iterable[object] | None,
    fallback_flag: object = None,
    uncertainty_score: object = None,
    disagreement_score: object = None,
) -> int | None:
    """Rank active-D5 report candidates with rank2-6 as the default basket.

    Historical PIT showed active rank1 was frequently overconfident while ranks 2-6
    carried more usable basket signal. This bucket keeps rank1 only when it clears a
    stricter model-risk guard, prefers ranks 2-6, and uses ranks 7-10 as fill.
    """

    try:
        rank = int(selection_rank)
    except (TypeError, ValueError):
        return None
    if rank < 1 or rank > D5_POLICY_FILL_RANK_END:
        return None
    if has_buyability_blocking_risk(risk_flags):
        return None

    expected = _finite_float(expected_excess_return)
    final_score = _finite_float(final_selection_value)
    strong_signal = (
        expected > BUYABILITY_MIN_EXPECTED_EXCESS_RETURN
        and final_score >= BUYABILITY_MIN_FINAL_SELECTION_VALUE
    )
    weak_signal = expected > 0.0

    if rank == 1:
        if not strong_signal:
            return None
        return (
            0
            if d5_rank1_passes_overconfidence_guard(
                risk_flags=risk_flags,
                fallback_flag=fallback_flag,
                uncertainty_score=uncertainty_score,
                disagreement_score=disagreement_score,
            )
            else None
        )
    if D5_POLICY_PREFERRED_RANK_START <= rank <= D5_POLICY_PREFERRED_RANK_END:
        if strong_signal:
            return 1
        if weak_signal:
            return 3
    if D5_POLICY_PREFERRED_RANK_END < rank <= D5_POLICY_FILL_RANK_END:
        if strong_signal:
            return 2
        if weak_signal:
            return 4
    return None
