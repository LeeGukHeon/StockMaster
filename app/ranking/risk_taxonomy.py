from __future__ import annotations

from collections.abc import Iterable

MODEL_ERROR_BUCKET_HIGH_THRESHOLD = 85.0
MODEL_DISAGREEMENT_HIGH_THRESHOLD = 90.0

PREDICTION_ERROR_BUCKET_HIGH_FLAG = "prediction_error_bucket_high"
MODEL_DISAGREEMENT_HIGH_FLAG = "model_disagreement_high"
MODEL_JOINT_INSTABILITY_HIGH_FLAG = "model_joint_instability_high"

BUYABILITY_BLOCKING_RISK_FLAGS = frozenset(
    {
        "data_missingness_high",
        "high_realized_volatility",
        "large_recent_drawdown",
        "thin_liquidity",
        "prediction_fallback",
        "implementation_friction_high",
        MODEL_JOINT_INSTABILITY_HIGH_FLAG,
    }
)

GRADE_CAPPING_RISK_FLAGS = frozenset(
    {
        "data_missingness_high",
        "high_realized_volatility",
        "large_recent_drawdown",
        "thin_liquidity",
        "implementation_friction_high",
        MODEL_JOINT_INSTABILITY_HIGH_FLAG,
    }
)


def risk_flag_set(risk_flags: Iterable[object] | None) -> set[str]:
    return {str(flag) for flag in (risk_flags or []) if str(flag).strip()}


def has_buyability_blocking_risk(risk_flags: Iterable[object] | None) -> bool:
    return bool(risk_flag_set(risk_flags) & BUYABILITY_BLOCKING_RISK_FLAGS)


def has_grade_capping_risk(risk_flags: Iterable[object] | None) -> bool:
    return bool(risk_flag_set(risk_flags) & GRADE_CAPPING_RISK_FLAGS)


def model_risk_flags(*, uncertainty_score: object, disagreement_score: object) -> list[str]:
    try:
        uncertainty = float(uncertainty_score)
    except (TypeError, ValueError):
        uncertainty = float("nan")
    try:
        disagreement = float(disagreement_score)
    except (TypeError, ValueError):
        disagreement = float("nan")

    flags: list[str] = []
    high_error_bucket = uncertainty >= MODEL_ERROR_BUCKET_HIGH_THRESHOLD
    high_disagreement = disagreement >= MODEL_DISAGREEMENT_HIGH_THRESHOLD
    if high_error_bucket:
        flags.append(PREDICTION_ERROR_BUCKET_HIGH_FLAG)
    if high_disagreement:
        flags.append(MODEL_DISAGREEMENT_HIGH_FLAG)
    if high_error_bucket and high_disagreement:
        flags.append(MODEL_JOINT_INSTABILITY_HIGH_FLAG)
    return flags
