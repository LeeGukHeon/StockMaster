from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

import duckdb


@dataclass(frozen=True, slots=True)
class ScoreBandEvidence:
    score_band: str
    sample_count: int
    avg_excess_return: float | None
    hit_rate: float | None
    start_date: str | None = None
    end_date: str | None = None


@dataclass(frozen=True, slots=True)
class RecommendationJudgement:
    label: str
    summary: str
    score_band: str
    evidence: ScoreBandEvidence | None = None


SEVERE_RISK_FLAGS = {
    "data_missingness_high",
    "high_realized_volatility",
    "large_recent_drawdown",
    "model_uncertainty_high",
    "prediction_fallback",
    "thin_liquidity",
}


def score_band_for_value(value: object) -> str:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if not math.isfinite(score):
        return "unknown"
    if score >= 75.0:
        return "75+"
    if score >= 65.0:
        return "65-75"
    if score >= 55.0:
        return "55-65"
    return "<55"


def _float_or_none(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _evidence_supports_buy(evidence: ScoreBandEvidence | None) -> bool:
    if evidence is None or evidence.sample_count < 30 or evidence.avg_excess_return is None:
        return True
    return evidence.avg_excess_return > 0.0


def _evidence_supports_aggressive(evidence: ScoreBandEvidence | None) -> bool:
    if evidence is None:
        return False
    if (
        evidence.sample_count < 30
        or evidence.avg_excess_return is None
        or evidence.hit_rate is None
    ):
        return False
    return evidence.avg_excess_return >= 0.02 and evidence.hit_rate >= 0.50


def _evidence_summary(evidence: ScoreBandEvidence | None) -> str:
    if evidence is None:
        return "점수대 성과 미연결"
    if evidence.sample_count <= 0 or evidence.avg_excess_return is None:
        return "점수대 성과 표본 부족"
    return f"{evidence.score_band}점대 과거 평균 {evidence.avg_excess_return:+.1%}"


def classify_recommendation(
    *,
    final_selection_value: object,
    expected_excess_return: object = None,
    risk_flags: list[str] | tuple[str, ...] | None = None,
    evidence_by_band: Mapping[str, ScoreBandEvidence] | None = None,
) -> RecommendationJudgement:
    score = _float_or_none(final_selection_value)
    expected = _float_or_none(expected_excess_return)
    band = score_band_for_value(final_selection_value)
    evidence = (evidence_by_band or {}).get(band)
    risks = {str(flag) for flag in (risk_flags or [])}
    has_severe_risk = bool(risks & SEVERE_RISK_FLAGS)
    evidence_ok = _evidence_supports_buy(evidence)
    evidence_text = _evidence_summary(evidence)

    if score is None:
        return RecommendationJudgement(
            label="판단 보류",
            summary="점수 확인 불가",
            score_band=band,
            evidence=evidence,
        )

    if expected is not None and expected <= 0:
        return RecommendationJudgement(
            label="매수 보류",
            summary=f"기대수익률이 낮음 · {evidence_text}",
            score_band=band,
            evidence=evidence,
        )

    if (
        score >= 75
        and expected is not None
        and expected >= 0.04
        and _evidence_supports_aggressive(evidence)
        and not has_severe_risk
    ):
        return RecommendationJudgement(
            label="적극매수 후보",
            summary=f"고점수·고기대수익 · {evidence_text}",
            score_band=band,
            evidence=evidence,
        )

    if (
        score >= 65
        and expected is not None
        and expected > 0
        and evidence_ok
        and not has_severe_risk
    ):
        return RecommendationJudgement(
            label="매수해볼 가치 있음",
            summary=f"점수대 성과 우위 · {evidence_text}",
            score_band=band,
            evidence=evidence,
        )

    if score >= 55 and expected is not None and expected > 0 and evidence_ok:
        caution = "리스크 확인 필요" if has_severe_risk else "분할 접근 권장"
        return RecommendationJudgement(
            label="관찰 우선",
            summary=f"{caution} · {evidence_text}",
            score_band=band,
            evidence=evidence,
        )

    return RecommendationJudgement(
        label="매수 보류",
        summary=f"점수대 우위 약함 · {evidence_text}",
        score_band=band,
        evidence=evidence,
    )


def load_score_band_evidence(
    connection,
    *,
    horizon: int,
    ranking_version: str,
    lookback_dates: int = 60,
) -> dict[str, ScoreBandEvidence]:
    try:
        frame = connection.execute(
            """
            WITH dates AS (
                SELECT DISTINCT selection_date
                FROM fact_selection_outcome
                WHERE horizon = ?
                  AND ranking_version = ?
                  AND realized_excess_return IS NOT NULL
                ORDER BY selection_date DESC
                LIMIT ?
            ), joined AS (
                SELECT *
                FROM fact_selection_outcome
                WHERE horizon = ?
                  AND ranking_version = ?
                  AND selection_date IN (SELECT selection_date FROM dates)
                  AND realized_excess_return IS NOT NULL
            )
            SELECT
                CASE
                    WHEN final_selection_value >= 75 THEN '75+'
                    WHEN final_selection_value >= 65 THEN '65-75'
                    WHEN final_selection_value >= 55 THEN '55-65'
                    ELSE '<55'
                END AS score_band,
                COUNT(*) AS sample_count,
                AVG(realized_excess_return) AS avg_excess_return,
                AVG(CASE WHEN realized_excess_return > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
                MIN(selection_date)::VARCHAR AS start_date,
                MAX(selection_date)::VARCHAR AS end_date
            FROM joined
            GROUP BY 1
            """,
            [horizon, ranking_version, lookback_dates, horizon, ranking_version],
        ).fetchdf()
    except duckdb.Error:
        return {}
    evidence: dict[str, ScoreBandEvidence] = {}
    for row in frame.itertuples(index=False):
        evidence[str(row.score_band)] = ScoreBandEvidence(
            score_band=str(row.score_band),
            sample_count=int(row.sample_count or 0),
            avg_excess_return=_float_or_none(row.avg_excess_return),
            hit_rate=_float_or_none(row.hit_rate),
            start_date=str(row.start_date) if row.start_date is not None else None,
            end_date=str(row.end_date) if row.end_date is not None else None,
        )
    return evidence
