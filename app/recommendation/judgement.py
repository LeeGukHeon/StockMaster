from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

import duckdb

from app.ranking.risk_taxonomy import BUYABILITY_BLOCKING_RISK_FLAGS
from app.recommendation.buyability import (
    BUYABILITY_MIN_DISPLAY_PRIORITY_SCORE,
    BUYABILITY_MIN_EXPECTED_EXCESS_RETURN,
)

D5_SELECTED_STRONG_PRIORITY_SCORE = 0.0


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


SEVERE_RISK_FLAGS = BUYABILITY_BLOCKING_RISK_FLAGS


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


def _int_or_none(value: object) -> int | None:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    return number


def _evidence_supports_buy(evidence: ScoreBandEvidence | None) -> bool:
    if evidence is None:
        return True
    if evidence.sample_count < 30 or evidence.avg_excess_return is None:
        return False
    return evidence.avg_excess_return > 0.0


def _evidence_warns_overconfidence(evidence: ScoreBandEvidence | None) -> bool:
    if evidence is None:
        return False
    if evidence.score_band != "75+":
        return False
    if evidence.sample_count < 30:
        return True
    if evidence.avg_excess_return is None:
        return True
    if evidence.avg_excess_return <= 0.0:
        return True
    return evidence.hit_rate is not None and evidence.hit_rate < 0.45




def _evidence_blocks_selected_candidate(evidence: ScoreBandEvidence | None) -> bool:
    if evidence is None:
        return False
    if evidence.avg_excess_return is not None and evidence.avg_excess_return <= -0.005:
        return True
    return evidence.hit_rate is not None and evidence.hit_rate < 0.35


def _selected_candidate_has_buyable_edge(
    *,
    expected: float | None,
    buyability_priority: float | None,
) -> bool:
    if expected is None or expected < BUYABILITY_MIN_EXPECTED_EXCESS_RETURN:
        return False
    if (
        buyability_priority is not None
        and buyability_priority < BUYABILITY_MIN_DISPLAY_PRIORITY_SCORE
    ):
        return False
    return True


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


def _selected_d5_judgement(
    *,
    score: float,
    expected: float | None,
    rank: int | None,
    buyability_priority: float | None,
    evidence: ScoreBandEvidence | None,
    evidence_text: str,
) -> RecommendationJudgement | None:
    if expected is None or expected <= 0:
        return None
    if expected < BUYABILITY_MIN_EXPECTED_EXCESS_RETURN:
        return RecommendationJudgement(
            label="관찰 우선",
            summary=f"D5 기대값 약함 · {evidence_text}",
            score_band=score_band_for_value(score),
            evidence=evidence,
        )
    if _evidence_blocks_selected_candidate(evidence):
        return RecommendationJudgement(
            label="관찰 우선",
            summary=f"후보권이나 성과 확인 필요 · {evidence_text}",
            score_band=score_band_for_value(score),
            evidence=evidence,
        )
    if buyability_priority is None:
        if score >= 55 or (rank is not None and rank <= 5):
            return RecommendationJudgement(
                label="매수해볼 가치 있음",
                summary=f"추천권·분할 접근 · {evidence_text}",
                score_band=score_band_for_value(score),
                evidence=evidence,
            )
        return None
    if buyability_priority >= D5_SELECTED_STRONG_PRIORITY_SCORE:
        return RecommendationJudgement(
            label="매수해볼 가치 있음",
            summary=f"추천권·우선순위 양호 · {evidence_text}",
            score_band=score_band_for_value(score),
            evidence=evidence,
        )
    if buyability_priority >= BUYABILITY_MIN_DISPLAY_PRIORITY_SCORE:
        return RecommendationJudgement(
            label="매수검토",
            summary=f"추천권·분할 접근 · {evidence_text}",
            score_band=score_band_for_value(score),
            evidence=evidence,
        )
    return RecommendationJudgement(
        label="관찰 우선",
        summary=f"모델위험 대비 보상 부족 · {evidence_text}",
        score_band=score_band_for_value(score),
        evidence=evidence,
    )


def classify_recommendation(
    *,
    final_selection_value: object,
    expected_excess_return: object = None,
    risk_flags: list[str] | tuple[str, ...] | None = None,
    evidence_by_band: Mapping[str, ScoreBandEvidence] | None = None,
    candidate_selected: bool = False,
    candidate_rank: object = None,
    buyability_priority_score: object = None,
) -> RecommendationJudgement:
    score = _float_or_none(final_selection_value)
    expected = _float_or_none(expected_excess_return)
    rank = _int_or_none(candidate_rank)
    buyability_priority = _float_or_none(buyability_priority_score)
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

    if has_severe_risk:
        return RecommendationJudgement(
            label="매수 보류",
            summary=f"차단 리스크 우선 확인 · {evidence_text}",
            score_band=band,
            evidence=evidence,
        )

    if _evidence_warns_overconfidence(evidence):
        return RecommendationJudgement(
            label="관찰 우선",
            summary=f"고점수 과확신 경고 · {evidence_text}",
            score_band=band,
            evidence=evidence,
        )

    if candidate_selected:
        d5_selected_judgement = _selected_d5_judgement(
            score=score,
            expected=expected,
            rank=rank,
            buyability_priority=buyability_priority,
            evidence=evidence,
            evidence_text=evidence_text,
        )
        if d5_selected_judgement is not None:
            return d5_selected_judgement

    if (
        score >= 75
        and expected is not None
        and expected >= 0.04
        and _evidence_supports_aggressive(evidence)
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
    ):
        return RecommendationJudgement(
            label="매수해볼 가치 있음",
            summary=f"점수대 성과 우위 · {evidence_text}",
            score_band=band,
            evidence=evidence,
        )

    if score >= 55 and expected is not None and expected > 0 and evidence_ok:
        return RecommendationJudgement(
            label="관찰 우선",
            summary=f"분할 접근 권장 · {evidence_text}",
            score_band=band,
            evidence=evidence,
        )

    if candidate_selected and expected is not None and expected > 0:
        return RecommendationJudgement(
            label="관찰 우선",
            summary=f"후보권이나 점수대 우위 약함 · {evidence_text}",
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
    lookback_dates: int = 120,
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
