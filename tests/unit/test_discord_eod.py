from __future__ import annotations

from datetime import date

import pandas as pd

from app.recommendation.judgement import ScoreBandEvidence
from app.reports.discord_eod import (
    _build_payload_content,
    _format_alpha_promotion_line,
    _format_pick_block,
)


def test_format_alpha_promotion_line_uses_korean_labels() -> None:
    row = pd.Series(
        {
            "horizon": 1,
            "decision_label": "Active kept",
            "active_model_label": "recursive",
            "comparison_model_label": "rolling 120d",
            "sample_count": 7,
            "p_value": 0.571,
            "decision_reason_label": "incumbent remained in the superior set",
        }
    )

    line = _format_alpha_promotion_line(row)

    assert "기존 모델 유지" in line
    assert "현재 모델이 우수 후보군에 남음" in line
    assert "하루 보유 기준 모델 점검 (D+1)" in line
    assert "확장형 누적 학습" in line
    assert "active serving spec" in line
    assert "기본 비교 모델" in line
    assert "p=" not in line


def test_build_payload_content_labels_candidate_horizon_explicitly() -> None:
    content = _build_payload_content(
        as_of_date=date(2026, 3, 20),
        sector_horizon=1,
        candidate_horizon=1,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        selection_gap=pd.DataFrame(
            [
                {
                    "horizon": 1,
                    "model_spec_id": "alpha_lead_d1_v1",
                    "insufficient_history_flag": False,
                    "selected_top5_mean_realized_excess_return": 0.012,
                    "report_candidates_mean_realized_excess_return": 0.010,
                    "drag_vs_raw_top5": -0.0005,
                    "selected_top5_hit_rate": 0.55,
                }
            ]
        ),
        sector_outlook=pd.DataFrame(),
        single_buy_candidates=pd.DataFrame(),
        market_news=pd.DataFrame(),
    )

    assert "**강세 예상 업종 | 하루 보유 기준 (D+1)**" in content
    assert "**다음 거래일 후보 | 하루 보유 기준 (D+1)**" in content
    assert "기대수익은 보장값이 아니라" in content
    assert "**모델/선택 점검**" not in content
    assert "공식 추천안" not in content


def test_build_payload_content_labels_d5_as_primary_and_d1_as_reference() -> None:
    content = _build_payload_content(
        as_of_date=date(2026, 3, 20),
        sector_horizon=5,
        candidate_horizon=5,
        reference_horizon=1,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        selection_gap=pd.DataFrame(),
        sector_outlook=pd.DataFrame(),
        single_buy_candidates=pd.DataFrame(),
        reference_candidates=pd.DataFrame(),
        market_news=pd.DataFrame(),
    )

    assert "**강세 예상 업종 | 5거래일 보유 기준 (D+5)**" in content
    assert "**2~5거래일 스윙 후보 | 5거래일 보유 기준 (D+5)**" in content
    assert "**참고용 D1 단기 후보 | 하루 보유 기준 (D+1)**" in content
    assert "메인 후보는 5거래일 보유 기준(D+5) 중심" in content


def test_format_pick_block_omits_active_model_id() -> None:
    row = pd.Series(
        {
            "symbol": "357580",
            "company_name": "아모센스",
            "market": "KOSDAQ",
            "industry": "전자부품/통신장비",
            "sector": "코스닥 제조/기술",
            "final_selection_value": 68.7,
            "grade": "C",
            "selection_date": "2026-03-20 00:00:00",
            "next_entry_trade_date": "2026-03-23 00:00:00",
            "selection_close_price": 8860,
            "expected_excess_return": 0.0014,
            "lower_band": -0.0137,
            "upper_band": 0.0131,
            "model_spec_id": "alpha_recursive_expanding_v1",
            "active_alpha_model_id": "freeze_alpha_active_model-xxx",
            "top_reason_tags_json": '["short_term_momentum_strong"]',
            "risk_flags_json": '["model_joint_instability_high"]',
        }
    )

    lines = _format_pick_block(row, rank=1)
    rendered = "\n".join(lines)

    assert "active serving spec" not in rendered
    assert "fallback baseline" not in rendered
    assert "활성 모델 ID" not in rendered
    assert "매수해볼 가치 있음" in rendered
    assert "점수대 성과 우위" in rendered
    assert "단기 탄력 강함" in rendered
    assert "고예측 오차와 모델 이견이 동시에 큼" in rendered
    assert "목표 8,872원" in rendered


def test_format_pick_block_translates_d5_reason_tags_to_korean() -> None:
    row = pd.Series(
        {
            "symbol": "000020",
            "company_name": "동화약품",
            "market": "KOSPI",
            "industry": "제약",
            "sector": "헬스케어",
            "final_selection_value": 72.1,
            "grade": "B",
            "selection_date": "2026-04-23 00:00:00",
            "next_entry_trade_date": "2026-04-24 00:00:00",
            "selection_close_price": 5970,
            "expected_excess_return": 0.0122,
            "lower_band": -0.0030,
            "upper_band": 0.0200,
            "model_spec_id": "alpha_swing_d5_v2",
            "active_alpha_model_id": "freeze_alpha_active_model-xxx",
            "top_reason_tags_json": '["residual_strength_improving","raw_alpha_leader_preserved"]',
            "risk_flags_json": '["model_disagreement_high"]',
        }
    )

    rendered = "\n".join(
        _format_pick_block(
            row,
            rank=1,
            score_evidence={
                "65-75": ScoreBandEvidence("65-75", 100, 0.006, 0.47)
            },
        )
    )

    assert "매수해볼 가치 있음" in rendered
    assert "판단 점수대 성과 우위" in rendered
    assert "상대 강도가 살아나는 흐름" in rendered
    assert "원점수 상위 신호를 최대한 보존함" not in rendered
    assert "raw_alpha_leader_preserved" not in rendered


def test_format_pick_block_labels_d5_buyability_candidate_without_score_band_conflict() -> None:
    row = pd.Series(
        {
            "horizon": 5,
            "symbol": "403870",
            "company_name": "HPSP",
            "market": "KOSDAQ",
            "industry": "반도체",
            "sector": "기술",
            "final_selection_value": 36.5,
            "grade": "C",
            "selection_date": "2026-04-24 00:00:00",
            "next_entry_trade_date": "2026-04-27 00:00:00",
            "selection_close_price": 30000,
            "expected_excess_return": 0.018,
            "buyability_priority_score": 1.42,
            "lower_band": -0.02,
            "upper_band": 0.04,
            "model_spec_id": "alpha_swing_d5_v2",
            "active_alpha_model_id": "freeze_alpha_active_model-xxx",
            "top_reason_tags_json": '["residual_strength_improving"]',
            "risk_flags_json": "[]",
        }
    )

    rendered = "\n".join(_format_pick_block(row, rank=1))

    assert "관찰 우선" in rendered
    assert "매수 보류" not in rendered
    assert "특이 리스크 없음" in rendered
    assert "후보권" in rendered
