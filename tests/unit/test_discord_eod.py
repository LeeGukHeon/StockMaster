from __future__ import annotations

import json
from datetime import date

import pandas as pd

from app.recommendation.judgement import ScoreBandEvidence
from app.reports.discord_eod import (
    _build_payload_content,
    _format_alpha_promotion_line,
    _format_pick_block,
    build_swing_gate_diagnostics,
    format_swing_gate_diagnostics_lines,
)


def _swing_row(
    *,
    symbol: str,
    risk_flags: list[str] | None = None,
    eligible: bool = False,
    recommendation_pass: bool = False,
    final_status: str = "WATCHLIST",
    entry_status: str = "BUYABLE",
    recommendation_group: str | None = None,
    pattern: str | None = None,
    risk_distance: float = 0.03,
    reward_risk_ratio: float = 2.0,
    rule_score: float = 72.0,
    ml_probability: float = 0.56,
    hybrid_score: float = 73.0,
) -> dict[str, object]:
    return {
        "horizon": 5,
        "symbol": symbol,
        "eligible_flag": eligible,
        "risk_flags_json": json.dumps(risk_flags or [], ensure_ascii=False),
        "explanatory_score_json": json.dumps(
            {
                "swing_3_5d": {
                    "recommendation_pass": recommendation_pass,
                    "final_status": final_status,
                    "entry_status": entry_status,
                    "entry_status_eod": entry_status,
                    "recommendation_group": recommendation_group
                    or ("EXECUTABLE_PICKS" if recommendation_pass else final_status),
                    "pattern": pattern,
                    "risk_distance": risk_distance,
                    "reward_risk_ratio": reward_risk_ratio,
                    "rr_at_reference": reward_risk_ratio,
                    "rule_score": rule_score,
                    "ml_probability_target_first": ml_probability,
                    "hybrid_score": hybrid_score,
                    "final_score": hybrid_score,
                }
            },
            ensure_ascii=False,
        ),
    }


def test_build_swing_gate_diagnostics_counts_and_translates_filters() -> None:
    diagnostics = build_swing_gate_diagnostics(
        pd.DataFrame(
            [
                _swing_row(
                    symbol="000001",
                    eligible=True,
                    recommendation_pass=True,
                    final_status="CANDIDATE",
                    recommendation_group="EXECUTABLE_PICKS",
                    pattern="pullback",
                ),
                _swing_row(
                    symbol="000002",
                    risk_flags=["swing_common_filter_failed"],
                    final_status="REJECTED",
                    recommendation_group="REJECTED",
                    pattern=None,
                    rule_score=65.0,
                    hybrid_score=66.0,
                ),
                _swing_row(
                    symbol="000003",
                    final_status="VALID_SIGNAL",
                    entry_status="RR_COLLAPSED",
                    recommendation_group="VALID_SIGNALS",
                    pattern="recovery_breakout",
                    risk_distance=0.07,
                    reward_risk_ratio=0.8,
                    ml_probability=0.49,
                ),
            ]
        )
    )

    counts = {item["key"]: item["pass_count"] for item in diagnostics["gates"]}
    assert diagnostics["total_rows"] == 3
    assert diagnostics["recommendation_pass_count"] == 1
    assert diagnostics["valid_signal_count"] == 2
    assert diagnostics["rr_collapsed_count"] == 1
    assert counts["common_not_rejected"] == 2
    assert counts["pattern_present"] == 2
    assert counts["risk_distance_le_5pct"] == 2
    assert counts["rr_ge_1_5"] == 2
    assert counts["ml_ge_0_50"] == 2
    assert counts["entry_buyable"] == 2

    rendered = "\n".join(format_swing_gate_diagnostics_lines(diagnostics))
    assert "공통 제외 필터: 2/3" in rendered
    assert "관리/거래정지/유동성/재무/과열" in rendered
    assert "유효 스윙 패턴: 2/3" in rendered
    assert "유효 신호 2개" in rendered
    assert "손익비 붕괴 1개" in rendered
    assert "누적 실행게이트" in rendered



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


def test_build_payload_content_includes_v3_no_recommendation_gate_diagnostics() -> None:
    diagnostics = build_swing_gate_diagnostics(
        pd.DataFrame(
            [
                _swing_row(
                    symbol="000001",
                    final_status="WATCHLIST",
                    pattern=None,
                    rule_score=72.0,
                    ml_probability=0.65,
                    hybrid_score=74.0,
                ),
                _swing_row(
                    symbol="000002",
                    risk_flags=["swing_common_filter_failed"],
                    final_status="REJECTED",
                    pattern=None,
                    rule_score=60.0,
                    ml_probability=0.45,
                    hybrid_score=62.0,
                ),
            ]
        )
    )

    content = _build_payload_content(
        as_of_date=date(2026, 5, 12),
        sector_horizon=5,
        candidate_horizon=5,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        selection_gap=pd.DataFrame(),
        sector_outlook=pd.DataFrame(),
        single_buy_candidates=pd.DataFrame(),
        market_news=pd.DataFrame(),
        swing_gate_diagnostics=diagnostics,
    )

    assert "좋은 신호는 있었지만 현재 가격 기준 실제 진입 가능한 종목은 없습니다" in content
    assert "H5 실행 추천: 0/2개" in content
    assert "핵심 병목: 유효 스윙 패턴" in content
    assert "필터별 개별 통과 수" not in content
    assert "누적 실행게이트" not in content
    assert "**실제 매수 가능 종목**" in content
    assert "**유효 신호(추격주의/손익비 부족)**" in content
    assert "**관찰 후보**" in content


def test_build_payload_content_shows_nearest_candidate_when_no_h5_pick() -> None:
    diagnostics = build_swing_gate_diagnostics(
        pd.DataFrame(
            [
                _swing_row(
                    symbol="241710",
                    final_status="VALID_SIGNAL",
                    entry_status="RR_COLLAPSED",
                    recommendation_group="VALID_SIGNALS",
                    pattern="recovery_breakout",
                    reward_risk_ratio=0.6045,
                    rule_score=62.0,
                    ml_probability=0.494,
                    hybrid_score=52.4,
                )
            ]
        )
    )
    nearest_row = _swing_row(
        symbol="241710",
        final_status="VALID_SIGNAL",
        entry_status="RR_COLLAPSED",
        recommendation_group="VALID_SIGNALS",
        pattern="recovery_breakout",
        reward_risk_ratio=0.6045,
        rule_score=62.0,
        ml_probability=0.494,
        hybrid_score=52.4,
    )
    nearest_row.update(
        {
            "company_name": "코스메카코리아",
            "market": "KOSDAQ",
            "industry": "화장품",
            "sector": "소비재",
            "final_selection_value": 52.4,
            "grade": "C",
            "selection_date": "2026-05-12 00:00:00",
            "next_entry_trade_date": "2026-05-13 00:00:00",
            "selection_close_price": 91200,
            "expected_excess_return": -0.001,
            "top_reason_tags_json": '["swing_recovery_breakout_pattern"]',
            "explanatory_score_json": json.dumps(
                {
                    "swing_3_5d": {
                        "recommendation_pass": False,
                        "final_status": "VALID_SIGNAL",
                        "entry_status": "RR_COLLAPSED",
                        "entry_status_eod": "RR_COLLAPSED",
                        "recommendation_group": "VALID_SIGNALS",
                        "pattern": "recovery_breakout",
                        "signal_tier": "BORDERLINE_SIGNAL",
                        "entry_reference_price": 91200.0,
                        "signal_close": 91200.0,
                        "max_buy_price": 90055.0,
                        "target_1": 93132.0,
                        "stop_price": 88004.0,
                        "rr_at_reference": 0.6045,
                        "rr_min": 1.5,
                        "signal_score": 55.5,
                        "entry_score": 47.7,
                        "rule_score": 62.0,
                        "ml_probability_target_first": 0.494,
                        "final_score": 52.4,
                    }
                },
                ensure_ascii=False,
            ),
        }
    )

    content = _build_payload_content(
        as_of_date=date(2026, 5, 12),
        sector_horizon=5,
        candidate_horizon=5,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        selection_gap=pd.DataFrame(),
        sector_outlook=pd.DataFrame(),
        single_buy_candidates=pd.DataFrame(),
        market_news=pd.DataFrame(),
        swing_gate_diagnostics=diagnostics,
        swing_message_group_rows=pd.DataFrame([nearest_row]),
    )

    assert "좋은 신호는 있었지만 현재 가격 기준 실제 진입 가능한 종목은 없습니다" in content
    assert "**유효 신호(추격주의/손익비 부족)**" in content
    assert "`241710` 코스메카코리아" in content
    assert "패턴 회복형 돌파" in content
    assert "signal 55.5" in content
    assert "entry 47.7" in content
    assert "final 52.4" in content
    assert "신호종가 91,200원" in content
    assert "현재가 91,200원" in content
    assert "max_buy 90,055원" in content
    assert "현재RR 0.60" in content
    assert "상태 손익비 1.5 미달" in content
    assert "90,055원 이하 눌림 전까지 관찰" in content


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
    assert "**3~5거래일 스윙 후보 | 5거래일 보유 기준 (H5/D+5)**" in content
    assert "**참고용 H1 단기 후보 | 하루 보유 기준 (D+1)**" in content
    assert "메인 후보는 5거래일 보유 기준(D+5) 중심" in content
    assert "3~5D v4는 신호 점수와 종가RR 실행가능성을 분리" in content
    assert "**실제 매수 가능 종목**" in content
    assert "**유효 신호(추격주의/손익비 부족)**" in content
    assert "**관찰 후보**" in content


def test_build_payload_content_renders_industry_code_not_broad_sector() -> None:
    content = _build_payload_content(
        as_of_date=date(2026, 3, 20),
        sector_horizon=5,
        candidate_horizon=5,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        selection_gap=pd.DataFrame(),
        sector_outlook=pd.DataFrame(
            [
                {
                    "outlook_label": "전기전자/반도체",
                    "outlook_group_key": "0013",
                    "outlook_group_type": "industry",
                    "broad_sector": "제조/산업재",
                    "top10_count": 2,
                    "avg_expected_excess_return": 0.012,
                    "sample_symbols": "삼성전자, SK하이닉스",
                }
            ]
        ),
        single_buy_candidates=pd.DataFrame(),
        market_news=pd.DataFrame(),
    )

    assert "전기전자/반도체 (산업코드 0013)" in content
    assert "제조/산업재" not in content


def test_build_payload_content_marks_d5_section_as_observation_when_no_actionable_pick() -> None:
    content = _build_payload_content(
        as_of_date=date(2026, 4, 27),
        sector_horizon=5,
        candidate_horizon=5,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        selection_gap=pd.DataFrame(),
        sector_outlook=pd.DataFrame(),
        single_buy_candidates=pd.DataFrame(
            [
                {
                    "horizon": 5,
                    "symbol": "054050",
                    "company_name": "농우바이오",
                    "market": "KOSDAQ",
                    "industry": "농업",
                    "sector": "소비재",
                    "final_selection_value": 54.7,
                    "grade": "A",
                    "selection_date": "2026-04-27 00:00:00",
                    "next_entry_trade_date": "2026-04-28 00:00:00",
                    "selection_close_price": 8150,
                    "expected_excess_return": 0.0018,
                    "buyability_priority_score": -0.55,
                    "top_reason_tags_json": '["flow_persistence_supportive"]',
                    "risk_flags_json": "[]",
                }
            ]
        ),
        market_news=pd.DataFrame(),
    )

    assert "**3~5거래일 관찰 후보 | 매수검토 이상 없음" in content
    assert "**실제 매수 가능 종목**" in content
    assert "**유효 신호(추격주의/손익비 부족)**" in content
    assert "**관찰 후보**" in content
    assert "054050" not in content


def test_build_payload_content_uses_swing_payload_despite_low_ml_expectation() -> None:
    content = _build_payload_content(
        as_of_date=date(2026, 4, 29),
        sector_horizon=5,
        candidate_horizon=5,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        selection_gap=pd.DataFrame(),
        sector_outlook=pd.DataFrame(),
        single_buy_candidates=pd.DataFrame(
            [
                {
                    "horizon": 5,
                    "symbol": "136480",
                    "company_name": "하림",
                    "market": "KOSDAQ",
                    "industry": "식품",
                    "sector": "소비재",
                    "final_selection_value": 73.1,
                    "d5_selection_rank": 1,
                    "grade": "A",
                    "selection_date": "2026-04-29 00:00:00",
                    "next_entry_trade_date": "2026-04-30 00:00:00",
                    "selection_close_price": 3000,
                    "expected_excess_return": -0.003,
                    "buyability_priority_score": -1.9,
                    "model_spec_id": "alpha_practical_d5_v3",
                    "top_reason_tags_json": (
                        '["swing_pullback_pattern","swing_volume_expansion"]'
                    ),
                    "risk_flags_json": "[]",
                    "explanatory_score_json": json.dumps(
                        {
                            "swing_3_5d": {
                                "methodology_version": "test",
                                "hybrid_score": 73.1,
                                "final_score": 73.1,
                                "signal_score": 80.0,
                                "entry_score": 75.0,
                                "rule_score": 84.0,
                                "recommendation_pass": True,
                                "recommendation_group": "EXECUTABLE_PICKS",
                                "executable_pick": True,
                                "candidate_pass": True,
                                "entry_status": "BUYABLE",
                                "entry_status_eod": "BUYABLE",
                                "signal_close": 3000.0,
                                "current_price": 3000.0,
                                "max_buy_price": 3090.0,
                                "target_1": 3150.0,
                                "stop_price": 2890.0,
                                "rr_at_reference": 1.65,
                                "rr_min": 1.5,
                                "entry_policy": {
                                    "signal_close": 3000.0,
                                    "entry_lower_price": 2950.0,
                                    "max_buy_price": 3090.0,
                                    "chase_warning_price": 3120.0,
                                    "target_zone_price": 3150.0,
                                    "invalidation_price": 2890.0,
                                    "target_1": 3150.0,
                                    "target_2": 3240.0,
                                },
                                "risk_line": 2890.0,
                                "resistance_line": 3200.0,
                                "reward_risk_ratio": 1.65,
                            }
                        },
                        ensure_ascii=False,
                    ),
                }
            ]
        ),
        market_news=pd.DataFrame(),
        score_evidence_by_horizon={
            5: {"65-75": ScoreBandEvidence("65-75", 3, -0.046, 0.0)}
        },
    )

    assert "**3~5거래일 스윙 후보" in content
    assert "매수검토 이상 기준을 통과한 H5 스윙 후보가 없어" not in content
    assert "**실제 매수 가능 종목**" in content
    assert "`136480` 하림" in content
    assert "매수검토" in content
    assert "패턴 -" in content
    assert "signal 80.0" in content
    assert "entry 75.0" in content
    assert "final 73.1" in content
    assert "신호종가 3,000원" in content
    assert "현재가 3,000원" in content
    assert "max_buy 3,090원" in content
    assert "목표1 3,150원" in content
    assert "손절 2,890원" in content
    assert "현재RR 1.65" in content
    assert "고점수 과확신" not in content
    assert "상태 종가 기준 실행 가능" in content
    assert "raw 점수대 성과" not in content


def test_build_payload_content_downgrades_v3_cash_path_negative_expected() -> None:
    content = _build_payload_content(
        as_of_date=date(2026, 4, 30),
        sector_horizon=5,
        candidate_horizon=5,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        selection_gap=pd.DataFrame(),
        sector_outlook=pd.DataFrame(),
        single_buy_candidates=pd.DataFrame(
            [
                {
                    "horizon": 5,
                    "symbol": "054050",
                    "company_name": "농우바이오",
                    "market": "KOSDAQ",
                    "industry": "농업",
                    "sector": "소비재",
                    "final_selection_value": 100.0,
                    "d5_selection_rank": 1,
                    "grade": "A",
                    "selection_date": "2026-04-30 00:00:00",
                    "next_entry_trade_date": "2026-05-01 00:00:00",
                    "selection_close_price": 8000,
                    "expected_excess_return": -0.0049,
                    "buyability_priority_score": -1.0,
                    "model_spec_id": "alpha_practical_d5_v3",
                    "top_reason_tags_json": '["quality_metrics_supportive"]',
                    "risk_flags_json": "[]",
                }
            ]
        ),
        market_news=pd.DataFrame(),
        score_evidence_by_horizon={
            5: {"75+": ScoreBandEvidence("75+", 3, -0.046, 0.0)}
        },
    )

    assert "**3~5거래일 관찰 후보 | 매수검토 이상 없음" in content
    assert "**실제 매수 가능 종목**" in content
    assert "**유효 신호(추격주의/손익비 부족)**" in content
    assert "**관찰 후보**" in content
    assert "054050" not in content
    assert "매수해볼 가치 있음" not in content


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


def test_format_pick_block_uses_row_horizon_label() -> None:
    row = pd.Series(
        {
            "horizon": 1,
            "symbol": "094840",
            "company_name": "슈프리마에이치큐",
            "market": "KOSDAQ",
            "industry": "-",
            "sector": "-",
            "final_selection_value": 57.4,
            "grade": "A",
            "selection_date": "2026-04-27 00:00:00",
            "next_entry_trade_date": "2026-04-28 00:00:00",
            "selection_close_price": 11720,
            "expected_excess_return": 0.004,
            "model_spec_id": "alpha_lead_d1_v1",
            "active_alpha_model_id": "freeze-alpha-d1",
            "top_reason_tags_json": '["residual_strength_improving"]',
            "risk_flags_json": "[]",
        }
    )

    rendered = "\n".join(_format_pick_block(row, rank=1))

    assert "H1 57.4/A" in rendered
    assert "H5 57.4/A" not in rendered


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

    assert "매수해볼 가치 있음" in rendered
    assert "매수 보류" not in rendered
    assert "특이 리스크 없음" in rendered
    assert "추천권·우선순위 양호" in rendered
