from __future__ import annotations

import pandas as pd

from app.discord_bot.read_store import (
    _build_pick_rows,
    _build_status_rows,
    _build_stock_summary_rows,
    _build_weekly_rows,
)


def test_build_status_rows_creates_latest_snapshot() -> None:
    rows = _build_status_rows(
        built_at="2026-03-23T18:00:00+09:00",
        as_of_date="2026-03-23",
        ranking_as_of_date="2026-03-23",
        ranking_version="selection_engine_v2",
        source_run_id="run-1",
    )

    assert len(rows) == 1
    assert rows[0]["snapshot_type"] == "status"
    assert rows[0]["snapshot_key"] == "latest"
    assert "추천 기준일 2026-03-23" in rows[0]["summary"]


def test_build_pick_rows_translates_reasons_and_risks() -> None:
    frame = pd.DataFrame(
        [
            {
                "horizon": 1,
                "symbol": "005930",
                "company_name": "삼성전자",
                "market": "KOSPI",
                "grade": "A",
                "expected_excess_return": 0.0123,
                "next_entry_trade_date": "2026-03-24",
                "final_selection_value": 88.2,
                "selection_date": "2026-03-23",
                "industry": "반도체",
                "sector": "제조",
                "model_spec_id": "alpha_recursive_expanding_v1",
                "reasons": '["short_term_momentum_strong","turnover_surge"]',
                "risks": '["model_uncertainty_high"]',
            }
        ]
    )

    rows = _build_pick_rows(
        frame,
        horizon=1,
        built_at="2026-03-23T18:00:00+09:00",
        as_of_date="2026-03-23",
        source_run_id="run-1",
    )

    assert len(rows) == 1
    assert rows[0]["snapshot_type"] == "next_picks"
    assert "핵심 근거 단기 탄력 강함, 거래대금 급증" in rows[0]["summary"]
    assert "유의할 리스크 모델 확신이 낮음" in rows[0]["summary"]


def test_build_weekly_rows_creates_korean_sections() -> None:
    alpha = pd.DataFrame(
        [
            {
                "horizon": 1,
                "decision_label": "기존 모델 유지",
                "active_model_label": "확장형 누적 학습",
                "decision_reason_label": "현재 모델이 우수 후보군에 남음",
                "sample_count": 7,
            }
        ]
    )
    evaluation = pd.DataFrame(
        [
            {
                "horizon": 1,
                "window_type": "동일 시작 묶음",
                "mean_realized_excess_return": 0.011,
                "hit_rate": 0.56,
                "count_evaluated": 20,
                "ranking_version": "selection_engine_v2",
            }
        ]
    )
    policy = pd.DataFrame(
        [
            {
                "horizon": 1,
                "template_id": "상승 우위 탄력형",
                "scope_type": "공통",
                "hit_rate": 0.55,
                "objective_score": 0.72,
                "test_session_count": 12,
            }
        ]
    )

    rows = _build_weekly_rows(
        alpha_promotion=alpha,
        evaluation_summary=evaluation,
        policy_eval=policy,
        built_at="2026-03-23T18:00:00+09:00",
        as_of_date="2026-03-23",
        source_run_id="run-1",
    )

    assert any(row["title"] == "하루 보유 기준 모델 점검" for row in rows)
    assert any(row["title"] == "하루 보유 기준 성과 요약" for row in rows)
    assert any(row["title"] == "하루 보유 기준 정책 점검" for row in rows)


def test_build_stock_summary_rows_prefers_live_grades_when_available() -> None:
    summary = pd.DataFrame(
        [
            {
                "symbol": "005930",
                "company_name": "삼성전자",
                "market": "KOSPI",
                "ret_5d": 0.023,
                "ret_20d": 0.051,
                "news_count_3d": 4,
                "d1_selection_v2_grade": "B",
                "d5_selection_v2_grade": "A",
                "d5_alpha_expected_excess_return": 0.013,
                "d5_alpha_uncertainty_score": 0.21,
            }
        ]
    )
    live = pd.DataFrame(
        [
            {
                "symbol": "005930",
                "live_d1_selection_v2_grade": "A",
                "live_d5_selection_v2_grade": "S",
                "live_d5_expected_excess_return": 0.017,
            }
        ]
    )

    rows = _build_stock_summary_rows(
        summary_frame=summary,
        live_frame=live,
        built_at="2026-03-23T18:00:00+09:00",
        as_of_date="2026-03-23",
        source_run_id="run-1",
    )

    assert len(rows) == 1
    assert rows[0]["snapshot_type"] == "stock_summary"
    assert "D1 A" in rows[0]["summary"]
    assert "D5 S" in rows[0]["summary"]
