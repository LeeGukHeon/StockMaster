from __future__ import annotations

import json

import pandas as pd

from app.discord_bot import read_store


def test_fetch_discord_bot_snapshot_rows_orders_by_latest_snapshot(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(read_store, "metadata_postgres_enabled", lambda _settings: True)

    def fake_fetchdf(settings, query, params):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(read_store, "fetchdf_postgres_sql", fake_fetchdf)

    read_store.fetch_discord_bot_snapshot_rows(object(), snapshot_type="status", limit=1)

    query = str(captured["query"])
    assert "ORDER BY snapshot_ts DESC, sort_order NULLS LAST, snapshot_key" in query


def test_fetch_active_job_runs_requires_unreleased_active_lock(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(read_store, "metadata_postgres_enabled", lambda _settings: True)

    def fake_fetchdf(settings, query, params):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(read_store, "fetchdf_postgres_sql", fake_fetchdf)

    read_store.fetch_active_job_runs(object(), limit=5)

    query = str(captured["query"])
    assert "JOIN fact_active_lock AS active_lock" in query
    assert "active_lock.owner_run_id = job.run_id" in query
    assert "active_lock.released_at IS NULL" in query


def test_build_pick_rows_uses_d5_buyability_basket_after_hard_blocks() -> None:
    frame = pd.DataFrame(
        [
            {
                "horizon": 5,
                "eligible_flag": True,
                "symbol": "111111",
                "company_name": "차단종목",
                "market": "KOSDAQ",
                "industry": "바이오",
                "sector": "헬스케어",
                "final_selection_value": 90.0,
                "grade": "A",
                "selection_date": "2026-04-24",
                "next_entry_trade_date": "2026-04-27",
                "expected_excess_return": 0.20,
                "uncertainty_score": 1.0,
                "disagreement_score": 1.0,
                "model_spec_id": "alpha_swing_d5_v2",
                "reasons": '["residual_strength_improving"]',
                "risks": '["thin_liquidity"]',
            },
            {
                "horizon": 5,
                "eligible_flag": True,
                "symbol": "222222",
                "company_name": "고불확실",
                "market": "KOSDAQ",
                "industry": "소재",
                "sector": "산업재",
                "final_selection_value": 82.0,
                "grade": "B",
                "selection_date": "2026-04-24",
                "next_entry_trade_date": "2026-04-27",
                "expected_excess_return": 0.04,
                "uncertainty_score": 95.0,
                "disagreement_score": 90.0,
                "model_spec_id": "alpha_swing_d5_v2",
                "reasons": '["raw_alpha_leader_preserved"]',
                "risks": '["model_disagreement_high"]',
            },
            {
                "horizon": 5,
                "eligible_flag": True,
                "symbol": "000001",
                "company_name": "저점수고우선",
                "market": "KOSDAQ",
                "industry": "기타",
                "sector": "기타",
                "final_selection_value": 10.0,
                "grade": "C",
                "selection_date": "2026-04-24",
                "next_entry_trade_date": "2026-04-27",
                "expected_excess_return": 0.05,
                "uncertainty_score": 1.0,
                "disagreement_score": 1.0,
                "model_spec_id": "alpha_swing_d5_v2",
                "reasons": '["ml_alpha_supportive"]',
                "risks": "[]",
            },
            {
                "horizon": 5,
                "eligible_flag": True,
                "symbol": "333333",
                "company_name": "안정후보",
                "market": "KOSDAQ",
                "industry": "반도체",
                "sector": "기술",
                "final_selection_value": 48.0,
                "grade": "C",
                "selection_date": "2026-04-24",
                "next_entry_trade_date": "2026-04-27",
                "expected_excess_return": 0.03,
                "uncertainty_score": 2.0,
                "disagreement_score": 3.0,
                "model_spec_id": "alpha_swing_d5_v2",
                "reasons": '["residual_strength_improving"]',
                "risks": "[]",
            },
        ]
    )

    rows = read_store._build_pick_rows(
        frame,
        horizon=5,
        built_at="2026-04-27T00:00:00+09:00",
        as_of_date="2026-04-24",
        source_run_id="test",
    )

    assert len(rows) == 2
    assert rows[0]["symbol"] == "333333"
    assert rows[0]["sort_order"] == 1
    assert "매수해볼 가치 있음" in rows[0]["summary"]
    assert "추천권" in rows[0]["payload_json"]
    assert rows[1]["symbol"] == "222222"
    assert "매수 보류" not in rows[0]["summary"]
    assert rows[0]["payload_json"]


def test_build_pick_rows_labels_weak_d5_candidate_as_observation() -> None:
    frame = pd.DataFrame(
        [
            {
                "horizon": 5,
                "eligible_flag": True,
                "symbol": "999999",
                "company_name": "강한후보",
                "market": "KOSDAQ",
                "industry": "-",
                "sector": "-",
                "final_selection_value": 60.0,
                "grade": "A",
                "selection_date": "2026-04-27",
                "next_entry_trade_date": "2026-04-28",
                "expected_excess_return": 0.012,
                "uncertainty_score": 1.0,
                "disagreement_score": 1.0,
                "model_spec_id": "alpha_practical_d5_v2",
                "reasons": '["flow_persistence_supportive"]',
                "risks": "[]",
            },
            {
                "horizon": 5,
                "eligible_flag": True,
                "symbol": "054050",
                "company_name": "농우바이오",
                "market": "KOSDAQ",
                "industry": "-",
                "sector": "-",
                "final_selection_value": 54.7,
                "grade": "A",
                "selection_date": "2026-04-27",
                "next_entry_trade_date": "2026-04-28",
                "expected_excess_return": 0.0018,
                "uncertainty_score": 17.5,
                "disagreement_score": 1.5,
                "model_spec_id": "alpha_practical_d5_v2",
                "reasons": '["flow_persistence_supportive"]',
                "risks": "[]",
            }
        ]
    )

    rows = read_store._build_pick_rows(
        frame,
        horizon=5,
        built_at="2026-04-28T00:00:00+09:00",
        as_of_date="2026-04-27",
        source_run_id="test",
    )

    weak_row = next(row for row in rows if row["symbol"] == "054050")
    assert "관찰 우선" in weak_row["summary"]
    assert "매수해볼 가치 있음" not in weak_row["summary"]
    assert "D5 기대값 약함" in weak_row["payload_json"]


def test_build_stock_summary_rows_omits_news_noise() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "005930",
                "company_name": "삼성전자",
                "market": "KOSPI",
                "d1_selection_v2_grade": "B",
                "d5_selection_v2_grade": "C",
                "d5_alpha_expected_excess_return": 0.01,
                "d5_selection_v2_value": 60.0,
                "ret_5d": 0.02,
                "ret_20d": 0.03,
                "news_count_3d": 99,
                "d5_alpha_uncertainty_score": 1.0,
            }
        ]
    )

    rows = read_store._build_stock_summary_rows(
        summary_frame=frame,
        live_frame=pd.DataFrame(),
        built_at="2026-04-27T00:00:00+09:00",
        as_of_date="2026-04-24",
        source_run_id="test",
    )

    assert len(rows) == 1
    assert "뉴스" not in rows[0]["summary"]


def test_build_stock_summary_rows_uses_buyability_priority_for_d5_candidate() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "009540",
                "company_name": "HD한국조선해양",
                "market": "KOSPI",
                "d1_selection_v2_grade": "B",
                "d5_selection_v2_grade": "A",
                "d5_alpha_expected_excess_return": 0.0065,
                "d5_selection_v2_value": 59.1,
                "ret_5d": 0.02,
                "ret_20d": 0.03,
                "news_count_3d": 0,
                "d5_alpha_uncertainty_score": 17.3,
                "d5_alpha_disagreement_score": 33.7,
            }
        ]
    )
    live_frame = pd.DataFrame(
        [
            {
                "symbol": "009540",
                "live_d5_selection_v2_grade": "A",
                "live_d5_expected_excess_return": 0.0065,
                "live_d5_selection_v2_value": 59.1,
                "live_d5_selection_rank": 1,
                "live_d5_uncertainty_score": 17.3,
                "live_d5_disagreement_score": 33.7,
                "live_d5_risk_flags_json": "[]",
                "live_d5_top_reason_tags_json": '["residual_strength_improving"]',
            }
        ]
    )

    rows = read_store._build_stock_summary_rows(
        summary_frame=frame,
        live_frame=live_frame,
        built_at="2026-04-28T00:00:00+09:00",
        as_of_date="2026-04-27",
        source_run_id="test",
    )

    assert len(rows) == 1
    assert "매수검토" in rows[0]["summary"]
    assert "매수해볼 가치 있음" not in rows[0]["summary"]
    assert "추천권·분할 접근" in rows[0]["payload_json"]
    assert "buyability_priority_score" in rows[0]["payload_json"]


def test_build_stock_summary_rows_uses_d5_display_rank_not_raw_score_rank() -> None:
    summary_frame = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "company_name": symbol,
                "market": "KOSPI",
                "sector": "조선" if symbol == "009540" else "기타",
                "industry": "조선" if symbol == "009540" else "기타",
                "d1_selection_v2_grade": "B",
                "d5_selection_v2_grade": "A",
                "d5_alpha_expected_excess_return": expected,
                "d5_selection_v2_value": score,
                "ret_5d": 0.0,
                "ret_20d": 0.0,
                "news_count_3d": 0,
                "d5_alpha_uncertainty_score": uncertainty,
                "d5_alpha_disagreement_score": disagreement,
            }
            for symbol, score, expected, uncertainty, disagreement in [
                ("000001", 90.0, 0.001, 10.0, 10.0),
                ("000002", 80.0, 0.001, 10.0, 10.0),
                ("000003", 70.0, 0.001, 10.0, 10.0),
                ("009540", 56.5, 0.0065, 17.3, 33.7),
            ]
        ]
    )
    live_frame = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "live_d5_selection_v2_grade": "A",
                "live_d5_expected_excess_return": expected,
                "live_d5_selection_v2_value": score,
                "live_d5_selection_rank": rank,
                "live_d5_uncertainty_score": uncertainty,
                "live_d5_disagreement_score": disagreement,
                "live_d5_risk_flags_json": "[]",
                "live_d5_top_reason_tags_json": '["residual_strength_improving"]',
                "live_d5_eligible_flag": True,
            }
            for rank, (symbol, score, expected, uncertainty, disagreement) in enumerate(
                [
                    ("000001", 90.0, 0.001, 10.0, 10.0),
                    ("000002", 80.0, 0.001, 10.0, 10.0),
                    ("000003", 70.0, 0.001, 10.0, 10.0),
                    ("009540", 56.5, 0.0065, 17.3, 33.7),
                ],
                start=1,
            )
        ]
    )

    rows = read_store._build_stock_summary_rows(
        summary_frame=summary_frame,
        live_frame=live_frame,
        built_at="2026-04-28T00:00:00+09:00",
        as_of_date="2026-04-28",
        source_run_id="test",
    )

    target = next(row for row in rows if row["symbol"] == "009540")
    assert "매수검토" in target["summary"]
    assert "후순위 후보" not in target["payload_json"]
    assert '"d5_display_rank": 1' in target["payload_json"]


def test_build_stock_summary_rows_blocks_validation_edge_guarded_d5_candidate() -> None:
    summary_frame = pd.DataFrame(
        [
            {
                "symbol": "394420",
                "company_name": "리센스메디컬",
                "market": "KOSDAQ",
                "sector": "헬스케어",
                "industry": "바이오",
                "d1_selection_v2_grade": "B",
                "d5_selection_v2_grade": "A",
                "d5_alpha_expected_excess_return": 0.02,
                "d5_selection_v2_value": 80.0,
                "ret_5d": 0.0,
                "ret_20d": 0.0,
                "news_count_3d": 0,
                "d5_alpha_uncertainty_score": 10.0,
                "d5_alpha_disagreement_score": 10.0,
            }
        ]
    )
    live_frame = pd.DataFrame(
        [
            {
                "symbol": "394420",
                "live_d5_selection_v2_grade": "A",
                "live_d5_expected_excess_return": 0.02,
                "live_d5_selection_v2_value": 80.0,
                "live_d5_selection_rank": 2,
                "live_d5_uncertainty_score": 10.0,
                "live_d5_disagreement_score": 10.0,
                "live_d5_risk_flags_json": "[]",
                "live_d5_top_reason_tags_json": '["residual_strength_improving"]',
                "live_d5_eligible_flag": True,
                "live_d5_explanatory_score_json": json.dumps(
                    {
                        "validation_top5_mean_excess_return": -0.018,
                        "validation_top5_edge_guard_applied": True,
                    }
                ),
            }
        ]
    )

    rows = read_store._build_stock_summary_rows(
        summary_frame=summary_frame,
        live_frame=live_frame,
        built_at="2026-04-29T00:00:00+09:00",
        as_of_date="2026-04-28",
        source_run_id="test",
    )

    assert len(rows) == 1
    payload = json.loads(rows[0]["payload_json"])
    assert payload["d5_report_candidate_flag"] is False
    assert payload["d5_display_rank"] is None
