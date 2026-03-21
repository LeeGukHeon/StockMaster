from __future__ import annotations

import json

import pandas as pd

from app.ui.helpers import (
    available_symbol_options,
    latest_portfolio_target_book_frame,
    latest_intraday_status_frame,
    latest_recommendation_timeline,
    leaderboard_frame,
    stock_workbench_summary_frame,
)
from app.ui.read_model import (
    ui_read_model_dataset_path,
    ui_read_model_manifest_path,
)
from tests._ticket003_support import build_test_settings


def _write_read_model_manifest(settings, payload: dict[str, object]) -> None:
    path = ui_read_model_manifest_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_read_model_frame(settings, dataset_name: str, frame: pd.DataFrame) -> None:
    path = ui_read_model_dataset_path(settings, dataset_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_leaderboard_frame_uses_read_model_snapshot(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    _write_read_model_manifest(
        settings,
        {
            "ranking_version": "selection_engine_v2",
            "ranking_as_of_date": "2026-03-20",
        },
    )
    _write_read_model_frame(
        settings,
        "leaderboard",
        pd.DataFrame(
            [
                {
                    "as_of_date": "2026-03-20",
                    "selection_date": "2026-03-20",
                    "next_entry_trade_date": "2026-03-23",
                    "symbol": "005930",
                    "company_name": "SamsungElec",
                    "market": "KOSPI",
                    "sector": "IT",
                    "industry": "Semis",
                    "horizon": 5,
                    "final_selection_value": 0.91,
                    "final_selection_rank_pct": 0.02,
                    "grade": "A",
                    "regime_state": "risk_on",
                    "ranking_version": "selection_engine_v2",
                    "top_reason_tags_json": "[]",
                    "risk_flags_json": "[]",
                    "explanatory_score_json": "{}",
                    "expected_excess_return": 0.031,
                    "lower_band": -0.015,
                    "median_band": 0.02,
                    "upper_band": 0.055,
                    "model_spec_id": "alpha_recursive_expanding_v1",
                    "active_alpha_model_id": "active-1",
                    "uncertainty_score": 0.12,
                    "disagreement_score": 0.03,
                    "fallback_flag": False,
                    "fallback_reason": None,
                    "selection_close_price": 100000.0,
                    "outcome_status": None,
                    "realized_excess_return": None,
                    "band_status": None,
                    "flat_target_price": 103100.0,
                    "flat_upper_target_price": 105500.0,
                    "flat_stop_price": 98500.0,
                    "reasons": "[]",
                    "risks": "[]",
                }
            ]
        ),
    )

    frame = leaderboard_frame(settings, horizon=5, market="KOSPI", limit=10)

    assert len(frame) == 1
    assert frame.iloc[0]["symbol"] == "005930"
    assert frame.iloc[0]["market"] == "KOSPI"


def test_portfolio_target_book_frame_uses_read_model_snapshot(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    _write_read_model_manifest(
        settings,
        {
            "portfolio_as_of_date": "2026-03-20",
        },
    )
    _write_read_model_frame(
        settings,
        "portfolio_target_book",
        pd.DataFrame(
            [
                {
                    "as_of_date": "2026-03-20",
                    "session_date": "2026-03-20",
                    "execution_mode": "TIMING_ASSISTED",
                    "symbol": "005930",
                    "company_name": "SamsungElec",
                    "market": "KOSPI",
                    "sector": "IT",
                    "candidate_state": "selected",
                    "target_rank": 1,
                    "target_weight": 0.2,
                    "target_notional": 2000000.0,
                    "target_shares": 20,
                    "target_price": 100000.0,
                    "plan_horizon": 5,
                    "entry_trade_date": "2026-03-23",
                    "exit_trade_date": "2026-03-30",
                    "entry_basis": "open",
                    "exit_basis": "close",
                    "model_spec_id": "alpha_recursive_expanding_v1",
                    "active_alpha_model_id": "active-1",
                    "action_plan_label": "진입",
                    "target_return": 0.03,
                    "stretch_target_return": 0.05,
                    "stop_return": -0.02,
                    "action_target_price": 103000.0,
                    "action_stretch_price": 105000.0,
                    "action_stop_price": 98000.0,
                    "current_shares": 0,
                    "current_weight": 0.0,
                    "score_value": 0.91,
                    "gate_status": "OPEN",
                    "included_flag": True,
                    "waitlist_flag": False,
                    "waitlist_rank": None,
                    "blocked_flag": False,
                    "blocked_reason": None,
                },
                {
                    "as_of_date": "2026-03-20",
                    "session_date": "2026-03-20",
                    "execution_mode": "OPEN_ALL",
                    "symbol": "__CASH__",
                    "company_name": "Cash",
                    "market": "CASH",
                    "sector": "Cash",
                    "candidate_state": "cash",
                    "target_rank": 999,
                    "target_weight": 0.1,
                    "target_notional": 1000000.0,
                    "target_shares": 1,
                    "target_price": 1.0,
                    "plan_horizon": 0,
                    "entry_trade_date": None,
                    "exit_trade_date": None,
                    "entry_basis": None,
                    "exit_basis": None,
                    "model_spec_id": None,
                    "active_alpha_model_id": None,
                    "action_plan_label": "현금",
                    "target_return": None,
                    "stretch_target_return": None,
                    "stop_return": None,
                    "action_target_price": None,
                    "action_stretch_price": None,
                    "action_stop_price": None,
                    "current_shares": 1,
                    "current_weight": 0.1,
                    "score_value": None,
                    "gate_status": "OPEN",
                    "included_flag": True,
                    "waitlist_flag": False,
                    "waitlist_rank": None,
                    "blocked_flag": False,
                    "blocked_reason": None,
                },
            ]
        ),
    )

    frame = latest_portfolio_target_book_frame(
        settings,
        execution_mode="TIMING_ASSISTED",
        included_only=True,
        include_cash=False,
        limit=10,
    )

    assert len(frame) == 1
    assert frame.iloc[0]["symbol"] == "005930"
    assert frame.iloc[0]["execution_mode"] == "TIMING_ASSISTED"


def test_latest_recommendation_timeline_uses_read_model_manifest(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    _write_read_model_manifest(
        settings,
        {
            "recommendation_timeline": {
                "selection_as_of_date": "2026-03-20",
                "portfolio_as_of_date": "2026-03-20",
                "portfolio_session_date": "2026-03-23",
                "intraday_session_date": None,
            }
        },
    )

    timeline = latest_recommendation_timeline(settings)

    assert timeline["selection_as_of_date"] == "2026-03-20"
    assert timeline["portfolio_session_date"] == "2026-03-23"


def test_available_symbol_options_uses_read_model_snapshot(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    _write_read_model_frame(
        settings,
        "symbol_options",
        pd.DataFrame(
            [
                {"symbol": "005930", "company_name": "SamsungElec"},
                {"symbol": "000660", "company_name": "SKHynix"},
            ]
        ),
    )

    options = available_symbol_options(settings)

    assert options == [("005930", "SamsungElec"), ("000660", "SKHynix")]


def test_stock_workbench_summary_frame_uses_read_model_snapshot(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    _write_read_model_frame(
        settings,
        "stock_workbench_summary",
        pd.DataFrame(
            [
                {
                    "symbol": "005930",
                    "company_name": "SamsungElec",
                    "market": "KOSPI",
                    "as_of_date": "2026-03-20",
                    "ret_5d": 0.05,
                    "ret_20d": 0.12,
                    "adv_20": 1000000.0,
                    "news_count_3d": 4,
                    "foreign_net_value_ratio_5d": 0.04,
                    "smart_money_flow_ratio_20d": 0.11,
                    "flow_coverage_flag": True,
                    "d1_selection_v2_value": 0.82,
                    "d1_selection_v2_grade": "A",
                    "d1_selection_value": 0.71,
                    "d1_grade": "B",
                    "d5_selection_v2_value": 0.91,
                    "d5_selection_v2_grade": "A",
                    "d5_selection_value": 0.73,
                    "d5_grade": "B",
                    "d5_alpha_expected_excess_return": 0.03,
                    "d5_alpha_lower_band": -0.01,
                    "d5_alpha_upper_band": 0.05,
                    "d5_alpha_uncertainty_score": 0.12,
                    "d5_alpha_disagreement_score": 0.03,
                    "d5_alpha_fallback_flag": False,
                    "d5_expected_excess_return": 0.025,
                    "d5_lower_band": -0.015,
                    "d5_upper_band": 0.04,
                    "d1_realized_excess_return": None,
                    "d1_band_status": None,
                    "d5_selection_v2_realized_excess_return": None,
                    "d5_selection_v2_band_status": None,
                    "d5_realized_excess_return": None,
                    "d5_band_status": None,
                }
            ]
        ),
    )

    frame = stock_workbench_summary_frame(settings, symbol="005930")

    assert len(frame) == 1
    assert frame.iloc[0]["company_name"] == "SamsungElec"
    assert frame.iloc[0]["d5_selection_v2_grade"] == "A"


def test_latest_intraday_status_frame_uses_read_model_snapshot(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    _write_read_model_frame(
        settings,
        "intraday_status_latest",
        pd.DataFrame(
            [
                {
                    "session_date": "2026-03-20",
                    "candidate_symbols": 15,
                    "bar_symbols": 15,
                    "trade_symbols": 15,
                    "quote_symbols": 15,
                    "signal_symbols": 15,
                    "raw_decision_symbols": 15,
                    "adjusted_symbols": 10,
                    "meta_prediction_symbols": 10,
                    "meta_decision_symbols": 10,
                    "final_action_symbols": 10,
                    "avg_bar_latency_ms": 120.0,
                    "avg_quote_latency_ms": 98.0,
                }
            ]
        ),
    )

    frame = latest_intraday_status_frame(settings)

    assert len(frame) == 1
    assert int(frame.iloc[0]["candidate_symbols"]) == 15
