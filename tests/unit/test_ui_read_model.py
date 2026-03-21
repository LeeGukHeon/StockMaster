from __future__ import annotations

import json

import pandas as pd

from app.ui.helpers import (
    latest_portfolio_target_book_frame,
    latest_recommendation_timeline,
    leaderboard_frame,
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
