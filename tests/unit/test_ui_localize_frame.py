from __future__ import annotations

import pandas as pd

from app.ui.helpers import (
    format_ui_date,
    format_ui_datetime,
    format_ui_number,
    format_ui_run_id,
    format_ui_value,
    localize_frame,
)


def test_localize_frame_formats_percent_like_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "expected_excess_return": 0.0123,
                "lower_band": -0.0345,
                "target_weight": 0.25,
                "execution_rate": 0.6,
                "final_selection_rank_pct": 0.125,
                "final_selection_value": 0.87,
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.iloc[0].tolist() == [
        "1.23%",
        "-3.45%",
        "25.00%",
        "60.00%",
        "12.50%",
        "0.87",
    ]


def test_localize_frame_formats_missing_values_as_dash() -> None:
    frame = pd.DataFrame(
        [
            {
                "target_price": float("nan"),
                "action_target_price": None,
                "target_weight": float("nan"),
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.iloc[0].tolist() == ["-", "-", "-"]


def test_localize_frame_formats_dates_times_and_run_ids() -> None:
    started_at = pd.Timestamp("2026-03-12 04:14:54", tz="Asia/Seoul")
    run_id = "run_daily_close_bundle-20260311T191454-837676de"
    frame = pd.DataFrame(
        [
            {
                "as_of_date": pd.Timestamp("2026-03-11"),
                "checkpoint_time": "0900",
                "started_at": started_at,
                "run_id": run_id,
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.iloc[0].tolist() == [
        format_ui_date(pd.Timestamp("2026-03-11")),
        "09:00",
        format_ui_datetime(started_at),
        format_ui_run_id(run_id),
    ]


def test_localize_frame_formats_scores_and_numbers_cleanly() -> None:
    frame = pd.DataFrame(
        [
            {
                "action_score": 82.444,
                "sample_count": 1250,
                "metric_value": 3.14159,
                "turnover_value": 1500000000,
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.iloc[0].tolist() == [
        "82.4",
        "1,250",
        format_ui_number(3.14159),
        "1,500,000,000",
    ]


def test_localize_frame_translates_case_insensitive_enum_values() -> None:
    frame = pd.DataFrame(
        [
            {
                "market_regime_family": "risk_on",
                "health_status": "CRITICAL",
                "trigger_type": "RECOVERY",
                "prior_daily_regime_state": "RISK_OFF",
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.iloc[0].tolist() == [
        "상승 우위 장",
        "치명",
        "자동 복구",
        "방어 우위 장",
    ]


def test_format_ui_value_translates_scalar_outside_tables() -> None:
    assert format_ui_value("regime_state", "risk_on") == "상승 우위 장"
    assert format_ui_value("health_status", "WARNING") == "주의"


def test_localize_frame_formats_json_columns_into_readable_korean() -> None:
    frame = pd.DataFrame(
        [
            {
                "policy_reason_codes_json": '["momentum_confirmed", "data_weak_guard"]',
                "detail_json": '{"status":"FAILED","recommended_action":"재실행 필요","sample_count":3}',
                "blocked_reason": '["thin_liquidity", "large_recent_drawdown"]',
                "top_actionable_symbol_list_json": '[{"symbol":"005930","grade":"A"},{"symbol":"000660","grade":"B"}]',
                "active_meta_model_ids_json": '["meta_enter_v1","meta_wait_v1"]',
            }
        ]
    )

    localized = localize_frame(frame)
    row = localized.iloc[0].tolist()

    assert "모멘텀 확인" in row[0]
    assert "데이터 약함 방어" in row[0]
    assert "상태" in row[1]
    assert "실패" in row[1]
    assert "{" not in row[1]
    assert "유동성 부족" in row[2]
    assert "최근 낙폭 큼" in row[2]
    assert "005930" in row[3]
    assert "000660" in row[3]
    assert row[4] == "사용 중 2개"
