from __future__ import annotations

from datetime import date

import pandas as pd

from app.ui.helpers import latest_intraday_console_basis_summary, latest_market_mood_summary
from tests._ticket003_support import build_test_settings


def test_latest_market_mood_summary_prefers_intraday_context(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    monkeypatch.setattr("app.ui.helpers.today_local", lambda _: date(2026, 3, 12))
    monkeypatch.setattr(
        "app.ui.helpers.latest_intraday_market_context_frame",
        lambda settings, session_date=None, limit=50: pd.DataFrame(
            [
                {
                    "session_date": date(2026, 3, 12),
                    "checkpoint_time": "1000",
                    "context_scope": "market",
                    "market_session_state": "active",
                    "prior_daily_regime_state": "neutral",
                    "market_breadth_ratio": 0.68,
                    "candidate_mean_return_from_open": 0.006,
                    "bar_coverage_ratio": 0.91,
                    "trade_coverage_ratio": 0.84,
                    "quote_coverage_ratio": 0.88,
                    "data_quality_flag": "strong",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "app.ui.helpers.latest_regime_frame",
        lambda settings: pd.DataFrame(),
    )

    result = latest_market_mood_summary(settings)

    assert result["mode"] == "intraday"
    assert result["headline"] == "장중 강세"
    assert result["label"] == "3월 12일 10:00 기준"


def test_latest_market_mood_summary_falls_back_to_daily_regime(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    monkeypatch.setattr("app.ui.helpers.today_local", lambda _: date(2026, 3, 12))
    monkeypatch.setattr(
        "app.ui.helpers.latest_intraday_market_context_frame",
        lambda settings, session_date=None, limit=50: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "app.ui.helpers.latest_regime_frame",
        lambda settings: pd.DataFrame(
            [
                {
                    "as_of_date": date(2026, 3, 11),
                    "market_scope": "KR_ALL",
                    "regime_state": "risk_on",
                }
            ]
        ),
    )

    result = latest_market_mood_summary(settings)

    assert result["mode"] == "daily"
    assert result["headline"] == "상승 우위 장"
    assert result["label"] == "3월 11일 종가 기준"


def test_latest_intraday_console_basis_summary_marks_historical_session(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    monkeypatch.setattr("app.ui.helpers._latest_intraday_session_date", lambda settings: date(2026, 3, 9))
    monkeypatch.setattr("app.ui.helpers.today_local", lambda _: date(2026, 3, 12))
    monkeypatch.setattr(
        "app.ui.helpers.latest_intraday_market_context_frame",
        lambda settings, session_date=None, limit=50: pd.DataFrame(
            [
                {
                    "session_date": date(2026, 3, 9),
                    "checkpoint_time": "1100",
                    "context_scope": "market",
                    "market_session_state": "historical",
                    "prior_daily_regime_state": "risk_on",
                    "bar_coverage_ratio": 0.91,
                    "trade_coverage_ratio": 0.88,
                    "quote_coverage_ratio": 0.86,
                    "data_quality_flag": "strong",
                }
            ]
        ),
    )

    result = latest_intraday_console_basis_summary(settings)

    assert result["mode"] == "historical"
    assert result["headline"] == "마지막 저장 세션"
    assert result["label"] == "3월 9일 11:00 기준"


def test_latest_intraday_console_basis_summary_marks_stale_today_session(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    monkeypatch.setattr("app.ui.helpers._latest_intraday_session_date", lambda settings: date(2026, 3, 12))
    monkeypatch.setattr("app.ui.helpers.today_local", lambda _: date(2026, 3, 12))
    monkeypatch.setattr(
        "app.ui.helpers.latest_intraday_market_context_frame",
        lambda settings, session_date=None, limit=50: pd.DataFrame(
            [
                {
                    "session_date": date(2026, 3, 12),
                    "checkpoint_time": "1000",
                    "context_scope": "market",
                    "market_session_state": "active",
                    "prior_daily_regime_state": "neutral",
                    "bar_coverage_ratio": 0.2,
                    "trade_coverage_ratio": 0.3,
                    "quote_coverage_ratio": 0.25,
                    "data_quality_flag": "strong",
                }
            ]
        ),
    )

    result = latest_intraday_console_basis_summary(settings)

    assert result["mode"] == "stale"
    assert result["headline"] == "장중 데이터 보강 중"
