from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

pytest.importorskip("holidays")
from app.ingestion.calendar_sync import build_trading_calendar_frame


def test_build_trading_calendar_marks_holidays_weekends_and_overrides():
    overrides = pd.DataFrame(
        [
            {
                "date": date(2026, 1, 2),
                "is_trading_day": False,
                "holiday_name": "Bridge Holiday",
                "note": "Unit test override",
            }
        ]
    )

    frame = build_trading_calendar_frame(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 5),
        overrides=overrides,
    ).set_index("trading_date")

    assert bool(frame.loc[date(2026, 1, 1), "is_public_holiday"]) is True
    assert bool(frame.loc[date(2026, 1, 2), "is_override"]) is True
    assert bool(frame.loc[date(2026, 1, 2), "is_trading_day"]) is False
    assert bool(frame.loc[date(2026, 1, 3), "is_weekend"]) is True
    assert frame.loc[date(2026, 1, 4), "next_trading_date"] == date(2026, 1, 5)
    assert frame.loc[date(2026, 1, 5), "prev_trading_date"] is None


def test_build_trading_calendar_allows_year_end_market_closure_override():
    overrides = pd.DataFrame(
        [
            {
                "date": date(2025, 12, 31),
                "is_trading_day": False,
                "holiday_name": "KRX Year-end Closure",
                "note": "Market-specific override",
            }
        ]
    )

    frame = build_trading_calendar_frame(
        start_date=date(2025, 12, 30),
        end_date=date(2026, 1, 2),
        overrides=overrides,
    ).set_index("trading_date")

    assert bool(frame.loc[date(2025, 12, 31), "is_override"]) is True
    assert bool(frame.loc[date(2025, 12, 31), "is_trading_day"]) is False
    assert frame.loc[date(2025, 12, 31), "market_session_type"] == "closed"
    assert frame.loc[date(2025, 12, 31), "holiday_name"] == "KRX Year-end Closure"


def test_build_trading_calendar_marks_krx_labor_day_closed():
    frame = build_trading_calendar_frame(
        start_date=date(2026, 4, 30),
        end_date=date(2026, 5, 4),
    ).set_index("trading_date")

    assert bool(frame.loc[date(2026, 5, 1), "is_trading_day"]) is False
    assert bool(frame.loc[date(2026, 5, 1), "is_public_holiday"]) is False
    assert frame.loc[date(2026, 5, 1), "market_session_type"] == "closed"
    assert frame.loc[date(2026, 5, 1), "holiday_name"] == "Labor Day"
    assert frame.loc[date(2026, 5, 1), "source"] == "krx_holiday"
    assert bool(frame.loc[date(2026, 5, 2), "is_weekend"]) is True
    assert bool(frame.loc[date(2026, 5, 2), "is_public_holiday"]) is False
    assert frame.loc[date(2026, 5, 2), "holiday_name"] is None
    assert frame.loc[date(2026, 5, 2), "source"] == "weekend+kr_holidays"
    assert bool(frame.loc[date(2026, 5, 3), "is_weekend"]) is True
    assert bool(frame.loc[date(2026, 5, 3), "is_public_holiday"]) is False
    assert frame.loc[date(2026, 5, 3), "holiday_name"] is None
    assert frame.loc[date(2026, 5, 3), "source"] == "weekend+kr_holidays"
    assert frame.loc[date(2026, 4, 30), "next_trading_date"] == date(2026, 5, 4)
    assert frame.loc[date(2026, 5, 1), "prev_trading_date"] == date(2026, 4, 30)
    assert frame.loc[date(2026, 5, 1), "next_trading_date"] == date(2026, 5, 4)
    assert frame.loc[date(2026, 5, 4), "prev_trading_date"] == date(2026, 4, 30)
