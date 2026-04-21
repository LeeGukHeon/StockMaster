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
