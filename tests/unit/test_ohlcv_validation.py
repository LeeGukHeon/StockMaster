from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from app.domain.validation.market_data import validate_daily_ohlcv


def test_validate_daily_ohlcv_accepts_valid_rows():
    frame = pd.DataFrame(
        [
            {
                "trading_date": date(2026, 3, 6),
                "symbol": "005930",
                "open": 100.0,
                "high": 110.0,
                "low": 95.0,
                "close": 105.0,
                "volume": 1000,
            }
        ]
    )

    validate_daily_ohlcv(frame)


def test_validate_daily_ohlcv_rejects_invalid_ranges():
    frame = pd.DataFrame(
        [
            {
                "trading_date": date(2026, 3, 6),
                "symbol": "005930",
                "open": 100.0,
                "high": 99.0,
                "low": 95.0,
                "close": 105.0,
                "volume": 1000,
            }
        ]
    )

    with pytest.raises(ValueError, match="invalid_ranges"):
        validate_daily_ohlcv(frame)
