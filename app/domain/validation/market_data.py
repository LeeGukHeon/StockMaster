from __future__ import annotations

import pandas as pd


def validate_daily_ohlcv(frame: pd.DataFrame) -> None:
    if frame.empty:
        return

    errors: list[str] = []
    duplicate_count = int(frame.duplicated(subset=["trading_date", "symbol"]).sum())
    if duplicate_count:
        errors.append(f"duplicate_keys={duplicate_count}")

    invalid_price = frame.loc[
        (frame["open"] <= 0) | (frame["high"] <= 0) | (frame["low"] <= 0) | (frame["close"] <= 0)
    ]
    if not invalid_price.empty:
        errors.append(f"invalid_prices={len(invalid_price)}")

    invalid_range = frame.loc[
        (frame["high"] < frame[["open", "close"]].max(axis=1))
        | (frame["low"] > frame[["open", "close"]].min(axis=1))
    ]
    if not invalid_range.empty:
        errors.append(f"invalid_ranges={len(invalid_range)}")

    invalid_volume = frame.loc[frame["volume"] < 0]
    if not invalid_volume.empty:
        errors.append(f"invalid_volume={len(invalid_volume)}")

    if errors:
        raise ValueError("daily_ohlcv validation failed: " + ", ".join(errors))
