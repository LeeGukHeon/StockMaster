from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from typing import Iterable

import pandas as pd

DEFAULT_CHECKPOINTS: tuple[str, ...] = ("09:05", "09:15", "09:30", "10:00", "11:00")
ENTER_ACTIONS: tuple[str, ...] = ("ENTER_NOW",)
INTRADAY_ACTIONS: tuple[str, ...] = (
    "ENTER_NOW",
    "WAIT_RECHECK",
    "AVOID_TODAY",
    "DATA_INSUFFICIENT",
)
INTRADAY_REGIME_FAMILIES: tuple[str, ...] = (
    "PANIC_OPEN",
    "WEAK_RISK_OFF",
    "NEUTRAL_CHOP",
    "HEALTHY_TREND",
    "OVERHEATED_GAP_CHASE",
    "DATA_WEAK",
)
ADJUSTMENT_PROFILES: tuple[str, ...] = (
    "DEFENSIVE",
    "NEUTRAL",
    "SELECTIVE_RISK_ON",
    "GAP_CHASE_GUARD",
    "DATA_WEAK_GUARD",
)
INTRADAY_STRATEGY_IDS: tuple[str, ...] = (
    "SEL_V2_OPEN_ALL",
    "SEL_V2_TIMING_RAW_FIRST_ENTER",
    "SEL_V2_TIMING_ADJ_FIRST_ENTER",
    "SEL_V2_TIMING_ADJ_0930_ONLY",
    "SEL_V2_TIMING_ADJ_1000_ONLY",
)

_CHECKPOINT_FRACTIONS = {
    "09:05": 0.08,
    "09:15": 0.14,
    "09:30": 0.24,
    "10:00": 0.34,
    "11:00": 0.52,
}


def json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def normalize_checkpoint(value: str) -> str:
    cleaned = value.strip()
    if ":" in cleaned:
        hour, minute = cleaned.split(":", 1)
        return f"{int(hour):02d}:{int(minute):02d}"
    if len(cleaned) == 4:
        return f"{cleaned[:2]}:{cleaned[2:]}"
    raise ValueError(f"Unsupported checkpoint format: {value}")


def normalize_checkpoint_to_hhmm(value: str) -> str:
    normalized = normalize_checkpoint(value)
    return normalized.replace(":", "")


def checkpoint_sort_key(value: str) -> tuple[int, int]:
    normalized = normalize_checkpoint(value)
    hour, minute = normalized.split(":")
    return int(hour), int(minute)


def checkpoint_timestamp(session_date: date, checkpoint: str) -> pd.Timestamp:
    normalized = normalize_checkpoint(checkpoint)
    hour, minute = normalized.split(":")
    return pd.Timestamp(
        datetime.combine(session_date, time(hour=int(hour), minute=int(minute))),
        tz="Asia/Seoul",
    )


def iter_trading_minutes(
    session_date: date,
    *,
    start: str = "09:00",
    end: str = "15:20",
) -> list[pd.Timestamp]:
    current = checkpoint_timestamp(session_date, start)
    end_ts = checkpoint_timestamp(session_date, end)
    values: list[pd.Timestamp] = []
    while current <= end_ts:
        values.append(current)
        current += timedelta(minutes=1)
    return values


def checkpoint_fraction(checkpoint: str) -> float:
    normalized = normalize_checkpoint(checkpoint)
    return _CHECKPOINT_FRACTIONS.get(normalized, 0.5)


def clip_score(value: float | int | None, *, lower: float = 0.0, upper: float = 100.0) -> float:
    if value is None or pd.isna(value):
        return 50.0
    return float(max(lower, min(upper, float(value))))


def quality_bucket(value: float | int | None) -> str:
    score = clip_score(value)
    if score >= 80:
        return "high"
    if score >= 55:
        return "medium"
    if score >= 35:
        return "low"
    return "critical"


def session_status(session_date: date, *, today: date) -> str:
    if session_date > today:
        return "planned"
    if session_date == today:
        return "active"
    return "historical"


def rank_list(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys([str(value) for value in values if str(value)]))


def selection_confidence_bucket(
    *,
    final_selection_value: float | int | None,
    percentile: float | int | None,
) -> str:
    score = clip_score(final_selection_value)
    pct = 0.0 if percentile is None or pd.isna(percentile) else float(percentile)
    if score >= 80 or pct >= 0.97:
        return "top"
    if score >= 68 or pct >= 0.90:
        return "high"
    if score >= 55 or pct >= 0.80:
        return "medium"
    return "low"
