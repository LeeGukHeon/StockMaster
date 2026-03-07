from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo


def get_timezone(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def now_local(name: str) -> datetime:
    return datetime.now(tz=get_timezone(name))


def today_local(name: str) -> date:
    return now_local(name).date()


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
