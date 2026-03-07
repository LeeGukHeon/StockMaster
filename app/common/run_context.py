from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterator
from uuid import uuid4

from app.common.time import utc_now

_RUN_ID: ContextVar[str | None] = ContextVar("run_id", default=None)
_RUN_TYPE: ContextVar[str | None] = ContextVar("run_type", default=None)


@dataclass(slots=True)
class RunContext:
    run_id: str
    run_type: str
    started_at: datetime
    as_of_date: date | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def make_run_id(run_type: str) -> str:
    timestamp = utc_now().strftime("%Y%m%dT%H%M%S")
    return f"{run_type}-{timestamp}-{uuid4().hex[:8]}"


def current_run_id() -> str | None:
    return _RUN_ID.get()


def current_run_type() -> str | None:
    return _RUN_TYPE.get()


@contextmanager
def activate_run_context(
    run_type: str,
    *,
    run_id: str | None = None,
    as_of_date: date | None = None,
    metadata: dict[str, Any] | None = None,
) -> Iterator[RunContext]:
    context = RunContext(
        run_id=run_id or make_run_id(run_type),
        run_type=run_type,
        started_at=utc_now(),
        as_of_date=as_of_date,
        metadata=metadata or {},
    )
    run_id_token = _RUN_ID.set(context.run_id)
    run_type_token = _RUN_TYPE.set(context.run_type)
    try:
        yield context
    finally:
        _RUN_ID.reset(run_id_token)
        _RUN_TYPE.reset(run_type_token)
