from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection

from .common import DEFAULT_CHECKPOINTS
from .data import (
    backfill_intraday_candidate_bars,
    backfill_intraday_candidate_quote_summary,
    backfill_intraday_candidate_trade_summary,
)
from .decisions import materialize_intraday_entry_decisions
from .session import load_intraday_candidate_session_frame, materialize_intraday_candidate_session
from .signals import materialize_intraday_signal_snapshots


@dataclass(slots=True)
class IntradayBasePipelineResult:
    session_date: date
    selection_date: date
    candidate_count: int
    checkpoints: list[str]


def resolve_selection_date_for_session(
    settings: Settings,
    *,
    session_date: date,
) -> date:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            """
            SELECT COALESCE(prev_trading_date, (
                SELECT MAX(trading_date)
                FROM dim_trading_calendar
                WHERE trading_date < base.trading_date
                  AND is_trading_day
            ))
            FROM dim_trading_calendar AS base
            WHERE base.trading_date = ?
            """,
            [session_date],
        ).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(
            "Unable to resolve selection_date from session_date. "
            "Run scripts/sync_trading_calendar.py first."
        )
    return pd.Timestamp(row[0]).date()


def ensure_intraday_base_pipeline(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    checkpoints: list[str] | None = None,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    max_candidates: int = 30,
) -> IntradayBasePipelineResult:
    normalized_checkpoints = checkpoints or list(DEFAULT_CHECKPOINTS)
    selection_date = resolve_selection_date_for_session(
        settings,
        session_date=session_date,
    )
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        candidate_frame = load_intraday_candidate_session_frame(
            connection,
            session_date=session_date,
            horizons=horizons,
            ranking_version=ranking_version,
        )
    if candidate_frame.empty:
        materialize_intraday_candidate_session(
            settings,
            selection_date=selection_date,
            horizons=horizons,
            max_candidates=max_candidates,
            ranking_version=ranking_version,
        )
        with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
            bootstrap_core_tables(connection)
            candidate_frame = load_intraday_candidate_session_frame(
                connection,
                session_date=session_date,
                horizons=horizons,
                ranking_version=ranking_version,
            )
    backfill_intraday_candidate_bars(
        settings,
        session_date=session_date,
        horizons=horizons,
        ranking_version=ranking_version,
    )
    backfill_intraday_candidate_trade_summary(
        settings,
        session_date=session_date,
        horizons=horizons,
        ranking_version=ranking_version,
        checkpoint_times=normalized_checkpoints,
    )
    backfill_intraday_candidate_quote_summary(
        settings,
        session_date=session_date,
        horizons=horizons,
        ranking_version=ranking_version,
        checkpoint_times=normalized_checkpoints,
    )
    for checkpoint in normalized_checkpoints:
        materialize_intraday_signal_snapshots(
            settings,
            session_date=session_date,
            checkpoint=checkpoint,
            horizons=horizons,
            ranking_version=ranking_version,
        )
        materialize_intraday_entry_decisions(
            settings,
            session_date=session_date,
            checkpoint=checkpoint,
            horizons=horizons,
            ranking_version=ranking_version,
        )
    return IntradayBasePipelineResult(
        session_date=session_date,
        selection_date=selection_date,
        candidate_count=len(candidate_frame),
        checkpoints=normalized_checkpoints,
    )


def latest_session_date(settings: Settings) -> date | None:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            "SELECT MAX(session_date) FROM fact_intraday_candidate_session"
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return pd.Timestamp(row[0]).date()


def current_timestamp(settings: Settings):
    return now_local(settings.app.timezone)
