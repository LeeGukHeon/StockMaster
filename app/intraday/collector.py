from __future__ import annotations

import time as time_module
from dataclasses import dataclass
from datetime import date

from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings

from .common import DEFAULT_CHECKPOINTS, checkpoint_sort_key
from .data import (
    backfill_intraday_candidate_bars,
    backfill_intraday_candidate_quote_summary,
    backfill_intraday_candidate_trade_summary,
)
from .decisions import materialize_intraday_entry_decisions
from .signals import materialize_intraday_signal_snapshots


@dataclass(slots=True)
class IntradayCollectorResult:
    session_date: date
    cycle_count: int
    processed_checkpoints: list[str]
    notes: str


def run_intraday_candidate_collector(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    poll_seconds: int = 15,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    dry_run: bool = False,
    max_cycles: int = 1,
) -> IntradayCollectorResult:
    processed: list[str] = []
    cycle_count = 0
    while cycle_count < max_cycles:
        cycle_count += 1
        backfill_intraday_candidate_bars(
            settings,
            session_date=session_date,
            horizons=horizons,
            ranking_version=ranking_version,
            dry_run=dry_run,
        )
        backfill_intraday_candidate_trade_summary(
            settings,
            session_date=session_date,
            horizons=horizons,
            ranking_version=ranking_version,
        )
        backfill_intraday_candidate_quote_summary(
            settings,
            session_date=session_date,
            horizons=horizons,
            ranking_version=ranking_version,
        )

        current_hhmm = now_local(settings.app.timezone).strftime("%H:%M")
        target_checkpoints = [
            checkpoint
            for checkpoint in DEFAULT_CHECKPOINTS
            if dry_run or checkpoint_sort_key(checkpoint) <= checkpoint_sort_key(current_hhmm)
        ]
        for checkpoint in target_checkpoints:
            if checkpoint in processed:
                continue
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
            processed.append(checkpoint)

        if dry_run or cycle_count >= max_cycles:
            break
        time_module.sleep(max(1, int(poll_seconds)))

    notes = (
        "Intraday candidate collector completed. "
        f"session_date={session_date.isoformat()} cycles={cycle_count} checkpoints={processed}"
    )
    return IntradayCollectorResult(
        session_date=session_date,
        cycle_count=cycle_count,
        processed_checkpoints=processed,
        notes=notes,
    )
