from __future__ import annotations

from datetime import date
from pathlib import Path

from app.intraday.data import (
    backfill_intraday_candidate_bars,
    backfill_intraday_candidate_quote_summary,
    backfill_intraday_candidate_trade_summary,
)
from app.intraday.decisions import materialize_intraday_entry_decisions
from app.intraday.evaluation import evaluate_intraday_timing_layer
from app.intraday.report import render_intraday_monitor_report
from app.intraday.session import materialize_intraday_candidate_session
from app.intraday.signals import materialize_intraday_signal_snapshots
from app.ml.training import train_alpha_model_v1
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def _prepare_ticket007_data(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings, limit_symbols=4)
    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    materialize_selection_engine_v2(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )
    return settings


def test_materialize_intraday_candidate_session(tmp_path):
    settings = _prepare_ticket007_data(tmp_path)

    result = materialize_intraday_candidate_session(
        settings,
        selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        max_candidates=2,
    )

    assert result.session_date == date(2026, 3, 9)
    assert result.row_count == 4

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_candidate_session
            WHERE selection_date = ?
              AND session_date = ?
            """,
            [date(2026, 3, 6), date(2026, 3, 9)],
        ).fetchone()
    assert int(row[0]) == 4


def test_intraday_backfill_signal_decision_evaluation_and_report(tmp_path):
    settings = _prepare_ticket007_data(tmp_path)
    materialize_intraday_candidate_session(
        settings,
        selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        max_candidates=2,
    )

    bars_result = backfill_intraday_candidate_bars(
        settings,
        session_date=date(2026, 3, 9),
        horizons=[1, 5],
        ranking_version="selection_engine_v2",
    )
    trade_result = backfill_intraday_candidate_trade_summary(
        settings,
        session_date=date(2026, 3, 9),
        horizons=[1, 5],
        ranking_version="selection_engine_v2",
    )
    quote_result = backfill_intraday_candidate_quote_summary(
        settings,
        session_date=date(2026, 3, 9),
        horizons=[1, 5],
        ranking_version="selection_engine_v2",
    )
    signal_result = materialize_intraday_signal_snapshots(
        settings,
        session_date=date(2026, 3, 9),
        checkpoint="09:30",
        horizons=[1, 5],
    )
    decision_result = materialize_intraday_entry_decisions(
        settings,
        session_date=date(2026, 3, 9),
        checkpoint="09:30",
        horizons=[1, 5],
    )
    evaluation_result = evaluate_intraday_timing_layer(
        settings,
        start_session_date=date(2026, 3, 9),
        end_session_date=date(2026, 3, 9),
        horizons=[1, 5],
    )
    report_result = render_intraday_monitor_report(
        settings,
        session_date=date(2026, 3, 9),
        checkpoint="09:30",
        dry_run=True,
    )

    assert bars_result.row_count > 0
    assert trade_result.row_count > 0
    assert quote_result.row_count > 0
    assert signal_result.row_count == 4
    assert decision_result.row_count == 4
    assert evaluation_result.row_count == 4
    assert report_result.artifact_paths
    assert all(Path(path).exists() for path in report_result.artifact_paths)

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bar_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_bar_1m WHERE session_date = ?",
            [date(2026, 3, 9)],
        ).fetchone()[0]
        unavailable_quotes = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_quote_summary
            WHERE session_date = ?
              AND quote_status = 'unavailable'
            """,
            [date(2026, 3, 9)],
        ).fetchone()[0]
        timing_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_timing_outcome
            WHERE session_date = ?
            """,
            [date(2026, 3, 9)],
        ).fetchone()[0]

    assert int(bar_count) > 100
    assert int(unavailable_quotes) > 0
    assert int(timing_rows) == 4
