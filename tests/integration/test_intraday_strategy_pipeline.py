from __future__ import annotations

from datetime import date
from pathlib import Path

from app.intraday.adjusted_decisions import materialize_intraday_adjusted_entry_decisions
from app.intraday.context import materialize_intraday_market_context_snapshots
from app.intraday.postmortem import (
    publish_discord_intraday_postmortem,
    render_intraday_postmortem_report,
)
from app.intraday.regime import materialize_intraday_regime_adjustments
from app.intraday.session import materialize_intraday_candidate_session
from app.intraday.strategy import (
    evaluate_intraday_strategy_comparison,
    materialize_intraday_decision_outcomes,
    materialize_intraday_timing_calibration,
)
from app.intraday.validation import validate_intraday_strategy_pipeline
from app.ml.training import train_alpha_model_v1
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.storage.duckdb import duckdb_connection
from app.ui.helpers import (
    latest_intraday_market_context_frame,
    latest_intraday_strategy_comparison_frame,
    latest_intraday_timing_calibration_frame,
    stock_workbench_intraday_decision_frame,
)
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def _prepare_ticket008_data(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    selection_dates = seed_ticket005_selection_history(
        settings,
        selection_dates=[date(2026, 3, 4), date(2026, 3, 5), date(2026, 3, 6)],
        limit_symbols=4,
    )
    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    for selection_date in selection_dates:
        materialize_selection_engine_v2(
            settings,
            as_of_date=selection_date,
            horizons=[1, 5],
            limit_symbols=4,
        )
        materialize_intraday_candidate_session(
            settings,
            selection_date=selection_date,
            horizons=[1, 5],
            max_candidates=2,
        )
    return settings


def test_intraday_regime_adjustment_strategy_and_postmortem_pipeline(tmp_path):
    settings = _prepare_ticket008_data(tmp_path)

    context_result = materialize_intraday_market_context_snapshots(
        settings,
        session_date=date(2026, 3, 9),
        checkpoints=["09:05", "09:15", "09:30", "10:00", "11:00"],
    )
    adjustment_result = materialize_intraday_regime_adjustments(
        settings,
        session_date=date(2026, 3, 9),
        checkpoints=["09:05", "09:15", "09:30", "10:00", "11:00"],
        horizons=[1, 5],
    )
    adjusted_decision_result = materialize_intraday_adjusted_entry_decisions(
        settings,
        session_date=date(2026, 3, 9),
        checkpoint="09:30",
        horizons=[1, 5],
    )
    outcome_result = materialize_intraday_decision_outcomes(
        settings,
        start_session_date=date(2026, 3, 5),
        end_session_date=date(2026, 3, 9),
        horizons=[1, 5],
    )
    comparison_result = evaluate_intraday_strategy_comparison(
        settings,
        start_session_date=date(2026, 3, 5),
        end_session_date=date(2026, 3, 9),
        horizons=[1, 5],
        cutoff="11:00",
    )
    calibration_result = materialize_intraday_timing_calibration(
        settings,
        start_session_date=date(2026, 3, 5),
        end_session_date=date(2026, 3, 9),
        horizons=[1, 5],
    )
    render_result = render_intraday_postmortem_report(
        settings,
        session_date=date(2026, 3, 9),
        horizons=[1, 5],
        dry_run=True,
    )
    publish_result = publish_discord_intraday_postmortem(
        settings,
        session_date=date(2026, 3, 9),
        horizons=[1, 5],
        dry_run=True,
    )
    validation_result = validate_intraday_strategy_pipeline(
        settings,
        session_date=date(2026, 3, 9),
        horizons=[1, 5],
    )

    assert context_result.row_count == 5
    assert adjustment_result.row_count > 0
    assert adjusted_decision_result.row_count > 0
    assert outcome_result.row_count > 0
    assert outcome_result.matured_row_count > 0
    assert comparison_result.row_count > 0
    assert calibration_result.row_count > 0
    assert render_result.artifact_paths
    assert publish_result.dry_run is True
    assert publish_result.published is False
    assert validation_result.check_count == 6
    assert all(Path(path).exists() for path in render_result.artifact_paths)

    market_context_frame = latest_intraday_market_context_frame(settings)
    comparison_frame = latest_intraday_strategy_comparison_frame(settings)
    calibration_frame = latest_intraday_timing_calibration_frame(settings)
    timeline_frame = stock_workbench_intraday_decision_frame(settings, symbol="005930", limit=10)

    assert not market_context_frame.empty
    assert not comparison_frame.empty
    assert not calibration_frame.empty
    assert not timeline_frame.empty

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        forbidden_transition_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_adjusted_entry_decision
            WHERE raw_action = 'DATA_INSUFFICIENT'
              AND adjusted_action = 'ENTER_NOW'
            """
        ).fetchone()[0]
        strategy_result_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_strategy_result
            WHERE session_date BETWEEN ? AND ?
            """,
            [date(2026, 3, 5), date(2026, 3, 9)],
        ).fetchone()[0]
        comparison_scope_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_strategy_comparison
            WHERE comparison_scope = 'regime_family'
              AND end_session_date = ?
            """,
            [date(2026, 3, 9)],
        ).fetchone()[0]

    assert int(forbidden_transition_count) == 0
    assert int(strategy_result_count) > 0
    assert int(comparison_scope_count) > 0
