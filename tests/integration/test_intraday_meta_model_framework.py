from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from app.intraday.adjusted_decisions import materialize_intraday_adjusted_entry_decisions
from app.intraday.context import materialize_intraday_market_context_snapshots
from app.intraday.data import (
    backfill_intraday_candidate_bars,
    backfill_intraday_candidate_quote_summary,
    backfill_intraday_candidate_trade_summary,
)
from app.intraday.decisions import materialize_intraday_entry_decisions
from app.intraday.meta_dataset import (
    build_intraday_meta_training_dataset,
    validate_intraday_meta_dataset,
)
from app.intraday.meta_inference import (
    evaluate_intraday_meta_models,
    materialize_intraday_final_actions,
    materialize_intraday_meta_predictions,
)
from app.intraday.meta_report import (
    publish_discord_intraday_meta_summary,
    render_intraday_meta_model_report,
)
from app.intraday.meta_training import (
    calibrate_intraday_meta_thresholds,
    freeze_intraday_active_meta_model,
    rollback_intraday_active_meta_model,
    run_intraday_meta_auto_promotion,
    run_intraday_meta_walkforward,
    train_intraday_meta_models,
)
from app.intraday.meta_validation import validate_intraday_meta_model_framework
from app.intraday.regime import materialize_intraday_regime_adjustments
from app.intraday.session import materialize_intraday_candidate_session
from app.intraday.signals import materialize_intraday_signal_snapshots
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)

CHECKPOINTS = ["09:05", "09:30"]


def _seed_meta_overlay_actions(settings, session_dates: list[date]) -> None:
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        for session_date in session_dates:
            for horizon in (1, 5):
                connection.execute(
                    """
                    UPDATE fact_intraday_adjusted_entry_decision
                    SET raw_action = 'WAIT_RECHECK',
                        adjusted_action = 'WAIT_RECHECK',
                        adjusted_timing_score = 58.0
                    WHERE session_date = ?
                      AND symbol = '005930'
                      AND horizon = ?
                      AND checkpoint_time = '09:05'
                    """,
                    [session_date, horizon],
                )
                connection.execute(
                    """
                    UPDATE fact_intraday_adjusted_entry_decision
                    SET raw_action = 'ENTER_NOW',
                        adjusted_action = 'ENTER_NOW',
                        adjusted_timing_score = 77.0
                    WHERE session_date = ?
                      AND symbol = '005930'
                      AND horizon = ?
                      AND checkpoint_time = '09:30'
                    """,
                    [session_date, horizon],
                )
                connection.execute(
                    """
                    UPDATE fact_intraday_adjusted_entry_decision
                    SET raw_action = 'ENTER_NOW',
                        adjusted_action = 'ENTER_NOW',
                        adjusted_timing_score = 72.0
                    WHERE session_date = ?
                      AND symbol = '000660'
                      AND horizon = ?
                      AND checkpoint_time = '09:05'
                    """,
                    [session_date, horizon],
                )
                connection.execute(
                    """
                    UPDATE fact_intraday_adjusted_entry_decision
                    SET raw_action = 'AVOID_TODAY',
                        adjusted_action = 'AVOID_TODAY',
                        adjusted_timing_score = 33.0
                    WHERE session_date = ?
                      AND symbol = '000660'
                      AND horizon = ?
                      AND checkpoint_time = '09:30'
                    """,
                    [session_date, horizon],
                )


def _prepare_ticket010_data(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    selection_dates = seed_ticket005_selection_history(
        settings,
        selection_dates=[
            date(2026, 3, 2),
            date(2026, 3, 3),
            date(2026, 3, 4),
            date(2026, 3, 5),
            date(2026, 3, 6),
        ],
        limit_symbols=2,
    )
    session_dates: list[date] = []
    for selection_date in selection_dates:
        materialize_selection_engine_v2(
            settings,
            as_of_date=selection_date,
            horizons=[1, 5],
            limit_symbols=2,
        )
        session_result = materialize_intraday_candidate_session(
            settings,
            selection_date=selection_date,
            horizons=[1, 5],
            max_candidates=2,
        )
        session_dates.append(session_result.session_date)
    for session_date in session_dates:
        backfill_intraday_candidate_bars(
            settings,
            session_date=session_date,
            horizons=[1, 5],
            ranking_version="selection_engine_v2",
        )
        backfill_intraday_candidate_trade_summary(
            settings,
            session_date=session_date,
            horizons=[1, 5],
            ranking_version="selection_engine_v2",
        )
        backfill_intraday_candidate_quote_summary(
            settings,
            session_date=session_date,
            horizons=[1, 5],
            ranking_version="selection_engine_v2",
        )
        for checkpoint in CHECKPOINTS:
            materialize_intraday_signal_snapshots(
                settings,
                session_date=session_date,
                checkpoint=checkpoint,
                horizons=[1, 5],
                ranking_version="selection_engine_v2",
            )
            materialize_intraday_entry_decisions(
                settings,
                session_date=session_date,
                checkpoint=checkpoint,
                horizons=[1, 5],
                ranking_version="selection_engine_v2",
            )
        materialize_intraday_market_context_snapshots(
            settings,
            session_date=session_date,
            checkpoints=CHECKPOINTS,
        )
        materialize_intraday_regime_adjustments(
            settings,
            session_date=session_date,
            checkpoints=CHECKPOINTS,
            horizons=[1, 5],
        )
        for checkpoint in CHECKPOINTS:
            materialize_intraday_adjusted_entry_decisions(
                settings,
                session_date=session_date,
                checkpoint=checkpoint,
                horizons=[1, 5],
            )
    _seed_meta_overlay_actions(settings, session_dates)
    return settings, session_dates


def test_intraday_meta_model_framework_end_to_end(tmp_path):
    settings, session_dates = _prepare_ticket010_data(tmp_path)

    dataset_result = build_intraday_meta_training_dataset(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
    )
    dataset_validation = validate_intraday_meta_dataset(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
    )
    training_result = train_intraday_meta_models(
        settings,
        train_end_date=max(session_dates),
        horizons=[1, 5],
        start_session_date=min(session_dates),
        validation_sessions=1,
    )
    walkforward_result = run_intraday_meta_walkforward(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        mode="rolling",
        train_sessions=3,
        validation_sessions=1,
        test_sessions=1,
        step_sessions=1,
        horizons=[1, 5],
    )
    threshold_result = calibrate_intraday_meta_thresholds(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
    )
    freeze_result = freeze_intraday_active_meta_model(
        settings,
        as_of_date=max(session_dates),
        source="latest_training",
        note="freeze for scoring",
        horizons=[1, 5],
    )
    prediction_result = materialize_intraday_meta_predictions(
        settings,
        session_date=max(session_dates),
        horizons=[1, 5],
    )
    decision_result = materialize_intraday_final_actions(
        settings,
        session_date=max(session_dates),
        horizons=[1, 5],
    )
    evaluation_result = evaluate_intraday_meta_models(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
    )
    rollback_result = rollback_intraday_active_meta_model(
        settings,
        as_of_date=date(2026, 3, 10),
        horizons=[1, 5],
        note="rollback check",
    )
    render_result = render_intraday_meta_model_report(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
        dry_run=True,
    )
    publish_result = publish_discord_intraday_meta_summary(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
        dry_run=True,
    )
    validation_result = validate_intraday_meta_model_framework(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
    )

    assert dataset_result.row_count > 0
    assert dataset_validation.row_count >= 3
    assert training_result.training_run_count >= 4
    assert walkforward_result.training_run_count >= 4
    assert threshold_result.row_count >= 1
    assert freeze_result.row_count >= 2
    assert prediction_result.row_count > 0
    assert decision_result.row_count > 0
    assert evaluation_result.row_count > 0
    assert rollback_result.row_count >= 0
    assert render_result.artifact_paths
    assert all(Path(path).exists() for path in render_result.artifact_paths)
    assert publish_result.dry_run is True
    assert publish_result.published is False
    assert validation_result.check_count >= 6

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        training_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_model_training_run
            WHERE model_domain = 'intraday_meta'
            """
        ).fetchone()[0]
        prediction_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_meta_prediction"
        ).fetchone()[0]
        decision_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_meta_decision"
        ).fetchone()[0]
        active_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_active_meta_model"
        ).fetchone()[0]

    assert int(training_count) > 0
    assert int(prediction_count) > 0
    assert int(decision_count) > 0
    assert int(active_count) > 0


def test_run_intraday_meta_auto_promotion_promotes_latest_oos_candidate(tmp_path, monkeypatch):
    settings = build_test_settings(tmp_path)
    created_at = pd.Timestamp("2026-03-15T00:00:00Z")

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            INSERT INTO fact_model_training_run (
                training_run_id,
                run_id,
                model_domain,
                model_version,
                model_spec_id,
                estimation_scheme,
                rolling_window_days,
                horizon,
                panel_name,
                train_end_date,
                training_window_start,
                training_window_end,
                validation_window_start,
                validation_window_end,
                train_row_count,
                validation_row_count,
                train_session_count,
                validation_session_count,
                feature_count,
                ensemble_weight_json,
                model_family_json,
                threshold_payload_json,
                diagnostic_artifact_uri,
                metadata_json,
                fallback_flag,
                fallback_reason,
                artifact_uri,
                notes,
                status,
                created_at
            )
            VALUES (
                'meta-train-h1-enter',
                'meta-run-1',
                'intraday_meta',
                'intraday_meta_v1',
                NULL,
                'meta_ensemble',
                NULL,
                1,
                'ENTER_PANEL',
                DATE '2026-03-13',
                DATE '2026-03-01',
                DATE '2026-03-13',
                DATE '2026-03-10',
                DATE '2026-03-13',
                100,
                20,
                10,
                4,
                24,
                '{}',
                '{}',
                '{}',
                NULL,
                '{}',
                FALSE,
                NULL,
                NULL,
                'latest candidate',
                'success',
                ?
            )
            """,
            [created_at],
        )
        connection.execute(
            """
            INSERT INTO fact_model_metric_summary (
                training_run_id,
                model_domain,
                model_version,
                horizon,
                panel_name,
                member_name,
                split_name,
                metric_scope,
                class_label,
                comparison_key,
                metric_name,
                metric_value,
                sample_count,
                created_at
            )
            VALUES (
                'meta-train-h1-enter',
                'intraday_meta',
                'intraday_meta_v1',
                1,
                'ENTER_PANEL',
                'ensemble',
                'test',
                'all',
                'all',
                'all',
                'macro_f1',
                0.71,
                20,
                ?
            )
            """,
            [created_at],
        )
        connection.execute(
            """
            INSERT INTO fact_alpha_active_model (
                active_alpha_model_id,
                horizon,
                model_spec_id,
                training_run_id,
                model_version,
                source_type,
                promotion_type,
                promotion_report_json,
                effective_from_date,
                effective_to_date,
                active_flag,
                rollback_of_active_alpha_model_id,
                note,
                created_at,
                updated_at
            )
            VALUES (
                'alpha-active-h1',
                1,
                'alpha_recursive_expanding_v1',
                'alpha-train-h1',
                'alpha_model_v1',
                'alpha_auto_promotion',
                'AUTO_PROMOTION',
                NULL,
                DATE '2026-03-09',
                NULL,
                TRUE,
                NULL,
                'stable alpha',
                ?,
                ?
            )
            """,
            [created_at, created_at],
        )

    monkeypatch.setattr(
        "app.intraday.meta_training.resolve_alpha_lineage_status",
        lambda *a, **k: SimpleNamespace(
            lineage_by_horizon={1: "alpha-active-h1"},
            blocked_horizons=[],
            detail_by_horizon={1: {"blocked": False}},
        ),
    )

    result = run_intraday_meta_auto_promotion(
        settings,
        as_of_date=date(2026, 3, 13),
        source="daily_overlay_meta_auto_promotion",
        note="auto promote meta",
        horizons=[1],
    )

    assert result.row_count == 1

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        active_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_active_meta_model
            WHERE active_flag = TRUE
            """
        ).fetchone()[0]

    assert active_rows == 1
