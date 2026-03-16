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
from app.intraday.policy import (
    apply_active_intraday_policy_frame,
    evaluate_intraday_policy_ablation,
    freeze_intraday_active_policy,
    materialize_intraday_policy_candidates,
    materialize_intraday_policy_recommendations,
    rollback_intraday_active_policy,
    run_intraday_policy_auto_promotion,
    run_intraday_policy_calibration,
    run_intraday_policy_walkforward,
    upsert_intraday_policy_candidate,
    upsert_intraday_policy_evaluation,
)
from app.intraday.policy_report import (
    publish_discord_intraday_policy_summary,
    render_intraday_policy_research_report,
)
from app.intraday.policy_validation import validate_intraday_policy_framework
from app.intraday.regime import materialize_intraday_regime_adjustments
from app.intraday.session import materialize_intraday_candidate_session
from app.intraday.signals import materialize_intraday_signal_snapshots
from app.intraday.strategy import (
    evaluate_intraday_strategy_comparison,
    materialize_intraday_decision_outcomes,
    materialize_intraday_timing_calibration,
)
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.storage.duckdb import duckdb_connection
from app.ui.helpers import (
    intraday_console_tuned_action_frame,
    latest_intraday_active_policy_frame,
    latest_intraday_policy_ablation_frame,
    latest_intraday_policy_evaluation_frame,
    latest_intraday_policy_experiment_frame,
    latest_intraday_policy_recommendation_frame,
)
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)

CHECKPOINTS = ["09:05", "09:30"]


def _prepare_ticket009_data(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    selection_dates = seed_ticket005_selection_history(
        settings,
        selection_dates=[date(2026, 3, 5), date(2026, 3, 6)],
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
            max_candidates=1,
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

    materialize_intraday_decision_outcomes(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
    )
    evaluate_intraday_strategy_comparison(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
        cutoff="11:00",
    )
    materialize_intraday_timing_calibration(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
    )
    return settings, session_dates


def test_intraday_policy_framework_end_to_end(tmp_path):
    settings, session_dates = _prepare_ticket009_data(tmp_path)

    candidate_result = materialize_intraday_policy_candidates(
        settings,
        search_space_version="pcal_v1",
        horizons=[1, 5],
        checkpoints=CHECKPOINTS,
        scopes=[
            "GLOBAL",
            "HORIZON",
            "HORIZON_CHECKPOINT",
            "HORIZON_REGIME_CLUSTER",
            "HORIZON_CHECKPOINT_REGIME_FAMILY",
        ],
    )
    calibration_result = run_intraday_policy_calibration(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
        checkpoints=CHECKPOINTS,
        objective_version="ip_obj_v1",
        split_version="wf_40_10_10_step5",
        search_space_version="pcal_v1",
    )
    walkforward_result = run_intraday_policy_walkforward(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        mode="rolling",
        train_sessions=40,
        validation_sessions=10,
        test_sessions=10,
        step_sessions=5,
        horizons=[1, 5],
        checkpoints=CHECKPOINTS,
    )
    ablation_result = evaluate_intraday_policy_ablation(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
        base_policy_source="latest_recommendation",
    )
    recommendation_result = materialize_intraday_policy_recommendations(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
        minimum_test_sessions=10,
    )
    freeze_result = freeze_intraday_active_policy(
        settings,
        as_of_date=max(session_dates),
        promotion_type="MANUAL_FREEZE",
        source="latest_recommendation",
        note="Promote after review",
    )
    tuned_frame = apply_active_intraday_policy_frame(
        settings,
        session_date=max(session_dates),
        horizons=[1, 5],
        limit=20,
    )
    rollback_result = rollback_intraday_active_policy(
        settings,
        as_of_date=date(2026, 3, 10),
        horizons=[1, 5],
        note="Rollback due to weak execution stability",
    )
    render_result = render_intraday_policy_research_report(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
        dry_run=True,
    )
    publish_result = publish_discord_intraday_policy_summary(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
        dry_run=True,
    )
    validation_result = validate_intraday_policy_framework(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
    )

    assert candidate_result.row_count > 0
    assert calibration_result.experiment_row_count > 0
    assert calibration_result.evaluation_row_count > 0
    assert walkforward_result.experiment_row_count > 0
    assert walkforward_result.evaluation_row_count > 0
    assert ablation_result.row_count > 0
    assert recommendation_result.row_count > 0
    assert freeze_result.row_count > 0
    assert not tuned_frame.empty
    assert rollback_result.row_count >= 0
    assert render_result.artifact_paths
    assert all(Path(path).exists() for path in render_result.artifact_paths)
    assert publish_result.dry_run is True
    assert publish_result.published is False
    assert validation_result.check_count >= 5

    experiment_frame = latest_intraday_policy_experiment_frame(settings, limit=10)
    evaluation_frame = latest_intraday_policy_evaluation_frame(
        settings,
        split_name="test",
        limit=10,
    )
    ablation_frame = latest_intraday_policy_ablation_frame(settings, limit=10)
    recommendation_frame = latest_intraday_policy_recommendation_frame(settings, limit=10)
    active_frame = latest_intraday_active_policy_frame(
        settings,
        as_of_date=max(session_dates),
        limit=10,
    )
    tuned_helper_frame = intraday_console_tuned_action_frame(
        settings,
        session_date=max(session_dates),
        limit=10,
    )

    assert not experiment_frame.empty
    assert not evaluation_frame.empty
    assert not ablation_frame.empty
    assert not recommendation_frame.empty
    assert not active_frame.empty
    assert not tuned_helper_frame.empty

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        candidate_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_policy_candidate"
        ).fetchone()[0]
        experiment_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_policy_experiment_run"
        ).fetchone()[0]
        evaluation_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_policy_evaluation"
        ).fetchone()[0]
        recommendation_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_policy_selection_recommendation"
        ).fetchone()[0]
        active_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_active_policy"
        ).fetchone()[0]

    assert int(candidate_count) > 0
    assert int(experiment_count) > 0
    assert int(evaluation_count) > 0
    assert int(recommendation_count) > 0
    assert int(active_count) > 0


def test_policy_recommendation_fallback_rows_remain_manual_review(tmp_path):
    settings = build_test_settings(tmp_path)
    created_at = pd.Timestamp("2026-03-15T00:00:00Z")
    evaluation_rows = []
    for split_index in range(3):
        evaluation_rows.append(
            {
                "experiment_run_id": f"policy-eval-{split_index}",
                "experiment_type": "WALKFORWARD",
                "search_space_version": "pcal_v1",
                "objective_version": "ip_obj_v1",
                "split_version": "wf_40_10_10_step5",
                "split_mode": "ROLLING",
                "split_name": "all",
                "split_index": split_index,
                "window_start_date": date(2026, 3, 4),
                "window_end_date": date(2026, 3, 13),
                "horizon": 1,
                "policy_candidate_id": "candidate-a",
                "template_id": "BASE_DEFAULT",
                "scope_type": "GLOBAL",
                "scope_key": "H1|GLOBAL",
                "checkpoint_time": None,
                "regime_cluster": None,
                "regime_family": None,
                "window_session_count": 4,
                "sample_count": 120,
                "matured_count": 120,
                "executed_count": 0,
                "no_entry_count": 120,
                "execution_rate": 0.0,
                "mean_realized_excess_return": -0.006131,
                "median_realized_excess_return": -0.005200,
                "hit_rate": 0.25,
                "mean_timing_edge_vs_open_bps": -88.558848,
                "positive_timing_edge_rate": 0.0,
                "skip_saved_loss_rate": 0.391667,
                "missed_winner_rate": 0.591667,
                "left_tail_proxy": -0.016167,
                "stability_score": 57.4039,
                "objective_score": -69.55994708928395,
                "manual_review_required_flag": True,
                "fallback_scope_type": None,
                "fallback_scope_key": None,
                "notes_json": "{}",
                "created_at": created_at + pd.Timedelta(minutes=split_index),
            }
        )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        upsert_intraday_policy_evaluation(connection, pd.DataFrame(evaluation_rows))

    result = materialize_intraday_policy_recommendations(
        settings,
        as_of_date=date(2026, 3, 13),
        horizons=[1],
        minimum_test_sessions=10,
    )

    assert result.row_count == 1

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        recommendation_row = connection.execute(
            """
            SELECT
                objective_score,
                manual_review_required_flag,
                test_session_count,
                recommendation_reason_json
            FROM fact_intraday_policy_selection_recommendation
            WHERE recommendation_date = ?
            """,
            [date(2026, 3, 13)],
        ).fetchone()

    assert recommendation_row is not None
    assert recommendation_row[0] is None
    assert recommendation_row[1] is True
    assert recommendation_row[2] == 0
    assert '"score_source_split": "all"' in str(recommendation_row[3])


def test_freeze_intraday_active_policy_skips_auto_activation_when_manual_review_is_required(
    tmp_path,
):
    settings = build_test_settings(tmp_path)
    created_at = pd.Timestamp("2026-03-15T00:00:00Z")

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        upsert_intraday_policy_candidate(
            connection,
            pd.DataFrame(
                [
                    {
                        "policy_candidate_id": "candidate-a",
                        "search_space_version": "pcal_v1",
                        "template_id": "BASE_DEFAULT",
                        "scope_type": "GLOBAL",
                        "scope_key": "H1|GLOBAL",
                        "horizon": 1,
                        "checkpoint_time": None,
                        "regime_cluster": None,
                        "regime_family": None,
                        "candidate_label": "candidate-a",
                        "parameter_hash": "hash-a",
                        "enter_threshold_delta": 0.0,
                        "wait_threshold_delta": 0.0,
                        "avoid_threshold_delta": 0.0,
                        "min_selection_confidence_gate": 55.0,
                        "min_signal_quality_gate": 50.0,
                        "uncertainty_penalty_weight": 0.55,
                        "spread_penalty_weight": 0.40,
                        "friction_penalty_weight": 0.50,
                        "gap_chase_penalty_weight": 0.45,
                        "cohort_weakness_penalty_weight": 0.45,
                        "market_shock_penalty_weight": 0.55,
                        "data_weak_guard_strength": 0.70,
                        "max_gap_up_allowance_pct": 4.5,
                        "min_execution_strength_gate": 48.0,
                        "min_orderbook_imbalance_gate": 0.47,
                        "allow_enter_under_data_weak": False,
                        "allow_wait_override": False,
                        "selection_rank_cap": 30,
                        "created_at": created_at,
                    }
                ]
            ),
        )
        connection.execute(
            """
            INSERT INTO fact_intraday_policy_selection_recommendation (
                recommendation_date,
                horizon,
                scope_type,
                scope_key,
                recommendation_rank,
                policy_candidate_id,
                template_id,
                source_experiment_run_id,
                search_space_version,
                objective_version,
                split_version,
                sample_count,
                executed_count,
                test_session_count,
                execution_rate,
                mean_realized_excess_return,
                median_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                positive_timing_edge_rate,
                skip_saved_loss_rate,
                missed_winner_rate,
                left_tail_proxy,
                stability_score,
                objective_score,
                manual_review_required_flag,
                fallback_scope_type,
                fallback_scope_key,
                recommendation_reason_json,
                created_at
            )
            VALUES (
                ?, 1, 'GLOBAL', 'H1|GLOBAL', 1, 'candidate-a',
                'BASE_DEFAULT', 'experiment-a', 'pcal_v1', 'ip_obj_v1', 'wf_40_10_10_step5',
                120, 0, 0, 0.0,
                NULL, NULL, 0.25, -88.0, 0.0, 0.39, 0.59, -0.01, 57.0, NULL,
                TRUE, NULL, NULL, '{}', ?
            )
            """,
            [
                date(2026, 3, 13),
                created_at,
            ],
        )

    freeze_result = freeze_intraday_active_policy(
        settings,
        as_of_date=date(2026, 3, 13),
        promotion_type="AUTO_PROMOTION",
        source="weekly_calibration_auto_activation",
        note="automatic activation",
        allow_manual_review=False,
    )

    assert freeze_result.row_count == 0
    assert "manual review" in freeze_result.notes.lower()

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        active_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_active_policy"
        ).fetchone()[0]

    assert active_count == 0


def test_run_intraday_policy_auto_promotion_blocks_during_alpha_stabilization(tmp_path):
    settings = build_test_settings(tmp_path)
    created_at = pd.Timestamp("2026-03-15T00:00:00Z")

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        upsert_intraday_policy_candidate(
            connection,
            pd.DataFrame(
                [
                    {
                        "policy_candidate_id": "candidate-a",
                        "search_space_version": "pcal_v1",
                        "template_id": "BASE_DEFAULT",
                        "scope_type": "GLOBAL",
                        "scope_key": "H1|GLOBAL",
                        "horizon": 1,
                        "checkpoint_time": None,
                        "regime_cluster": None,
                        "regime_family": None,
                        "candidate_label": "candidate-a",
                        "parameter_hash": "hash-a",
                        "enter_threshold_delta": 0.0,
                        "wait_threshold_delta": 0.0,
                        "avoid_threshold_delta": 0.0,
                        "min_selection_confidence_gate": 55.0,
                        "min_signal_quality_gate": 50.0,
                        "uncertainty_penalty_weight": 0.55,
                        "spread_penalty_weight": 0.40,
                        "friction_penalty_weight": 0.50,
                        "gap_chase_penalty_weight": 0.45,
                        "cohort_weakness_penalty_weight": 0.45,
                        "market_shock_penalty_weight": 0.55,
                        "data_weak_guard_strength": 0.70,
                        "max_gap_up_allowance_pct": 4.5,
                        "min_execution_strength_gate": 48.0,
                        "min_orderbook_imbalance_gate": 0.47,
                        "allow_enter_under_data_weak": False,
                        "allow_wait_override": False,
                        "selection_rank_cap": 30,
                        "created_at": created_at,
                    }
                ]
            ),
        )
        connection.execute(
            """
            INSERT INTO fact_intraday_policy_selection_recommendation (
                recommendation_date,
                horizon,
                scope_type,
                scope_key,
                recommendation_rank,
                policy_candidate_id,
                template_id,
                source_experiment_run_id,
                search_space_version,
                objective_version,
                split_version,
                sample_count,
                executed_count,
                test_session_count,
                execution_rate,
                mean_realized_excess_return,
                median_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                positive_timing_edge_rate,
                skip_saved_loss_rate,
                missed_winner_rate,
                left_tail_proxy,
                stability_score,
                objective_score,
                manual_review_required_flag,
                fallback_scope_type,
                fallback_scope_key,
                recommendation_reason_json,
                created_at
            )
            VALUES (
                ?, 1, 'GLOBAL', 'H1|GLOBAL', 1, 'candidate-a',
                'BASE_DEFAULT', 'experiment-a', 'pcal_v1', 'ip_obj_v1', 'wf_40_10_10_step5',
                120, 20, 12, 0.65,
                0.01, 0.01, 0.55, 15.0, 0.60, 0.35, 0.25, -0.01, 57.0, 12.5,
                FALSE, NULL, NULL, '{}', ?
            )
            """,
            [date(2026, 3, 13), created_at],
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
                ?,
                NULL,
                TRUE,
                NULL,
                'recent promotion',
                ?,
                ?
            )
            """,
            [date(2026, 3, 13), created_at, created_at],
        )

    result = run_intraday_policy_auto_promotion(
        settings,
        as_of_date=date(2026, 3, 13),
        source="daily_overlay_auto_promotion",
        note="auto promotion",
    )

    assert result.row_count == 0
    assert "stabilization" in result.notes.lower()


def test_run_intraday_policy_auto_promotion_freezes_when_alpha_is_stable(tmp_path, monkeypatch):
    settings = build_test_settings(tmp_path)
    created_at = pd.Timestamp("2026-03-15T00:00:00Z")

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        upsert_intraday_policy_candidate(
            connection,
            pd.DataFrame(
                [
                    {
                        "policy_candidate_id": "candidate-a",
                        "search_space_version": "pcal_v1",
                        "template_id": "BASE_DEFAULT",
                        "scope_type": "GLOBAL",
                        "scope_key": "H1|GLOBAL",
                        "horizon": 1,
                        "checkpoint_time": None,
                        "regime_cluster": None,
                        "regime_family": None,
                        "candidate_label": "candidate-a",
                        "parameter_hash": "hash-a",
                        "enter_threshold_delta": 0.0,
                        "wait_threshold_delta": 0.0,
                        "avoid_threshold_delta": 0.0,
                        "min_selection_confidence_gate": 55.0,
                        "min_signal_quality_gate": 50.0,
                        "uncertainty_penalty_weight": 0.55,
                        "spread_penalty_weight": 0.40,
                        "friction_penalty_weight": 0.50,
                        "gap_chase_penalty_weight": 0.45,
                        "cohort_weakness_penalty_weight": 0.45,
                        "market_shock_penalty_weight": 0.55,
                        "data_weak_guard_strength": 0.70,
                        "max_gap_up_allowance_pct": 4.5,
                        "min_execution_strength_gate": 48.0,
                        "min_orderbook_imbalance_gate": 0.47,
                        "allow_enter_under_data_weak": False,
                        "allow_wait_override": False,
                        "selection_rank_cap": 30,
                        "created_at": created_at,
                    }
                ]
            ),
        )
        connection.execute(
            """
            INSERT INTO fact_intraday_policy_selection_recommendation (
                recommendation_date,
                horizon,
                scope_type,
                scope_key,
                recommendation_rank,
                policy_candidate_id,
                template_id,
                source_experiment_run_id,
                search_space_version,
                objective_version,
                split_version,
                sample_count,
                executed_count,
                test_session_count,
                execution_rate,
                mean_realized_excess_return,
                median_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                positive_timing_edge_rate,
                skip_saved_loss_rate,
                missed_winner_rate,
                left_tail_proxy,
                stability_score,
                objective_score,
                manual_review_required_flag,
                fallback_scope_type,
                fallback_scope_key,
                recommendation_reason_json,
                created_at
            )
            VALUES (
                ?, 1, 'GLOBAL', 'H1|GLOBAL', 1, 'candidate-a',
                'BASE_DEFAULT', 'experiment-a', 'pcal_v1', 'ip_obj_v1', 'wf_40_10_10_step5',
                120, 20, 12, 0.65,
                0.01, 0.01, 0.55, 15.0, 0.60, 0.35, 0.25, -0.01, 57.0, 12.5,
                FALSE, NULL, NULL, '{}', ?
            )
            """,
            [date(2026, 3, 13), created_at],
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
                ?,
                NULL,
                TRUE,
                NULL,
                'stable promotion',
                ?,
                ?
            )
            """,
            [date(2026, 3, 9), created_at, created_at],
        )

    monkeypatch.setattr(
        "app.intraday.policy.resolve_alpha_lineage_status",
        lambda *a, **k: SimpleNamespace(
            lineage_by_horizon={1: "alpha-active-h1"},
            blocked_horizons=[],
            detail_by_horizon={1: {"blocked": False}},
        ),
    )

    result = run_intraday_policy_auto_promotion(
        settings,
        as_of_date=date(2026, 3, 13),
        source="daily_overlay_auto_promotion",
        note="auto promotion",
    )

    assert result.row_count == 1

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        active_count = connection.execute(
            "SELECT COUNT(*) FROM fact_intraday_active_policy WHERE active_flag = TRUE"
        ).fetchone()[0]

    assert active_count == 1
