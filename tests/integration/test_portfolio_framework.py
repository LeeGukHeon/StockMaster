from __future__ import annotations

from datetime import date
from pathlib import Path

from app.portfolio.allocation import (
    evaluate_portfolio_policies,
    materialize_portfolio_nav,
    run_portfolio_walkforward,
)
from app.portfolio.candidate_book import (
    build_portfolio_candidate_book,
    validate_portfolio_candidate_book,
)
from app.portfolio.policies import (
    freeze_active_portfolio_policy,
    rollback_active_portfolio_policy,
)
from app.portfolio.report import (
    publish_discord_portfolio_summary,
    render_portfolio_report,
)
from app.portfolio.validation import validate_portfolio_framework
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    TRADING_DATES,
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def _next_trading_date(selection_date: date) -> date:
    index = TRADING_DATES.index(selection_date)
    return TRADING_DATES[index + 1]


def _seed_portfolio_timing_actions(settings, selection_dates: list[date]) -> None:
    rows: list[tuple[object, ...]] = []
    for selection_date in selection_dates:
        session_date = _next_trading_date(selection_date)
        for symbol, final_action in [
            ("005930", "ENTER_NOW"),
            ("000660", "WAIT_RECHECK"),
            ("123456", "ENTER_NOW"),
            ("123457", "AVOID_TODAY"),
        ]:
            rows.append(
                (
                    "test-portfolio-meta",
                    session_date,
                    symbol,
                    5,
                    "09:30",
                    "selection_engine_v2",
                    final_action,
                    final_action,
                    final_action,
                    final_action,
                )
            )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.executemany(
            """
            INSERT INTO fact_intraday_meta_decision (
                run_id,
                session_date,
                symbol,
                horizon,
                checkpoint_time,
                ranking_version,
                raw_action,
                adjusted_action,
                tuned_action,
                final_action,
                panel_name,
                predicted_class,
                predicted_class_probability,
                confidence_margin,
                uncertainty_score,
                disagreement_score,
                active_policy_candidate_id,
                active_meta_model_id,
                active_meta_training_run_id,
                hard_guard_block_flag,
                override_applied_flag,
                override_type,
                fallback_flag,
                fallback_reason,
                decision_reason_codes_json,
                risk_flags_json,
                source_notes_json,
                created_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, FALSE, FALSE, NULL, FALSE, NULL, '[]', '[]',
                NULL, now()
            )
            """,
            rows,
        )


def test_portfolio_framework_end_to_end(tmp_path):
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
        limit_symbols=4,
    )
    for selection_date in selection_dates:
        materialize_selection_engine_v2(
            settings,
            as_of_date=selection_date,
            horizons=[1, 5],
            limit_symbols=4,
        )
    _seed_portfolio_timing_actions(settings, selection_dates)

    freeze_result = freeze_active_portfolio_policy(
        settings,
        as_of_date=min(selection_dates),
        policy_config_path="config/portfolio_policies/balanced_long_only_v1.yaml",
        note="freeze for test",
    )
    candidate_result = build_portfolio_candidate_book(
        settings,
        as_of_date=max(selection_dates),
    )
    candidate_validation = validate_portfolio_candidate_book(
        settings,
        as_of_date=max(selection_dates),
    )
    walkforward_result = run_portfolio_walkforward(
        settings,
        start_as_of_date=min(selection_dates),
        end_as_of_date=max(selection_dates),
    )
    nav_result = materialize_portfolio_nav(
        settings,
        start_date=date(2026, 3, 3),
        end_date=date(2026, 3, 9),
    )
    evaluation_result = evaluate_portfolio_policies(
        settings,
        start_date=date(2026, 3, 3),
        end_date=date(2026, 3, 9),
    )
    render_result = render_portfolio_report(
        settings,
        as_of_date=max(selection_dates),
        dry_run=True,
    )
    publish_result = publish_discord_portfolio_summary(
        settings,
        as_of_date=max(selection_dates),
        dry_run=True,
    )
    rollback_result = rollback_active_portfolio_policy(
        settings,
        as_of_date=date(2026, 3, 9),
        note="rollback for test",
    )
    framework_result = validate_portfolio_framework(
        settings,
        as_of_date=max(selection_dates),
    )

    assert freeze_result.row_count == 1
    assert candidate_result.row_count > 0
    assert candidate_validation.check_count >= 6
    assert walkforward_result.processed_dates == len(selection_dates)
    assert nav_result.row_count > 0
    assert evaluation_result.row_count > 0
    assert render_result.artifact_paths
    assert all(Path(path).exists() for path in render_result.artifact_paths)
    assert publish_result.dry_run is True
    assert publish_result.published is False
    assert rollback_result.row_count >= 0
    assert framework_result.check_count >= 10

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        candidate_count = connection.execute(
            "SELECT COUNT(*) FROM fact_portfolio_candidate"
        ).fetchone()[0]
        target_count = connection.execute(
            "SELECT COUNT(*) FROM fact_portfolio_target_book"
        ).fetchone()[0]
        rebalance_count = connection.execute(
            "SELECT COUNT(*) FROM fact_portfolio_rebalance_plan"
        ).fetchone()[0]
        position_count = connection.execute(
            "SELECT COUNT(*) FROM fact_portfolio_position_snapshot"
        ).fetchone()[0]
        nav_count = connection.execute(
            "SELECT COUNT(*) FROM fact_portfolio_nav_snapshot"
        ).fetchone()[0]
        evaluation_count = connection.execute(
            "SELECT COUNT(*) FROM fact_portfolio_evaluation_summary"
        ).fetchone()[0]

    assert int(candidate_count) > 0
    assert int(target_count) > 0
    assert int(rebalance_count) > 0
    assert int(position_count) > 0
    assert int(nav_count) > 0
    assert int(evaluation_count) > 0
