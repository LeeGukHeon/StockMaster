from __future__ import annotations

from datetime import date

from app.audit.alpha_phase0_repair import detect_phase0_repair_scope
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def test_detect_phase0_repair_scope_unions_overlap_and_prelisting_dates(tmp_path):
    settings = build_test_settings(tmp_path)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(
            """
            INSERT INTO dim_symbol (
                symbol, company_name, market, listing_date, is_common_stock, source, updated_at
            ) VALUES
                ('111111', 'FutureCo', 'KOSPI', DATE '2026-03-09', TRUE, 'test', now())
            """
        )
        connection.execute(
            """
            INSERT INTO fact_model_training_run (
                training_run_id, run_id, model_domain, model_version, horizon, train_end_date,
                train_row_count, validation_row_count, feature_count, fallback_flag, status, created_at,
                training_window_end, validation_window_end
            ) VALUES
                ('train-run', 'run', 'alpha', 'alpha_model_v1', 1, DATE '2026-03-06',
                 1, 1, 1, FALSE, 'success', now(), DATE '2026-03-05', DATE '2026-03-06')
            """
        )
        connection.execute(
            """
            INSERT INTO fact_prediction (
                run_id, as_of_date, symbol, horizon, market, ranking_version, prediction_version,
                expected_excess_return, created_at, training_run_id
            ) VALUES
                ('run', DATE '2026-03-06', '111111', 1, 'KOSPI', 'selection_engine_v2', 'alpha_prediction_v1',
                 0.1, now(), 'train-run')
            """
        )
        connection.execute(
            """
            INSERT INTO fact_ranking (
                run_id, as_of_date, symbol, horizon, final_selection_value, final_selection_rank_pct,
                grade, explanatory_score_json, top_reason_tags_json, risk_flags_json, eligible_flag,
                eligibility_notes_json, regime_state, ranking_version, created_at
            ) VALUES
                ('run', DATE '2026-03-06', '111111', 1, 80.0, 0.95, 'A', '{}', '[]', '[]', TRUE,
                 '[]', 'risk_on', 'selection_engine_v2', now())
            """
        )

        train_dates, prediction_dates = detect_phase0_repair_scope(
            connection,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 10),
        )

    assert train_dates == [date(2026, 3, 6)]
    assert prediction_dates == [date(2026, 3, 6)]
