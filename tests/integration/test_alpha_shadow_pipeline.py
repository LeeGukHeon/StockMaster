from __future__ import annotations

from datetime import date

from app.evaluation.alpha_shadow import (
    resolve_alpha_shadow_db_only_windows,
    materialize_alpha_shadow_evaluation_summary,
    materialize_alpha_shadow_selection_outcomes,
)
from app.features.feature_store import build_feature_store, load_feature_matrix
from app.ml.shadow import _ensure_feature_snapshot, materialize_alpha_shadow_candidates
from app.ml.training import train_alpha_candidate_models, train_alpha_model_v1
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def _prepare_shadow_settings(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings, limit_symbols=4)
    return settings


def test_shadow_ensure_feature_snapshot_rebuilds_invalid_quality_features(tmp_path, monkeypatch):
    settings = _prepare_shadow_settings(tmp_path)
    rebuild_calls: list[bool] = []
    real_build_feature_store = build_feature_store

    real_build_feature_store(
        settings,
        as_of_date=date(2026, 3, 6),
        limit_symbols=4,
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            UPDATE fact_feature_snapshot
            SET feature_value = NULL
            WHERE as_of_date = ?
              AND feature_name IN ('has_daily_ohlcv_flag', 'stale_price_flag', 'missing_key_feature_count')
            """,
            [date(2026, 3, 6)],
        )

    def _wrapped_build_feature_store(settings_arg, *, as_of_date, **kwargs):
        rebuild_calls.append(bool(kwargs.get("force")))
        return real_build_feature_store(settings_arg, as_of_date=as_of_date, **kwargs)

    monkeypatch.setattr("app.ml.shadow.build_feature_store", _wrapped_build_feature_store)

    _ensure_feature_snapshot(settings, as_of_date=date(2026, 3, 6))

    assert rebuild_calls == [True]

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        frame = load_feature_matrix(connection, as_of_date=date(2026, 3, 6))

    assert frame["has_daily_ohlcv_flag"].notna().all()
    assert frame["stale_price_flag"].notna().all()
    assert frame["missing_key_feature_count"].notna().all()


def test_materialize_alpha_shadow_candidates_and_self_backtest(tmp_path):
    settings = _prepare_shadow_settings(tmp_path)

    for train_end_date in [date(2026, 3, 4), date(2026, 3, 5), date(2026, 3, 6)]:
        train_alpha_model_v1(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        train_alpha_candidate_models(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        materialize_alpha_shadow_candidates(
            settings,
            as_of_date=train_end_date,
            horizons=[1, 5],
            limit_symbols=4,
        )

    outcome_result = materialize_alpha_shadow_selection_outcomes(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
    )
    summary_result = materialize_alpha_shadow_evaluation_summary(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        rolling_windows=[2],
    )

    assert outcome_result.row_count > 0
    assert outcome_result.matured_row_count > 0
    assert summary_result.row_count > 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row = connection.execute(
            """
            SELECT
                (SELECT COUNT(DISTINCT model_spec_id)
                 FROM fact_alpha_shadow_prediction
                 WHERE selection_date = ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_prediction
                 WHERE selection_date = ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_ranking
                 WHERE selection_date = ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_selection_outcome
                 WHERE selection_date BETWEEN ? AND ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_evaluation_summary
                 WHERE summary_date = ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_evaluation_summary
                 WHERE summary_date = ?
                   AND model_spec_id = 'alpha_rank_rolling_120_v1'
                   AND mean_point_loss IS NULL),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_evaluation_summary
                 WHERE summary_date = ?
                   AND model_spec_id = 'alpha_rank_rolling_120_v1'),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_evaluation_summary
                 WHERE summary_date = ?
                   AND model_spec_id = 'alpha_recursive_expanding_v1'
                   AND mean_point_loss IS NOT NULL)
            """,
            [
                date(2026, 3, 6),
                date(2026, 3, 6),
                date(2026, 3, 6),
                date(2026, 3, 4),
                date(2026, 3, 6),
                date(2026, 3, 6),
                date(2026, 3, 6),
                date(2026, 3, 6),
                date(2026, 3, 6),
            ],
        ).fetchone()

    assert int(row[0] or 0) == 5
    assert int(row[1] or 0) == 32
    assert int(row[2] or 0) == 32
    assert int(row[3] or 0) > 0
    assert int(row[4] or 0) > 0
    assert int(row[5] or 0) == int(row[6] or 0) > 0
    assert int(row[7] or 0) > 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        routing = connection.execute(
            """
            SELECT model_spec_id, horizon, COUNT(*) AS row_count
            FROM fact_alpha_shadow_prediction
            WHERE selection_date = ?
              AND model_spec_id IN ('alpha_rank_rolling_120_v1', 'alpha_topbucket_h1_rolling_120_v1')
            GROUP BY model_spec_id, horizon
            ORDER BY model_spec_id, horizon
            """,
            [date(2026, 3, 6)],
        ).fetchall()

    assert routing == [
        ("alpha_rank_rolling_120_v1", 5, 4),
        ("alpha_topbucket_h1_rolling_120_v1", 1, 4),
    ]


def test_resolve_alpha_shadow_db_only_windows_clips_to_candidate_and_market_range(tmp_path):
    settings = _prepare_shadow_settings(tmp_path)

    for train_end_date in [date(2026, 3, 4), date(2026, 3, 5), date(2026, 3, 6)]:
        train_alpha_model_v1(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        train_alpha_candidate_models(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        materialize_alpha_shadow_candidates(
            settings,
            as_of_date=train_end_date,
            horizons=[1, 5],
            limit_symbols=4,
        )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        h5_windows = resolve_alpha_shadow_db_only_windows(
            connection,
            requested_start_selection_date=date(2026, 3, 4),
            requested_end_selection_date=date(2026, 3, 10),
            horizons=[1, 5],
            model_spec_ids=["alpha_rank_rolling_120_v1"],
        )
        h1_windows = resolve_alpha_shadow_db_only_windows(
            connection,
            requested_start_selection_date=date(2026, 3, 4),
            requested_end_selection_date=date(2026, 3, 10),
            horizons=[1, 5],
            model_spec_ids=["alpha_topbucket_h1_rolling_120_v1"],
        )

    h5_by_horizon = {window.horizon: window for window in h5_windows}
    assert h5_by_horizon[1].candidate_min_selection_date is None
    assert h5_by_horizon[1].candidate_max_selection_date is None
    assert h5_by_horizon[1].effective_start_selection_date is None
    assert h5_by_horizon[1].effective_end_selection_date is None

    assert h5_by_horizon[5].candidate_min_selection_date == date(2026, 3, 5)
    assert h5_by_horizon[5].candidate_max_selection_date == date(2026, 3, 6)
    assert h5_by_horizon[5].effective_start_selection_date == date(2026, 3, 5)
    assert h5_by_horizon[5].effective_end_selection_date == date(2026, 3, 6)

    h1_by_horizon = {window.horizon: window for window in h1_windows}
    assert h1_by_horizon[1].candidate_min_selection_date == date(2026, 3, 4)
    assert h1_by_horizon[1].candidate_max_selection_date == date(2026, 3, 6)
    assert h1_by_horizon[1].effective_start_selection_date == date(2026, 3, 4)
    assert h1_by_horizon[1].effective_end_selection_date == date(2026, 3, 6)

    assert h1_by_horizon[5].candidate_min_selection_date is None
    assert h1_by_horizon[5].candidate_max_selection_date is None
    assert h1_by_horizon[5].effective_start_selection_date is None
    assert h1_by_horizon[5].effective_end_selection_date is None
