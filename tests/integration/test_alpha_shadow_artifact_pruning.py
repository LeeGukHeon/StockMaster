from __future__ import annotations

from datetime import date
from pathlib import Path

from app.ml.constants import MODEL_DOMAIN, MODEL_VERSION, get_alpha_model_spec
from app.ml.registry import load_latest_training_run
from app.ml.shadow import materialize_alpha_shadow_candidates
from app.ml.training import prune_training_result_artifacts, train_alpha_candidate_models
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


def test_pruned_shadow_training_artifacts_preserve_shadow_rows_and_fallback(tmp_path):
    settings = _prepare_shadow_settings(tmp_path)
    model_spec = get_alpha_model_spec("alpha_rank_rolling_120_v1")

    first_result = train_alpha_candidate_models(
        settings,
        train_end_date=date(2026, 3, 4),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        model_specs=(model_spec,),
    )
    materialize_alpha_shadow_candidates(
        settings,
        as_of_date=date(2026, 3, 4),
        horizons=[1, 5],
        limit_symbols=4,
    )

    second_result = train_alpha_candidate_models(
        settings,
        train_end_date=date(2026, 3, 5),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        model_specs=(model_spec,),
    )
    materialize_alpha_shadow_candidates(
        settings,
        as_of_date=date(2026, 3, 5),
        horizons=[1, 5],
        limit_symbols=4,
    )

    prune_result = prune_training_result_artifacts(settings, training_result=second_result)

    assert prune_result.pruned_artifact_uri_count == 2
    assert prune_result.removed_root_count == 1
    for removed_root in prune_result.removed_roots:
        assert not Path(removed_root).exists()

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        latest_run = load_latest_training_run(
            connection,
            horizon=1,
            model_version=MODEL_VERSION,
            train_end_date=date(2026, 3, 5),
            model_domain=MODEL_DOMAIN,
            model_spec_id=model_spec.model_spec_id,
        )
        pruned_row = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_model_training_run
            WHERE run_id = ?
              AND artifact_uri IS NULL
            """,
            [second_result.run_id],
        ).fetchone()
        shadow_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alpha_shadow_ranking
            WHERE selection_date = ?
              AND model_spec_id = ?
            """,
            [date(2026, 3, 5), model_spec.model_spec_id],
        ).fetchone()

    assert latest_run is not None
    assert latest_run["run_id"] == first_result.run_id
    assert int(pruned_row[0] or 0) == 2
    assert int(shadow_count[0] or 0) > 0
