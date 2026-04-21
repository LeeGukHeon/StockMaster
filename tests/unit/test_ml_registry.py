from __future__ import annotations

from datetime import date

import pandas as pd

from app.ml.constants import MODEL_DOMAIN, MODEL_SPEC_ID, MODEL_VERSION
from app.ml.registry import load_latest_training_run, upsert_model_training_runs
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def test_load_latest_training_run_does_not_match_null_model_spec_to_non_default(tmp_path):
    settings = build_test_settings(tmp_path)
    rows = pd.DataFrame(
        [
            {
                "training_run_id": "default-null-h1",
                "run_id": "seed",
                "model_domain": MODEL_DOMAIN,
                "model_version": MODEL_VERSION,
                "model_spec_id": None,
                "horizon": 1,
                "train_end_date": date(2026, 3, 6),
                "training_window_start": date(2026, 3, 1),
                "training_window_end": date(2026, 3, 5),
                "validation_window_start": date(2026, 3, 6),
                "validation_window_end": date(2026, 3, 6),
                "train_row_count": 10,
                "validation_row_count": 2,
                "feature_count": 5,
                "ensemble_weight_json": "{}",
                "model_family_json": "{}",
                "fallback_flag": False,
                "fallback_reason": None,
                "notes": "seed",
                "artifact_uri": "artifacts/default.pkl",
                "status": "success",
                "created_at": pd.Timestamp("2026-03-06T00:00:00Z"),
            },
            {
                "training_run_id": "lead-d1-h1",
                "run_id": "seed",
                "model_domain": MODEL_DOMAIN,
                "model_version": MODEL_VERSION,
                "model_spec_id": "alpha_lead_d1_v1",
                "horizon": 1,
                "train_end_date": date(2026, 3, 6),
                "training_window_start": date(2026, 3, 1),
                "training_window_end": date(2026, 3, 5),
                "validation_window_start": date(2026, 3, 6),
                "validation_window_end": date(2026, 3, 6),
                "train_row_count": 10,
                "validation_row_count": 2,
                "feature_count": 5,
                "ensemble_weight_json": "{}",
                "model_family_json": "{}",
                "fallback_flag": False,
                "fallback_reason": None,
                "notes": "seed",
                "artifact_uri": "artifacts/lead.pkl",
                "status": "success",
                "created_at": pd.Timestamp("2026-03-06T00:01:00Z"),
            },
        ]
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        upsert_model_training_runs(connection, rows)

        default_run = load_latest_training_run(
            connection,
            horizon=1,
            model_version=MODEL_VERSION,
            train_end_date=date(2026, 3, 6),
            model_domain=MODEL_DOMAIN,
            model_spec_id=MODEL_SPEC_ID,
        )
        lead_run = load_latest_training_run(
            connection,
            horizon=1,
            model_version=MODEL_VERSION,
            train_end_date=date(2026, 3, 6),
            model_domain=MODEL_DOMAIN,
            model_spec_id="alpha_lead_d1_v1",
        )

    assert default_run is not None
    assert lead_run is not None
    assert default_run["training_run_id"] == "default-null-h1"
    assert lead_run["training_run_id"] == "lead-d1-h1"
