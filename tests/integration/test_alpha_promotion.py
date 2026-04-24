from __future__ import annotations

import json
from datetime import date, timedelta

import pandas as pd

from app.evaluation.alpha_shadow import upsert_alpha_shadow_selection_outcomes
from app.ml.active import freeze_alpha_active_model, rollback_alpha_active_model
from app.ml.constants import (
    ALPHA_CANDIDATE_MODEL_SPECS,
    MODEL_DOMAIN,
    MODEL_SPEC_ID,
    MODEL_VERSION,
    supports_horizon_for_spec,
)
from app.ml.promotion import run_alpha_auto_promotion
from app.ml.registry import (
    load_active_alpha_model,
    upsert_alpha_model_specs,
    upsert_model_training_runs,
)
from app.ml.shadow import upsert_alpha_shadow_predictions, upsert_alpha_shadow_rankings
from app.query_views import (
    latest_alpha_active_model_frame,
    latest_alpha_model_spec_frame,
    latest_alpha_rollback_frame,
    latest_alpha_selection_gap_scorecard_frame,
    latest_alpha_training_candidate_frame,
)
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings

SELECTION_DATES = [
    date(2026, 2, 26),
    date(2026, 2, 27),
    date(2026, 3, 2),
    date(2026, 3, 3),
    date(2026, 3, 4),
    date(2026, 3, 5),
    date(2026, 3, 6),
]
SYMBOLS = ["005930", "000660", "123456", "123457"]
SPEC_BASE_RETURNS = {
    MODEL_SPEC_ID: [0.005, -0.002, 0.003, 0.000, -0.004, 0.001, -0.003],
    "alpha_rolling_120_v1": [0.030, 0.024, 0.028, 0.021, 0.026, 0.019, 0.023],
    "alpha_rolling_250_v1": [-0.012, -0.018, -0.010, -0.022, -0.016, -0.020, -0.014],
    "alpha_rank_rolling_120_v1": [0.027, 0.022, 0.026, 0.019, 0.024, 0.018, 0.021],
    "alpha_topbucket_h1_rolling_120_v1": [0.018, 0.016, 0.019, 0.015, 0.017, 0.014, 0.016],
    "alpha_lead_d1_v1": [0.032, 0.028, 0.031, 0.026, 0.029, 0.025, 0.027],
    "alpha_swing_d5_v2": [0.020, 0.018, 0.021, 0.017, 0.019, 0.016, 0.018],
}
SPEC_BASE_ERRORS = {
    MODEL_SPEC_ID: [0.018, 0.015, 0.017, 0.014, 0.016, 0.015, 0.017],
    "alpha_rolling_120_v1": [0.004, 0.005, 0.003, 0.004, 0.005, 0.004, 0.003],
    "alpha_rolling_250_v1": [0.026, 0.024, 0.028, 0.025, 0.027, 0.026, 0.029],
    "alpha_rank_rolling_120_v1": [0.005, 0.006, 0.004, 0.005, 0.006, 0.005, 0.004],
    "alpha_topbucket_h1_rolling_120_v1": [0.007, 0.008, 0.006, 0.007, 0.008, 0.007, 0.006],
    "alpha_lead_d1_v1": [0.003, 0.004, 0.003, 0.004, 0.004, 0.003, 0.004],
    "alpha_swing_d5_v2": [0.006, 0.006, 0.005, 0.006, 0.006, 0.005, 0.006],
}
SYMBOL_RETURN_ADJUSTMENTS = [0.006, 0.002, -0.002, -0.006]
SYMBOL_ERROR_ADJUSTMENTS = [0.0015, -0.0010, 0.0005, -0.0005]


def _seed_alpha_model_registry(settings) -> None:
    created_at = pd.Timestamp("2026-03-06T09:00:00Z")
    spec_rows = [
        {
            "model_spec_id": spec.model_spec_id,
            "model_domain": MODEL_DOMAIN,
            "model_version": MODEL_VERSION,
            "estimation_scheme": spec.estimation_scheme,
            "rolling_window_days": spec.rolling_window_days,
            "feature_version": "feature_store_v1",
            "label_version": "forward_return_v1",
            "selection_engine_version": "selection_engine_v2",
            "spec_payload_json": json.dumps(
                {
                    "lifecycle_role": spec.lifecycle_role,
                    "lifecycle_fallback_flag": bool(spec.lifecycle_fallback_flag),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "active_candidate_flag": bool(spec.active_candidate_flag),
            "created_at": created_at,
            "updated_at": created_at,
        }
        for spec in ALPHA_CANDIDATE_MODEL_SPECS
    ]
    training_rows: list[dict[str, object]] = []
    for spec in ALPHA_CANDIDATE_MODEL_SPECS:
        for horizon in (1, 5):
            if not supports_horizon_for_spec(spec, horizon=horizon):
                continue
            training_rows.append(
                {
                    "training_run_id": f"seed-{spec.model_spec_id}-h{int(horizon)}",
                    "run_id": "seed-training",
                    "model_domain": MODEL_DOMAIN,
                    "model_version": MODEL_VERSION,
                    "model_spec_id": spec.model_spec_id,
                    "estimation_scheme": spec.estimation_scheme,
                    "rolling_window_days": spec.rolling_window_days,
                    "horizon": int(horizon),
                    "panel_name": "all",
                    "train_end_date": date(2026, 3, 6),
                    "training_window_start": date(2026, 2, 26),
                    "training_window_end": date(2026, 3, 5),
                    "validation_window_start": date(2026, 3, 6),
                    "validation_window_end": date(2026, 3, 6),
                    "train_row_count": 128,
                    "validation_row_count": 32,
                    "feature_count": 10,
                    "ensemble_weight_json": "{}",
                    "model_family_json": '{"members":["elasticnet","hist_gbm","extra_trees"]}',
                    "threshold_payload_json": None,
                    "diagnostic_artifact_uri": None,
                    "metadata_json": None,
                    "fallback_flag": False,
                    "fallback_reason": None,
                    "artifact_uri": f"artifacts/{spec.model_spec_id}/horizon={int(horizon)}.pkl",
                    "notes": "seed training run",
                    "status": "success",
                    "created_at": created_at,
                }
            )
    for row in training_rows:
        artifact_path = settings.paths.artifacts_dir / str(row["artifact_uri"])
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text("seed artifact", encoding="utf-8")

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        upsert_alpha_model_specs(connection, pd.DataFrame(spec_rows))
        upsert_model_training_runs(connection, pd.DataFrame(training_rows))


def _seed_shadow_outcomes(settings) -> None:
    created_at = pd.Timestamp("2026-03-10T00:00:00Z")
    rows: list[dict[str, object]] = []
    for horizon in (1, 5):
        for spec in ALPHA_CANDIDATE_MODEL_SPECS:
            if not supports_horizon_for_spec(spec, horizon=horizon):
                continue
            spec_id = spec.model_spec_id
            for date_index, selection_date in enumerate(SELECTION_DATES):
                for symbol_index, symbol in enumerate(SYMBOLS):
                    realized = (
                        SPEC_BASE_RETURNS[spec_id][date_index]
                        + SYMBOL_RETURN_ADJUSTMENTS[symbol_index]
                    )
                    error = (
                        SPEC_BASE_ERRORS[spec_id][date_index]
                        + SYMBOL_ERROR_ADJUSTMENTS[symbol_index]
                    )
                    expected = realized - error
                    rows.append(
                        {
                            "selection_date": selection_date,
                            "evaluation_date": selection_date + timedelta(days=1),
                            "symbol": symbol,
                            "market": "KOSPI" if symbol in {"005930", "000660"} else "KOSDAQ",
                            "horizon": int(horizon),
                            "model_spec_id": spec_id,
                            "training_run_id": f"seed-{spec_id}-h{int(horizon)}",
                            "selection_percentile": 1.0 - (0.25 * symbol_index),
                            "report_candidate_flag": symbol_index < 2,
                            "grade": ["A", "A", "B", "C"][symbol_index],
                            "eligible_flag": True,
                            "final_selection_value": float(100 - symbol_index),
                            "expected_excess_return_at_selection": float(expected),
                            "lower_band_at_selection": float(expected - 0.02),
                            "median_band_at_selection": float(expected),
                            "upper_band_at_selection": float(expected + 0.02),
                            "uncertainty_score_at_selection": 0.15 + (0.01 * symbol_index),
                            "disagreement_score_at_selection": 0.10 + (0.01 * symbol_index),
                            "realized_excess_return": float(realized),
                            "prediction_error": float(error),
                            "outcome_status": "matured",
                            "source_label_version": "test_label_v1",
                            "evaluation_run_id": "seed-evaluation",
                            "created_at": created_at,
                            "updated_at": created_at,
                        }
                    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        upsert_alpha_shadow_selection_outcomes(connection, pd.DataFrame(rows))


def _seed_shadow_prediction_and_ranking(
    settings,
    *,
    model_spec_id: str,
    training_run_prefix: str | None = None,
) -> None:
    created_at = pd.Timestamp("2026-03-10T00:00:00Z")
    predictions: list[dict[str, object]] = []
    rankings: list[dict[str, object]] = []
    training_prefix = training_run_prefix or f"seed-{model_spec_id}"
    for horizon in (1, 5):
        spec = next(
            candidate
            for candidate in ALPHA_CANDIDATE_MODEL_SPECS
            if candidate.model_spec_id == model_spec_id
        )
        if not supports_horizon_for_spec(spec, horizon=horizon):
            continue
        training_run_id = f"{training_prefix}-h{int(horizon)}"
        for selection_date in SELECTION_DATES:
            for symbol_index, symbol in enumerate(SYMBOLS):
                predictions.append(
                    {
                        "run_id": "seed-shadow",
                        "selection_date": selection_date,
                        "symbol": symbol,
                        "horizon": int(horizon),
                        "model_spec_id": model_spec_id,
                        "training_run_id": training_run_id,
                        "expected_excess_return": 0.01,
                        "lower_band": -0.01,
                        "median_band": 0.01,
                        "upper_band": 0.03,
                        "uncertainty_score": 0.1,
                        "disagreement_score": 0.1,
                        "fallback_flag": False,
                        "fallback_reason": None,
                        "created_at": created_at,
                    }
                )
                rankings.append(
                    {
                        "run_id": "seed-shadow",
                        "selection_date": selection_date,
                        "symbol": symbol,
                        "horizon": int(horizon),
                        "model_spec_id": model_spec_id,
                        "training_run_id": training_run_id,
                        "final_selection_value": float(100 - symbol_index),
                        "selection_percentile": 1.0 - (0.25 * symbol_index),
                        "grade": ["A", "A", "B", "C"][symbol_index],
                        "report_candidate_flag": symbol_index < 2,
                        "eligible_flag": True,
                        "created_at": created_at,
                    }
                )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        upsert_alpha_shadow_predictions(connection, pd.DataFrame(predictions))
        upsert_alpha_shadow_rankings(connection, pd.DataFrame(rankings))


def _build_promotion_settings(tmp_path):
    settings = build_test_settings(tmp_path)
    _seed_alpha_model_registry(settings)
    _seed_shadow_outcomes(settings)
    return settings


def test_run_alpha_auto_promotion_promotes_superior_challenger(tmp_path):
    settings = _build_promotion_settings(tmp_path)
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test_seed",
        note="seed incumbent",
        horizons=[1, 5],
        model_spec_id=MODEL_SPEC_ID,
        train_end_date=date(2026, 3, 6),
    )
    _seed_shadow_prediction_and_ranking(settings, model_spec_id="alpha_lead_d1_v1")
    _seed_shadow_prediction_and_ranking(settings, model_spec_id="alpha_swing_d5_v2")

    result = run_alpha_auto_promotion(
        settings,
        as_of_date=date(2026, 3, 10),
        horizons=[1, 5],
        lookback_selection_dates=len(SELECTION_DATES),
        bootstrap_reps=200,
        block_length=1,
    )

    assert result.row_count > 0
    assert result.promoted_horizon_count == 2

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        active_h1 = load_active_alpha_model(connection, as_of_date=date(2026, 3, 10), horizon=1)
        active_h5 = load_active_alpha_model(connection, as_of_date=date(2026, 3, 10), horizon=5)
        promotion_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alpha_promotion_test
            WHERE promotion_date = ?
            """,
            [date(2026, 3, 10)],
        ).fetchone()[0]
        decision_row = connection.execute(
            """
            SELECT decision, mcs_member_flag, incumbent_mcs_member_flag, sample_count
            FROM fact_alpha_promotion_test
            WHERE promotion_date = ?
              AND horizon = 1
              AND challenger_model_spec_id = 'alpha_lead_d1_v1'
              AND loss_name = 'loss_top5'
            """,
            [date(2026, 3, 10)],
        ).fetchone()

    assert active_h1 is not None
    assert active_h5 is not None
    assert active_h1["model_spec_id"] == "alpha_lead_d1_v1"
    assert active_h5["model_spec_id"] == "alpha_swing_d5_v2"
    assert active_h1["promotion_type"] == "AUTO_PROMOTION"
    assert active_h1["source_type"] == "alpha_auto_promotion"
    assert "alpha_lead_d1_v1" in active_h1["promotion_report_json"]["superior_set"]
    assert "alpha_swing_d5_v2" in active_h5["promotion_report_json"]["superior_set"]
    assert promotion_rows > 0
    assert decision_row == ("PROMOTE_CHALLENGER", True, False, len(SELECTION_DATES))


def test_run_alpha_auto_promotion_blocks_lineage_mismatched_challenger(tmp_path):
    settings = _build_promotion_settings(tmp_path)
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test_seed",
        note="seed incumbent",
        horizons=[1, 5],
        model_spec_id=MODEL_SPEC_ID,
        train_end_date=date(2026, 3, 6),
    )
    _seed_shadow_prediction_and_ranking(settings, model_spec_id="alpha_lead_d1_v1")
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(
            """
            UPDATE fact_alpha_shadow_selection_outcome
            SET training_run_id = 'stale-alpha-rolling-120'
            WHERE model_spec_id = 'alpha_lead_d1_v1'
            """
        )

    result = run_alpha_auto_promotion(
        settings,
        as_of_date=date(2026, 3, 10),
        horizons=[1],
        lookback_selection_dates=len(SELECTION_DATES),
        bootstrap_reps=200,
        block_length=1,
    )

    assert result.promoted_horizon_count == 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        active_h1 = load_active_alpha_model(connection, as_of_date=date(2026, 3, 10), horizon=1)
        blocked_row = connection.execute(
            """
            SELECT decision, detail_json
            FROM fact_alpha_promotion_test
            WHERE promotion_date = ?
              AND horizon = 1
              AND challenger_model_spec_id = 'alpha_lead_d1_v1'
              AND loss_name = 'loss_top5'
            """,
            [date(2026, 3, 10)],
        ).fetchone()

    assert active_h1 is not None
    assert active_h1["model_spec_id"] == MODEL_SPEC_ID
    assert blocked_row is not None
    assert blocked_row[0] == "NO_AUTO_PROMOTION"
    assert "shadow_validation_failed" in str(blocked_row[1])


def test_run_alpha_auto_promotion_uses_current_h5_candidate_set(tmp_path):
    settings = _build_promotion_settings(tmp_path)
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test_seed",
        note="seed incumbent",
        horizons=[5],
        model_spec_id="alpha_swing_d5_v2",
        train_end_date=date(2026, 3, 6),
    )
    _seed_shadow_prediction_and_ranking(settings, model_spec_id="alpha_swing_d5_v2")

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        active_h5_specs = {
            row[0]
            for row in connection.execute(
                """
                SELECT model_spec_id
                FROM dim_alpha_model_spec
                WHERE active_candidate_flag = TRUE
                  AND model_spec_id LIKE 'alpha_swing_d5%'
                """
            ).fetchall()
        }

    result = run_alpha_auto_promotion(
        settings,
        as_of_date=date(2026, 3, 10),
        horizons=[5],
        lookback_selection_dates=len(SELECTION_DATES),
        bootstrap_reps=200,
        block_length=1,
    )

    assert result.row_count > 0
    assert active_h5_specs == {"alpha_swing_d5_v2"}

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        challenger_ids = {
            row[0]
            for row in connection.execute(
                """
                SELECT DISTINCT challenger_model_spec_id
                FROM fact_alpha_promotion_test
                WHERE promotion_date = ?
                  AND horizon = 5
                """,
                [date(2026, 3, 10)],
            ).fetchall()
        }
        h5_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alpha_promotion_test
            WHERE promotion_date = ?
              AND horizon = 5
            """,
            [date(2026, 3, 10)],
        ).fetchone()[0]

    assert challenger_ids == {"alpha_swing_d5_v2"}
    assert int(h5_rows or 0) > 0


def test_rollback_alpha_active_model_is_noop_without_previous_and_restores_previous(tmp_path):
    settings = _build_promotion_settings(tmp_path)
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test_seed",
        note="seed incumbent",
        horizons=[1, 5],
        model_spec_id=MODEL_SPEC_ID,
        train_end_date=date(2026, 3, 6),
    )

    no_op = rollback_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 9),
        horizons=[1, 5],
        note="no prior registry row",
    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        active_h1_noop = load_active_alpha_model(connection, as_of_date=date(2026, 3, 9), horizon=1)

    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 10),
        source="test_seed",
        note="seed challenger",
        horizons=[1, 5],
        model_spec_id="alpha_rolling_120_v1",
        train_end_date=date(2026, 3, 6),
    )
    rollback = rollback_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 11),
        horizons=[1, 5],
        note="restore incumbent",
    )

    assert no_op.row_count == 0
    assert rollback.row_count == 2

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        active_h1_restored = load_active_alpha_model(
            connection,
            as_of_date=date(2026, 3, 11),
            horizon=1,
        )

    assert active_h1_noop is not None
    assert active_h1_noop["model_spec_id"] == MODEL_SPEC_ID
    assert active_h1_restored is not None
    assert active_h1_restored["model_spec_id"] == MODEL_SPEC_ID
    assert active_h1_restored["source_type"] == "rollback_restore"
    assert active_h1_restored["promotion_type"] == "ROLLBACK"
    assert active_h1_restored["rollback_of_active_alpha_model_id"] is not None


def test_alpha_ops_helper_frames_surface_registry_and_candidates(tmp_path):
    settings = _build_promotion_settings(tmp_path)
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test_seed",
        note="seed incumbent",
        horizons=[1, 5],
        model_spec_id=MODEL_SPEC_ID,
        train_end_date=date(2026, 3, 6),
    )
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 10),
        source="test_seed",
        note="seed challenger",
        horizons=[1, 5],
        model_spec_id="alpha_rolling_120_v1",
        train_end_date=date(2026, 3, 6),
    )
    rollback_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 11),
        horizons=[1, 5],
        note="restore incumbent",
    )

    active_frame = latest_alpha_active_model_frame(
        settings,
        as_of_date=date(2026, 3, 11),
        limit=10,
    )
    candidate_frame = latest_alpha_training_candidate_frame(settings, limit=10)
    spec_frame = latest_alpha_model_spec_frame(settings, limit=10)
    rollback_frame = latest_alpha_rollback_frame(settings, limit=10)

    assert not active_frame.empty
    assert not candidate_frame.empty
    assert not spec_frame.empty
    assert not rollback_frame.empty
    assert set(active_frame["model_spec_id"]) == {MODEL_SPEC_ID}
    assert set(candidate_frame["model_spec_id"]) >= {
        MODEL_SPEC_ID,
        "alpha_rolling_120_v1",
        "alpha_rolling_250_v1",
        "alpha_lead_d1_v1",
        "alpha_swing_d5_v2",
    }
    assert set(spec_frame["model_spec_id"]) == {
        "alpha_lead_d1_v1",
        "alpha_swing_d5_v2",
    }
    assert set(spec_frame["lifecycle_role"]) == {"active_candidate"}
    assert set(spec_frame["lifecycle_fallback_flag"]) == {False}
    assert set(rollback_frame["promotion_type"]) == {"ROLLBACK"}


def test_latest_alpha_selection_gap_scorecard_frame_returns_latest_rows(tmp_path):
    settings = _build_promotion_settings(tmp_path)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(
            """
            INSERT INTO fact_alpha_shadow_selection_gap_scorecard (
                summary_date, window_name, window_start, window_end, horizon, model_spec_id,
                segment_name, matured_selection_date_count, required_selection_date_count,
                insufficient_history_flag, raw_top5_source, hit_rate_formula,
                raw_top5_mean_realized_excess_return, selected_top5_mean_realized_excess_return,
                report_candidates_mean_realized_excess_return, raw_top5_hit_rate,
                selected_top5_hit_rate, report_candidates_hit_rate, top5_overlap,
                pred_only_top5_mean_realized_excess_return, sel_only_top5_mean_realized_excess_return,
                drag_vs_raw_top5, evaluation_run_id, created_at
            ) VALUES (
                ?, 'rolling_20', ?, ?, 1, 'alpha_lead_d1_v1', 'top5', 20, 20, FALSE,
                'prediction desc', 'realized_excess_return > 0', 0.02, 0.018, 0.017,
                0.55, 0.53, 0.52, 0.60, 0.021, 0.015, -0.002, 'seed-gap', now()
            )
            """,
            [date(2026, 3, 10), date(2026, 2, 10), date(2026, 3, 10)],
        )

    gap_frame = latest_alpha_selection_gap_scorecard_frame(
        settings,
        summary_date=date(2026, 3, 10),
        window_name="rolling_20",
        limit=10,
    )

    assert len(gap_frame) == 1
    assert gap_frame.iloc[0]["model_spec_id"] == "alpha_lead_d1_v1"
    assert bool(gap_frame.iloc[0]["insufficient_history_flag"]) is False
