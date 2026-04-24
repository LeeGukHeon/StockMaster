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
    load_training_run_by_id,
    upsert_alpha_model_specs,
    upsert_model_training_runs,
    write_model_artifact,
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
CHECKPOINT_SELECTION_DATES = [
    date(2026, 2, 23),
    date(2026, 2, 24),
    date(2026, 2, 25),
    date(2026, 2, 26),
    date(2026, 2, 27),
    date(2026, 3, 2),
    date(2026, 3, 3),
    date(2026, 3, 4),
    date(2026, 3, 5),
    date(2026, 3, 6),
]
CHECKPOINT_SYMBOLS = ["005930", "000660", "051910", "035420", "068270", "247540"]


class PredictFeatureModel:
    def __init__(self, feature_name: str, *, scale: float = 1.0) -> None:
        self.feature_name = feature_name
        self.scale = float(scale)

    def predict(self, X):  # pragma: no cover - exercised through pickle + inference
        values = pd.to_numeric(X[self.feature_name], errors="coerce").fillna(0.0)
        return values.to_numpy(dtype="float64", copy=False) * self.scale


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


def _seed_h5_checkpoint_challenge_data(settings) -> dict[str, str]:
    created_at = pd.Timestamp("2026-03-10T00:00:00Z")
    candidate_signal = [0.90, 0.72, 0.54, 0.36, 0.18, -0.04]
    champion_signal = list(reversed(candidate_signal))
    realized_base = [0.060, 0.048, 0.032, 0.014, -0.008, -0.026]

    symbol_rows = [
        {
            "symbol": symbol,
            "company_name": f"Test {symbol}",
            "market": "KOSPI" if index < 3 else "KOSDAQ",
            "sector": "테스트",
            "industry": "테스트산업",
            "listing_date": date(2020, 1, 1),
            "is_common_stock": True,
            "is_etf": False,
            "is_etn": False,
            "is_spac": False,
            "is_delisted": False,
            "source": "test",
            "as_of_date": CHECKPOINT_SELECTION_DATES[-1],
            "updated_at": created_at,
        }
        for index, symbol in enumerate(CHECKPOINT_SYMBOLS)
    ]

    market_regime_rows = []
    feature_rows = []
    label_rows = []
    neutral_rank_features = {
        "ret_5d": 0.55,
        "ret_20d": 0.55,
        "ma20_over_ma60": 0.55,
        "drawdown_20d": 0.45,
        "volume_ratio_1d_vs_20d": 0.50,
        "turnover_z_5_20": 0.50,
        "turnover_value_1d": 0.50,
        "roe_latest": 0.55,
        "operating_margin_latest": 0.55,
        "net_income_positive_flag": 0.60,
        "days_since_latest_report": 0.45,
        "debt_ratio_latest": 0.45,
        "low_debt_preference_proxy": 0.55,
        "profitability_support_proxy": 0.55,
        "earnings_yield_proxy": 0.55,
        "news_count_1d": 0.45,
        "news_count_3d": 0.45,
        "latest_news_age_hours": 0.45,
        "distinct_publishers_3d": 0.45,
        "positive_catalyst_count_3d": 0.45,
        "negative_catalyst_count_3d": 0.20,
        "foreign_net_value_ratio_5d": 0.55,
        "institution_net_value_ratio_5d": 0.55,
        "flow_alignment_score": 0.55,
        "flow_coverage_flag": 1.0,
        "smart_money_flow_ratio_20d": 0.55,
        "individual_net_value_ratio_5d": 0.45,
        "realized_vol_20d": 0.45,
        "gap_abs_avg_20d": 0.45,
    }
    neutral_value_features = {
        "has_daily_ohlcv_flag": 1.0,
        "stale_price_flag": 0.0,
        "adv_20": 150_000_000.0,
        "missing_key_feature_count": 0.0,
        "data_confidence_score": 0.95,
        "ret_5d": 0.03,
        "ret_20d": 0.08,
        "ma20_over_ma60": 1.02,
        "drawdown_20d": -0.04,
        "volume_ratio_1d_vs_20d": 1.05,
        "turnover_z_5_20": 0.10,
        "turnover_value_1d": 5_000_000_000.0,
        "roe_latest": 0.10,
        "operating_margin_latest": 0.12,
        "net_income_positive_flag": 1.0,
        "days_since_latest_report": 12.0,
        "debt_ratio_latest": 0.40,
        "low_debt_preference_proxy": 0.55,
        "profitability_support_proxy": 0.58,
        "earnings_yield_proxy": 0.05,
        "news_count_1d": 1.0,
        "news_count_3d": 2.0,
        "latest_news_age_hours": 8.0,
        "distinct_publishers_3d": 2.0,
        "positive_catalyst_count_3d": 1.0,
        "negative_catalyst_count_3d": 0.0,
        "foreign_net_value_ratio_5d": 0.02,
        "institution_net_value_ratio_5d": 0.02,
        "flow_alignment_score": 0.60,
        "flow_coverage_flag": 1.0,
        "smart_money_flow_ratio_20d": 0.03,
        "individual_net_value_ratio_5d": -0.01,
        "realized_vol_20d": 0.03,
        "gap_abs_avg_20d": 0.01,
    }

    for selection_index, selection_date in enumerate(CHECKPOINT_SELECTION_DATES):
        for market_scope in ("KR_ALL", "KOSPI", "KOSDAQ"):
            market_regime_rows.append(
                    {
                        "run_id": "seed-checkpoint-regime",
                        "as_of_date": selection_date,
                        "market_scope": market_scope,
                        "breadth_up_ratio": 0.55,
                        "breadth_down_ratio": 0.45,
                        "median_symbol_return_1d": 0.004,
                        "median_symbol_return_5d": 0.012,
                        "market_realized_vol_20d": 0.018,
                        "turnover_burst_z": 0.1,
                        "new_high_ratio_20d": 0.20,
                    "new_low_ratio_20d": 0.08,
                    "regime_state": "neutral",
                    "regime_score": 55.0,
                    "notes_json": "{}",
                    "created_at": created_at,
                }
            )

        for symbol_index, symbol in enumerate(CHECKPOINT_SYMBOLS):
            market = "KOSPI" if symbol_index < 3 else "KOSDAQ"
            realized = realized_base[symbol_index] + (selection_index % 3) * 0.001
            feature_values = {
                **neutral_value_features,
                "alpha_candidate_signal": candidate_signal[symbol_index] + (selection_index * 0.001),
                "alpha_champion_signal": champion_signal[symbol_index] - (selection_index * 0.001),
                "adv_20": neutral_value_features["adv_20"] + (symbol_index * 1_000_000),
            }
            feature_ranks = {
                **{name: 0.50 for name in feature_values},
                **neutral_rank_features,
            }
            feature_ranks["adv_20"] = 0.80
            feature_ranks["alpha_candidate_signal"] = 0.50
            feature_ranks["alpha_champion_signal"] = 0.50
            for feature_name, feature_value in feature_values.items():
                feature_rows.append(
                    {
                        "run_id": "seed-checkpoint-features",
                        "as_of_date": selection_date,
                        "symbol": symbol,
                        "feature_name": feature_name,
                        "feature_value": float(feature_value),
                        "feature_group": "test",
                        "source_version": "test",
                        "feature_rank_pct": float(feature_ranks.get(feature_name, 0.50)),
                        "feature_zscore": 0.0,
                        "is_imputed": False,
                        "notes_json": None,
                        "created_at": created_at,
                    }
                )
            label_rows.append(
                {
                    "run_id": "seed-checkpoint-labels",
                    "as_of_date": selection_date,
                    "symbol": symbol,
                    "horizon": 5,
                    "market": market,
                    "entry_date": selection_date,
                    "exit_date": selection_date + timedelta(days=7),
                    "entry_basis": "close",
                    "exit_basis": "close",
                    "entry_price": 100.0,
                    "exit_price": 100.0 * (1.0 + realized),
                    "gross_forward_return": realized,
                    "baseline_type": "market",
                    "baseline_forward_return": 0.0,
                    "excess_forward_return": realized,
                    "label_available_flag": True,
                    "exclusion_reason": None,
                    "notes_json": "{}",
                    "created_at": created_at,
                }
            )

    def _artifact_payload(feature_name: str) -> dict[str, object]:
        return {
            "feature_columns": [feature_name],
            "member_order": ["hist_gbm"],
            "members": {"hist_gbm": PredictFeatureModel(feature_name)},
            "ensemble_weights": {"hist_gbm": 1.0},
            "calibration": [
                {
                    "bucket": "global",
                    "prediction_lower": None,
                    "prediction_upper": None,
                    "residual_q25": -0.01,
                    "residual_median": 0.0,
                    "residual_q75": 0.01,
                    "expected_abs_error": 0.01,
                    "sample_count": 60,
                }
            ],
            "model_domain": MODEL_DOMAIN,
            "model_spec_id": "alpha_swing_d5_v2",
            "estimation_scheme": "rolling",
            "rolling_window_days": 250,
            "target_variant": "top5_binary",
            "training_target_variant": "top5_binary",
            "fallback_flag": False,
            "fallback_reason": None,
        }

    old_training_run_id = "checkpoint-alpha_swing_d5_v2-h5-old"
    new_training_run_id = "checkpoint-alpha_swing_d5_v2-h5-new"
    old_artifact_path = settings.paths.artifacts_dir / "tests" / old_training_run_id / "model.pkl"
    new_artifact_path = settings.paths.artifacts_dir / "tests" / new_training_run_id / "model.pkl"
    write_model_artifact(old_artifact_path, _artifact_payload("alpha_champion_signal"))
    write_model_artifact(new_artifact_path, _artifact_payload("alpha_candidate_signal"))

    training_rows = [
        {
            "training_run_id": old_training_run_id,
            "run_id": "seed-checkpoint-training",
            "model_domain": MODEL_DOMAIN,
            "model_version": MODEL_VERSION,
            "model_spec_id": "alpha_swing_d5_v2",
            "estimation_scheme": "rolling",
            "rolling_window_days": 250,
            "horizon": 5,
            "panel_name": "all",
            "train_end_date": date(2026, 3, 1),
            "training_window_start": date(2025, 6, 1),
            "training_window_end": date(2026, 2, 27),
            "validation_window_start": date(2026, 2, 23),
            "validation_window_end": date(2026, 2, 27),
            "train_row_count": 300,
            "validation_row_count": 60,
            "feature_count": 1,
            "ensemble_weight_json": '{"hist_gbm":1.0}',
            "model_family_json": '{"members":["hist_gbm"]}',
            "threshold_payload_json": None,
            "diagnostic_artifact_uri": None,
            "metadata_json": None,
            "fallback_flag": False,
            "fallback_reason": None,
            "artifact_uri": str(old_artifact_path),
            "notes": "old h5 champion run",
            "status": "success",
            "created_at": created_at,
        },
        {
            "training_run_id": new_training_run_id,
            "run_id": "seed-checkpoint-training",
            "model_domain": MODEL_DOMAIN,
            "model_version": MODEL_VERSION,
            "model_spec_id": "alpha_swing_d5_v2",
            "estimation_scheme": "rolling",
            "rolling_window_days": 250,
            "horizon": 5,
            "panel_name": "all",
            "train_end_date": date(2026, 3, 7),
            "training_window_start": date(2025, 6, 7),
            "training_window_end": date(2026, 3, 6),
            "validation_window_start": date(2026, 3, 2),
            "validation_window_end": date(2026, 3, 6),
            "train_row_count": 320,
            "validation_row_count": 60,
            "feature_count": 1,
            "ensemble_weight_json": '{"hist_gbm":1.0}',
            "model_family_json": '{"members":["hist_gbm"]}',
            "threshold_payload_json": None,
            "diagnostic_artifact_uri": None,
            "metadata_json": None,
            "fallback_flag": False,
            "fallback_reason": None,
            "artifact_uri": str(new_artifact_path),
            "notes": "latest h5 challenger run",
            "status": "success",
            "created_at": created_at + pd.Timedelta(minutes=5),
        },
    ]

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.register("checkpoint_symbol_stage", pd.DataFrame(symbol_rows))
        connection.execute(
            """
            INSERT OR REPLACE INTO dim_symbol (
                symbol, company_name, market, sector, industry, listing_date,
                is_common_stock, is_etf, is_etn, is_spac, is_delisted,
                source, as_of_date, updated_at
            )
            SELECT
                symbol, company_name, market, sector, industry, listing_date,
                is_common_stock, is_etf, is_etn, is_spac, is_delisted,
                source, as_of_date, updated_at
            FROM checkpoint_symbol_stage
            """
        )
        connection.unregister("checkpoint_symbol_stage")

        connection.register("checkpoint_regime_stage", pd.DataFrame(market_regime_rows))
        connection.execute(
            """
            INSERT OR REPLACE INTO fact_market_regime_snapshot (
                run_id, as_of_date, market_scope, breadth_up_ratio, breadth_down_ratio,
                median_symbol_return_1d, median_symbol_return_5d, market_realized_vol_20d, turnover_burst_z,
                new_high_ratio_20d, new_low_ratio_20d, regime_state, regime_score,
                notes_json, created_at
            )
            SELECT
                run_id, as_of_date, market_scope, breadth_up_ratio, breadth_down_ratio,
                median_symbol_return_1d, median_symbol_return_5d, market_realized_vol_20d, turnover_burst_z,
                new_high_ratio_20d, new_low_ratio_20d, regime_state, regime_score,
                notes_json, created_at
            FROM checkpoint_regime_stage
            """
        )
        connection.unregister("checkpoint_regime_stage")

        connection.register("checkpoint_feature_stage", pd.DataFrame(feature_rows))
        connection.execute(
            """
            INSERT OR REPLACE INTO fact_feature_snapshot (
                run_id, as_of_date, symbol, feature_name, feature_value, feature_group,
                source_version, feature_rank_pct, feature_zscore, is_imputed,
                notes_json, created_at
            )
            SELECT
                run_id, as_of_date, symbol, feature_name, feature_value, feature_group,
                source_version, feature_rank_pct, feature_zscore, is_imputed,
                notes_json, created_at
            FROM checkpoint_feature_stage
            """
        )
        connection.unregister("checkpoint_feature_stage")

        connection.register("checkpoint_label_stage", pd.DataFrame(label_rows))
        connection.execute(
            """
            INSERT OR REPLACE INTO fact_forward_return_label (
                run_id, as_of_date, symbol, horizon, market, entry_date, exit_date,
                entry_basis, exit_basis, entry_price, exit_price, gross_forward_return,
                baseline_type, baseline_forward_return, excess_forward_return,
                label_available_flag, exclusion_reason, notes_json, created_at
            )
            SELECT
                run_id, as_of_date, symbol, horizon, market, entry_date, exit_date,
                entry_basis, exit_basis, entry_price, exit_price, gross_forward_return,
                baseline_type, baseline_forward_return, excess_forward_return,
                label_available_flag, exclusion_reason, notes_json, created_at
            FROM checkpoint_label_stage
            """
        )
        connection.unregister("checkpoint_label_stage")

        upsert_model_training_runs(connection, pd.DataFrame(training_rows))

    return {
        "old_training_run_id": old_training_run_id,
        "new_training_run_id": new_training_run_id,
    }


def test_run_alpha_auto_promotion_promotes_h1_and_defers_h5_without_checkpoint_history(tmp_path):
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
    assert result.promoted_horizon_count == 1

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
        h5_decision_row = connection.execute(
            """
            SELECT decision, promotion_scope, detail_json
            FROM fact_alpha_promotion_test
            WHERE promotion_date = ?
              AND horizon = 5
              AND challenger_model_spec_id = 'alpha_swing_d5_v2'
              AND loss_name = 'loss_top5'
            """,
            [date(2026, 3, 10)],
        ).fetchone()

    assert active_h1 is not None
    assert active_h5 is not None
    assert active_h1["model_spec_id"] == "alpha_lead_d1_v1"
    assert active_h5["model_spec_id"] == MODEL_SPEC_ID
    assert active_h1["promotion_type"] == "AUTO_PROMOTION"
    assert active_h1["source_type"] == "alpha_auto_promotion"
    assert "alpha_lead_d1_v1" in active_h1["promotion_report_json"]["superior_set"]
    assert promotion_rows > 0
    assert decision_row == ("PROMOTE_CHALLENGER", True, False, len(SELECTION_DATES))
    assert h5_decision_row is not None
    assert h5_decision_row[0] == "NO_AUTO_PROMOTION"
    assert h5_decision_row[1] == "training_run_checkpoint"
    assert "checkpoint_no_matured_shadow_history" in str(h5_decision_row[2])


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


def test_run_alpha_auto_promotion_promotes_h5_latest_checkpoint_candidate(tmp_path):
    settings = _build_promotion_settings(tmp_path)
    checkpoint_ids = _seed_h5_checkpoint_challenge_data(settings)
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 1),
        source="test_seed",
        note="seed old h5 champion",
        horizons=[5],
        model_spec_id="alpha_swing_d5_v2",
        train_end_date=date(2026, 3, 1),
    )

    result = run_alpha_auto_promotion(
        settings,
        as_of_date=date(2026, 3, 10),
        horizons=[5],
        lookback_selection_dates=len(CHECKPOINT_SELECTION_DATES),
        bootstrap_reps=200,
        block_length=1,
    )

    assert result.promoted_horizon_count == 1

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        active_h5 = load_active_alpha_model(connection, as_of_date=date(2026, 3, 10), horizon=5)
        active_training_run = load_training_run_by_id(
            connection,
            training_run_id=str(active_h5["training_run_id"]),
        )
        promotion_row = connection.execute(
            """
            SELECT
                decision,
                promotion_scope,
                incumbent_training_run_id,
                challenger_training_run_id,
                chosen_training_run_id,
                sample_count,
                detail_json
            FROM fact_alpha_promotion_test
            WHERE promotion_date = ?
              AND horizon = 5
              AND challenger_model_spec_id = 'alpha_swing_d5_v2'
              AND loss_name = 'loss_top5'
            """,
            [date(2026, 3, 10)],
        ).fetchone()

    assert active_h5 is not None
    assert active_h5["model_spec_id"] == "alpha_swing_d5_v2"
    assert active_h5["promotion_type"] == "AUTO_PROMOTION"
    assert active_h5["training_run_id"] == checkpoint_ids["new_training_run_id"]
    assert active_training_run is not None
    assert active_training_run["train_end_date"] == date(2026, 3, 7)
    assert promotion_row is not None
    assert promotion_row[0] == "PROMOTE_CHALLENGER"
    assert promotion_row[1] == "training_run_checkpoint"
    assert promotion_row[2] == checkpoint_ids["old_training_run_id"]
    assert promotion_row[3] == checkpoint_ids["new_training_run_id"]
    assert promotion_row[4] == checkpoint_ids["new_training_run_id"]
    assert int(promotion_row[5] or 0) == len(CHECKPOINT_SELECTION_DATES)
    assert "checkpoint_candidate_promoted" in str(promotion_row[6])


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
