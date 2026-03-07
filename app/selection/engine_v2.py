from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.ml.inference import materialize_alpha_predictions_v1
from app.ranking.explanatory_score import _load_regime_map, upsert_ranking
from app.ranking.grade_assignment import assign_grades
from app.ranking.reason_tags import build_eligibility_notes, build_reason_tags, build_risk_flags
from app.selection.engine_v1 import _apply_selection_engine_v1
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

SELECTION_V2_WEIGHTS = {
    1: {
        "alpha_core_score": 52,
        "flow_score": 14,
        "trend_momentum_score": 10,
        "news_catalyst_score": 8,
        "quality_score": 6,
        "regime_fit_score": 10,
        "risk_penalty_score": -6,
        "uncertainty_score": -12,
        "disagreement_score": -8,
        "implementation_penalty_score": -8,
        "fallback_penalty": -4,
    },
    5: {
        "alpha_core_score": 48,
        "flow_score": 16,
        "trend_momentum_score": 10,
        "quality_score": 10,
        "value_safety_score": 8,
        "regime_fit_score": 8,
        "risk_penalty_score": -6,
        "uncertainty_score": -10,
        "disagreement_score": -10,
        "implementation_penalty_score": -8,
        "fallback_penalty": -4,
    },
}


@dataclass(slots=True)
class SelectionEngineV2Result:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str
    ranking_version: str


def _augment_reason_tags(row: pd.Series, tags: list[str]) -> list[str]:
    values = list(tags)
    if pd.notna(row.get("expected_excess_return")) and float(row["expected_excess_return"]) > 0:
        values.append("ml_alpha_supportive")
    if bool(row.get("fallback_flag")):
        values.append("prediction_fallback_used")
    return values[:3]


def _augment_risk_flags(row: pd.Series, flags: list[str]) -> list[str]:
    values = list(flags)
    if pd.to_numeric(row.get("uncertainty_score"), errors="coerce") >= 70:
        values.append("model_uncertainty_high")
    if pd.to_numeric(row.get("disagreement_score"), errors="coerce") >= 70:
        values.append("model_disagreement_high")
    if bool(row.get("fallback_flag")):
        values.append("prediction_fallback")
    return sorted(set(values))


def _alpha_core_score(frame: pd.DataFrame) -> pd.Series:
    expected = pd.to_numeric(frame["expected_excess_return"], errors="coerce")
    if expected.notna().sum() <= 1:
        return pd.Series(50.0, index=frame.index)
    return expected.rank(method="average", pct=True).mul(100.0).fillna(50.0)


def _load_predictions(connection, *, as_of_date: date, horizon: int) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            symbol,
            expected_excess_return,
            lower_band,
            median_band,
            upper_band,
            uncertainty_score,
            disagreement_score,
            fallback_flag,
            fallback_reason,
            prediction_version,
            member_count,
            ensemble_weight_json,
            source_notes_json
        FROM fact_prediction
        WHERE as_of_date = ?
          AND horizon = ?
          AND prediction_version = ?
          AND ranking_version = ?
        """,
        [as_of_date, horizon, ALPHA_PREDICTION_VERSION, SELECTION_ENGINE_VERSION],
    ).fetchdf()


def materialize_selection_engine_v2(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
) -> SelectionEngineV2Result:
    ensure_storage_layout(settings)
    materialize_alpha_predictions_v1(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
        symbols=symbols,
        limit_symbols=limit_symbols,
        market=market,
    )

    with activate_run_context(
        "materialize_selection_engine_v2", as_of_date=as_of_date
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_feature_snapshot",
                    "fact_prediction",
                    "fact_market_regime_snapshot",
                ],
                notes=(
                    "Materialize selection engine v2. "
                    f"as_of_date={as_of_date.isoformat()} horizons={horizons}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                from app.features.feature_store import load_feature_matrix

                feature_matrix = load_feature_matrix(
                    connection,
                    as_of_date=as_of_date,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                )
                if feature_matrix.empty:
                    raise RuntimeError(
                        "Feature snapshot is missing for selection engine v2. "
                        "Run scripts/build_feature_store.py first."
                    )
                regime_map = _load_regime_map(connection, as_of_date=as_of_date)
                feature_matrix["regime_state"] = feature_matrix["market"].map(
                    lambda value: regime_map.get(str(value).upper(), regime_map["KR_ALL"])[
                        "regime_state"
                    ]
                )
                feature_matrix["regime_score"] = feature_matrix["market"].map(
                    lambda value: regime_map.get(str(value).upper(), regime_map["KR_ALL"])[
                        "regime_score"
                    ]
                )

                ranking_frames: list[pd.DataFrame] = []
                artifact_paths: list[str] = []
                for horizon in horizons:
                    base = _apply_selection_engine_v1(
                        feature_matrix, horizon=horizon, settings=settings
                    )
                    prediction_frame = _load_predictions(
                        connection,
                        as_of_date=as_of_date,
                        horizon=int(horizon),
                    )
                    scored = base.merge(prediction_frame, on="symbol", how="left")
                    scored["alpha_core_score"] = _alpha_core_score(scored)
                    scored["uncertainty_score"] = pd.to_numeric(
                        scored["uncertainty_score"],
                        errors="coerce",
                    ).fillna(pd.to_numeric(scored["uncertainty_proxy_score"], errors="coerce"))
                    scored["disagreement_score"] = pd.to_numeric(
                        scored["disagreement_score"],
                        errors="coerce",
                    )
                    disagreement_fill = (
                        scored["disagreement_score"].rank(pct=True).mul(100.0)
                        if scored["disagreement_score"].notna().any()
                        else pd.Series(pd.NA, index=scored.index)
                    )
                    scored["disagreement_score"] = scored["disagreement_score"].fillna(
                        disagreement_fill
                    )
                    scored["fallback_flag"] = scored["fallback_flag"].fillna(False).astype(bool)
                    scored["fallback_reason"] = scored["fallback_reason"].fillna("")

                    weights = dict(SELECTION_V2_WEIGHTS[int(horizon)])
                    alpha_positive_components = {
                        key: value for key, value in weights.items() if value > 0
                    }
                    positive_score = sum(
                        pd.to_numeric(scored[name], errors="coerce").fillna(50.0) * weight
                        for name, weight in alpha_positive_components.items()
                    ) / sum(alpha_positive_components.values())
                    risk_penalty = (
                        pd.to_numeric(scored["risk_penalty_score"], errors="coerce").fillna(50.0)
                        * abs(weights["risk_penalty_score"])
                        / 100.0
                    )
                    uncertainty_penalty = (
                        pd.to_numeric(scored["uncertainty_score"], errors="coerce").fillna(50.0)
                        * abs(weights["uncertainty_score"])
                        * settings.model.uncertainty_lambda
                        / 100.0
                    )
                    disagreement_penalty = (
                        pd.to_numeric(scored["disagreement_score"], errors="coerce").fillna(50.0)
                        * abs(weights["disagreement_score"])
                        * settings.model.disagreement_eta
                        / 100.0
                    )
                    implementation_penalty = (
                        pd.to_numeric(
                            scored["implementation_penalty_score"], errors="coerce"
                        ).fillna(50.0)
                        * abs(weights["implementation_penalty_score"])
                        * settings.model.implementation_kappa
                        / 100.0
                    )
                    fallback_penalty = scored["fallback_flag"].astype(float) * abs(
                        weights["fallback_penalty"]
                    )
                    scored["final_selection_value"] = (
                        positive_score
                        - risk_penalty
                        - uncertainty_penalty
                        - disagreement_penalty
                        - implementation_penalty
                        - fallback_penalty
                    ).clip(lower=0.0, upper=100.0)
                    scored["final_selection_rank_pct"] = scored["final_selection_value"].rank(
                        method="average",
                        pct=True,
                    )

                    risk_flags = scored.apply(build_risk_flags, axis=1)
                    risk_flags = pd.Series(
                        [
                            _augment_risk_flags(row, values)
                            for (_, row), values in zip(scored.iterrows(), risk_flags, strict=False)
                        ],
                        index=scored.index,
                    )
                    scored["critical_risk_flag"] = risk_flags.map(
                        lambda values: any(
                            flag
                            in {
                                "high_realized_volatility",
                                "large_recent_drawdown",
                                "model_uncertainty_high",
                            }
                            for flag in values
                        )
                    )
                    scored["grade"] = assign_grades(scored)
                    scored["risk_flags_json"] = risk_flags.map(
                        lambda values: json.dumps(values, ensure_ascii=False)
                    )
                    scored["top_reason_tags_json"] = scored.apply(
                        lambda row: json.dumps(
                            _augment_reason_tags(row, build_reason_tags(row)),
                            ensure_ascii=False,
                        ),
                        axis=1,
                    )
                    scored["eligibility_notes_json"] = scored.apply(
                        lambda row: build_eligibility_notes(
                            row,
                            risk_flags=json.loads(str(row["risk_flags_json"])),
                        ),
                        axis=1,
                    )
                    weight_payload = dict(weights)
                    scored["explanatory_score_json"] = scored.apply(
                        lambda row, weight_payload=weight_payload: json.dumps(
                            {
                                "alpha_core_score": float(row["alpha_core_score"]),
                                "expected_excess_return": None
                                if pd.isna(row["expected_excess_return"])
                                else float(row["expected_excess_return"]),
                                "flow_score": float(row["flow_score"]),
                                "trend_momentum_score": float(row["trend_momentum_score"]),
                                "quality_score": float(row["quality_score"]),
                                "value_safety_score": float(row["value_safety_score"]),
                                "regime_fit_score": float(row["regime_fit_score"]),
                                "risk_penalty_score": float(row["risk_penalty_score"]),
                                "uncertainty_score": None
                                if pd.isna(row["uncertainty_score"])
                                else float(row["uncertainty_score"]),
                                "disagreement_score": None
                                if pd.isna(row["disagreement_score"])
                                else float(row["disagreement_score"]),
                                "implementation_penalty_score": float(
                                    row["implementation_penalty_score"]
                                ),
                                "fallback_flag": bool(row["fallback_flag"]),
                                "fallback_reason": row["fallback_reason"] or None,
                                "prediction_version": row.get("prediction_version"),
                                "score_version": SELECTION_ENGINE_VERSION,
                                "score_type": "selection_engine_v2",
                                "active_weights": weight_payload,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        axis=1,
                    )
                    scored["run_id"] = run_context.run_id
                    scored["as_of_date"] = as_of_date
                    scored["ranking_version"] = SELECTION_ENGINE_VERSION
                    scored["created_at"] = pd.Timestamp.utcnow()
                    scored["horizon"] = int(horizon)
                    ranking_output = scored[
                        [
                            "run_id",
                            "as_of_date",
                            "symbol",
                            "horizon",
                            "final_selection_value",
                            "final_selection_rank_pct",
                            "grade",
                            "explanatory_score_json",
                            "top_reason_tags_json",
                            "risk_flags_json",
                            "eligible_flag",
                            "eligibility_notes_json",
                            "regime_state",
                            "ranking_version",
                            "created_at",
                        ]
                    ].copy()
                    ranking_frames.append(ranking_output)
                    artifact_paths.append(
                        str(
                            write_parquet(
                                ranking_output,
                                base_dir=settings.paths.curated_dir,
                                dataset="ranking",
                                partitions={
                                    "as_of_date": as_of_date.isoformat(),
                                    "horizon": str(int(horizon)),
                                    "ranking_version": SELECTION_ENGINE_VERSION,
                                },
                                filename="selection_engine_v2.parquet",
                            )
                        )
                    )

                combined = pd.concat(ranking_frames, ignore_index=True)
                if force:
                    connection.execute(
                        """
                        DELETE FROM fact_ranking
                        WHERE as_of_date = ?
                          AND ranking_version = ?
                        """,
                        [as_of_date, SELECTION_ENGINE_VERSION],
                    )
                upsert_ranking(connection, combined)
                notes = (
                    "Selection engine v2 materialized. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(combined)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=ALPHA_PREDICTION_VERSION,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return SelectionEngineV2Result(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(combined),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Selection engine v2 materialization failed.",
                    error_message=str(exc),
                    model_version=ALPHA_PREDICTION_VERSION,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
