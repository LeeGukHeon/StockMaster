from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.feature_store import load_feature_matrix
from app.ranking.explanatory_score import (
    _apply_horizon_scores,
    _component_score,
    _feature_inverse_rank,
    _feature_rank,
    _load_regime_map,
    upsert_ranking,
)
from app.ranking.grade_assignment import assign_grades
from app.ranking.reason_tags import build_eligibility_notes, build_reason_tags, build_risk_flags
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

SELECTION_ENGINE_VERSION = "selection_engine_v1"
SELECTION_COMPONENT_WEIGHTS = {
    1: {
        "trend_momentum_score": 20,
        "turnover_participation_score": 12,
        "quality_score": 6,
        "value_safety_score": 4,
        "news_catalyst_score": 12,
        "flow_score": 18,
        "regime_fit_score": 10,
        "risk_penalty_score": -10,
        "uncertainty_proxy_score": -5,
        "implementation_penalty_score": -3,
    },
    5: {
        "trend_momentum_score": 20,
        "turnover_participation_score": 8,
        "quality_score": 12,
        "value_safety_score": 10,
        "news_catalyst_score": 8,
        "flow_score": 20,
        "regime_fit_score": 10,
        "risk_penalty_score": -8,
        "uncertainty_proxy_score": -6,
        "implementation_penalty_score": -6,
    },
}


@dataclass(slots=True)
class SelectionEngineMaterializationResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str
    ranking_version: str


def _coverage_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(float("nan"), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _compute_flow_score(frame: pd.DataFrame, *, horizon: int) -> pd.Series:
    common_series = [
        _feature_rank(frame, "foreign_net_value_ratio_5d"),
        _feature_rank(frame, "institution_net_value_ratio_5d"),
        _feature_rank(frame, "flow_alignment_score"),
        _feature_rank(frame, "flow_coverage_flag"),
    ]
    if horizon == 1:
        return _component_score(
            _feature_rank(frame, "foreign_net_value_ratio_1d"),
            _feature_rank(frame, "smart_money_flow_ratio_5d"),
            *common_series,
        )
    return _component_score(
        _feature_rank(frame, "smart_money_flow_ratio_20d"),
        _feature_inverse_rank(frame, "individual_net_value_ratio_5d"),
        *common_series,
    )


def _compute_uncertainty_proxy_score(frame: pd.DataFrame) -> pd.Series:
    flow_missing = 1.0 - _coverage_series(frame, "flow_coverage_flag").fillna(0.0)
    return _component_score(
        _feature_rank(frame, "realized_vol_20d"),
        _feature_rank(frame, "gap_abs_avg_20d"),
        _feature_inverse_rank(frame, "data_confidence_score"),
        flow_missing,
    )


def _compute_implementation_penalty_score(frame: pd.DataFrame) -> pd.Series:
    flow_missing = 1.0 - _coverage_series(frame, "flow_coverage_flag").fillna(0.0)
    return _component_score(
        _feature_inverse_rank(frame, "adv_20"),
        _feature_inverse_rank(frame, "turnover_value_1d"),
        _feature_rank(frame, "realized_vol_20d"),
        _coverage_series(frame, "stale_price_flag").fillna(1.0),
        flow_missing,
    )


def _augment_reason_tags(row: pd.Series, tags: list[str]) -> list[str]:
    output = list(tags)
    if row.get("flow_score", 0) >= 65:
        output.append("foreign_institution_flow_supportive")
    if row.get("implementation_penalty_score", 0) <= 35 and row.get("adv_20", 0) >= 0:
        output.append("implementation_friction_contained")
    return output[:3]


def _augment_risk_flags(row: pd.Series, flags: list[str]) -> list[str]:
    output = list(flags)
    if row.get("uncertainty_proxy_score", 0) >= 70:
        output.append("uncertainty_proxy_high")
    if row.get("implementation_penalty_score", 0) >= 70:
        output.append("implementation_friction_high")
    if row.get("flow_coverage_flag", 0) < 1:
        output.append("flow_coverage_missing")
    return sorted(set(output))


def _apply_selection_engine_v1(
    frame: pd.DataFrame,
    *,
    horizon: int,
    settings: Settings,
) -> pd.DataFrame:
    scored = _apply_horizon_scores(frame, horizon=horizon)
    scored["flow_score"] = _compute_flow_score(scored, horizon=horizon)
    scored["uncertainty_proxy_score"] = _compute_uncertainty_proxy_score(scored)
    scored["implementation_penalty_score"] = _compute_implementation_penalty_score(scored)

    weights = dict(SELECTION_COMPONENT_WEIGHTS[horizon])
    positive_components = {key: value for key, value in weights.items() if value > 0}
    positive_score = sum(
        scored[name]
        * (
            settings.model.regime_rho
            if name == "regime_fit_score"
            else 1.0
        )
        * weight
        for name, weight in positive_components.items()
    ) / sum(positive_components.values())

    risk_penalty = scored["risk_penalty_score"] * abs(weights["risk_penalty_score"]) / 100.0
    uncertainty_penalty = (
        scored["uncertainty_proxy_score"]
        * abs(weights["uncertainty_proxy_score"])
        * settings.model.uncertainty_lambda
        / 100.0
    )
    implementation_penalty = (
        scored["implementation_penalty_score"]
        * abs(weights["implementation_penalty_score"])
        * settings.model.implementation_kappa
        / 100.0
    )
    scored["final_selection_value"] = (
        positive_score - risk_penalty - uncertainty_penalty - implementation_penalty
    ).clip(lower=0.0, upper=100.0)
    scored["final_selection_rank_pct"] = scored["final_selection_value"].rank(
        method="average",
        pct=True,
    )

    risk_flags = scored.apply(build_risk_flags, axis=1)
    risk_flags = pd.Series(
        [
            _augment_risk_flags(row, flags)
            for (_, row), flags in zip(scored.iterrows(), risk_flags, strict=False)
        ],
        index=scored.index,
    )
    scored["critical_risk_flag"] = risk_flags.map(
        lambda items: any(
            flag
            in {
                "high_realized_volatility",
                "large_recent_drawdown",
                "implementation_friction_high",
            }
            for flag in items
        )
    )
    scored["eligible_flag"] = scored["eligible_flag"] & (
        pd.to_numeric(scored["implementation_penalty_score"], errors="coerce").fillna(100.0) < 90
    )
    scored["grade"] = assign_grades(scored)
    scored["risk_flags_json"] = risk_flags.map(lambda items: json.dumps(items, ensure_ascii=False))
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
    scored["explanatory_score_json"] = scored.apply(
        lambda row: json.dumps(
            {
                "trend_momentum_score": float(row["trend_momentum_score"]),
                "turnover_participation_score": float(row["turnover_participation_score"]),
                "quality_score": float(row["quality_score"]),
                "value_safety_score": float(row["value_safety_score"]),
                "news_catalyst_score": float(row["news_catalyst_score"]),
                "regime_fit_score": float(row["regime_fit_score"]),
                "flow_score": float(row["flow_score"]),
                "risk_penalty_score": float(row["risk_penalty_score"]),
                "uncertainty_proxy_score": float(row["uncertainty_proxy_score"]),
                "implementation_penalty_score": float(row["implementation_penalty_score"]),
                "disagreement_score": None,
                "active_weights": weights,
                "score_version": SELECTION_ENGINE_VERSION,
                "score_type": "selection_engine_v1",
                "prediction_note": "explanatory_and_proxy_selection_only",
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        axis=1,
    )
    scored["horizon"] = horizon
    return scored


def materialize_selection_engine_v1(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
    dry_run: bool = False,
) -> SelectionEngineMaterializationResult:
    ensure_storage_layout(settings)

    with activate_run_context(
        "materialize_selection_engine_v1",
        as_of_date=as_of_date,
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
                    "fact_market_regime_snapshot",
                    "dim_symbol",
                ],
                notes=(
                    "Materialize selection engine v1. "
                    f"as_of_date={as_of_date.isoformat()} horizons={horizons}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                feature_matrix = load_feature_matrix(
                    connection,
                    as_of_date=as_of_date,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                )
                if feature_matrix.empty:
                    raise RuntimeError(
                        "Feature snapshot is missing for the requested date. "
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
                if dry_run:
                    notes = (
                        f"Dry run only. as_of_date={as_of_date.isoformat()} "
                        f"symbols={len(feature_matrix)} horizons={horizons}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return SelectionEngineMaterializationResult(
                        run_id=run_context.run_id,
                        as_of_date=as_of_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )

                ranking_frames: list[pd.DataFrame] = []
                for horizon in horizons:
                    scored = _apply_selection_engine_v1(
                        feature_matrix,
                        horizon=horizon,
                        settings=settings,
                    )
                    scored["run_id"] = run_context.run_id
                    scored["as_of_date"] = as_of_date
                    scored["ranking_version"] = SELECTION_ENGINE_VERSION
                    scored["created_at"] = pd.Timestamp.utcnow()
                    ranking_frames.append(scored)

                combined = pd.concat(ranking_frames, ignore_index=True)
                ranking_output = combined[
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
                if force:
                    connection.execute(
                        """
                        DELETE FROM fact_ranking
                        WHERE as_of_date = ?
                          AND ranking_version = ?
                        """,
                        [as_of_date, SELECTION_ENGINE_VERSION],
                    )
                upsert_ranking(connection, ranking_output)

                artifact_paths: list[str] = []
                for horizon, partition_frame in ranking_output.groupby("horizon", sort=True):
                    artifact_paths.append(
                        str(
                            write_parquet(
                                partition_frame,
                                base_dir=settings.paths.curated_dir,
                                dataset="ranking",
                                partitions={
                                    "as_of_date": as_of_date.isoformat(),
                                    "horizon": str(horizon),
                                    "ranking_version": SELECTION_ENGINE_VERSION,
                                },
                                filename="selection_engine_v1.parquet",
                            )
                        )
                    )

                notes = (
                    f"Selection engine v1 completed. as_of_date={as_of_date.isoformat()}, "
                    f"rows={len(ranking_output)}, horizons={horizons}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return SelectionEngineMaterializationResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(ranking_output),
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
                    notes=f"Selection engine v1 failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
