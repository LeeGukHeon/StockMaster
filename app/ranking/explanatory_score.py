from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.feature_store import load_feature_matrix
from app.ranking.grade_assignment import assign_grades
from app.ranking.reason_tags import build_eligibility_notes, build_reason_tags, build_risk_flags
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

RANKING_VERSION = "explanatory_ranking_v0"
RESERVED_COMPONENTS = {"flow_score": "reserved"}
HORIZON_WEIGHTS = {
    1: {
        "trend_momentum_score": 25,
        "turnover_participation_score": 20,
        "quality_score": 5,
        "value_safety_score": 5,
        "news_catalyst_score": 20,
        "regime_fit_score": 15,
        "risk_penalty_score": -15,
    },
    5: {
        "trend_momentum_score": 30,
        "turnover_participation_score": 15,
        "quality_score": 15,
        "value_safety_score": 10,
        "news_catalyst_score": 10,
        "regime_fit_score": 10,
        "risk_penalty_score": -15,
    },
}


@dataclass(slots=True)
class RankingMaterializationResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str
    ranking_version: str


def _feature_rank(frame: pd.DataFrame, feature_name: str) -> pd.Series:
    column = f"{feature_name}_rank_pct"
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _feature_inverse_rank(frame: pd.DataFrame, feature_name: str) -> pd.Series:
    values = _feature_rank(frame, feature_name)
    return 1.0 - values if values.notna().any() else values


def _component_score(*series_list: pd.Series) -> pd.Series:
    if not series_list:
        raise ValueError("At least one series is required to compute a component score.")
    working = pd.concat(series_list, axis=1)
    score = working.mean(axis=1, skipna=True).fillna(0.5)
    return (score * 100.0).clip(lower=0.0, upper=100.0)


def _load_regime_map(connection, *, as_of_date: date) -> dict[str, dict[str, object]]:
    frame = connection.execute(
        """
        SELECT market_scope, regime_state, regime_score
        FROM fact_market_regime_snapshot
        WHERE as_of_date = ?
        """,
        [as_of_date],
    ).fetchdf()
    if frame.empty:
        raise RuntimeError(
            "Market regime snapshot is missing for the requested date. "
            "Run scripts/build_market_regime_snapshot.py first."
        )
    return {
        str(row.market_scope): {
            "regime_state": row.regime_state,
            "regime_score": row.regime_score,
        }
        for row in frame.itertuples(index=False)
    }


def upsert_ranking(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("ranking_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_ranking
        WHERE (as_of_date, symbol, horizon, ranking_version) IN (
            SELECT as_of_date, symbol, horizon, ranking_version
            FROM ranking_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_ranking (
            run_id,
            as_of_date,
            symbol,
            horizon,
            final_selection_value,
            final_selection_rank_pct,
            grade,
            explanatory_score_json,
            top_reason_tags_json,
            risk_flags_json,
            eligible_flag,
            eligibility_notes_json,
            regime_state,
            ranking_version,
            created_at
        )
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            final_selection_value,
            final_selection_rank_pct,
            grade,
            explanatory_score_json,
            top_reason_tags_json,
            risk_flags_json,
            eligible_flag,
            eligibility_notes_json,
            regime_state,
            ranking_version,
            created_at
        FROM ranking_stage
        """
    )
    connection.unregister("ranking_stage")


def _compute_regime_fit_score(frame: pd.DataFrame) -> pd.Series:
    risk_on_like = frame["regime_state"].isin({"risk_on", "euphoria"})
    risk_off_like = frame["regime_state"].isin({"panic", "risk_off"})
    neutral = ~(risk_on_like | risk_off_like)

    result = pd.Series(50.0, index=frame.index)
    if risk_on_like.any():
        result.loc[risk_on_like] = _component_score(
            frame.loc[risk_on_like, "trend_momentum_score"] / 100.0,
            frame.loc[risk_on_like, "turnover_participation_score"] / 100.0,
            frame.loc[risk_on_like, "news_catalyst_score"] / 100.0,
        )
    if risk_off_like.any():
        result.loc[risk_off_like] = _component_score(
            1.0 - _feature_rank(frame.loc[risk_off_like], "realized_vol_20d"),
            _feature_rank(frame.loc[risk_off_like], "drawdown_20d"),
            frame.loc[risk_off_like, "quality_score"] / 100.0,
            frame.loc[risk_off_like, "data_confidence_score"] / 100.0,
        )
    if neutral.any():
        result.loc[neutral] = _component_score(
            frame.loc[neutral, "trend_momentum_score"] / 100.0,
            frame.loc[neutral, "quality_score"] / 100.0,
            frame.loc[neutral, "data_confidence_score"] / 100.0,
        )
    return result.clip(lower=0.0, upper=100.0)


def _compute_risk_penalty_score(frame: pd.DataFrame) -> pd.Series:
    missingness_scaled = (
        pd.to_numeric(frame["missing_key_feature_count"], errors="coerce") / 7.0
    ).clip(
        lower=0.0,
        upper=1.0,
    )
    return _component_score(
        _feature_rank(frame, "realized_vol_20d"),
        _feature_inverse_rank(frame, "drawdown_20d"),
        _feature_rank(frame, "gap_abs_avg_20d"),
        pd.to_numeric(frame["stale_price_flag"], errors="coerce"),
        missingness_scaled,
    )


def _apply_horizon_scores(frame: pd.DataFrame, *, horizon: int) -> pd.DataFrame:
    scored = frame.copy()
    if horizon == 1:
        scored["trend_momentum_score"] = _component_score(
            _feature_rank(scored, "ret_3d"),
            _feature_rank(scored, "ret_5d"),
            _feature_rank(scored, "ma5_over_ma20"),
            _feature_rank(scored, "dist_from_20d_high"),
        )
    else:
        scored["trend_momentum_score"] = _component_score(
            _feature_rank(scored, "ret_5d"),
            _feature_rank(scored, "ret_20d"),
            _feature_rank(scored, "ma20_over_ma60"),
            _feature_rank(scored, "drawdown_20d"),
        )

    scored["turnover_participation_score"] = _component_score(
        _feature_rank(scored, "volume_ratio_1d_vs_20d"),
        _feature_rank(scored, "turnover_z_5_20"),
        _feature_rank(scored, "adv_20"),
        _feature_rank(scored, "turnover_value_1d"),
    )
    scored["quality_score"] = _component_score(
        _feature_rank(scored, "roe_latest"),
        _feature_rank(scored, "operating_margin_latest"),
        _feature_rank(scored, "net_income_positive_flag"),
        _feature_inverse_rank(scored, "days_since_latest_report"),
    )
    scored["value_safety_score"] = _component_score(
        _feature_inverse_rank(scored, "debt_ratio_latest"),
        _feature_rank(scored, "low_debt_preference_proxy"),
        _feature_rank(scored, "profitability_support_proxy"),
        _feature_rank(scored, "earnings_yield_proxy"),
    )
    scored["news_catalyst_score"] = (
        _component_score(
            _feature_rank(scored, "news_count_1d"),
            _feature_rank(scored, "news_count_3d"),
            _feature_inverse_rank(scored, "latest_news_age_hours"),
            _feature_rank(scored, "distinct_publishers_3d"),
            _feature_rank(scored, "positive_catalyst_count_3d"),
        )
        - _feature_rank(scored, "negative_catalyst_count_3d").fillna(0.5) * 10.0
    ).clip(lower=0.0, upper=100.0)
    scored["risk_penalty_score"] = _compute_risk_penalty_score(scored)
    scored["regime_fit_score"] = _compute_regime_fit_score(scored)

    weights = HORIZON_WEIGHTS[horizon]
    positive_components = {key: weight for key, weight in weights.items() if weight > 0}
    positive_sum = sum(scored[name] * weight for name, weight in positive_components.items())
    positive_weight_total = sum(positive_components.values())
    positive_score = positive_sum / positive_weight_total
    risk_penalty = scored["risk_penalty_score"] * abs(weights["risk_penalty_score"]) / 100.0
    scored["final_selection_value"] = (positive_score - risk_penalty).clip(lower=0.0, upper=100.0)

    scored["eligible_flag"] = (
        (pd.to_numeric(scored["has_daily_ohlcv_flag"], errors="coerce").fillna(0) >= 1)
        & (pd.to_numeric(scored["stale_price_flag"], errors="coerce").fillna(1) < 1)
        & (
            (pd.to_numeric(scored["adv_20"], errors="coerce").fillna(0) >= 50_000_000)
            | (_feature_rank(scored, "adv_20").fillna(0) >= 0.2)
        )
        & (pd.to_numeric(scored["missing_key_feature_count"], errors="coerce").fillna(99) <= 5)
    )
    scored["final_selection_rank_pct"] = scored["final_selection_value"].rank(
        method="average", pct=True
    )
    risk_flags = scored.apply(build_risk_flags, axis=1)
    scored["critical_risk_flag"] = risk_flags.map(
        lambda items: any(
            flag in {"high_realized_volatility", "large_recent_drawdown"} for flag in items
        )
    )
    scored["grade"] = assign_grades(scored)
    scored["risk_flags_json"] = risk_flags.map(lambda items: json.dumps(items, ensure_ascii=False))
    scored["top_reason_tags_json"] = scored.apply(
        lambda row: json.dumps(build_reason_tags(row), ensure_ascii=False),
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
                "risk_penalty_score": float(row["risk_penalty_score"]),
                "flow_score_status": RESERVED_COMPONENTS["flow_score"],
                "active_weights": weights,
                "score_version": RANKING_VERSION,
                "data_confidence_score": float(row["data_confidence_score"]),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        axis=1,
    )
    scored["horizon"] = horizon
    return scored


def materialize_explanatory_ranking(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
    dry_run: bool = False,
) -> RankingMaterializationResult:
    ensure_storage_layout(settings)

    with activate_run_context(
        "materialize_explanatory_ranking", as_of_date=as_of_date
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
                    "Materialize explanatory ranking. "
                    f"as_of_date={as_of_date.isoformat()} horizons={horizons}"
                ),
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
                        ranking_version=RANKING_VERSION,
                    )
                    return RankingMaterializationResult(
                        run_id=run_context.run_id,
                        as_of_date=as_of_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                        ranking_version=RANKING_VERSION,
                    )

                ranking_frames: list[pd.DataFrame] = []
                for horizon in horizons:
                    scored = _apply_horizon_scores(feature_matrix, horizon=horizon)
                    scored["run_id"] = run_context.run_id
                    scored["as_of_date"] = as_of_date
                    scored["ranking_version"] = RANKING_VERSION
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
                        [as_of_date, RANKING_VERSION],
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
                                },
                                filename="explanatory_ranking.parquet",
                            )
                        )
                    )

                notes = (
                    f"Ranking materialization completed. as_of_date={as_of_date.isoformat()}, "
                    f"rows={len(ranking_output)}, horizons={horizons}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=RANKING_VERSION,
                )
                return RankingMaterializationResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(ranking_output),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    ranking_version=RANKING_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=f"Ranking materialization failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=RANKING_VERSION,
                )
                raise
