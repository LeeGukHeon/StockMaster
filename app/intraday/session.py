from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local, today_local
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from .common import DEFAULT_CHECKPOINTS, json_text, session_status


@dataclass(slots=True)
class IntradayCandidateSessionResult:
    run_id: str
    selection_date: date
    session_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def _resolve_next_trading_day(connection, *, selection_date: date) -> date:
    row = connection.execute(
        """
        SELECT COALESCE(next_trading_date, (
            SELECT MIN(trading_date)
            FROM dim_trading_calendar
            WHERE trading_date > base.trading_date
              AND is_trading_day
        ))
        FROM dim_trading_calendar AS base
        WHERE base.trading_date = ?
        """,
        [selection_date],
    ).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(
            "Unable to resolve next trading day for the requested selection_date. "
            "Run scripts/sync_trading_calendar.py first."
        )
    return pd.Timestamp(row[0]).date()


def _load_selection_candidates(
    connection,
    *,
    selection_date: date,
    horizons: list[int],
    max_candidates: int,
    ranking_version: str,
    market: str,
) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    params: list[object] = [
        ALPHA_PREDICTION_VERSION,
        selection_date,
        ranking_version,
        *horizons,
    ]
    market_clause = ""
    if market.upper() != "ALL":
        market_clause = "AND symbol_meta.market = ?"
        params.append(market.upper())
    params.append(max_candidates)
    return connection.execute(
        f"""
        WITH ranked AS (
            SELECT
                ranking.as_of_date AS selection_date,
                ranking.symbol,
                symbol_meta.market,
                symbol_meta.company_name,
                ranking.horizon,
                ranking.ranking_version,
                ranking.final_selection_value,
                ranking.final_selection_rank_pct,
                ranking.grade,
                ranking.eligible_flag,
                ranking.top_reason_tags_json,
                ranking.risk_flags_json,
                prediction.expected_excess_return,
                prediction.lower_band,
                prediction.upper_band,
                prediction.uncertainty_score,
                prediction.disagreement_score,
                prediction.model_spec_id,
                prediction.active_alpha_model_id,
                prediction.fallback_flag,
                ROW_NUMBER() OVER (
                    PARTITION BY ranking.horizon
                    ORDER BY ranking.final_selection_value DESC, ranking.symbol
                ) AS candidate_rank
            FROM fact_ranking AS ranking
            JOIN dim_symbol AS symbol_meta
              ON ranking.symbol = symbol_meta.symbol
            LEFT JOIN fact_prediction AS prediction
              ON ranking.as_of_date = prediction.as_of_date
             AND ranking.symbol = prediction.symbol
             AND ranking.horizon = prediction.horizon
             AND ranking.ranking_version = prediction.ranking_version
             AND prediction.prediction_version = ?
            WHERE ranking.as_of_date = ?
              AND ranking.ranking_version = ?
              AND ranking.horizon IN ({horizon_placeholders})
              {market_clause}
        )
        SELECT *
        FROM ranked
        WHERE candidate_rank <= ?
        ORDER BY horizon, candidate_rank, symbol
        """,
        params,
    ).fetchdf()


def upsert_intraday_candidate_session(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_candidate_session_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_candidate_session
        WHERE (session_date, symbol, horizon, ranking_version) IN (
            SELECT session_date, symbol, horizon, ranking_version
            FROM intraday_candidate_session_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_candidate_session (
            run_id,
            selection_date,
            session_date,
            symbol,
            market,
            company_name,
            horizon,
            ranking_version,
            candidate_rank,
            final_selection_value,
            final_selection_rank_pct,
            grade,
            eligible_flag,
            expected_excess_return,
            lower_band,
            upper_band,
            uncertainty_score,
            disagreement_score,
            model_spec_id,
            active_alpha_model_id,
            fallback_flag,
            top_reason_tags_json,
            risk_flags_json,
            session_status,
            checkpoint_plan_json,
            notes_json,
            created_at,
            updated_at
        )
        SELECT
            run_id,
            selection_date,
            session_date,
            symbol,
            market,
            company_name,
            horizon,
            ranking_version,
            candidate_rank,
            final_selection_value,
            final_selection_rank_pct,
            grade,
            eligible_flag,
            expected_excess_return,
            lower_band,
            upper_band,
            uncertainty_score,
            disagreement_score,
            model_spec_id,
            active_alpha_model_id,
            fallback_flag,
            top_reason_tags_json,
            risk_flags_json,
            session_status,
            checkpoint_plan_json,
            notes_json,
            created_at,
            updated_at
        FROM intraday_candidate_session_stage
        """
    )
    connection.unregister("intraday_candidate_session_stage")


def load_intraday_candidate_session_frame(
    connection,
    *,
    session_date: date,
    horizons: list[int] | None = None,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    symbols: list[str] | None = None,
    unique_symbols: bool = False,
) -> pd.DataFrame:
    where_clauses = ["session_date = ?", "ranking_version = ?"]
    params: list[object] = [session_date, ranking_version]
    if horizons:
        placeholders = ",".join("?" for _ in horizons)
        where_clauses.append(f"horizon IN ({placeholders})")
        params.extend(horizons)
    if symbols:
        normalized = [symbol.zfill(6) for symbol in symbols]
        placeholders = ",".join("?" for _ in normalized)
        where_clauses.append(f"symbol IN ({placeholders})")
        params.extend(normalized)
    frame = connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_candidate_session
        WHERE {" AND ".join(where_clauses)}
        ORDER BY horizon, candidate_rank, symbol
        """,
        params,
    ).fetchdf()
    if unique_symbols and not frame.empty:
        frame = frame.sort_values(["candidate_rank", "symbol"]).drop_duplicates("symbol")
    return frame.reset_index(drop=True)


def materialize_intraday_candidate_session(
    settings: Settings,
    *,
    selection_date: date,
    horizons: list[int],
    max_candidates: int = 30,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    market: str = "ALL",
    force: bool = False,
) -> IntradayCandidateSessionResult:
    ensure_storage_layout(settings)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        existing = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_ranking
            WHERE as_of_date = ?
              AND ranking_version = ?
            """,
            [selection_date, ranking_version],
        ).fetchone()[0]
    if int(existing or 0) == 0:
        materialize_selection_engine_v2(
            settings,
            as_of_date=selection_date,
            horizons=horizons,
            market=market,
            limit_symbols=max_candidates,
        )

    with activate_run_context(
        "materialize_intraday_candidate_session",
        as_of_date=selection_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=selection_date,
                input_sources=["fact_ranking", "fact_prediction", "dim_trading_calendar"],
                notes=(
                    "Materialize intraday candidate session from selection engine v2. "
                    f"selection_date={selection_date.isoformat()} horizons={horizons}"
                ),
                ranking_version=ranking_version,
            )
            try:
                session_date = _resolve_next_trading_day(connection, selection_date=selection_date)
                candidates = _load_selection_candidates(
                    connection,
                    selection_date=selection_date,
                    horizons=horizons,
                    max_candidates=max_candidates,
                    ranking_version=ranking_version,
                    market=market,
                )
                if candidates.empty:
                    raise RuntimeError(
                        "No selection_engine_v2 candidates were found "
                        "for intraday session materialization."
                    )

                if force:
                    connection.execute(
                        """
                        DELETE FROM fact_intraday_candidate_session
                        WHERE selection_date = ?
                          AND session_date = ?
                          AND ranking_version = ?
                        """,
                        [selection_date, session_date, ranking_version],
                    )

                now_ts = now_local(settings.app.timezone)
                today = today_local(settings.app.timezone)
                candidates["run_id"] = run_context.run_id
                candidates["session_date"] = session_date
                candidates["session_status"] = session_status(session_date, today=today)
                candidates["checkpoint_plan_json"] = json_text(list(DEFAULT_CHECKPOINTS))
                candidates["notes_json"] = candidates.apply(
                    lambda row: json_text(
                        {
                            "selection_engine_version": ranking_version,
                            "prediction_version": ALPHA_PREDICTION_VERSION,
                            "candidate_rank": int(row["candidate_rank"]),
                            "session_status": row["session_status"],
                            "model_spec_id": row.get("model_spec_id"),
                            "active_alpha_model_id": row.get("active_alpha_model_id"),
                        }
                    ),
                    axis=1,
                )
                candidates["created_at"] = now_ts
                candidates["updated_at"] = now_ts
                output = candidates[
                    [
                        "run_id",
                        "selection_date",
                        "session_date",
                        "symbol",
                        "market",
                        "company_name",
                        "horizon",
                        "ranking_version",
                        "candidate_rank",
                        "final_selection_value",
                        "final_selection_rank_pct",
                        "grade",
                        "eligible_flag",
                        "expected_excess_return",
                        "lower_band",
                        "upper_band",
                        "uncertainty_score",
                        "disagreement_score",
                        "model_spec_id",
                        "active_alpha_model_id",
                        "fallback_flag",
                        "top_reason_tags_json",
                        "risk_flags_json",
                        "session_status",
                        "checkpoint_plan_json",
                        "notes_json",
                        "created_at",
                        "updated_at",
                    ]
                ].copy()
                upsert_intraday_candidate_session(connection, output)

                artifact_paths: list[str] = []
                for (_, horizon), partition in output.groupby(
                    ["session_date", "horizon"],
                    sort=True,
                ):
                    artifact_paths.append(
                        str(
                            write_parquet(
                                partition,
                                base_dir=settings.paths.curated_dir,
                                dataset="intraday/candidate_session",
                                partitions={
                                    "session_date": session_date.isoformat(),
                                    "horizon": str(int(horizon)),
                                    "ranking_version": ranking_version,
                                },
                                filename="candidate_session.parquet",
                            )
                        )
                    )

                notes = (
                    "Intraday candidate session materialized. "
                    f"selection_date={selection_date.isoformat()} "
                    f"session_date={session_date.isoformat()} "
                    f"rows={len(output)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=ranking_version,
                )
                return IntradayCandidateSessionResult(
                    run_id=run_context.run_id,
                    selection_date=selection_date,
                    session_date=session_date,
                    row_count=len(output),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=(
                        "Intraday candidate session materialization failed. "
                        f"selection_date={selection_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
