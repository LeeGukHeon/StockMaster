from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from .common import clip_score, rank_list


@dataclass(slots=True)
class IntradayDecisionResult:
    run_id: str
    session_date: date
    checkpoint: str
    row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_intraday_entry_decision(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_entry_decision_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_entry_decision
        WHERE (session_date, symbol, horizon, checkpoint_time, ranking_version) IN (
            SELECT session_date, symbol, horizon, checkpoint_time, ranking_version
            FROM intraday_entry_decision_stage
        )
        """
    )
    connection.execute(
        "INSERT INTO fact_intraday_entry_decision SELECT * FROM intraday_entry_decision_stage"
    )
    connection.unregister("intraday_entry_decision_stage")


def _choose_action(row: pd.Series, *, checkpoint: str) -> tuple[str, list[str], list[str]]:
    reasons: list[str] = []
    risks: list[str] = []
    signal_quality = float(row["signal_quality_score"])
    timing_score = float(row["timing_adjustment_score"])
    action_score = clip_score(timing_score * 0.72 + float(row["final_selection_value"]) * 0.28)

    fallback_flags = []
    if row.get("fallback_flags_json"):
        try:
            fallback_flags = json.loads(str(row["fallback_flags_json"]))
        except json.JSONDecodeError:
            fallback_flags = []

    if signal_quality < 40 or pd.isna(row.get("entry_reference_price")):
        reasons.append("signal_quality_too_low")
        risks.append("data_insufficient")
        return "DATA_INSUFFICIENT", reasons, risks

    if row["gap_opening_quality_score"] < 25:
        reasons.append("opening_gap_unfavorable")
        risks.append("opening_shock")
    if row["risk_friction_score"] < 32:
        reasons.append("risk_friction_too_high")
        risks.append("friction_high")
    if (
        row["uncertainty_score"]
        and pd.notna(row["uncertainty_score"])
        and float(row["uncertainty_score"]) >= 70
    ):
        risks.append("selection_uncertainty_high")
    if row["fallback_flag"]:
        risks.append("selection_fallback")
    if "quote_unavailable" in fallback_flags:
        risks.append("quote_missing")
    if "trade_unavailable" in fallback_flags:
        risks.append("trade_missing")

    if (
        row["gap_opening_quality_score"] < 25
        or row["risk_friction_score"] < 32
        or action_score < 35
    ):
        return "AVOID_TODAY", rank_list(reasons + ["avoid_by_risk_rule"]), rank_list(risks)

    enter_conditions = [
        action_score >= 65,
        row["micro_trend_score"] >= 55,
        row["execution_strength_score"] >= 55,
        row["risk_friction_score"] >= 45,
    ]
    if all(enter_conditions):
        return "ENTER_NOW", rank_list(["momentum_confirmed", "timing_supportive"]), rank_list(risks)

    if checkpoint == "11:00":
        if action_score >= 58 and row["micro_trend_score"] >= 50:
            return "ENTER_NOW", rank_list(["late_checkpoint_entry"]), rank_list(risks)
        return "AVOID_TODAY", rank_list(["final_checkpoint_not_strong_enough"]), rank_list(risks)

    return "WAIT_RECHECK", rank_list(["checkpoint_recheck_needed"]), rank_list(risks)


def materialize_intraday_entry_decisions(
    settings: Settings,
    *,
    session_date: date,
    checkpoint: str,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayDecisionResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "materialize_intraday_entry_decisions",
        as_of_date=session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=session_date,
                input_sources=[
                    "fact_intraday_signal_snapshot",
                    "fact_intraday_candidate_session",
                    "fact_intraday_bar_1m",
                ],
                notes=(
                    "Materialize intraday entry decisions for "
                    f"{session_date.isoformat()} {checkpoint}"
                ),
                ranking_version=ranking_version,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                joined = connection.execute(
                    f"""
                    SELECT
                        signal.*,
                        candidate.final_selection_value,
                        candidate.uncertainty_score,
                        candidate.disagreement_score,
                        candidate.fallback_flag,
                        bar.close AS entry_reference_price
                    FROM fact_intraday_signal_snapshot AS signal
                    JOIN fact_intraday_candidate_session AS candidate
                      ON signal.session_date = candidate.session_date
                     AND signal.symbol = candidate.symbol
                     AND signal.horizon = candidate.horizon
                     AND signal.ranking_version = candidate.ranking_version
                    LEFT JOIN fact_intraday_bar_1m AS bar
                      ON signal.session_date = bar.session_date
                     AND signal.symbol = bar.symbol
                     AND bar.bar_time = ?
                    WHERE signal.session_date = ?
                      AND signal.checkpoint_time = ?
                      AND signal.ranking_version = ?
                      AND signal.horizon IN ({placeholders})
                    ORDER BY signal.horizon, candidate.candidate_rank, signal.symbol
                    """,
                    [
                        checkpoint.replace(":", ""),
                        session_date,
                        checkpoint,
                        ranking_version,
                        *horizons,
                    ],
                ).fetchdf()
                if joined.empty:
                    raise RuntimeError(
                        "No intraday signal snapshots were found. "
                        "Run scripts/materialize_intraday_signal_snapshots.py first."
                    )

                rows: list[dict[str, object]] = []
                now_ts = pd.Timestamp.now(tz="UTC")
                for _, row in joined.iterrows():
                    action, reasons, risks = _choose_action(row, checkpoint=checkpoint)
                    action_score = clip_score(
                        float(row["timing_adjustment_score"]) * 0.72
                        + float(row["final_selection_value"]) * 0.28
                    )
                    rows.append(
                        {
                            "run_id": run_context.run_id,
                            "session_date": session_date,
                            "symbol": row["symbol"],
                            "horizon": int(row["horizon"]),
                            "checkpoint_time": checkpoint,
                            "ranking_version": ranking_version,
                            "action": action,
                            "action_score": action_score,
                            "timing_adjustment_score": float(row["timing_adjustment_score"]),
                            "signal_quality_score": float(row["signal_quality_score"]),
                            "entry_reference_price": row["entry_reference_price"],
                            "fallback_flag": bool(row["fallback_flag"])
                            or action == "DATA_INSUFFICIENT",
                            "action_reason_json": json.dumps(reasons, ensure_ascii=False),
                            "risk_flags_json": json.dumps(rank_list(risks), ensure_ascii=False),
                            "notes_json": json.dumps(
                                {
                                    "gap_score": float(row["gap_opening_quality_score"]),
                                    "micro_trend_score": float(row["micro_trend_score"]),
                                    "relative_activity_score": float(
                                        row["relative_activity_score"]
                                    ),
                                    "orderbook_score": float(row["orderbook_score"]),
                                    "execution_strength_score": float(
                                        row["execution_strength_score"]
                                    ),
                                    "risk_friction_score": float(row["risk_friction_score"]),
                                },
                                ensure_ascii=False,
                            ),
                            "created_at": now_ts,
                        }
                    )
                output = pd.DataFrame(rows)
                upsert_intraday_entry_decision(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/entry_decision",
                            partitions={
                                "session_date": session_date.isoformat(),
                                "checkpoint": checkpoint.replace(":", ""),
                            },
                            filename="entry_decision.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday entry decisions materialized. "
                    f"session_date={session_date.isoformat()} "
                    f"checkpoint={checkpoint} rows={len(output)}"
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
                return IntradayDecisionResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    checkpoint=checkpoint,
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
                        "Intraday entry decision materialization failed for "
                        f"{session_date.isoformat()} {checkpoint}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
