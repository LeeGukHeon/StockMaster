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

from .common import json_text, rank_list
from .regime import materialize_intraday_regime_adjustments


@dataclass(slots=True)
class IntradayAdjustedDecisionResult:
    run_id: str
    session_date: date
    checkpoint: str
    row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_intraday_adjusted_entry_decision(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_adjusted_entry_decision_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_adjusted_entry_decision
        WHERE (session_date, symbol, horizon, checkpoint_time, ranking_version) IN (
            SELECT session_date, symbol, horizon, checkpoint_time, ranking_version
            FROM intraday_adjusted_entry_decision_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_adjusted_entry_decision
        SELECT * FROM intraday_adjusted_entry_decision_stage
        """
    )
    connection.unregister("intraday_adjusted_entry_decision_stage")


def _load_adjusted_join(
    connection,
    *,
    session_date: date,
    checkpoint: str,
    horizons: list[int],
    ranking_version: str,
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT
            adjust.*,
            raw.action_reason_json,
            raw.risk_flags_json,
            raw.fallback_flag AS raw_fallback_flag,
            raw.notes_json AS raw_notes_json,
            candidate.fallback_flag AS selection_fallback_flag,
            candidate.top_reason_tags_json,
            candidate.risk_flags_json AS selection_risk_flags_json
        FROM fact_intraday_regime_adjustment AS adjust
        JOIN fact_intraday_entry_decision AS raw
          ON adjust.session_date = raw.session_date
         AND adjust.symbol = raw.symbol
         AND adjust.horizon = raw.horizon
         AND adjust.checkpoint_time = raw.checkpoint_time
         AND adjust.ranking_version = raw.ranking_version
        JOIN fact_intraday_candidate_session AS candidate
          ON adjust.session_date = candidate.session_date
         AND adjust.symbol = candidate.symbol
         AND adjust.horizon = candidate.horizon
         AND adjust.ranking_version = candidate.ranking_version
        WHERE adjust.session_date = ?
          AND adjust.checkpoint_time = ?
          AND adjust.ranking_version = ?
          AND adjust.horizon IN ({placeholders})
        ORDER BY adjust.horizon, candidate.candidate_rank, adjust.symbol
        """,
        [session_date, checkpoint, ranking_version, *horizons],
    ).fetchdf()


def _profile_thresholds(profile: str) -> dict[str, object]:
    defaults = {
        "enter": 68.0,
        "avoid": 36.0,
        "min_signal": {"low", "medium", "high"},
        "allow_enter": True,
    }
    profiles = {
        "DEFENSIVE": {
            "enter": 78.0,
            "avoid": 42.0,
            "min_signal": {"medium", "high"},
            "allow_enter": True,
        },
        "NEUTRAL": defaults,
        "SELECTIVE_RISK_ON": {
            "enter": 62.0,
            "avoid": 32.0,
            "min_signal": {"low", "medium", "high"},
            "allow_enter": True,
        },
        "GAP_CHASE_GUARD": {
            "enter": 74.0,
            "avoid": 40.0,
            "min_signal": {"medium", "high"},
            "allow_enter": True,
        },
        "DATA_WEAK_GUARD": {
            "enter": 999.0,
            "avoid": 44.0,
            "min_signal": {"high"},
            "allow_enter": False,
        },
    }
    return profiles.get(profile, defaults)


def _normalize_reason_list(value: object) -> list[str]:
    if value in {None, ""}:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _decide_adjusted_action(row: pd.Series) -> tuple[str, list[str], bool]:
    raw_action = str(row["raw_action"] or "DATA_INSUFFICIENT")
    profile = str(row["adjustment_profile"] or "NEUTRAL")
    signal_quality_flag = str(row["signal_quality_flag"] or "low")
    adjusted_score = float(row["adjusted_timing_score"] or 0.0)
    thresholds = _profile_thresholds(profile)
    reasons = _normalize_reason_list(row["adjustment_reason_codes_json"])
    eligible = bool(row["eligible_to_execute_flag"])

    if raw_action == "DATA_INSUFFICIENT":
        return "DATA_INSUFFICIENT", rank_list(reasons + ["raw_data_insufficient_locked"]), False
    if raw_action == "AVOID_TODAY":
        return "AVOID_TODAY", rank_list(reasons + ["raw_avoid_preserved"]), False
    if signal_quality_flag == "critical":
        return "DATA_INSUFFICIENT", rank_list(reasons + ["critical_signal_quality_guard"]), False
    if signal_quality_flag not in thresholds["min_signal"]:
        if raw_action == "ENTER_NOW":
            return "WAIT_RECHECK", rank_list(reasons + ["signal_quality_requires_recheck"]), False
        return "WAIT_RECHECK", rank_list(reasons + ["signal_quality_guard"]), False
    if not eligible:
        return "WAIT_RECHECK", rank_list(reasons + ["eligibility_gate_block"]), False
    if adjusted_score <= float(thresholds["avoid"]):
        return "AVOID_TODAY", rank_list(reasons + ["adjusted_score_below_avoid"]), False
    if not bool(thresholds["allow_enter"]):
        return "WAIT_RECHECK", rank_list(reasons + ["profile_blocks_enter"]), False
    if adjusted_score >= float(thresholds["enter"]):
        return "ENTER_NOW", rank_list(reasons + ["adjusted_enter_threshold_hit"]), True
    if raw_action == "ENTER_NOW" and adjusted_score >= float(thresholds["avoid"]) + 8.0:
        return "WAIT_RECHECK", rank_list(reasons + ["enter_downgraded_to_wait"]), False
    return "WAIT_RECHECK", rank_list(reasons + ["adjusted_wait_zone"]), False


def materialize_intraday_adjusted_entry_decisions(
    settings: Settings,
    *,
    session_date: date,
    checkpoint: str,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayAdjustedDecisionResult:
    ensure_storage_layout(settings)
    materialize_intraday_regime_adjustments(
        settings,
        session_date=session_date,
        checkpoints=[checkpoint],
        horizons=horizons,
        ranking_version=ranking_version,
    )
    with activate_run_context(
        "materialize_intraday_adjusted_entry_decisions",
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
                    "fact_intraday_regime_adjustment",
                    "fact_intraday_entry_decision",
                    "fact_intraday_candidate_session",
                ],
                notes=(
                    "Materialize adjusted intraday entry decisions for "
                    f"{session_date.isoformat()} {checkpoint}"
                ),
                ranking_version=ranking_version,
            )
            try:
                joined = _load_adjusted_join(
                    connection,
                    session_date=session_date,
                    checkpoint=checkpoint,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                if joined.empty:
                    notes = (
                        "No regime adjustment rows were available for adjusted decisions "
                        f"on {session_date.isoformat()} {checkpoint}."
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=ranking_version,
                    )
                    return IntradayAdjustedDecisionResult(
                        run_id=run_context.run_id,
                        session_date=session_date,
                        checkpoint=checkpoint,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                rows: list[dict[str, object]] = []
                now_ts = pd.Timestamp.now(tz="UTC")
                for _, row in joined.iterrows():
                    adjusted_action, reasons, eligible = _decide_adjusted_action(row)
                    risk_flags = rank_list(
                        _normalize_reason_list(row["risk_flags_json"])
                        + _normalize_reason_list(row["selection_risk_flags_json"])
                    )
                    rows.append(
                        {
                            "run_id": run_context.run_id,
                            "selection_date": row["selection_date"],
                            "session_date": row["session_date"],
                            "symbol": row["symbol"],
                            "horizon": int(row["horizon"]),
                            "checkpoint_time": row["checkpoint_time"],
                            "ranking_version": row["ranking_version"],
                            "market_regime_family": row["market_regime_family"],
                            "adjustment_profile": row["adjustment_profile"],
                            "raw_action": row["raw_action"],
                            "adjusted_action": adjusted_action,
                            "raw_timing_score": float(row["raw_timing_score"] or 0.0),
                            "adjusted_timing_score": float(row["adjusted_timing_score"] or 0.0),
                            "selection_confidence_bucket": row["selection_confidence_bucket"],
                            "signal_quality_flag": row["signal_quality_flag"],
                            "eligible_to_execute_flag": eligible,
                            "fallback_flag": bool(row["raw_fallback_flag"])
                            or bool(row["selection_fallback_flag"])
                            or adjusted_action == "DATA_INSUFFICIENT",
                            "adjustment_reason_codes_json": json_text(reasons),
                            "risk_flags_json": json_text(risk_flags),
                            "decision_notes_json": json_text(
                                {
                                    "raw_notes_json": row["raw_notes_json"],
                                    "top_reason_tags_json": row["top_reason_tags_json"],
                                    "profile": row["adjustment_profile"],
                                }
                            ),
                            "created_at": now_ts,
                        }
                    )
                output = pd.DataFrame(rows)
                upsert_intraday_adjusted_entry_decision(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/adjusted_entry_decision",
                            partitions={
                                "session_date": session_date.isoformat(),
                                "checkpoint": checkpoint.replace(":", ""),
                            },
                            filename="adjusted_entry_decision.parquet",
                        )
                    )
                ]
                notes = (
                    "Adjusted intraday entry decisions materialized. "
                    f"session_date={session_date.isoformat()} checkpoint={checkpoint} "
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
                return IntradayAdjustedDecisionResult(
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
                        "Adjusted intraday entry decision materialization failed for "
                        f"{session_date.isoformat()} {checkpoint}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
