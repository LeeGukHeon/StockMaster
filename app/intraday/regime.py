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

from .common import (
    clip_score,
    json_text,
    quality_bucket,
    rank_list,
    selection_confidence_bucket,
)
from .context import materialize_intraday_market_context_snapshots
from .pipeline import ensure_intraday_base_pipeline


@dataclass(slots=True)
class IntradayRegimeAdjustmentResult:
    run_id: str
    session_date: date
    checkpoints: list[str]
    row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_intraday_regime_adjustment(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_regime_adjustment_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_regime_adjustment
        WHERE (session_date, symbol, horizon, checkpoint_time, ranking_version) IN (
            SELECT session_date, symbol, horizon, checkpoint_time, ranking_version
            FROM intraday_regime_adjustment_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_regime_adjustment
        SELECT * FROM intraday_regime_adjustment_stage
        """
    )
    connection.unregister("intraday_regime_adjustment_stage")


def _load_context_frame(connection, *, session_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_market_context_snapshot
        WHERE session_date = ?
          AND context_scope = 'market'
        ORDER BY checkpoint_time
        """,
        [session_date],
    ).fetchdf()


def _load_adjustment_join(
    connection,
    *,
    session_date: date,
    checkpoints: list[str],
    horizons: list[int],
    ranking_version: str,
) -> pd.DataFrame:
    checkpoint_placeholders = ",".join("?" for _ in checkpoints)
    horizon_placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT
            candidate.selection_date,
            candidate.session_date,
            candidate.symbol,
            candidate.market,
            candidate.company_name,
            candidate.horizon,
            candidate.ranking_version,
            candidate.candidate_rank,
            candidate.final_selection_value,
            candidate.final_selection_rank_pct,
            candidate.grade,
            candidate.eligible_flag,
            candidate.expected_excess_return,
            candidate.uncertainty_score,
            candidate.disagreement_score,
            candidate.fallback_flag,
            signal.checkpoint_time,
            signal.gap_opening_quality_score,
            signal.micro_trend_score,
            signal.relative_activity_score,
            signal.orderbook_score,
            signal.execution_strength_score,
            signal.risk_friction_score,
            signal.signal_quality_score,
            signal.timing_adjustment_score,
            signal.signal_notes_json,
            signal.fallback_flags_json,
            decision.action AS raw_action,
            decision.action_score AS raw_timing_score,
            decision.action_reason_json,
            decision.risk_flags_json,
            context.market_session_state,
            context.prior_daily_regime_state,
            context.prior_daily_regime_score,
            context.market_breadth_ratio,
            context.candidate_mean_return_from_open,
            context.candidate_mean_relative_volume,
            context.candidate_mean_spread_bps,
            context.candidate_mean_execution_strength,
            context.candidate_mean_orderbook_imbalance,
            context.candidate_mean_gap_score,
            context.candidate_mean_signal_quality,
            context.market_shock_proxy,
            context.intraday_volatility_proxy,
            context.dispersion_proxy,
            context.bar_coverage_ratio,
            context.trade_coverage_ratio,
            context.quote_coverage_ratio,
            context.data_quality_flag,
            context.context_reason_codes_json
        FROM fact_intraday_candidate_session AS candidate
        JOIN fact_intraday_signal_snapshot AS signal
          ON candidate.session_date = signal.session_date
         AND candidate.symbol = signal.symbol
         AND candidate.horizon = signal.horizon
         AND candidate.ranking_version = signal.ranking_version
        LEFT JOIN fact_intraday_entry_decision AS decision
          ON signal.session_date = decision.session_date
         AND signal.symbol = decision.symbol
         AND signal.horizon = decision.horizon
         AND signal.checkpoint_time = decision.checkpoint_time
         AND signal.ranking_version = decision.ranking_version
        JOIN fact_intraday_market_context_snapshot AS context
          ON signal.session_date = context.session_date
         AND signal.checkpoint_time = context.checkpoint_time
         AND context.context_scope = 'market'
        WHERE candidate.session_date = ?
          AND candidate.ranking_version = ?
          AND signal.checkpoint_time IN ({checkpoint_placeholders})
          AND candidate.horizon IN ({horizon_placeholders})
        ORDER BY
            signal.checkpoint_time,
            candidate.horizon,
            candidate.candidate_rank,
            candidate.symbol
        """,
        [session_date, ranking_version, *checkpoints, *horizons],
    ).fetchdf()


def _parse_json_list(value: object) -> list[str]:
    if value in {None, ""}:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _classify_market_regime_family(row: pd.Series) -> str:
    data_quality_flag = str(row["data_quality_flag"] or "weak")
    breadth = 0.5 if pd.isna(row["market_breadth_ratio"]) else float(row["market_breadth_ratio"])
    candidate_return = (
        0.0
        if pd.isna(row["candidate_mean_return_from_open"])
        else float(row["candidate_mean_return_from_open"])
    )
    gap_score = (
        50.0 if pd.isna(row["candidate_mean_gap_score"]) else float(row["candidate_mean_gap_score"])
    )
    signal_quality = (
        50.0
        if pd.isna(row["candidate_mean_signal_quality"])
        else float(row["candidate_mean_signal_quality"])
    )
    shock_proxy = 50.0 if pd.isna(row["market_shock_proxy"]) else float(row["market_shock_proxy"])
    trade_cov = 0.0 if pd.isna(row["trade_coverage_ratio"]) else float(row["trade_coverage_ratio"])
    quote_cov = 0.0 if pd.isna(row["quote_coverage_ratio"]) else float(row["quote_coverage_ratio"])

    if data_quality_flag == "weak" or min(trade_cov, quote_cov) < 0.35:
        return "DATA_WEAK"
    if shock_proxy >= 78 or breadth <= 0.30 or candidate_return <= -0.02:
        return "PANIC_OPEN"
    if shock_proxy >= 63 or breadth < 0.46 or candidate_return < -0.005:
        return "WEAK_RISK_OFF"
    if candidate_return >= 0.012 and gap_score <= 42 and breadth >= 0.58:
        return "OVERHEATED_GAP_CHASE"
    if breadth >= 0.58 and candidate_return >= 0.004 and signal_quality >= 58:
        return "HEALTHY_TREND"
    return "NEUTRAL_CHOP"


def _profile_for_family(family: str) -> str:
    mapping = {
        "PANIC_OPEN": "DEFENSIVE",
        "WEAK_RISK_OFF": "DEFENSIVE",
        "NEUTRAL_CHOP": "NEUTRAL",
        "HEALTHY_TREND": "SELECTIVE_RISK_ON",
        "OVERHEATED_GAP_CHASE": "GAP_CHASE_GUARD",
        "DATA_WEAK": "DATA_WEAK_GUARD",
    }
    return mapping.get(family, "NEUTRAL")


def _confidence_gate(profile: str, bucket: str) -> bool:
    if profile in {"DEFENSIVE", "GAP_CHASE_GUARD", "DATA_WEAK_GUARD"}:
        return bucket in {"top", "high", "medium"}
    return bucket in {"top", "high", "medium", "low"}


def _build_adjustment_row(run_id: str, row: pd.Series) -> dict[str, object]:
    family = _classify_market_regime_family(row)
    profile = _profile_for_family(family)
    selection_bucket = selection_confidence_bucket(
        final_selection_value=row["final_selection_value"],
        percentile=row["final_selection_rank_pct"],
    )
    signal_quality_flag = quality_bucket(row["signal_quality_score"])
    raw_action = str(row["raw_action"] or "DATA_INSUFFICIENT")
    raw_score = clip_score(row["raw_timing_score"])

    reasons = _parse_json_list(row["context_reason_codes_json"])
    fallback_flags = _parse_json_list(row["fallback_flags_json"])
    regime_support_delta = 0.0
    regime_risk_penalty = 0.0
    gap_chase_penalty = 0.0
    data_quality_penalty = 0.0
    friction_penalty_delta = 0.0

    if family == "PANIC_OPEN":
        regime_risk_penalty += 14.0
        reasons.append("panic_open_guard")
    elif family == "WEAK_RISK_OFF":
        regime_risk_penalty += 8.0
        reasons.append("weak_risk_off_guard")
    elif family == "HEALTHY_TREND":
        regime_support_delta += 5.0 if selection_bucket in {"top", "high"} else 2.0
        reasons.append("healthy_trend_support")
    elif family == "OVERHEATED_GAP_CHASE":
        gap_chase_penalty += max(
            0.0, (55.0 - float(row["gap_opening_quality_score"] or 50.0)) * 0.45
        )
        reasons.append("gap_chase_guard")
    elif family == "DATA_WEAK":
        data_quality_penalty += 12.0
        reasons.append("data_weak_guard")

    if signal_quality_flag == "critical":
        data_quality_penalty += 10.0
        reasons.append("critical_signal_quality")
    elif signal_quality_flag == "low":
        data_quality_penalty += 4.0
        reasons.append("low_signal_quality")

    risk_score = 50.0 if pd.isna(row["risk_friction_score"]) else float(row["risk_friction_score"])
    if risk_score < 50:
        friction_penalty_delta += (50.0 - risk_score) * 0.40
        reasons.append("friction_penalty")
    if "quote_unavailable" in fallback_flags:
        data_quality_penalty += 6.0
        reasons.append("quote_unavailable")
    if "trade_unavailable" in fallback_flags:
        data_quality_penalty += 5.0
        reasons.append("trade_unavailable")
    if bool(row["fallback_flag"]):
        regime_risk_penalty += 5.0
        reasons.append("selection_fallback_penalty")
    if pd.notna(row["uncertainty_score"]) and float(row["uncertainty_score"]) >= 70:
        regime_risk_penalty += 4.0
        reasons.append("uncertainty_high")
    if pd.notna(row["disagreement_score"]) and float(row["disagreement_score"]) >= 75:
        regime_risk_penalty += 3.0
        reasons.append("disagreement_high")

    adjusted_score = clip_score(
        raw_score
        + regime_support_delta
        - regime_risk_penalty
        - gap_chase_penalty
        - data_quality_penalty
        - friction_penalty_delta
    )
    eligible = (
        signal_quality_flag != "critical"
        and _confidence_gate(profile, selection_bucket)
        and raw_action != "DATA_INSUFFICIENT"
    )
    net_delta = adjusted_score - raw_score
    notes = {
        "profile": profile,
        "family": family,
        "raw_action": raw_action,
        "selection_bucket": selection_bucket,
        "signal_quality_flag": signal_quality_flag,
    }
    return {
        "run_id": run_id,
        "selection_date": row["selection_date"],
        "session_date": row["session_date"],
        "symbol": row["symbol"],
        "horizon": int(row["horizon"]),
        "checkpoint_time": row["checkpoint_time"],
        "ranking_version": row["ranking_version"],
        "market_regime_family": family,
        "adjustment_profile": profile,
        "selection_confidence_bucket": selection_bucket,
        "signal_quality_flag": signal_quality_flag,
        "raw_action": raw_action,
        "raw_timing_score": raw_score,
        "adjusted_timing_score": adjusted_score,
        "regime_support_delta": regime_support_delta,
        "regime_risk_penalty": regime_risk_penalty,
        "gap_chase_penalty": gap_chase_penalty,
        "data_quality_penalty": data_quality_penalty,
        "friction_penalty_delta": friction_penalty_delta,
        "regime_adjustment_delta": net_delta,
        "eligible_to_execute_flag": eligible,
        "context_reason_codes_json": json_text(rank_list(reasons)),
        "adjustment_reason_codes_json": json_text(rank_list(reasons)),
        "notes_json": json_text(notes),
        "created_at": pd.Timestamp.now(tz="UTC"),
    }


def materialize_intraday_regime_adjustments(
    settings: Settings,
    *,
    session_date: date,
    checkpoints: list[str],
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayRegimeAdjustmentResult:
    ensure_storage_layout(settings)
    ensure_intraday_base_pipeline(
        settings,
        session_date=session_date,
        horizons=horizons,
        checkpoints=checkpoints,
        ranking_version=ranking_version,
    )
    materialize_intraday_market_context_snapshots(
        settings,
        session_date=session_date,
        checkpoints=checkpoints,
        ranking_version=ranking_version,
        horizons=horizons,
    )
    with activate_run_context(
        "materialize_intraday_regime_adjustments",
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
                    "fact_intraday_market_context_snapshot",
                    "fact_intraday_candidate_session",
                    "fact_intraday_signal_snapshot",
                    "fact_intraday_entry_decision",
                ],
                notes=(
                    "Materialize intraday regime adjustments for "
                    f"{session_date.isoformat()} checkpoints={checkpoints}"
                ),
                ranking_version=ranking_version,
            )
            try:
                joined = _load_adjustment_join(
                    connection,
                    session_date=session_date,
                    checkpoints=checkpoints,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                if joined.empty:
                    notes = (
                        "No intraday rows were available for regime adjustment "
                        f"on {session_date.isoformat()}."
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
                    return IntradayRegimeAdjustmentResult(
                        run_id=run_context.run_id,
                        session_date=session_date,
                        checkpoints=checkpoints,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                rows = [
                    _build_adjustment_row(run_context.run_id, row) for _, row in joined.iterrows()
                ]
                output = pd.DataFrame(rows)
                upsert_intraday_regime_adjustment(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/regime_adjustment",
                            partitions={"session_date": session_date.isoformat()},
                            filename="regime_adjustment.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday regime adjustments materialized. "
                    f"session_date={session_date.isoformat()} rows={len(output)}"
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
                return IntradayRegimeAdjustmentResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    checkpoints=checkpoints,
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
                        "Intraday regime adjustment materialization failed for "
                        f"{session_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
