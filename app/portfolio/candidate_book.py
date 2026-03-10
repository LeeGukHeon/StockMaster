# ruff: noqa: E501

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.feature_store import load_feature_matrix
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from .common import (
    CANDIDATE_STATES,
    EXECUTION_MODES,
    PortfolioCandidateBookResult,
    PortfolioValidationResult,
    json_text,
    load_active_or_default_portfolio_policy,
    normalize_decimal,
    normalize_score_100,
    ordered_frame,
)


@dataclass(slots=True)
class _PortfolioContext:
    policy_id: str
    policy_version: str
    active_policy_id: str | None
    config_path: str | None
    session_date: date
    regime_state: str
    regime_cash_target: float


def upsert_portfolio_candidate(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    frame = frame.drop_duplicates(subset=["as_of_date", "execution_mode", "symbol"], keep="last")
    connection.register("portfolio_candidate_stage", frame)
    connection.execute(
        """
        INSERT OR REPLACE INTO fact_portfolio_candidate
        SELECT * FROM portfolio_candidate_stage
        """
    )
    connection.unregister("portfolio_candidate_stage")


def _next_trading_day(connection, *, as_of_date: date) -> date:
    row = connection.execute(
        """
        SELECT MIN(trading_date)
        FROM dim_trading_calendar
        WHERE trading_date > ?
          AND is_trading_day = TRUE
        """,
        [as_of_date],
    ).fetchone()
    if row and row[0]:
        return pd.Timestamp(row[0]).date()
    return as_of_date


def _load_regime_context(connection, *, as_of_date: date) -> tuple[str, float]:
    frame = connection.execute(
        """
        SELECT regime_state, regime_score
        FROM fact_market_regime_snapshot
        WHERE as_of_date = ?
        QUALIFY ROW_NUMBER() OVER (
            ORDER BY CASE WHEN market_scope = 'KR_ALL' THEN 0 ELSE 1 END, created_at DESC
        ) = 1
        """,
        [as_of_date],
    ).fetchdf()
    if frame.empty:
        return "neutral", 0.0
    row = frame.iloc[0]
    return str(row.get("regime_state") or "neutral"), float(row.get("regime_score") or 0.0)


def _load_previous_positions(
    connection,
    *,
    session_date: date,
    execution_mode: str,
    policy_id: str,
    policy_version: str,
) -> pd.DataFrame:
    snapshot_date_row = connection.execute(
        """
        SELECT MAX(snapshot_date)
        FROM fact_portfolio_position_snapshot
        WHERE snapshot_date < ?
          AND execution_mode = ?
          AND portfolio_policy_id = ?
          AND portfolio_policy_version = ?
        """,
        [session_date, execution_mode, policy_id, policy_version],
    ).fetchone()
    if not snapshot_date_row or snapshot_date_row[0] is None:
        return pd.DataFrame(columns=["symbol", "current_shares", "current_weight"])
    snapshot_date = pd.Timestamp(snapshot_date_row[0]).date()
    return connection.execute(
        """
        SELECT
            symbol,
            COALESCE(shares, 0) AS current_shares,
            COALESCE(actual_weight, 0) AS current_weight
        FROM fact_portfolio_position_snapshot
        WHERE snapshot_date = ?
          AND execution_mode = ?
          AND portfolio_policy_id = ?
          AND portfolio_policy_version = ?
          AND COALESCE(cash_like_flag, FALSE) = FALSE
        """,
        [snapshot_date, execution_mode, policy_id, policy_version],
    ).fetchdf()


def _load_latest_timing_action(
    connection,
    *,
    session_date: date,
    horizon: int,
) -> pd.DataFrame:
    meta_frame = connection.execute(
        """
        SELECT
            symbol,
            final_action AS timing_action,
            confidence_margin,
            uncertainty_score,
            disagreement_score,
            fallback_flag,
            fallback_reason
        FROM fact_intraday_meta_decision
        WHERE session_date = ?
          AND horizon = ?
          AND ranking_version = ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY checkpoint_time DESC, created_at DESC
        ) = 1
        """,
        [session_date, horizon, SELECTION_ENGINE_VERSION],
    ).fetchdf()
    if not meta_frame.empty:
        return meta_frame
    return connection.execute(
        """
        SELECT
            symbol,
            adjusted_action AS timing_action,
            NULL AS confidence_margin,
            NULL AS uncertainty_score,
            NULL AS disagreement_score,
            fallback_flag,
            NULL AS fallback_reason
        FROM fact_intraday_adjusted_entry_decision
        WHERE session_date = ?
          AND horizon = ?
          AND ranking_version = ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY checkpoint_time DESC, created_at DESC
        ) = 1
        """,
        [session_date, horizon, SELECTION_ENGINE_VERSION],
    ).fetchdf()


def _load_ranking_prediction_frame(
    connection,
    *,
    as_of_date: date,
    primary_horizon: int,
    tactical_horizon: int,
    symbols: list[str] | None,
    limit_symbols: int | None,
) -> pd.DataFrame:
    feature_matrix = load_feature_matrix(
        connection,
        as_of_date=as_of_date,
        symbols=symbols,
        limit_symbols=limit_symbols,
        market="ALL",
    )
    if feature_matrix.empty:
        return feature_matrix
    ranking = connection.execute(
        """
        SELECT
            symbol,
            final_selection_value,
            final_selection_rank_pct,
            grade,
            eligible_flag,
            explanatory_score_json,
            top_reason_tags_json,
            risk_flags_json
        FROM fact_ranking
        WHERE as_of_date = ?
          AND horizon = ?
          AND ranking_version = ?
        """,
        [as_of_date, primary_horizon, SELECTION_ENGINE_VERSION],
    ).fetchdf()
    primary_prediction = connection.execute(
        """
        SELECT
            symbol,
            expected_excess_return,
            lower_band,
            upper_band,
            uncertainty_score,
            disagreement_score,
            fallback_flag,
            fallback_reason,
            prediction_version
        FROM fact_prediction
        WHERE as_of_date = ?
          AND horizon = ?
          AND ranking_version = ?
          AND prediction_version = ?
        """,
        [
            as_of_date,
            primary_horizon,
            SELECTION_ENGINE_VERSION,
            ALPHA_PREDICTION_VERSION,
        ],
    ).fetchdf()
    tactical_prediction = connection.execute(
        """
        SELECT
            symbol,
            expected_excess_return AS tactical_expected_excess_return,
            lower_band AS tactical_lower_band
        FROM fact_prediction
        WHERE as_of_date = ?
          AND horizon = ?
          AND ranking_version = ?
          AND prediction_version = ?
        """,
        [
            as_of_date,
            tactical_horizon,
            SELECTION_ENGINE_VERSION,
            ALPHA_PREDICTION_VERSION,
        ],
    ).fetchdf()
    symbols_dim = connection.execute(
        """
        SELECT symbol, company_name, market, COALESCE(sector, '미분류') AS sector
        FROM dim_symbol
        """
    ).fetchdf()
    frame = (
        feature_matrix.merge(symbols_dim, on="symbol", how="left", suffixes=("", "_dim"))
        .merge(ranking, on="symbol", how="left")
        .merge(primary_prediction, on="symbol", how="left")
        .merge(tactical_prediction, on="symbol", how="left")
    )
    frame["company_name"] = frame["company_name_dim"].combine_first(frame["company_name"])
    frame["market"] = frame["market_dim"].combine_first(frame["market"])
    payloads = frame["explanatory_score_json"].map(
        lambda value: json.loads(value) if isinstance(value, str) and value else {}
    )
    frame["flow_score"] = payloads.map(lambda payload: payload.get("flow_score")).combine_first(
        pd.to_numeric(frame.get("flow_alignment_score"), errors="coerce")
    )
    frame["regime_fit_score"] = payloads.map(
        lambda payload: payload.get("regime_fit_score")
    ).fillna(50.0)
    frame["implementation_penalty_score"] = payloads.map(
        lambda payload: payload.get("implementation_penalty_score")
    ).fillna(50.0)
    return frame.drop(
        columns=[column for column in ["company_name_dim", "market_dim"] if column in frame.columns]
    )


def _effective_alpha_long(row: pd.Series, *, current_holding_bonus: float) -> float:
    primary_alpha = normalize_decimal(row.get("expected_excess_return"))
    tactical_alpha = normalize_decimal(row.get("tactical_expected_excess_return"))
    lower_band = normalize_decimal(row.get("lower_band"))
    flow_support = (normalize_score_100(row.get("flow_score")) - 50.0) / 5000.0
    regime_support = (normalize_score_100(row.get("regime_fit_score")) - 50.0) / 6000.0
    confidence_bonus = (normalize_score_100(row.get("data_confidence_score")) - 50.0) / 8000.0
    uncertainty_penalty = normalize_score_100(row.get("uncertainty_score")) / 5000.0
    disagreement_penalty = normalize_score_100(row.get("disagreement_score")) / 7000.0
    implementation_penalty = (
        normalize_score_100(row.get("implementation_penalty_score")) / 4500.0
    )
    fallback_penalty = 0.0025 if bool(row.get("fallback_flag")) else 0.0
    holding_bonus = current_holding_bonus if bool(row.get("current_holding_flag")) else 0.0
    return (
        primary_alpha
        + 0.35 * tactical_alpha
        + 0.25 * lower_band
        + flow_support
        + regime_support
        + confidence_bonus
        + holding_bonus
        - uncertainty_penalty
        - disagreement_penalty
        - implementation_penalty
        - fallback_penalty
    )


def _candidate_state(
    row: pd.Series,
    *,
    entry_score_floor: float,
    hold_score_floor: float,
    hard_exit_score_floor: float,
    lower_band_floor: float,
) -> tuple[str, bool, bool, bool, str | None]:
    lower_band = normalize_decimal(row.get("lower_band"))
    effective_alpha = float(row.get("effective_alpha_long") or 0.0)
    adv20 = float(pd.to_numeric(row.get("adv_20"), errors="coerce") or 0.0)
    blocked_reason: str | None = None
    hard_exit = bool(lower_band <= lower_band_floor or effective_alpha <= hard_exit_score_floor)
    entry_eligible = bool(
        row.get("eligible_flag")
        and adv20 > 0
        and effective_alpha >= entry_score_floor
        and not hard_exit
    )
    hold_eligible = bool(
        row.get("eligible_flag")
        and adv20 > 0
        and effective_alpha >= hold_score_floor
        and not hard_exit
    )
    if adv20 <= 0:
        blocked_reason = "adv20_missing"
    if bool(row.get("current_holding_flag")):
        if hard_exit:
            return "EXIT_CANDIDATE", False, False, True, blocked_reason
        if hold_eligible:
            return "HOLD_CANDIDATE", False, True, False, blocked_reason
        return "TRIM_CANDIDATE", False, False, False, blocked_reason
    if blocked_reason is not None:
        return "BLOCKED", False, False, hard_exit, blocked_reason
    if entry_eligible:
        return "NEW_ENTRY_CANDIDATE", True, False, False, None
    if hard_exit:
        return "BLOCKED", False, False, True, "hard_exit_rule"
    return "WATCH_ONLY", False, False, False, None


def _timing_gate(mode: str, timing_action: object) -> str:
    if mode == "OPEN_ALL":
        return "OPEN_ALL"
    if pd.isna(timing_action) or timing_action in {None, ""}:
        return "TIMING_UNAVAILABLE"
    action = str(timing_action)
    if action == "ENTER_NOW":
        return "ENTER_ALLOWED"
    if action == "WAIT_RECHECK":
        return "WAIT_GATE"
    if action in {"AVOID_TODAY", "DATA_INSUFFICIENT"}:
        return "BLOCKED_BY_TIMING"
    return "TIMING_UNAVAILABLE"


def _tie_breaker_payload(row: pd.Series) -> dict[str, object]:
    return {
        "risk_scaled_conviction": None
        if pd.isna(row.get("risk_scaled_conviction"))
        else round(float(row["risk_scaled_conviction"]), 8),
        "current_holding_flag": bool(row.get("current_holding_flag")),
        "final_selection_value": None
        if pd.isna(row.get("final_selection_value"))
        else round(float(row["final_selection_value"]), 8),
        "expected_excess_return": None
        if pd.isna(row.get("expected_excess_return"))
        else round(float(row["expected_excess_return"]), 8),
        "symbol": str(row["symbol"]),
    }


def _rank_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    ranked = frame.sort_values(
        by=[
            "risk_scaled_conviction",
            "current_holding_flag",
            "final_selection_value",
            "expected_excess_return",
            "symbol",
        ],
        ascending=[False, False, False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    ranked["candidate_rank"] = range(1, len(ranked) + 1)
    return ranked


def build_portfolio_candidate_book(
    settings: Settings,
    *,
    as_of_date: date,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    execution_modes: list[str] | None = None,
    policy_config_path: str | None = None,
) -> PortfolioCandidateBookResult:
    ensure_storage_layout(settings)
    with activate_run_context("build_portfolio_candidate_book", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=[
                    "fact_ranking",
                    "fact_prediction",
                    "fact_feature_snapshot",
                    "fact_intraday_meta_decision",
                    "fact_portfolio_position_snapshot",
                ],
                notes=f"Build portfolio candidate book for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                policy, active_policy_id, config_path = load_active_or_default_portfolio_policy(
                    settings,
                    connection,
                    as_of_date=as_of_date,
                    policy_config_path=policy_config_path,
                )
                session_date = _next_trading_day(connection, as_of_date=as_of_date)
                regime_state, _ = _load_regime_context(connection, as_of_date=as_of_date)
                context = _PortfolioContext(
                    policy_id=policy.portfolio_policy_id,
                    policy_version=policy.portfolio_policy_version,
                    active_policy_id=active_policy_id,
                    config_path=config_path,
                    session_date=session_date,
                    regime_state=regime_state,
                    regime_cash_target=policy.regime_cash_target(regime_state),
                )
                base = _load_ranking_prediction_frame(
                    connection,
                    as_of_date=as_of_date,
                    primary_horizon=policy.primary_horizon,
                    tactical_horizon=policy.tactical_horizon,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                )
                if base.empty:
                    raise RuntimeError(
                        "No ranking/feature rows available for portfolio candidate assembly."
                    )
                requested_modes = [str(mode).upper() for mode in (execution_modes or policy.execution_modes)]
                requested_modes = [mode for mode in requested_modes if mode in EXECUTION_MODES]
                if not requested_modes:
                    requested_modes = list(EXECUTION_MODES)

                timing_actions = _load_latest_timing_action(
                    connection,
                    session_date=session_date,
                    horizon=policy.primary_horizon,
                )
                if timing_actions.empty:
                    timing_actions = pd.DataFrame(
                        columns=[
                            "symbol",
                            "timing_action",
                            "confidence_margin",
                            "uncertainty_score",
                            "disagreement_score",
                            "fallback_flag",
                            "fallback_reason",
                        ]
                    )
                else:
                    timing_actions = timing_actions.rename(
                        columns={
                            "confidence_margin": "timing_confidence_margin",
                            "uncertainty_score": "timing_uncertainty_score",
                            "disagreement_score": "timing_disagreement_score",
                            "fallback_flag": "timing_fallback_flag",
                            "fallback_reason": "timing_fallback_reason",
                        }
                    )

                frames: list[pd.DataFrame] = []
                artifact_paths: list[str] = []
                for mode in requested_modes:
                    mode_frame = base.copy()
                    previous_positions = _load_previous_positions(
                        connection,
                        session_date=session_date,
                        execution_mode=mode,
                        policy_id=policy.portfolio_policy_id,
                        policy_version=policy.portfolio_policy_version,
                    )
                    mode_frame = mode_frame.merge(previous_positions, on="symbol", how="left")
                    mode_frame["current_shares"] = (
                        pd.to_numeric(mode_frame["current_shares"], errors="coerce")
                        .fillna(0)
                        .astype(int)
                    )
                    mode_frame["current_weight"] = pd.to_numeric(
                        mode_frame["current_weight"], errors="coerce"
                    ).fillna(0.0)
                    mode_frame["current_holding_flag"] = mode_frame["current_shares"] > 0
                    mode_frame = mode_frame.merge(timing_actions, on="symbol", how="left")
                    mode_frame["timing_gate_status"] = mode_frame["timing_action"].map(
                        lambda value, mode=mode: _timing_gate(mode, value)
                    )
                    mode_frame["effective_alpha_long"] = mode_frame.apply(
                        lambda row: _effective_alpha_long(
                            row,
                            current_holding_bonus=policy.current_holding_bonus,
                        ),
                        axis=1,
                    )
                    mode_frame["volatility_proxy"] = pd.to_numeric(
                        mode_frame.get("realized_vol_20d"), errors="coerce"
                    ).fillna(policy.vol_floor)
                    mode_frame["adv20_krw"] = pd.to_numeric(
                        mode_frame.get("adv_20"), errors="coerce"
                    ).fillna(0.0)
                    mode_frame["risk_scaled_conviction"] = (
                        mode_frame["effective_alpha_long"]
                        / mode_frame["volatility_proxy"].clip(lower=policy.vol_floor)
                    )
                    hold_floor = min(
                        policy.entry_score_floor,
                        policy.hold_score_floor + (policy.entry_score_floor * policy.hold_hysteresis),
                    )
                    states = mode_frame.apply(
                        lambda row,
                        hold_score_floor=hold_floor,
                        hard_exit_score_floor=policy.hard_exit_score_floor,
                        lower_band_floor=policy.lower_band_floor: _candidate_state(
                            row,
                            entry_score_floor=policy.entry_score_floor,
                            hold_score_floor=hold_score_floor,
                            hard_exit_score_floor=hard_exit_score_floor,
                            lower_band_floor=lower_band_floor,
                        ),
                        axis=1,
                    )
                    state_frame = pd.DataFrame(
                        states.tolist(),
                        columns=[
                            "candidate_state",
                            "entry_eligible_flag",
                            "hold_eligible_flag",
                            "hard_exit_flag",
                            "blocked_reason",
                        ],
                        index=mode_frame.index,
                    )
                    mode_frame = pd.concat([mode_frame, state_frame], axis=1)
                    mode_frame = _rank_candidates(mode_frame)
                    mode_frame["run_id"] = run_context.run_id
                    mode_frame["as_of_date"] = as_of_date
                    mode_frame["session_date"] = session_date
                    mode_frame["execution_mode"] = mode
                    mode_frame["portfolio_policy_id"] = context.policy_id
                    mode_frame["portfolio_policy_version"] = context.policy_version
                    mode_frame["active_portfolio_policy_id"] = context.active_policy_id
                    mode_frame["ranking_version"] = SELECTION_ENGINE_VERSION
                    mode_frame["primary_horizon"] = policy.primary_horizon
                    mode_frame["tactical_horizon"] = policy.tactical_horizon
                    mode_frame["tie_breaker_json"] = mode_frame.apply(
                        lambda row: json.dumps(
                            _tie_breaker_payload(row),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        axis=1,
                    )
                    mode_frame["notes_json"] = mode_frame.apply(
                        lambda row, ctx=context: json_text(
                            {
                                "regime_state": ctx.regime_state,
                                "regime_cash_target": ctx.regime_cash_target,
                                "config_path": ctx.config_path,
                            }
                        ),
                        axis=1,
                    )
                    mode_frame["created_at"] = pd.Timestamp.now(tz="UTC")
                    output = ordered_frame(
                        mode_frame[
                            [
                                "run_id",
                                "as_of_date",
                                "session_date",
                                "execution_mode",
                                "portfolio_policy_id",
                                "portfolio_policy_version",
                                "active_portfolio_policy_id",
                                "symbol",
                                "company_name",
                                "market",
                                "sector",
                                "ranking_version",
                                "primary_horizon",
                                "tactical_horizon",
                                "candidate_rank",
                                "current_holding_flag",
                                "current_shares",
                                "current_weight",
                                "final_selection_value",
                                "effective_alpha_long",
                                "tactical_expected_excess_return",
                                "lower_band",
                                "flow_score",
                                "regime_fit_score",
                                "uncertainty_score",
                                "disagreement_score",
                                "implementation_penalty_score",
                                "volatility_proxy",
                                "adv20_krw",
                                "risk_scaled_conviction",
                                "candidate_state",
                                "timing_action",
                                "timing_gate_status",
                                "entry_eligible_flag",
                                "hold_eligible_flag",
                                "hard_exit_flag",
                                "blocked_reason",
                                "tie_breaker_json",
                                "notes_json",
                                "created_at",
                            ]
                        ].rename(
                            columns={
                                "tactical_expected_excess_return": "tactical_alpha",
                                "regime_fit_score": "regime_score",
                            }
                        ),
                        [
                            "run_id",
                            "as_of_date",
                            "session_date",
                            "execution_mode",
                            "portfolio_policy_id",
                            "portfolio_policy_version",
                            "active_portfolio_policy_id",
                            "symbol",
                            "company_name",
                            "market",
                            "sector",
                            "ranking_version",
                            "primary_horizon",
                            "tactical_horizon",
                            "candidate_rank",
                            "current_holding_flag",
                            "current_shares",
                            "current_weight",
                            "final_selection_value",
                            "effective_alpha_long",
                            "tactical_alpha",
                            "lower_band",
                            "flow_score",
                            "regime_score",
                            "uncertainty_score",
                            "disagreement_score",
                            "implementation_penalty_score",
                            "volatility_proxy",
                            "adv20_krw",
                            "risk_scaled_conviction",
                            "candidate_state",
                            "timing_action",
                            "timing_gate_status",
                            "entry_eligible_flag",
                            "hold_eligible_flag",
                            "hard_exit_flag",
                            "blocked_reason",
                            "tie_breaker_json",
                            "notes_json",
                            "created_at",
                        ],
                    )
                    frames.append(output)
                    artifact_paths.append(
                        str(
                            write_parquet(
                                output,
                                base_dir=settings.paths.curated_dir,
                                dataset="portfolio/candidate_book",
                                partitions={
                                    "as_of_date": as_of_date.isoformat(),
                                    "execution_mode": mode.lower(),
                                },
                                filename="candidate_book.parquet",
                            )
                        )
                    )

                combined = (
                    pd.concat(frames, ignore_index=True)
                    .sort_values(
                        by=["as_of_date", "execution_mode", "candidate_rank", "symbol"],
                        ascending=[True, True, True, True],
                    )
                    .drop_duplicates(
                        subset=["as_of_date", "execution_mode", "symbol"],
                        keep="first",
                    )
                    .reset_index(drop=True)
                )
                upsert_portfolio_candidate(connection, combined)
                notes = (
                    "Portfolio candidate book materialized. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(combined)} "
                    f"session_date={session_date.isoformat()}"
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
                return PortfolioCandidateBookResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(combined),
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
                    notes="Portfolio candidate book failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def validate_portfolio_candidate_book(
    settings: Settings,
    *,
    as_of_date: date,
    execution_modes: list[str] | None = None,
) -> PortfolioValidationResult:
    ensure_storage_layout(settings)
    with activate_run_context("validate_portfolio_candidate_book", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_portfolio_candidate"],
                notes=f"Validate portfolio candidate book for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                requested_modes = [str(mode).upper() for mode in (execution_modes or EXECUTION_MODES)]
                placeholders = ",".join("?" for _ in requested_modes)
                frame = connection.execute(
                    f"""
                    SELECT *
                    FROM fact_portfolio_candidate
                    WHERE as_of_date = ?
                      AND execution_mode IN ({placeholders})
                    """,
                    [as_of_date, *requested_modes],
                ).fetchdf()
                checks: list[dict[str, object]] = []
                checks.append({"check_name": "row_presence", "passed": bool(not frame.empty)})
                duplicate_count = int(
                    frame.duplicated(subset=["as_of_date", "execution_mode", "symbol"]).sum()
                )
                checks.append({"check_name": "duplicate_keys", "passed": bool(duplicate_count == 0)})
                mode_count = int(frame["execution_mode"].nunique()) if not frame.empty else 0
                checks.append(
                    {
                        "check_name": "execution_modes_present",
                        "passed": bool(mode_count == len(set(requested_modes))),
                    }
                )
                invalid_states = (
                    set(frame["candidate_state"].dropna().astype(str).unique()) - set(CANDIDATE_STATES)
                )
                checks.append(
                    {"check_name": "candidate_state_values", "passed": bool(not invalid_states)}
                )
                next_date = _next_trading_day(connection, as_of_date=as_of_date)
                session_ok = False
                if not frame.empty:
                    session_ok = frame["session_date"].map(lambda value: pd.Timestamp(value).date()).eq(next_date).all()
                checks.append({"check_name": "next_trading_day_session", "passed": bool(session_ok)})
                negative_holding = int((pd.to_numeric(frame["current_shares"], errors="coerce") < 0).sum())
                checks.append(
                    {
                        "check_name": "non_negative_current_shares",
                        "passed": bool(negative_holding == 0),
                    }
                )

                artifact_dir = (
                    settings.paths.artifacts_dir
                    / "portfolio_validation"
                    / f"as_of_date={as_of_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                artifact_path = artifact_dir / "candidate_book_validation.json"
                artifact_path.write_text(
                    json.dumps(checks, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                warning_count = sum(1 for item in checks if not bool(item["passed"]))
                notes = (
                    "Portfolio candidate book validated. "
                    f"checks={len(checks)} warnings={warning_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[str(artifact_path)],
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return PortfolioValidationResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    check_count=len(checks),
                    artifact_paths=[str(artifact_path)],
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Portfolio candidate validation failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
