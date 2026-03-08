# ruff: noqa: E501

from __future__ import annotations

import json
from collections import defaultdict
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

from .candidate_book import build_portfolio_candidate_book
from .common import (
    CASH_SYMBOL,
    EXECUTION_MODES,
    PortfolioEvaluationResult,
    PortfolioNavResult,
    PortfolioPositionSnapshotResult,
    PortfolioRebalancePlanResult,
    PortfolioTargetBookResult,
    PortfolioWalkforwardResult,
    json_text,
    load_active_or_default_portfolio_policy,
)


def upsert_portfolio_target_book(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("portfolio_target_book_stage", frame)
    connection.execute(
        """
        INSERT OR REPLACE INTO fact_portfolio_target_book
        SELECT * FROM portfolio_target_book_stage
        """
    )
    connection.unregister("portfolio_target_book_stage")


def upsert_portfolio_rebalance_plan(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("portfolio_rebalance_plan_stage", frame)
    connection.execute(
        """
        INSERT OR REPLACE INTO fact_portfolio_rebalance_plan
        SELECT * FROM portfolio_rebalance_plan_stage
        """
    )
    connection.unregister("portfolio_rebalance_plan_stage")


def upsert_portfolio_position_snapshot(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("portfolio_position_snapshot_stage", frame)
    connection.execute(
        """
        INSERT OR REPLACE INTO fact_portfolio_position_snapshot
        SELECT * FROM portfolio_position_snapshot_stage
        """
    )
    connection.unregister("portfolio_position_snapshot_stage")


def upsert_portfolio_nav_snapshot(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("portfolio_nav_snapshot_stage", frame)
    connection.execute(
        """
        INSERT OR REPLACE INTO fact_portfolio_nav_snapshot
        SELECT * FROM portfolio_nav_snapshot_stage
        """
    )
    connection.unregister("portfolio_nav_snapshot_stage")


def upsert_portfolio_constraint_event(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("portfolio_constraint_event_stage", frame)
    connection.execute(
        """
        INSERT OR REPLACE INTO fact_portfolio_constraint_event
        SELECT * FROM portfolio_constraint_event_stage
        """
    )
    connection.unregister("portfolio_constraint_event_stage")


def upsert_portfolio_evaluation_summary(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("portfolio_evaluation_summary_stage", frame)
    connection.execute(
        """
        INSERT OR REPLACE INTO fact_portfolio_evaluation_summary
        SELECT * FROM portfolio_evaluation_summary_stage
        """
    )
    connection.unregister("portfolio_evaluation_summary_stage")


def _load_candidate_rows(connection, *, as_of_date: date, execution_mode: str) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_portfolio_candidate
        WHERE as_of_date = ?
          AND execution_mode = ?
        ORDER BY candidate_rank, symbol
        """,
        [as_of_date, execution_mode],
    ).fetchdf()


def _previous_nav_value(
    connection,
    *,
    session_date: date,
    execution_mode: str,
    policy_id: str,
    policy_version: str,
    default_capital: float,
) -> tuple[float, date | None]:
    frame = connection.execute(
        """
        SELECT snapshot_date, nav_value
        FROM fact_portfolio_nav_snapshot
        WHERE snapshot_date < ?
          AND execution_mode = ?
          AND portfolio_policy_id = ?
          AND portfolio_policy_version = ?
        ORDER BY snapshot_date DESC, created_at DESC
        LIMIT 1
        """,
        [session_date, execution_mode, policy_id, policy_version],
    ).fetchdf()
    if frame.empty:
        return float(default_capital), None
    row = frame.iloc[0]
    return float(row["nav_value"] or default_capital), pd.Timestamp(row["snapshot_date"]).date()


def _load_latest_positions(
    connection,
    *,
    session_date: date,
    execution_mode: str,
    policy_id: str,
    policy_version: str,
) -> pd.DataFrame:
    snapshot_row = connection.execute(
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
    if not snapshot_row or snapshot_row[0] is None:
        return pd.DataFrame(columns=["symbol", "shares", "actual_weight", "market_value"])
    snapshot_date = pd.Timestamp(snapshot_row[0]).date()
    return connection.execute(
        """
        SELECT symbol, shares, actual_weight, market_value
        FROM fact_portfolio_position_snapshot
        WHERE snapshot_date = ?
          AND execution_mode = ?
          AND portfolio_policy_id = ?
          AND portfolio_policy_version = ?
          AND COALESCE(cash_like_flag, FALSE) = FALSE
        """,
        [snapshot_date, execution_mode, policy_id, policy_version],
    ).fetchdf()


def _reference_price_series(connection, *, target_date: date, fallback_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        WITH ranked AS (
            SELECT
                symbol,
                trading_date,
                close,
                open,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY CASE WHEN trading_date = ? THEN 0 ELSE 1 END, trading_date DESC
                ) AS row_number
            FROM fact_daily_ohlcv
            WHERE trading_date IN (?, ?)
        )
        SELECT
            symbol,
            trading_date,
            COALESCE(open, close) AS reference_price,
            close
        FROM ranked
        WHERE row_number = 1
        """,
        [target_date, target_date, fallback_date],
    ).fetchdf()


def _cash_target_from_candidates(frame: pd.DataFrame, default_value: float) -> float:
    if frame.empty:
        return default_value
    payloads = frame["notes_json"].dropna().tolist()
    for value in payloads:
        try:
            payload = json.loads(str(value))
        except Exception:
            continue
        if "regime_cash_target" in payload:
            return float(payload["regime_cash_target"])
    return default_value


def _candidate_score_base(frame: pd.DataFrame) -> pd.Series:
    score = pd.to_numeric(frame["risk_scaled_conviction"], errors="coerce").fillna(0.0)
    score = score.clip(lower=0.0)
    score = score.where(frame["candidate_state"] != "TRIM_CANDIDATE", score * 0.55)
    score = score.where(frame["candidate_state"] != "HOLD_CANDIDATE", score * 1.05)
    return score


def _select_target_universe(frame: pd.DataFrame, *, min_names: int, max_names: int) -> pd.DataFrame:
    hold_candidates = frame.loc[
        frame["candidate_state"].isin(["HOLD_CANDIDATE", "TRIM_CANDIDATE"])
    ].copy()
    new_candidates = frame.loc[frame["candidate_state"] == "NEW_ENTRY_CANDIDATE"].copy()
    combined = pd.concat([hold_candidates, new_candidates], ignore_index=True)
    combined = combined.sort_values(
        by=["current_holding_flag", "candidate_rank", "symbol"],
        ascending=[False, True, True],
    )
    selected = combined.head(max_names).copy()
    if len(selected) < min_names:
        fillers = frame.loc[
            ~frame["symbol"].isin(selected["symbol"])
            & frame["candidate_state"].isin(["WATCH_ONLY", "NEW_ENTRY_CANDIDATE"])
        ].sort_values(by=["candidate_rank", "symbol"])
        selected = pd.concat([selected, fillers.head(min_names - len(selected))], ignore_index=True)
    return selected.drop_duplicates(subset=["symbol"]).reset_index(drop=True)


def _allocate_weights(
    frame: pd.DataFrame,
    *,
    investable_weight: float,
    nav_value: float,
    max_single_weight: float,
    max_sector_weight: float,
    max_kosdaq_weight: float,
    adv20_participation_limit: float,
    liquidity_min_adv20_krw: float,
) -> tuple[dict[str, float], list[dict[str, object]], dict[str, float]]:
    if frame.empty:
        return {}, [], {}
    working = frame.copy()
    working["score_base"] = _candidate_score_base(working)
    working["requested_weight"] = 0.0
    score_sum = float(working["score_base"].sum())
    if score_sum > 0:
        working["requested_weight"] = investable_weight * (working["score_base"] / score_sum)
    allocations: dict[str, float] = defaultdict(float)
    constraint_rows: list[dict[str, object]] = []
    requested = dict(zip(working["symbol"], working["requested_weight"], strict=False))
    sector_use: dict[str, float] = defaultdict(float)
    kosdaq_use = 0.0
    remaining_symbols = list(working.to_dict("records"))
    remaining_budget = float(max(0.0, investable_weight))
    while remaining_budget > 1e-6 and remaining_symbols:
        total_score = sum(max(float(row["score_base"]), 0.0) for row in remaining_symbols)
        if total_score <= 0:
            break
        progress = False
        next_remaining: list[dict[str, object]] = []
        for row in remaining_symbols:
            symbol = str(row["symbol"])
            sector = str(row.get("sector") or "미분류")
            market = str(row.get("market") or "")
            adv20 = float(pd.to_numeric(row.get("adv20_krw"), errors="coerce") or 0.0)
            if adv20 < liquidity_min_adv20_krw:
                constraint_rows.append(
                    {
                        "symbol": symbol,
                        "constraint_type": "liquidity",
                        "severity": "hard",
                        "event_code": "liquidity_min_adv20",
                        "requested_value": float(requested.get(symbol, 0.0)),
                        "applied_value": allocations[symbol],
                        "limit_value": liquidity_min_adv20_krw,
                    }
                )
                continue
            liquidity_cap = min(max_single_weight, (adv20 * adv20_participation_limit) / nav_value)
            sector_remaining = max(0.0, max_sector_weight - sector_use[sector])
            market_remaining = float("inf")
            if market.upper() == "KOSDAQ":
                market_remaining = max(0.0, max_kosdaq_weight - kosdaq_use)
            symbol_remaining = max(0.0, liquidity_cap - allocations[symbol])
            hard_cap = min(max_single_weight - allocations[symbol], symbol_remaining, sector_remaining, market_remaining)
            if hard_cap <= 1e-9:
                continue
            desired = remaining_budget * (max(float(row["score_base"]), 0.0) / total_score)
            delta = max(0.0, min(desired, hard_cap))
            if delta <= 1e-9:
                next_remaining.append(row)
                continue
            allocations[symbol] += delta
            sector_use[sector] += delta
            if market.upper() == "KOSDAQ":
                kosdaq_use += delta
            remaining_budget -= delta
            progress = True
            if hard_cap - delta > 1e-6:
                next_remaining.append(row)
        if not progress:
            break
        remaining_symbols = next_remaining
    for row in working.to_dict("records"):
        symbol = str(row["symbol"])
        allocated = allocations.get(symbol, 0.0)
        req = float(requested.get(symbol, 0.0))
        if allocated >= max_single_weight - 1e-6 and req > allocated:
            constraint_rows.append(
                {
                    "symbol": symbol,
                    "constraint_type": "weight_cap",
                    "severity": "soft",
                    "event_code": "single_name_cap",
                    "requested_value": req,
                    "applied_value": allocated,
                    "limit_value": max_single_weight,
                }
            )
        if str(row.get("market") or "").upper() == "KOSDAQ" and allocated < req:
            constraint_rows.append(
                {
                    "symbol": symbol,
                    "constraint_type": "market_cap",
                    "severity": "soft",
                    "event_code": "kosdaq_cap",
                    "requested_value": req,
                    "applied_value": allocated,
                    "limit_value": max_kosdaq_weight,
                }
            )
    return allocations, constraint_rows, requested


def materialize_portfolio_target_book(
    settings: Settings,
    *,
    as_of_date: date,
    execution_modes: list[str] | None = None,
    policy_config_path: str | None = None,
) -> PortfolioTargetBookResult:
    ensure_storage_layout(settings)
    with activate_run_context("materialize_portfolio_target_book", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_portfolio_candidate", "fact_portfolio_position_snapshot"],
                notes=f"Materialize portfolio target book for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                policy, active_policy_id, _ = load_active_or_default_portfolio_policy(
                    settings,
                    connection,
                    as_of_date=as_of_date,
                    policy_config_path=policy_config_path,
                )
                requested_modes = [str(mode).upper() for mode in (execution_modes or policy.execution_modes)]
                requested_modes = [mode for mode in requested_modes if mode in EXECUTION_MODES]
                if not requested_modes:
                    requested_modes = list(EXECUTION_MODES)

                output_frames: list[pd.DataFrame] = []
                constraint_frames: list[pd.DataFrame] = []
                artifact_paths: list[str] = []
                for mode in requested_modes:
                    candidate_frame = _load_candidate_rows(
                        connection,
                        as_of_date=as_of_date,
                        execution_mode=mode,
                    )
                    if candidate_frame.empty:
                        build_portfolio_candidate_book(
                            settings,
                            as_of_date=as_of_date,
                            execution_modes=[mode],
                            policy_config_path=policy_config_path,
                        )
                        candidate_frame = _load_candidate_rows(
                            connection,
                            as_of_date=as_of_date,
                            execution_mode=mode,
                        )
                    if candidate_frame.empty:
                        continue
                    session_date = pd.Timestamp(candidate_frame["session_date"].iloc[0]).date()
                    nav_value, _ = _previous_nav_value(
                        connection,
                        session_date=session_date,
                        execution_mode=mode,
                        policy_id=policy.portfolio_policy_id,
                        policy_version=policy.portfolio_policy_version,
                        default_capital=policy.virtual_capital_krw,
                    )
                    investable_weight = 1.0 - _cash_target_from_candidates(
                        candidate_frame,
                        policy.regime_cash_target("neutral"),
                    )
                    investable_weight = max(0.0, min(1.0, investable_weight))
                    reference_prices = _reference_price_series(
                        connection,
                        target_date=session_date,
                        fallback_date=as_of_date,
                    )
                    working = candidate_frame.merge(reference_prices, on="symbol", how="left")
                    selected = _select_target_universe(
                        working,
                        min_names=policy.min_names,
                        max_names=policy.max_names,
                    )
                    allocations, constraint_rows, requested_weights = _allocate_weights(
                        selected,
                        investable_weight=investable_weight,
                        nav_value=max(nav_value, 1.0),
                        max_single_weight=policy.max_single_weight,
                        max_sector_weight=policy.max_sector_weight,
                        max_kosdaq_weight=policy.max_kosdaq_weight,
                        adv20_participation_limit=policy.adv20_participation_limit,
                        liquidity_min_adv20_krw=policy.liquidity_min_adv20_krw,
                    )
                    waitlist_symbols = [
                        symbol
                        for symbol in working.loc[
                            working["candidate_state"].isin(["NEW_ENTRY_CANDIDATE", "HOLD_CANDIDATE", "TRIM_CANDIDATE"])
                        ].sort_values(by=["candidate_rank", "symbol"])["symbol"].tolist()
                        if symbol not in set(selected["symbol"])
                    ]
                    waitlist_rank = {symbol: index + 1 for index, symbol in enumerate(waitlist_symbols)}
                    rows: list[dict[str, object]] = []
                    for row in working.to_dict("records"):
                        symbol = str(row["symbol"])
                        target_weight = float(allocations.get(symbol, 0.0))
                        reference_price = float(pd.to_numeric(row.get("reference_price"), errors="coerce") or 0.0)
                        target_notional = nav_value * target_weight
                        target_shares = int(target_notional // reference_price) if reference_price > 0 else 0
                        included = target_weight > 0 or bool(row.get("current_holding_flag"))
                        blocked = row.get("candidate_state") == "BLOCKED"
                        rows.append(
                            {
                                "run_id": run_context.run_id,
                                "as_of_date": as_of_date,
                                "session_date": session_date,
                                "execution_mode": mode,
                                "portfolio_policy_id": policy.portfolio_policy_id,
                                "portfolio_policy_version": policy.portfolio_policy_version,
                                "active_portfolio_policy_id": active_policy_id,
                                "symbol": symbol,
                                "company_name": row.get("company_name"),
                                "market": row.get("market"),
                                "sector": row.get("sector"),
                                "candidate_state": row.get("candidate_state"),
                                "target_rank": int(row.get("candidate_rank") or 0),
                                "target_weight": target_weight,
                                "target_notional": target_shares * reference_price,
                                "target_shares": target_shares,
                                "target_price": reference_price if reference_price > 0 else None,
                                "current_shares": int(row.get("current_shares") or 0),
                                "current_weight": float(row.get("current_weight") or 0.0),
                                "score_value": float(row.get("risk_scaled_conviction") or 0.0),
                                "gate_status": row.get("timing_gate_status"),
                                "included_flag": bool(included),
                                "blocked_flag": bool(blocked),
                                "waitlist_flag": symbol in waitlist_rank,
                                "waitlist_rank": waitlist_rank.get(symbol),
                                "constraint_flags_json": json_text(
                                    {
                                        "requested_weight": requested_weights.get(symbol),
                                        "adv20_krw": row.get("adv20_krw"),
                                    }
                                ),
                                "notes_json": row.get("notes_json"),
                                "created_at": pd.Timestamp.now(tz="UTC"),
                            }
                        )
                    cash_weight = max(0.0, 1.0 - sum(allocations.values()))
                    cash_value = nav_value - sum(item["target_notional"] for item in rows)
                    rows.append(
                        {
                            "run_id": run_context.run_id,
                            "as_of_date": as_of_date,
                            "session_date": session_date,
                            "execution_mode": mode,
                            "portfolio_policy_id": policy.portfolio_policy_id,
                            "portfolio_policy_version": policy.portfolio_policy_version,
                            "active_portfolio_policy_id": active_policy_id,
                            "symbol": CASH_SYMBOL,
                            "company_name": "현금",
                            "market": "CASH",
                            "sector": "현금",
                            "candidate_state": "CASH",
                            "target_rank": 9999,
                            "target_weight": cash_weight,
                            "target_notional": cash_value,
                            "target_shares": 0,
                            "target_price": 1.0,
                            "current_shares": 0,
                            "current_weight": max(0.0, 1.0 - investable_weight),
                            "score_value": 0.0,
                            "gate_status": "CASH_BUFFER",
                            "included_flag": True,
                            "blocked_flag": False,
                            "waitlist_flag": False,
                            "waitlist_rank": None,
                            "constraint_flags_json": None,
                            "notes_json": json_text({"cash_target_weight": cash_weight}),
                            "created_at": pd.Timestamp.now(tz="UTC"),
                        }
                    )
                    output = pd.DataFrame(rows)
                    output_frames.append(output)
                    if constraint_rows:
                        constraint_frame = pd.DataFrame(constraint_rows)
                        constraint_frame.insert(0, "run_id", run_context.run_id)
                        constraint_frame.insert(1, "as_of_date", as_of_date)
                        constraint_frame.insert(2, "session_date", session_date)
                        constraint_frame.insert(3, "execution_mode", mode)
                        constraint_frame.insert(4, "portfolio_policy_id", policy.portfolio_policy_id)
                        constraint_frame.insert(5, "portfolio_policy_version", policy.portfolio_policy_version)
                        constraint_frame["message"] = constraint_frame["event_code"]
                        constraint_frame["notes_json"] = None
                        constraint_frame["created_at"] = pd.Timestamp.now(tz="UTC")
                        constraint_frames.append(constraint_frame)
                    artifact_paths.append(
                        str(
                            write_parquet(
                                output,
                                base_dir=settings.paths.curated_dir,
                                dataset="portfolio/target_book",
                                partitions={
                                    "as_of_date": as_of_date.isoformat(),
                                    "execution_mode": mode.lower(),
                                },
                                filename="target_book.parquet",
                            )
                        )
                    )

                combined = pd.concat(output_frames, ignore_index=True) if output_frames else pd.DataFrame()
                upsert_portfolio_target_book(connection, combined)
                if constraint_frames:
                    upsert_portfolio_constraint_event(
                        connection,
                        pd.concat(constraint_frames, ignore_index=True),
                    )
                notes = (
                    "Portfolio target book materialized. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(combined)}"
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
                return PortfolioTargetBookResult(
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
                    notes="Portfolio target book failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def _action_from_delta(current_shares: int, target_shares: int) -> str:
    if current_shares == 0 and target_shares > 0:
        return "BUY_NEW"
    if current_shares > 0 and target_shares > current_shares:
        return "ADD"
    if current_shares > 0 and target_shares == current_shares:
        return "HOLD"
    if current_shares > 0 and 0 < target_shares < current_shares:
        return "TRIM"
    if current_shares > 0 and target_shares == 0:
        return "EXIT"
    if current_shares == 0 and target_shares == 0:
        return "NO_ACTION"
    return "NO_ACTION"


def _action_priority(action: str) -> int:
    return {
        "EXIT": 10,
        "TRIM": 20,
        "HOLD": 30,
        "ADD": 40,
        "BUY_NEW": 50,
        "SKIP": 60,
        "NO_ACTION": 70,
    }.get(action, 99)


def materialize_portfolio_rebalance_plan(
    settings: Settings,
    *,
    as_of_date: date,
    execution_modes: list[str] | None = None,
    policy_config_path: str | None = None,
) -> PortfolioRebalancePlanResult:
    ensure_storage_layout(settings)
    with activate_run_context("materialize_portfolio_rebalance_plan", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_portfolio_target_book", "fact_portfolio_position_snapshot"],
                notes=f"Materialize portfolio rebalance plan for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                policy, active_policy_id, _ = load_active_or_default_portfolio_policy(
                    settings,
                    connection,
                    as_of_date=as_of_date,
                    policy_config_path=policy_config_path,
                )
                requested_modes = [str(mode).upper() for mode in (execution_modes or policy.execution_modes)]
                requested_modes = [mode for mode in requested_modes if mode in EXECUTION_MODES]
                if not requested_modes:
                    requested_modes = list(EXECUTION_MODES)

                frames: list[pd.DataFrame] = []
                constraint_frames: list[pd.DataFrame] = []
                artifact_paths: list[str] = []
                for mode in requested_modes:
                    target_frame = connection.execute(
                        """
                        SELECT *
                        FROM fact_portfolio_target_book
                        WHERE as_of_date = ?
                          AND execution_mode = ?
                        ORDER BY target_rank, symbol
                        """,
                        [as_of_date, mode],
                    ).fetchdf()
                    if target_frame.empty:
                        materialize_portfolio_target_book(
                            settings,
                            as_of_date=as_of_date,
                            execution_modes=[mode],
                            policy_config_path=policy_config_path,
                        )
                        target_frame = connection.execute(
                            """
                            SELECT *
                            FROM fact_portfolio_target_book
                            WHERE as_of_date = ?
                              AND execution_mode = ?
                            ORDER BY target_rank, symbol
                            """,
                            [as_of_date, mode],
                        ).fetchdf()
                    if target_frame.empty:
                        continue
                    session_date = pd.Timestamp(target_frame["session_date"].iloc[0]).date()
                    nav_value, _ = _previous_nav_value(
                        connection,
                        session_date=session_date,
                        execution_mode=mode,
                        policy_id=policy.portfolio_policy_id,
                        policy_version=policy.portfolio_policy_version,
                        default_capital=policy.virtual_capital_krw,
                    )
                    buy_budget = nav_value * policy.max_turnover_ratio
                    remaining_buy_budget = buy_budget
                    plan_rows: list[dict[str, object]] = []
                    turnover_constraint_rows: list[dict[str, object]] = []
                    working = target_frame.loc[target_frame["symbol"] != CASH_SYMBOL].copy()
                    working["rebalance_action"] = working.apply(
                        lambda row: _action_from_delta(
                            int(row.get("current_shares") or 0),
                            int(row.get("target_shares") or 0),
                        ),
                        axis=1,
                    )
                    working = working.sort_values(
                        by=["rebalance_action", "target_rank", "symbol"],
                        key=lambda series: series.map(_action_priority)
                        if series.name == "rebalance_action"
                        else series,
                    ).reset_index(drop=True)
                    for index, row in enumerate(working.to_dict("records"), start=1):
                        action = str(row["rebalance_action"])
                        current_shares = int(row.get("current_shares") or 0)
                        target_shares = int(row.get("target_shares") or 0)
                        reference_price = float(pd.to_numeric(row.get("target_price"), errors="coerce") or 0.0)
                        gate_status = str(row.get("gate_status") or "")
                        blocked_reason = row.get("blocked_reason")
                        if mode == "TIMING_ASSISTED" and action in {"BUY_NEW", "ADD"} and gate_status != "ENTER_ALLOWED":
                            target_shares = current_shares
                            action = "SKIP" if current_shares == 0 else "HOLD"
                            blocked_reason = gate_status.lower()
                        current_notional = current_shares * reference_price
                        target_notional = target_shares * reference_price
                        notional_delta = target_notional - current_notional
                        if action in {"BUY_NEW", "ADD"} and notional_delta > remaining_buy_budget:
                            affordable_shares = int(remaining_buy_budget // reference_price) if reference_price > 0 else 0
                            if affordable_shares <= 0:
                                target_shares = current_shares
                                target_notional = current_notional
                                notional_delta = 0.0
                                action = "SKIP" if current_shares == 0 else "HOLD"
                                blocked_reason = "turnover_budget_exhausted"
                            else:
                                if current_shares == 0:
                                    target_shares = affordable_shares
                                    action = "BUY_NEW"
                                else:
                                    target_shares = current_shares + affordable_shares
                                    action = "ADD"
                                target_notional = target_shares * reference_price
                                notional_delta = target_notional - current_notional
                                blocked_reason = "turnover_budget_partial"
                            turnover_constraint_rows.append(
                                {
                                    "run_id": run_context.run_id,
                                    "as_of_date": as_of_date,
                                    "session_date": session_date,
                                    "execution_mode": mode,
                                    "portfolio_policy_id": policy.portfolio_policy_id,
                                    "portfolio_policy_version": policy.portfolio_policy_version,
                                    "symbol": row["symbol"],
                                    "constraint_type": "turnover",
                                    "severity": "soft",
                                    "event_code": "turnover_budget",
                                    "requested_value": row.get("target_notional"),
                                    "applied_value": target_notional,
                                    "limit_value": remaining_buy_budget,
                                    "message": blocked_reason,
                                    "notes_json": None,
                                    "created_at": pd.Timestamp.now(tz="UTC"),
                                }
                            )
                        if action in {"BUY_NEW", "ADD"}:
                            remaining_buy_budget = max(0.0, remaining_buy_budget - max(notional_delta, 0.0))
                        plan_rows.append(
                            {
                                "run_id": run_context.run_id,
                                "as_of_date": as_of_date,
                                "session_date": session_date,
                                "execution_mode": mode,
                                "portfolio_policy_id": policy.portfolio_policy_id,
                                "portfolio_policy_version": policy.portfolio_policy_version,
                                "active_portfolio_policy_id": active_policy_id,
                                "symbol": row["symbol"],
                                "company_name": row.get("company_name"),
                                "market": row.get("market"),
                                "sector": row.get("sector"),
                                "rebalance_action": action,
                                "action_sequence": index,
                                "gate_status": gate_status,
                                "candidate_state": row.get("candidate_state"),
                                "current_shares": current_shares,
                                "target_shares": target_shares,
                                "delta_shares": target_shares - current_shares,
                                "reference_price": reference_price if reference_price > 0 else None,
                                "current_notional": current_notional,
                                "target_notional": target_notional,
                                "notional_delta": notional_delta,
                                "turnover_contribution": abs(notional_delta) / nav_value if nav_value > 0 else None,
                                "cash_delta": -notional_delta,
                                "waitlist_flag": bool(row.get("waitlist_flag")),
                                "blocked_reason": blocked_reason,
                                "notes_json": row.get("notes_json"),
                                "created_at": pd.Timestamp.now(tz="UTC"),
                            }
                        )
                    cash_row = target_frame.loc[target_frame["symbol"] == CASH_SYMBOL]
                    if not cash_row.empty:
                        cash = cash_row.iloc[0]
                        plan_rows.append(
                            {
                                "run_id": run_context.run_id,
                                "as_of_date": as_of_date,
                                "session_date": session_date,
                                "execution_mode": mode,
                                "portfolio_policy_id": policy.portfolio_policy_id,
                                "portfolio_policy_version": policy.portfolio_policy_version,
                                "active_portfolio_policy_id": active_policy_id,
                                "symbol": CASH_SYMBOL,
                                "company_name": "현금",
                                "market": "CASH",
                                "sector": "현금",
                                "rebalance_action": "NO_ACTION",
                                "action_sequence": 9999,
                                "gate_status": "CASH_BUFFER",
                                "candidate_state": "CASH",
                                "current_shares": 0,
                                "target_shares": 0,
                                "delta_shares": 0,
                                "reference_price": 1.0,
                                "current_notional": float(cash.get("current_weight") or 0.0) * nav_value,
                                "target_notional": float(cash.get("target_notional") or 0.0),
                                "notional_delta": 0.0,
                                "turnover_contribution": 0.0,
                                "cash_delta": 0.0,
                                "waitlist_flag": False,
                                "blocked_reason": None,
                                "notes_json": cash.get("notes_json"),
                                "created_at": pd.Timestamp.now(tz="UTC"),
                            }
                        )
                    mode_frame = pd.DataFrame(plan_rows)
                    frames.append(mode_frame)
                    if turnover_constraint_rows:
                        constraint_frames.append(pd.DataFrame(turnover_constraint_rows))
                    artifact_paths.append(
                        str(
                            write_parquet(
                                mode_frame,
                                base_dir=settings.paths.curated_dir,
                                dataset="portfolio/rebalance_plan",
                                partitions={
                                    "as_of_date": as_of_date.isoformat(),
                                    "execution_mode": mode.lower(),
                                },
                                filename="rebalance_plan.parquet",
                            )
                        )
                    )

                combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                upsert_portfolio_rebalance_plan(connection, combined)
                if constraint_frames:
                    upsert_portfolio_constraint_event(
                        connection,
                        pd.concat(constraint_frames, ignore_index=True),
                    )
                notes = (
                    "Portfolio rebalance plan materialized. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(combined)}"
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
                return PortfolioRebalancePlanResult(
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
                    notes="Portfolio rebalance plan failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def materialize_portfolio_position_snapshots(
    settings: Settings,
    *,
    as_of_date: date,
    execution_modes: list[str] | None = None,
    policy_config_path: str | None = None,
) -> PortfolioPositionSnapshotResult:
    ensure_storage_layout(settings)
    with activate_run_context("materialize_portfolio_position_snapshots", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_portfolio_rebalance_plan", "fact_daily_ohlcv"],
                notes=f"Materialize portfolio position snapshots for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                policy, active_policy_id, _ = load_active_or_default_portfolio_policy(
                    settings,
                    connection,
                    as_of_date=as_of_date,
                    policy_config_path=policy_config_path,
                )
                requested_modes = [str(mode).upper() for mode in (execution_modes or policy.execution_modes)]
                requested_modes = [mode for mode in requested_modes if mode in EXECUTION_MODES]
                if not requested_modes:
                    requested_modes = list(EXECUTION_MODES)

                frames: list[pd.DataFrame] = []
                artifact_paths: list[str] = []
                for mode in requested_modes:
                    plan = connection.execute(
                        """
                        SELECT *
                        FROM fact_portfolio_rebalance_plan
                        WHERE as_of_date = ?
                          AND execution_mode = ?
                        ORDER BY action_sequence, symbol
                        """,
                        [as_of_date, mode],
                    ).fetchdf()
                    if plan.empty:
                        materialize_portfolio_rebalance_plan(
                            settings,
                            as_of_date=as_of_date,
                            execution_modes=[mode],
                            policy_config_path=policy_config_path,
                        )
                        plan = connection.execute(
                            """
                            SELECT *
                            FROM fact_portfolio_rebalance_plan
                            WHERE as_of_date = ?
                              AND execution_mode = ?
                            ORDER BY action_sequence, symbol
                            """,
                            [as_of_date, mode],
                        ).fetchdf()
                    if plan.empty:
                        continue
                    session_date = pd.Timestamp(plan["session_date"].iloc[0]).date()
                    nav_value, _ = _previous_nav_value(
                        connection,
                        session_date=session_date,
                        execution_mode=mode,
                        policy_id=policy.portfolio_policy_id,
                        policy_version=policy.portfolio_policy_version,
                        default_capital=policy.virtual_capital_krw,
                    )
                    close_prices = _reference_price_series(
                        connection,
                        target_date=session_date,
                        fallback_date=as_of_date,
                    ).rename(columns={"reference_price": "open_reference_price"})
                    working = plan.merge(close_prices, on="symbol", how="left")
                    rows: list[dict[str, object]] = []
                    invested_cost = 0.0
                    for row in working.loc[working["symbol"] != CASH_SYMBOL].to_dict("records"):
                        shares = int(row.get("target_shares") or 0)
                        if shares <= 0:
                            continue
                        trade_price = float(pd.to_numeric(row.get("reference_price"), errors="coerce") or 0.0)
                        close_price = float(pd.to_numeric(row.get("close"), errors="coerce") or trade_price or 0.0)
                        market_value = shares * close_price
                        invested_cost += shares * trade_price
                        rows.append(
                            {
                                "run_id": run_context.run_id,
                                "snapshot_date": session_date,
                                "execution_mode": mode,
                                "portfolio_policy_id": policy.portfolio_policy_id,
                                "portfolio_policy_version": policy.portfolio_policy_version,
                                "active_portfolio_policy_id": active_policy_id,
                                "symbol": row["symbol"],
                                "company_name": row.get("company_name"),
                                "market": row.get("market"),
                                "sector": row.get("sector"),
                                "shares": shares,
                                "average_cost": trade_price if trade_price > 0 else None,
                                "close_price": close_price if close_price > 0 else None,
                                "market_value": market_value,
                                "target_weight": row.get("target_notional", 0.0) / nav_value if nav_value > 0 else None,
                                "actual_weight": None,
                                "cash_like_flag": False,
                                "source_rebalance_run_id": run_context.run_id,
                                "notes_json": row.get("notes_json"),
                                "created_at": pd.Timestamp.now(tz="UTC"),
                            }
                        )
                    cash_value = max(0.0, nav_value - invested_cost)
                    total_nav = cash_value + sum(item["market_value"] for item in rows)
                    for item in rows:
                        item["actual_weight"] = item["market_value"] / total_nav if total_nav > 0 else None
                    rows.append(
                        {
                            "run_id": run_context.run_id,
                            "snapshot_date": session_date,
                            "execution_mode": mode,
                            "portfolio_policy_id": policy.portfolio_policy_id,
                            "portfolio_policy_version": policy.portfolio_policy_version,
                            "active_portfolio_policy_id": active_policy_id,
                            "symbol": CASH_SYMBOL,
                            "company_name": "현금",
                            "market": "CASH",
                            "sector": "현금",
                            "shares": 0,
                            "average_cost": 1.0,
                            "close_price": 1.0,
                            "market_value": cash_value,
                            "target_weight": cash_value / total_nav if total_nav > 0 else None,
                            "actual_weight": cash_value / total_nav if total_nav > 0 else None,
                            "cash_like_flag": True,
                            "source_rebalance_run_id": run_context.run_id,
                            "notes_json": json_text({"cash_residual": cash_value}),
                            "created_at": pd.Timestamp.now(tz="UTC"),
                        }
                    )
                    mode_frame = pd.DataFrame(rows)
                    frames.append(mode_frame)
                    artifact_paths.append(
                        str(
                            write_parquet(
                                mode_frame,
                                base_dir=settings.paths.curated_dir,
                                dataset="portfolio/position_snapshot",
                                partitions={
                                    "snapshot_date": session_date.isoformat(),
                                    "execution_mode": mode.lower(),
                                },
                                filename="position_snapshot.parquet",
                            )
                        )
                    )

                combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                upsert_portfolio_position_snapshot(connection, combined)
                notes = (
                    "Portfolio position snapshots materialized. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(combined)}"
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
                return PortfolioPositionSnapshotResult(
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
                    notes="Portfolio position snapshots failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def materialize_portfolio_nav(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    execution_modes: list[str] | None = None,
    policy_config_path: str | None = None,
) -> PortfolioNavResult:
    ensure_storage_layout(settings)
    with activate_run_context("materialize_portfolio_nav", as_of_date=end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_date,
                input_sources=["fact_portfolio_position_snapshot", "fact_portfolio_rebalance_plan"],
                notes=f"Materialize portfolio NAV from {start_date.isoformat()} to {end_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                policy, active_policy_id, _ = load_active_or_default_portfolio_policy(
                    settings,
                    connection,
                    as_of_date=end_date,
                    policy_config_path=policy_config_path,
                )
                requested_modes = [str(mode).upper() for mode in (execution_modes or policy.execution_modes)]
                requested_modes = [mode for mode in requested_modes if mode in EXECUTION_MODES]
                if not requested_modes:
                    requested_modes = list(EXECUTION_MODES)

                positions = connection.execute(
                    """
                    SELECT *
                    FROM fact_portfolio_position_snapshot
                    WHERE snapshot_date BETWEEN ? AND ?
                    ORDER BY snapshot_date, execution_mode, symbol
                    """,
                    [start_date, end_date],
                ).fetchdf()
                if positions.empty:
                    notes = "No portfolio position snapshots available for NAV materialization."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return PortfolioNavResult(
                        run_id=run_context.run_id,
                        end_date=end_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                rebalance = connection.execute(
                    """
                    SELECT
                        session_date AS snapshot_date,
                        execution_mode,
                        portfolio_policy_id,
                        portfolio_policy_version,
                        SUM(ABS(notional_delta)) AS turnover_notional
                    FROM fact_portfolio_rebalance_plan
                    WHERE session_date BETWEEN ? AND ?
                    GROUP BY 1, 2, 3, 4
                    """,
                    [start_date, end_date],
                ).fetchdf()

                rows: list[dict[str, object]] = []
                grouped = positions.groupby(
                    ["execution_mode", "portfolio_policy_id", "portfolio_policy_version"],
                    sort=True,
                )
                for (mode, policy_id, policy_version), partition in grouped:
                    if mode not in requested_modes:
                        continue
                    partition = partition.sort_values(["snapshot_date", "cash_like_flag", "symbol"])
                    nav_track: list[dict[str, object]] = []
                    for snapshot_date, snapshot_rows in partition.groupby("snapshot_date", sort=True):
                        invested_value = float(
                            snapshot_rows.loc[snapshot_rows["cash_like_flag"] == False, "market_value"].sum()  # noqa: E712
                        )
                        cash_value = float(
                            snapshot_rows.loc[snapshot_rows["cash_like_flag"] == True, "market_value"].sum()  # noqa: E712
                        )
                        nav_value = invested_value + cash_value
                        weights = snapshot_rows.loc[snapshot_rows["cash_like_flag"] == False, "actual_weight"]  # noqa: E712
                        weights = pd.to_numeric(weights, errors="coerce").dropna()
                        holding_count = int(snapshot_rows["cash_like_flag"].eq(False).sum())
                        turnover_match = rebalance.loc[
                            (rebalance["snapshot_date"] == snapshot_date)
                            & (rebalance["execution_mode"] == mode)
                            & (rebalance["portfolio_policy_id"] == policy_id)
                            & (rebalance["portfolio_policy_version"] == policy_version)
                        ]
                        turnover_ratio = None
                        if not turnover_match.empty and nav_value > 0:
                            turnover_ratio = float(turnover_match["turnover_notional"].iloc[0]) / nav_value
                        nav_track.append(
                            {
                                "snapshot_date": pd.Timestamp(snapshot_date).date(),
                                "nav_value": nav_value,
                                "invested_value": invested_value,
                                "cash_value": cash_value,
                                "cash_weight": cash_value / nav_value if nav_value > 0 else None,
                                "holding_count": holding_count,
                                "max_single_weight": float(weights.max()) if not weights.empty else 0.0,
                                "top3_weight": float(weights.nlargest(3).sum()) if not weights.empty else 0.0,
                                "turnover_ratio": turnover_ratio,
                            }
                        )
                    nav_frame = pd.DataFrame(nav_track).sort_values("snapshot_date").reset_index(drop=True)
                    nav_frame["daily_return"] = nav_frame["nav_value"].pct_change().fillna(0.0)
                    nav_frame["cumulative_return"] = nav_frame["nav_value"] / nav_frame["nav_value"].iloc[0] - 1.0
                    nav_frame["running_peak"] = nav_frame["nav_value"].cummax()
                    nav_frame["drawdown"] = nav_frame["nav_value"] / nav_frame["running_peak"] - 1.0
                    for row in nav_frame.to_dict("records"):
                        rows.append(
                            {
                                "run_id": run_context.run_id,
                                "snapshot_date": row["snapshot_date"],
                                "execution_mode": mode,
                                "portfolio_policy_id": policy_id,
                                "portfolio_policy_version": policy_version,
                                "active_portfolio_policy_id": active_policy_id,
                                "nav_value": row["nav_value"],
                                "invested_value": row["invested_value"],
                                "cash_value": row["cash_value"],
                                "gross_exposure": row["invested_value"] / row["nav_value"] if row["nav_value"] > 0 else None,
                                "net_exposure": row["invested_value"] / row["nav_value"] if row["nav_value"] > 0 else None,
                                "daily_return": row["daily_return"],
                                "cumulative_return": row["cumulative_return"],
                                "drawdown": row["drawdown"],
                                "turnover_ratio": row["turnover_ratio"],
                                "cash_weight": row["cash_weight"],
                                "holding_count": row["holding_count"],
                                "max_single_weight": row["max_single_weight"],
                                "top3_weight": row["top3_weight"],
                                "source_position_run_id": run_context.run_id,
                                "notes_json": None,
                                "created_at": pd.Timestamp.now(tz="UTC"),
                            }
                        )

                output = pd.DataFrame(rows)
                upsert_portfolio_nav_snapshot(connection, output)
                artifact_paths = []
                if not output.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                output,
                                base_dir=settings.paths.curated_dir,
                                dataset="portfolio/nav_snapshot",
                                partitions={"end_date": end_date.isoformat()},
                                filename="nav_snapshot.parquet",
                            )
                        )
                    )
                notes = (
                    "Portfolio NAV materialized. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()} rows={len(output)}"
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
                return PortfolioNavResult(
                    run_id=run_context.run_id,
                    end_date=end_date,
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
                    notes="Portfolio NAV materialization failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def run_portfolio_walkforward(
    settings: Settings,
    *,
    start_as_of_date: date,
    end_as_of_date: date,
    execution_modes: list[str] | None = None,
    policy_config_path: str | None = None,
) -> PortfolioWalkforwardResult:
    ensure_storage_layout(settings)
    with activate_run_context("run_portfolio_walkforward", as_of_date=end_as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_as_of_date,
                input_sources=[
                    "dim_trading_calendar",
                    "fact_ranking",
                    "fact_intraday_meta_decision",
                ],
                notes=(
                    "Run portfolio walkforward. "
                    f"range={start_as_of_date.isoformat()}..{end_as_of_date.isoformat()}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                selection_rows = connection.execute(
                    """
                    SELECT trading_date
                    FROM dim_trading_calendar
                    WHERE trading_date BETWEEN ? AND ?
                      AND is_trading_day = TRUE
                    ORDER BY trading_date
                    """,
                    [start_as_of_date, end_as_of_date],
                ).fetchall()
                selection_dates = [pd.Timestamp(row[0]).date() for row in selection_rows]
                session_dates: list[date] = []
                for as_of_date in selection_dates:
                    build_portfolio_candidate_book(
                        settings,
                        as_of_date=as_of_date,
                        execution_modes=execution_modes,
                        policy_config_path=policy_config_path,
                    )
                    materialize_portfolio_target_book(
                        settings,
                        as_of_date=as_of_date,
                        execution_modes=execution_modes,
                        policy_config_path=policy_config_path,
                    )
                    plan_result = materialize_portfolio_rebalance_plan(
                        settings,
                        as_of_date=as_of_date,
                        execution_modes=execution_modes,
                        policy_config_path=policy_config_path,
                    )
                    materialize_portfolio_position_snapshots(
                        settings,
                        as_of_date=as_of_date,
                        execution_modes=execution_modes,
                        policy_config_path=policy_config_path,
                    )
                    if plan_result.row_count <= 0:
                        continue
                    row = connection.execute(
                        """
                        SELECT MAX(session_date)
                        FROM fact_portfolio_rebalance_plan
                        WHERE as_of_date = ?
                        """,
                        [as_of_date],
                    ).fetchone()
                    if row and row[0]:
                        session_dates.append(pd.Timestamp(row[0]).date())
                artifact_paths: list[str] = []
                if session_dates:
                    nav_result = materialize_portfolio_nav(
                        settings,
                        start_date=min(session_dates),
                        end_date=max(session_dates),
                        execution_modes=execution_modes,
                        policy_config_path=policy_config_path,
                    )
                    artifact_paths.extend(nav_result.artifact_paths)
                notes = (
                    "Portfolio walkforward completed. "
                    f"processed_dates={len(selection_dates)}"
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
                return PortfolioWalkforwardResult(
                    run_id=run_context.run_id,
                    start_as_of_date=start_as_of_date,
                    end_as_of_date=end_as_of_date,
                    processed_dates=len(selection_dates),
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
                    notes="Portfolio walkforward failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def _summary_metric_rows(
    *,
    evaluation_date: date,
    start_date: date,
    end_date: date,
    policy_id: str,
    policy_version: str,
    execution_mode: str,
    comparison_key: str,
    nav_frame: pd.DataFrame,
    evaluation_run_id: str,
) -> list[dict[str, object]]:
    if nav_frame.empty:
        return []
    daily_returns = pd.to_numeric(nav_frame["daily_return"], errors="coerce").dropna()
    cumulative_return = float(nav_frame["nav_value"].iloc[-1] / nav_frame["nav_value"].iloc[0] - 1.0)
    annualized_vol = float(daily_returns.std(ddof=0) * (252 ** 0.5)) if len(daily_returns) > 1 else 0.0
    sharpe_like = float(daily_returns.mean() / daily_returns.std(ddof=0) * (252 ** 0.5)) if len(daily_returns) > 1 and daily_returns.std(ddof=0) > 0 else 0.0
    metrics = {
        "cumulative_return": cumulative_return,
        "annualized_volatility": annualized_vol,
        "sharpe_like_ratio": sharpe_like,
        "max_drawdown": float(pd.to_numeric(nav_frame["drawdown"], errors="coerce").min()),
        "average_turnover": float(pd.to_numeric(nav_frame["turnover_ratio"], errors="coerce").fillna(0.0).mean()),
        "average_cash_weight": float(pd.to_numeric(nav_frame["cash_weight"], errors="coerce").fillna(0.0).mean()),
        "average_holding_count": float(pd.to_numeric(nav_frame["holding_count"], errors="coerce").fillna(0.0).mean()),
        "average_max_single_weight": float(pd.to_numeric(nav_frame["max_single_weight"], errors="coerce").fillna(0.0).mean()),
        "average_top3_weight": float(pd.to_numeric(nav_frame["top3_weight"], errors="coerce").fillna(0.0).mean()),
    }
    return [
        {
            "evaluation_date": evaluation_date,
            "start_date": start_date,
            "end_date": end_date,
            "portfolio_policy_id": policy_id,
            "portfolio_policy_version": policy_version,
            "execution_mode": execution_mode,
            "comparison_key": comparison_key,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "sample_count": len(nav_frame),
            "notes_json": None,
            "evaluation_run_id": evaluation_run_id,
            "created_at": pd.Timestamp.now(tz="UTC"),
        }
        for metric_name, metric_value in metrics.items()
    ]


def evaluate_portfolio_policies(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    execution_modes: list[str] | None = None,
    policy_config_path: str | None = None,
) -> PortfolioEvaluationResult:
    ensure_storage_layout(settings)
    with activate_run_context("evaluate_portfolio_policies", as_of_date=end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_date,
                input_sources=["fact_portfolio_nav_snapshot"],
                notes=f"Evaluate portfolio policies through {end_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                policy, _, _ = load_active_or_default_portfolio_policy(
                    settings,
                    connection,
                    as_of_date=end_date,
                    policy_config_path=policy_config_path,
                )
                requested_modes = [str(mode).upper() for mode in (execution_modes or policy.execution_modes)]
                requested_modes = [mode for mode in requested_modes if mode in EXECUTION_MODES]
                if not requested_modes:
                    requested_modes = list(EXECUTION_MODES)
                nav = connection.execute(
                    """
                    SELECT *
                    FROM fact_portfolio_nav_snapshot
                    WHERE snapshot_date BETWEEN ? AND ?
                      AND portfolio_policy_id = ?
                      AND portfolio_policy_version = ?
                    ORDER BY snapshot_date, execution_mode
                    """,
                    [start_date, end_date, policy.portfolio_policy_id, policy.portfolio_policy_version],
                ).fetchdf()
                rows: list[dict[str, object]] = []
                for mode in requested_modes:
                    mode_frame = nav.loc[nav["execution_mode"] == mode].copy()
                    rows.extend(
                        _summary_metric_rows(
                            evaluation_date=end_date,
                            start_date=start_date,
                            end_date=end_date,
                            policy_id=policy.portfolio_policy_id,
                            policy_version=policy.portfolio_policy_version,
                            execution_mode=mode,
                            comparison_key=mode,
                            nav_frame=mode_frame,
                            evaluation_run_id=run_context.run_id,
                        )
                    )
                open_all = nav.loc[nav["execution_mode"] == "OPEN_ALL"].copy()
                if not open_all.empty:
                    baseline = open_all.copy()
                    baseline["nav_value"] = open_all["nav_value"].iloc[0] * (1.0 + open_all["daily_return"].fillna(0.0)).cumprod()
                    baseline["drawdown"] = baseline["nav_value"] / baseline["nav_value"].cummax() - 1.0
                    rows.extend(
                        _summary_metric_rows(
                            evaluation_date=end_date,
                            start_date=start_date,
                            end_date=end_date,
                            policy_id=policy.portfolio_policy_id,
                            policy_version=policy.portfolio_policy_version,
                            execution_mode="OPEN_ALL",
                            comparison_key="EQUAL_WEIGHT_BASELINE",
                            nav_frame=baseline,
                            evaluation_run_id=run_context.run_id,
                        )
                    )
                output = pd.DataFrame(rows)
                upsert_portfolio_evaluation_summary(connection, output)
                artifact_paths = []
                if not output.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                output,
                                base_dir=settings.paths.curated_dir,
                                dataset="portfolio/evaluation_summary",
                                partitions={"evaluation_date": end_date.isoformat()},
                                filename="evaluation_summary.parquet",
                            )
                        )
                    )
                notes = (
                    "Portfolio policy evaluation materialized. "
                    f"rows={len(output)} range={start_date.isoformat()}..{end_date.isoformat()}"
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
                return PortfolioEvaluationResult(
                    run_id=run_context.run_id,
                    end_date=end_date,
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
                    notes="Portfolio evaluation failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
