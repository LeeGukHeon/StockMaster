from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.intraday.adjusted_decisions import materialize_intraday_adjusted_entry_decisions
from app.labels.forward_returns import LABEL_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from .common import DEFAULT_CHECKPOINTS, INTRADAY_STRATEGY_IDS, checkpoint_timestamp, json_text


@dataclass(slots=True)
class IntradayDecisionOutcomeResult:
    run_id: str
    start_session_date: date
    end_session_date: date
    row_count: int
    matured_row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayStrategyComparisonResult:
    run_id: str
    start_session_date: date
    end_session_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayTimingCalibrationResult:
    run_id: str
    start_session_date: date
    end_session_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_intraday_strategy_result(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_strategy_result_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_strategy_result
        WHERE (session_date, symbol, horizon, strategy_id) IN (
            SELECT session_date, symbol, horizon, strategy_id
            FROM intraday_strategy_result_stage
        )
        """
    )
    connection.execute(
        "INSERT INTO fact_intraday_strategy_result SELECT * FROM intraday_strategy_result_stage"
    )
    connection.unregister("intraday_strategy_result_stage")


def upsert_intraday_strategy_comparison(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_strategy_comparison_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_strategy_comparison
        WHERE (
            start_session_date,
            end_session_date,
            horizon,
            strategy_id,
            comparison_scope,
            comparison_value,
            cutoff_checkpoint_time
        ) IN (
            SELECT
                start_session_date,
                end_session_date,
                horizon,
                strategy_id,
                comparison_scope,
                comparison_value,
                cutoff_checkpoint_time
            FROM intraday_strategy_comparison_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_strategy_comparison
        SELECT * FROM intraday_strategy_comparison_stage
        """
    )
    connection.unregister("intraday_strategy_comparison_stage")


def upsert_intraday_timing_calibration(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_timing_calibration_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_timing_calibration
        WHERE (
            window_start_date,
            window_end_date,
            horizon,
            grouping_key,
            grouping_value
        ) IN (
            SELECT
                window_start_date,
                window_end_date,
                horizon,
                grouping_key,
                grouping_value
            FROM intraday_timing_calibration_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_timing_calibration
        SELECT * FROM intraday_timing_calibration_stage
        """
    )
    connection.unregister("intraday_timing_calibration_stage")


def _normalize_outcome_status(label_available: object, exclusion_reason: object) -> str:
    if bool(label_available):
        return "matured"
    if exclusion_reason in {
        "insufficient_future_trading_days",
        "missing_entry_day_ohlcv",
        "missing_exit_day_ohlcv",
    }:
        return "pending"
    return "unavailable"


def _safe_mean(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.mean())


def _safe_median(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.median())


def _load_candidate_base(
    connection,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str,
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        WITH first_open AS (
            SELECT
                session_date,
                symbol,
                open AS baseline_open_price
            FROM fact_intraday_bar_1m
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY session_date, symbol
                ORDER BY bar_ts
            ) = 1
        )
        SELECT
            candidate.selection_date,
            candidate.session_date,
            candidate.symbol,
            candidate.market,
            candidate.company_name,
            candidate.horizon,
            candidate.ranking_version,
            candidate.candidate_rank,
            candidate.grade,
            candidate.eligible_flag,
            candidate.final_selection_value,
            candidate.final_selection_rank_pct,
            candidate.expected_excess_return,
            candidate.fallback_flag,
            label.label_available_flag,
            label.exclusion_reason,
            label.entry_date,
            label.exit_date,
            label.entry_price AS label_entry_price,
            label.exit_price,
            label.gross_forward_return AS baseline_open_return,
            label.excess_forward_return AS baseline_open_excess_return,
            label.baseline_forward_return,
            open_bar.baseline_open_price
        FROM fact_intraday_candidate_session AS candidate
        LEFT JOIN fact_forward_return_label AS label
          ON candidate.selection_date = label.as_of_date
         AND candidate.symbol = label.symbol
         AND candidate.horizon = label.horizon
        LEFT JOIN first_open AS open_bar
          ON candidate.session_date = open_bar.session_date
         AND candidate.symbol = open_bar.symbol
        WHERE candidate.session_date BETWEEN ? AND ?
          AND candidate.ranking_version = ?
          AND candidate.horizon IN ({placeholders})
        ORDER BY
            candidate.session_date,
            candidate.horizon,
            candidate.candidate_rank,
            candidate.symbol
        """,
        [start_session_date, end_session_date, ranking_version, *horizons],
    ).fetchdf()


def _load_raw_decisions(
    connection,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str,
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_entry_decision
        WHERE session_date BETWEEN ? AND ?
          AND ranking_version = ?
          AND horizon IN ({placeholders})
        ORDER BY session_date, symbol, horizon, checkpoint_time
        """,
        [start_session_date, end_session_date, ranking_version, *horizons],
    ).fetchdf()


def _load_adjusted_decisions(
    connection,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str,
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_adjusted_entry_decision
        WHERE session_date BETWEEN ? AND ?
          AND ranking_version = ?
          AND horizon IN ({placeholders})
        ORDER BY session_date, symbol, horizon, checkpoint_time
        """,
        [start_session_date, end_session_date, ranking_version, *horizons],
    ).fetchdf()


def _ensure_adjusted_decision_history(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str,
) -> list[date]:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        rows = connection.execute(
            """
            SELECT DISTINCT session_date
            FROM fact_intraday_candidate_session
            WHERE session_date BETWEEN ? AND ?
              AND ranking_version = ?
            ORDER BY session_date
            """,
            [start_session_date, end_session_date, ranking_version],
        ).fetchall()
    session_dates = [pd.Timestamp(row[0]).date() for row in rows]
    for session_date in session_dates:
        for checkpoint in DEFAULT_CHECKPOINTS:
            materialize_intraday_adjusted_entry_decisions(
                settings,
                session_date=session_date,
                checkpoint=checkpoint,
                horizons=horizons,
                ranking_version=ranking_version,
            )
    return session_dates


def _prepare_decision_groups(frame: pd.DataFrame) -> dict[tuple[date, str, int], pd.DataFrame]:
    if frame.empty:
        return {}
    grouped: dict[tuple[date, str, int], pd.DataFrame] = {}
    for key, partition in frame.groupby(["session_date", "symbol", "horizon"], sort=False):
        grouped[key] = partition.sort_values("checkpoint_time").reset_index(drop=True)
    return grouped


def _checkpoint_leq(frame: pd.DataFrame, cutoff: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.loc[frame["checkpoint_time"].astype(str) <= cutoff].copy()


def _first_enter(frame: pd.DataFrame, *, action_column: str) -> pd.Series | None:
    if frame.empty:
        return None
    enters = frame.loc[frame[action_column] == "ENTER_NOW"]
    if enters.empty:
        return None
    return enters.iloc[0]


def _exact_checkpoint(
    frame: pd.DataFrame, checkpoint: str, *, action_column: str
) -> pd.Series | None:
    if frame.empty:
        return None
    exact = frame.loc[frame["checkpoint_time"] == checkpoint]
    if exact.empty or exact.iloc[0][action_column] != "ENTER_NOW":
        return None
    return exact.iloc[0]


def _build_strategy_row(
    *,
    run_id: str,
    candidate: pd.Series,
    strategy_id: str,
    strategy_family: str,
    cutoff: str,
    chosen_row: pd.Series | None,
    last_raw: pd.Series | None,
    last_adjusted: pd.Series | None,
    executed_flag: bool,
    no_entry_flag: bool,
    skip_reason_code: str | None,
) -> dict[str, object]:
    outcome_status = _normalize_outcome_status(
        candidate["label_available_flag"],
        candidate["exclusion_reason"],
    )
    entry_checkpoint_time = chosen_row["checkpoint_time"] if chosen_row is not None else None
    entry_price = chosen_row["entry_reference_price"] if chosen_row is not None else None
    entry_timestamp = (
        checkpoint_timestamp(
            pd.Timestamp(candidate["session_date"]).date(), str(entry_checkpoint_time)
        )
        if entry_checkpoint_time
        else None
    )
    baseline_open_return = (
        None
        if pd.isna(candidate["baseline_open_return"])
        else float(candidate["baseline_open_return"])
    )
    baseline_open_excess = (
        None
        if pd.isna(candidate["baseline_open_excess_return"])
        else float(candidate["baseline_open_excess_return"])
    )
    baseline_forward_return = (
        None
        if pd.isna(candidate["baseline_forward_return"])
        else float(candidate["baseline_forward_return"])
    )
    exit_price = None if pd.isna(candidate["exit_price"]) else float(candidate["exit_price"])
    baseline_open_price = (
        None
        if pd.isna(candidate["baseline_open_price"])
        else float(candidate["baseline_open_price"])
    )
    if outcome_status != "matured":
        realized_return = None
        realized_excess_return = None
        timing_edge = None
        final_status = outcome_status
        executed_flag = False
    elif strategy_id == "SEL_V2_OPEN_ALL":
        realized_return = baseline_open_return
        realized_excess_return = baseline_open_excess
        timing_edge = 0.0 if baseline_open_return is not None else None
        executed_flag = baseline_open_price is not None
        no_entry_flag = not executed_flag
        final_status = "executed" if executed_flag else "no_entry"
    elif executed_flag and entry_price not in {None, 0} and exit_price is not None:
        realized_return = exit_price / float(entry_price) - 1.0
        realized_excess_return = (
            None if baseline_forward_return is None else realized_return - baseline_forward_return
        )
        timing_edge = (
            None if baseline_open_return is None else realized_return - baseline_open_return
        )
        final_status = "executed"
    else:
        realized_return = 0.0
        realized_excess_return = (
            None if baseline_forward_return is None else 0.0 - baseline_forward_return
        )
        timing_edge = None if baseline_open_return is None else 0.0 - baseline_open_return
        executed_flag = False
        no_entry_flag = True
        final_status = "no_entry"

    reference_adjusted = chosen_row if chosen_row is not None else last_adjusted
    reference_raw = chosen_row if chosen_row is not None else last_raw
    raw_action = (
        None
        if reference_raw is None
        else reference_raw.get("action") or reference_raw.get("raw_action")
    )
    adjusted_action = (
        None if reference_adjusted is None else reference_adjusted.get("adjusted_action")
    )
    market_regime_family = (
        None if reference_adjusted is None else reference_adjusted.get("market_regime_family")
    )
    adjustment_profile = (
        None if reference_adjusted is None else reference_adjusted.get("adjustment_profile")
    )
    eligible = (
        None if reference_adjusted is None else reference_adjusted.get("eligible_to_execute_flag")
    )
    selection_bucket = (
        None
        if reference_adjusted is None
        else reference_adjusted.get("selection_confidence_bucket")
    )
    signal_quality_flag = (
        None if reference_adjusted is None else reference_adjusted.get("signal_quality_flag")
    )
    skip_saved_loss_flag = (
        bool(no_entry_flag) and baseline_open_return is not None and baseline_open_return < 0
    )
    missed_winner_flag = (
        bool(no_entry_flag) and baseline_open_return is not None and baseline_open_return > 0
    )
    return {
        "selection_date": candidate["selection_date"],
        "session_date": candidate["session_date"],
        "symbol": candidate["symbol"],
        "market": candidate["market"],
        "company_name": candidate["company_name"],
        "horizon": int(candidate["horizon"]),
        "strategy_id": strategy_id,
        "strategy_family": strategy_family,
        "cutoff_checkpoint_time": cutoff,
        "entry_checkpoint_time": entry_checkpoint_time,
        "entry_action_source": strategy_family,
        "raw_action": raw_action,
        "adjusted_action": adjusted_action,
        "market_regime_family": market_regime_family,
        "adjustment_profile": adjustment_profile,
        "executed_flag": bool(executed_flag),
        "no_entry_flag": bool(no_entry_flag),
        "eligible_to_execute_flag": eligible,
        "entry_timestamp": entry_timestamp,
        "entry_price": entry_price,
        "exit_trade_date": candidate["exit_date"],
        "exit_price": exit_price,
        "baseline_open_price": baseline_open_price,
        "baseline_open_return": baseline_open_return,
        "baseline_open_excess_return": baseline_open_excess,
        "realized_return": realized_return,
        "realized_excess_return": realized_excess_return,
        "timing_edge_vs_open_return": timing_edge,
        "timing_edge_vs_open_bps": None if timing_edge is None else timing_edge * 10000.0,
        "skip_reason_code": skip_reason_code,
        "skip_saved_loss_flag": skip_saved_loss_flag,
        "missed_winner_flag": missed_winner_flag,
        "outcome_status": final_status,
        "source_decision_run_id": None if chosen_row is None else chosen_row.get("run_id"),
        "evaluation_run_id": run_id,
        "notes_json": json_text(
            {
                "selection_confidence_bucket": selection_bucket,
                "signal_quality_flag": signal_quality_flag,
                "label_version": LABEL_VERSION,
                "expected_excess_return": candidate["expected_excess_return"],
            }
        ),
        "created_at": pd.Timestamp.now(tz="UTC"),
        "updated_at": pd.Timestamp.now(tz="UTC"),
    }


def _strategy_rows_for_candidate(
    *,
    run_id: str,
    candidate: pd.Series,
    raw_group: pd.DataFrame,
    adjusted_group: pd.DataFrame,
    cutoff: str,
) -> list[dict[str, object]]:
    raw_window = _checkpoint_leq(raw_group, cutoff)
    adjusted_window = _checkpoint_leq(adjusted_group, cutoff)
    last_raw = raw_window.iloc[-1] if not raw_window.empty else None
    last_adjusted = adjusted_window.iloc[-1] if not adjusted_window.empty else None
    raw_enter = _first_enter(raw_window, action_column="action")
    adjusted_enter = _first_enter(adjusted_window, action_column="adjusted_action")
    adjusted_0930 = _exact_checkpoint(adjusted_window, "09:30", action_column="adjusted_action")
    adjusted_1000 = _exact_checkpoint(adjusted_window, "10:00", action_column="adjusted_action")
    return [
        _build_strategy_row(
            run_id=run_id,
            candidate=candidate,
            strategy_id="SEL_V2_OPEN_ALL",
            strategy_family="open_baseline",
            cutoff=cutoff,
            chosen_row=None,
            last_raw=last_raw,
            last_adjusted=last_adjusted,
            executed_flag=True,
            no_entry_flag=False,
            skip_reason_code=None
            if not pd.isna(candidate["baseline_open_price"])
            else "baseline_open_missing",
        ),
        _build_strategy_row(
            run_id=run_id,
            candidate=candidate,
            strategy_id="SEL_V2_TIMING_RAW_FIRST_ENTER",
            strategy_family="raw_timing",
            cutoff=cutoff,
            chosen_row=raw_enter,
            last_raw=last_raw,
            last_adjusted=last_adjusted,
            executed_flag=raw_enter is not None,
            no_entry_flag=raw_enter is None,
            skip_reason_code=None if raw_enter is not None else "no_raw_enter_before_cutoff",
        ),
        _build_strategy_row(
            run_id=run_id,
            candidate=candidate,
            strategy_id="SEL_V2_TIMING_ADJ_FIRST_ENTER",
            strategy_family="adjusted_timing",
            cutoff=cutoff,
            chosen_row=adjusted_enter,
            last_raw=last_raw,
            last_adjusted=last_adjusted,
            executed_flag=adjusted_enter is not None,
            no_entry_flag=adjusted_enter is None,
            skip_reason_code=None
            if adjusted_enter is not None
            else "no_adjusted_enter_before_cutoff",
        ),
        _build_strategy_row(
            run_id=run_id,
            candidate=candidate,
            strategy_id="SEL_V2_TIMING_ADJ_0930_ONLY",
            strategy_family="adjusted_timing_fixed",
            cutoff=cutoff,
            chosen_row=adjusted_0930,
            last_raw=last_raw,
            last_adjusted=last_adjusted,
            executed_flag=adjusted_0930 is not None,
            no_entry_flag=adjusted_0930 is None,
            skip_reason_code=None if adjusted_0930 is not None else "0930_not_enter",
        ),
        _build_strategy_row(
            run_id=run_id,
            candidate=candidate,
            strategy_id="SEL_V2_TIMING_ADJ_1000_ONLY",
            strategy_family="adjusted_timing_fixed",
            cutoff=cutoff,
            chosen_row=adjusted_1000,
            last_raw=last_raw,
            last_adjusted=last_adjusted,
            executed_flag=adjusted_1000 is not None,
            no_entry_flag=adjusted_1000 is None,
            skip_reason_code=None if adjusted_1000 is not None else "1000_not_enter",
        ),
    ]


def materialize_intraday_decision_outcomes(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    cutoff: str = "11:00",
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayDecisionOutcomeResult:
    ensure_storage_layout(settings)
    _ensure_adjusted_decision_history(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
        ranking_version=ranking_version,
    )
    with activate_run_context(
        "materialize_intraday_decision_outcomes",
        as_of_date=end_session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=[
                    "fact_intraday_candidate_session",
                    "fact_intraday_entry_decision",
                    "fact_intraday_adjusted_entry_decision",
                    "fact_forward_return_label",
                ],
                notes=(
                    "Materialize intraday decision outcomes with same-exit comparison. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                ),
                ranking_version=ranking_version,
            )
            try:
                candidates = _load_candidate_base(
                    connection,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                raw_decisions = _load_raw_decisions(
                    connection,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                adjusted_decisions = _load_adjusted_decisions(
                    connection,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                if candidates.empty:
                    notes = (
                        "No candidate session rows were available for intraday decision outcomes. "
                        f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
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
                    return IntradayDecisionOutcomeResult(
                        run_id=run_context.run_id,
                        start_session_date=start_session_date,
                        end_session_date=end_session_date,
                        row_count=0,
                        matured_row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                raw_groups = _prepare_decision_groups(raw_decisions)
                adjusted_groups = _prepare_decision_groups(adjusted_decisions)
                rows: list[dict[str, object]] = []
                for _, candidate in candidates.iterrows():
                    key = (
                        pd.Timestamp(candidate["session_date"]).date(),
                        str(candidate["symbol"]).zfill(6),
                        int(candidate["horizon"]),
                    )
                    raw_group = raw_groups.get(key, pd.DataFrame())
                    adjusted_group = adjusted_groups.get(key, pd.DataFrame())
                    rows.extend(
                        _strategy_rows_for_candidate(
                            run_id=run_context.run_id,
                            candidate=candidate,
                            raw_group=raw_group,
                            adjusted_group=adjusted_group,
                            cutoff=cutoff,
                        )
                    )
                output = pd.DataFrame(rows)
                output = output.loc[output["strategy_id"].isin(INTRADAY_STRATEGY_IDS)].copy()
                upsert_intraday_strategy_result(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/strategy_result",
                            partitions={"end_session_date": end_session_date.isoformat()},
                            filename="strategy_result.parquet",
                        )
                    )
                ]
                matured_row_count = int(
                    output["outcome_status"].isin(["executed", "no_entry"]).sum()
                )
                notes = (
                    "Intraday decision outcomes materialized. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()} "
                    f"rows={len(output)} matured={matured_row_count}"
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
                return IntradayDecisionOutcomeResult(
                    run_id=run_context.run_id,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    row_count=len(output),
                    matured_row_count=matured_row_count,
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
                        "Intraday decision outcome materialization failed. "
                        f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise


def _comparison_rows(
    frame: pd.DataFrame,
    *,
    run_id: str,
    start_session_date: date,
    end_session_date: date,
    cutoff: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    scopes: list[tuple[str, str | None]] = [
        ("all", None),
        ("regime_family", "market_regime_family"),
    ]
    for horizon, horizon_frame in frame.groupby("horizon", sort=True):
        for strategy_id, strategy_frame in horizon_frame.groupby("strategy_id", sort=True):
            for scope_name, column in scopes:
                if column is None:
                    grouped_items = [("all", strategy_frame)]
                else:
                    scoped_frame = strategy_frame.copy()
                    scoped_frame[column] = scoped_frame[column].fillna("unknown").astype(str)
                    grouped_items = list(scoped_frame.groupby(column, sort=True))
                for raw_value, partition in grouped_items:
                    matured = partition.loc[
                        partition["outcome_status"].isin(["executed", "no_entry"])
                    ]
                    no_entry = matured.loc[matured["no_entry_flag"] == True]  # noqa: E712
                    value = (
                        "all"
                        if column is None
                        else str(raw_value)
                    )
                    rows.append(
                        {
                            "start_session_date": start_session_date,
                            "end_session_date": end_session_date,
                            "horizon": int(horizon),
                            "strategy_id": str(strategy_id),
                            "comparison_scope": scope_name,
                            "comparison_value": value,
                            "cutoff_checkpoint_time": cutoff,
                            "sample_count": int(len(partition)),
                            "matured_count": int(len(matured)),
                            "executed_count": int(matured["executed_flag"].fillna(False).sum()),
                            "no_entry_count": int(matured["no_entry_flag"].fillna(False).sum()),
                            "execution_rate": matured["executed_flag"].fillna(False).mean()
                            if not matured.empty
                            else None,
                            "mean_realized_excess_return": _safe_mean(
                                matured["realized_excess_return"]
                            ),
                            "median_realized_excess_return": _safe_median(
                                matured["realized_excess_return"]
                            ),
                            "hit_rate": matured["realized_excess_return"].gt(0).mean()
                            if not matured["realized_excess_return"].dropna().empty
                            else None,
                            "mean_timing_edge_vs_open_bps": _safe_mean(
                                matured["timing_edge_vs_open_bps"]
                            ),
                            "median_timing_edge_vs_open_bps": _safe_median(
                                matured["timing_edge_vs_open_bps"]
                            ),
                            "positive_timing_edge_rate": matured["timing_edge_vs_open_bps"]
                            .gt(0)
                            .mean()
                            if not matured["timing_edge_vs_open_bps"].dropna().empty
                            else None,
                            "skip_saved_loss_rate": no_entry["skip_saved_loss_flag"]
                            .fillna(False)
                            .mean()
                            if not no_entry.empty
                            else None,
                            "missed_winner_rate": no_entry["missed_winner_flag"]
                            .fillna(False)
                            .mean()
                            if not no_entry.empty
                            else None,
                            "coverage_ok_rate": matured["outcome_status"]
                            .isin(["executed", "no_entry"])
                            .mean()
                            if not matured.empty
                            else None,
                            "evaluation_run_id": run_id,
                            "notes_json": json_text(
                                {
                                    "pending_count": int(
                                        partition["outcome_status"].eq("pending").sum()
                                    ),
                                    "unavailable_count": int(
                                        partition["outcome_status"].eq("unavailable").sum()
                                    ),
                                }
                            ),
                            "created_at": pd.Timestamp.now(tz="UTC"),
                        }
                    )
    return rows


def evaluate_intraday_strategy_comparison(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    cutoff: str = "11:00",
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayStrategyComparisonResult:
    ensure_storage_layout(settings)
    materialize_intraday_decision_outcomes(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
        cutoff=cutoff,
        ranking_version=ranking_version,
    )
    with activate_run_context(
        "evaluate_intraday_strategy_comparison",
        as_of_date=end_session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=["fact_intraday_strategy_result"],
                notes=(
                    "Evaluate intraday strategy comparison. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                ),
                ranking_version=ranking_version,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                frame = connection.execute(
                    f"""
                    SELECT *
                    FROM fact_intraday_strategy_result
                    WHERE session_date BETWEEN ? AND ?
                      AND horizon IN ({placeholders})
                    ORDER BY session_date, strategy_id, symbol
                    """,
                    [start_session_date, end_session_date, *horizons],
                ).fetchdf()
                if frame.empty:
                    notes = (
                        "No strategy result rows were available for comparison. "
                        f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
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
                    return IntradayStrategyComparisonResult(
                        run_id=run_context.run_id,
                        start_session_date=start_session_date,
                        end_session_date=end_session_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                output = pd.DataFrame(
                    _comparison_rows(
                        frame,
                        run_id=run_context.run_id,
                        start_session_date=start_session_date,
                        end_session_date=end_session_date,
                        cutoff=cutoff,
                    )
                )
                upsert_intraday_strategy_comparison(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/strategy_comparison",
                            partitions={"end_session_date": end_session_date.isoformat()},
                            filename="strategy_comparison.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday strategy comparison evaluated. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()} "
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
                return IntradayStrategyComparisonResult(
                    run_id=run_context.run_id,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
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
                        "Intraday strategy comparison failed. "
                        f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise


def materialize_intraday_timing_calibration(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayTimingCalibrationResult:
    ensure_storage_layout(settings)
    materialize_intraday_decision_outcomes(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
        ranking_version=ranking_version,
    )
    with activate_run_context(
        "materialize_intraday_timing_calibration",
        as_of_date=end_session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=["fact_intraday_strategy_result"],
                notes=(
                    "Materialize intraday timing calibration diagnostics. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                ),
                ranking_version=ranking_version,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                frame = connection.execute(
                    f"""
                    SELECT *
                    FROM fact_intraday_strategy_result
                    WHERE session_date BETWEEN ? AND ?
                      AND horizon IN ({placeholders})
                    ORDER BY session_date, strategy_id, symbol
                    """,
                    [start_session_date, end_session_date, *horizons],
                ).fetchdf()
                if frame.empty:
                    notes = (
                        "No strategy result rows were available for intraday timing calibration. "
                        f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
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
                    return IntradayTimingCalibrationResult(
                        run_id=run_context.run_id,
                        start_session_date=start_session_date,
                        end_session_date=end_session_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                frame["notes_payload"] = frame["notes_json"].map(
                    lambda value: json.loads(value) if value else {}
                )
                frame["selection_confidence_bucket"] = frame["notes_payload"].map(
                    lambda payload: payload.get("selection_confidence_bucket")
                )
                grouped_sets: list[tuple[str, pd.Series]] = [
                    ("overall", pd.Series(["all"] * len(frame), index=frame.index)),
                    ("strategy_id", frame["strategy_id"]),
                    ("regime_family", frame["market_regime_family"].fillna("unknown")),
                    (
                        "selection_confidence_bucket",
                        frame["selection_confidence_bucket"].fillna("unknown"),
                    ),
                ]
                rows: list[dict[str, object]] = []
                for grouping_key, values in grouped_sets:
                    working = frame.copy()
                    working["grouping_value"] = values
                    for (horizon, grouping_value), partition in working.groupby(
                        ["horizon", "grouping_value"],
                        sort=True,
                    ):
                        matured = partition.loc[
                            partition["outcome_status"].isin(["executed", "no_entry"])
                        ]
                        no_entry = matured.loc[matured["no_entry_flag"] == True]  # noqa: E712
                        quality_flag = "ok" if len(matured) >= 5 else "thin_sample"
                        rows.append(
                            {
                                "window_start_date": start_session_date,
                                "window_end_date": end_session_date,
                                "horizon": int(horizon),
                                "grouping_key": grouping_key,
                                "grouping_value": str(grouping_value),
                                "sample_count": int(len(partition)),
                                "executed_count": int(matured["executed_flag"].fillna(False).sum()),
                                "execution_rate": matured["executed_flag"].fillna(False).mean()
                                if not matured.empty
                                else None,
                                "mean_realized_excess_return": _safe_mean(
                                    matured["realized_excess_return"]
                                ),
                                "median_realized_excess_return": _safe_median(
                                    matured["realized_excess_return"]
                                ),
                                "hit_rate": matured["realized_excess_return"].gt(0).mean()
                                if not matured["realized_excess_return"].dropna().empty
                                else None,
                                "mean_timing_edge_vs_open_bps": _safe_mean(
                                    matured["timing_edge_vs_open_bps"]
                                ),
                                "positive_timing_edge_rate": matured["timing_edge_vs_open_bps"]
                                .gt(0)
                                .mean()
                                if not matured["timing_edge_vs_open_bps"].dropna().empty
                                else None,
                                "skip_saved_loss_rate": no_entry["skip_saved_loss_flag"]
                                .fillna(False)
                                .mean()
                                if not no_entry.empty
                                else None,
                                "missed_winner_rate": no_entry["missed_winner_flag"]
                                .fillna(False)
                                .mean()
                                if not no_entry.empty
                                else None,
                                "quality_flag": quality_flag,
                                "evaluation_run_id": run_context.run_id,
                                "notes_json": json_text(
                                    {
                                        "matured_count": int(len(matured)),
                                        "no_entry_count": int(len(no_entry)),
                                    }
                                ),
                                "created_at": pd.Timestamp.now(tz="UTC"),
                            }
                        )
                output = pd.DataFrame(rows)
                upsert_intraday_timing_calibration(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/timing_calibration",
                            partitions={"window_end_date": end_session_date.isoformat()},
                            filename="timing_calibration.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday timing calibration materialized. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()} "
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
                return IntradayTimingCalibrationResult(
                    run_id=run_context.run_id,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
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
                        "Intraday timing calibration failed. "
                        f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
