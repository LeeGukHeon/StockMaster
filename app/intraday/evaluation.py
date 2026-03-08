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


@dataclass(slots=True)
class IntradayTimingEvaluationResult:
    run_id: str
    start_session_date: date
    end_session_date: date
    row_count: int
    matured_row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_intraday_timing_outcome(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_timing_outcome_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_timing_outcome
        WHERE (session_date, symbol, horizon, ranking_version) IN (
            SELECT session_date, symbol, horizon, ranking_version
            FROM intraday_timing_outcome_stage
        )
        """
    )
    connection.execute(
        "INSERT INTO fact_intraday_timing_outcome SELECT * FROM intraday_timing_outcome_stage"
    )
    connection.unregister("intraday_timing_outcome_stage")


def _load_evaluation_join(
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
                open AS naive_open_price
            FROM fact_intraday_bar_1m
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY session_date, symbol
                ORDER BY bar_ts
            ) = 1
        ),
        decision_ranked AS (
            SELECT
                decision.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        decision.session_date,
                        decision.symbol,
                        decision.horizon,
                        decision.ranking_version
                    ORDER BY
                        CASE WHEN decision.action = 'ENTER_NOW' THEN 0 ELSE 1 END,
                        decision.checkpoint_time
                ) AS preferred_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        decision.session_date,
                        decision.symbol,
                        decision.horizon,
                        decision.ranking_version
                    ORDER BY decision.checkpoint_time DESC
                ) AS latest_rank
            FROM fact_intraday_entry_decision AS decision
            WHERE decision.session_date BETWEEN ? AND ?
              AND decision.ranking_version = ?
              AND decision.horizon IN ({placeholders})
        ),
        chosen_decision AS (
            SELECT *
            FROM decision_ranked
            QUALIFY CASE
                WHEN SUM(CASE WHEN action = 'ENTER_NOW' THEN 1 ELSE 0 END) OVER (
                    PARTITION BY session_date, symbol, horizon, ranking_version
                ) > 0 THEN preferred_rank = 1
                ELSE latest_rank = 1
            END
        )
        SELECT
            candidate.selection_date,
            candidate.session_date,
            candidate.symbol,
            candidate.horizon,
            candidate.ranking_version,
            decision.checkpoint_time AS selected_checkpoint_time,
            decision.action AS selected_action,
            decision.entry_reference_price AS decision_entry_price,
            decision.signal_quality_score,
            first_open.naive_open_price,
            label.exit_date,
            label.exit_price,
            label.gross_forward_return,
            label.label_available_flag,
            label.exclusion_reason
        FROM fact_intraday_candidate_session AS candidate
        LEFT JOIN chosen_decision AS decision
          ON candidate.session_date = decision.session_date
         AND candidate.symbol = decision.symbol
         AND candidate.horizon = decision.horizon
         AND candidate.ranking_version = decision.ranking_version
        LEFT JOIN first_open
          ON candidate.session_date = first_open.session_date
         AND candidate.symbol = first_open.symbol
        LEFT JOIN fact_forward_return_label AS label
          ON candidate.selection_date = label.as_of_date
         AND candidate.symbol = label.symbol
         AND candidate.horizon = label.horizon
        WHERE candidate.session_date BETWEEN ? AND ?
          AND candidate.ranking_version = ?
          AND candidate.horizon IN ({placeholders})
        ORDER BY candidate.session_date, candidate.horizon, candidate.symbol
        """,
        [
            start_session_date,
            end_session_date,
            ranking_version,
            *horizons,
            start_session_date,
            end_session_date,
            ranking_version,
            *horizons,
        ],
    ).fetchdf()


def evaluate_intraday_timing_layer(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayTimingEvaluationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "evaluate_intraday_timing_layer",
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
                    "fact_intraday_bar_1m",
                    "fact_forward_return_label",
                ],
                notes=(
                    "Evaluate intraday timing layer against naive open baseline. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                ),
                ranking_version=ranking_version,
            )
            try:
                joined = _load_evaluation_join(
                    connection,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                if joined.empty:
                    notes = (
                        "No intraday candidate session rows were available for timing evaluation. "
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
                    return IntradayTimingEvaluationResult(
                        run_id=run_context.run_id,
                        start_session_date=start_session_date,
                        end_session_date=end_session_date,
                        row_count=0,
                        matured_row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                rows: list[dict[str, object]] = []
                now_ts = pd.Timestamp.now(tz="UTC")
                for _, row in joined.iterrows():
                    selected_action = str(row["selected_action"] or "DATA_INSUFFICIENT")
                    label_available = (
                        bool(row["label_available_flag"])
                        if pd.notna(row["label_available_flag"])
                        else False
                    )
                    exit_price = None if pd.isna(row["exit_price"]) else float(row["exit_price"])
                    naive_open = (
                        None if pd.isna(row["naive_open_price"]) else float(row["naive_open_price"])
                    )
                    decision_entry = (
                        None
                        if pd.isna(row["decision_entry_price"])
                        else float(row["decision_entry_price"])
                    )
                    if not label_available:
                        realized_open = None
                        realized_decision = None
                        timing_edge = None
                        outcome_status = (
                            "pending"
                            if str(row["exclusion_reason"] or "")
                            in {
                                "insufficient_future_trading_days",
                                "missing_entry_day_ohlcv",
                                "missing_exit_day_ohlcv",
                            }
                            else "unavailable"
                        )
                    else:
                        realized_open = (
                            exit_price / naive_open - 1.0 if naive_open not in {None, 0} else None
                        )
                        if selected_action == "ENTER_NOW" and decision_entry not in {None, 0}:
                            realized_decision = exit_price / decision_entry - 1.0
                            execution_flag = True
                            outcome_status = "executed"
                        elif selected_action in {
                            "AVOID_TODAY",
                            "WAIT_RECHECK",
                            "DATA_INSUFFICIENT",
                        }:
                            realized_decision = 0.0
                            execution_flag = False
                            outcome_status = selected_action.lower()
                        else:
                            realized_decision = None
                            execution_flag = False
                            outcome_status = "unavailable"
                        timing_edge = (
                            realized_decision - realized_open
                            if realized_decision is not None and realized_open is not None
                            else None
                        )
                    if not label_available:
                        execution_flag = False

                    rows.append(
                        {
                            "session_date": row["session_date"],
                            "symbol": row["symbol"],
                            "horizon": int(row["horizon"]),
                            "ranking_version": ranking_version,
                            "selection_date": row["selection_date"],
                            "selected_checkpoint_time": row["selected_checkpoint_time"],
                            "selected_action": selected_action,
                            "execution_flag": execution_flag,
                            "naive_open_price": naive_open,
                            "decision_entry_price": decision_entry,
                            "exit_trade_date": row["exit_date"],
                            "future_exit_price": exit_price,
                            "realized_return_from_open": realized_open,
                            "realized_return_from_decision": realized_decision,
                            "timing_edge_return": timing_edge,
                            "timing_edge_bps": None
                            if timing_edge is None
                            else timing_edge * 10000.0,
                            "outcome_status": outcome_status,
                            "evaluation_run_id": run_context.run_id,
                            "notes_json": json.dumps(
                                {
                                    "signal_quality_score": row["signal_quality_score"],
                                    "label_available_flag": label_available,
                                    "exclusion_reason": row["exclusion_reason"],
                                    "gross_forward_return": row["gross_forward_return"],
                                },
                                ensure_ascii=False,
                            ),
                            "created_at": now_ts,
                            "updated_at": now_ts,
                        }
                    )

                output = pd.DataFrame(rows)
                upsert_intraday_timing_outcome(connection, output)
                artifact_paths: list[str] = []
                for (session_dt, horizon), partition in output.groupby(
                    ["session_date", "horizon"], sort=True
                ):
                    artifact_paths.append(
                        str(
                            write_parquet(
                                partition,
                                base_dir=settings.paths.curated_dir,
                                dataset="intraday/timing_outcome",
                                partitions={
                                    "session_date": pd.Timestamp(session_dt).date().isoformat(),
                                    "horizon": str(int(horizon)),
                                },
                                filename="timing_outcome.parquet",
                            )
                        )
                    )

                matured_row_count = int(
                    output["outcome_status"]
                    .isin(["executed", "avoid_today", "wait_recheck", "data_insufficient"])
                    .sum()
                )
                notes = (
                    "Intraday timing evaluation completed. "
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
                return IntradayTimingEvaluationResult(
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
                        "Intraday timing evaluation failed. "
                        f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
