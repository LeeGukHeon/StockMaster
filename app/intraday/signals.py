from __future__ import annotations

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

from .common import clip_score, json_text, normalize_checkpoint_to_hhmm, quality_bucket, rank_list
from .session import load_intraday_candidate_session_frame


@dataclass(slots=True)
class IntradaySignalResult:
    run_id: str
    session_date: date
    checkpoint: str
    row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_intraday_signal_snapshot(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_signal_snapshot_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_signal_snapshot
        WHERE (session_date, symbol, horizon, checkpoint_time, ranking_version) IN (
            SELECT session_date, symbol, horizon, checkpoint_time, ranking_version
            FROM intraday_signal_snapshot_stage
        )
        """
    )
    connection.execute(
        "INSERT INTO fact_intraday_signal_snapshot SELECT * FROM intraday_signal_snapshot_stage"
    )
    connection.unregister("intraday_signal_snapshot_stage")


def _load_bar_state(connection, *, session_date: date, checkpoint: str) -> pd.DataFrame:
    return connection.execute(
        """
        WITH latest_bar AS (
            SELECT *
            FROM fact_intraday_bar_1m
            WHERE session_date = ?
              AND bar_time <= ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY bar_ts DESC
            ) = 1
        ),
        first_bar AS (
            SELECT *
            FROM fact_intraday_bar_1m
            WHERE session_date = ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY bar_ts
            ) = 1
        )
        SELECT
            latest.symbol,
            latest.open AS latest_open,
            latest.high AS latest_high,
            latest.low AS latest_low,
            latest.close AS latest_close,
            latest.volume AS latest_volume,
            latest.turnover_value AS latest_turnover,
            latest.vwap AS latest_vwap,
            latest.source AS bar_source,
            latest.data_quality AS bar_data_quality,
            first.open AS opening_price
        FROM latest_bar AS latest
        LEFT JOIN first_bar AS first
          ON latest.symbol = first.symbol
        """,
        [session_date, normalize_checkpoint_to_hhmm(checkpoint), session_date],
    ).fetchdf()


def _load_trade_summary(connection, *, session_date: date, checkpoint: str) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_trade_summary
        WHERE session_date = ?
          AND checkpoint_time = ?
        """,
        [session_date, checkpoint],
    ).fetchdf()


def _load_quote_summary(connection, *, session_date: date, checkpoint: str) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_quote_summary
        WHERE session_date = ?
          AND checkpoint_time = ?
        """,
        [session_date, checkpoint],
    ).fetchdf()


def _load_prev_close(connection, *, session_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        WITH prev_day AS (
            SELECT
                calendar.trading_date AS session_date,
                calendar.prev_trading_date AS prev_trading_date
            FROM dim_trading_calendar AS calendar
            WHERE calendar.trading_date = ?
        )
        SELECT
            price.symbol,
            price.close AS prev_close
        FROM fact_daily_ohlcv AS price
        JOIN prev_day
          ON price.trading_date = prev_day.prev_trading_date
        """,
        [session_date],
    ).fetchdf()


def _score_gap_quality(opening_price: float | None, prev_close: float | None) -> float:
    if opening_price is None or prev_close is None or prev_close <= 0:
        return 30.0
    gap_pct = opening_price / prev_close - 1.0
    return clip_score(85.0 - abs(gap_pct) * 2600.0)


def _score_micro_trend(
    current_price: float | None, opening_price: float | None, vwap: float | None
) -> float:
    if current_price is None or opening_price is None or opening_price <= 0:
        return 35.0
    open_move = current_price / opening_price - 1.0
    vwap_edge = 0.0
    if vwap not in {None, 0} and pd.notna(vwap):
        vwap_edge = current_price / float(vwap) - 1.0
    return clip_score(50.0 + open_move * 3200.0 + vwap_edge * 2200.0)


def _score_relative_activity(activity_ratio: float | None) -> float:
    if activity_ratio is None or pd.isna(activity_ratio):
        return 35.0
    return clip_score(40.0 + min(float(activity_ratio), 2.5) * 24.0)


def _score_orderbook(
    imbalance_ratio: float | None, spread_bps: float | None, *, unavailable: bool
) -> float:
    if unavailable:
        return 35.0
    imbalance_term = float(imbalance_ratio or 0.0) * 30.0
    spread_term = min(float(spread_bps or 0.0), 40.0) * 1.2
    return clip_score(55.0 + imbalance_term - spread_term)


def _score_execution_strength(execution_strength: float | None) -> float:
    if execution_strength is None or pd.isna(execution_strength):
        return 35.0
    return clip_score(float(execution_strength))


def _score_risk_friction(
    *,
    gap_score: float,
    orderbook_score: float,
    bar_quality: str | None,
    quote_available: bool,
    trade_available: bool,
) -> float:
    score = 55.0 + (gap_score - 50.0) * 0.25 + (orderbook_score - 50.0) * 0.2
    if bar_quality == "proxy":
        score -= 8.0
    if not quote_available:
        score -= 12.0
    if not trade_available:
        score -= 10.0
    return clip_score(score)


def materialize_intraday_signal_snapshots(
    settings: Settings,
    *,
    session_date: date,
    checkpoint: str,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradaySignalResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "materialize_intraday_signal_snapshots",
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
                    "fact_intraday_candidate_session",
                    "fact_intraday_bar_1m",
                    "fact_intraday_trade_summary",
                    "fact_intraday_quote_summary",
                ],
                notes=(
                    "Materialize intraday signal snapshots for "
                    f"{session_date.isoformat()} {checkpoint}"
                ),
                ranking_version=ranking_version,
            )
            try:
                candidates = load_intraday_candidate_session_frame(
                    connection,
                    session_date=session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                bar_state = _load_bar_state(
                    connection, session_date=session_date, checkpoint=checkpoint
                )
                trade_summary = _load_trade_summary(
                    connection, session_date=session_date, checkpoint=checkpoint
                )
                quote_summary = _load_quote_summary(
                    connection, session_date=session_date, checkpoint=checkpoint
                )
                prev_close = _load_prev_close(connection, session_date=session_date)

                frame = candidates.merge(bar_state, on="symbol", how="left")
                frame = frame.merge(
                    trade_summary[
                        [
                            "symbol",
                            "execution_strength",
                            "activity_ratio",
                            "trade_summary_status",
                        ]
                    ],
                    on="symbol",
                    how="left",
                )
                frame = frame.merge(
                    quote_summary[
                        [
                            "symbol",
                            "imbalance_ratio",
                            "spread_bps",
                            "quote_status",
                        ]
                    ],
                    on="symbol",
                    how="left",
                )
                frame = frame.merge(prev_close, on="symbol", how="left")

                output_rows: list[dict[str, object]] = []
                now_ts = pd.Timestamp.now(tz="UTC")
                for row in frame.itertuples(index=False):
                    fallback_flags: list[str] = []
                    latest_close = None if pd.isna(row.latest_close) else float(row.latest_close)
                    opening_price = None if pd.isna(row.opening_price) else float(row.opening_price)
                    latest_vwap = None if pd.isna(row.latest_vwap) else float(row.latest_vwap)
                    prev_close_value = None if pd.isna(row.prev_close) else float(row.prev_close)
                    activity_ratio = (
                        None if pd.isna(row.activity_ratio) else float(row.activity_ratio)
                    )
                    imbalance_ratio = (
                        None if pd.isna(row.imbalance_ratio) else float(row.imbalance_ratio)
                    )
                    spread_bps = None if pd.isna(row.spread_bps) else float(row.spread_bps)
                    execution_strength = (
                        None if pd.isna(row.execution_strength) else float(row.execution_strength)
                    )
                    quote_available = str(row.quote_status or "") != "unavailable"
                    trade_available = str(row.trade_summary_status or "") != "unavailable"
                    if row.bar_data_quality == "proxy":
                        fallback_flags.append("bar_proxy")
                    if not quote_available:
                        fallback_flags.append("quote_unavailable")
                    if not trade_available:
                        fallback_flags.append("trade_unavailable")
                    if prev_close_value is None:
                        fallback_flags.append("prev_close_missing")

                    gap_score = _score_gap_quality(opening_price, prev_close_value)
                    micro_trend_score = _score_micro_trend(latest_close, opening_price, latest_vwap)
                    activity_score = _score_relative_activity(activity_ratio)
                    orderbook_score = _score_orderbook(
                        imbalance_ratio,
                        spread_bps,
                        unavailable=not quote_available,
                    )
                    execution_score = _score_execution_strength(execution_strength)
                    risk_score = _score_risk_friction(
                        gap_score=gap_score,
                        orderbook_score=orderbook_score,
                        bar_quality=row.bar_data_quality,
                        quote_available=quote_available,
                        trade_available=trade_available,
                    )
                    signal_quality_score = clip_score(
                        100.0
                        - (15.0 if row.bar_data_quality == "proxy" else 0.0)
                        - (20.0 if not quote_available else 0.0)
                        - (18.0 if not trade_available else 0.0)
                        - (12.0 if prev_close_value is None else 0.0)
                    )
                    timing_adjustment_score = clip_score(
                        gap_score * 0.17
                        + micro_trend_score * 0.24
                        + activity_score * 0.16
                        + orderbook_score * 0.14
                        + execution_score * 0.14
                        + risk_score * 0.15
                    )
                    output_rows.append(
                        {
                            "run_id": run_context.run_id,
                            "session_date": session_date,
                            "symbol": row.symbol,
                            "horizon": int(row.horizon),
                            "checkpoint_time": checkpoint,
                            "ranking_version": ranking_version,
                            "gap_opening_quality_score": gap_score,
                            "micro_trend_score": micro_trend_score,
                            "relative_activity_score": activity_score,
                            "orderbook_score": orderbook_score,
                            "execution_strength_score": execution_score,
                            "risk_friction_score": risk_score,
                            "signal_quality_score": signal_quality_score,
                            "timing_adjustment_score": timing_adjustment_score,
                            "signal_notes_json": json_text(
                                {
                                    "opening_price": opening_price,
                                    "prev_close": prev_close_value,
                                    "latest_close": latest_close,
                                    "latest_vwap": latest_vwap,
                                    "activity_ratio": activity_ratio,
                                    "imbalance_ratio": imbalance_ratio,
                                    "spread_bps": spread_bps,
                                    "signal_quality_bucket": quality_bucket(signal_quality_score),
                                    "candidate_rank": int(row.candidate_rank),
                                }
                            ),
                            "fallback_flags_json": json_text(rank_list(fallback_flags)),
                            "created_at": now_ts,
                        }
                    )
                output = pd.DataFrame(output_rows)
                upsert_intraday_signal_snapshot(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/signal_snapshot",
                            partitions={
                                "session_date": session_date.isoformat(),
                                "checkpoint": checkpoint.replace(":", ""),
                            },
                            filename="signal_snapshot.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday signal snapshots materialized. "
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
                return IntradaySignalResult(
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
                        "Intraday signal snapshot materialization failed for "
                        f"{session_date.isoformat()} {checkpoint}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
