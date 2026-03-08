from __future__ import annotations

import math
import time as time_module
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local, today_local
from app.pipelines._helpers import write_json_payload
from app.providers.kis.client import KISProvider
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from .common import (
    DEFAULT_CHECKPOINTS,
    checkpoint_fraction,
    checkpoint_timestamp,
    iter_trading_minutes,
    json_text,
    normalize_checkpoint_to_hhmm,
)
from .session import load_intraday_candidate_session_frame


@dataclass(slots=True)
class IntradayBackfillResult:
    run_id: str
    session_date: date
    row_count: int
    symbol_count: int
    missing_symbol_count: int
    artifact_paths: list[str]
    notes: str


def upsert_intraday_bar_1m(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_bar_1m_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_bar_1m
        WHERE (session_date, symbol, bar_ts) IN (
            SELECT session_date, symbol, bar_ts
            FROM intraday_bar_1m_stage
        )
        """
    )
    connection.execute("INSERT INTO fact_intraday_bar_1m SELECT * FROM intraday_bar_1m_stage")
    connection.unregister("intraday_bar_1m_stage")


def upsert_intraday_trade_summary(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_trade_summary_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_trade_summary
        WHERE (session_date, symbol, checkpoint_time) IN (
            SELECT session_date, symbol, checkpoint_time
            FROM intraday_trade_summary_stage
        )
        """
    )
    connection.execute(
        "INSERT INTO fact_intraday_trade_summary SELECT * FROM intraday_trade_summary_stage"
    )
    connection.unregister("intraday_trade_summary_stage")


def upsert_intraday_quote_summary(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_quote_summary_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_quote_summary
        WHERE (session_date, symbol, checkpoint_time) IN (
            SELECT session_date, symbol, checkpoint_time
            FROM intraday_quote_summary_stage
        )
        """
    )
    connection.execute(
        "INSERT INTO fact_intraday_quote_summary SELECT * FROM intraday_quote_summary_stage"
    )
    connection.unregister("intraday_quote_summary_stage")


def _load_candidate_symbols(
    connection,
    *,
    session_date: date,
    horizons: list[int],
    ranking_version: str,
) -> pd.DataFrame:
    frame = load_intraday_candidate_session_frame(
        connection,
        session_date=session_date,
        horizons=horizons,
        ranking_version=ranking_version,
        unique_symbols=True,
    )
    if frame.empty:
        return frame
    return frame[["selection_date", "session_date", "symbol", "market", "company_name"]].copy()


def _normalize_live_bars(
    *,
    session_date: date,
    symbol: str,
    frame: pd.DataFrame,
    raw_json_path: str,
    fetch_latency_ms: float,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    normalized = frame.copy()
    normalized.columns = [str(column).lower() for column in normalized.columns]
    time_column = next(
        (
            key
            for key in ("stck_cntg_hour", "cntg_hour", "bsop_hour", "hour")
            if key in normalized.columns
        ),
        None,
    )
    if time_column is None:
        return pd.DataFrame()

    for target, keys in {
        "open": ("stck_oprc", "open", "oprc"),
        "high": ("stck_hgpr", "high", "hgpr"),
        "low": ("stck_lwpr", "low", "lwpr"),
        "close": ("stck_prpr", "close", "cnpr", "prpr"),
        "volume": ("cntg_vol", "volume"),
        "turnover_cum": ("acml_tr_pbmn", "turnover", "tr_pbmn"),
    }.items():
        for key in keys:
            if key in normalized.columns:
                normalized[target] = pd.to_numeric(normalized[key], errors="coerce")
                break
        else:
            normalized[target] = pd.NA

    normalized["bar_time"] = normalized[time_column].astype(str).str.zfill(6).str.slice(0, 4)
    normalized["bar_ts"] = normalized["bar_time"].map(
        lambda value: checkpoint_timestamp(session_date, f"{value[:2]}:{value[2:]}")
    )
    normalized = normalized.sort_values("bar_ts").drop_duplicates("bar_ts")
    normalized["turnover_value"] = pd.to_numeric(normalized["turnover_cum"], errors="coerce").diff()
    if not normalized.empty:
        normalized.loc[normalized.index[0], "turnover_value"] = normalized["turnover_cum"].iloc[0]
    normalized["turnover_value"] = (
        pd.to_numeric(normalized["turnover_value"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    normalized["volume"] = (
        pd.to_numeric(normalized["volume"], errors="coerce").fillna(0).clip(lower=0).astype(int)
    )
    normalized["cum_turnover"] = normalized["turnover_value"].cumsum()
    normalized["cum_volume"] = normalized["volume"].cumsum().replace(0, pd.NA)
    normalized["vwap"] = normalized["cum_turnover"] / normalized["cum_volume"]
    normalized["run_id"] = None
    normalized["session_date"] = session_date
    normalized["symbol"] = symbol
    normalized["source"] = "kis_intraday_live"
    normalized["data_quality"] = "live"
    normalized["fetch_latency_ms"] = fetch_latency_ms
    normalized["notes_json"] = json_text({"mode": "live", "raw_json_path": raw_json_path})
    normalized["created_at"] = pd.Timestamp.now(tz="UTC")
    return normalized[
        [
            "run_id",
            "session_date",
            "symbol",
            "bar_ts",
            "bar_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover_value",
            "vwap",
            "source",
            "data_quality",
            "fetch_latency_ms",
            "notes_json",
            "created_at",
        ]
    ]


def _load_daily_row(connection, *, session_date: date, symbol: str) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT trading_date, open, high, low, close, volume, turnover_value
        FROM fact_daily_ohlcv
        WHERE trading_date = ?
          AND symbol = ?
        """,
        [session_date, symbol],
    ).fetchdf()


def _deterministic_phase(symbol: str) -> float:
    return (sum(ord(char) for char in symbol) % 23) / 23.0 * math.pi


def _synthesize_bars_from_daily(
    *,
    session_date: date,
    symbol: str,
    daily_row: pd.Series,
    artifact_dir: Path,
) -> tuple[pd.DataFrame, list[str]]:
    open_price = float(daily_row["open"])
    high_price = float(daily_row["high"])
    low_price = float(daily_row["low"])
    close_price = float(daily_row["close"])
    total_volume = max(int(daily_row["volume"] or 0), 1)
    total_turnover = float(daily_row["turnover_value"] or close_price * total_volume)

    minutes = iter_trading_minutes(session_date)
    n = len(minutes)
    phase = _deterministic_phase(symbol)
    high_index = max(1, int(n * 0.18))
    low_index = max(2, int(n * 0.32))
    if close_price < open_price:
        high_index, low_index = low_index, high_index
    anchor_index = [0, high_index, low_index, n - 1]
    anchor_price = [open_price, high_price, low_price, close_price]

    path: list[float] = []
    for index in range(n):
        if index in anchor_index:
            path.append(anchor_price[anchor_index.index(index)])
            continue
        if index < min(high_index, low_index):
            left, right = 0, min(high_index, low_index)
        elif index < max(high_index, low_index):
            left, right = min(high_index, low_index), max(high_index, low_index)
        else:
            left, right = max(high_index, low_index), n - 1
        left_price = anchor_price[anchor_index.index(left)]
        right_price = anchor_price[anchor_index.index(right)]
        progress = (index - left) / max(right - left, 1)
        base = left_price + (right_price - left_price) * progress
        wiggle = math.sin(progress * math.pi + phase) * max((high_price - low_price) * 0.01, 0.02)
        path.append(min(high_price, max(low_price, base + wiggle)))

    path[0] = open_price
    path[-1] = close_price
    path[high_index] = high_price
    path[low_index] = low_price

    weights = []
    for index in range(n):
        progress = index / max(n - 1, 1)
        weight = (
            0.8
            + 0.9 * math.exp(-(((progress - 0.03) / 0.08) ** 2))
            + 0.4 * math.exp(-(((progress - 0.88) / 0.12) ** 2))
        )
        weights.append(weight)
    weight_sum = sum(weights)
    volumes = [max(1, int(round(total_volume * weight / weight_sum))) for weight in weights]
    volumes[-1] += total_volume - sum(volumes)

    rows: list[dict[str, object]] = []
    cumulative_turnover = 0.0
    cumulative_volume = 0
    previous_close = open_price
    for index, minute_ts in enumerate(minutes):
        minute_close = float(path[index])
        minute_open = previous_close if index > 0 else open_price
        wiggle = abs(minute_close - minute_open) * 0.35 + max(
            (high_price - low_price) * 0.0015, 0.01
        )
        minute_high = min(high_price, max(minute_open, minute_close) + wiggle)
        minute_low = max(low_price, min(minute_open, minute_close) - wiggle)
        volume = int(max(0, volumes[index]))
        turnover = total_turnover * (volume / max(total_volume, 1))
        cumulative_turnover += turnover
        cumulative_volume += volume
        rows.append(
            {
                "run_id": None,
                "session_date": session_date,
                "symbol": symbol,
                "bar_ts": minute_ts,
                "bar_time": minute_ts.strftime("%H%M"),
                "open": round(minute_open, 4),
                "high": round(max(minute_high, minute_open, minute_close), 4),
                "low": round(min(minute_low, minute_open, minute_close), 4),
                "close": round(minute_close, 4),
                "volume": volume,
                "turnover_value": round(turnover, 4),
                "vwap": round(cumulative_turnover / max(cumulative_volume, 1), 6),
                "source": "proxy_daily_ohlcv",
                "data_quality": "proxy",
                "fetch_latency_ms": 0.0,
                "notes_json": json_text(
                    {
                        "mode": "proxy_daily_ohlcv",
                        "daily_row": {
                            "open": open_price,
                            "high": high_price,
                            "low": low_price,
                            "close": close_price,
                            "volume": total_volume,
                        },
                    }
                ),
                "created_at": pd.Timestamp.now(tz="UTC"),
            }
        )
        previous_close = minute_close

    frame = pd.DataFrame(rows)
    metadata_path = artifact_dir / f"{symbol}.json"
    parquet_path = artifact_dir / f"{symbol}.parquet"
    write_json_payload(
        metadata_path,
        {
            "symbol": symbol,
            "session_date": session_date.isoformat(),
            "source": "proxy_daily_ohlcv",
            "bar_count": len(frame),
        },
    )
    frame.to_parquet(parquet_path, index=False)
    return frame, [str(metadata_path), str(parquet_path)]


def _bars_until_checkpoint(
    connection, *, session_date: date, symbol: str, checkpoint: str
) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_bar_1m
        WHERE session_date = ?
          AND symbol = ?
          AND bar_time <= ?
        ORDER BY bar_ts
        """,
        [session_date, symbol, normalize_checkpoint_to_hhmm(checkpoint)],
    ).fetchdf()


def _avg_daily_volume(
    connection, *, session_date: date, symbol: str, lookback_days: int = 20
) -> float:
    row = connection.execute(
        """
        SELECT AVG(volume)
        FROM (
            SELECT volume
            FROM fact_daily_ohlcv
            WHERE symbol = ?
              AND trading_date < ?
            ORDER BY trading_date DESC
            LIMIT ?
        )
        """,
        [symbol, session_date, lookback_days],
    ).fetchone()
    return float(row[0] or 0.0)


def _summarize_orderbook_probe(probe) -> dict[str, object]:
    row = probe.quote_frame.iloc[0].to_dict() if not probe.quote_frame.empty else {}
    normalized = {str(key).lower(): value for key, value in row.items()}
    best_ask = pd.to_numeric(normalized.get("askp1"), errors="coerce")
    best_bid = pd.to_numeric(normalized.get("bidp1"), errors="coerce")
    total_ask = pd.to_numeric(normalized.get("total_askp_rsqn"), errors="coerce")
    total_bid = pd.to_numeric(normalized.get("total_bidp_rsqn"), errors="coerce")
    mid = (best_ask + best_bid) / 2.0 if pd.notna(best_ask) and pd.notna(best_bid) else pd.NA
    spread_bps = (
        (best_ask - best_bid) / mid * 10000.0
        if pd.notna(best_ask) and pd.notna(best_bid) and float(mid or 0.0) > 0
        else pd.NA
    )
    imbalance = (
        (total_bid - total_ask) / (total_bid + total_ask)
        if pd.notna(total_bid) and pd.notna(total_ask) and float(total_bid + total_ask) > 0
        else pd.NA
    )
    return {
        "best_bid": None if pd.isna(best_bid) else float(best_bid),
        "best_ask": None if pd.isna(best_ask) else float(best_ask),
        "mid_price": None if pd.isna(mid) else float(mid),
        "spread_bps": None if pd.isna(spread_bps) else float(spread_bps),
        "total_bid_quantity": None if pd.isna(total_bid) else float(total_bid),
        "total_ask_quantity": None if pd.isna(total_ask) else float(total_ask),
        "imbalance_ratio": None if pd.isna(imbalance) else float(imbalance),
    }


def backfill_intraday_candidate_bars(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    ranking_version: str,
    dry_run: bool = False,
    kis_provider: KISProvider | None = None,
) -> IntradayBackfillResult:
    ensure_storage_layout(settings)
    owns_provider = kis_provider is None
    provider = kis_provider or KISProvider(settings)
    with activate_run_context(
        "backfill_intraday_candidate_bars", as_of_date=session_date
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
                    "fact_daily_ohlcv",
                    "kis_intraday_1m",
                ],
                notes=f"Backfill intraday 1m bars for {session_date.isoformat()}",
                ranking_version=ranking_version,
            )
            try:
                candidate_symbols = _load_candidate_symbols(
                    connection,
                    session_date=session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                if candidate_symbols.empty:
                    raise RuntimeError(
                        "No intraday candidate session rows found. "
                        "Run scripts/materialize_intraday_candidate_session.py first."
                    )
                if dry_run:
                    notes = (
                        "Dry run only. "
                        f"session_date={session_date.isoformat()} "
                        f"symbols={len(candidate_symbols)}"
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
                    return IntradayBackfillResult(
                        run_id=run_context.run_id,
                        session_date=session_date,
                        row_count=0,
                        symbol_count=len(candidate_symbols),
                        missing_symbol_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                today = today_local(settings.app.timezone)
                artifact_paths: list[str] = []
                frames: list[pd.DataFrame] = []
                missing_symbol_count = 0
                proxy_dir = (
                    settings.paths.raw_dir
                    / "intraday"
                    / "candidate_bar_proxy"
                    / f"session_date={session_date.isoformat()}"
                )
                proxy_dir.mkdir(parents=True, exist_ok=True)

                for row in candidate_symbols.itertuples(index=False):
                    symbol = str(row.symbol).zfill(6)
                    try:
                        if session_date == today and provider.is_configured():
                            started = time_module.perf_counter()
                            probe = provider.fetch_intraday_bars(
                                symbol=symbol, session_date=session_date
                            )
                            fetch_latency_ms = round(
                                (time_module.perf_counter() - started) * 1000.0, 3
                            )
                            normalized = _normalize_live_bars(
                                session_date=session_date,
                                symbol=symbol,
                                frame=probe.frame,
                                raw_json_path=probe.raw_json_path,
                                fetch_latency_ms=fetch_latency_ms,
                            )
                            artifact_paths.extend([probe.raw_json_path, probe.raw_parquet_path])
                        else:
                            daily_row = _load_daily_row(
                                connection, session_date=session_date, symbol=symbol
                            )
                            if daily_row.empty:
                                missing_symbol_count += 1
                                continue
                            normalized, proxy_artifacts = _synthesize_bars_from_daily(
                                session_date=session_date,
                                symbol=symbol,
                                daily_row=daily_row.iloc[0],
                                artifact_dir=proxy_dir,
                            )
                            artifact_paths.extend(proxy_artifacts)
                        if normalized.empty:
                            missing_symbol_count += 1
                            continue
                        normalized["run_id"] = run_context.run_id
                        frames.append(normalized)
                    except Exception:
                        missing_symbol_count += 1

                combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                if not combined.empty:
                    upsert_intraday_bar_1m(connection, combined)
                    artifact_paths.append(
                        str(
                            write_parquet(
                                combined,
                                base_dir=settings.paths.curated_dir,
                                dataset="intraday/bar_1m",
                                partitions={"session_date": session_date.isoformat()},
                                filename="bar_1m.parquet",
                            )
                        )
                    )

                notes = (
                    "Intraday candidate bar backfill completed. "
                    f"session_date={session_date.isoformat()} rows={len(combined)} "
                    f"symbols={len(candidate_symbols)} missing_symbols={missing_symbol_count}"
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
                return IntradayBackfillResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    row_count=len(combined),
                    symbol_count=len(candidate_symbols),
                    missing_symbol_count=missing_symbol_count,
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
                    notes=f"Intraday candidate bar backfill failed for {session_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
            finally:
                if owns_provider:
                    provider.close()


def backfill_intraday_candidate_trade_summary(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    ranking_version: str,
    checkpoint_times: list[str] | None = None,
) -> IntradayBackfillResult:
    ensure_storage_layout(settings)
    checkpoints = checkpoint_times or list(DEFAULT_CHECKPOINTS)
    with activate_run_context(
        "backfill_intraday_candidate_trade_summary",
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
                input_sources=["fact_intraday_candidate_session", "fact_intraday_bar_1m"],
                notes=f"Backfill intraday trade summary for {session_date.isoformat()}",
                ranking_version=ranking_version,
            )
            try:
                candidate_symbols = _load_candidate_symbols(
                    connection,
                    session_date=session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                rows: list[dict[str, object]] = []
                for candidate in candidate_symbols.itertuples(index=False):
                    symbol = str(candidate.symbol).zfill(6)
                    avg_daily_volume = _avg_daily_volume(
                        connection,
                        session_date=session_date,
                        symbol=symbol,
                    )
                    for checkpoint in checkpoints:
                        bars = _bars_until_checkpoint(
                            connection,
                            session_date=session_date,
                            symbol=symbol,
                            checkpoint=checkpoint,
                        )
                        if bars.empty:
                            rows.append(
                                {
                                    "run_id": run_context.run_id,
                                    "session_date": session_date,
                                    "symbol": symbol,
                                    "checkpoint_time": checkpoint,
                                    "cumulative_volume": None,
                                    "cumulative_turnover": None,
                                    "execution_strength": None,
                                    "buy_pressure_proxy": None,
                                    "sell_pressure_proxy": None,
                                    "activity_ratio": None,
                                    "trade_count_estimate": None,
                                    "trade_summary_status": "unavailable",
                                    "source": "intraday_unavailable",
                                    "fetch_latency_ms": None,
                                    "notes_json": json_text({"reason": "bar_data_missing"}),
                                    "created_at": pd.Timestamp.now(tz="UTC"),
                                }
                            )
                            continue

                        price_change = (
                            pd.to_numeric(bars["close"], errors="coerce").diff().fillna(0.0)
                        )
                        volume = pd.to_numeric(bars["volume"], errors="coerce").fillna(0.0)
                        positive_volume = float(volume.where(price_change >= 0, 0.0).sum())
                        negative_volume = float(volume.where(price_change < 0, 0.0).sum())
                        total_volume = float(volume.sum())
                        execution_strength = (
                            positive_volume / max(positive_volume + negative_volume, 1.0) * 100.0
                        )
                        expected_volume = avg_daily_volume * checkpoint_fraction(checkpoint)
                        activity_ratio = total_volume / max(expected_volume, 1.0)
                        rows.append(
                            {
                                "run_id": run_context.run_id,
                                "session_date": session_date,
                                "symbol": symbol,
                                "checkpoint_time": checkpoint,
                                "cumulative_volume": int(total_volume),
                                "cumulative_turnover": float(
                                    pd.to_numeric(bars["turnover_value"], errors="coerce")
                                    .fillna(0.0)
                                    .sum()
                                ),
                                "execution_strength": round(execution_strength, 4),
                                "buy_pressure_proxy": round(execution_strength, 4),
                                "sell_pressure_proxy": round(100.0 - execution_strength, 4),
                                "activity_ratio": round(activity_ratio, 6),
                                "trade_count_estimate": int(len(bars)),
                                "trade_summary_status": "proxy_1m_bar",
                                "source": "proxy_1m_bar",
                                "fetch_latency_ms": 0.0,
                                "notes_json": json_text(
                                    {
                                        "avg_daily_volume_20d": avg_daily_volume,
                                        "checkpoint_fraction": checkpoint_fraction(checkpoint),
                                        "bar_count": int(len(bars)),
                                    }
                                ),
                                "created_at": pd.Timestamp.now(tz="UTC"),
                            }
                        )
                frame = pd.DataFrame(rows)
                upsert_intraday_trade_summary(connection, frame)
                artifact_paths = [
                    str(
                        write_parquet(
                            frame,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/trade_summary",
                            partitions={"session_date": session_date.isoformat()},
                            filename="trade_summary.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday trade summary backfill completed. "
                    f"session_date={session_date.isoformat()} rows={len(frame)}"
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
                return IntradayBackfillResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    row_count=len(frame),
                    symbol_count=int(frame["symbol"].nunique()) if not frame.empty else 0,
                    missing_symbol_count=0,
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
                    notes=f"Intraday trade summary backfill failed for {session_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise


def backfill_intraday_candidate_quote_summary(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    ranking_version: str,
    checkpoint_times: list[str] | None = None,
    kis_provider: KISProvider | None = None,
) -> IntradayBackfillResult:
    ensure_storage_layout(settings)
    checkpoints = checkpoint_times or list(DEFAULT_CHECKPOINTS)
    owns_provider = kis_provider is None
    provider = kis_provider or KISProvider(settings)
    with activate_run_context(
        "backfill_intraday_candidate_quote_summary",
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
                input_sources=["fact_intraday_candidate_session", "kis_orderbook_snapshot"],
                notes=f"Backfill intraday quote summary for {session_date.isoformat()}",
                ranking_version=ranking_version,
            )
            try:
                candidate_symbols = _load_candidate_symbols(
                    connection,
                    session_date=session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                )
                rows: list[dict[str, object]] = []
                artifact_paths: list[str] = []
                today = today_local(settings.app.timezone)
                for candidate in candidate_symbols.itertuples(index=False):
                    symbol = str(candidate.symbol).zfill(6)
                    live_summary: dict[str, object] | None = None
                    live_notes: dict[str, object] | None = None
                    latency_ms: float | None = None
                    if session_date == today and provider.is_configured():
                        try:
                            started = time_module.perf_counter()
                            probe = provider.fetch_orderbook_snapshot(
                                symbol=symbol,
                                session_date=session_date,
                            )
                            latency_ms = round((time_module.perf_counter() - started) * 1000.0, 3)
                            live_summary = _summarize_orderbook_probe(probe)
                            live_notes = {
                                "mode": "live",
                                "raw_json_path": probe.raw_json_path,
                                "quote_parquet_path": probe.quote_parquet_path,
                                "expected_trade_parquet_path": probe.expected_trade_parquet_path,
                            }
                            artifact_paths.extend(
                                [
                                    probe.raw_json_path,
                                    probe.quote_parquet_path,
                                    probe.expected_trade_parquet_path,
                                ]
                            )
                        except Exception:
                            live_summary = None
                            live_notes = None
                            latency_ms = None

                    for checkpoint in checkpoints:
                        if live_summary is None:
                            rows.append(
                                {
                                    "run_id": run_context.run_id,
                                    "session_date": session_date,
                                    "symbol": symbol,
                                    "checkpoint_time": checkpoint,
                                    "best_bid": None,
                                    "best_ask": None,
                                    "mid_price": None,
                                    "spread_bps": None,
                                    "total_bid_quantity": None,
                                    "total_ask_quantity": None,
                                    "imbalance_ratio": None,
                                    "quote_status": "unavailable",
                                    "source": "intraday_unavailable",
                                    "fetch_latency_ms": None,
                                    "notes_json": json_text(
                                        {
                                            "reason": (
                                                "future_or_historical_session"
                                                if session_date != today
                                                else "live_snapshot_failed"
                                            )
                                        }
                                    ),
                                    "created_at": pd.Timestamp.now(tz="UTC"),
                                }
                            )
                            continue

                        rows.append(
                            {
                                "run_id": run_context.run_id,
                                "session_date": session_date,
                                "symbol": symbol,
                                "checkpoint_time": checkpoint,
                                "best_bid": live_summary["best_bid"],
                                "best_ask": live_summary["best_ask"],
                                "mid_price": live_summary["mid_price"],
                                "spread_bps": live_summary["spread_bps"],
                                "total_bid_quantity": live_summary["total_bid_quantity"],
                                "total_ask_quantity": live_summary["total_ask_quantity"],
                                "imbalance_ratio": live_summary["imbalance_ratio"],
                                "quote_status": "live_snapshot",
                                "source": "kis_orderbook_snapshot",
                                "fetch_latency_ms": latency_ms,
                                "notes_json": json_text(live_notes or {}),
                                "created_at": pd.Timestamp.now(tz="UTC"),
                            }
                        )
                frame = pd.DataFrame(rows)
                upsert_intraday_quote_summary(connection, frame)
                artifact_paths.append(
                    str(
                        write_parquet(
                            frame,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/quote_summary",
                            partitions={"session_date": session_date.isoformat()},
                            filename="quote_summary.parquet",
                        )
                    )
                )
                notes = (
                    "Intraday quote summary backfill completed. "
                    f"session_date={session_date.isoformat()} rows={len(frame)}"
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
                return IntradayBackfillResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    row_count=len(frame),
                    symbol_count=int(frame["symbol"].nunique()) if not frame.empty else 0,
                    missing_symbol_count=0,
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
                    notes=f"Intraday quote summary backfill failed for {session_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
            finally:
                if owns_provider:
                    provider.close()
