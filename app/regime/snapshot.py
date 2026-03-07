from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.regime.classifier import classify_regime
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

REGIME_VERSION = "market_regime_v1"
MARKET_SCOPES = ("KR_ALL", "KOSPI", "KOSDAQ")


@dataclass(slots=True)
class MarketRegimeBuildResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str
    regime_version: str


def upsert_market_regime_snapshot(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("market_regime_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_market_regime_snapshot
        WHERE (as_of_date, market_scope) IN (
            SELECT as_of_date, market_scope
            FROM market_regime_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_market_regime_snapshot (
            run_id,
            as_of_date,
            market_scope,
            breadth_up_ratio,
            breadth_down_ratio,
            median_symbol_return_1d,
            median_symbol_return_5d,
            market_realized_vol_20d,
            turnover_burst_z,
            new_high_ratio_20d,
            new_low_ratio_20d,
            regime_state,
            regime_score,
            notes_json,
            created_at
        )
        SELECT
            run_id,
            as_of_date,
            market_scope,
            breadth_up_ratio,
            breadth_down_ratio,
            median_symbol_return_1d,
            median_symbol_return_5d,
            market_realized_vol_20d,
            turnover_burst_z,
            new_high_ratio_20d,
            new_low_ratio_20d,
            regime_state,
            regime_score,
            notes_json,
            created_at
        FROM market_regime_stage
        """
    )
    connection.unregister("market_regime_stage")


def _scope_filter(frame: pd.DataFrame, market_scope: str) -> pd.DataFrame:
    if market_scope == "KR_ALL":
        return frame
    return frame.loc[frame["market"].astype(str).str.upper() == market_scope]


def _compute_regime_metrics(
    history: pd.DataFrame, *, as_of_date: date, market_scope: str
) -> dict[str, object]:
    scoped = _scope_filter(history, market_scope)
    scoped = scoped.sort_values(["symbol", "trading_date"]).copy()
    scoped["trading_date"] = pd.to_datetime(scoped["trading_date"]).dt.date
    if scoped.empty:
        raise RuntimeError(f"No OHLCV history available for market scope {market_scope}.")

    group = scoped.groupby("symbol", group_keys=False)
    scoped["ret_1d"] = group["close"].pct_change()
    scoped["ret_5d"] = group["close"].pct_change(periods=5)
    scoped["close_rolling_high_20"] = group["close"].transform(
        lambda series: series.rolling(20, min_periods=max(3, min(20, len(series)))).max()
    )
    scoped["close_rolling_low_20"] = group["close"].transform(
        lambda series: series.rolling(20, min_periods=max(3, min(20, len(series)))).min()
    )
    scoped["turnover_effective"] = scoped["turnover_value"].fillna(
        scoped["close"] * scoped["volume"]
    )

    latest = scoped.loc[scoped["trading_date"] == as_of_date].copy()
    if latest.empty:
        raise RuntimeError(f"No OHLCV rows for market scope {market_scope} on {as_of_date}.")

    breadth_up_ratio = float(latest["ret_1d"].gt(0).mean())
    breadth_down_ratio = float(latest["ret_1d"].lt(0).mean())
    median_symbol_return_1d = (
        float(latest["ret_1d"].median()) if latest["ret_1d"].notna().any() else None
    )
    median_symbol_return_5d = (
        float(latest["ret_5d"].median()) if latest["ret_5d"].notna().any() else None
    )
    new_high_ratio_20d = float(latest["close"].ge(latest["close_rolling_high_20"]).mean())
    new_low_ratio_20d = float(latest["close"].le(latest["close_rolling_low_20"]).mean())

    daily_market = (
        scoped.groupby("trading_date", as_index=False)
        .agg(
            market_return=("ret_1d", "mean"),
            total_turnover=("turnover_effective", "sum"),
        )
        .sort_values("trading_date")
    )
    current_daily = daily_market.loc[daily_market["trading_date"] == as_of_date]
    rolling_market = daily_market.tail(20)
    market_realized_vol_20d = (
        float(rolling_market["market_return"].dropna().std(ddof=0))
        if rolling_market["market_return"].dropna().shape[0] >= 3
        else None
    )

    turnover_history = rolling_market["total_turnover"].dropna()
    if turnover_history.shape[0] >= 3:
        turnover_mean = float(turnover_history.mean())
        turnover_std = float(turnover_history.std(ddof=0))
        current_turnover = (
            float(current_daily["total_turnover"].iloc[0])
            if not current_daily.empty
            else turnover_mean
        )
        turnover_burst_z = (
            0.0 if turnover_std == 0 else float((current_turnover - turnover_mean) / turnover_std)
        )
    else:
        turnover_burst_z = 0.0

    classification = classify_regime(
        breadth_up_ratio=breadth_up_ratio,
        median_symbol_return_1d=median_symbol_return_1d,
        median_symbol_return_5d=median_symbol_return_5d,
        market_realized_vol_20d=market_realized_vol_20d,
        turnover_burst_z=turnover_burst_z,
        new_high_ratio_20d=new_high_ratio_20d,
        new_low_ratio_20d=new_low_ratio_20d,
    )
    return {
        "market_scope": market_scope,
        "breadth_up_ratio": breadth_up_ratio,
        "breadth_down_ratio": breadth_down_ratio,
        "median_symbol_return_1d": median_symbol_return_1d,
        "median_symbol_return_5d": median_symbol_return_5d,
        "market_realized_vol_20d": market_realized_vol_20d,
        "turnover_burst_z": turnover_burst_z,
        "new_high_ratio_20d": new_high_ratio_20d,
        "new_low_ratio_20d": new_low_ratio_20d,
        "regime_state": classification.regime_state,
        "regime_score": classification.regime_score,
        "notes_json": json.dumps({"rule_tag": classification.rule_tag}, ensure_ascii=False),
    }


def build_market_regime_snapshot(
    settings: Settings,
    *,
    as_of_date: date,
    force: bool = False,
    dry_run: bool = False,
) -> MarketRegimeBuildResult:
    ensure_storage_layout(settings)

    with activate_run_context("build_market_regime_snapshot", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_daily_ohlcv", "dim_symbol"],
                notes=f"Build market regime snapshot for {as_of_date.isoformat()}",
            )
            try:
                history = connection.execute(
                    """
                    SELECT
                        price.trading_date,
                        price.symbol,
                        symbol.market,
                        price.close,
                        price.volume,
                        price.turnover_value
                    FROM fact_daily_ohlcv AS price
                    JOIN dim_symbol AS symbol
                      ON price.symbol = symbol.symbol
                    WHERE price.trading_date <= ?
                      AND symbol.market IN ('KOSPI', 'KOSDAQ')
                    ORDER BY price.symbol, price.trading_date
                    """,
                    [as_of_date],
                ).fetchdf()
                if history.empty:
                    raise RuntimeError("No OHLCV history available to build market regime.")

                if dry_run:
                    notes = f"Dry run only. as_of_date={as_of_date.isoformat()}"
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return MarketRegimeBuildResult(
                        run_id=run_context.run_id,
                        as_of_date=as_of_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                        regime_version=REGIME_VERSION,
                    )

                rows = []
                for market_scope in MARKET_SCOPES:
                    metric_row = _compute_regime_metrics(
                        history, as_of_date=as_of_date, market_scope=market_scope
                    )
                    metric_row["run_id"] = run_context.run_id
                    metric_row["as_of_date"] = as_of_date
                    metric_row["created_at"] = pd.Timestamp.utcnow()
                    rows.append(metric_row)

                frame = pd.DataFrame(rows)
                if force:
                    connection.execute(
                        "DELETE FROM fact_market_regime_snapshot WHERE as_of_date = ?",
                        [as_of_date],
                    )
                upsert_market_regime_snapshot(connection, frame)

                artifact_paths = [
                    str(
                        write_parquet(
                            frame,
                            base_dir=settings.paths.curated_dir,
                            dataset="regime",
                            partitions={"as_of_date": as_of_date.isoformat()},
                            filename="market_regime_snapshot.parquet",
                        )
                    )
                ]
                notes = (
                    f"Market regime snapshot completed. as_of_date={as_of_date.isoformat()}, "
                    f"rows={len(frame)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return MarketRegimeBuildResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(frame),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    regime_version=REGIME_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=f"Market regime snapshot failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                )
                raise
