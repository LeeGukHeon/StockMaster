from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.pipelines._helpers import load_symbol_frame
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

LABEL_VERSION = "forward_label_v1"
TAKE_PROFIT_3_RETURN = 0.03
TAKE_PROFIT_5_RETURN = 0.05
STOP_LOSS_3_RETURN = -0.03
STOP_LOSS_5_RETURN = -0.05


@dataclass(slots=True)
class ForwardLabelBuildResult:
    run_id: str
    start_date: date
    end_date: date
    row_count: int
    available_row_count: int
    artifact_paths: list[str]
    notes: str
    label_version: str


def _chunk_dates(values: list[date], chunk_size: int | None) -> list[list[date]]:
    if chunk_size is None or chunk_size <= 0 or chunk_size >= len(values):
        return [values]
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


def _build_market_baseline_frame(
    price_frame: pd.DataFrame,
    *,
    as_of_dates: list[date],
    horizons: list[int],
    trading_days: list[date],
    trading_day_index: dict[date, int],
) -> pd.DataFrame:
    if price_frame.empty:
        return pd.DataFrame(
            columns=["as_of_date", "horizon", "market", "baseline_forward_return"]
        )

    baseline_rows: list[dict[str, object]] = []
    for as_of_date in as_of_dates:
        as_of_index = trading_day_index.get(as_of_date)
        if as_of_index is None:
            continue
        entry_index = as_of_index + 1
        if entry_index >= len(trading_days):
            continue
        entry_date = trading_days[entry_index]
        entry_slice = price_frame.loc[
            price_frame["trading_date"] == entry_date,
            ["symbol", "market", "open"],
        ].copy()
        if entry_slice.empty:
            continue
        for horizon in horizons:
            exit_index = entry_index + (horizon - 1)
            if exit_index >= len(trading_days):
                continue
            exit_date = trading_days[exit_index]
            exit_slice = price_frame.loc[
                price_frame["trading_date"] == exit_date,
                ["symbol", "close"],
            ].copy()
            if exit_slice.empty:
                continue
            merged = entry_slice.merge(exit_slice, on="symbol", how="inner")
            merged = merged.loc[
                merged["open"].notna()
                & merged["close"].notna()
                & merged["open"].gt(0)
                & merged["close"].gt(0)
            ].copy()
            if merged.empty:
                continue
            merged["gross_forward_return"] = merged["close"] / merged["open"] - 1.0
            baseline = (
                merged.groupby("market", as_index=False)["gross_forward_return"]
                .mean()
                .rename(columns={"gross_forward_return": "baseline_forward_return"})
            )
            baseline["as_of_date"] = as_of_date
            baseline["horizon"] = horizon
            baseline_rows.extend(baseline.to_dict("records"))

    if not baseline_rows:
        return pd.DataFrame(
            columns=["as_of_date", "horizon", "market", "baseline_forward_return"]
        )
    return pd.DataFrame(baseline_rows)


def _build_symbol_path_lookup(
    price_frame: pd.DataFrame,
) -> dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]]:
    lookup: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for symbol, group in price_frame.sort_values(["symbol", "trading_date"]).groupby(
        "symbol", sort=False
    ):
        lookup[str(symbol).zfill(6)] = (
            pd.to_datetime(group["trading_date"]).to_numpy(dtype="datetime64[D]"),
            pd.to_numeric(group["high"], errors="coerce").to_numpy(dtype=float),
            pd.to_numeric(group["low"], errors="coerce").to_numpy(dtype=float),
        )
    return lookup


def _first_touch_date_from_returns(
    dates: np.ndarray,
    returns: np.ndarray,
    threshold: float,
    *,
    above: bool,
):
    if len(dates) == 0 or len(returns) == 0:
        return None
    finite_returns = np.asarray(returns, dtype=float)
    finite_mask = np.isfinite(finite_returns)
    touch_mask = finite_returns >= threshold if above else finite_returns <= threshold
    indices = np.flatnonzero(finite_mask & touch_mask)
    if len(indices) == 0:
        return None
    return pd.Timestamp(dates[int(indices[0])]).date()


def _conservative_barrier_return(
    *,
    gross_forward_return: float,
    take_profit: float,
    stop_loss: float,
    take_profit_date,
    stop_loss_date,
) -> float:
    """Return a daily-OHLC conservative triple-barrier trade outcome.

    Daily bars do not reveal whether high or low touched first inside the same session,
    so same-day take-profit/stop-loss collisions are treated as stop-loss first.
    """

    if stop_loss_date is not None and (
        take_profit_date is None or stop_loss_date <= take_profit_date
    ):
        return float(stop_loss)
    if take_profit_date is not None:
        return float(take_profit)
    return float(gross_forward_return)


def _path_return_metrics(
    path_lookup: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]],
    *,
    symbol: str,
    entry_date: date,
    exit_date: date,
    entry_price: float,
    gross_forward_return: float,
) -> dict[str, object]:
    dates, highs, lows = path_lookup.get(str(symbol).zfill(6), (None, None, None))
    if dates is None or highs is None or lows is None:
        return {
            "max_forward_return": pd.NA,
            "min_forward_return": pd.NA,
            "take_profit_3_hit": False,
            "take_profit_3_date": pd.NA,
            "take_profit_5_hit": False,
            "take_profit_5_date": pd.NA,
            "stop_loss_3_hit": False,
            "stop_loss_3_date": pd.NA,
            "stop_loss_5_hit": False,
            "stop_loss_5_date": pd.NA,
            "path_return_tp3_sl3_conservative": float(gross_forward_return),
            "path_return_tp5_sl3_conservative": float(gross_forward_return),
        }
    start_index = int(np.searchsorted(dates, np.datetime64(entry_date), side="left"))
    end_index = int(np.searchsorted(dates, np.datetime64(exit_date), side="right"))
    high_return = highs[start_index:end_index] / float(entry_price) - 1.0
    low_return = lows[start_index:end_index] / float(entry_price) - 1.0
    path_dates = dates[start_index:end_index]
    take_profit_3_date = _first_touch_date_from_returns(
        path_dates, high_return, TAKE_PROFIT_3_RETURN, above=True
    )
    take_profit_5_date = _first_touch_date_from_returns(
        path_dates, high_return, TAKE_PROFIT_5_RETURN, above=True
    )
    stop_loss_3_date = _first_touch_date_from_returns(
        path_dates, low_return, STOP_LOSS_3_RETURN, above=False
    )
    stop_loss_5_date = _first_touch_date_from_returns(
        path_dates, low_return, STOP_LOSS_5_RETURN, above=False
    )
    has_high = bool(np.isfinite(high_return).any())
    has_low = bool(np.isfinite(low_return).any())
    return {
        "max_forward_return": float(np.nanmax(high_return)) if has_high else pd.NA,
        "min_forward_return": float(np.nanmin(low_return)) if has_low else pd.NA,
        "take_profit_3_hit": take_profit_3_date is not None,
        "take_profit_3_date": take_profit_3_date,
        "take_profit_5_hit": take_profit_5_date is not None,
        "take_profit_5_date": take_profit_5_date,
        "stop_loss_3_hit": stop_loss_3_date is not None,
        "stop_loss_3_date": stop_loss_3_date,
        "stop_loss_5_hit": stop_loss_5_date is not None,
        "stop_loss_5_date": stop_loss_5_date,
        "path_return_tp3_sl3_conservative": _conservative_barrier_return(
            gross_forward_return=gross_forward_return,
            take_profit=TAKE_PROFIT_3_RETURN,
            stop_loss=STOP_LOSS_3_RETURN,
            take_profit_date=take_profit_3_date,
            stop_loss_date=stop_loss_3_date,
        ),
        "path_return_tp5_sl3_conservative": _conservative_barrier_return(
            gross_forward_return=gross_forward_return,
            take_profit=TAKE_PROFIT_5_RETURN,
            stop_loss=STOP_LOSS_3_RETURN,
            take_profit_date=take_profit_5_date,
            stop_loss_date=stop_loss_3_date,
        ),
    }


def _load_label_symbol_frame(
    connection,
    *,
    start_date: date,
    end_date: date,
    symbols: list[str] | None,
    limit_symbols: int | None,
    market: str,
) -> pd.DataFrame:
    if symbols:
        return load_symbol_frame(
            connection,
            symbols=symbols,
            market=market,
            limit_symbols=limit_symbols,
            as_of_date=end_date,
        )

    frame = connection.execute(
        """
        SELECT DISTINCT
            universe.symbol,
            universe.company_name,
            universe.market,
            universe.dart_corp_code
        FROM vw_universe_active_common_stock AS universe
        JOIN fact_daily_ohlcv AS price
          ON universe.symbol = price.symbol
        WHERE price.trading_date BETWEEN ? AND ?
        ORDER BY universe.symbol
        """,
        [start_date, end_date],
    ).fetchdf()
    if market.upper() != "ALL":
        frame = frame.loc[frame["market"].str.upper() == market.upper()]
    if limit_symbols is not None and limit_symbols > 0:
        frame = frame.head(limit_symbols)
    if not frame.empty:
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
        return frame.reset_index(drop=True)
    return load_symbol_frame(
        connection,
        market=market,
        limit_symbols=limit_symbols,
        as_of_date=end_date,
    )


def upsert_forward_labels(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("forward_label_stage", frame)
    connection.execute(
        """
        INSERT OR REPLACE INTO fact_forward_return_label (
            run_id,
            as_of_date,
            symbol,
            horizon,
            market,
            entry_date,
            exit_date,
            entry_basis,
            exit_basis,
            entry_price,
            exit_price,
            gross_forward_return,
            max_forward_return,
            min_forward_return,
            take_profit_3_hit,
            take_profit_3_date,
            take_profit_5_hit,
            take_profit_5_date,
            stop_loss_3_hit,
            stop_loss_3_date,
            stop_loss_5_hit,
            stop_loss_5_date,
            path_return_tp3_sl3_conservative,
            path_return_tp5_sl3_conservative,
            baseline_type,
            baseline_forward_return,
            excess_forward_return,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            label_available_flag,
            exclusion_reason,
            notes_json,
            created_at
        )
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            market,
            entry_date,
            exit_date,
            entry_basis,
            exit_basis,
            entry_price,
            exit_price,
            gross_forward_return,
            max_forward_return,
            min_forward_return,
            take_profit_3_hit,
            take_profit_3_date,
            take_profit_5_hit,
            take_profit_5_date,
            stop_loss_3_hit,
            stop_loss_3_date,
            stop_loss_5_hit,
            stop_loss_5_date,
            path_return_tp3_sl3_conservative,
            path_return_tp5_sl3_conservative,
            baseline_type,
            baseline_forward_return,
            excess_forward_return,
            path_excess_return_tp3_sl3_conservative,
            path_excess_return_tp5_sl3_conservative,
            label_available_flag,
            exclusion_reason,
            notes_json,
            created_at
        FROM forward_label_stage
        """
    )
    connection.unregister("forward_label_stage")


def ensure_forward_path_label_table(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_forward_return_path_label (
            run_id VARCHAR NOT NULL,
            as_of_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            horizon INTEGER NOT NULL,
            max_forward_return DOUBLE,
            min_forward_return DOUBLE,
            take_profit_3_hit BOOLEAN,
            take_profit_3_date DATE,
            take_profit_5_hit BOOLEAN,
            take_profit_5_date DATE,
            stop_loss_3_hit BOOLEAN,
            stop_loss_3_date DATE,
            stop_loss_5_hit BOOLEAN,
            stop_loss_5_date DATE,
            path_return_tp3_sl3_conservative DOUBLE,
            path_return_tp5_sl3_conservative DOUBLE,
            path_excess_return_tp3_sl3_conservative DOUBLE,
            path_excess_return_tp5_sl3_conservative DOUBLE,
            label_available_flag BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (as_of_date, symbol, horizon)
        )
        """
    )


def upsert_forward_path_labels(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    ensure_forward_path_label_table(connection)
    stage_columns = [
        "run_id",
        "as_of_date",
        "symbol",
        "horizon",
        "max_forward_return",
        "min_forward_return",
        "take_profit_3_hit",
        "take_profit_3_date",
        "take_profit_5_hit",
        "take_profit_5_date",
        "stop_loss_3_hit",
        "stop_loss_3_date",
        "stop_loss_5_hit",
        "stop_loss_5_date",
        "path_return_tp3_sl3_conservative",
        "path_return_tp5_sl3_conservative",
        "path_excess_return_tp3_sl3_conservative",
        "path_excess_return_tp5_sl3_conservative",
        "label_available_flag",
        "created_at",
    ]
    connection.register("forward_path_label_stage", frame[stage_columns])
    try:
        column_list = ", ".join(stage_columns)
        connection.execute(
            f"""
            INSERT OR REPLACE INTO fact_forward_return_path_label ({column_list})
            SELECT {column_list}
            FROM forward_path_label_stage
            """
        )
    finally:
        connection.unregister("forward_path_label_stage")


def build_forward_labels(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
    dry_run: bool = False,
    bootstrap: bool = True,
    path_overlay_only: bool = False,
    chunk_trading_days: int | None = None,
) -> ForwardLabelBuildResult:
    ensure_storage_layout(settings)

    with activate_run_context("build_forward_labels", as_of_date=end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            if bootstrap:
                bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_daily_ohlcv", "dim_trading_calendar", "dim_symbol"],
                notes=(
                    "Build forward return labels. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()} horizons={horizons}"
                ),
            )
            try:
                trading_days = (
                    connection.execute(
                        """
                    SELECT trading_date
                    FROM dim_trading_calendar
                    WHERE is_trading_day
                    ORDER BY trading_date
                    """
                    )
                    .fetchdf()["trading_date"]
                    .tolist()
                )
                trading_days = [pd.Timestamp(value).date() for value in trading_days]
                trading_day_index = {value: index for index, value in enumerate(trading_days)}

                as_of_dates = [
                    trading_date
                    for trading_date in trading_days
                    if start_date <= trading_date <= end_date
                ]
                if not as_of_dates:
                    raise RuntimeError("No trading dates available in the requested range.")

                symbol_frame = _load_label_symbol_frame(
                    connection,
                    start_date=start_date,
                    end_date=end_date,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                )
                if symbol_frame.empty:
                    raise RuntimeError("No symbols available for label building.")

                if dry_run:
                    notes = (
                        f"Dry run only. range={start_date.isoformat()}..{end_date.isoformat()} "
                        f"dates={len(as_of_dates)} symbols={len(symbol_frame)} "
                        f"chunk_trading_days={chunk_trading_days or 'all'}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return ForwardLabelBuildResult(
                        run_id=run_context.run_id,
                        start_date=start_date,
                        end_date=end_date,
                        row_count=0,
                        available_row_count=0,
                        artifact_paths=[],
                        notes=notes,
                        label_version=LABEL_VERSION,
                    )

                future_end_index = max(
                    trading_day_index[date_value] + max(horizons)
                    for date_value in as_of_dates
                    if date_value in trading_day_index
                )
                relevant_end = trading_days[min(future_end_index, len(trading_days) - 1)]
                price_frame = connection.execute(
                    """
                    SELECT
                        price.trading_date,
                        price.symbol,
                        price.open,
                        price.high,
                        price.low,
                        price.close,
                        symbol.market
                    FROM fact_daily_ohlcv AS price
                    JOIN dim_symbol AS symbol
                      ON price.symbol = symbol.symbol
                    WHERE price.trading_date BETWEEN ? AND ?
                      AND symbol.market IN ('KOSPI', 'KOSDAQ')
                    """,
                    [start_date, relevant_end],
                ).fetchdf()

                price_frame["trading_date"] = pd.to_datetime(price_frame["trading_date"]).dt.date
                price_frame["symbol"] = price_frame["symbol"].astype(str).str.zfill(6)
                price_lookup = price_frame.set_index(["trading_date", "symbol"])[
                    ["open", "high", "low", "close"]
                ].sort_index()
                path_lookup = _build_symbol_path_lookup(price_frame)
                market_baseline = _build_market_baseline_frame(
                    price_frame,
                    as_of_dates=as_of_dates,
                    horizons=horizons,
                    trading_days=trading_days,
                    trading_day_index=trading_day_index,
                )

                artifact_paths: list[str] = []
                row_count = 0
                available_row_count = 0
                date_chunks = _chunk_dates(as_of_dates, chunk_trading_days)
                for chunk_dates in date_chunks:
                    rows: list[dict[str, object]] = []
                    for as_of_date in chunk_dates:
                        as_of_index = trading_day_index.get(as_of_date)
                        if as_of_index is None:
                            continue
                        entry_index = as_of_index + 1
                        entry_date = (
                            trading_days[entry_index] if entry_index < len(trading_days) else None
                        )
                        for horizon in horizons:
                            exit_index = entry_index + (horizon - 1)
                            exit_date = (
                                trading_days[exit_index]
                                if exit_index < len(trading_days)
                                else None
                            )
                            for row in symbol_frame.itertuples(index=False):
                                symbol = str(row.symbol).zfill(6)
                                market_name = str(row.market)
                                label_row = {
                                    "run_id": run_context.run_id,
                                    "as_of_date": as_of_date,
                                    "symbol": symbol,
                                    "horizon": int(horizon),
                                    "market": market_name,
                                    "entry_date": entry_date,
                                    "exit_date": exit_date,
                                    "entry_basis": "next_open",
                                    "exit_basis": (
                                        "same_day_close" if horizon == 1 else "future_close"
                                    ),
                                    "entry_price": pd.NA,
                                    "exit_price": pd.NA,
                                    "gross_forward_return": pd.NA,
                                    "max_forward_return": pd.NA,
                                    "min_forward_return": pd.NA,
                                    "take_profit_3_hit": pd.NA,
                                    "take_profit_3_date": pd.NA,
                                    "take_profit_5_hit": pd.NA,
                                    "take_profit_5_date": pd.NA,
                                    "stop_loss_3_hit": pd.NA,
                                    "stop_loss_3_date": pd.NA,
                                    "stop_loss_5_hit": pd.NA,
                                    "stop_loss_5_date": pd.NA,
                                    "path_return_tp3_sl3_conservative": pd.NA,
                                    "path_return_tp5_sl3_conservative": pd.NA,
                                    "baseline_type": "same_market_equal_weight",
                                    "baseline_forward_return": pd.NA,
                                    "excess_forward_return": pd.NA,
                                    "path_excess_return_tp3_sl3_conservative": pd.NA,
                                    "path_excess_return_tp5_sl3_conservative": pd.NA,
                                    "label_available_flag": False,
                                    "exclusion_reason": None,
                                    "notes_json": pd.NA,
                                    "created_at": pd.Timestamp.utcnow(),
                                }
                                if entry_date is None or exit_date is None:
                                    label_row["exclusion_reason"] = (
                                        "insufficient_future_trading_days"
                                    )
                                    rows.append(label_row)
                                    continue

                                entry_key = (entry_date, symbol)
                                exit_key = (exit_date, symbol)
                                if entry_key not in price_lookup.index:
                                    label_row["exclusion_reason"] = "missing_entry_day_ohlcv"
                                    rows.append(label_row)
                                    continue
                                if exit_key not in price_lookup.index:
                                    label_row["exclusion_reason"] = "missing_exit_day_ohlcv"
                                    rows.append(label_row)
                                    continue

                                entry_price = price_lookup.loc[entry_key, "open"]
                                exit_price = price_lookup.loc[exit_key, "close"]
                                if pd.isna(entry_price) or entry_price <= 0:
                                    label_row["exclusion_reason"] = "invalid_entry_open"
                                    rows.append(label_row)
                                    continue
                                if pd.isna(exit_price) or exit_price <= 0:
                                    label_row["exclusion_reason"] = "invalid_exit_close"
                                    rows.append(label_row)
                                    continue

                                label_row["entry_price"] = float(entry_price)
                                label_row["exit_price"] = float(exit_price)
                                label_row["gross_forward_return"] = float(
                                    exit_price / entry_price - 1.0
                                )
                                label_row.update(
                                    _path_return_metrics(
                                        path_lookup,
                                        symbol=symbol,
                                        entry_date=entry_date,
                                        exit_date=exit_date,
                                        entry_price=float(entry_price),
                                        gross_forward_return=float(
                                            label_row["gross_forward_return"]
                                        ),
                                    )
                                )
                                label_row["label_available_flag"] = True
                                rows.append(label_row)

                    label_frame = pd.DataFrame(rows)
                    if label_frame.empty:
                        continue
                    if not market_baseline.empty:
                        label_frame = label_frame.drop(columns=["baseline_forward_return"])
                        label_frame = label_frame.merge(
                            market_baseline.loc[market_baseline["as_of_date"].isin(chunk_dates)],
                            on=["as_of_date", "horizon", "market"],
                            how="left",
                        )
                        label_frame["excess_forward_return"] = (
                            label_frame["gross_forward_return"]
                            - label_frame["baseline_forward_return"]
                        )
                        label_frame["path_excess_return_tp3_sl3_conservative"] = (
                            label_frame["path_return_tp3_sl3_conservative"]
                            - label_frame["baseline_forward_return"]
                        )
                        label_frame["path_excess_return_tp5_sl3_conservative"] = (
                            label_frame["path_return_tp5_sl3_conservative"]
                            - label_frame["baseline_forward_return"]
                        )

                    if path_overlay_only:
                        upsert_forward_path_labels(connection, label_frame)
                    else:
                        upsert_forward_labels(connection, label_frame)
                        for partition_date, partition_frame in label_frame.groupby(
                            "as_of_date", sort=True
                        ):
                            artifact_paths.append(
                                str(
                                    write_parquet(
                                        partition_frame,
                                        base_dir=settings.paths.curated_dir,
                                        dataset="labels",
                                        partitions={"as_of_date": partition_date.isoformat()},
                                        filename="forward_return_labels.parquet",
                                    )
                                )
                            )
                    row_count += len(label_frame)
                    available_row_count += int(label_frame["label_available_flag"].sum())

                notes = (
                    "Forward label build completed. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()}, "
                    f"rows={row_count}, "
                    f"available={available_row_count}, "
                    f"symbols={len(symbol_frame)}, "
                    f"chunks={len(date_chunks)}, "
                    f"chunk_trading_days={chunk_trading_days or 'all'}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return ForwardLabelBuildResult(
                    run_id=run_context.run_id,
                    start_date=start_date,
                    end_date=end_date,
                    row_count=row_count,
                    available_row_count=available_row_count,
                    artifact_paths=artifact_paths,
                    notes=notes,
                    label_version=LABEL_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=(
                        "Forward label build failed. "
                        f"range={start_date.isoformat()}..{end_date.isoformat()}"
                    ),
                    error_message=str(exc),
                )
                raise
