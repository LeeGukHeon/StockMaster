from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.builders.flow_features import build_flow_feature_frame
from app.features.builders.fundamentals_features import build_fundamentals_feature_frame
from app.features.builders.liquidity_features import build_liquidity_feature_frame
from app.features.builders.news_features import build_news_feature_frame
from app.features.builders.price_features import build_price_feature_frame
from app.features.builders.quality_features import build_data_quality_feature_frame
from app.features.constants import FEATURE_NAMES, FEATURE_VERSION
from app.features.normalization import build_feature_snapshot_frame
from app.pipelines._helpers import load_symbol_frame
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet


@dataclass(slots=True)
class FeatureStoreBuildResult:
    run_id: str
    as_of_date: date
    symbol_count: int
    feature_row_count: int
    artifact_paths: list[str]
    notes: str
    feature_version: str


def _load_feature_symbol_frame(
    connection,
    *,
    as_of_date: date,
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
            as_of_date=as_of_date,
        )

    frame = connection.execute(
        """
        SELECT
            universe.symbol,
            universe.company_name,
            universe.market,
            universe.dart_corp_code
        FROM vw_universe_active_common_stock AS universe
        JOIN fact_daily_ohlcv AS price
          ON universe.symbol = price.symbol
         AND price.trading_date = ?
        ORDER BY universe.symbol
        """,
        [as_of_date],
    ).fetchdf()
    if market.upper() != "ALL":
        frame = frame.loc[frame["market"].str.upper() == market.upper()]
    if limit_symbols is not None and limit_symbols > 0:
        frame = frame.head(limit_symbols)
    if not frame.empty:
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
        return frame.reset_index(drop=True)
    calendar_row = connection.execute(
        """
        SELECT is_trading_day
        FROM dim_trading_calendar
        WHERE trading_date = ?
        """,
        [as_of_date],
    ).fetchone()
    if calendar_row is not None and bool(calendar_row[0]):
        raise RuntimeError(
            "Feature store cannot build a market-wide snapshot because same-day OHLCV "
            f"is missing for trading date {as_of_date.isoformat()}."
        )
    return load_symbol_frame(
        connection,
        market=market,
        limit_symbols=limit_symbols,
        as_of_date=as_of_date,
    )


def _register_symbol_stage(connection, symbol_frame: pd.DataFrame) -> None:
    connection.register("feature_symbol_stage", symbol_frame[["symbol"]].drop_duplicates())


def _unregister_symbol_stage(connection) -> None:
    try:
        connection.unregister("feature_symbol_stage")
    except Exception:
        pass


def _load_ohlcv_history(connection, *, as_of_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            price.trading_date,
            price.symbol,
            symbol.market,
            price.open,
            price.high,
            price.low,
            price.close,
            price.volume,
            price.turnover_value,
            price.market_cap
        FROM fact_daily_ohlcv AS price
        JOIN feature_symbol_stage AS selected
          ON price.symbol = selected.symbol
        JOIN dim_symbol AS symbol
          ON price.symbol = symbol.symbol
        WHERE price.trading_date <= ?
        ORDER BY price.symbol, price.trading_date
        """,
        [as_of_date],
    ).fetchdf()


def _load_latest_fundamentals(connection, *, as_of_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_fundamentals_snapshot
        WHERE symbol IN (SELECT symbol FROM feature_symbol_stage)
          AND as_of_date <= ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY as_of_date DESC, disclosed_at DESC NULLS LAST, ingested_at DESC
        ) = 1
        """,
        [as_of_date],
    ).fetchdf()


def _load_investor_flow_history(connection, *, as_of_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            trading_date,
            symbol,
            market,
            foreign_net_volume,
            institution_net_volume,
            individual_net_volume,
            foreign_net_value,
            institution_net_value,
            individual_net_value
        FROM fact_investor_flow
        WHERE symbol IN (SELECT symbol FROM feature_symbol_stage)
          AND trading_date <= ?
        ORDER BY symbol, trading_date
        """,
        [as_of_date],
    ).fetchdf()


def _load_recent_news(connection, *, as_of_date: date) -> pd.DataFrame:
    start_date = as_of_date - timedelta(days=4)
    return connection.execute(
        """
        SELECT
            signal_date,
            published_at,
            symbol_candidates,
            publisher,
            tags_json,
            catalyst_score,
            match_method_json
        FROM fact_news_item
        WHERE signal_date BETWEEN ? AND ?
          AND COALESCE(symbol_candidates, '[]') <> '[]'
        ORDER BY published_at DESC
        """,
        [start_date, as_of_date],
    ).fetchdf()


def upsert_feature_snapshot(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("feature_snapshot_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_feature_snapshot
        WHERE (as_of_date, symbol, feature_name) IN (
            SELECT as_of_date, symbol, feature_name
            FROM feature_snapshot_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_feature_snapshot (
            run_id,
            as_of_date,
            symbol,
            feature_name,
            feature_value,
            feature_group,
            source_version,
            feature_rank_pct,
            feature_zscore,
            is_imputed,
            notes_json,
            created_at
        )
        SELECT
            run_id,
            as_of_date,
            symbol,
            feature_name,
            feature_value,
            feature_group,
            source_version,
            feature_rank_pct,
            feature_zscore,
            is_imputed,
            notes_json,
            created_at
        FROM feature_snapshot_stage
        """
    )
    connection.unregister("feature_snapshot_stage")


def load_feature_matrix(
    connection,
    *,
    as_of_date: date,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    include_rank_features: bool = True,
    include_zscore_features: bool = True,
) -> pd.DataFrame:
    feature_rows = connection.execute(
        """
        SELECT
            as_of_date,
            symbol,
            feature_name,
            feature_value,
            feature_rank_pct,
            feature_zscore
        FROM fact_feature_snapshot
        WHERE as_of_date = ?
        """,
        [as_of_date],
    ).fetchdf()
    if feature_rows.empty:
        return pd.DataFrame()

    symbol_frame = load_symbol_frame(
        connection,
        symbols=symbols,
        market=market,
        limit_symbols=limit_symbols,
        as_of_date=as_of_date,
    )
    if symbol_frame.empty:
        symbol_frame = connection.execute(
            """
            SELECT symbol, company_name, market, dart_corp_code
            FROM dim_symbol
            """
        ).fetchdf()
        symbol_frame["symbol"] = symbol_frame["symbol"].astype(str).str.zfill(6)
    if symbols:
        requested = [symbol.zfill(6) for symbol in symbols]
        symbol_frame = symbol_frame.loc[symbol_frame["symbol"].isin(requested)]

    feature_rows["symbol"] = feature_rows["symbol"].astype(str).str.zfill(6)
    if limit_symbols is not None and limit_symbols > 0:
        symbol_frame = symbol_frame.head(limit_symbols)
    selected_symbols = set(symbol_frame["symbol"].astype(str))
    feature_rows = feature_rows.loc[feature_rows["symbol"].isin(selected_symbols)].copy()

    matrices: list[pd.DataFrame] = [
        feature_rows.pivot(
            index="symbol",
            columns="feature_name",
            values="feature_value",
        )
    ]
    if include_rank_features:
        rank_matrix = feature_rows.pivot(
            index="symbol",
            columns="feature_name",
            values="feature_rank_pct",
        )
        rank_matrix = rank_matrix.rename(
            columns={column: f"{column}_rank_pct" for column in rank_matrix.columns}
        )
        matrices.append(rank_matrix)
    if include_zscore_features:
        zscore_matrix = feature_rows.pivot(
            index="symbol",
            columns="feature_name",
            values="feature_zscore",
        )
        zscore_matrix = zscore_matrix.rename(
            columns={column: f"{column}_zscore" for column in zscore_matrix.columns}
        )
        matrices.append(zscore_matrix)

    combined = symbol_frame.set_index("symbol").join(matrices, how="left")
    combined.insert(0, "as_of_date", as_of_date)
    return combined.reset_index()


def build_feature_store(
    settings: Settings,
    *,
    as_of_date: date,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
    dry_run: bool = False,
) -> FeatureStoreBuildResult:
    ensure_storage_layout(settings)

    with activate_run_context("build_feature_store", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_daily_ohlcv",
                    "fact_fundamentals_snapshot",
                    "fact_news_item",
                    "fact_investor_flow",
                    "dim_symbol",
                ],
                notes=f"Build feature store snapshot for {as_of_date.isoformat()}",
            )
            try:
                symbol_frame = _load_feature_symbol_frame(
                    connection,
                    as_of_date=as_of_date,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                )
                if symbol_frame.empty:
                    raise RuntimeError(
                        "No symbols available for feature building. "
                        "Run OHLCV syncs before building the feature store."
                    )

                if dry_run:
                    notes = (
                        f"Dry run only. as_of_date={as_of_date.isoformat()} "
                        f"symbol_count={len(symbol_frame)}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        feature_version=FEATURE_VERSION,
                    )
                    return FeatureStoreBuildResult(
                        run_id=run_context.run_id,
                        as_of_date=as_of_date,
                        symbol_count=len(symbol_frame),
                        feature_row_count=0,
                        artifact_paths=[],
                        notes=notes,
                        feature_version=FEATURE_VERSION,
                    )

                if force:
                    connection.execute(
                        "DELETE FROM fact_feature_snapshot WHERE as_of_date = ?",
                        [as_of_date],
                    )

                _register_symbol_stage(connection, symbol_frame)
                try:
                    ohlcv_history = _load_ohlcv_history(connection, as_of_date=as_of_date)
                    latest_fundamentals = _load_latest_fundamentals(
                        connection, as_of_date=as_of_date
                    )
                    investor_flow_history = _load_investor_flow_history(
                        connection,
                        as_of_date=as_of_date,
                    )
                    recent_news = _load_recent_news(connection, as_of_date=as_of_date)
                finally:
                    _unregister_symbol_stage(connection)

                latest_price_dates = (
                    ohlcv_history.groupby("symbol", as_index=False)["trading_date"].max()
                    if not ohlcv_history.empty
                    else pd.DataFrame(columns=["symbol", "trading_date"])
                ).rename(columns={"trading_date": "latest_price_date"})
                latest_close = (
                    ohlcv_history.loc[
                        pd.to_datetime(ohlcv_history["trading_date"]).dt.date == as_of_date,
                        [
                            "symbol",
                            "close",
                            "market_cap",
                        ],
                    ]
                    if not ohlcv_history.empty
                    else pd.DataFrame(columns=["symbol", "close", "market_cap"])
                )

                price_features = build_price_feature_frame(ohlcv_history, as_of_date=as_of_date)
                liquidity_features = build_liquidity_feature_frame(
                    ohlcv_history, as_of_date=as_of_date
                )
                fundamentals_features = build_fundamentals_feature_frame(
                    latest_fundamentals,
                    as_of_date=as_of_date,
                )
                flow_features = build_flow_feature_frame(
                    investor_flow_history,
                    ohlcv_history=ohlcv_history,
                    as_of_date=as_of_date,
                )
                news_features = build_news_feature_frame(recent_news, as_of_date=as_of_date)

                feature_matrix = (
                    symbol_frame[["symbol", "company_name", "market"]]
                    .merge(latest_price_dates, on="symbol", how="left")
                    .merge(latest_close, on="symbol", how="left")
                    .merge(price_features, on="symbol", how="left")
                    .merge(liquidity_features, on="symbol", how="left")
                    .merge(fundamentals_features, on="symbol", how="left")
                    .merge(flow_features, on="symbol", how="left")
                    .merge(news_features, on="symbol", how="left")
                )
                for feature_name in FEATURE_NAMES:
                    if feature_name not in feature_matrix.columns:
                        feature_matrix[feature_name] = pd.NA

                feature_matrix["earnings_yield_proxy"] = feature_matrix[
                    "net_income_latest"
                ] / feature_matrix["market_cap"].replace(0, pd.NA)
                feature_matrix["value_proxy_available_flag"] = (
                    feature_matrix[
                        [
                            "earnings_yield_proxy",
                            "low_debt_preference_proxy",
                            "profitability_support_proxy",
                        ]
                    ]
                    .notna()
                    .any(axis=1)
                    .astype(float)
                )
                feature_matrix["liquidity_rank_pct"] = feature_matrix.groupby("market")[
                    "adv_20"
                ].rank(
                    method="average",
                    pct=True,
                )

                quality_features = build_data_quality_feature_frame(
                    feature_matrix, as_of_date=as_of_date
                )
                feature_matrix = feature_matrix.merge(
                    quality_features, on="symbol", how="left", suffixes=("", "_dup")
                )
                feature_matrix = feature_matrix.drop(
                    columns=[column for column in feature_matrix.columns if column.endswith("_dup")]
                )
                feature_matrix.insert(0, "as_of_date", as_of_date)

                tall_snapshot = build_feature_snapshot_frame(
                    feature_matrix,
                    as_of_date=as_of_date,
                    run_id=run_context.run_id,
                    source_version=FEATURE_VERSION,
                    feature_names=FEATURE_NAMES,
                )

                upsert_feature_snapshot(connection, tall_snapshot)
                artifact_paths = [
                    str(
                        write_parquet(
                            tall_snapshot,
                            base_dir=settings.paths.curated_dir,
                            dataset="features",
                            partitions={"as_of_date": as_of_date.isoformat()},
                            filename="feature_snapshot.parquet",
                        )
                    )
                ]
                matrix_columns = [
                    column
                    for column in feature_matrix.columns
                    if column not in {"company_name", "market_cap", "latest_price_date", "close"}
                ]
                artifact_paths.append(
                    str(
                        write_parquet(
                            feature_matrix[matrix_columns],
                            base_dir=settings.paths.curated_dir,
                            dataset="features",
                            partitions={"as_of_date": as_of_date.isoformat()},
                            filename="feature_matrix.parquet",
                        )
                    )
                )

                notes = (
                    f"Feature store build completed. as_of_date={as_of_date.isoformat()}, "
                    f"symbols={len(feature_matrix)}, feature_rows={len(tall_snapshot)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    feature_version=FEATURE_VERSION,
                )
                return FeatureStoreBuildResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    symbol_count=len(feature_matrix),
                    feature_row_count=len(tall_snapshot),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    feature_version=FEATURE_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=f"Feature store build failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                    feature_version=FEATURE_VERSION,
                )
                raise
