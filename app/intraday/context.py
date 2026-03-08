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

from .common import clip_score, json_text, normalize_checkpoint_to_hhmm, quality_bucket, rank_list
from .pipeline import ensure_intraday_base_pipeline


@dataclass(slots=True)
class IntradayMarketContextResult:
    run_id: str
    session_date: date
    checkpoints: list[str]
    row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_intraday_market_context_snapshot(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("intraday_market_context_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_intraday_market_context_snapshot
        WHERE (session_date, checkpoint_time, context_scope) IN (
            SELECT session_date, checkpoint_time, context_scope
            FROM intraday_market_context_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_market_context_snapshot
        SELECT * FROM intraday_market_context_stage
        """
    )
    connection.unregister("intraday_market_context_stage")


def _load_candidate_base(connection, *, session_date: date, ranking_version: str) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_candidate_session
        WHERE session_date = ?
          AND ranking_version = ?
        ORDER BY candidate_rank, horizon, symbol
        """,
        [session_date, ranking_version],
    ).fetchdf()


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
            latest.close AS latest_close,
            latest.open AS latest_open,
            latest.vwap AS latest_vwap,
            latest.fetch_latency_ms AS bar_fetch_latency_ms,
            latest.data_quality AS bar_data_quality,
            latest.source AS bar_source,
            first.open AS opening_price
        FROM latest_bar AS latest
        LEFT JOIN first_bar AS first
          ON latest.symbol = first.symbol
        """,
        [session_date, normalize_checkpoint_to_hhmm(checkpoint), session_date],
    ).fetchdf()


def _load_signal_frame(connection, *, session_date: date, checkpoint: str, ranking_version: str):
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_signal_snapshot
        WHERE session_date = ?
          AND checkpoint_time = ?
          AND ranking_version = ?
        ORDER BY horizon, symbol
        """,
        [session_date, checkpoint, ranking_version],
    ).fetchdf()


def _load_trade_frame(connection, *, session_date: date, checkpoint: str) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_trade_summary
        WHERE session_date = ?
          AND checkpoint_time = ?
        """,
        [session_date, checkpoint],
    ).fetchdf()


def _load_quote_frame(connection, *, session_date: date, checkpoint: str) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_quote_summary
        WHERE session_date = ?
          AND checkpoint_time = ?
        """,
        [session_date, checkpoint],
    ).fetchdf()


def _load_prior_regime(
    connection, *, selection_date: date | None
) -> tuple[str | None, float | None]:
    if selection_date is None:
        return None, None
    row = connection.execute(
        """
        SELECT regime_state, regime_score
        FROM fact_market_regime_snapshot
        WHERE as_of_date = ?
          AND market_scope = 'KR_ALL'
        """,
        [selection_date],
    ).fetchone()
    if row is None:
        return None, None
    regime_state = None if row[0] is None else str(row[0])
    regime_score = None if row[1] is None else float(row[1])
    return regime_state, regime_score


def _parse_signal_notes(value: object) -> dict[str, object]:
    if value in {None, ""}:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _safe_numeric_mean(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.mean())


def _safe_numeric_median(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.median())


def _safe_bool_rate(series: pd.Series) -> float | None:
    clean = series.dropna()
    if clean.empty:
        return None
    return float(clean.astype(float).mean())


def _aggregate_context_row(
    *,
    run_id: str,
    session_date: date,
    checkpoint: str,
    market_session_state: str | None,
    selection_date: date | None,
    prior_regime_state: str | None,
    prior_regime_score: float | None,
    candidate_frame: pd.DataFrame,
    signal_frame: pd.DataFrame,
    trade_frame: pd.DataFrame,
    quote_frame: pd.DataFrame,
    bar_state: pd.DataFrame,
) -> dict[str, object]:
    candidate_unique = candidate_frame.sort_values(
        ["candidate_rank", "horizon", "symbol"]
    ).drop_duplicates("symbol")
    if not signal_frame.empty:
        signal_unique = (
            signal_frame.sort_values(["horizon", "symbol"]).drop_duplicates("symbol").copy()
        )
        signal_unique["signal_notes_payload"] = signal_unique["signal_notes_json"].map(
            _parse_signal_notes
        )
        signal_unique["latest_close"] = signal_unique["signal_notes_payload"].map(
            lambda payload: payload.get("latest_close")
        )
        signal_unique["opening_price"] = signal_unique["signal_notes_payload"].map(
            lambda payload: payload.get("opening_price")
        )
        signal_unique["prev_close"] = signal_unique["signal_notes_payload"].map(
            lambda payload: payload.get("prev_close")
        )
        signal_unique["activity_ratio"] = signal_unique["signal_notes_payload"].map(
            lambda payload: payload.get("activity_ratio")
        )
    else:
        signal_unique = pd.DataFrame(columns=["symbol"])
    signal_unique = signal_unique.merge(
        bar_state,
        on="symbol",
        how="outer",
        suffixes=("", "_bar"),
    )
    if "opening_price_bar" in signal_unique.columns:
        signal_unique["opening_price"] = pd.to_numeric(
            signal_unique.get("opening_price"),
            errors="coerce",
        ).fillna(pd.to_numeric(signal_unique["opening_price_bar"], errors="coerce"))
    trade_unique = trade_frame.sort_values(["symbol"]).drop_duplicates("symbol")
    quote_unique = quote_frame.sort_values(["symbol"]).drop_duplicates("symbol")

    frame = candidate_unique.merge(signal_unique, on="symbol", how="left")
    frame = frame.merge(
        trade_unique[
            [
                "symbol",
                "execution_strength",
                "activity_ratio",
                "trade_summary_status",
                "fetch_latency_ms",
            ]
        ],
        on="symbol",
        how="left",
        suffixes=("", "_trade"),
    )
    frame = frame.merge(
        quote_unique[
            ["symbol", "spread_bps", "imbalance_ratio", "quote_status", "fetch_latency_ms"]
        ],
        on="symbol",
        how="left",
        suffixes=("", "_quote"),
    )
    if frame.empty:
        reasons = ["candidate_session_missing"]
        data_quality_flag = "weak"
        return {
            "run_id": run_id,
            "selection_date": selection_date,
            "session_date": session_date,
            "checkpoint_time": checkpoint,
            "context_scope": "market",
            "market_session_state": market_session_state,
            "prior_daily_regime_state": prior_regime_state,
            "prior_daily_regime_score": prior_regime_score,
            "candidate_count": 0,
            "advancers_count": 0,
            "decliners_count": 0,
            "market_breadth_ratio": None,
            "kospi_return_from_open": None,
            "kosdaq_return_from_open": None,
            "candidate_mean_return_from_open": None,
            "candidate_median_return_from_open": None,
            "candidate_hit_ratio_from_open": None,
            "candidate_mean_relative_volume": None,
            "candidate_mean_spread_bps": None,
            "candidate_mean_execution_strength": None,
            "candidate_mean_orderbook_imbalance": None,
            "candidate_mean_gap_score": None,
            "candidate_mean_signal_quality": None,
            "market_shock_proxy": 90.0,
            "intraday_volatility_proxy": None,
            "dispersion_proxy": None,
            "bar_coverage_ratio": 0.0,
            "trade_coverage_ratio": 0.0,
            "quote_coverage_ratio": 0.0,
            "provider_latency_ms": None,
            "data_quality_flag": data_quality_flag,
            "context_reason_codes_json": json_text(reasons),
            "source_notes_json": json_text({"checkpoint": checkpoint}),
            "created_at": pd.Timestamp.now(tz="UTC"),
        }

    opening_price = pd.to_numeric(frame["opening_price"], errors="coerce")
    latest_close = pd.to_numeric(frame["latest_close"], errors="coerce")
    prev_close = pd.to_numeric(frame["prev_close"], errors="coerce")
    frame["return_from_open"] = latest_close / opening_price - 1.0
    frame.loc[(opening_price <= 0) | opening_price.isna(), "return_from_open"] = pd.NA
    frame["gap_from_prev_close"] = opening_price / prev_close - 1.0
    frame.loc[(prev_close <= 0) | prev_close.isna(), "gap_from_prev_close"] = pd.NA
    frame["activity_ratio_final"] = pd.to_numeric(
        frame["activity_ratio_trade"].fillna(frame["activity_ratio"]),
        errors="coerce",
    )
    frame["spread_bps"] = pd.to_numeric(frame["spread_bps"], errors="coerce")
    frame["execution_strength"] = pd.to_numeric(frame["execution_strength"], errors="coerce")
    frame["imbalance_ratio"] = pd.to_numeric(frame["imbalance_ratio"], errors="coerce")
    frame["signal_quality_score"] = pd.to_numeric(frame["signal_quality_score"], errors="coerce")
    frame["gap_opening_quality_score"] = pd.to_numeric(
        frame["gap_opening_quality_score"], errors="coerce"
    )
    frame["bar_available"] = frame["latest_close"].notna()
    frame["trade_available"] = frame["trade_summary_status"].fillna("unavailable").ne("unavailable")
    frame["quote_available"] = frame["quote_status"].fillna("unavailable").ne("unavailable")
    frame["is_advancer"] = frame["return_from_open"].gt(0)
    frame["is_decliner"] = frame["return_from_open"].lt(0)

    candidate_count = int(len(frame))
    advancers_count = int(frame["is_advancer"].fillna(False).sum())
    decliners_count = int(frame["is_decliner"].fillna(False).sum())
    breadth_ratio = advancers_count / candidate_count if candidate_count else None
    bar_coverage_ratio = float(frame["bar_available"].mean()) if candidate_count else 0.0
    trade_coverage_ratio = float(frame["trade_available"].mean()) if candidate_count else 0.0
    quote_coverage_ratio = float(frame["quote_available"].mean()) if candidate_count else 0.0

    by_market: dict[str, float | None] = {}
    if "market" in frame.columns:
        for market, market_frame in frame.groupby("market", dropna=False):
            by_market[str(market)] = _safe_numeric_mean(market_frame["return_from_open"])
    latency_values = pd.concat(
        [
            pd.to_numeric(frame["bar_fetch_latency_ms"], errors="coerce"),
            pd.to_numeric(frame["fetch_latency_ms"], errors="coerce"),
            pd.to_numeric(frame["fetch_latency_ms_quote"], errors="coerce"),
        ],
        ignore_index=True,
    ).dropna()
    reasons: list[str] = []
    if bar_coverage_ratio < 0.6:
        reasons.append("bar_coverage_low")
    if trade_coverage_ratio < 0.7:
        reasons.append("trade_coverage_low")
    if quote_coverage_ratio < 0.7:
        reasons.append("quote_coverage_low")
    avg_signal_quality = _safe_numeric_mean(frame["signal_quality_score"])
    if avg_signal_quality is not None and avg_signal_quality < 55:
        reasons.append("signal_quality_soft")

    if min(bar_coverage_ratio, trade_coverage_ratio, quote_coverage_ratio) < 0.35:
        data_quality_flag = "weak"
    elif min(bar_coverage_ratio, trade_coverage_ratio, quote_coverage_ratio) < 0.75:
        data_quality_flag = "partial"
    else:
        data_quality_flag = "strong"
    if not reasons:
        reasons.append("coverage_ok")

    shock_proxy = clip_score(
        50.0
        + (0.5 - (breadth_ratio if breadth_ratio is not None else 0.5)) * 120.0
        + frame["spread_bps"].fillna(15.0).mean() * 0.8
        + (1.0 - quote_coverage_ratio) * 18.0
        + (1.0 - trade_coverage_ratio) * 15.0
    )
    dispersion_proxy = (
        None
        if frame["return_from_open"].dropna().empty
        else float(frame["return_from_open"].dropna().std())
    )
    volatility_proxy = dispersion_proxy

    return {
        "run_id": run_id,
        "selection_date": selection_date,
        "session_date": session_date,
        "checkpoint_time": checkpoint,
        "context_scope": "market",
        "market_session_state": market_session_state,
        "prior_daily_regime_state": prior_regime_state,
        "prior_daily_regime_score": prior_regime_score,
        "candidate_count": candidate_count,
        "advancers_count": advancers_count,
        "decliners_count": decliners_count,
        "market_breadth_ratio": breadth_ratio,
        "kospi_return_from_open": by_market.get("KOSPI"),
        "kosdaq_return_from_open": by_market.get("KOSDAQ"),
        "candidate_mean_return_from_open": _safe_numeric_mean(frame["return_from_open"]),
        "candidate_median_return_from_open": _safe_numeric_median(frame["return_from_open"]),
        "candidate_hit_ratio_from_open": _safe_bool_rate(frame["is_advancer"]),
        "candidate_mean_relative_volume": _safe_numeric_mean(frame["activity_ratio_final"]),
        "candidate_mean_spread_bps": _safe_numeric_mean(frame["spread_bps"]),
        "candidate_mean_execution_strength": _safe_numeric_mean(frame["execution_strength"]),
        "candidate_mean_orderbook_imbalance": _safe_numeric_mean(frame["imbalance_ratio"]),
        "candidate_mean_gap_score": _safe_numeric_mean(frame["gap_opening_quality_score"]),
        "candidate_mean_signal_quality": avg_signal_quality,
        "market_shock_proxy": shock_proxy,
        "intraday_volatility_proxy": volatility_proxy,
        "dispersion_proxy": dispersion_proxy,
        "bar_coverage_ratio": bar_coverage_ratio,
        "trade_coverage_ratio": trade_coverage_ratio,
        "quote_coverage_ratio": quote_coverage_ratio,
        "provider_latency_ms": None if latency_values.empty else float(latency_values.mean()),
        "data_quality_flag": data_quality_flag,
        "context_reason_codes_json": json_text(rank_list(reasons)),
        "source_notes_json": json_text(
            {
                "checkpoint": checkpoint,
                "signal_quality_bucket": quality_bucket(avg_signal_quality),
            }
        ),
        "created_at": pd.Timestamp.now(tz="UTC"),
    }


def materialize_intraday_market_context_snapshots(
    settings: Settings,
    *,
    session_date: date,
    checkpoints: list[str],
    ranking_version: str = SELECTION_ENGINE_VERSION,
    horizons: list[int] | None = None,
) -> IntradayMarketContextResult:
    ensure_storage_layout(settings)
    normalized_horizons = horizons or [1, 5]
    ensure_intraday_base_pipeline(
        settings,
        session_date=session_date,
        horizons=normalized_horizons,
        checkpoints=checkpoints,
        ranking_version=ranking_version,
    )
    with activate_run_context(
        "materialize_intraday_market_context_snapshots",
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
                    "fact_intraday_signal_snapshot",
                    "fact_intraday_trade_summary",
                    "fact_intraday_quote_summary",
                    "fact_intraday_bar_1m",
                    "fact_market_regime_snapshot",
                ],
                notes=(
                    "Materialize intraday market context snapshots for "
                    f"{session_date.isoformat()} checkpoints={checkpoints}"
                ),
                ranking_version=ranking_version,
            )
            try:
                candidate_frame = _load_candidate_base(
                    connection,
                    session_date=session_date,
                    ranking_version=ranking_version,
                )
                selection_date = None
                market_session_state = None
                if not candidate_frame.empty:
                    selection_date = pd.Timestamp(candidate_frame["selection_date"].iloc[0]).date()
                    market_session_state = str(candidate_frame["session_status"].iloc[0])
                prior_regime_state, prior_regime_score = _load_prior_regime(
                    connection,
                    selection_date=selection_date,
                )
                rows: list[dict[str, object]] = []
                for checkpoint in checkpoints:
                    signal_frame = _load_signal_frame(
                        connection,
                        session_date=session_date,
                        checkpoint=checkpoint,
                        ranking_version=ranking_version,
                    )
                    trade_frame = _load_trade_frame(
                        connection,
                        session_date=session_date,
                        checkpoint=checkpoint,
                    )
                    quote_frame = _load_quote_frame(
                        connection,
                        session_date=session_date,
                        checkpoint=checkpoint,
                    )
                    bar_state = _load_bar_state(
                        connection,
                        session_date=session_date,
                        checkpoint=checkpoint,
                    )
                    rows.append(
                        _aggregate_context_row(
                            run_id=run_context.run_id,
                            session_date=session_date,
                            checkpoint=checkpoint,
                            market_session_state=market_session_state,
                            selection_date=selection_date,
                            prior_regime_state=prior_regime_state,
                            prior_regime_score=prior_regime_score,
                            candidate_frame=candidate_frame,
                            signal_frame=signal_frame,
                            trade_frame=trade_frame,
                            quote_frame=quote_frame,
                            bar_state=bar_state,
                        )
                    )
                output = pd.DataFrame(rows)
                upsert_intraday_market_context_snapshot(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/market_context",
                            partitions={"session_date": session_date.isoformat()},
                            filename="market_context_snapshot.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday market context snapshots materialized. "
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
                return IntradayMarketContextResult(
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
                        "Intraday market context snapshot materialization failed for "
                        f"{session_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
