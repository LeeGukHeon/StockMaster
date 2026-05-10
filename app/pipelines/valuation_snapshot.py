from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.domain.valuation import (
    ConfidenceInput,
    ValuationMetricInput,
    assign_valuation_label,
    build_sector_valuation_baseline,
    calculate_valuation_metrics,
    evaluate_confidence,
    select_share_source,
)
from app.providers.dart.client import DartProvider
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class ValuationSnapshotResult:
    run_id: str
    as_of_date: date
    requested_symbol_count: int
    ingredient_row_count: int
    metric_row_count: int
    baseline_row_count: int
    label_row_count: int
    skipped_symbol_count: int
    failed_symbol_count: int
    notes: str


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _normalize_symbols(symbols: list[str] | None) -> list[str] | None:
    if not symbols:
        return None
    return [str(symbol).strip().zfill(6) for symbol in symbols if str(symbol).strip()]


def _symbol_filter_sql(symbols: list[str] | None) -> tuple[str, list[object]]:
    if not symbols:
        return "", []
    placeholders = ", ".join(["?"] * len(symbols))
    return f" AND symbol IN ({placeholders})", list(symbols)


def _load_symbol_metadata(
    connection, symbols: list[str] | None, limit_symbols: int | None
) -> pd.DataFrame:
    filter_sql, params = _symbol_filter_sql(symbols)
    query = f"""
        SELECT *
        FROM dim_symbol
        WHERE COALESCE(is_common_stock, TRUE)
          AND NOT COALESCE(is_etf, FALSE)
          AND NOT COALESCE(is_etn, FALSE)
          AND NOT COALESCE(is_spac, FALSE)
          AND NOT COALESCE(is_reit, FALSE)
          AND NOT COALESCE(is_delisted, FALSE)
          {filter_sql}
        ORDER BY symbol
    """
    frame = connection.execute(query, params).fetchdf()
    if limit_symbols is not None and limit_symbols > 0:
        frame = frame.head(int(limit_symbols))
    if not frame.empty:
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    return frame


def _load_latest_fundamentals(connection, *, as_of_date: date, symbols: list[str]) -> pd.DataFrame:
    filter_sql, params = _symbol_filter_sql(symbols)
    query = f"""
        SELECT *
        FROM fact_fundamentals_snapshot
        WHERE as_of_date <= ?
          {filter_sql}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY as_of_date DESC, disclosed_at DESC NULLS LAST, ingested_at DESC
        ) = 1
    """
    frame = connection.execute(query, [as_of_date, *params]).fetchdf()
    if not frame.empty:
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    return frame


def _load_latest_prices(connection, *, as_of_date: date, symbols: list[str]) -> pd.DataFrame:
    filter_sql, params = _symbol_filter_sql(symbols)
    query = f"""
        SELECT *
        FROM fact_daily_ohlcv
        WHERE trading_date <= ?
          {filter_sql}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY trading_date DESC, ingested_at DESC
        ) = 1
    """
    frame = connection.execute(query, [as_of_date, *params]).fetchdf()
    if not frame.empty:
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    return frame


def _scalar(row: pd.Series | None, name: str) -> Any:
    if row is None or name not in row.index:
        return None
    value = row.get(name)
    if pd.isna(value):
        return None
    return value


def _days_between(as_of_date: date, value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return max(0, (as_of_date - pd.Timestamp(value).date()).days)


def _upsert_frame(
    connection, *, table: str, frame: pd.DataFrame, key_columns: tuple[str, ...]
) -> None:
    if frame.empty:
        return
    stage = f"{table}_stage"
    connection.register(stage, frame)
    where = " AND ".join([f"target.{column} = stage.{column}" for column in key_columns])
    connection.execute(f"DELETE FROM {table} AS target USING {stage} AS stage WHERE {where}")
    columns = list(frame.columns)
    column_sql = ", ".join(columns)
    select_sql = ", ".join(columns)
    connection.execute(f"INSERT INTO {table} ({column_sql}) SELECT {select_sql} FROM {stage}")
    connection.unregister(stage)


def _wide_metric_frame(
    metric_rows: list[dict[str, object]], metadata: pd.DataFrame
) -> pd.DataFrame:
    if not metric_rows:
        return pd.DataFrame()
    metric_frame = pd.DataFrame(metric_rows)
    wide = metric_frame.pivot_table(
        index="symbol",
        columns="metric_name",
        values="metric_value",
        aggfunc="first",
    ).reset_index()
    return metadata.merge(wide, on="symbol", how="left")


def materialize_valuation_snapshot(
    settings: Settings,
    *,
    as_of_date: date,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    force: bool = False,
    dry_run: bool = False,
    dart_provider: DartProvider | None = None,
) -> ValuationSnapshotResult:
    ensure_storage_layout(settings)
    normalized_symbols = _normalize_symbols(symbols)
    owns_provider = dart_provider is None
    provider = dart_provider or DartProvider(settings)

    with activate_run_context(
        "materialize_valuation_snapshot", as_of_date=as_of_date
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_fundamentals_snapshot",
                    "fact_daily_ohlcv",
                    "dart_stockTotqySttus",
                    "dim_symbol",
                ],
                notes=f"Materialize valuation snapshot for {as_of_date.isoformat()}",
            )
            try:
                metadata = _load_symbol_metadata(connection, normalized_symbols, limit_symbols)
                symbols_to_process = (
                    metadata["symbol"].astype(str).str.zfill(6).tolist()
                    if not metadata.empty
                    else []
                )
                requested_count = len(symbols_to_process)
                if not force and symbols_to_process:
                    existing = {
                        str(row[0]).zfill(6)
                        for row in connection.execute(
                            """
                            SELECT symbol
                            FROM fact_valuation_label_snapshot
                            WHERE as_of_date = ?
                            """,
                            [as_of_date],
                        ).fetchall()
                    }
                    symbols_to_process = [
                        symbol for symbol in symbols_to_process if symbol not in existing
                    ]
                    metadata = metadata.loc[metadata["symbol"].isin(symbols_to_process)].copy()
                if dry_run:
                    notes = (
                        f"Dry run only. as_of_date={as_of_date.isoformat()} "
                        f"symbols={requested_count}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return ValuationSnapshotResult(
                        run_context.run_id, as_of_date, requested_count, 0, 0, 0, 0, 0, 0, notes
                    )

                fundamentals = (
                    _load_latest_fundamentals(
                        connection,
                        as_of_date=as_of_date,
                        symbols=symbols_to_process,
                    )
                    if symbols_to_process
                    else pd.DataFrame()
                )
                prices = (
                    _load_latest_prices(
                        connection,
                        as_of_date=as_of_date,
                        symbols=symbols_to_process,
                    )
                    if symbols_to_process
                    else pd.DataFrame()
                )
                fund_by_symbol = (
                    {
                        str(row.symbol).zfill(6): row._asdict()
                        for row in fundamentals.itertuples(index=False)
                    }
                    if not fundamentals.empty
                    else {}
                )
                price_by_symbol = (
                    {
                        str(row.symbol).zfill(6): row._asdict()
                        for row in prices.itertuples(index=False)
                    }
                    if not prices.empty
                    else {}
                )

                ingredient_rows: list[dict[str, object]] = []
                metric_rows: list[dict[str, object]] = []
                failed_symbol_count = 0
                skipped_symbol_count = 0
                created_at = now_local(settings.app.timezone)

                for meta in metadata.itertuples(index=False):
                    symbol = str(meta.symbol).zfill(6)
                    fund = pd.Series(fund_by_symbol.get(symbol, {}))
                    price = pd.Series(price_by_symbol.get(symbol, {}))
                    if fund.empty or price.empty:
                        skipped_symbol_count += 1
                        continue
                    try:
                        share_snapshot = provider.fetch_stock_total_status(
                            corp_code=str(getattr(meta, "dart_corp_code", "") or ""),
                            bsns_year=int(_scalar(fund, "fiscal_year")),
                            reprt_code=str(_scalar(fund, "report_code")),
                        )
                        share_selection = select_share_source(share_snapshot.frame)
                    except Exception:
                        failed_symbol_count += 1
                        share_selection = select_share_source([])

                    lineage = {
                        "financial_source": _scalar(fund, "source"),
                        "source_doc_id": _scalar(fund, "source_doc_id"),
                        "statement_basis": _scalar(fund, "statement_basis"),
                        "share_source": "dart_stockTotqySttus",
                        "share_selection": share_selection.lineage(),
                        "price_source": _scalar(price, "source"),
                        "basis_date": _scalar(price, "trading_date"),
                    }
                    metric_input = ValuationMetricInput(
                        net_income=_scalar(fund, "net_income"),
                        equity=_scalar(fund, "equity"),
                        revenue=_scalar(fund, "revenue"),
                        operating_income=_scalar(fund, "operating_income"),
                        liabilities=_scalar(fund, "liabilities"),
                        shares_outstanding=share_selection.share_count,
                        basis_close=_scalar(price, "close"),
                        currency=_scalar(fund, "currency") or "KRW",
                        official_market_cap=_scalar(price, "market_cap"),
                        source_lineage=lineage,
                    )
                    metrics = calculate_valuation_metrics(metric_input)
                    ingredient_rows.append(
                        {
                            "as_of_date": as_of_date,
                            "symbol": symbol,
                            "fiscal_year": _scalar(fund, "fiscal_year"),
                            "report_code": _scalar(fund, "report_code"),
                            "statement_basis": _scalar(fund, "statement_basis"),
                            "source_doc_id": _scalar(fund, "source_doc_id"),
                            "disclosed_at": _scalar(fund, "disclosed_at"),
                            "basis_date": _scalar(price, "trading_date"),
                            "basis_close": _scalar(price, "close"),
                            "revenue": _scalar(fund, "revenue"),
                            "operating_income": _scalar(fund, "operating_income"),
                            "net_income": _scalar(fund, "net_income"),
                            "equity": _scalar(fund, "equity"),
                            "liabilities": _scalar(fund, "liabilities"),
                            "shares_outstanding": share_selection.share_count,
                            "denominator_basis": share_selection.denominator_basis,
                            "selected_share_class": share_selection.selected_se,
                            "istc_totqy": share_selection.istc_totqy,
                            "tesstk_co": share_selection.tesstk_co,
                            "distb_stock_co": share_selection.distb_stock_co,
                            "share_stlm_dt": share_selection.stlm_dt,
                            "source_lineage_json": _json(lineage),
                            "reason_codes_json": _json(list(share_selection.reasons)),
                            "ingested_at": created_at,
                        }
                    )
                    for metric in metrics.values():
                        metric_rows.append(
                            {
                                "as_of_date": as_of_date,
                                "symbol": symbol,
                                "metric_name": metric.name,
                                "metric_value": metric.value,
                                "metric_unit": metric.metric_unit,
                                "source_type": metric.source_type,
                                "formula_version": metric.formula_version,
                                "reason": metric.reason,
                                "formula_inputs_json": _json(metric.formula_inputs),
                                "created_at": created_at,
                            }
                        )

                wide_metrics = _wide_metric_frame(metric_rows, metadata)
                baseline_rows: list[dict[str, object]] = []
                label_rows: list[dict[str, object]] = []
                ingredient_by_symbol = {row["symbol"]: row for row in ingredient_rows}
                metrics_by_symbol = {
                    symbol: {
                        metric.name: metric
                        for metric in calculate_valuation_metrics(
                            ValuationMetricInput(
                                net_income=ingredient_by_symbol[symbol].get("net_income"),
                                equity=ingredient_by_symbol[symbol].get("equity"),
                                revenue=ingredient_by_symbol[symbol].get("revenue"),
                                operating_income=ingredient_by_symbol[symbol].get(
                                    "operating_income"
                                ),
                                liabilities=ingredient_by_symbol[symbol].get("liabilities"),
                                shares_outstanding=ingredient_by_symbol[symbol].get(
                                    "shares_outstanding"
                                ),
                                basis_close=ingredient_by_symbol[symbol].get("basis_close"),
                                currency="KRW",
                                official_market_cap=None,
                                source_lineage=json.loads(
                                    str(
                                        ingredient_by_symbol[symbol].get("source_lineage_json")
                                        or "{}"
                                    )
                                ),
                            )
                        ).values()
                    }
                    for symbol in ingredient_by_symbol
                }
                for row in wide_metrics.itertuples(index=False):
                    symbol = str(row.symbol).zfill(6)
                    ingredient = ingredient_by_symbol.get(symbol)
                    if ingredient is None:
                        continue
                    baseline = build_sector_valuation_baseline(
                        wide_metrics,
                        target_symbol=symbol,
                        sector=str(getattr(row, "sector", "") or ""),
                        industry=str(getattr(row, "industry", "") or ""),
                        sector_code=str(getattr(row, "sector_code", "") or ""),
                        industry_code=str(getattr(row, "industry_code", "") or ""),
                    )
                    confidence = evaluate_confidence(
                        ConfidenceInput(
                            disclosure_age_days=_days_between(
                                as_of_date, ingredient.get("disclosed_at")
                            ),
                            metrics=metrics_by_symbol.get(symbol, {}),
                            peer_count=baseline.peer_count,
                            pbr_peer_coverage=baseline.pbr_coverage,
                            per_peer_coverage=baseline.per_coverage,
                            source_lineage_present=bool(ingredient.get("source_lineage_json")),
                            share_basis_reasons=tuple(
                                json.loads(str(ingredient.get("reason_codes_json") or "[]"))
                            ),
                            net_income=ingredient.get("net_income"),
                            equity=ingredient.get("equity"),
                        )
                    )
                    label = assign_valuation_label(
                        confidence_pass=confidence.passed,
                        pbr_percentile=baseline.pbr_percentile,
                        per_percentile=baseline.per_percentile,
                        net_income=ingredient.get("net_income"),
                        equity=ingredient.get("equity"),
                        roe=metrics_by_symbol.get(symbol, {}).get("roe").value
                        if metrics_by_symbol.get(symbol, {}).get("roe")
                        else None,
                    )
                    baseline_rows.append(
                        {
                            "as_of_date": as_of_date,
                            "symbol": symbol,
                            "group_type": baseline.group_type,
                            "group_value": baseline.group_value,
                            "peer_count": baseline.peer_count,
                            "valid_pbr_count": baseline.valid_pbr_count,
                            "valid_per_count": baseline.valid_per_count,
                            "pbr_coverage": baseline.pbr_coverage,
                            "per_coverage": baseline.per_coverage,
                            "median_pbr": baseline.median_pbr,
                            "median_per": baseline.median_per,
                            "pbr_percentile": baseline.pbr_percentile,
                            "per_percentile": baseline.per_percentile,
                            "filter_note": baseline.filter_note,
                            "reason_codes_json": _json(list(baseline.reasons)),
                            "created_at": created_at,
                        }
                    )
                    label_rows.append(
                        {
                            "as_of_date": as_of_date,
                            "symbol": symbol,
                            "valuation_label": label,
                            "confidence_pass": confidence.passed,
                            "hard_gate_reasons_json": _json(list(confidence.hard_gate_reasons)),
                            "annotations_json": _json(list(confidence.annotations)),
                            "source_notes_json": _json(
                                {
                                    "source_lineage": json.loads(
                                        str(ingredient.get("source_lineage_json") or "{}")
                                    ),
                                    "baseline": baseline.to_payload(),
                                }
                            ),
                            "created_at": created_at,
                        }
                    )

                ingredient_frame = pd.DataFrame(ingredient_rows)
                metric_frame = pd.DataFrame(metric_rows)
                baseline_frame = pd.DataFrame(baseline_rows)
                label_frame = pd.DataFrame(label_rows)
                _upsert_frame(
                    connection,
                    table="fact_valuation_source_ingredient",
                    frame=ingredient_frame,
                    key_columns=("as_of_date", "symbol"),
                )
                _upsert_frame(
                    connection,
                    table="fact_valuation_metric_snapshot",
                    frame=metric_frame,
                    key_columns=("as_of_date", "symbol", "metric_name"),
                )
                _upsert_frame(
                    connection,
                    table="fact_sector_valuation_baseline",
                    frame=baseline_frame,
                    key_columns=("as_of_date", "symbol"),
                )
                _upsert_frame(
                    connection,
                    table="fact_valuation_label_snapshot",
                    frame=label_frame,
                    key_columns=("as_of_date", "symbol"),
                )

                notes = (
                    f"Valuation snapshot completed. as_of_date={as_of_date.isoformat()}, "
                    f"ingredients={len(ingredient_frame)}, metrics={len(metric_frame)}, "
                    f"baselines={len(baseline_frame)}, labels={len(label_frame)}, "
                    f"skipped={skipped_symbol_count}, failed={failed_symbol_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[str(settings.paths.duckdb_path)],
                    notes=notes,
                )
                return ValuationSnapshotResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    requested_symbol_count=requested_count,
                    ingredient_row_count=len(ingredient_frame),
                    metric_row_count=len(metric_frame),
                    baseline_row_count=len(baseline_frame),
                    label_row_count=len(label_frame),
                    skipped_symbol_count=skipped_symbol_count,
                    failed_symbol_count=failed_symbol_count,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=f"Valuation snapshot failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                )
                raise
            finally:
                if owns_provider:
                    provider.close()
