# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.selection.swing_3_5d import SWING_3_5D_VERSION, build_swing_3_5d_frame
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_core_tables
from app.storage.duckdb import duckdb_connection

HORIZON = 5
TAKE_PROFIT_RETURN = 0.05
STOP_LOSS_RETURN = -0.03
DEFAULT_TOP_K = (3, 5, 10)
BASELINE_RANKING_VERSION = "selection_engine_v2"


@dataclass(frozen=True, slots=True)
class BarrierOutcome:
    symbol: str
    as_of_date: date
    entry_date: date | None
    exit_date: date | None
    outcome: str
    entry_open: float | None
    exit_price: float | None
    realized_return: float | None
    gap_return: float | None
    gap_risk_flag: bool
    holding_days: int


@dataclass(frozen=True, slots=True)
class BacktestArtifacts:
    summary_path: Path
    metrics_path: Path
    selections_path: Path
    report_path: Path


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _date_text(value: object) -> str:
    return pd.Timestamp(value).date().isoformat()


def _json_default(value: object) -> object:
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return pd.Timestamp(value).date().isoformat()
    if pd.isna(value):
        return None
    return value


def _safe_float(value: object) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _ensure_output_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_prediction_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date: date,
) -> pd.DataFrame:
    frame = connection.execute(
        """
        SELECT *
        FROM fact_prediction
        WHERE as_of_date = ?
          AND horizon = ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY created_at DESC NULLS LAST, run_id DESC
        ) = 1
        """,
        [as_of_date, HORIZON],
    ).fetchdf()
    if not frame.empty:
        frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.date
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    return frame


def _load_baseline_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date: date,
    limit_symbols: int | None,
) -> pd.DataFrame:
    limit_clause = ""
    params: list[object] = [as_of_date, HORIZON, BASELINE_RANKING_VERSION]
    if limit_symbols is not None and int(limit_symbols) > 0:
        limit_clause = " LIMIT ?"
        params.append(int(limit_symbols))
    frame = connection.execute(
        f"""
        SELECT
            ranking.as_of_date,
            ranking.symbol,
            symbol.company_name,
            symbol.market,
            symbol.industry_code,
            symbol.industry,
            ranking.final_selection_value,
            ranking.final_selection_rank_pct,
            ranking.grade,
            ranking.eligible_flag,
            ranking.top_reason_tags_json,
            ranking.risk_flags_json
        FROM fact_ranking AS ranking
        LEFT JOIN dim_symbol AS symbol
          ON ranking.symbol = symbol.symbol
        WHERE ranking.as_of_date = ?
          AND ranking.horizon = ?
          AND ranking.ranking_version = ?
          AND COALESCE(ranking.eligible_flag, FALSE)
        ORDER BY ranking.final_selection_value DESC NULLS LAST, ranking.symbol
        {limit_clause}
        """,
        params,
    ).fetchdf()
    if not frame.empty:
        frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.date
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
        frame["strategy"] = "baseline_selection_engine_v2"
        frame["method_score"] = pd.to_numeric(frame["final_selection_value"], errors="coerce")
        frame["swing_pattern"] = pd.NA
        frame["swing_rule_score"] = pd.NA
        frame["swing_hybrid_score"] = pd.NA
        frame["swing_recommendation_pass"] = True
    return frame


def _load_calendar(
    connection: duckdb.DuckDBPyConnection,
    *,
    start_date: date,
    end_date: date,
) -> list[date]:
    rows = connection.execute(
        """
        SELECT DISTINCT trading_date
        FROM fact_daily_ohlcv
        WHERE trading_date BETWEEN ? AND ?
        ORDER BY trading_date
        """,
        [start_date, end_date],
    ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows]


def _resolve_selection_dates(
    connection: duckdb.DuckDBPyConnection,
    *,
    start_date: date,
    end_date: date,
    limit_dates: int | None,
) -> list[date]:
    calendar = _load_calendar(connection, start_date=start_date, end_date=end_date)
    if not calendar:
        return []
    max_available = connection.execute(
        "SELECT MAX(trading_date) FROM fact_daily_ohlcv"
    ).fetchone()[0]
    max_available_date = pd.Timestamp(max_available).date() if max_available else end_date
    full_calendar = _load_calendar(
        connection,
        start_date=calendar[0],
        end_date=max_available_date,
    )
    full_index = {value: idx for idx, value in enumerate(full_calendar)}
    dates = [
        value
        for value in calendar
        if value in full_index and full_index[value] + HORIZON < len(full_calendar)
    ]
    if limit_dates is not None and int(limit_dates) > 0:
        dates = dates[-int(limit_dates) :]
    return dates


def _load_price_frame_for_outcomes(
    connection: duckdb.DuckDBPyConnection,
    *,
    symbols: Iterable[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    normalized = sorted({str(symbol).zfill(6) for symbol in symbols if str(symbol).strip()})
    if not normalized:
        return pd.DataFrame()
    symbol_frame = pd.DataFrame({"symbol": normalized})
    connection.register("swing_backtest_symbols", symbol_frame)
    try:
        frame = connection.execute(
            """
            SELECT trading_date, symbol, open, high, low, close
            FROM fact_daily_ohlcv
            WHERE symbol IN (SELECT symbol FROM swing_backtest_symbols)
              AND trading_date BETWEEN ? AND ?
            ORDER BY symbol, trading_date
            """,
            [start_date, end_date],
        ).fetchdf()
    finally:
        try:
            connection.unregister("swing_backtest_symbols")
        except Exception:
            pass
    if not frame.empty:
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.date
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
        for column in ("open", "high", "low", "close"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _build_price_lookup(price_frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if price_frame.empty:
        return {}
    return {
        str(symbol).zfill(6): group.sort_values("trading_date").reset_index(drop=True)
        for symbol, group in price_frame.groupby("symbol")
    }


def _barrier_outcome(
    *,
    row: pd.Series,
    calendar: list[date],
    calendar_index: dict[date, int],
    price_lookup: dict[str, pd.DataFrame],
) -> BarrierOutcome:
    symbol = str(row["symbol"]).zfill(6)
    as_of_date = pd.Timestamp(row["as_of_date"]).date()

    def _missing(
        outcome: str,
        entry_date_value: date | None,
    ) -> BarrierOutcome:
        return BarrierOutcome(
            symbol,
            as_of_date,
            entry_date_value,
            None,
            outcome,
            None,
            None,
            None,
            None,
            False,
            0,
        )

    date_index = calendar_index.get(as_of_date)
    if date_index is None or date_index + 1 >= len(calendar):
        return _missing("missing_future", None)

    entry_date = calendar[date_index + 1]
    max_exit_index = min(date_index + HORIZON, len(calendar) - 1)
    max_exit_date = calendar[max_exit_index]
    symbol_prices = price_lookup.get(symbol)
    if symbol_prices is None or symbol_prices.empty:
        return _missing("missing_price", entry_date)

    history = symbol_prices.loc[
        (symbol_prices["trading_date"] >= as_of_date)
        & (symbol_prices["trading_date"] <= max_exit_date)
    ].copy()
    if history.empty:
        return _missing("missing_price", entry_date)

    prior_rows = history.loc[history["trading_date"] == as_of_date]
    prior_close = _safe_float(prior_rows["close"].iloc[-1]) if not prior_rows.empty else None
    path = history.loc[history["trading_date"] >= entry_date].head(HORIZON).copy()
    if path.empty:
        return _missing("missing_entry", entry_date)

    entry_open = _safe_float(path["open"].iloc[0])
    if entry_open is None or entry_open <= 0:
        return _missing("missing_entry", entry_date)

    gap_return = (entry_open / prior_close - 1.0) if prior_close and prior_close > 0 else None
    gap_risk = bool(gap_return is not None and abs(gap_return) >= TAKE_PROFIT_RETURN)
    target_price = entry_open * (1.0 + TAKE_PROFIT_RETURN)
    stop_price = entry_open * (1.0 + STOP_LOSS_RETURN)

    for idx, price_row in enumerate(path.itertuples(index=False), start=1):
        low = _safe_float(price_row.low)
        high = _safe_float(price_row.high)
        current_date = pd.Timestamp(price_row.trading_date).date()
        stop_hit = low is not None and low <= stop_price
        target_hit = high is not None and high >= target_price
        if stop_hit and target_hit:
            return BarrierOutcome(
                symbol,
                as_of_date,
                entry_date,
                current_date,
                "ambiguous_stop_first",
                entry_open,
                stop_price,
                STOP_LOSS_RETURN,
                gap_return,
                gap_risk,
                idx,
            )
        if stop_hit:
            return BarrierOutcome(
                symbol,
                as_of_date,
                entry_date,
                current_date,
                "stop_loss",
                entry_open,
                stop_price,
                STOP_LOSS_RETURN,
                gap_return,
                gap_risk,
                idx,
            )
        if target_hit:
            return BarrierOutcome(
                symbol,
                as_of_date,
                entry_date,
                current_date,
                "take_profit",
                entry_open,
                target_price,
                TAKE_PROFIT_RETURN,
                gap_return,
                gap_risk,
                idx,
            )

    exit_row = path.iloc[-1]
    exit_price = _safe_float(exit_row["close"])
    realized_return = (exit_price / entry_open - 1.0) if exit_price is not None else None
    return BarrierOutcome(
        symbol,
        as_of_date,
        entry_date,
        pd.Timestamp(exit_row["trading_date"]).date(),
        "timeout",
        entry_open,
        exit_price,
        realized_return,
        gap_return,
        gap_risk,
        len(path),
    )


def _normalize_swing_selection(frame: pd.DataFrame, *, strategy: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    working = frame.copy()
    working["as_of_date"] = pd.to_datetime(working["as_of_date"]).dt.date
    working["symbol"] = working["symbol"].astype(str).str.zfill(6)
    working["strategy"] = strategy
    if strategy == "swing_rule_only":
        working = working.loc[
            working["swing_candidate_pass"].fillna(False).astype(bool)
            & pd.to_numeric(working["swing_rule_score"], errors="coerce").ge(70.0)
        ].copy()
        working["method_score"] = pd.to_numeric(working["swing_rule_score"], errors="coerce")
    else:
        working = working.loc[
            working["swing_recommendation_pass"].fillna(False).astype(bool)
        ].copy()
        working["method_score"] = pd.to_numeric(working["swing_hybrid_score"], errors="coerce")
    return working.sort_values(["method_score", "symbol"], ascending=[False, True])


def _selection_columns(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "strategy",
        "as_of_date",
        "symbol",
        "company_name",
        "market",
        "industry_code",
        "industry",
        "swing_pattern",
        "method_score",
        "swing_rule_score",
        "swing_hybrid_score",
        "swing_recommendation_pass",
        "final_selection_value",
        "grade",
    ]
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[columns].copy()


def _attach_topk_and_outcomes(
    selections: pd.DataFrame,
    *,
    top_k_values: list[int],
    calendar: list[date],
    price_lookup: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    if selections.empty:
        return pd.DataFrame()
    calendar_index = {value: idx for idx, value in enumerate(calendar)}
    rows: list[dict[str, object]] = []
    max_k = max(top_k_values)
    for (_strategy, _as_of_date), group in selections.groupby(
        ["strategy", "as_of_date"],
        sort=True,
    ):
        ranked = group.sort_values(["method_score", "symbol"], ascending=[False, True]).head(max_k)
        for rank, (_, row) in enumerate(ranked.iterrows(), start=1):
            outcome = _barrier_outcome(
                row=row,
                calendar=calendar,
                calendar_index=calendar_index,
                price_lookup=price_lookup,
            )
            base = row.to_dict()
            base.update(asdict(outcome))
            base["rank"] = rank
            base["top_k_memberships"] = json.dumps(
                [value for value in top_k_values if rank <= value],
                ensure_ascii=False,
            )
            rows.append(base)
    return pd.DataFrame(rows)


def _metric_rows(outcomes: pd.DataFrame, *, top_k_values: list[int]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if outcomes.empty:
        return pd.DataFrame()
    for top_k in top_k_values:
        subset = outcomes.loc[outcomes["rank"].le(top_k)].copy()
        for strategy, group in subset.groupby("strategy", sort=True):
            realized = pd.to_numeric(group["realized_return"], errors="coerce")
            evaluable = realized.notna()
            target_hits = group["outcome"].eq("take_profit")
            stop_hits = group["outcome"].isin(["stop_loss", "ambiguous_stop_first"])
            rows.append(
                {
                    "strategy": strategy,
                    "top_k": top_k,
                    "selection_dates": int(group["as_of_date"].nunique()),
                    "selected_count": int(len(group)),
                    "evaluable_count": int(evaluable.sum()),
                    "take_profit_rate": float(target_hits.mean()) if len(group) else None,
                    "stop_or_ambiguous_rate": float(stop_hits.mean()) if len(group) else None,
                    "timeout_rate": float(group["outcome"].eq("timeout").mean())
                    if len(group)
                    else None,
                    "positive_return_rate": float(realized[evaluable].gt(0).mean())
                    if evaluable.any()
                    else None,
                    "mean_return": float(realized[evaluable].mean()) if evaluable.any() else None,
                    "median_return": float(realized[evaluable].median())
                    if evaluable.any()
                    else None,
                    "mean_gap_return": float(
                        pd.to_numeric(group["gap_return"], errors="coerce").mean()
                    ),
                    "gap_risk_rate": float(group["gap_risk_flag"].fillna(False).mean()),
                    "avg_method_score": float(
                        pd.to_numeric(group["method_score"], errors="coerce").mean()
                    ),
                }
            )
    metrics = pd.DataFrame(rows)
    if metrics.empty:
        return metrics
    baseline = metrics.loc[metrics["strategy"].eq("baseline_selection_engine_v2")]
    if not baseline.empty:
        baseline_lookup = baseline.set_index("top_k")
        metrics["take_profit_rate_vs_baseline"] = metrics.apply(
            lambda row: row["take_profit_rate"]
            - baseline_lookup.loc[row["top_k"], "take_profit_rate"]
            if row["top_k"] in baseline_lookup.index
            and pd.notna(row["take_profit_rate"])
            and pd.notna(baseline_lookup.loc[row["top_k"], "take_profit_rate"])
            else None,
            axis=1,
        )
        metrics["mean_return_vs_baseline"] = metrics.apply(
            lambda row: row["mean_return"] - baseline_lookup.loc[row["top_k"], "mean_return"]
            if row["top_k"] in baseline_lookup.index
            and pd.notna(row["mean_return"])
            and pd.notna(baseline_lookup.loc[row["top_k"], "mean_return"])
            else None,
            axis=1,
        )
    return metrics


def _write_report(
    *,
    output_dir: Path,
    start_date: date,
    end_date: date,
    selection_dates: list[date],
    metrics: pd.DataFrame,
    summary_path: Path,
    selections_path: Path,
) -> Path:
    report_path = output_dir / "swing_3_5d_backtest_report.md"
    lines = [
        "# StockMaster 3~5D Swing Methodology Backtest",
        "",
        f"- Methodology version: `{SWING_3_5D_VERSION}`",
        f"- Range: {start_date.isoformat()} .. {end_date.isoformat()}",
        f"- Selection dates evaluated: {len(selection_dates)}",
        "- Entry/exit: t+1 open entry, +5% take-profit before -3% stop-loss, "
        "5 trading-day timeout; same-day target+stop is conservatively counted as stop.",
        f"- Summary JSON: `{summary_path}`",
        f"- Selection/outcome CSV: `{selections_path}`",
        "",
        "## Metrics",
        "",
    ]
    if metrics.empty:
        lines.append("No evaluable selections.")
    else:
        display = metrics.copy()
        percent_columns = [
            "take_profit_rate",
            "stop_or_ambiguous_rate",
            "timeout_rate",
            "positive_return_rate",
            "mean_return",
            "median_return",
            "mean_gap_return",
            "gap_risk_rate",
            "take_profit_rate_vs_baseline",
            "mean_return_vs_baseline",
        ]
        for column in percent_columns:
            if column in display.columns:
                display[column] = display[column].map(
                    lambda value: "" if pd.isna(value) else f"{float(value):.2%}"
                )
        lines.append(display.to_markdown(index=False))
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def run_backtest(args: argparse.Namespace) -> BacktestArtifacts:
    settings = load_settings(project_root=PROJECT_ROOT)
    output_dir = _ensure_output_dir(Path(args.output_dir or PROJECT_ROOT / ".omx" / "reports"))
    top_k_values = sorted({int(value) for value in args.top_k if int(value) > 0})
    if not top_k_values:
        raise ValueError("At least one positive --top-k value is required")

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        selection_dates = _resolve_selection_dates(
            connection,
            start_date=args.start_date,
            end_date=args.end_date,
            limit_dates=args.limit_dates,
        )
        if not selection_dates:
            raise RuntimeError("No selection dates have enough future OHLCV for 5D backtest")

        per_date_selections: list[pd.DataFrame] = []
        for as_of_date in selection_dates:
            prediction_frame = _load_prediction_frame(connection, as_of_date=as_of_date)
            swing_frame = build_swing_3_5d_frame(
                connection,
                as_of_date=as_of_date,
                settings=settings,
                market=args.market,
                limit_symbols=args.limit_symbols,
                prediction_frame=prediction_frame,
            )
            if not swing_frame.empty:
                swing_frame["as_of_date"] = as_of_date
                per_date_selections.append(
                    _selection_columns(
                        _normalize_swing_selection(swing_frame, strategy="swing_hybrid")
                    )
                )
                per_date_selections.append(
                    _selection_columns(
                        _normalize_swing_selection(
                            swing_frame,
                            strategy="swing_rule_only",
                        )
                    )
                )
            baseline = _load_baseline_frame(
                connection,
                as_of_date=as_of_date,
                limit_symbols=args.limit_symbols,
            )
            if not baseline.empty:
                per_date_selections.append(_selection_columns(baseline))

        if not per_date_selections:
            raise RuntimeError("No candidate selections were produced for backtest")
        selections = pd.concat(per_date_selections, ignore_index=True)
        unique_symbols = selections["symbol"].dropna().astype(str).str.zfill(6).unique().tolist()
        calendar_start = min(selection_dates)
        max_available = connection.execute(
            "SELECT MAX(trading_date) FROM fact_daily_ohlcv"
        ).fetchone()[0]
        calendar_end = pd.Timestamp(max_available).date()
        calendar = _load_calendar(connection, start_date=calendar_start, end_date=calendar_end)
        price_frame = _load_price_frame_for_outcomes(
            connection,
            symbols=unique_symbols,
            start_date=calendar_start,
            end_date=calendar_end,
        )

    outcomes = _attach_topk_and_outcomes(
        selections,
        top_k_values=top_k_values,
        calendar=calendar,
        price_lookup=_build_price_lookup(price_frame),
    )
    metrics = _metric_rows(outcomes, top_k_values=top_k_values)

    stem = f"swing_3_5d_backtest_{args.start_date.isoformat()}_{args.end_date.isoformat()}"
    selections_path = output_dir / f"{stem}_selections.csv"
    metrics_path = output_dir / f"{stem}_metrics.csv"
    summary_path = output_dir / f"{stem}_summary.json"
    outcomes.to_csv(selections_path, index=False)
    metrics.to_csv(metrics_path, index=False)
    summary = {
        "kind": "stockmaster_3_5d_swing_backtest",
        "methodology_version": SWING_3_5D_VERSION,
        "range": {
            "start_date": args.start_date.isoformat(),
            "end_date": args.end_date.isoformat(),
        },
        "selection_dates": [_date_text(value) for value in selection_dates],
        "top_k": top_k_values,
        "barrier": {
            "entry": "t_plus_1_open",
            "take_profit_return": TAKE_PROFIT_RETURN,
            "stop_loss_return": STOP_LOSS_RETURN,
            "horizon_trading_days": HORIZON,
            "same_day_target_and_stop": "counted_as_ambiguous_stop_first",
        },
        "row_counts": {
            "selection_outcomes": int(len(outcomes)),
            "metrics": int(len(metrics)),
        },
        "metrics": metrics.to_dict(orient="records"),
    }
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    report_path = _write_report(
        output_dir=output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        selection_dates=selection_dates,
        metrics=metrics,
        summary_path=summary_path,
        selections_path=selections_path,
    )
    return BacktestArtifacts(
        summary_path=summary_path,
        metrics_path=metrics_path,
        selections_path=selections_path,
        report_path=report_path,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest StockMaster 3~5D swing methodology against selection_engine_v2."
    )
    parser.add_argument("--start-date", required=True, type=_parse_date)
    parser.add_argument("--end-date", required=True, type=_parse_date)
    parser.add_argument("--top-k", nargs="+", type=int, default=list(DEFAULT_TOP_K))
    parser.add_argument("--limit-dates", type=int)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    parser.add_argument("--output-dir")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    artifacts = run_backtest(args)
    print("3~5D swing methodology backtest completed.")
    print(f"summary={artifacts.summary_path}")
    print(f"metrics={artifacts.metrics_path}")
    print(f"selections={artifacts.selections_path}")
    print(f"report={artifacts.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
