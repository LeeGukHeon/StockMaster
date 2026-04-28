# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION
from app.recommendation.buyability import (
    BUYABILITY_MIN_FINAL_SELECTION_VALUE,
    buyability_priority_score,
    d5_buyability_policy_bucket,
)
from app.recommendation.judgement import (
    RecommendationJudgement,
    ScoreBandEvidence,
    classify_recommendation,
    score_band_for_value,
)
from app.settings import load_settings

ACTIONABLE_LABELS = {"매수검토", "매수해볼 가치 있음", "적극매수 후보"}
DEFAULT_TOP_LIMIT = 5
DEFAULT_MAX_PER_SECTOR = 2


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    start_date: date | None
    end_date: date | None
    horizon: int
    ranking_version: str
    top_limit: int
    max_per_sector: int
    evidence_mode: str
    evidence_lookback_dates: int
    min_matured_dates: int


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _git_sha() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _finite_float(value: object, *, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _risk_flags(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def _load_matured_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    config: ReplayConfig,
) -> pd.DataFrame:
    filters = [
        "outcome.horizon = ?",
        "outcome.ranking_version = ?",
        "outcome.outcome_status = 'matured'",
        "outcome.realized_excess_return IS NOT NULL",
        "outcome.final_selection_value IS NOT NULL",
    ]
    params: list[object] = [config.horizon, config.ranking_version]
    if config.start_date is not None:
        filters.append("outcome.selection_date >= ?")
        params.append(config.start_date)
    if config.end_date is not None:
        filters.append("outcome.selection_date <= ?")
        params.append(config.end_date)
    where_sql = " AND\n          ".join(filters)
    return connection.execute(
        f"""
        SELECT
            outcome.selection_date,
            outcome.evaluation_date,
            outcome.symbol,
            COALESCE(symbol.company_name, outcome.symbol) AS company_name,
            COALESCE(symbol.market, outcome.market) AS market,
            symbol.sector,
            symbol.industry,
            outcome.horizon,
            outcome.ranking_version,
            outcome.grade,
            outcome.eligible_flag,
            outcome.final_selection_value,
            outcome.selection_percentile,
            outcome.expected_excess_return_at_selection AS expected_excess_return,
            outcome.uncertainty_score_at_selection AS uncertainty_score,
            outcome.disagreement_score_at_selection AS disagreement_score,
            outcome.fallback_flag_at_selection AS fallback_flag,
            outcome.risk_flags_json,
            outcome.top_reason_tags_json,
            outcome.model_spec_id_at_selection AS model_spec_id,
            outcome.active_alpha_model_id_at_selection AS active_alpha_model_id,
            outcome.realized_return,
            outcome.realized_excess_return,
            outcome.direction_hit_flag,
            outcome.outcome_status
        FROM fact_selection_outcome AS outcome
        LEFT JOIN dim_symbol AS symbol
          ON outcome.symbol = symbol.symbol
        WHERE {where_sql}
        ORDER BY outcome.selection_date, outcome.final_selection_value DESC, outcome.symbol
        """,
        params,
    ).fetchdf()


def _score_band_evidence_from_history(
    history: pd.DataFrame,
    *,
    min_dates: int,
) -> dict[str, ScoreBandEvidence]:
    if history.empty or history["selection_date"].nunique() < int(min_dates):
        return {}
    working = history.loc[history["realized_excess_return"].notna()].copy()
    if working.empty:
        return {}
    working["score_band"] = working["final_selection_value"].map(score_band_for_value)
    evidence: dict[str, ScoreBandEvidence] = {}
    for band, group in working.groupby("score_band", sort=False):
        returns = pd.to_numeric(group["realized_excess_return"], errors="coerce").dropna()
        if returns.empty:
            continue
        evidence[str(band)] = ScoreBandEvidence(
            score_band=str(band),
            sample_count=int(len(returns)),
            avg_excess_return=float(returns.mean()),
            hit_rate=float(returns.gt(0.0).mean()),
            start_date=str(group["selection_date"].min()),
            end_date=str(group["selection_date"].max()),
        )
    return evidence


def _evidence_for_date(
    full_frame: pd.DataFrame,
    *,
    selection_date: date,
    mode: str,
    lookback_dates: int,
    min_dates: int,
) -> dict[str, ScoreBandEvidence]:
    if mode == "none":
        return {}
    dates = pd.to_datetime(full_frame["selection_date"]).dt.date
    if mode == "latest":
        eligible = full_frame.copy()
    elif mode == "trailing":
        past_dates = sorted(set(d for d in dates if d < selection_date))[-int(lookback_dates) :]
        eligible = full_frame.loc[dates.isin(past_dates)].copy()
    else:
        raise ValueError(f"Unknown evidence mode: {mode}")
    return _score_band_evidence_from_history(eligible, min_dates=min_dates)


def _rank_and_filter_day(day: pd.DataFrame) -> pd.DataFrame:
    working = day.copy()
    working["d5_selection_rank"] = (
        working["final_selection_value"]
        .rank(method="first", ascending=False)
        .astype("int64")
    )
    working["risk_flag_list"] = working["risk_flags_json"].map(_risk_flags)
    working["expected_excess_return"] = pd.to_numeric(
        working["expected_excess_return"], errors="coerce"
    )
    working["uncertainty_score"] = pd.to_numeric(
        working["uncertainty_score"], errors="coerce"
    )
    working["disagreement_score"] = pd.to_numeric(
        working["disagreement_score"], errors="coerce"
    )
    working["buyability_priority_score"] = working.apply(
        lambda row: buyability_priority_score(
            expected_excess_return=row.get("expected_excess_return"),
            uncertainty_score=row.get("uncertainty_score"),
            disagreement_score=row.get("disagreement_score"),
        ),
        axis=1,
    )
    working["d5_policy_bucket"] = working.apply(
        lambda row: d5_buyability_policy_bucket(
            selection_rank=row.get("d5_selection_rank"),
            expected_excess_return=row.get("expected_excess_return"),
            final_selection_value=row.get("final_selection_value"),
            risk_flags=row.get("risk_flag_list"),
            fallback_flag=row.get("fallback_flag"),
            uncertainty_score=row.get("uncertainty_score"),
            disagreement_score=row.get("disagreement_score"),
        ),
        axis=1,
    )
    mask = (
        working["eligible_flag"].fillna(False).astype(bool)
        & working["d5_selection_rank"].between(1, 10)
        & pd.to_numeric(working["final_selection_value"], errors="coerce").ge(
            BUYABILITY_MIN_FINAL_SELECTION_VALUE
        )
        & working["expected_excess_return"].gt(0.0)
        & working["d5_policy_bucket"].notna()
    )
    candidates = working.loc[mask].copy()
    if candidates.empty:
        return candidates
    candidates["d5_policy_bucket"] = candidates["d5_policy_bucket"].astype(int)
    return candidates.sort_values(
        ["d5_policy_bucket", "d5_selection_rank", "symbol"],
        ascending=[True, True, True],
    ).reset_index(drop=True)


def _limit_sector_concentration(
    frame: pd.DataFrame,
    *,
    limit: int,
    max_per_sector: int,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    selected_indices: list[object] = []
    sector_counts: dict[str, int] = {}
    for index, row in frame.iterrows():
        sector = str(row.get("sector") or row.get("industry") or "-")
        if sector_counts.get(sector, 0) >= int(max_per_sector):
            continue
        selected_indices.append(index)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(selected_indices) >= int(limit):
            break
    if len(selected_indices) < int(limit):
        for index in frame.index:
            if index in selected_indices:
                continue
            selected_indices.append(index)
            if len(selected_indices) >= int(limit):
                break
    return frame.loc[selected_indices].head(int(limit)).copy()


def _classify_row(
    row: pd.Series,
    *,
    evidence_by_band: dict[str, ScoreBandEvidence],
) -> RecommendationJudgement:
    return classify_recommendation(
        final_selection_value=row.get("final_selection_value"),
        expected_excess_return=row.get("expected_excess_return"),
        risk_flags=row.get("risk_flag_list"),
        evidence_by_band=evidence_by_band,
        candidate_selected=True,
        candidate_rank=row.get("d5_selection_rank"),
        buyability_priority_score=row.get("buyability_priority_score"),
    )


def replay_policy(
    frame: pd.DataFrame,
    *,
    config: ReplayConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return pd.DataFrame(), pd.DataFrame()
    working = frame.copy()
    working["selection_date"] = pd.to_datetime(working["selection_date"]).dt.date
    rows: list[pd.DataFrame] = []
    daily_rows: list[dict[str, object]] = []
    for selection_date, day in working.groupby("selection_date", sort=True):
        candidates = _rank_and_filter_day(day)
        fetched = candidates.head(max(int(config.top_limit) * 4, int(config.top_limit)))
        selected = _limit_sector_concentration(
            fetched,
            limit=config.top_limit,
            max_per_sector=config.max_per_sector,
        )
        evidence = _evidence_for_date(
            working,
            selection_date=selection_date,
            mode=config.evidence_mode,
            lookback_dates=config.evidence_lookback_dates,
            min_dates=config.min_matured_dates,
        )
        if not selected.empty:
            selected = selected.copy()
            judgements = [
                _classify_row(row, evidence_by_band=evidence)
                for _, row in selected.iterrows()
            ]
            selected["replay_rank"] = range(1, len(selected) + 1)
            selected["judgement_label"] = [item.label for item in judgements]
            selected["judgement_summary"] = [item.summary for item in judgements]
            selected["score_band"] = [item.score_band for item in judgements]
            selected["actionable_flag"] = selected["judgement_label"].isin(ACTIONABLE_LABELS)
            rows.append(selected)
        else:
            selected = pd.DataFrame()
        day_returns = (
            pd.to_numeric(selected.get("realized_excess_return"), errors="coerce")
            if not selected.empty
            else pd.Series(dtype=float)
        )
        actionable = selected.loc[selected["actionable_flag"]] if not selected.empty else selected
        actionable_returns = (
            pd.to_numeric(actionable.get("realized_excess_return"), errors="coerce")
            if not actionable.empty
            else pd.Series(dtype=float)
        )
        daily_rows.append(
            {
                "selection_date": selection_date.isoformat(),
                "candidate_pool_count": int(len(candidates)),
                "selected_count": int(len(selected)),
                "actionable_count": int(len(actionable)),
                "selected_symbols": (
                    ",".join(selected["symbol"].astype(str))
                    if not selected.empty
                    else ""
                ),
                "actionable_symbols": (
                    ",".join(actionable["symbol"].astype(str))
                    if not actionable.empty
                    else ""
                ),
                "selected_avg_excess_return_cash0": float(day_returns.mean())
                if not day_returns.dropna().empty
                else 0.0,
                "actionable_avg_excess_return_cash0": float(actionable_returns.mean())
                if not actionable_returns.dropna().empty
                else 0.0,
                "selected_hit": float(day_returns.gt(0).mean())
                if not day_returns.dropna().empty
                else None,
                "actionable_hit": float(actionable_returns.gt(0).mean())
                if not actionable_returns.dropna().empty
                else None,
            }
        )
    replay_rows = pd.concat(rows, ignore_index=True, sort=False) if rows else pd.DataFrame()
    return replay_rows, pd.DataFrame(daily_rows)


def _summarize_return_series(values: pd.Series) -> dict[str, object]:
    returns = pd.to_numeric(values, errors="coerce").fillna(0.0)
    if returns.empty:
        return {
            "dates": 0,
            "avg": None,
            "median": None,
            "p10": None,
            "p05": None,
            "hit": None,
            "cumulative": None,
            "max_drawdown": None,
        }
    cumulative_curve = (1.0 + returns).cumprod()
    drawdown = cumulative_curve / cumulative_curve.cummax() - 1.0
    return {
        "dates": int(len(returns)),
        "avg": float(returns.mean()),
        "median": float(returns.median()),
        "p10": float(returns.quantile(0.10)),
        "p05": float(returns.quantile(0.05)),
        "hit": float(returns.gt(0.0).mean()),
        "cumulative": float(cumulative_curve.iloc[-1] - 1.0),
        "max_drawdown": float(drawdown.min()),
    }


def build_summaries(replay_rows: pd.DataFrame, daily: pd.DataFrame) -> dict[str, pd.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    variants = {
        "selected_all_cash0": "selected_avg_excess_return_cash0",
        "actionable_only_cash0": "actionable_avg_excess_return_cash0",
    }
    for variant, column in variants.items():
        values = daily[column] if column in daily else pd.Series(dtype=float)
        stats = _summarize_return_series(values)
        count_column = "actionable_count" if "actionable" in variant else "selected_count"
        active_dates = (
            int((pd.to_numeric(daily.get(count_column), errors="coerce") > 0).sum())
            if not daily.empty
            else 0
        )
        summary_rows.append(
            {
                "variant": variant,
                "active_dates": active_dates,
                "coverage": float(active_dates / len(daily)) if len(daily) else 0.0,
                **stats,
            }
        )
    summary = pd.DataFrame(summary_rows)
    band_summary = pd.DataFrame()
    rank_summary = pd.DataFrame()
    label_summary = pd.DataFrame()
    if not replay_rows.empty:
        replay_rows = replay_rows.copy()
        replay_rows["realized_excess_return"] = pd.to_numeric(
            replay_rows["realized_excess_return"], errors="coerce"
        )
        band_summary = (
            replay_rows.groupby(["score_band", "judgement_label"], dropna=False)
            .agg(
                n=("symbol", "count"),
                dates=("selection_date", "nunique"),
                avg_excess=("realized_excess_return", "mean"),
                median_excess=("realized_excess_return", "median"),
                hit=("realized_excess_return", lambda s: float(
                    pd.to_numeric(s, errors="coerce").gt(0).mean()
                )),
                avg_expected=("expected_excess_return", "mean"),
                avg_priority=("buyability_priority_score", "mean"),
            )
            .reset_index()
        )
        rank_summary = (
            replay_rows.groupby("replay_rank", dropna=False)
            .agg(
                n=("symbol", "count"),
                dates=("selection_date", "nunique"),
                avg_excess=("realized_excess_return", "mean"),
                median_excess=("realized_excess_return", "median"),
                hit=("realized_excess_return", lambda s: float(
                    pd.to_numeric(s, errors="coerce").gt(0).mean()
                )),
                avg_expected=("expected_excess_return", "mean"),
                actionable_rate=("actionable_flag", "mean"),
            )
            .reset_index()
        )
        label_summary = (
            replay_rows.groupby("judgement_label", dropna=False)
            .agg(
                n=("symbol", "count"),
                dates=("selection_date", "nunique"),
                avg_excess=("realized_excess_return", "mean"),
                median_excess=("realized_excess_return", "median"),
                hit=("realized_excess_return", lambda s: float(
                    pd.to_numeric(s, errors="coerce").gt(0).mean()
                )),
                avg_expected=("expected_excess_return", "mean"),
                avg_priority=("buyability_priority_score", "mean"),
            )
            .reset_index()
        )
    return {
        "summary": summary,
        "band_summary": band_summary,
        "rank_summary": rank_summary,
        "label_summary": label_summary,
    }


def _write_report(
    path: Path,
    *,
    config: ReplayConfig,
    manifest: dict[str, object],
    summaries: dict[str, pd.DataFrame],
    daily: pd.DataFrame,
) -> None:
    lines = [
        f"# Current D5 policy replay — {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Contract",
        "",
        "- Read-only PIT-style replay over matured `fact_selection_outcome` rows.",
        "- Uses the current D5 buyability ordering/classifier code.",
        "- Cash return is 0 on days with no selected/actionable candidates.",
        f"- Evidence mode: `{config.evidence_mode}`; ranking_version: `{config.ranking_version}`.",
        "",
        "## Summary",
        "",
        summaries["summary"].to_markdown(index=False),
        "",
    ]
    for name in ("label_summary", "rank_summary", "band_summary"):
        frame = summaries.get(name, pd.DataFrame())
        lines.extend([f"## {name}", ""])
        lines.append("_empty_" if frame.empty else frame.to_markdown(index=False))
        lines.append("")
    if not daily.empty:
        lines.extend(["## Recent daily replay tail", ""])
        lines.append(daily.tail(15).to_markdown(index=False))
        lines.append("")
    lines.extend(
        [
            "## Manifest",
            "",
            "```json",
            json.dumps(manifest, ensure_ascii=False, indent=2),
            "```",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    if not args.artifact_only or not args.promotion_disabled:
        raise RuntimeError("Replay is read-only: pass --artifact-only --promotion-disabled")
    config = ReplayConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        horizon=args.horizon,
        ranking_version=args.ranking_version,
        top_limit=args.top_limit,
        max_per_sector=args.max_per_sector,
        evidence_mode=args.evidence_mode,
        evidence_lookback_dates=args.evidence_lookback_dates,
        min_matured_dates=args.min_matured_dates,
    )
    settings = load_settings(project_root=PROJECT_ROOT)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        source = _load_matured_rows(connection, config=config)
    finally:
        connection.close()
    replay_rows, daily = replay_policy(source, config=config)
    summaries = build_summaries(replay_rows, daily)
    outputs = {
        "source_rows": output_dir / "current_d5_policy_replay_source_rows.csv",
        "replay_rows": output_dir / "current_d5_policy_replay_rows.csv",
        "daily": output_dir / "current_d5_policy_replay_daily.csv",
        "summary": output_dir / "current_d5_policy_replay_summary.csv",
        "band_summary": output_dir / "current_d5_policy_replay_band_summary.csv",
        "rank_summary": output_dir / "current_d5_policy_replay_rank_summary.csv",
        "label_summary": output_dir / "current_d5_policy_replay_label_summary.csv",
        "report": output_dir / "current_d5_policy_replay_report.md",
        "manifest": output_dir / "current_d5_policy_replay_manifest.json",
    }
    source.to_csv(outputs["source_rows"], index=False)
    replay_rows.to_csv(outputs["replay_rows"], index=False)
    daily.to_csv(outputs["daily"], index=False)
    for key, frame in summaries.items():
        frame.to_csv(outputs[key], index=False)
    manifest = {
        "kind": "current_d5_policy_replay",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": _git_sha(),
        "config": {
            **asdict(config),
            "start_date": None if config.start_date is None else config.start_date.isoformat(),
            "end_date": None if config.end_date is None else config.end_date.isoformat(),
        },
        "promotion_disabled": True,
        "artifact_only": True,
        "db_read_only": True,
        "source_row_count": int(len(source)),
        "source_date_count": int(source["selection_date"].nunique()) if not source.empty else 0,
        "replay_row_count": int(len(replay_rows)),
        "replay_date_count": int(daily["selection_date"].nunique()) if not daily.empty else 0,
        "outputs": {key: str(path) for key, path in outputs.items()},
    }
    outputs["manifest"].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_report(
        outputs["report"],
        config=config,
        manifest=manifest,
        summaries=summaries,
        daily=daily,
    )
    print(
        json.dumps(
            {"manifest": str(outputs["manifest"]), "report": str(outputs["report"])},
            ensure_ascii=False,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replay the current D5 EOD buyability policy over matured historical outcomes."
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--start-date", type=_parse_date)
    parser.add_argument("--end-date", type=_parse_date)
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--ranking-version", default=SELECTION_ENGINE_VERSION)
    parser.add_argument("--top-limit", type=int, default=DEFAULT_TOP_LIMIT)
    parser.add_argument("--max-per-sector", type=int, default=DEFAULT_MAX_PER_SECTOR)
    parser.add_argument(
        "--evidence-mode",
        choices=["none", "trailing", "latest"],
        default="trailing",
        help="Use no score-band evidence, PIT trailing evidence, or full latest evidence.",
    )
    parser.add_argument("--evidence-lookback-dates", type=int, default=120)
    parser.add_argument("--min-matured-dates", type=int, default=5)
    parser.add_argument("--artifact-only", action="store_true")
    parser.add_argument("--promotion-disabled", action="store_true")
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
