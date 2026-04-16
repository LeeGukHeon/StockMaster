from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

DEFAULT_TOP_K = 10
DEFAULT_BRANCH_THRESHOLDS = {
    "model_chase_threshold": 0.08,
    "selection_delta_threshold": 0.03,
}


@dataclass(slots=True)
class AlphaPhase0AuditResult:
    run_id: str
    start_date: date
    end_date: date
    row_count: int
    pit_status: str
    branch_recommendation: str
    artifact_paths: list[str]
    notes: str


def _phase0_artifact_dir(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    run_id: str,
) -> Path:
    path = (
        settings.paths.artifacts_dir
        / "alpha_phase0"
        / f"start_date={start_date.isoformat()}"
        / f"end_date={end_date.isoformat()}"
        / run_id
    )
    path.mkdir(parents=True, exist_ok=True)
    return path


def _source_contains_explicit_cutoff(source_path: Path, token: str) -> bool:
    try:
        text = source_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return False
    return token in text


def run_pit_checks(
    connection,
    *,
    settings: Settings,
    start_date: date,
    end_date: date,
    cutoff_time: str,
) -> pd.DataFrame:
    cutoff = time.fromisoformat(cutoff_time)
    cutoff_text = cutoff.strftime("%H:%M:%S")

    news_after_cutoff = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_news_item
            WHERE signal_date BETWEEN ? AND ?
              AND published_at IS NOT NULL
              AND CAST(published_at AT TIME ZONE 'Asia/Seoul' AS DATE) = signal_date
              AND CAST(published_at AT TIME ZONE 'Asia/Seoul' AS TIME) > CAST(? AS TIME)
            """,
            [start_date, end_date, cutoff_text],
        ).fetchone()[0]
        or 0
    )
    fundamentals_after_cutoff = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_fundamentals_snapshot
            WHERE as_of_date BETWEEN ? AND ?
              AND disclosed_at IS NOT NULL
              AND CAST(disclosed_at AT TIME ZONE 'Asia/Seoul' AS DATE) = as_of_date
              AND CAST(disclosed_at AT TIME ZONE 'Asia/Seoul' AS TIME) > CAST(? AS TIME)
            """,
            [start_date, end_date, cutoff_text],
        ).fetchone()[0]
        or 0
    )
    pre_listing_selection = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_ranking AS ranking
            JOIN dim_symbol AS symbol_meta
              ON ranking.symbol = symbol_meta.symbol
            WHERE ranking.ranking_version = ?
              AND ranking.as_of_date BETWEEN ? AND ?
              AND symbol_meta.listing_date IS NOT NULL
              AND symbol_meta.listing_date > ranking.as_of_date
            """,
            [SELECTION_ENGINE_VERSION, start_date, end_date],
        ).fetchone()[0]
        or 0
    )
    model_window_overlap = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_prediction AS prediction
            JOIN fact_model_training_run AS training
              ON prediction.training_run_id = training.training_run_id
            WHERE prediction.ranking_version = ?
              AND prediction.prediction_version = ?
              AND prediction.as_of_date BETWEEN ? AND ?
              AND COALESCE(training.validation_window_end, training.training_window_end) >= prediction.as_of_date
            """,
            [SELECTION_ENGINE_VERSION, ALPHA_PREDICTION_VERSION, start_date, end_date],
        ).fetchone()[0]
        or 0
    )

    news_loader_has_cutoff = _source_contains_explicit_cutoff(
        settings.paths.project_root / "app" / "features" / "feature_store.py",
        "published_at <=",
    ) or _source_contains_explicit_cutoff(
        settings.paths.project_root / "app" / "features" / "builders" / "news_features.py",
        "published_at <=",
    )
    fundamentals_loader_has_cutoff = _source_contains_explicit_cutoff(
        settings.paths.project_root / "app" / "features" / "feature_store.py",
        "disclosed_at <=",
    )

    rows = [
        {
            "check_name": "news_after_cutoff_same_day_rows",
            "severity": "critical",
            "status": "fail" if news_after_cutoff > 0 else "pass",
            "violation_count": news_after_cutoff,
            "hard_fail_flag": news_after_cutoff > 0,
            "details": (
                f"signal_date same-day news published after cutoff {cutoff_time}; "
                f"count={news_after_cutoff}"
            ),
        },
        {
            "check_name": "fundamentals_after_cutoff_same_day_rows",
            "severity": "critical",
            "status": "fail" if fundamentals_after_cutoff > 0 else "pass",
            "violation_count": fundamentals_after_cutoff,
            "hard_fail_flag": fundamentals_after_cutoff > 0,
            "details": (
                f"same-day disclosures after cutoff {cutoff_time}; "
                f"count={fundamentals_after_cutoff}"
            ),
        },
        {
            "check_name": "prediction_training_window_overlaps_selection_date",
            "severity": "critical",
            "status": "fail" if model_window_overlap > 0 else "pass",
            "violation_count": model_window_overlap,
            "hard_fail_flag": model_window_overlap > 0,
            "details": (
                "prediction rows whose training/validation window reaches the prediction date; "
                f"count={model_window_overlap}"
            ),
        },
        {
            "check_name": "selection_before_listing_date",
            "severity": "critical",
            "status": "fail" if pre_listing_selection > 0 else "pass",
            "violation_count": pre_listing_selection,
            "hard_fail_flag": pre_listing_selection > 0,
            "details": (
                "ranking rows produced before a symbol listing date; "
                f"count={pre_listing_selection}"
            ),
        },
        {
            "check_name": "news_loader_cutoff_contract",
            "severity": "warning",
            "status": (
                "pass"
                if news_loader_has_cutoff
                else ("fail" if news_after_cutoff > 0 else "warn")
            ),
            "violation_count": 0 if news_loader_has_cutoff or news_after_cutoff <= 0 else news_after_cutoff,
            "hard_fail_flag": bool((not news_loader_has_cutoff) and news_after_cutoff > 0),
            "details": (
                "feature-store news loader has explicit published_at cutoff filter"
                if news_loader_has_cutoff
                else "feature-store news loader does not encode an explicit published_at cutoff filter"
            ),
        },
        {
            "check_name": "fundamentals_loader_cutoff_contract",
            "severity": "warning",
            "status": (
                "pass"
                if fundamentals_loader_has_cutoff
                else ("fail" if fundamentals_after_cutoff > 0 else "warn")
            ),
            "violation_count": (
                0 if fundamentals_loader_has_cutoff or fundamentals_after_cutoff <= 0 else fundamentals_after_cutoff
            ),
            "hard_fail_flag": bool(
                (not fundamentals_loader_has_cutoff) and fundamentals_after_cutoff > 0
            ),
            "details": (
                "feature-store fundamentals loader has explicit disclosed_at cutoff filter"
                if fundamentals_loader_has_cutoff
                else "feature-store fundamentals loader does not encode an explicit disclosed_at cutoff filter"
            ),
        },
    ]
    return pd.DataFrame(rows)


def _cohort_frame(frame: pd.DataFrame, *, scorer_variant: str, cohort: str, top_k: int) -> pd.DataFrame:
    if scorer_variant == "raw_model":
        rank_column = "raw_model_rank"
    elif scorer_variant == "selection_v2":
        rank_column = "selection_v2_rank"
    else:
        raise KeyError(f"Unknown scorer_variant: {scorer_variant}")

    working = frame.loc[frame["score_variant"] == scorer_variant].copy()
    if cohort == "all":
        return working
    if cohort == f"top{top_k}":
        return working.loc[working[rank_column] <= top_k].copy()
    if cohort == "top20":
        return working.loc[working[rank_column] <= 20].copy()
    raise KeyError(f"Unknown cohort: {cohort}")


def _safe_mean(frame: pd.DataFrame, column: str) -> float | None:
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _safe_spearman(frame: pd.DataFrame, score_column: str) -> float | None:
    pair = frame[[score_column, "realized_excess_return"]].copy()
    pair[score_column] = pd.to_numeric(pair[score_column], errors="coerce")
    pair["realized_excess_return"] = pd.to_numeric(pair["realized_excess_return"], errors="coerce")
    pair = pair.dropna()
    if len(pair) < 2:
        return None
    score_rank = pair[score_column].rank(method="average")
    outcome_rank = pair["realized_excess_return"].rank(method="average")
    corr = score_rank.corr(outcome_rank)
    if pd.isna(corr):
        return None
    return float(corr)


def _sector_concentration(frame: pd.DataFrame) -> float | None:
    if frame.empty:
        return None
    counts = frame["sector"].fillna("UNKNOWN").value_counts(normalize=True)
    if counts.empty:
        return None
    return float(counts.iloc[0])


def _liquidity_tail_exposure(frame: pd.DataFrame) -> float | None:
    if frame.empty or "liquidity_tail_flag" not in frame.columns:
        return None
    values = pd.to_numeric(frame["liquidity_tail_flag"], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _build_variant_metrics(
    base_frame: pd.DataFrame,
    *,
    selection_date: date,
    horizon: int,
    scorer_variant: str,
    cohort: str,
    top_k: int,
) -> dict[str, object]:
    cohort_frame = _cohort_frame(
        base_frame,
        scorer_variant=scorer_variant,
        cohort=cohort,
        top_k=top_k,
    )
    score_column = "score_value"
    overlap = None
    if cohort.startswith("top"):
        raw_symbols = set(
            _cohort_frame(base_frame, scorer_variant="raw_model", cohort=cohort, top_k=top_k)[
                "symbol"
            ].tolist()
        )
        selection_symbols = set(
            _cohort_frame(
                base_frame,
                scorer_variant="selection_v2",
                cohort=cohort,
                top_k=top_k,
            )["symbol"].tolist()
        )
        if raw_symbols or selection_symbols:
            denominator = max(len(raw_symbols | selection_symbols), 1)
            overlap = len(raw_symbols & selection_symbols) / denominator
    return {
        "selection_date": selection_date,
        "horizon": int(horizon),
        "scorer_variant": scorer_variant,
        "cohort": cohort,
        "symbol_count": int(len(cohort_frame)),
        "rank_ic": _safe_spearman(cohort_frame, score_column),
        "mean_realized_excess_return": _safe_mean(cohort_frame, "realized_excess_return"),
        "hit_rate": _safe_mean(
            cohort_frame.assign(hit_flag=pd.to_numeric(cohort_frame["realized_excess_return"], errors="coerce").gt(0).astype(float)),
            "hit_flag",
        ),
        "top10_mean_excess_return": _safe_mean(
            cohort_frame.sort_values(score_column, ascending=False).head(10),
            "realized_excess_return",
        ),
        "top20_mean_excess_return": _safe_mean(
            cohort_frame.sort_values(score_column, ascending=False).head(20),
            "realized_excess_return",
        ),
        "avg_recent_1d_return": _safe_mean(cohort_frame, "recent_1d_return"),
        "avg_recent_3d_return": _safe_mean(cohort_frame, "recent_3d_return"),
        "avg_recent_5d_return": _safe_mean(cohort_frame, "recent_5d_return"),
        "avg_recent_10d_return": _safe_mean(cohort_frame, "recent_10d_return"),
        "avg_distance_to_20d_high": _safe_mean(cohort_frame, "distance_to_20d_high"),
        "avg_turnover_zscore": _safe_mean(cohort_frame, "turnover_zscore"),
        "avg_news_density_3d": _safe_mean(cohort_frame, "news_density_3d"),
        "sector_concentration": _sector_concentration(cohort_frame),
        "liquidity_tail_exposure": _liquidity_tail_exposure(cohort_frame),
        "overlap_with_selection_v2": overlap if scorer_variant == "raw_model" else 1.0,
    }


def compute_decomposition_metrics(
    connection,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
    top_k: int,
) -> pd.DataFrame:
    feature_names = (
        "ret_1d",
        "ret_3d",
        "ret_5d",
        "ret_10d",
        "dist_from_20d_high",
        "turnover_z_5_20",
        "news_count_3d",
        "adv_20",
    )
    horizon_placeholders = ",".join("?" for _ in horizons)
    feature_placeholders = ",".join("?" for _ in feature_names)
    joined = connection.execute(
        f"""
        WITH feature_pivot AS (
            SELECT
                as_of_date,
                symbol,
                MAX(CASE WHEN feature_name = 'ret_1d' THEN feature_value END) AS recent_1d_return,
                MAX(CASE WHEN feature_name = 'ret_3d' THEN feature_value END) AS recent_3d_return,
                MAX(CASE WHEN feature_name = 'ret_5d' THEN feature_value END) AS recent_5d_return,
                MAX(CASE WHEN feature_name = 'ret_10d' THEN feature_value END) AS recent_10d_return,
                MAX(CASE WHEN feature_name = 'dist_from_20d_high' THEN feature_value END) AS distance_to_20d_high,
                MAX(CASE WHEN feature_name = 'turnover_z_5_20' THEN feature_value END) AS turnover_zscore,
                MAX(CASE WHEN feature_name = 'news_count_3d' THEN feature_value END) AS news_density_3d,
                MAX(CASE WHEN feature_name = 'adv_20' THEN feature_value END) AS adv_20
            FROM fact_feature_snapshot
            WHERE as_of_date BETWEEN ? AND ?
              AND feature_name IN ({feature_placeholders})
            GROUP BY as_of_date, symbol
        )
        SELECT
            ranking.as_of_date AS selection_date,
            ranking.symbol,
            ranking.horizon,
            ranking.final_selection_value,
            ranking.final_selection_rank_pct,
            prediction.expected_excess_return,
            outcome.realized_excess_return,
            symbol_meta.sector,
            symbol_meta.market,
            feature_pivot.recent_1d_return,
            feature_pivot.recent_3d_return,
            feature_pivot.recent_5d_return,
            feature_pivot.recent_10d_return,
            feature_pivot.distance_to_20d_high,
            feature_pivot.turnover_zscore,
            feature_pivot.news_density_3d,
            feature_pivot.adv_20
        FROM fact_ranking AS ranking
        LEFT JOIN fact_prediction AS prediction
          ON ranking.as_of_date = prediction.as_of_date
         AND ranking.symbol = prediction.symbol
         AND ranking.horizon = prediction.horizon
         AND prediction.ranking_version = ranking.ranking_version
         AND prediction.prediction_version = ?
        LEFT JOIN fact_selection_outcome AS outcome
          ON ranking.as_of_date = outcome.selection_date
         AND ranking.symbol = outcome.symbol
         AND ranking.horizon = outcome.horizon
         AND ranking.ranking_version = outcome.ranking_version
         AND outcome.outcome_status = 'matured'
        LEFT JOIN feature_pivot
          ON ranking.as_of_date = feature_pivot.as_of_date
         AND ranking.symbol = feature_pivot.symbol
        LEFT JOIN dim_symbol AS symbol_meta
          ON ranking.symbol = symbol_meta.symbol
        WHERE ranking.as_of_date BETWEEN ? AND ?
          AND ranking.horizon IN ({horizon_placeholders})
          AND ranking.ranking_version = ?
        ORDER BY ranking.as_of_date, ranking.horizon, ranking.symbol
        """,
        [
            start_date,
            end_date,
            *feature_names,
            ALPHA_PREDICTION_VERSION,
            start_date,
            end_date,
            *horizons,
            SELECTION_ENGINE_VERSION,
        ],
    ).fetchdf()
    if joined.empty:
        return pd.DataFrame()

    joined["selection_date"] = pd.to_datetime(joined["selection_date"]).dt.date
    joined["raw_model_rank"] = joined.groupby(["selection_date", "horizon"])[
        "expected_excess_return"
    ].rank(method="first", ascending=False, na_option="bottom")
    joined["selection_v2_rank"] = joined.groupby(["selection_date", "horizon"])[
        "final_selection_value"
    ].rank(method="first", ascending=False, na_option="bottom")

    joined["liquidity_decile"] = (
        joined.groupby(["selection_date", "horizon"])["adv_20"]
        .rank(method="average", pct=True)
        .mul(10)
        .clip(upper=9.999)
        .fillna(1.0)
        .astype(int)
        + 1
    )
    joined["liquidity_tail_flag"] = joined["liquidity_decile"].eq(1).astype(float)

    metric_rows: list[dict[str, object]] = []
    for (selection_date, horizon), group in joined.groupby(["selection_date", "horizon"], sort=True):
        raw = group.copy()
        raw["score_variant"] = "raw_model"
        raw["score_value"] = pd.to_numeric(raw["expected_excess_return"], errors="coerce")
        selected = group.copy()
        selected["score_variant"] = "selection_v2"
        selected["score_value"] = pd.to_numeric(selected["final_selection_value"], errors="coerce")
        combined = pd.concat([raw, selected], ignore_index=True)
        for scorer_variant in ("raw_model", "selection_v2"):
            for cohort in ("all", f"top{top_k}", "top20"):
                metric_rows.append(
                    _build_variant_metrics(
                        combined,
                        selection_date=selection_date,
                        horizon=int(horizon),
                        scorer_variant=scorer_variant,
                        cohort=cohort,
                        top_k=top_k,
                    )
                )
    return pd.DataFrame(metric_rows)


def _safe_metric(frame: pd.DataFrame, scorer_variant: str, metric_name: str, *, cohort: str) -> float:
    subset = frame.loc[
        (frame["scorer_variant"] == scorer_variant) & (frame["cohort"] == cohort),
        metric_name,
    ]
    values = pd.to_numeric(subset, errors="coerce").dropna()
    if values.empty:
        return 0.0
    return float(values.mean())


def _chase_score(frame: pd.DataFrame, scorer_variant: str, *, cohort: str) -> float:
    run_5 = max(_safe_metric(frame, scorer_variant, "avg_recent_5d_return", cohort=cohort), 0.0)
    run_10 = max(_safe_metric(frame, scorer_variant, "avg_recent_10d_return", cohort=cohort), 0.0)
    near_high = max(
        -_safe_metric(frame, scorer_variant, "avg_distance_to_20d_high", cohort=cohort),
        0.0,
    )
    turnover = max(
        _safe_metric(frame, scorer_variant, "avg_turnover_zscore", cohort=cohort),
        0.0,
    )
    news = max(_safe_metric(frame, scorer_variant, "avg_news_density_3d", cohort=cohort), 0.0)
    return run_5 + 0.5 * run_10 + 0.5 * near_high + 0.05 * turnover + 0.02 * news


def decide_branch(
    *,
    pit_checks: pd.DataFrame,
    decomposition_metrics: pd.DataFrame,
    top_k: int,
    thresholds: dict[str, float] | None = None,
) -> dict[str, object]:
    active_thresholds = dict(DEFAULT_BRANCH_THRESHOLDS)
    if thresholds:
        active_thresholds.update(thresholds)

    hard_fails = pit_checks.loc[pit_checks["hard_fail_flag"].fillna(False).astype(bool)].copy()
    if not hard_fails.empty:
        reasons = hard_fails["check_name"].astype(str).tolist()
        return {
            "pit_status": "fail",
            "branch_recommendation": "C",
            "decision_reasons": reasons,
            "hard_fail_checks": reasons,
            "selection_chase_delta": None,
            "model_chase_signal": None,
            "selection_chase_signal": None,
            "notes": "Critical PIT failures detected; fix timing/data contract before model or selection changes.",
        }

    cohort = f"top{top_k}"
    raw_subset = decomposition_metrics.loc[
        (decomposition_metrics["scorer_variant"] == "raw_model")
        & (decomposition_metrics["cohort"] == cohort)
    ]
    selection_subset = decomposition_metrics.loc[
        (decomposition_metrics["scorer_variant"] == "selection_v2")
        & (decomposition_metrics["cohort"] == cohort)
    ]
    if raw_subset.empty or selection_subset.empty:
        return {
            "pit_status": "warn",
            "branch_recommendation": "C",
            "decision_reasons": ["insufficient_phase0_metrics"],
            "hard_fail_checks": [],
            "selection_chase_delta": None,
            "model_chase_signal": None,
            "selection_chase_signal": None,
            "notes": "Phase 0 metrics are insufficient to distinguish model-vs-selection chasing.",
        }

    model_chase_signal = _chase_score(decomposition_metrics, "raw_model", cohort=cohort)
    selection_chase_signal = _chase_score(decomposition_metrics, "selection_v2", cohort=cohort)
    selection_chase_delta = selection_chase_signal - model_chase_signal

    if model_chase_signal >= float(active_thresholds["model_chase_threshold"]):
        branch = "B"
        reasons = [
            "raw_model_chase_signal_high",
            "selection_overlay_review_required",
        ]
    elif selection_chase_delta >= float(active_thresholds["selection_delta_threshold"]):
        branch = "A"
        reasons = ["selection_amplifies_chase_exposure"]
    else:
        branch = "A"
        reasons = ["selection_first_conservative_path"]

    return {
        "pit_status": "pass",
        "branch_recommendation": branch,
        "decision_reasons": reasons,
        "hard_fail_checks": [],
        "selection_chase_delta": selection_chase_delta,
        "model_chase_signal": model_chase_signal,
        "selection_chase_signal": selection_chase_signal,
        "notes": "Branch chosen from phase0 decomposition metrics with no critical PIT failures.",
    }


def _write_markdown_report(
    path: Path,
    *,
    start_date: date,
    end_date: date,
    cutoff_time: str,
    branch_decision: dict[str, object],
    pit_checks: pd.DataFrame,
    decomposition_metrics: pd.DataFrame,
) -> Path:
    lines = [
        "# Alpha Phase 0 Audit Report",
        "",
        f"- Range: `{start_date.isoformat()}..{end_date.isoformat()}`",
        f"- Cutoff time: `{cutoff_time}`",
        f"- PIT status: `{branch_decision['pit_status']}`",
        f"- Recommended branch: `{branch_decision['branch_recommendation']}`",
        f"- Reasons: `{', '.join(branch_decision['decision_reasons'])}`",
        "",
        "## PIT Checks",
        "",
        "| Check | Severity | Status | Violations | Hard fail |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for row in pit_checks.itertuples(index=False):
        lines.append(
            f"| {row.check_name} | {row.severity} | {row.status} | {int(row.violation_count)} | {bool(row.hard_fail_flag)} |"
        )
    lines.extend(
        [
            "",
            "## Decomposition Snapshot",
            "",
            "| Variant | Cohort | Mean excess | Hit rate | Recent 5d | Dist to 20d high | Turnover z | News density | Sector concentration | Liquidity tail |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    summary = (
        decomposition_metrics.groupby(["scorer_variant", "cohort"], as_index=False)
        .agg(
            mean_realized_excess_return=("mean_realized_excess_return", "mean"),
            hit_rate=("hit_rate", "mean"),
            avg_recent_5d_return=("avg_recent_5d_return", "mean"),
            avg_distance_to_20d_high=("avg_distance_to_20d_high", "mean"),
            avg_turnover_zscore=("avg_turnover_zscore", "mean"),
            avg_news_density_3d=("avg_news_density_3d", "mean"),
            sector_concentration=("sector_concentration", "mean"),
            liquidity_tail_exposure=("liquidity_tail_exposure", "mean"),
        )
        if not decomposition_metrics.empty
        else pd.DataFrame()
    )
    for row in summary.itertuples(index=False):
        lines.append(
            "| {variant} | {cohort} | {mean_excess:.4f} | {hit_rate:.4f} | {recent_5d:.4f} | {dist_high:.4f} | {turnover:.4f} | {news:.4f} | {sector:.4f} | {liq:.4f} |".format(
                variant=row.scorer_variant,
                cohort=row.cohort,
                mean_excess=float(row.mean_realized_excess_return or 0.0),
                hit_rate=float(row.hit_rate or 0.0),
                recent_5d=float(row.avg_recent_5d_return or 0.0),
                dist_high=float(row.avg_distance_to_20d_high or 0.0),
                turnover=float(row.avg_turnover_zscore or 0.0),
                news=float(row.avg_news_density_3d or 0.0),
                sector=float(row.sector_concentration or 0.0),
                liq=float(row.liquidity_tail_exposure or 0.0),
            )
        )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def run_alpha_phase0_audit(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
    cutoff_time: str = "17:30",
    top_k: int = DEFAULT_TOP_K,
) -> AlphaPhase0AuditResult:
    ensure_storage_layout(settings)
    with activate_run_context("run_alpha_phase0_audit", as_of_date=end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_news_item",
                    "fact_fundamentals_snapshot",
                    "fact_feature_snapshot",
                    "fact_prediction",
                    "fact_ranking",
                    "fact_selection_outcome",
                    "fact_model_training_run",
                ],
                notes=(
                    "Run alpha-edge phase0 audit. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()} horizons={horizons}"
                ),
            )
            artifact_paths: list[str] = []
            try:
                pit_checks = run_pit_checks(
                    connection,
                    settings=settings,
                    start_date=start_date,
                    end_date=end_date,
                    cutoff_time=cutoff_time,
                )
                decomposition_metrics = compute_decomposition_metrics(
                    connection,
                    start_date=start_date,
                    end_date=end_date,
                    horizons=horizons,
                    top_k=top_k,
                )
                branch_decision = decide_branch(
                    pit_checks=pit_checks,
                    decomposition_metrics=decomposition_metrics,
                    top_k=top_k,
                )
                artifact_dir = _phase0_artifact_dir(
                    settings,
                    start_date=start_date,
                    end_date=end_date,
                    run_id=run_context.run_id,
                )
                pit_path = artifact_dir / "pit_checks.parquet"
                pit_checks.to_parquet(pit_path, index=False)
                artifact_paths.append(str(pit_path))

                metrics_path = artifact_dir / "decomposition_metrics.parquet"
                decomposition_metrics.to_parquet(metrics_path, index=False)
                artifact_paths.append(str(metrics_path))

                branch_payload = {
                    "run_id": run_context.run_id,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "horizons": [int(value) for value in horizons],
                    "pit_status": branch_decision["pit_status"],
                    "branch_recommendation": branch_decision["branch_recommendation"],
                    "decision_reasons": list(branch_decision["decision_reasons"]),
                    "hard_fail_checks": list(branch_decision["hard_fail_checks"]),
                    "selection_chase_delta": branch_decision["selection_chase_delta"],
                    "model_chase_signal": branch_decision["model_chase_signal"],
                    "selection_chase_signal": branch_decision["selection_chase_signal"],
                    "notes": branch_decision["notes"],
                }
                branch_path = artifact_dir / "branch_decision.json"
                branch_path.write_text(
                    json.dumps(branch_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                artifact_paths.append(str(branch_path))

                report_path = _write_markdown_report(
                    artifact_dir / "phase0_report.md",
                    start_date=start_date,
                    end_date=end_date,
                    cutoff_time=cutoff_time,
                    branch_decision=branch_payload,
                    pit_checks=pit_checks,
                    decomposition_metrics=decomposition_metrics,
                )
                artifact_paths.append(str(report_path))

                notes = (
                    "Alpha phase0 audit completed. "
                    f"branch={branch_decision['branch_recommendation']} "
                    f"pit_status={branch_decision['pit_status']} "
                    f"metric_rows={len(decomposition_metrics)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return AlphaPhase0AuditResult(
                    run_id=run_context.run_id,
                    start_date=start_date,
                    end_date=end_date,
                    row_count=len(decomposition_metrics),
                    pit_status=str(branch_decision["pit_status"]),
                    branch_recommendation=str(branch_decision["branch_recommendation"]),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=artifact_paths,
                    notes="Alpha phase0 audit failed.",
                    error_message=str(exc),
                )
                raise
