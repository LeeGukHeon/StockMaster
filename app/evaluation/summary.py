from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.evaluation.outcomes import (
    DEFAULT_RANKING_VERSIONS,
    materialize_selection_outcomes,
)
from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet


@dataclass(slots=True)
class PredictionEvaluationResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_evaluation_summary(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("evaluation_summary_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_evaluation_summary
        WHERE (
            summary_date,
            window_type,
            horizon,
            ranking_version,
            segment_type,
            segment_value
        ) IN (
            SELECT
                summary_date,
                window_type,
                horizon,
                ranking_version,
                segment_type,
                segment_value
            FROM evaluation_summary_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_evaluation_summary (
            summary_date,
            window_type,
            window_start,
            window_end,
            horizon,
            ranking_version,
            segment_type,
            segment_value,
            count_total,
            count_evaluated,
            count_pending,
            mean_realized_return,
            mean_realized_excess_return,
            median_realized_excess_return,
            hit_rate,
            positive_raw_return_rate,
            band_coverage_rate,
            above_upper_rate,
            below_lower_rate,
            avg_expected_excess_return,
            avg_prediction_error,
            overlap_count,
            score_monotonicity_hint,
            evaluation_run_id,
            created_at
        )
        SELECT
            summary_date,
            window_type,
            window_start,
            window_end,
            horizon,
            ranking_version,
            segment_type,
            segment_value,
            count_total,
            count_evaluated,
            count_pending,
            mean_realized_return,
            mean_realized_excess_return,
            median_realized_excess_return,
            hit_rate,
            positive_raw_return_rate,
            band_coverage_rate,
            above_upper_rate,
            below_lower_rate,
            avg_expected_excess_return,
            avg_prediction_error,
            overlap_count,
            score_monotonicity_hint,
            evaluation_run_id,
            created_at
        FROM evaluation_summary_stage
        """
    )
    connection.unregister("evaluation_summary_stage")


def _mean_or_none(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _median_or_none(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.median())


def _correlation_or_none(left: pd.Series, right: pd.Series) -> float | None:
    pair = pd.DataFrame(
        {
            "left": pd.to_numeric(left, errors="coerce"),
            "right": pd.to_numeric(right, errors="coerce"),
        }
    ).dropna()
    if len(pair) < 2:
        return None
    correlation = pair["left"].corr(pair["right"])
    if pd.isna(correlation):
        return None
    return float(correlation)


def _segment_definitions(frame: pd.DataFrame) -> list[tuple[str, str, pd.DataFrame]]:
    return [
        ("coverage", "all", frame.copy()),
        (
            "selection",
            "top_decile",
            frame.loc[frame["selection_percentile"].fillna(0.0) >= 0.90].copy(),
        ),
        (
            "selection",
            "report_candidates",
            frame.loc[frame["report_candidate_flag"].fillna(False).astype(bool)].copy(),
        ),
    ]


def _overlap_count(
    frame: pd.DataFrame,
    comparison: pd.DataFrame,
) -> int | None:
    if frame.empty or comparison.empty:
        return None
    left_keys = {
        (pd.Timestamp(row.selection_date).date(), str(row.symbol))
        for row in frame.itertuples(index=False)
    }
    right_keys = {
        (pd.Timestamp(row.selection_date).date(), str(row.symbol))
        for row in comparison.itertuples(index=False)
    }
    return len(left_keys & right_keys)


def _build_summary_row(
    frame: pd.DataFrame,
    *,
    comparison_frame: pd.DataFrame,
    summary_date: date,
    window_type: str,
    window_start: date,
    window_end: date,
    horizon: int,
    ranking_version: str,
    segment_type: str,
    segment_value: str,
    run_id: str,
) -> dict[str, object]:
    matured = frame.loc[frame["outcome_status"] == "matured"].copy()
    band_subset = matured.loc[matured["band_available_flag"].fillna(False).astype(bool)].copy()
    return {
        "summary_date": summary_date,
        "window_type": window_type,
        "window_start": window_start,
        "window_end": window_end,
        "horizon": int(horizon),
        "ranking_version": ranking_version,
        "segment_type": segment_type,
        "segment_value": segment_value,
        "count_total": int(len(frame)),
        "count_evaluated": int(len(matured)),
        "count_pending": int(len(frame) - len(matured)),
        "mean_realized_return": _mean_or_none(matured["realized_return"]),
        "mean_realized_excess_return": _mean_or_none(matured["realized_excess_return"]),
        "median_realized_excess_return": _median_or_none(matured["realized_excess_return"]),
        "hit_rate": _mean_or_none(matured["realized_excess_return"].gt(0).astype(float)),
        "positive_raw_return_rate": _mean_or_none(matured["raw_positive_flag"].astype(float)),
        "band_coverage_rate": _mean_or_none(band_subset["in_band_flag"].astype(float)),
        "above_upper_rate": _mean_or_none(band_subset["above_upper_flag"].astype(float)),
        "below_lower_rate": _mean_or_none(band_subset["below_lower_flag"].astype(float)),
        "avg_expected_excess_return": _mean_or_none(matured["expected_excess_return_at_selection"]),
        "avg_prediction_error": _mean_or_none(matured["prediction_error"]),
        "overlap_count": _overlap_count(frame, comparison_frame),
        "score_monotonicity_hint": _correlation_or_none(
            matured["final_selection_value"],
            matured["realized_excess_return"],
        ),
        "evaluation_run_id": run_id,
        "created_at": pd.Timestamp.utcnow(),
    }


def _load_outcomes(
    connection,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    ranking_versions: list[str],
    limit_symbols: int | None,
) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    version_placeholders = ",".join("?" for _ in ranking_versions)
    params: list[object] = [
        start_selection_date,
        end_selection_date,
        *horizons,
        *ranking_versions,
    ]
    limit_clause = ""
    if limit_symbols is not None and limit_symbols > 0:
        limit_clause = (
            "QUALIFY ROW_NUMBER() OVER ("
            "PARTITION BY selection_date, horizon, ranking_version "
            "ORDER BY final_selection_value DESC, symbol"
            f") <= {int(limit_symbols)}"
        )
    return connection.execute(
        f"""
        SELECT *
        FROM fact_selection_outcome
        WHERE selection_date BETWEEN ? AND ?
          AND horizon IN ({horizon_placeholders})
          AND ranking_version IN ({version_placeholders})
        {limit_clause}
        ORDER BY selection_date, ranking_version, horizon, symbol
        """,
        params,
    ).fetchdf()


def _write_markdown_report(
    path: Path,
    *,
    summary: pd.DataFrame,
) -> Path:
    lines = [
        "# Prediction Evaluation Summary",
        "",
        (
            "| Summary Date | Window | Horizon | Ranking | Segment | "
            "Total | Evaluated | Avg Excess | Hit Rate |"
        ),
        "| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        avg_excess = (
            ""
            if pd.isna(row.mean_realized_excess_return)
            else f"{row.mean_realized_excess_return:.4f}"
        )
        hit_rate = "" if pd.isna(row.hit_rate) else f"{row.hit_rate:.4f}"
        lines.append(
            "| "
            f"{row.summary_date} | {row.window_type} | {int(row.horizon)} | "
            f"{row.ranking_version} | {row.segment_value} | {int(row.count_total)} | "
            f"{int(row.count_evaluated)} | {avg_excess} | {hit_rate} |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def materialize_prediction_evaluation(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    rolling_windows: list[int],
    limit_symbols: int | None = None,
    ranking_versions: list[str] | None = None,
) -> PredictionEvaluationResult:
    ensure_storage_layout(settings)
    ranking_versions = list(ranking_versions or DEFAULT_RANKING_VERSIONS)
    materialize_selection_outcomes(
        settings,
        start_selection_date=start_selection_date,
        end_selection_date=end_selection_date,
        horizons=horizons,
        limit_symbols=limit_symbols,
        ranking_versions=ranking_versions,
    )

    with activate_run_context(
        "materialize_prediction_evaluation",
        as_of_date=end_selection_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_selection_outcome"],
                notes=(
                    "Aggregate cohort and rolling evaluation summaries from frozen outcomes. "
                    f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                ),
                ranking_version=",".join(ranking_versions),
            )
            try:
                outcomes = _load_outcomes(
                    connection,
                    start_selection_date=start_selection_date,
                    end_selection_date=end_selection_date,
                    horizons=horizons,
                    ranking_versions=ranking_versions,
                    limit_symbols=limit_symbols,
                )
                if outcomes.empty:
                    notes = (
                        "No selection outcomes were available for evaluation "
                        "summary materialization. "
                        f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=",".join(ranking_versions),
                    )
                    return PredictionEvaluationResult(
                        run_id=run_context.run_id,
                        start_selection_date=start_selection_date,
                        end_selection_date=end_selection_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                outcomes["selection_date"] = pd.to_datetime(outcomes["selection_date"]).dt.date
                summary_rows: list[dict[str, object]] = []
                counterpart_map = {
                    SELECTION_ENGINE_VERSION: EXPLANATORY_RANKING_VERSION,
                    EXPLANATORY_RANKING_VERSION: SELECTION_ENGINE_VERSION,
                }
                for (selection_dt, horizon, ranking_version), group in outcomes.groupby(
                    ["selection_date", "horizon", "ranking_version"],
                    sort=True,
                ):
                    counterpart_group = outcomes.loc[
                        (outcomes["selection_date"] == selection_dt)
                        & (outcomes["horizon"] == horizon)
                        & (
                            outcomes["ranking_version"]
                            == counterpart_map.get(str(ranking_version), "__none__")
                        )
                    ].copy()
                    counterpart_segments = {
                        (seg_type, seg_value): seg_frame
                        for seg_type, seg_value, seg_frame in _segment_definitions(
                            counterpart_group
                        )
                    }
                    for segment_type, segment_value, segment_frame in _segment_definitions(group):
                        summary_rows.append(
                            _build_summary_row(
                                segment_frame,
                                comparison_frame=counterpart_segments.get(
                                    (segment_type, segment_value),
                                    pd.DataFrame(),
                                ),
                                summary_date=selection_dt,
                                window_type="cohort",
                                window_start=selection_dt,
                                window_end=selection_dt,
                                horizon=int(horizon),
                                ranking_version=str(ranking_version),
                                segment_type=segment_type,
                                segment_value=segment_value,
                                run_id=run_context.run_id,
                            )
                        )

                for window in sorted(
                    set(int(value) for value in rolling_windows if int(value) > 0)
                ):
                    for (horizon, ranking_version), group in outcomes.groupby(
                        ["horizon", "ranking_version"],
                        sort=True,
                    ):
                        selection_dates = sorted(group["selection_date"].drop_duplicates().tolist())
                        if not selection_dates:
                            continue
                        window_dates = (
                            selection_dates[-window:]
                            if len(selection_dates) > window
                            else selection_dates
                        )
                        window_frame = group.loc[group["selection_date"].isin(window_dates)].copy()
                        counterpart_frame = outcomes.loc[
                            outcomes["selection_date"].isin(window_dates)
                            & (outcomes["horizon"] == horizon)
                            & (
                                outcomes["ranking_version"]
                                == counterpart_map.get(str(ranking_version), "__none__")
                            )
                        ].copy()
                        counterpart_segments = {
                            (seg_type, seg_value): seg_frame
                            for seg_type, seg_value, seg_frame in _segment_definitions(
                                counterpart_frame
                            )
                        }
                        for segment_type, segment_value, segment_frame in _segment_definitions(
                            window_frame
                        ):
                            summary_rows.append(
                                _build_summary_row(
                                    segment_frame,
                                    comparison_frame=counterpart_segments.get(
                                        (segment_type, segment_value),
                                        pd.DataFrame(),
                                    ),
                                    summary_date=end_selection_date,
                                    window_type=f"rolling_{window}d",
                                    window_start=min(window_dates),
                                    window_end=max(window_dates),
                                    horizon=int(horizon),
                                    ranking_version=str(ranking_version),
                                    segment_type=segment_type,
                                    segment_value=segment_value,
                                    run_id=run_context.run_id,
                                )
                            )

                summary_frame = pd.DataFrame(summary_rows)
                upsert_evaluation_summary(connection, summary_frame)

                artifact_paths = [
                    str(
                        write_parquet(
                            summary_frame,
                            base_dir=settings.paths.curated_dir,
                            dataset="evaluation/summary",
                            partitions={
                                "start_selection_date": start_selection_date.isoformat(),
                                "end_selection_date": end_selection_date.isoformat(),
                            },
                            filename="evaluation_summary.parquet",
                        )
                    )
                ]
                markdown_path = _write_markdown_report(
                    settings.paths.artifacts_dir
                    / "evaluation"
                    / "summary"
                    / f"{run_context.run_id}.md",
                    summary=summary_frame.sort_values(
                        [
                            "summary_date",
                            "window_type",
                            "horizon",
                            "ranking_version",
                            "segment_value",
                        ]
                    ),
                )
                artifact_paths.append(str(markdown_path))
                notes = (
                    "Prediction evaluation summary materialized. "
                    f"rows={len(summary_frame)} range={start_selection_date.isoformat()}.."
                    f"{end_selection_date.isoformat()}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=",".join(ranking_versions),
                )
                return PredictionEvaluationResult(
                    run_id=run_context.run_id,
                    start_selection_date=start_selection_date,
                    end_selection_date=end_selection_date,
                    row_count=len(summary_frame),
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
                        "Prediction evaluation summary failed. "
                        f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=",".join(ranking_versions),
                )
                raise
