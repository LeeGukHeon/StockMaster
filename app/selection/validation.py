from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.selection.calibration import PREDICTION_VERSION, _ensure_selection_history
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet


@dataclass(slots=True)
class SelectionValidationResult:
    run_id: str
    start_date: date
    end_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str
    ranking_version: str


def upsert_selection_validation_summary(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("selection_validation_stage", frame)
    connection.execute(
        """
        DELETE FROM ops_selection_validation_summary
        WHERE run_id = (SELECT DISTINCT run_id FROM selection_validation_stage LIMIT 1)
        """
    )
    connection.execute(
        """
        INSERT INTO ops_selection_validation_summary (
            run_id,
            start_date,
            end_date,
            horizon,
            bucket_type,
            bucket_name,
            symbol_count,
            avg_excess_forward_return,
            median_excess_forward_return,
            hit_rate,
            avg_expected_excess_return,
            avg_prediction_error,
            top_decile_gap,
            ranking_version,
            created_at
        )
        SELECT
            run_id,
            start_date,
            end_date,
            horizon,
            bucket_type,
            bucket_name,
            symbol_count,
            avg_excess_forward_return,
            median_excess_forward_return,
            hit_rate,
            avg_expected_excess_return,
            avg_prediction_error,
            top_decile_gap,
            ranking_version,
            created_at
        FROM selection_validation_stage
        """
    )
    connection.unregister("selection_validation_stage")


def _write_markdown_report(
    path: Path,
    *,
    start_date: date,
    end_date: date,
    summary: pd.DataFrame,
) -> Path:
    lines = [
        "# Selection Engine v1 Validation Summary",
        "",
        f"- Range: `{start_date.isoformat()}..{end_date.isoformat()}`",
        f"- Rows: `{len(summary)}`",
        "",
        (
            "| Horizon | Bucket Type | Bucket | Count | Avg Excess | "
            "Median Excess | Hit Rate | Top Decile Gap |"
        ),
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            "| "
            f"{row.horizon} | {row.bucket_type} | {row.bucket_name} | {int(row.symbol_count)} | "
            f"{row.avg_excess_forward_return:.4f} | {row.median_excess_forward_return:.4f} | "
            f"{row.hit_rate:.4f} | {row.top_decile_gap:.4f} |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def validate_selection_engine_v1(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
) -> SelectionValidationResult:
    ensure_storage_layout(settings)
    _ensure_selection_history(
        settings,
        start_date=start_date,
        end_date=end_date,
        horizons=horizons,
    )

    with activate_run_context("validate_selection_engine_v1", as_of_date=end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_ranking", "fact_forward_return_label", "fact_prediction"],
                notes=(
                    "Validate selection engine v1 sanity. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()} horizons={horizons}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                horizon_placeholders = ",".join("?" for _ in horizons)
                joined = connection.execute(
                    f"""
                    SELECT
                        ranking.as_of_date,
                        ranking.symbol,
                        ranking.horizon,
                        ranking.grade,
                        ranking.final_selection_value,
                        ranking.final_selection_rank_pct,
                        label.excess_forward_return,
                        prediction.expected_excess_return
                    FROM fact_ranking AS ranking
                    JOIN fact_forward_return_label AS label
                      ON ranking.as_of_date = label.as_of_date
                     AND ranking.symbol = label.symbol
                     AND ranking.horizon = label.horizon
                    LEFT JOIN fact_prediction AS prediction
                      ON ranking.as_of_date = prediction.as_of_date
                     AND ranking.symbol = prediction.symbol
                     AND ranking.horizon = prediction.horizon
                     AND prediction.prediction_version = ?
                     AND prediction.ranking_version = ?
                    WHERE ranking.as_of_date BETWEEN ? AND ?
                      AND ranking.horizon IN ({horizon_placeholders})
                      AND ranking.ranking_version = ?
                      AND label.label_available_flag
                    """,
                    [
                        PREDICTION_VERSION,
                        SELECTION_ENGINE_VERSION,
                        start_date,
                        end_date,
                        *horizons,
                        SELECTION_ENGINE_VERSION,
                    ],
                ).fetchdf()
                if joined.empty:
                    notes = (
                        "No overlapping selection-engine rows and forward labels were available "
                        f"for validation. range={start_date.isoformat()}..{end_date.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return SelectionValidationResult(
                        run_id=run_context.run_id,
                        start_date=start_date,
                        end_date=end_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )

                joined["decile_bucket"] = (
                    (
                        joined["final_selection_rank_pct"]
                        .clip(lower=0.0, upper=0.9999)
                        .mul(10)
                        .astype(int)
                    )
                    + 1
                ).astype(str)
                joined["hit_flag"] = joined["excess_forward_return"].gt(0).astype(float)
                joined["prediction_error"] = (
                    joined["excess_forward_return"] - joined["expected_excess_return"]
                )

                grade_summary = (
                    joined.groupby(["horizon", "grade"], as_index=False)
                    .agg(
                        symbol_count=("symbol", "count"),
                        avg_excess_forward_return=("excess_forward_return", "mean"),
                        median_excess_forward_return=("excess_forward_return", "median"),
                        hit_rate=("hit_flag", "mean"),
                        avg_expected_excess_return=("expected_excess_return", "mean"),
                        avg_prediction_error=("prediction_error", "mean"),
                    )
                    .rename(columns={"grade": "bucket_name"})
                )
                grade_summary["bucket_type"] = "grade"

                decile_summary = (
                    joined.groupby(["horizon", "decile_bucket"], as_index=False)
                    .agg(
                        symbol_count=("symbol", "count"),
                        avg_excess_forward_return=("excess_forward_return", "mean"),
                        median_excess_forward_return=("excess_forward_return", "median"),
                        hit_rate=("hit_flag", "mean"),
                        avg_expected_excess_return=("expected_excess_return", "mean"),
                        avg_prediction_error=("prediction_error", "mean"),
                    )
                    .rename(columns={"decile_bucket": "bucket_name"})
                )
                decile_summary["bucket_type"] = "decile"

                summary = pd.concat([grade_summary, decile_summary], ignore_index=True)
                gap_map: dict[int, float] = {}
                for horizon, group in joined.groupby("horizon"):
                    top_decile = group.loc[
                        group["decile_bucket"] == "10",
                        "excess_forward_return",
                    ].mean()
                    bottom_decile = group.loc[
                        group["decile_bucket"] == "1",
                        "excess_forward_return",
                    ].mean()
                    gap_map[int(horizon)] = float(top_decile - bottom_decile)
                summary["run_id"] = run_context.run_id
                summary["start_date"] = start_date
                summary["end_date"] = end_date
                summary["top_decile_gap"] = summary["horizon"].map(gap_map)
                summary["ranking_version"] = SELECTION_ENGINE_VERSION
                summary["created_at"] = pd.Timestamp.utcnow()
                upsert_selection_validation_summary(connection, summary)

                artifact_paths = [
                    str(
                        write_parquet(
                            summary,
                            base_dir=settings.paths.artifacts_dir,
                            dataset="validation/selection_engine_v1",
                            partitions={
                                "start_date": start_date.isoformat(),
                                "end_date": end_date.isoformat(),
                            },
                            filename="selection_validation_summary.parquet",
                        )
                    )
                ]
                markdown_path = _write_markdown_report(
                    settings.paths.artifacts_dir
                    / "validation"
                    / "selection_engine_v1"
                    / f"{run_context.run_id}.md",
                    start_date=start_date,
                    end_date=end_date,
                    summary=summary.sort_values(["bucket_type", "horizon", "bucket_name"]),
                )
                artifact_paths.append(str(markdown_path))

                notes = (
                    "Selection engine v1 validation completed. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()}, "
                    f"summary_rows={len(summary)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return SelectionValidationResult(
                    run_id=run_context.run_id,
                    start_date=start_date,
                    end_date=end_date,
                    row_count=len(summary),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=(
                        "Selection engine v1 validation failed. "
                        f"range={start_date.isoformat()}..{end_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
