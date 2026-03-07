from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ranking.explanatory_score import RANKING_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet


@dataclass(slots=True)
class RankingValidationResult:
    run_id: str
    start_date: date
    end_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str
    ranking_version: str


def upsert_ranking_validation_summary(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("ranking_validation_stage", frame)
    connection.execute(
        """
        DELETE FROM ops_ranking_validation_summary
        WHERE run_id = (SELECT DISTINCT run_id FROM ranking_validation_stage LIMIT 1)
        """
    )
    connection.execute(
        """
        INSERT INTO ops_ranking_validation_summary (
            run_id,
            start_date,
            end_date,
            horizon,
            bucket_type,
            bucket_name,
            symbol_count,
            avg_gross_forward_return,
            avg_excess_forward_return,
            median_excess_forward_return,
            top_decile_gap,
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
            avg_gross_forward_return,
            avg_excess_forward_return,
            median_excess_forward_return,
            top_decile_gap,
            created_at
        FROM ranking_validation_stage
        """
    )
    connection.unregister("ranking_validation_stage")


def _write_markdown_report(
    path: Path, *, start_date: date, end_date: date, summary: pd.DataFrame
) -> Path:
    lines = [
        "# Ranking Validation Summary",
        "",
        f"- Range: `{start_date.isoformat()}..{end_date.isoformat()}`",
        f"- Rows: `{len(summary)}`",
        "",
        (
            "| Horizon | Bucket Type | Bucket | Count | Avg Gross | Avg Excess | "
            "Median Excess | Top Decile Gap |"
        ),
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            "| "
            f"{row.horizon} | {row.bucket_type} | {row.bucket_name} | {int(row.symbol_count)} | "
            f"{row.avg_gross_forward_return:.4f} | {row.avg_excess_forward_return:.4f} | "
            f"{row.median_excess_forward_return:.4f} | {row.top_decile_gap:.4f} |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def validate_explanatory_ranking(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
) -> RankingValidationResult:
    ensure_storage_layout(settings)

    with activate_run_context("validate_explanatory_ranking", as_of_date=end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_ranking", "fact_forward_return_label"],
                notes=(
                    "Validate explanatory ranking sanity. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()} horizons={horizons}"
                ),
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
                        label.gross_forward_return,
                        label.excess_forward_return
                    FROM fact_ranking AS ranking
                    JOIN fact_forward_return_label AS label
                      ON ranking.as_of_date = label.as_of_date
                     AND ranking.symbol = label.symbol
                     AND ranking.horizon = label.horizon
                    WHERE ranking.as_of_date BETWEEN ? AND ?
                      AND ranking.horizon IN ({horizon_placeholders})
                      AND label.label_available_flag
                    """,
                    [start_date, end_date, *horizons],
                ).fetchdf()
                if joined.empty:
                    notes = (
                        "No overlapping ranking/label rows were available for validation. "
                        f"range={start_date.isoformat()}..{end_date.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=RANKING_VERSION,
                    )
                    return RankingValidationResult(
                        run_id=run_context.run_id,
                        start_date=start_date,
                        end_date=end_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                        ranking_version=RANKING_VERSION,
                    )

                joined["decile_bucket"] = (
                    (joined["final_selection_rank_pct"].clip(lower=0, upper=0.9999) * 10).astype(
                        int
                    )
                    + 1
                ).astype(str)

                grade_summary = (
                    joined.groupby(["horizon", "grade"], as_index=False)
                    .agg(
                        symbol_count=("symbol", "count"),
                        avg_gross_forward_return=("gross_forward_return", "mean"),
                        avg_excess_forward_return=("excess_forward_return", "mean"),
                        median_excess_forward_return=("excess_forward_return", "median"),
                    )
                    .rename(columns={"grade": "bucket_name"})
                )
                grade_summary["bucket_type"] = "grade"

                decile_summary = (
                    joined.groupby(["horizon", "decile_bucket"], as_index=False)
                    .agg(
                        symbol_count=("symbol", "count"),
                        avg_gross_forward_return=("gross_forward_return", "mean"),
                        avg_excess_forward_return=("excess_forward_return", "mean"),
                        median_excess_forward_return=("excess_forward_return", "median"),
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
                summary["created_at"] = pd.Timestamp.utcnow()

                upsert_ranking_validation_summary(connection, summary)
                artifact_paths = [
                    str(
                        write_parquet(
                            summary,
                            base_dir=settings.paths.artifacts_dir,
                            dataset="validation/ranking",
                            partitions={
                                "start_date": start_date.isoformat(),
                                "end_date": end_date.isoformat(),
                            },
                            filename="ranking_validation_summary.parquet",
                        )
                    )
                ]
                markdown_path = _write_markdown_report(
                    settings.paths.artifacts_dir
                    / "validation"
                    / "ranking"
                    / f"{run_context.run_id}.md",
                    start_date=start_date,
                    end_date=end_date,
                    summary=summary.sort_values(["bucket_type", "horizon", "bucket_name"]),
                )
                artifact_paths.append(str(markdown_path))

                notes = (
                    "Ranking validation completed. "
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
                    ranking_version=RANKING_VERSION,
                )
                return RankingValidationResult(
                    run_id=run_context.run_id,
                    start_date=start_date,
                    end_date=end_date,
                    row_count=len(summary),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    ranking_version=RANKING_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=(
                        "Ranking validation failed. "
                        f"range={start_date.isoformat()}..{end_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=RANKING_VERSION,
                )
                raise
