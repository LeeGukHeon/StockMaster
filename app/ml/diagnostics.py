from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import MODEL_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class ModelDiagnosticReportResult:
    run_id: str
    train_end_date: date
    artifact_paths: list[str]
    notes: str


def render_model_diagnostic_report(
    settings: Settings,
    *,
    train_end_date: date,
    horizons: list[int],
    dry_run: bool = False,
) -> ModelDiagnosticReportResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "render_model_diagnostic_report", as_of_date=train_end_date
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_model_training_run", "fact_model_metric_summary"],
                notes=(
                    "Render model diagnostic report. "
                    f"train_end_date={train_end_date.isoformat()} horizons={horizons}"
                ),
            )
            try:
                horizon_placeholders = ",".join("?" for _ in horizons)
                latest_runs = connection.execute(
                    f"""
                    SELECT *
                    FROM vw_latest_model_training_run
                    WHERE model_version = ?
                      AND horizon IN ({horizon_placeholders})
                      AND train_end_date <= ?
                    ORDER BY horizon
                    """,
                    [MODEL_VERSION, *horizons, train_end_date],
                ).fetchdf()
                metrics = connection.execute(
                    f"""
                    SELECT *
                    FROM vw_latest_model_metric_summary
                    WHERE model_version = ?
                      AND horizon IN ({horizon_placeholders})
                    ORDER BY horizon, member_name, split_name, metric_name
                    """,
                    [MODEL_VERSION, *horizons],
                ).fetchdf()
                artifact_root = (
                    settings.paths.artifacts_dir
                    / "model_diagnostics"
                    / f"train_end_date={train_end_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_root.mkdir(parents=True, exist_ok=True)
                markdown_path = artifact_root / "model_diagnostic_report.md"
                lines = [
                    "# Model Diagnostic Report",
                    "",
                    f"- Model version: `{MODEL_VERSION}`",
                    f"- Train end date: `{train_end_date.isoformat()}`",
                    f"- Dry run: `{dry_run}`",
                    "",
                    "## Latest Training Runs",
                    "",
                ]
                if latest_runs.empty:
                    lines.append("No successful training runs were found.")
                else:
                    lines.extend(
                        [
                            (
                                "| Horizon | Train End | Train Rows | Validation Rows | "
                                "Fallback | Reason |"
                            ),
                            "| --- | --- | ---: | ---: | --- | --- |",
                        ]
                    )
                    for row in latest_runs.itertuples(index=False):
                        lines.append(
                            "| "
                            f"{int(row.horizon)} | {row.train_end_date} | "
                            f"{int(row.train_row_count)} | "
                            f"{int(row.validation_row_count)} | {bool(row.fallback_flag)} | "
                            f"{row.fallback_reason or ''} |"
                        )
                lines.extend(["", "## Latest Metrics", ""])
                if metrics.empty:
                    lines.append("No metric rows were found.")
                else:
                    lines.extend(
                        [
                            "| Horizon | Member | Split | Metric | Value | Sample Count |",
                            "| --- | --- | --- | --- | ---: | ---: |",
                        ]
                    )
                    for row in metrics.itertuples(index=False):
                        value = "" if pd.isna(row.metric_value) else f"{row.metric_value:.6f}"
                        lines.append(
                            "| "
                            f"{int(row.horizon)} | {row.member_name} | {row.split_name} | "
                            f"{row.metric_name} | {value} | {int(row.sample_count or 0)} |"
                        )
                markdown_path.write_text("\n".join(lines), encoding="utf-8")
                artifact_paths = [str(markdown_path)]
                notes = (
                    "Model diagnostic report rendered. "
                    f"training_runs={len(latest_runs)} metric_rows={len(metrics)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=MODEL_VERSION,
                )
                return ModelDiagnosticReportResult(
                    run_id=run_context.run_id,
                    train_end_date=train_end_date,
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
                    notes="Model diagnostic report rendering failed.",
                    error_message=str(exc),
                    model_version=MODEL_VERSION,
                )
                raise
