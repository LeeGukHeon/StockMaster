from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import MODEL_VERSION, PREDICTION_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class AlphaModelValidationResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def _write_validation_artifacts(
    base_dir: Path,
    *,
    run_id: str,
    checks: list[dict[str, object]],
) -> list[str]:
    base_dir.mkdir(parents=True, exist_ok=True)
    json_path = base_dir / f"{run_id}.json"
    markdown_path = base_dir / f"{run_id}.md"
    json_path.write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Alpha Model v1 Validation",
        "",
        "| Check | Status | Value | Threshold | Detail |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for check in checks:
        lines.append(
            "| "
            f"{check['check_name']} | {check['status']} | {check['value']} | "
            f"{check['threshold']} | {check['detail']} |"
        )
    markdown_path.write_text("\n".join(lines), encoding="utf-8")
    return [str(json_path), str(markdown_path)]


def validate_alpha_model_v1(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
) -> AlphaModelValidationResult:
    ensure_storage_layout(settings)
    with activate_run_context("validate_alpha_model_v1", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_model_training_run",
                    "fact_model_metric_summary",
                    "fact_prediction",
                ],
                notes=(
                    "Validate alpha model v1 registry and latest inference coverage. "
                    f"as_of_date={as_of_date.isoformat()}"
                ),
            )
            try:
                horizon_placeholders = ",".join("?" for _ in horizons)
                horizon_array_sql = ",".join(str(int(value)) for value in horizons)
                checks: list[dict[str, object]] = []
                missing_training_runs = connection.execute(
                    f"""
                    WITH required AS (
                        SELECT UNNEST([{horizon_array_sql}]) AS horizon
                    )
                    SELECT COUNT(*)
                    FROM required
                    LEFT JOIN (
                        SELECT DISTINCT horizon
                        FROM fact_model_training_run
                        WHERE model_version = ?
                          AND train_end_date <= ?
                          AND status = 'success'
                    ) AS existing
                      ON required.horizon = existing.horizon
                    WHERE existing.horizon IS NULL
                    """,
                    [MODEL_VERSION, as_of_date],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "training_run_present",
                        "status": "pass" if int(missing_training_runs or 0) == 0 else "warn",
                        "value": int(missing_training_runs or 0),
                        "threshold": 0,
                        "detail": (
                            "Successful alpha training run should exist for each requested horizon."
                        ),
                    }
                )

                missing_validation_metrics = connection.execute(
                    f"""
                    WITH required AS (
                        SELECT UNNEST([{horizon_array_sql}]) AS horizon
                    )
                    SELECT COUNT(*)
                    FROM required
                    LEFT JOIN (
                        SELECT DISTINCT horizon
                        FROM fact_model_metric_summary
                        WHERE model_version = ?
                          AND member_name = 'ensemble'
                          AND split_name = 'validation'
                    ) AS existing
                      ON required.horizon = existing.horizon
                    WHERE existing.horizon IS NULL
                    """,
                    [MODEL_VERSION],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "validation_metrics_present",
                        "status": "pass" if int(missing_validation_metrics or 0) == 0 else "warn",
                        "value": int(missing_validation_metrics or 0),
                        "threshold": 0,
                        "detail": "Validation metrics should exist for the ensemble member.",
                    }
                )

                inference_gaps = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT horizon
                        FROM fact_prediction
                        WHERE as_of_date = ?
                          AND prediction_version = ?
                        GROUP BY horizon
                    ) AS inferred
                    RIGHT JOIN (
                        SELECT UNNEST([{horizon_array_sql}]) AS horizon
                    ) AS required
                      ON inferred.horizon = required.horizon
                    WHERE inferred.horizon IS NULL
                    """,
                    [as_of_date, PREDICTION_VERSION],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "latest_inference_present",
                        "status": "pass" if int(inference_gaps or 0) == 0 else "warn",
                        "value": int(inference_gaps or 0),
                        "threshold": 0,
                        "detail": "Prediction rows should exist for the requested as-of date.",
                    }
                )

                fallback_ratio = connection.execute(
                    f"""
                    SELECT AVG(CASE WHEN fallback_flag THEN 1.0 ELSE 0.0 END)
                    FROM fact_prediction
                    WHERE as_of_date = ?
                      AND horizon IN ({horizon_placeholders})
                      AND prediction_version = ?
                    """,
                    [as_of_date, *horizons, PREDICTION_VERSION],
                ).fetchone()[0]
                fallback_ratio_value = 0.0 if fallback_ratio is None else float(fallback_ratio)
                checks.append(
                    {
                        "check_name": "fallback_ratio",
                        "status": "pass" if fallback_ratio_value <= 0.75 else "warn",
                        "value": round(fallback_ratio_value, 4),
                        "threshold": 0.75,
                        "detail": "High fallback ratio means ML coverage is still thin.",
                    }
                )

                invalid_disagreement = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM fact_prediction
                    WHERE as_of_date = ?
                      AND horizon IN ({horizon_placeholders})
                      AND prediction_version = ?
                      AND disagreement_score IS NOT NULL
                      AND disagreement_score < 0
                    """,
                    [as_of_date, *horizons, PREDICTION_VERSION],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "disagreement_non_negative",
                        "status": "pass" if int(invalid_disagreement or 0) == 0 else "fail",
                        "value": int(invalid_disagreement or 0),
                        "threshold": 0,
                        "detail": "Disagreement score must be non-negative when populated.",
                    }
                )

                artifact_paths = _write_validation_artifacts(
                    settings.paths.artifacts_dir / "model_validation",
                    run_id=run_context.run_id,
                    checks=checks,
                )
                status = (
                    "failed" if any(check["status"] == "fail" for check in checks) else "success"
                )
                notes = (
                    "Alpha model v1 validation completed. "
                    f"failures={sum(check['status'] == 'fail' for check in checks)} "
                    f"warnings={sum(check['status'] == 'warn' for check in checks)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status=status,
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=MODEL_VERSION,
                    error_message=None
                    if status == "success"
                    else "Alpha validation detected failures.",
                )
                return AlphaModelValidationResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(checks),
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
                    notes="Alpha model v1 validation failed.",
                    error_message=str(exc),
                    model_version=MODEL_VERSION,
                )
                raise
