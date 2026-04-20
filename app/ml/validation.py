from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import (
    MODEL_SPEC_ID,
    MODEL_VERSION,
    PREDICTION_VERSION,
    get_alpha_model_spec,
    resolve_validation_primary_metric_for_spec,
)
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


def _validation_reference_runs_sql(horizon_array_sql: str) -> str:
    return f"""
        WITH required_horizons AS (
            SELECT UNNEST([{horizon_array_sql}]) AS horizon
        ),
        active_runs AS (
            SELECT horizon, training_run_id
            FROM (
                SELECT
                    horizon,
                    training_run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY horizon
                        ORDER BY effective_from_date DESC, created_at DESC, active_alpha_model_id DESC
                    ) AS row_number
                FROM fact_alpha_active_model
                WHERE model_version = ?
                  AND effective_from_date <= ?
                  AND (effective_to_date IS NULL OR effective_to_date >= ?)
                  AND active_flag = TRUE
            )
            WHERE row_number = 1
        ),
        latest_default_runs AS (
            SELECT horizon, training_run_id
            FROM (
                SELECT
                    horizon,
                    training_run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY horizon
                        ORDER BY train_end_date DESC, created_at DESC, training_run_id DESC
                    ) AS row_number
                FROM fact_model_training_run
                WHERE model_version = ?
                  AND model_spec_id = ?
                  AND train_end_date <= ?
                  AND status = 'success'
            )
            WHERE row_number = 1
        ),
        reference_runs AS (
            SELECT
                required_horizons.horizon,
                COALESCE(active_runs.training_run_id, latest_default_runs.training_run_id) AS training_run_id
            FROM required_horizons
            LEFT JOIN active_runs
              ON active_runs.horizon = required_horizons.horizon
            LEFT JOIN latest_default_runs
              ON latest_default_runs.horizon = required_horizons.horizon
        )
    """


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


def _resolve_required_validation_metrics(
    model_spec_id: str | None,
    *,
    horizon: int,
) -> tuple[str, ...]:
    if model_spec_id:
        try:
            model_spec = get_alpha_model_spec(str(model_spec_id))
        except KeyError:
            model_spec = None
        if model_spec is not None:
            return (
                "mae",
                "corr",
                "rank_ic",
                resolve_validation_primary_metric_for_spec(model_spec, horizon=horizon),
                "top20_mean_excess_return",
            )
    return (
        "mae",
        "corr",
        "rank_ic",
        "top10_mean_excess_return",
        "top20_mean_excess_return",
    )


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
                reference_runs_sql = _validation_reference_runs_sql(horizon_array_sql)
                validation_reference_params = [
                    MODEL_VERSION,
                    as_of_date,
                    as_of_date,
                    MODEL_VERSION,
                    MODEL_SPEC_ID,
                    as_of_date,
                ]
                reference_run_rows = connection.execute(
                    reference_runs_sql
                    + """
                    SELECT
                        reference_runs.horizon,
                        reference_runs.training_run_id,
                        training_run.model_spec_id
                    FROM reference_runs
                    LEFT JOIN fact_model_training_run AS training_run
                      ON reference_runs.training_run_id = training_run.training_run_id
                    ORDER BY reference_runs.horizon
                    """,
                    validation_reference_params,
                ).fetchall()
                missing_training_runs = sum(
                    1 for _, training_run_id, _ in reference_run_rows if training_run_id is None
                )
                checks.append(
                    {
                        "check_name": "training_run_present",
                        "status": "pass" if int(missing_training_runs or 0) == 0 else "warn",
                        "value": int(missing_training_runs or 0),
                        "threshold": 0,
                        "detail": (
                            "An active-model run or latest default-spec run should exist for each requested horizon."
                        ),
                    }
                )

                training_run_ids = [
                    str(training_run_id)
                    for _, training_run_id, _ in reference_run_rows
                    if training_run_id is not None
                ]
                metric_rows: list[tuple[str, str, object]] = []
                if training_run_ids:
                    metric_placeholders = ",".join("?" for _ in training_run_ids)
                    metric_rows = connection.execute(
                        f"""
                        SELECT training_run_id, metric_name, metric_value
                        FROM fact_model_metric_summary
                        WHERE model_version = ?
                          AND member_name = 'ensemble'
                          AND split_name = 'validation'
                          AND training_run_id IN ({metric_placeholders})
                        """,
                        [MODEL_VERSION, *training_run_ids],
                    ).fetchall()
                metric_lookup = {
                    (str(training_run_id), str(metric_name)): metric_value
                    for training_run_id, metric_name, metric_value in metric_rows
                }
                missing_validation_metrics = 0
                horizon_metric_map: dict[int, tuple[str, ...]] = {}
                training_run_lookup: dict[int, str | None] = {}
                model_spec_lookup: dict[int, str | None] = {}
                for horizon, training_run_id, model_spec_id in reference_run_rows:
                    horizon_int = int(horizon)
                    horizon_metric_map[horizon_int] = _resolve_required_validation_metrics(
                        None if model_spec_id in (None, "") else str(model_spec_id),
                        horizon=horizon_int,
                    )
                    training_run_lookup[horizon_int] = (
                        None if training_run_id in (None, "") else str(training_run_id)
                    )
                    model_spec_lookup[horizon_int] = (
                        None if model_spec_id in (None, "") else str(model_spec_id)
                    )
                    if training_run_id is None:
                        missing_validation_metrics += len(horizon_metric_map[horizon_int])
                        continue
                    missing_validation_metrics += sum(
                        (str(training_run_id), metric_name) not in metric_lookup
                        for metric_name in horizon_metric_map[horizon_int]
                    )
                checks.append(
                    {
                        "check_name": "validation_metrics_present",
                        "status": "pass" if int(missing_validation_metrics or 0) == 0 else "warn",
                        "value": int(missing_validation_metrics or 0),
                        "threshold": 0,
                        "detail": (
                            "Latest ensemble validation metrics should include "
                            "mae/corr/rank_ic/top10/top20 for each requested horizon."
                        ),
                    }
                )

                for horizon in horizons:
                    horizon_int = int(horizon)
                    training_run_id = training_run_lookup.get(horizon_int)
                    metric_names = horizon_metric_map.get(
                        horizon_int,
                        _resolve_required_validation_metrics(None, horizon=horizon_int),
                    )
                    display_metric_names = tuple(
                        dict.fromkeys(
                            metric_name
                            for metric_name in (
                                metric_names[-2] if len(metric_names) >= 2 else "top10_mean_excess_return",
                                "top20_mean_excess_return",
                                "rank_ic",
                            )
                        )
                    )
                    for metric_name in display_metric_names:
                        raw_metric_value = (
                            None
                            if training_run_id is None
                            else metric_lookup.get((training_run_id, metric_name))
                        )
                        metric_value = (
                            None if raw_metric_value is None else float(raw_metric_value)
                        )
                        checks.append(
                            {
                                "check_name": f"{metric_name}_h{horizon_int}",
                                "status": "pass" if metric_value is not None else "warn",
                                "value": "" if metric_value is None else round(metric_value, 6),
                                "threshold": "present",
                                "detail": (
                                    "Latest ensemble validation metric should be available for "
                                    f"horizon={horizon_int} model_spec_id="
                                    f"{model_spec_lookup.get(horizon_int) or MODEL_SPEC_ID}."
                                ),
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
