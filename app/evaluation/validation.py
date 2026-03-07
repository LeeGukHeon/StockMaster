from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.calibration import PREDICTION_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class EvaluationPipelineValidationResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
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
        "# Evaluation Pipeline Validation",
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


def validate_evaluation_pipeline(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
) -> EvaluationPipelineValidationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "validate_evaluation_pipeline",
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
                input_sources=[
                    "fact_selection_outcome",
                    "fact_evaluation_summary",
                    "fact_calibration_diagnostic",
                ],
                notes=(
                    "Validate frozen evaluation artifacts for consistency. "
                    f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                ),
                ranking_version=f"{SELECTION_ENGINE_VERSION},{EXPLANATORY_RANKING_VERSION}",
            )
            try:
                horizon_placeholders = ",".join("?" for _ in horizons)
                checks: list[dict[str, object]] = []

                duplicate_count = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT
                            selection_date,
                            symbol,
                            horizon,
                            ranking_version,
                            COUNT(*) AS row_count
                        FROM fact_selection_outcome
                        WHERE selection_date BETWEEN ? AND ?
                          AND horizon IN ({horizon_placeholders})
                        GROUP BY selection_date, symbol, horizon, ranking_version
                        HAVING COUNT(*) > 1
                    )
                    """,
                    [start_selection_date, end_selection_date, *horizons],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "selection_outcome_duplicate_keys",
                        "status": "pass" if int(duplicate_count or 0) == 0 else "fail",
                        "value": int(duplicate_count or 0),
                        "threshold": 0,
                        "detail": (
                            "Duplicate (selection_date, symbol, horizon, ranking_version) rows"
                        ),
                    }
                )

                label_mismatch_count = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM fact_selection_outcome AS outcome
                    JOIN fact_forward_return_label AS label
                      ON outcome.selection_date = label.as_of_date
                     AND outcome.symbol = label.symbol
                     AND outcome.horizon = label.horizon
                    WHERE outcome.selection_date BETWEEN ? AND ?
                      AND outcome.horizon IN ({horizon_placeholders})
                      AND outcome.outcome_status = 'matured'
                      AND (
                        ABS(
                            COALESCE(outcome.realized_return, 0.0)
                            - COALESCE(label.gross_forward_return, 0.0)
                        ) > 1e-9
                        OR ABS(
                            COALESCE(outcome.realized_excess_return, 0.0)
                            - COALESCE(label.excess_forward_return, 0.0)
                        ) > 1e-9
                      )
                    """,
                    [start_selection_date, end_selection_date, *horizons],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "matured_outcome_matches_label",
                        "status": "pass" if int(label_mismatch_count or 0) == 0 else "fail",
                        "value": int(label_mismatch_count or 0),
                        "threshold": 0,
                        "detail": (
                            "Frozen realized return/excess must match fact_forward_return_label"
                        ),
                    }
                )

                prediction_freeze_mismatch = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM fact_selection_outcome AS outcome
                    JOIN fact_prediction AS prediction
                      ON outcome.selection_date = prediction.as_of_date
                     AND outcome.symbol = prediction.symbol
                     AND outcome.horizon = prediction.horizon
                     AND outcome.ranking_version = prediction.ranking_version
                     AND prediction.prediction_version = ?
                    WHERE outcome.selection_date BETWEEN ? AND ?
                      AND outcome.horizon IN ({horizon_placeholders})
                      AND outcome.ranking_version = ?
                      AND (
                        ABS(
                            COALESCE(outcome.expected_excess_return_at_selection, 0.0)
                            - COALESCE(prediction.expected_excess_return, 0.0)
                        ) > 1e-9
                        OR ABS(
                            COALESCE(outcome.lower_band_at_selection, 0.0)
                            - COALESCE(prediction.lower_band, 0.0)
                        ) > 1e-9
                        OR ABS(
                            COALESCE(outcome.upper_band_at_selection, 0.0)
                            - COALESCE(prediction.upper_band, 0.0)
                        ) > 1e-9
                      )
                    """,
                    [
                        PREDICTION_VERSION,
                        start_selection_date,
                        end_selection_date,
                        *horizons,
                        SELECTION_ENGINE_VERSION,
                    ],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "frozen_prediction_matches_snapshot",
                        "status": "pass" if int(prediction_freeze_mismatch or 0) == 0 else "fail",
                        "value": int(prediction_freeze_mismatch or 0),
                        "threshold": 0,
                        "detail": (
                            "Frozen prediction fields should match fact_prediction snapshot rows"
                        ),
                    }
                )

                cohort_gap_count = connection.execute(
                    f"""
                    WITH outcome_counts AS (
                        SELECT
                            selection_date AS summary_date,
                            horizon,
                            ranking_version,
                            COUNT(*) FILTER (WHERE outcome_status = 'matured') AS matured_count
                        FROM fact_selection_outcome
                        WHERE selection_date BETWEEN ? AND ?
                          AND horizon IN ({horizon_placeholders})
                        GROUP BY selection_date, horizon, ranking_version
                    ),
                    summary_counts AS (
                        SELECT
                            summary_date,
                            horizon,
                            ranking_version,
                            count_evaluated
                        FROM fact_evaluation_summary
                        WHERE summary_date BETWEEN ? AND ?
                          AND horizon IN ({horizon_placeholders})
                          AND window_type = 'cohort'
                          AND segment_type = 'coverage'
                          AND segment_value = 'all'
                    )
                    SELECT COUNT(*)
                    FROM outcome_counts AS outcomes
                    LEFT JOIN summary_counts AS summary
                      ON outcomes.summary_date = summary.summary_date
                     AND outcomes.horizon = summary.horizon
                     AND outcomes.ranking_version = summary.ranking_version
                    WHERE
                        COALESCE(outcomes.matured_count, -1)
                        <> COALESCE(summary.count_evaluated, -1)
                    """,
                    [
                        start_selection_date,
                        end_selection_date,
                        *horizons,
                        start_selection_date,
                        end_selection_date,
                        *horizons,
                    ],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "cohort_summary_matches_outcomes",
                        "status": "pass" if int(cohort_gap_count or 0) == 0 else "fail",
                        "value": int(cohort_gap_count or 0),
                        "threshold": 0,
                        "detail": (
                            "fact_evaluation_summary cohort rows should match "
                            "matured outcome counts"
                        ),
                    }
                )

                missing_calibration_rows = connection.execute(
                    f"""
                    WITH required_horizons AS (
                        SELECT UNNEST([
                            {",".join(str(int(value)) for value in horizons)}
                        ]) AS horizon
                    )
                    SELECT COUNT(*)
                    FROM required_horizons AS required
                    LEFT JOIN (
                        SELECT DISTINCT horizon
                        FROM fact_calibration_diagnostic
                        WHERE diagnostic_date >= ?
                          AND ranking_version = ?
                    ) AS existing
                      ON required.horizon = existing.horizon
                    WHERE existing.horizon IS NULL
                    """,
                    [start_selection_date, SELECTION_ENGINE_VERSION],
                ).fetchone()[0]
                checks.append(
                    {
                        "check_name": "calibration_diagnostic_present",
                        "status": "pass" if int(missing_calibration_rows or 0) == 0 else "warn",
                        "value": int(missing_calibration_rows or 0),
                        "threshold": 0,
                        "detail": (
                            "Missing horizon-level calibration diagnostics for selection_engine_v1"
                        ),
                    }
                )

                status = "success"
                if any(check["status"] == "fail" for check in checks):
                    status = "failed"
                artifact_paths = _write_validation_artifacts(
                    settings.paths.artifacts_dir / "evaluation" / "validation",
                    run_id=run_context.run_id,
                    checks=checks,
                )
                notes = (
                    "Evaluation pipeline validation completed. "
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
                    ranking_version=f"{SELECTION_ENGINE_VERSION},{EXPLANATORY_RANKING_VERSION}",
                    error_message=None
                    if status == "success"
                    else "Evaluation validation detected failures.",
                )
                return EvaluationPipelineValidationResult(
                    run_id=run_context.run_id,
                    start_selection_date=start_selection_date,
                    end_selection_date=end_selection_date,
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
                    notes=(
                        "Evaluation pipeline validation failed. "
                        f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=f"{SELECTION_ENGINE_VERSION},{EXPLANATORY_RANKING_VERSION}",
                )
                raise
