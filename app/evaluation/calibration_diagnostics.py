from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.evaluation.outcomes import DEFAULT_RANKING_VERSIONS, materialize_selection_outcomes
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet


@dataclass(slots=True)
class CalibrationDiagnosticResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def upsert_calibration_diagnostics(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("calibration_diagnostic_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_calibration_diagnostic
        WHERE (
            diagnostic_date,
            horizon,
            ranking_version,
            bin_type,
            bin_value
        ) IN (
            SELECT
                diagnostic_date,
                horizon,
                ranking_version,
                bin_type,
                bin_value
            FROM calibration_diagnostic_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_calibration_diagnostic (
            diagnostic_date,
            window_start,
            window_end,
            horizon,
            ranking_version,
            bin_type,
            bin_value,
            sample_count,
            expected_median,
            expected_q25,
            expected_q75,
            observed_mean,
            observed_median,
            observed_q25,
            observed_q75,
            median_bias,
            coverage_rate,
            above_upper_rate,
            below_lower_rate,
            monotonicity_order,
            quality_flag,
            evaluation_run_id,
            created_at
        )
        SELECT
            diagnostic_date,
            window_start,
            window_end,
            horizon,
            ranking_version,
            bin_type,
            bin_value,
            sample_count,
            expected_median,
            expected_q25,
            expected_q75,
            observed_mean,
            observed_median,
            observed_q25,
            observed_q75,
            median_bias,
            coverage_rate,
            above_upper_rate,
            below_lower_rate,
            monotonicity_order,
            quality_flag,
            evaluation_run_id,
            created_at
        FROM calibration_diagnostic_stage
        """
    )
    connection.unregister("calibration_diagnostic_stage")


def _quantile_or_none(series: pd.Series, quantile: float) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.quantile(quantile))


def _mean_or_none(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _assign_expected_return_bins(frame: pd.DataFrame, *, bin_count: int) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype="object")
    ranked = pd.to_numeric(frame["expected_excess_return_at_selection"], errors="coerce").rank(
        method="first"
    )
    active_bins = max(1, min(int(bin_count), len(frame)))
    labels = [f"bin_{index:02d}" for index in range(1, active_bins + 1)]
    return pd.qcut(ranked, q=active_bins, labels=labels, duplicates="drop")


def _quality_flag(sample_count: int, coverage_rate: float | None, *, bin_count: int) -> str:
    if sample_count < max(5, bin_count):
        return "low_sample"
    if coverage_rate is None:
        return "band_missing"
    if coverage_rate < 0.30 or coverage_rate > 0.70:
        return "coverage_drift"
    return "ok"


def _write_markdown_report(path: Path, *, diagnostic_frame: pd.DataFrame) -> Path:
    lines = [
        "# Calibration Diagnostics",
        "",
        (
            "| Date | Horizon | Bin Type | Bin | Sample | Expected Median | "
            "Observed Mean | Coverage | Quality |"
        ),
        "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in diagnostic_frame.itertuples(index=False):
        lines.append(
            "| "
            f"{row.diagnostic_date} | {int(row.horizon)} | {row.bin_type} | {row.bin_value} | "
            f"{int(row.sample_count)} | "
            f"{'' if pd.isna(row.expected_median) else f'{row.expected_median:.4f}'} | "
            f"{'' if pd.isna(row.observed_mean) else f'{row.observed_mean:.4f}'} | "
            f"{'' if pd.isna(row.coverage_rate) else f'{row.coverage_rate:.4f}'} | "
            f"{row.quality_flag} |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def materialize_calibration_diagnostics(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    bin_count: int,
    limit_symbols: int | None = None,
    ranking_versions: list[str] | None = None,
) -> CalibrationDiagnosticResult:
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
        "materialize_calibration_diagnostics",
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
                    "Build calibration diagnostics from frozen prediction bands. "
                    f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                ),
                ranking_version=",".join(ranking_versions),
            )
            try:
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
                outcomes = connection.execute(
                    f"""
                    SELECT *
                    FROM fact_selection_outcome
                    WHERE selection_date BETWEEN ? AND ?
                      AND horizon IN ({horizon_placeholders})
                      AND ranking_version IN ({version_placeholders})
                      AND outcome_status = 'matured'
                      AND band_available_flag
                    {limit_clause}
                    ORDER BY selection_date, ranking_version, horizon, symbol
                    """,
                    params,
                ).fetchdf()
                if outcomes.empty:
                    notes = (
                        "No matured band-backed outcomes were available "
                        "for calibration diagnostics. "
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
                    return CalibrationDiagnosticResult(
                        run_id=run_context.run_id,
                        start_selection_date=start_selection_date,
                        end_selection_date=end_selection_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                outcomes["selection_date"] = pd.to_datetime(outcomes["selection_date"]).dt.date
                diagnostic_rows: list[dict[str, object]] = []
                for (horizon, ranking_version), group in outcomes.groupby(
                    ["horizon", "ranking_version"],
                    sort=True,
                ):
                    working = group.copy()
                    working["expected_return_bin"] = _assign_expected_return_bins(
                        working,
                        bin_count=bin_count,
                    )
                    bin_groups: list[tuple[str, str, pd.DataFrame]] = [("overall", "all", working)]
                    bin_groups.extend(
                        (
                            "expected_return_bin",
                            str(bin_name),
                            working.loc[working["expected_return_bin"] == bin_name].copy(),
                        )
                        for bin_name in working["expected_return_bin"]
                        .dropna()
                        .astype(str)
                        .sort_values()
                        .unique()
                    )

                    expected_bin_rows: list[dict[str, object]] = []
                    for bin_type, bin_value, subset in bin_groups:
                        if subset.empty:
                            continue
                        coverage_rate = _mean_or_none(subset["in_band_flag"].astype(float))
                        row = {
                            "diagnostic_date": end_selection_date,
                            "window_start": start_selection_date,
                            "window_end": end_selection_date,
                            "horizon": int(horizon),
                            "ranking_version": str(ranking_version),
                            "bin_type": bin_type,
                            "bin_value": bin_value,
                            "sample_count": int(len(subset)),
                            "expected_median": _quantile_or_none(
                                subset["median_band_at_selection"],
                                0.50,
                            ),
                            "expected_q25": _quantile_or_none(
                                subset["lower_band_at_selection"],
                                0.50,
                            ),
                            "expected_q75": _quantile_or_none(
                                subset["upper_band_at_selection"],
                                0.50,
                            ),
                            "observed_mean": _mean_or_none(subset["realized_excess_return"]),
                            "observed_median": _quantile_or_none(
                                subset["realized_excess_return"],
                                0.50,
                            ),
                            "observed_q25": _quantile_or_none(
                                subset["realized_excess_return"],
                                0.25,
                            ),
                            "observed_q75": _quantile_or_none(
                                subset["realized_excess_return"],
                                0.75,
                            ),
                            "median_bias": None,
                            "coverage_rate": coverage_rate,
                            "above_upper_rate": _mean_or_none(
                                subset["above_upper_flag"].astype(float)
                            ),
                            "below_lower_rate": _mean_or_none(
                                subset["below_lower_flag"].astype(float)
                            ),
                            "monotonicity_order": None,
                            "quality_flag": _quality_flag(
                                len(subset),
                                coverage_rate,
                                bin_count=bin_count,
                            ),
                            "evaluation_run_id": run_context.run_id,
                            "created_at": pd.Timestamp.utcnow(),
                        }
                        if (
                            row["observed_median"] is not None
                            and row["expected_median"] is not None
                        ):
                            row["median_bias"] = float(row["observed_median"]) - float(
                                row["expected_median"]
                            )
                        diagnostic_rows.append(row)
                        if bin_type == "expected_return_bin":
                            expected_bin_rows.append(row)

                    if len(expected_bin_rows) >= 2:
                        monotonic_frame = pd.DataFrame(expected_bin_rows).copy()
                        monotonic_frame["bin_index"] = range(1, len(monotonic_frame) + 1)
                        monotonicity = monotonic_frame["bin_index"].corr(
                            monotonic_frame["observed_mean"]
                        )
                        if pd.notna(monotonicity):
                            monotonicity_value = float(monotonicity)
                            for row in diagnostic_rows:
                                if row["horizon"] == int(horizon) and row["ranking_version"] == str(
                                    ranking_version
                                ):
                                    row["monotonicity_order"] = monotonicity_value

                diagnostic_frame = pd.DataFrame(diagnostic_rows)
                upsert_calibration_diagnostics(connection, diagnostic_frame)

                artifact_paths = [
                    str(
                        write_parquet(
                            diagnostic_frame,
                            base_dir=settings.paths.curated_dir,
                            dataset="evaluation/calibration_diagnostics",
                            partitions={
                                "start_selection_date": start_selection_date.isoformat(),
                                "end_selection_date": end_selection_date.isoformat(),
                            },
                            filename="calibration_diagnostics.parquet",
                        )
                    )
                ]
                markdown_path = _write_markdown_report(
                    settings.paths.artifacts_dir
                    / "evaluation"
                    / "calibration"
                    / f"{run_context.run_id}.md",
                    diagnostic_frame=diagnostic_frame.sort_values(
                        ["horizon", "bin_type", "bin_value"]
                    ),
                )
                artifact_paths.append(str(markdown_path))
                notes = (
                    "Calibration diagnostics materialized. "
                    f"rows={len(diagnostic_frame)} range={start_selection_date.isoformat()}.."
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
                return CalibrationDiagnosticResult(
                    run_id=run_context.run_id,
                    start_selection_date=start_selection_date,
                    end_selection_date=end_selection_date,
                    row_count=len(diagnostic_frame),
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
                        "Calibration diagnostics failed. "
                        f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                    ),
                    error_message=str(exc),
                    ranking_version=",".join(ranking_versions),
                )
                raise
