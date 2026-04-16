from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.evaluation.outcomes import materialize_selection_outcomes
from app.evaluation.summary import materialize_prediction_evaluation
from app.features.feature_store import build_feature_store
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.regime.snapshot import build_market_regime_snapshot
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V1_VERSION
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class SelectionEngineComparisonResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def _resolve_dates(
    connection, *, start_selection_date: date, end_selection_date: date
) -> list[date]:
    rows = connection.execute(
        """
        SELECT DISTINCT trading_date
        FROM fact_daily_ohlcv
        WHERE trading_date BETWEEN ? AND ?
        ORDER BY trading_date
        """,
        [start_selection_date, end_selection_date],
    ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows]


def compare_selection_engines(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    limit_symbols: int | None = None,
) -> SelectionEngineComparisonResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "compare_selection_engines", as_of_date=end_selection_date
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_ranking", "fact_selection_outcome", "fact_evaluation_summary"],
                notes=(
                    "Compare selection engine v2 against v1 and explanatory v0. "
                    f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                ),
            )
            try:
                selection_dates = _resolve_dates(
                    connection,
                    start_selection_date=start_selection_date,
                    end_selection_date=end_selection_date,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Selection engine comparison failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise

        for selection_date in selection_dates:
            build_feature_store(
                settings,
                as_of_date=selection_date,
                limit_symbols=limit_symbols,
                cutoff_time="17:30",
            )
            build_market_regime_snapshot(settings, as_of_date=selection_date)
            materialize_selection_engine_v2(
                settings,
                as_of_date=selection_date,
                horizons=horizons,
                limit_symbols=limit_symbols,
            )

        ranking_versions = [
            SELECTION_ENGINE_VERSION,
            SELECTION_ENGINE_V1_VERSION,
            EXPLANATORY_RANKING_VERSION,
        ]
        outcomes_result = materialize_selection_outcomes(
            settings,
            start_selection_date=start_selection_date,
            end_selection_date=end_selection_date,
            horizons=horizons,
            limit_symbols=limit_symbols,
            ranking_versions=ranking_versions,
        )
        evaluation_result = materialize_prediction_evaluation(
            settings,
            start_selection_date=start_selection_date,
            end_selection_date=end_selection_date,
            horizons=horizons,
            rolling_windows=[20, 60],
            limit_symbols=limit_symbols,
            ranking_versions=ranking_versions,
        )

        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            comparison_frame = connection.execute(
                """
                WITH latest_summary AS (
                    SELECT *
                    FROM vw_latest_evaluation_summary
                    WHERE segment_type = 'coverage'
                      AND segment_value = 'all'
                      AND ranking_version IN (?, ?, ?)
                )
                SELECT
                    v2.summary_date,
                    v2.window_type,
                    v2.horizon,
                    v2.mean_realized_excess_return AS selection_v2_avg_excess,
                    v1.mean_realized_excess_return AS selection_v1_avg_excess,
                    expl.mean_realized_excess_return AS explanatory_v0_avg_excess,
                    v2.mean_realized_excess_return - v1.mean_realized_excess_return
                        AS v2_vs_v1_gap,
                    v2.mean_realized_excess_return - expl.mean_realized_excess_return
                        AS v2_vs_explanatory_gap,
                    v2.hit_rate - v1.hit_rate AS v2_vs_v1_hit_rate_gap,
                    v2.hit_rate - expl.hit_rate AS v2_vs_explanatory_hit_rate_gap
                FROM latest_summary AS v2
                LEFT JOIN latest_summary AS v1
                  ON v2.summary_date = v1.summary_date
                 AND v2.window_type = v1.window_type
                 AND v2.horizon = v1.horizon
                 AND v1.ranking_version = ?
                LEFT JOIN latest_summary AS expl
                  ON v2.summary_date = expl.summary_date
                 AND v2.window_type = expl.window_type
                 AND v2.horizon = expl.horizon
                 AND expl.ranking_version = ?
                WHERE v2.ranking_version = ?
                ORDER BY v2.window_type, v2.horizon
                """,
                [
                    SELECTION_ENGINE_VERSION,
                    SELECTION_ENGINE_V1_VERSION,
                    EXPLANATORY_RANKING_VERSION,
                    SELECTION_ENGINE_V1_VERSION,
                    EXPLANATORY_RANKING_VERSION,
                    SELECTION_ENGINE_VERSION,
                ],
            ).fetchdf()

            artifact_root = settings.paths.artifacts_dir / "selection_engine_comparison"
            artifact_root.mkdir(parents=True, exist_ok=True)
            parquet_path = artifact_root / f"{run_context.run_id}.parquet"
            markdown_path = artifact_root / f"{run_context.run_id}.md"
            comparison_frame.to_parquet(parquet_path, index=False)
            lines = [
                "# Selection Engine Comparison",
                "",
                (
                    "| Summary Date | Window | Horizon | V2 Avg Excess | V1 Avg Excess | "
                    "Explanatory Avg Excess | V2-V1 Gap | V2-Explanatory Gap |"
                ),
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
            for row in comparison_frame.itertuples(index=False):
                lines.append(
                    "| "
                    f"{row.summary_date} | {row.window_type} | {int(row.horizon)} | "
                    f"{row.selection_v2_avg_excess or 0:.4f} | "
                    f"{row.selection_v1_avg_excess or 0:.4f} | "
                    f"{row.explanatory_v0_avg_excess or 0:.4f} | "
                    f"{row.v2_vs_v1_gap or 0:.4f} | "
                    f"{row.v2_vs_explanatory_gap or 0:.4f} |"
                )
            markdown_path.write_text("\n".join(lines), encoding="utf-8")
            artifact_paths = outcomes_result.artifact_paths + evaluation_result.artifact_paths
            artifact_paths.extend([str(parquet_path), str(markdown_path)])
            notes = (
                f"Selection engine comparison completed. comparison_rows={len(comparison_frame)}"
            )
            record_run_finish(
                connection,
                run_id=run_context.run_id,
                finished_at=now_local(settings.app.timezone),
                status="success",
                output_artifacts=artifact_paths,
                notes=notes,
                ranking_version="selection_engine_v2,selection_engine_v1,explanatory_ranking_v0",
            )
            return SelectionEngineComparisonResult(
                run_id=run_context.run_id,
                start_selection_date=start_selection_date,
                end_selection_date=end_selection_date,
                row_count=len(comparison_frame),
                artifact_paths=artifact_paths,
                notes=notes,
            )
