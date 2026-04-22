from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import (
    D5_PRIMARY_BUCKET_SEGMENTS,
    D5_PRIMARY_COMPARATOR_PAIRS,
    D5_PRIMARY_DRAG_BASELINE_BY_WINDOW,
    D5_PRIMARY_DRAG_IMPROVEMENT_TARGET,
    D5_PRIMARY_FOCUS_MODEL_SPEC_ID,
    MODEL_SPEC_ID,
)
from app.ml.promotion import load_alpha_promotion_summary
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet


@dataclass(slots=True)
class AlphaShadowComparisonReportResult:
    run_id: str
    start_selection_date: date
    end_selection_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def _frame(connection, query: str, params: list[object] | None = None) -> pd.DataFrame:
    return connection.execute(query, params or []).fetchdf()


def _write_report_artifacts(
    settings: Settings,
    *,
    run_id: str,
    start_selection_date: date,
    end_selection_date: date,
    content: str,
    payload: dict[str, Any],
) -> list[str]:
    artifact_dir = (
        settings.paths.artifacts_dir
        / "alpha_shadow_report"
        / f"start_selection_date={start_selection_date.isoformat()}"
        / f"end_selection_date={end_selection_date.isoformat()}"
        / run_id
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    preview_path = artifact_dir / "alpha_shadow_report_preview.md"
    preview_path.write_text(content, encoding="utf-8")
    payload_path = artifact_dir / "alpha_shadow_report_payload.json"
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return [str(preview_path), str(payload_path)]


def _load_shadow_summary(
    connection,
    *,
    end_selection_date: date,
    horizons: list[int],
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return _frame(
        connection,
        f"""
        WITH latest_summary_dates AS (
            SELECT
                horizon,
                model_spec_id,
                MAX(summary_date) AS summary_date
            FROM fact_alpha_shadow_evaluation_summary
            WHERE summary_date <= ?
              AND horizon IN ({placeholders})
            GROUP BY horizon, model_spec_id
        )
        SELECT
            summary.summary_date,
            summary.window_type,
            summary.horizon,
            summary.model_spec_id,
            summary.segment_value,
            summary.count_evaluated,
            COALESCE(summary.matured_selection_date_count, 0) AS matured_selection_date_count,
            summary.mean_realized_excess_return,
            summary.mean_point_loss,
            summary.rank_ic
        FROM fact_alpha_shadow_evaluation_summary AS summary
        JOIN latest_summary_dates AS latest
          ON summary.horizon = latest.horizon
         AND summary.model_spec_id = latest.model_spec_id
         AND summary.summary_date = latest.summary_date
        WHERE 1 = 1
          AND segment_value IN ('all', 'top5', 'top10', 'report_candidates', 'bucket_continuation', 'bucket_reversal_recovery', 'bucket_crowded_risk')
        ORDER BY summary.horizon, summary.window_type, summary.segment_value, summary.model_spec_id
        """,
        [end_selection_date, *horizons],
    )


def _load_selection_gap_summary(
    connection,
    *,
    end_selection_date: date,
    model_spec_ids: list[str],
) -> pd.DataFrame:
    if not model_spec_ids:
        return pd.DataFrame()
    model_placeholders = ",".join("?" for _ in model_spec_ids)
    return _frame(
        connection,
        f"""
        WITH latest_gap_dates AS (
            SELECT
                window_name,
                horizon,
                model_spec_id,
                MAX(summary_date) AS summary_date
            FROM fact_alpha_shadow_selection_gap_scorecard
            WHERE summary_date <= ?
              AND segment_name = 'top5'
              AND horizon = 5
              AND window_name IN ('cohort', 'rolling_20')
              AND model_spec_id IN ({model_placeholders})
            GROUP BY window_name, horizon, model_spec_id
        )
        SELECT
            gap.summary_date,
            gap.window_name,
            gap.horizon,
            gap.model_spec_id,
            COALESCE(gap.matured_selection_date_count, 0) AS matured_selection_date_count,
            gap.raw_top5_mean_realized_excess_return,
            gap.selected_top5_mean_realized_excess_return,
            gap.top5_overlap,
            gap.pred_only_top5_mean_realized_excess_return,
            gap.sel_only_top5_mean_realized_excess_return,
            gap.drag_vs_raw_top5
        FROM fact_alpha_shadow_selection_gap_scorecard AS gap
        JOIN latest_gap_dates AS latest
          ON gap.summary_date = latest.summary_date
         AND gap.window_name = latest.window_name
         AND gap.horizon = latest.horizon
         AND gap.model_spec_id = latest.model_spec_id
        ORDER BY gap.window_name, gap.model_spec_id
        """,
        [end_selection_date, *model_spec_ids],
    )


def _build_comparison_ledger(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame()
    baseline = summary.loc[summary["model_spec_id"] == MODEL_SPEC_ID].copy()
    challengers = summary.loc[summary["model_spec_id"] != MODEL_SPEC_ID].copy()
    if challengers.empty or baseline.empty:
        return pd.DataFrame()
    merged = challengers.merge(
        baseline,
        on=["summary_date", "window_type", "horizon", "segment_value"],
        how="left",
        suffixes=("", "_baseline"),
    )
    merged["return_gap_vs_baseline"] = (
        pd.to_numeric(merged["mean_realized_excess_return"], errors="coerce")
        - pd.to_numeric(merged["mean_realized_excess_return_baseline"], errors="coerce")
    )
    merged["rank_ic_gap_vs_baseline"] = (
        pd.to_numeric(merged["rank_ic"], errors="coerce")
        - pd.to_numeric(merged["rank_ic_baseline"], errors="coerce")
    )
    merged["point_loss_improvement_vs_baseline"] = (
        pd.to_numeric(merged["mean_point_loss_baseline"], errors="coerce")
        - pd.to_numeric(merged["mean_point_loss"], errors="coerce")
    )
    return merged[
        [
            "summary_date",
            "window_type",
            "horizon",
            "segment_value",
            "model_spec_id",
            "matured_selection_date_count",
            "count_evaluated",
            "mean_realized_excess_return",
            "rank_ic",
            "mean_point_loss",
            "mean_realized_excess_return_baseline",
            "rank_ic_baseline",
            "mean_point_loss_baseline",
            "return_gap_vs_baseline",
            "rank_ic_gap_vs_baseline",
            "point_loss_improvement_vs_baseline",
        ]
    ].sort_values(["horizon", "window_type", "segment_value", "model_spec_id"]).reset_index(drop=True)


def _build_pairwise_ledger(
    summary: pd.DataFrame,
    *,
    focus_model_spec_id: str,
    comparator_pairs: tuple[tuple[int, str], ...],
) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame()
    focus_rows = summary.loc[summary["model_spec_id"] == focus_model_spec_id].copy()
    if focus_rows.empty:
        return pd.DataFrame()
    ledgers: list[pd.DataFrame] = []
    for horizon, comparator_model_spec_id in comparator_pairs:
        comparator_rows = summary.loc[
            (summary["horizon"] == int(horizon))
            & (summary["model_spec_id"] == comparator_model_spec_id)
        ].copy()
        if comparator_rows.empty:
            continue
        merged = focus_rows.loc[focus_rows["horizon"] == int(horizon)].merge(
            comparator_rows,
            on=["summary_date", "window_type", "horizon", "segment_value"],
            how="inner",
            suffixes=("_focus", "_comparator"),
        )
        if merged.empty:
            continue
        merged["focus_model_spec_id"] = focus_model_spec_id
        merged["comparator_model_spec_id"] = comparator_model_spec_id
        merged["return_gap_vs_comparator"] = (
            pd.to_numeric(merged["mean_realized_excess_return_focus"], errors="coerce")
            - pd.to_numeric(merged["mean_realized_excess_return_comparator"], errors="coerce")
        )
        merged["rank_ic_gap_vs_comparator"] = (
            pd.to_numeric(merged["rank_ic_focus"], errors="coerce")
            - pd.to_numeric(merged["rank_ic_comparator"], errors="coerce")
        )
        merged["point_loss_improvement_vs_comparator"] = (
            pd.to_numeric(merged["mean_point_loss_comparator"], errors="coerce")
            - pd.to_numeric(merged["mean_point_loss_focus"], errors="coerce")
        )
        ledgers.append(
            merged[
                [
                    "summary_date",
                    "window_type",
                    "horizon",
                    "segment_value",
                    "focus_model_spec_id",
                    "comparator_model_spec_id",
                    "count_evaluated_focus",
                    "matured_selection_date_count_focus",
                    "mean_realized_excess_return_focus",
                    "rank_ic_focus",
                    "mean_point_loss_focus",
                    "count_evaluated_comparator",
                    "matured_selection_date_count_comparator",
                    "mean_realized_excess_return_comparator",
                    "rank_ic_comparator",
                    "mean_point_loss_comparator",
                    "return_gap_vs_comparator",
                    "rank_ic_gap_vs_comparator",
                    "point_loss_improvement_vs_comparator",
                ]
            ]
        )
    if not ledgers:
        return pd.DataFrame()
    return pd.concat(ledgers, ignore_index=True).sort_values(
        ["horizon", "window_type", "segment_value", "comparator_model_spec_id"]
    ).reset_index(drop=True)


def _format_metric(value: object, *, pct: bool = False, signed: bool = False) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    numeric = float(value)
    if pct:
        return format(numeric, "+.2%" if signed else ".2%")
    return format(numeric, "+.3f" if signed else ".3f")


def _humanize_segment(segment_value: str) -> str:
    mapping = {
        "bucket_continuation": "Continuation",
        "bucket_reversal_recovery": "Reversal / recovery",
        "bucket_crowded_risk": "Crowded / high-risk",
    }
    return mapping.get(segment_value, segment_value)


def _build_markdown(
    ledger: pd.DataFrame,
    *,
    start_selection_date: date,
    end_selection_date: date,
    promotion_summary: pd.DataFrame,
) -> str:
    lines = [
        "# Alpha Shadow Comparison Report",
        "",
        f"- Selection range: `{start_selection_date.isoformat()}..{end_selection_date.isoformat()}`",
        f"- Baseline model: `{MODEL_SPEC_ID}`",
        "",
        "## Challenger vs baseline gaps",
        "",
    ]
    if ledger.empty:
        lines.append("- 비교 가능한 challenger shadow summary가 아직 없습니다.")
    else:
        for (horizon, window_type, segment_value), group in ledger.groupby(
            ["horizon", "window_type", "segment_value"], sort=True
        ):
            lines.append(f"### H{int(horizon)} | {window_type} | {segment_value}")
            for row in group.itertuples(index=False):
                lines.append(
                    "- {model}: excess {ret} (gap {ret_gap}), rank IC {rank_ic} (gap {rank_gap}), point loss {loss} (improvement {loss_gap})".format(
                        model=row.model_spec_id,
                        ret=_format_metric(row.mean_realized_excess_return, pct=True, signed=True),
                        ret_gap=_format_metric(row.return_gap_vs_baseline, pct=True, signed=True),
                        rank_ic=_format_metric(row.rank_ic, signed=True),
                        rank_gap=_format_metric(row.rank_ic_gap_vs_baseline, signed=True),
                        loss=_format_metric(row.mean_point_loss),
                        loss_gap=_format_metric(row.point_loss_improvement_vs_baseline, signed=True),
                    )
                )
            lines.append("")
    lines.append("## Latest promotion summary")
    if promotion_summary.empty:
        lines.append("- promotion summary 없음")
    else:
        for row in promotion_summary.itertuples(index=False):
            lines.append(
                f"- H{int(row.horizon)} | {row.decision_label} | active={row.active_model_label} | compare={row.comparison_model_label} | gap={_format_metric(row.promotion_gap, pct=True, signed=True)}"
            )
    return "\n".join(lines).strip()


def _build_d5_primary_markdown(
    pairwise_ledger: pd.DataFrame,
    *,
    summary: pd.DataFrame,
    drag_summary: pd.DataFrame,
    start_selection_date: date,
    end_selection_date: date,
    promotion_summary: pd.DataFrame,
) -> str:
    lines = [
        "# Alpha Shadow Comparison Report",
        "",
        (
            "- Selection range: "
            f"`{start_selection_date.isoformat()}..{end_selection_date.isoformat()}`"
        ),
        f"- Focus model: `{D5_PRIMARY_FOCUS_MODEL_SPEC_ID}`",
        "- Primary D+5 comparator: `alpha_swing_d5_v1`",
        f"- Secondary D+5 comparator: `{MODEL_SPEC_ID}` (H5)",
        f"- D+1 auxiliary comparators: `{MODEL_SPEC_ID}`, `alpha_topbucket_h1_rolling_120_v1`",
        "",
        "## Lag-first proof",
        "",
    ]
    if drag_summary.empty:
        lines.append("- D+5 selection-gap rows not available yet.")
    else:
        for row in drag_summary.itertuples(index=False):
            lines.append(
                (
                    "- {window} | {model}: `top5_overlap` {overlap} | "
                    "`pred_only_top5_mean_realized_excess_return` {pred_only} | "
                    "`sel_only_top5_mean_realized_excess_return` {sel_only} | "
                    "matured_dates={dates}"
                ).format(
                    window=row.window_name,
                    model=row.model_spec_id,
                    overlap=_format_metric(row.top5_overlap, pct=True),
                    pred_only=_format_metric(
                        row.pred_only_top5_mean_realized_excess_return,
                        pct=True,
                        signed=True,
                    ),
                    sel_only=_format_metric(
                        row.sel_only_top5_mean_realized_excess_return,
                        pct=True,
                        signed=True,
                    ),
                    dates=int(row.matured_selection_date_count or 0),
                )
            )
    lines.extend(["", "## D+5 raw-vs-selected drag", ""])
    if drag_summary.empty:
        lines.append("- D+5 selection-gap rows not available yet.")
    else:
        for row in drag_summary.itertuples(index=False):
            baseline_drag = D5_PRIMARY_DRAG_BASELINE_BY_WINDOW.get(str(row.window_name))
            target_drag = (
                None
                if row.model_spec_id != D5_PRIMARY_FOCUS_MODEL_SPEC_ID or baseline_drag is None
                else baseline_drag + D5_PRIMARY_DRAG_IMPROVEMENT_TARGET
            )
            target_suffix = (
                ""
                if target_drag is None
                else " | frozen baseline {baseline} | lane target >= {target}".format(
                    baseline=_format_metric(baseline_drag, pct=True, signed=True),
                    target=_format_metric(target_drag, pct=True, signed=True),
                )
            )
            lines.append(
                (
                    "- {window} | {model}: raw {raw} | selected {selected} | "
                    "`drag_vs_raw_top5` {drag} | matured_dates={dates}{target_suffix}"
                ).format(
                    window=row.window_name,
                    model=row.model_spec_id,
                    raw=_format_metric(
                        row.raw_top5_mean_realized_excess_return,
                        pct=True,
                        signed=True,
                    ),
                    selected=_format_metric(
                        row.selected_top5_mean_realized_excess_return,
                        pct=True,
                        signed=True,
                    ),
                    drag=_format_metric(row.drag_vs_raw_top5, pct=True, signed=True),
                    dates=int(row.matured_selection_date_count or 0),
                    target_suffix=target_suffix,
                )
            )
    lines.extend(
        [
            "",
            "- `report_candidates` remains a compatibility/reporting surface only for this lane and is excluded from primary drag success criteria.",
            "",
            "## D+5 overall comparator table",
            "",
        ]
    )
    overall_rows = pairwise_ledger.loc[
        (pairwise_ledger["horizon"] == 5)
        & (pairwise_ledger["segment_value"] == "top5")
        & (
            pairwise_ledger["comparator_model_spec_id"].isin(
                ["alpha_swing_d5_v1", MODEL_SPEC_ID]
            )
        )
        & (pairwise_ledger["window_type"].isin(["cohort", "rolling_20"]))
    ].copy()
    if overall_rows.empty:
        lines.append("- D+5 comparator rows not available yet.")
    else:
        for row in overall_rows.itertuples(index=False):
            lines.append(
                "- {window} vs {comparator}: focus {focus_ret} | comparator {comp_ret} | gap {gap} | matured_dates={dates}".format(
                    window=row.window_type,
                    comparator=row.comparator_model_spec_id,
                    focus_ret=_format_metric(
                        row.mean_realized_excess_return_focus,
                        pct=True,
                        signed=True,
                    ),
                    comp_ret=_format_metric(
                        row.mean_realized_excess_return_comparator,
                        pct=True,
                        signed=True,
                    ),
                    gap=_format_metric(row.return_gap_vs_comparator, pct=True, signed=True),
                    dates=int(row.matured_selection_date_count_focus or 0),
                )
            )
    lines.extend(["", "## D+5 robustness buckets vs alpha_swing_d5_v1", ""])
    bucket_rows = pairwise_ledger.loc[
        (pairwise_ledger["horizon"] == 5)
        & (pairwise_ledger["window_type"] == "cohort")
        & (pairwise_ledger["segment_value"].isin(D5_PRIMARY_BUCKET_SEGMENTS))
        & (pairwise_ledger["comparator_model_spec_id"] == "alpha_swing_d5_v1")
    ].copy()
    if bucket_rows.empty:
        lines.append("- Bucket rows not available yet.")
    else:
        for row in bucket_rows.itertuples(index=False):
            lines.append(
                "- {bucket}: focus {focus_ret} | comparator {comp_ret} | gap {gap} | matured_dates={dates}".format(
                    bucket=_humanize_segment(str(row.segment_value)),
                    focus_ret=_format_metric(
                        row.mean_realized_excess_return_focus,
                        pct=True,
                        signed=True,
                    ),
                    comp_ret=_format_metric(
                        row.mean_realized_excess_return_comparator,
                        pct=True,
                        signed=True,
                    ),
                    gap=_format_metric(row.return_gap_vs_comparator, pct=True, signed=True),
                    dates=int(row.matured_selection_date_count_focus or 0),
                )
            )
    lines.extend(["", "## D+1 auxiliary interpretation", ""])
    d1_rows = summary.loc[
        (summary["horizon"] == 1)
        & (summary["segment_value"] == "top5")
        & (summary["window_type"].isin(["cohort", "rolling_20"]))
        & (summary["model_spec_id"].isin([MODEL_SPEC_ID, "alpha_topbucket_h1_rolling_120_v1"]))
    ].copy()
    if d1_rows.empty:
        lines.append("- D+1 auxiliary comparator rows not available yet.")
    else:
        for row in d1_rows.sort_values(["window_type", "model_spec_id"]).itertuples(index=False):
            lines.append(
                "- {window} | {model}: excess {ret} | rank IC {rank_ic} | matured_dates={dates} | auxiliary precursor reference only".format(
                    window=row.window_type,
                    model=row.model_spec_id,
                    ret=_format_metric(row.mean_realized_excess_return, pct=True, signed=True),
                    rank_ic=_format_metric(row.rank_ic, signed=True),
                    dates=int(row.matured_selection_date_count or 0),
                )
            )
    lines.append("")
    lines.append("## Latest promotion summary")
    if promotion_summary.empty:
        lines.append("- promotion summary 없음")
    else:
        for row in promotion_summary.itertuples(index=False):
            lines.append(
                f"- H{int(row.horizon)} | {row.decision_label} | active={row.active_model_label} | compare={row.comparison_model_label} | gap={_format_metric(row.promotion_gap, pct=True, signed=True)}"
            )
    return "\n".join(lines).strip()


def render_alpha_shadow_comparison_report(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
) -> AlphaShadowComparisonReportResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "render_alpha_shadow_comparison_report",
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
                    "fact_alpha_shadow_evaluation_summary",
                    "fact_alpha_promotion_test",
                ],
                notes=(
                    "Render alpha shadow comparison report. "
                    f"range={start_selection_date.isoformat()}..{end_selection_date.isoformat()}"
                ),
            )
            try:
                summary = _load_shadow_summary(
                    connection,
                    end_selection_date=end_selection_date,
                    horizons=horizons,
                )
                model_spec_ids = set(summary.get("model_spec_id", pd.Series(dtype="object")).astype(str))
                d5_focus_enabled = D5_PRIMARY_FOCUS_MODEL_SPEC_ID in model_spec_ids
                drag_summary = (
                    _load_selection_gap_summary(
                        connection,
                        end_selection_date=end_selection_date,
                        model_spec_ids=[
                            D5_PRIMARY_FOCUS_MODEL_SPEC_ID,
                            "alpha_swing_d5_v1",
                            MODEL_SPEC_ID,
                        ],
                    )
                    if d5_focus_enabled
                    else pd.DataFrame()
                )
                ledger = (
                    _build_pairwise_ledger(
                        summary,
                        focus_model_spec_id=D5_PRIMARY_FOCUS_MODEL_SPEC_ID,
                        comparator_pairs=D5_PRIMARY_COMPARATOR_PAIRS,
                    )
                    if d5_focus_enabled
                    else _build_comparison_ledger(summary)
                )
                promotion_summary = load_alpha_promotion_summary(
                    connection,
                    as_of_date=end_selection_date,
                )
                artifact_paths: list[str] = []
                if not ledger.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                ledger,
                                base_dir=settings.paths.curated_dir,
                                dataset="alpha_shadow/comparison_ledger",
                                partitions={
                                    "summary_date": end_selection_date.isoformat(),
                                },
                                filename="alpha_shadow_comparison_ledger.parquet",
                            )
                        )
                    )
                content = (
                    _build_d5_primary_markdown(
                        ledger,
                        summary=summary,
                        drag_summary=drag_summary,
                        start_selection_date=start_selection_date,
                        end_selection_date=end_selection_date,
                        promotion_summary=promotion_summary,
                    )
                    if d5_focus_enabled
                    else _build_markdown(
                        ledger,
                        start_selection_date=start_selection_date,
                        end_selection_date=end_selection_date,
                        promotion_summary=promotion_summary,
                    )
                )
                payload = {
                    "report_type": "alpha_shadow_comparison_report",
                    "row_count": int(len(ledger)),
                    "baseline_model_spec_id": MODEL_SPEC_ID,
                    "focus_model_spec_id": D5_PRIMARY_FOCUS_MODEL_SPEC_ID if d5_focus_enabled else None,
                    "d5_drag_row_count": int(len(drag_summary)) if d5_focus_enabled else None,
                }
                artifact_paths.extend(
                    _write_report_artifacts(
                        settings,
                        run_id=run_context.run_id,
                        start_selection_date=start_selection_date,
                        end_selection_date=end_selection_date,
                        content=content,
                        payload=payload,
                    )
                )
                notes = (
                    "Alpha shadow comparison report rendered. "
                    f"summary_date={end_selection_date.isoformat()} rows={len(ledger)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return AlphaShadowComparisonReportResult(
                    run_id=run_context.run_id,
                    start_selection_date=start_selection_date,
                    end_selection_date=end_selection_date,
                    row_count=len(ledger),
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
                    notes="Alpha shadow comparison report failed.",
                    error_message=str(exc),
                )
                raise
