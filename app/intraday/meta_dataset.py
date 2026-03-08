from __future__ import annotations

# ruff: noqa: E501
import json
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.labels.forward_returns import build_forward_labels
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from .meta_common import (
    ENTER_PANEL,
    META_DATASET_COLUMNS,
    META_LABEL_THRESHOLDS,
    META_PANELS,
    WAIT_PANEL,
    IntradayMetaDatasetResult,
    IntradayMetaDatasetValidationResult,
    ordered_frame,
)
from .policy import apply_active_intraday_policy_frame


def _load_candidate_session_dates(
    connection,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str,
) -> list[date]:
    placeholders = ",".join("?" for _ in horizons)
    rows = connection.execute(
        f"""
        SELECT DISTINCT session_date
        FROM fact_intraday_candidate_session
        WHERE session_date BETWEEN ? AND ?
          AND ranking_version = ?
          AND horizon IN ({placeholders})
        ORDER BY session_date
        """,
        [start_session_date, end_session_date, ranking_version, *horizons],
    ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows]


def _load_meta_label_scope(
    connection,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str,
) -> tuple[date | None, date | None, list[str]]:
    placeholders = ",".join("?" for _ in horizons)
    scope_frame = connection.execute(
        f"""
        SELECT DISTINCT
            selection_date,
            symbol
        FROM fact_intraday_candidate_session
        WHERE session_date BETWEEN ? AND ?
          AND ranking_version = ?
          AND horizon IN ({placeholders})
        ORDER BY selection_date, symbol
        """,
        [start_session_date, end_session_date, ranking_version, *horizons],
    ).fetchdf()
    if scope_frame.empty:
        return None, None, []
    selection_dates = pd.to_datetime(scope_frame["selection_date"]).dt.date.tolist()
    symbols = scope_frame["symbol"].astype(str).str.zfill(6).drop_duplicates().tolist()
    return min(selection_dates), max(selection_dates), symbols


def ensure_intraday_meta_label_inputs(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> None:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        label_start_date, label_end_date, symbols = _load_meta_label_scope(
            connection,
            start_session_date=start_session_date,
            end_session_date=end_session_date,
            horizons=horizons,
            ranking_version=ranking_version,
        )
    if label_start_date is None or label_end_date is None or not symbols:
        return
    build_forward_labels(
        settings,
        start_date=label_start_date,
        end_date=label_end_date,
        horizons=horizons,
        symbols=symbols,
        force=False,
        dry_run=False,
    )


def normalize_outcome_status(label_available_flag: object, exclusion_reason: object) -> str:
    if pd.notna(label_available_flag) and bool(label_available_flag):
        return "matured"
    if exclusion_reason in {
        "insufficient_future_trading_days",
        "missing_entry_day_ohlcv",
        "missing_exit_day_ohlcv",
    }:
        return "pending"
    return "unavailable"


def _safe_float(value: object) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def decision_outcome_metrics(row: pd.Series) -> dict[str, float | None]:
    outcome_status = normalize_outcome_status(
        row.get("label_available_flag"),
        row.get("exclusion_reason"),
    )
    if outcome_status != "matured":
        return {
            "realized_excess_return": None,
            "timing_edge_vs_open_bps": None,
            "no_entry_realized_excess_return": None,
            "outcome_status": outcome_status,
        }
    entry_price = _safe_float(row.get("entry_reference_price")) or _safe_float(
        row.get("baseline_open_price")
    )
    exit_price = _safe_float(row.get("exit_price"))
    baseline_forward_return = _safe_float(row.get("baseline_forward_return"))
    baseline_open_return = _safe_float(row.get("baseline_open_return"))
    if entry_price in {None, 0.0} or exit_price is None:
        realized_return = 0.0
        realized_excess_return = (
            None if baseline_forward_return is None else 0.0 - baseline_forward_return
        )
        timing_edge_bps = (
            None if baseline_open_return is None else (0.0 - baseline_open_return) * 10000.0
        )
    else:
        realized_return = exit_price / entry_price - 1.0
        realized_excess_return = (
            None if baseline_forward_return is None else realized_return - baseline_forward_return
        )
        timing_edge_bps = (
            None
            if baseline_open_return is None
            else (realized_return - baseline_open_return) * 10000.0
        )
    no_entry_realized_excess_return = (
        None if baseline_forward_return is None else 0.0 - baseline_forward_return
    )
    return {
        "realized_excess_return": realized_excess_return,
        "timing_edge_vs_open_bps": timing_edge_bps,
        "no_entry_realized_excess_return": no_entry_realized_excess_return,
        "outcome_status": "matured",
    }


def later_enter_metrics(group: pd.DataFrame, row_index: int) -> tuple[float | None, float | None]:
    later = group.iloc[row_index + 1 :].copy()
    later = later.loc[later["tuned_action"] == "ENTER_NOW"]
    if later.empty:
        return None, None
    target = later.iloc[0]
    metrics = decision_outcome_metrics(target)
    return (
        metrics["realized_excess_return"],
        metrics["timing_edge_vs_open_bps"],
    )


def derive_target_class(
    *,
    panel_name: str,
    current_excess: float | None,
    later_enter_excess: float | None,
    no_entry_excess: float | None,
) -> tuple[str | None, str]:
    enter_vs_wait_delta = META_LABEL_THRESHOLDS["enter_vs_wait_delta_bps"]
    wait_vs_enter_delta = META_LABEL_THRESHOLDS["wait_vs_enter_delta_bps"]
    avoid_vs_enter_delta = META_LABEL_THRESHOLDS["avoid_vs_enter_delta_bps"]
    avoid_vs_wait_delta = META_LABEL_THRESHOLDS["avoid_vs_wait_delta_bps"]
    min_effective_outcome = META_LABEL_THRESHOLDS["min_effective_trade_outcome_bps"]

    if panel_name == ENTER_PANEL:
        if current_excess is None:
            return None, "current_excess_missing"
        if (
            no_entry_excess is not None
            and (no_entry_excess - current_excess) * 10000.0 >= avoid_vs_enter_delta
            and current_excess * 10000.0 <= -min_effective_outcome
        ):
            return "DOWNGRADE_AVOID", "no_entry_beats_enter"
        if (
            later_enter_excess is not None
            and (later_enter_excess - current_excess) * 10000.0 >= enter_vs_wait_delta
        ):
            return "DOWNGRADE_WAIT", "later_enter_beats_enter"
        return "KEEP_ENTER", "enter_kept"

    if panel_name == WAIT_PANEL:
        wait_reference_candidates = [
            value for value in [later_enter_excess, no_entry_excess] if value is not None
        ]
        wait_reference = max(wait_reference_candidates) if wait_reference_candidates else None
        if (
            current_excess is not None
            and wait_reference is not None
            and (current_excess - wait_reference) * 10000.0 >= wait_vs_enter_delta
            and current_excess * 10000.0 >= min_effective_outcome
        ):
            return "UPGRADE_ENTER", "enter_now_beats_wait_path"
        if (
            no_entry_excess is not None
            and wait_reference is not None
            and (no_entry_excess - wait_reference) * 10000.0 >= avoid_vs_wait_delta
        ):
            return "DOWNGRADE_AVOID", "no_entry_beats_wait_path"
        return "KEEP_WAIT", "wait_kept"

    return None, "unsupported_panel"


def assemble_intraday_meta_dataset_frame(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
    connection=None,
) -> pd.DataFrame:
    owns_connection = connection is None
    if owns_connection:
        connection_context = duckdb_connection(settings.paths.duckdb_path, read_only=True)
        connection = connection_context.__enter__()
    try:
        bootstrap_core_tables(connection)
        session_dates = _load_candidate_session_dates(
            connection,
            start_session_date=start_session_date,
            end_session_date=end_session_date,
            horizons=horizons,
            ranking_version=ranking_version,
        )

        rows: list[dict[str, object]] = []
        for session_date in session_dates:
            tuned_frame = apply_active_intraday_policy_frame(
                settings,
                session_date=session_date,
                horizons=horizons,
                connection=connection,
            )
            if tuned_frame.empty:
                continue
            tuned_frame = tuned_frame.sort_values(["symbol", "horizon", "checkpoint_time"]).reset_index(
                drop=True
            )
            for (_, _, horizon), group in tuned_frame.groupby(
                ["symbol", "session_date", "horizon"], sort=False
            ):
                ordered = group.sort_values("checkpoint_time").reset_index(drop=True)
                for row_index, row in ordered.iterrows():
                    panel_name = None
                    if row.get("tuned_action") == "ENTER_NOW":
                        panel_name = ENTER_PANEL
                    elif row.get("tuned_action") == "WAIT_RECHECK":
                        panel_name = WAIT_PANEL
                    if panel_name is None:
                        continue
                    current_metrics = decision_outcome_metrics(row)
                    if current_metrics["outcome_status"] != "matured":
                        continue
                    later_excess, later_edge = later_enter_metrics(ordered, row_index)
                    target_class, target_reason = derive_target_class(
                        panel_name=panel_name,
                        current_excess=current_metrics["realized_excess_return"],
                        later_enter_excess=later_excess,
                        no_entry_excess=current_metrics["no_entry_realized_excess_return"],
                    )
                    if target_class is None:
                        continue
                    record = {
                        "session_date": session_date,
                        "selection_date": row.get("selection_date"),
                        "symbol": row.get("symbol"),
                        "market": row.get("market"),
                        "horizon": int(horizon),
                        "checkpoint_time": row.get("checkpoint_time"),
                        "panel_name": panel_name,
                        "active_policy_action": row.get("tuned_action"),
                        "raw_action": row.get("raw_action"),
                        "adjusted_action": row.get("adjusted_action"),
                        "tuned_action": row.get("tuned_action"),
                        "target_class": target_class,
                        "target_reason": target_reason,
                        "label_available_flag": row.get("label_available_flag"),
                        "current_realized_excess_return": current_metrics["realized_excess_return"],
                        "current_timing_edge_vs_open_bps": current_metrics["timing_edge_vs_open_bps"],
                        "later_enter_realized_excess_return": later_excess,
                        "later_enter_timing_edge_vs_open_bps": later_edge,
                        "no_entry_realized_excess_return": current_metrics[
                            "no_entry_realized_excess_return"
                        ],
                        "active_policy_candidate_id": row.get("active_policy_candidate_id"),
                        "active_policy_template_id": row.get("active_policy_template_id"),
                        "active_policy_scope_type": row.get("active_policy_scope_type"),
                        "active_policy_scope_key": row.get("active_policy_scope_key"),
                        "policy_trace": row.get("policy_trace"),
                        "fallback_used_flag": row.get("fallback_used_flag"),
                        "market_regime_family": row.get("market_regime_family"),
                        "adjustment_profile": row.get("adjustment_profile"),
                        "signal_quality_flag": row.get("signal_quality_flag"),
                        "selection_confidence_bucket": row.get("selection_confidence_bucket"),
                        "trade_summary_status": row.get("trade_summary_status"),
                        "quote_status": row.get("quote_status"),
                        "data_quality_flag": row.get("data_quality_flag"),
                    }
                    for column in META_DATASET_COLUMNS:
                        if column not in record:
                            record[column] = row.get(column)
                    rows.append(record)

        output = pd.DataFrame(rows)
        if output.empty:
            return output
        return ordered_frame(output, META_DATASET_COLUMNS)
    finally:
        if owns_connection:
            connection_context.__exit__(None, None, None)


def build_intraday_meta_training_dataset(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayMetaDatasetResult:
    ensure_storage_layout(settings)
    ensure_intraday_meta_label_inputs(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
        ranking_version=ranking_version,
    )
    with activate_run_context(
        "build_intraday_meta_training_dataset",
        as_of_date=end_session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=[
                    "fact_intraday_adjusted_entry_decision",
                    "fact_intraday_market_context_snapshot",
                    "fact_forward_return_label",
                    "fact_intraday_active_policy",
                ],
                notes=(
                    "Build intraday meta-model training dataset. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                ),
                ranking_version=ranking_version,
            )
            try:
                output = assemble_intraday_meta_dataset_frame(
                    settings,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    ranking_version=ranking_version,
                    connection=connection,
                )
                artifact_paths: list[str] = []
                if not output.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                output,
                                base_dir=settings.paths.curated_dir,
                                dataset="intraday/meta_training_dataset",
                                partitions={"end_session_date": end_session_date.isoformat()},
                                filename="meta_training_dataset.parquet",
                            )
                        )
                    )
                notes = (
                    "Intraday meta training dataset built. "
                    f"rows={len(output)} horizons={','.join(str(value) for value in horizons)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=ranking_version,
                )
                return IntradayMetaDatasetResult(
                    run_id=run_context.run_id,
                    row_count=len(output),
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
                    notes="Intraday meta training dataset build failed.",
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise


def validate_intraday_meta_dataset(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayMetaDatasetValidationResult:
    ensure_intraday_meta_label_inputs(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
        ranking_version=ranking_version,
    )
    output = assemble_intraday_meta_dataset_frame(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
        ranking_version=ranking_version,
    )
    checks: list[dict[str, object]] = []
    panel_counts = output["panel_name"].value_counts(dropna=False).to_dict() if not output.empty else {}
    checks.append(
        {
            "check_name": "dataset_non_empty",
            "status": "pass" if not output.empty else "warn",
            "value": int(len(output)),
            "detail": "Dataset should contain matured ENTER/WAIT panel rows.",
        }
    )
    for panel_name in META_PANELS:
        class_count = (
            output.loc[output["panel_name"] == panel_name, "target_class"]
            .nunique(dropna=True)
            if not output.empty
            else 0
        )
        checks.append(
            {
                "check_name": f"{panel_name.lower()}_class_count",
                "status": "pass" if int(class_count) >= 1 else "warn",
                "value": int(class_count),
                "detail": "Panel should expose at least one target class.",
            }
        )
    markdown_lines = [
        "# Intraday Meta Dataset Validation",
        "",
        f"- rows: {len(output)}",
        f"- panel_counts: {json.dumps(panel_counts, ensure_ascii=False, sort_keys=True)}",
        "",
        "| Check | Status | Value | Detail |",
        "| --- | --- | ---: | --- |",
    ]
    for check in checks:
        markdown_lines.append(
            f"| {check['check_name']} | {check['status']} | {check['value']} | {check['detail']} |"
        )
    artifact_dir = settings.paths.artifacts_dir / "intraday_meta_dataset_validation"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    markdown_path = artifact_dir / f"{end_session_date.isoformat()}-meta-dataset-validation.md"
    markdown_path.write_text("\n".join(markdown_lines), encoding="utf-8")
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with activate_run_context(
            "validate_intraday_meta_dataset",
            as_of_date=end_session_date,
        ) as run_context:
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=["fact_intraday_adjusted_entry_decision"],
                notes="Validate intraday meta training dataset.",
                ranking_version=ranking_version,
            )
            notes = (
                "Intraday meta dataset validation completed. "
                f"warnings={sum(check['status'] == 'warn' for check in checks)}"
            )
            record_run_finish(
                connection,
                run_id=run_context.run_id,
                finished_at=now_local(settings.app.timezone),
                status="success",
                output_artifacts=[str(markdown_path)],
                notes=notes,
                ranking_version=ranking_version,
            )
            return IntradayMetaDatasetValidationResult(
                run_id=run_context.run_id,
                row_count=len(checks),
                artifact_paths=[str(markdown_path)],
                notes=notes,
            )
