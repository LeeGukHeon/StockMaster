# ruff: noqa: E501

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import duckdb

from app.common.run_context import activate_run_context
from app.common.time import now_local, today_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection, duckdb_snapshot_connection
from app.storage.manifests import record_run_finish, record_run_start

INTRADAY_RESEARCH_ROLLOUT = "RESEARCH_NON_TRADING"


@dataclass(frozen=True, slots=True)
class IntradayResearchCapabilitySpec:
    feature_slug: str
    enabled_flag_name: str
    dependency_queries: tuple[tuple[str, str], ...]
    report_types: tuple[str, ...] = ()
    job_names: tuple[str, ...] = ()


@dataclass(slots=True)
class IntradayResearchCapabilityResult:
    as_of_date: date
    run_id: str
    row_count: int
    notes: str


@dataclass(slots=True)
class IntradayResearchModeValidationResult:
    run_id: str
    as_of_date: date
    check_count: int
    warning_count: int
    notes: str


CAPABILITY_SPECS: tuple[IntradayResearchCapabilitySpec, ...] = (
    IntradayResearchCapabilitySpec(
        feature_slug="intraday_assist",
        enabled_flag_name="assist_enabled",
        dependency_queries=(
            ("fact_intraday_candidate_session", "SELECT COUNT(*) FROM fact_intraday_candidate_session WHERE session_date <= ?"),
            ("fact_intraday_entry_decision", "SELECT COUNT(*) FROM fact_intraday_entry_decision WHERE session_date <= ?"),
            ("fact_intraday_adjusted_entry_decision", "SELECT COUNT(*) FROM fact_intraday_adjusted_entry_decision WHERE session_date <= ?"),
            ("fact_intraday_meta_decision", "SELECT COUNT(*) FROM fact_intraday_meta_decision WHERE session_date <= ?"),
        ),
        report_types=("intraday_summary_report",),
        job_names=("run_intraday_assist_bundle",),
    ),
    IntradayResearchCapabilitySpec(
        feature_slug="intraday_policy_adjustment",
        enabled_flag_name="policy_adjustment_enabled",
        dependency_queries=(
            ("fact_intraday_regime_adjustment", "SELECT COUNT(*) FROM fact_intraday_regime_adjustment WHERE session_date <= ?"),
            ("fact_intraday_adjusted_entry_decision", "SELECT COUNT(*) FROM fact_intraday_adjusted_entry_decision WHERE session_date <= ?"),
        ),
        report_types=("intraday_policy_research_report",),
        job_names=("run_weekly_calibration_bundle",),
    ),
    IntradayResearchCapabilitySpec(
        feature_slug="intraday_meta_model",
        enabled_flag_name="meta_model_enabled",
        dependency_queries=(
            ("fact_intraday_meta_prediction", "SELECT COUNT(*) FROM fact_intraday_meta_prediction WHERE session_date <= ?"),
            ("fact_intraday_meta_decision", "SELECT COUNT(*) FROM fact_intraday_meta_decision WHERE session_date <= ?"),
            ("fact_intraday_active_meta_model", "SELECT COUNT(*) FROM fact_intraday_active_meta_model WHERE effective_from_date <= ?"),
        ),
        report_types=("intraday_meta_model_report",),
        job_names=("run_weekly_training_bundle",),
    ),
    IntradayResearchCapabilitySpec(
        feature_slug="intraday_postmortem",
        enabled_flag_name="postmortem_enabled",
        dependency_queries=(
            ("fact_intraday_strategy_comparison", "SELECT COUNT(*) FROM fact_intraday_strategy_comparison WHERE end_session_date <= ?"),
            ("fact_intraday_timing_calibration", "SELECT COUNT(*) FROM fact_intraday_timing_calibration WHERE window_end_date <= ?"),
        ),
        report_types=("intraday_postmortem_report",),
        job_names=("run_evaluation_bundle",),
    ),
    IntradayResearchCapabilitySpec(
        feature_slug="intraday_research_reports",
        enabled_flag_name="research_reports_enabled",
        dependency_queries=(
            ("fact_latest_report_index", "SELECT COUNT(*) FROM fact_latest_report_index WHERE generated_ts::DATE <= ?"),
        ),
        report_types=(
            "intraday_summary_report",
            "intraday_postmortem_report",
            "intraday_policy_research_report",
            "intraday_meta_model_report",
        ),
        job_names=(
            "run_intraday_assist_bundle",
            "run_evaluation_bundle",
            "run_weekly_training_bundle",
            "run_weekly_calibration_bundle",
        ),
    ),
    IntradayResearchCapabilitySpec(
        feature_slug="intraday_discord_summary",
        enabled_flag_name="discord_summary_enabled",
        dependency_queries=(
            ("fact_job_run", "SELECT COUNT(*) FROM fact_job_run WHERE job_name LIKE 'run_%bundle' AND started_at::DATE <= ?"),
        ),
        report_types=(
            "intraday_summary_report",
            "intraday_postmortem_report",
            "intraday_policy_research_report",
            "intraday_meta_model_report",
        ),
        job_names=(
            "publish_discord_intraday_postmortem",
            "publish_discord_intraday_policy_summary",
            "publish_discord_intraday_meta_summary",
        ),
    ),
    IntradayResearchCapabilitySpec(
        feature_slug="intraday_writeback",
        enabled_flag_name="writeback_enabled",
        dependency_queries=(
            ("fact_intraday_entry_decision", "SELECT COUNT(*) FROM fact_intraday_entry_decision WHERE session_date <= ?"),
            ("fact_intraday_adjusted_entry_decision", "SELECT COUNT(*) FROM fact_intraday_adjusted_entry_decision WHERE session_date <= ?"),
            ("fact_intraday_meta_decision", "SELECT COUNT(*) FROM fact_intraday_meta_decision WHERE session_date <= ?"),
        ),
        job_names=("run_intraday_assist_bundle",),
    ),
)


def intraday_research_feature_flags(settings: Settings) -> dict[str, bool]:
    config = settings.intraday_research
    return {
        "intraday_assist": config.enabled and config.assist_enabled,
        "intraday_policy_adjustment": config.enabled and config.policy_adjustment_enabled,
        "intraday_meta_model": config.enabled and config.meta_model_enabled,
        "intraday_postmortem": config.enabled and config.postmortem_enabled,
        "intraday_research_reports": config.enabled and config.research_reports_enabled,
        "intraday_discord_summary": config.enabled and config.discord_summary_enabled,
        "intraday_writeback": config.enabled and config.writeback_enabled,
    }


def intraday_research_enabled(settings: Settings) -> bool:
    return settings.intraday_research.enabled


def intraday_research_rollout_label(settings: Settings) -> str:
    return settings.intraday_research.rollout_mode or INTRADAY_RESEARCH_ROLLOUT


def _active_ids(connection, query: str) -> list[str]:
    frame = connection.execute(query).fetchdf()
    if frame.empty:
        return []
    return [str(value) for value in frame.iloc[:, 0].dropna().astype(str).tolist()]


def _latest_report_info(connection, report_types: Iterable[str], as_of_date: date) -> tuple[bool, str | None]:
    types = list(report_types)
    if not types:
        return False, None
    placeholders = ",".join("?" for _ in types)
    row = connection.execute(
        f"""
        SELECT report_type
        FROM fact_latest_report_index
        WHERE report_type IN ({placeholders})
          AND as_of_date <= ?
        ORDER BY generated_ts DESC
        LIMIT 1
        """,
        [*types, as_of_date],
    ).fetchone()
    return (row is not None, None if row is None else str(row[0]))


def _latest_job_summary(
    connection,
    *,
    job_names: Iterable[str],
) -> tuple[str | None, object | None, str | None, object | None, str | None]:
    names = list(job_names)
    if not names:
        return None, None, None, None, None
    placeholders = ",".join("?" for _ in names)
    success_row = connection.execute(
        f"""
        SELECT run_id, COALESCE(finished_at, started_at)
        FROM fact_job_run
        WHERE job_name IN ({placeholders})
          AND status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'DEGRADED_SUCCESS')
        ORDER BY COALESCE(finished_at, started_at) DESC
        LIMIT 1
        """,
        names,
    ).fetchone()
    degraded_row = connection.execute(
        f"""
        SELECT run_id, COALESCE(finished_at, started_at)
        FROM fact_job_run
        WHERE job_name IN ({placeholders})
          AND status IN ('DEGRADED_SUCCESS', 'BLOCKED', 'FAILED')
        ORDER BY COALESCE(finished_at, started_at) DESC
        LIMIT 1
        """,
        names,
    ).fetchone()
    skip_row = connection.execute(
        f"""
        SELECT notes
        FROM fact_job_run
        WHERE job_name IN ({placeholders})
          AND status LIKE 'SKIPPED%'
        ORDER BY COALESCE(finished_at, started_at) DESC
        LIMIT 1
        """,
        names,
    ).fetchone()
    return (
        None if success_row is None else str(success_row[0]),
        None if success_row is None else success_row[1],
        None if degraded_row is None else str(degraded_row[0]),
        None if degraded_row is None else degraded_row[1],
        None if skip_row is None or skip_row[0] is None else str(skip_row[0]),
    )


def materialize_intraday_research_capability(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    run_id: str | None = None,
    connection=None,
) -> IntradayResearchCapabilityResult:
    ensure_storage_layout(settings)
    target_date = as_of_date or today_local(settings.app.timezone)
    capability_run_id = run_id or f"intraday_research_capability-{target_date.isoformat()}"
    manage_connection = connection is None
    active_policy_ids: list[str] = []
    active_meta_model_ids: list[str] = []
    if manage_connection:
        context_manager = duckdb_connection(settings.paths.duckdb_path)
    else:
        context_manager = None
    try:
        if manage_connection:
            connection = context_manager.__enter__()
        bootstrap_core_tables(connection)
        active_policy_ids = _active_ids(
            connection,
            """
            SELECT policy_candidate_id
            FROM vw_latest_intraday_active_policy
            ORDER BY horizon, scope_type, scope_key
            """,
        )
        active_meta_model_ids = _active_ids(
            connection,
            """
            SELECT active_meta_model_id
            FROM vw_latest_intraday_active_meta_model
            ORDER BY horizon, panel_name
            """,
        )
        flags = intraday_research_feature_flags(settings)
        created_at = now_local(settings.app.timezone)
        for spec in CAPABILITY_SPECS:
            dependency_counts: dict[str, int] = {}
            for dependency_name, query in spec.dependency_queries:
                count_row = connection.execute(query, [target_date]).fetchone()
                dependency_counts[dependency_name] = int(count_row[0] or 0) if count_row else 0
            blocking_dependency = next(
                (name for name, count in dependency_counts.items() if count <= 0),
                None,
            )
            report_available_flag, latest_report_type = _latest_report_info(
                connection,
                spec.report_types,
                target_date,
            )
            last_successful_run_id, last_successful_run_at, last_degraded_run_id, last_degraded_run_at, last_skip_reason = _latest_job_summary(
                connection,
                job_names=spec.job_names,
            )
            dependency_ready_flag = blocking_dependency is None
            notes_json = json.dumps(
                {
                    "dependency_counts": dependency_counts,
                    "report_types": list(spec.report_types),
                    "job_names": list(spec.job_names),
                },
                ensure_ascii=False,
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO fact_intraday_research_capability (
                    run_id,
                    as_of_date,
                    feature_slug,
                    enabled_flag,
                    rollout_mode,
                    blocking_dependency,
                    dependency_ready_flag,
                    active_policy_ids_json,
                    active_meta_model_ids_json,
                    report_available_flag,
                    latest_report_type,
                    last_successful_run_id,
                    last_successful_run_at,
                    last_degraded_run_id,
                    last_degraded_run_at,
                    last_skip_reason,
                    notes_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    capability_run_id,
                    target_date,
                    spec.feature_slug,
                    bool(flags.get(spec.feature_slug, False)),
                    intraday_research_rollout_label(settings),
                    blocking_dependency,
                    dependency_ready_flag,
                    json.dumps(active_policy_ids, ensure_ascii=False),
                    json.dumps(active_meta_model_ids, ensure_ascii=False),
                    report_available_flag,
                    latest_report_type,
                    last_successful_run_id,
                    last_successful_run_at,
                    last_degraded_run_id,
                    last_degraded_run_at,
                    last_skip_reason,
                    notes_json,
                    created_at,
                ],
            )
        notes = (
            "Intraday research capability materialized. "
            f"features={len(CAPABILITY_SPECS)} rollout={intraday_research_rollout_label(settings)} "
            f"active_policy_ids={len(active_policy_ids)} active_meta_model_ids={len(active_meta_model_ids)}"
        )
        return IntradayResearchCapabilityResult(
            as_of_date=target_date,
            run_id=capability_run_id,
            row_count=len(CAPABILITY_SPECS),
            notes=notes,
        )
    finally:
        if manage_connection and context_manager is not None:
            context_manager.__exit__(None, None, None)


def validate_intraday_research_mode(
    settings: Settings,
    *,
    as_of_date: date,
) -> IntradayResearchModeValidationResult:
    ensure_storage_layout(settings)

    def _collect_counts(connection: duckdb.DuckDBPyConnection) -> tuple:
        return connection.execute(
            """
            SELECT
                (
                    SELECT COUNT(*)
                    FROM fact_intraday_research_capability
                    WHERE as_of_date = ?
                ) AS capability_count,
                (
                    SELECT COUNT(*)
                    FROM fact_intraday_research_capability
                    WHERE as_of_date = ?
                      AND enabled_flag
                ) AS enabled_count,
                (
                    SELECT COUNT(*)
                    FROM fact_intraday_entry_decision
                    WHERE session_date <= ?
                ) AS raw_count,
                (
                    SELECT COUNT(*)
                    FROM fact_intraday_adjusted_entry_decision
                    WHERE session_date <= ?
                ) AS adjusted_count,
                (
                    SELECT COUNT(*)
                    FROM fact_intraday_meta_decision
                    WHERE session_date <= ?
                ) AS meta_count,
                (
                    SELECT COUNT(*)
                    FROM vw_intraday_decision_lineage
                    WHERE session_date <= ?
                ) AS lineage_count,
                (
                    SELECT COUNT(*)
                    FROM fact_latest_report_index
                    WHERE report_type IN (
                        'intraday_summary_report',
                        'intraday_postmortem_report',
                        'intraday_policy_research_report',
                        'intraday_meta_model_report'
                    )
                ) AS report_count
            """,
            [as_of_date, as_of_date, as_of_date, as_of_date, as_of_date, as_of_date],
        ).fetchone()

    def _result_from_counts(run_id: str, counts: tuple, *, suffix: str = "") -> IntradayResearchModeValidationResult:
        warnings = sum(int(value or 0) == 0 for value in counts)
        notes = (
            "Intraday research mode validated. "
            f"capabilities={int(counts[0] or 0)} enabled={int(counts[1] or 0)} "
            f"raw={int(counts[2] or 0)} adjusted={int(counts[3] or 0)} "
            f"meta={int(counts[4] or 0)} lineage={int(counts[5] or 0)} "
            f"reports={int(counts[6] or 0)} warnings={warnings}"
        )
        if suffix:
            notes = f"{notes} {suffix}"
        return IntradayResearchModeValidationResult(
            run_id=run_id,
            as_of_date=as_of_date,
            check_count=7,
            warning_count=warnings,
            notes=notes,
        )

    with activate_run_context(
        "validate_intraday_research_mode",
        as_of_date=as_of_date,
    ) as run_context:
        try:
            with duckdb_connection(settings.paths.duckdb_path) as connection:
                bootstrap_core_tables(connection)
                record_run_start(
                    connection,
                    run_id=run_context.run_id,
                    run_type=run_context.run_type,
                    started_at=run_context.started_at,
                    as_of_date=as_of_date,
                    input_sources=[
                        "fact_intraday_research_capability",
                        "fact_intraday_entry_decision",
                        "fact_intraday_adjusted_entry_decision",
                        "fact_intraday_meta_decision",
                        "fact_latest_report_index",
                    ],
                    notes=f"Validate intraday research mode for {as_of_date.isoformat()}",
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                try:
                    result = _result_from_counts(run_context.run_id, _collect_counts(connection))
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=result.notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return result
                except Exception as exc:
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="failed",
                        output_artifacts=[],
                        notes=f"Intraday research mode validation failed for {as_of_date.isoformat()}",
                        error_message=str(exc),
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    raise
        except duckdb.IOException as exc:
            message = str(exc).lower()
            if (
                "could not set lock on file" not in message
                and "cannot open file" not in message
                and "다른 프로세스" not in message
            ):
                raise
            with duckdb_snapshot_connection(settings.paths.duckdb_path) as connection:
                result = _result_from_counts(
                    run_context.run_id,
                    _collect_counts(connection),
                    suffix="(snapshot read-only fallback due active writer lock)",
                )
            return result
