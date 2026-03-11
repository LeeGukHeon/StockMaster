from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from app.common.discord import publish_discord_messages
from app.ops.common import JobStatus, OpsJobResult
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables


def _fetch_frame(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    params: list[object] | None = None,
):
    return connection.execute(query, params or []).fetchdf()


def _frame_to_markdown(frame) -> str:
    if frame.empty:
        return ""
    columns = [str(column) for column in frame.columns]
    widths = [len(column) for column in columns]
    rows: list[list[str]] = []
    for values in frame.itertuples(index=False, name=None):
        row = []
        for index, value in enumerate(values):
            text = "" if value is None else str(value)
            widths[index] = max(widths[index], len(text))
            row.append(text)
        rows.append(row)
    header = "| " + " | ".join(
        column.ljust(widths[index]) for index, column in enumerate(columns)
    ) + " |"
    separator = "| " + " | ".join("-" * widths[index] for index in range(len(columns))) + " |"
    body = [
        "| " + " | ".join(value.ljust(widths[index]) for index, value in enumerate(row)) + " |"
        for row in rows
    ]
    return "\n".join([header, separator, *body])


def _status_label(value: object) -> str:
    mapping = {
        "SUCCESS": "정상",
        "PARTIAL_SUCCESS": "부분 완료",
        "DEGRADED_SUCCESS": "주의",
        "SKIPPED": "건너뜀",
        "BLOCKED": "차단",
        "FAILED": "장애",
    }
    text = str(value or "").upper()
    return mapping.get(text, text or "미확인")


def _metric_value(frame, *, scope: str, component: str, metric: str):
    if frame.empty:
        return None
    matched = frame.loc[
        (frame["health_scope"] == scope)
        & (frame["component_name"] == component)
        & (frame["metric_name"] == metric)
    ]
    if matched.empty:
        return None
    row = matched.iloc[0]
    if "metric_value_text" in matched.columns and row.get("metric_value_text") not in (None, "", "nan"):
        return row.get("metric_value_text")
    return row.get("metric_value_double")


def _build_ops_discord_summary(*, as_of_date: date, health, recovery) -> str:
    overall_status = "미확인"
    if not health.empty:
        overall_rows = health.loc[
            (health["health_scope"] == "overall") & (health["component_name"] == "platform")
        ]
        if not overall_rows.empty:
            overall_status = _status_label(overall_rows.iloc[0]["status"])

    failed_24h = int(float(_metric_value(health, scope="overall", component="platform", metric="failed_run_count_24h") or 0))
    open_alerts = int(float(_metric_value(health, scope="overall", component="platform", metric="open_alert_count") or 0))
    active_locks = int(float(_metric_value(health, scope="overall", component="platform", metric="active_lock_count") or 0))
    stale_locks = int(float(_metric_value(health, scope="overall", component="platform", metric="stale_lock_count") or 0))
    disk_watermark = _metric_value(health, scope="overall", component="platform", metric="disk_watermark") or "미확인"
    disk_ratio = _metric_value(health, scope="overall", component="platform", metric="disk_usage_ratio")
    latest_daily = _metric_value(health, scope="pipeline", component="daily_report", metric="latest_successful_output") or "-"
    latest_eval = _metric_value(health, scope="pipeline", component="evaluation_summary", metric="latest_successful_output") or "-"
    open_recovery = 0 if recovery.empty else int((recovery["status"] == "OPEN").sum())

    pieces = [
        f"**운영 요약 | {as_of_date.isoformat()}**",
        f"- 전체 상태: {overall_status}",
        f"- 최근 24시간 실패 작업: {failed_24h}건",
        f"- 열린 경고: {open_alerts}건 / 복구 대기: {open_recovery}건",
    ]
    if disk_ratio not in (None, "", "nan"):
        pieces.append(f"- 디스크: {float(disk_ratio):.1%} ({disk_watermark})")
    else:
        pieces.append(f"- 디스크 상태: {disk_watermark}")
    if active_locks or stale_locks:
        pieces.append(f"- 락 상태: 활성 {active_locks}개 / stale {stale_locks}개")
    pieces.append(f"- 최신 일일 요약 기준일: {latest_daily}")
    pieces.append(f"- 최신 사후평가 기준일: {latest_eval}")
    return "\n".join(pieces)


def render_ops_report(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    recent_runs = _fetch_frame(
        connection,
        """
        SELECT run_id, job_name, trigger_type, status, started_at, finished_at
        FROM fact_job_run
        ORDER BY started_at DESC
        LIMIT 10
        """,
    )
    step_failures = _fetch_frame(
        connection,
        """
        SELECT job_run_id, step_name, status, started_at, finished_at, error_message
        FROM fact_job_step_run
        WHERE status = 'FAILED'
        ORDER BY started_at DESC
        LIMIT 10
        """,
    )
    health = _fetch_frame(
        connection,
        """
        SELECT *
        FROM vw_latest_health_snapshot
        ORDER BY health_scope, component_name, metric_name
        """,
    )
    dependencies = _fetch_frame(
        connection,
        """
        SELECT *
        FROM vw_latest_pipeline_dependency_state
        ORDER BY pipeline_name, dependency_name
        """,
    )
    alerts = _fetch_frame(
        connection,
        """
        SELECT *
        FROM vw_latest_alert_event
        ORDER BY created_at DESC
        LIMIT 10
        """,
    )
    locks = _fetch_frame(
        connection,
        """
        SELECT *
        FROM vw_latest_active_lock
        WHERE released_at IS NULL
        ORDER BY acquired_at DESC
        """,
    )
    cleanup = _fetch_frame(
        connection,
        """
        SELECT *
        FROM vw_latest_retention_cleanup_run
        ORDER BY started_at DESC
        LIMIT 10
        """,
    )
    recovery = _fetch_frame(
        connection,
        """
        SELECT *
        FROM vw_latest_recovery_action
        ORDER BY created_at DESC
        LIMIT 10
        """,
    )
    outputs = _fetch_frame(
        connection,
        """
        SELECT component_name, metric_value_text
        FROM vw_latest_health_snapshot
        WHERE health_scope = 'pipeline'
          AND metric_name = 'latest_successful_output'
        ORDER BY component_name
        """,
    )
    sections = [
        f"# Ops Report\n\nAs of date: {as_of_date.isoformat()}",
        "## Overall Health",
        _frame_to_markdown(health) if not health.empty else "_No health snapshots available._",
        "## Recent Runs",
        _frame_to_markdown(recent_runs) if not recent_runs.empty else "_No recent runs._",
        "## Step Failures",
        _frame_to_markdown(step_failures) if not step_failures.empty else "_No failed steps._",
        "## Dependency Readiness",
        (
            _frame_to_markdown(dependencies)
            if not dependencies.empty
            else "_No dependency snapshots._"
        ),
        "## Alerts",
        _frame_to_markdown(alerts) if not alerts.empty else "_No alerts._",
        "## Active Locks",
        _frame_to_markdown(locks) if not locks.empty else "_No active locks._",
        "## Cleanup History",
        _frame_to_markdown(cleanup) if not cleanup.empty else "_No cleanup history._",
        "## Recovery Queue",
        _frame_to_markdown(recovery) if not recovery.empty else "_No recovery actions._",
        "## Latest Outputs",
        _frame_to_markdown(outputs) if not outputs.empty else "_No output summary._",
    ]
    content = "\n\n".join(sections)
    artifact_dir = (
        settings.paths.artifacts_dir
        / "ops"
        / "report"
        / f"as_of_date={as_of_date.isoformat()}"
        / (job_run_id or "embedded")
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    preview_path = artifact_dir / "ops_report_preview.md"
    preview_path.write_text(content, encoding="utf-8")
    payload_path = artifact_dir / "ops_report_payload.json"
    payload = {
        "username": settings.discord.username,
        "messages": [{"content": content[i : i + 1800]} for i in range(0, len(content), 1800)],
        "message_count": max(1, (len(content) + 1799) // 1800),
        "dry_run": dry_run,
    }
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_ops_report",
        status=JobStatus.SUCCESS,
        notes=f"Ops report rendered. dry_run={dry_run}",
        artifact_paths=[str(preview_path), str(payload_path)],
    )


def publish_discord_ops_alerts(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    rendered = render_ops_report(
        settings,
        connection=connection,
        as_of_date=as_of_date,
        job_run_id=job_run_id,
        dry_run=dry_run,
    )
    health = _fetch_frame(
        connection,
        """
        SELECT *
        FROM vw_latest_health_snapshot
        ORDER BY health_scope, component_name, metric_name
        """,
    )
    recovery = _fetch_frame(
        connection,
        """
        SELECT *
        FROM vw_latest_recovery_action
        ORDER BY created_at DESC
        LIMIT 20
        """,
    )
    summary = _build_ops_discord_summary(
        as_of_date=as_of_date,
        health=health,
        recovery=recovery,
    )
    artifact_dir = (
        settings.paths.artifacts_dir
        / "ops"
        / "report"
        / f"as_of_date={as_of_date.isoformat()}"
        / (job_run_id or "embedded")
    )
    summary_payload_path = artifact_dir / "ops_report_discord_summary.json"
    payload = {
        "username": settings.discord.username,
        "messages": [{"content": summary}],
        "message_count": 1,
        "dry_run": dry_run,
    }
    summary_payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    published = False
    if not dry_run and settings.discord.webhook_url:
        publish_discord_messages(
            settings.discord.webhook_url,
            list(payload.get("messages", [])),
            timeout=15.0,
        )
        published = True
    notes = f"Ops alerts {'published' if published else 'prepared'}."
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="publish_discord_ops_alerts",
        status=JobStatus.SUCCESS,
        notes=notes,
        artifact_paths=[*rendered.artifact_paths, str(summary_payload_path)],
    )
