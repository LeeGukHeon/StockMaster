from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class IntradayMonitorReportResult:
    run_id: str
    session_date: date
    checkpoint: str
    artifact_paths: list[str]
    notes: str


def render_intraday_monitor_report(
    settings: Settings,
    *,
    session_date: date,
    checkpoint: str,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    dry_run: bool = False,
) -> IntradayMonitorReportResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "render_intraday_monitor_report",
        as_of_date=session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=session_date,
                input_sources=[
                    "fact_intraday_candidate_session",
                    "fact_intraday_signal_snapshot",
                    "fact_intraday_entry_decision",
                    "fact_intraday_trade_summary",
                    "fact_intraday_quote_summary",
                ],
                notes=f"Render intraday monitor report for {session_date.isoformat()} {checkpoint}",
                ranking_version=ranking_version,
            )
            try:
                candidate_frame = connection.execute(
                    """
                    SELECT horizon, COUNT(*) AS candidate_count
                    FROM fact_intraday_candidate_session
                    WHERE session_date = ?
                      AND ranking_version = ?
                    GROUP BY horizon
                    ORDER BY horizon
                    """,
                    [session_date, ranking_version],
                ).fetchdf()
                decision_frame = connection.execute(
                    """
                    SELECT action, COUNT(*) AS row_count
                    FROM fact_intraday_entry_decision
                    WHERE session_date = ?
                      AND checkpoint_time = ?
                      AND ranking_version = ?
                    GROUP BY action
                    ORDER BY action
                    """,
                    [session_date, checkpoint, ranking_version],
                ).fetchdf()
                top_frame = connection.execute(
                    """
                    SELECT
                        decision.horizon,
                        decision.symbol,
                        candidate.company_name,
                        decision.action,
                        decision.action_score,
                        decision.signal_quality_score
                    FROM fact_intraday_entry_decision AS decision
                    JOIN fact_intraday_candidate_session AS candidate
                      ON decision.session_date = candidate.session_date
                     AND decision.symbol = candidate.symbol
                     AND decision.horizon = candidate.horizon
                     AND decision.ranking_version = candidate.ranking_version
                    WHERE decision.session_date = ?
                      AND decision.checkpoint_time = ?
                      AND decision.ranking_version = ?
                    ORDER BY decision.action_score DESC, decision.symbol
                    LIMIT 10
                    """,
                    [session_date, checkpoint, ranking_version],
                ).fetchdf()
                coverage_frame = connection.execute(
                    """
                    SELECT
                        COUNT(DISTINCT bar.symbol) AS bar_symbols,
                        COUNT(DISTINCT trade.symbol) AS trade_symbols,
                        COUNT(DISTINCT quote.symbol) AS quote_symbols
                    FROM fact_intraday_candidate_session AS candidate
                    LEFT JOIN fact_intraday_bar_1m AS bar
                      ON candidate.session_date = bar.session_date
                     AND candidate.symbol = bar.symbol
                    LEFT JOIN fact_intraday_trade_summary AS trade
                      ON candidate.session_date = trade.session_date
                     AND candidate.symbol = trade.symbol
                     AND trade.checkpoint_time = ?
                    LEFT JOIN fact_intraday_quote_summary AS quote
                      ON candidate.session_date = quote.session_date
                     AND candidate.symbol = quote.symbol
                     AND quote.checkpoint_time = ?
                    WHERE candidate.session_date = ?
                      AND candidate.ranking_version = ?
                    """,
                    [checkpoint, checkpoint, session_date, ranking_version],
                ).fetchdf()

                lines = [
                    "# Intraday Monitor Report",
                    "",
                    f"- session_date: {session_date.isoformat()}",
                    f"- checkpoint: {checkpoint}",
                    f"- ranking_version: {ranking_version}",
                    "",
                    "## Candidate Counts",
                ]
                if candidate_frame.empty:
                    lines.append("- no candidate session rows")
                else:
                    for row in candidate_frame.itertuples(index=False):
                        lines.append(
                            f"- horizon D+{int(row.horizon)}: {int(row.candidate_count)} candidates"
                        )
                lines.extend(["", "## Action Mix"])
                if decision_frame.empty:
                    lines.append("- no decision rows")
                else:
                    for row in decision_frame.itertuples(index=False):
                        lines.append(f"- {row.action}: {int(row.row_count)}")
                lines.extend(["", "## Data Coverage"])
                if coverage_frame.empty:
                    lines.append("- coverage unavailable")
                else:
                    coverage = coverage_frame.iloc[0]
                    lines.append(f"- bar symbols: {int(coverage['bar_symbols'] or 0)}")
                    lines.append(f"- trade summary symbols: {int(coverage['trade_symbols'] or 0)}")
                    lines.append(f"- quote summary symbols: {int(coverage['quote_symbols'] or 0)}")
                lines.extend(["", "## Top Candidates"])
                if top_frame.empty:
                    lines.append("- no ranked decision rows")
                else:
                    for row in top_frame.itertuples(index=False):
                        lines.append(
                            f"- D+{int(row.horizon)} {row.symbol} {row.company_name}: "
                            f"{row.action} score={float(row.action_score):.1f} "
                            f"quality={float(row.signal_quality_score):.1f}"
                        )

                report_text = "\n".join(lines).strip() + "\n"
                artifact_dir = (
                    settings.paths.artifacts_dir
                    / "intraday_monitor"
                    / f"session_date={session_date.isoformat()}"
                    / f"{run_context.run_id}"
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                report_path = artifact_dir / "intraday_monitor_preview.md"
                report_path.write_text(report_text, encoding="utf-8")
                artifact_paths = [str(report_path)]
                notes = (
                    "Intraday monitor report rendered. "
                    f"session_date={session_date.isoformat()} checkpoint={checkpoint}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes + (" dry_run=True" if dry_run else ""),
                    ranking_version=ranking_version,
                )
                return IntradayMonitorReportResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    checkpoint=checkpoint,
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
                        "Intraday monitor report failed for "
                        f"{session_date.isoformat()} {checkpoint}"
                    ),
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
