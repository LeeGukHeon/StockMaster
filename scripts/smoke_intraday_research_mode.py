# ruff: noqa: E402, E501

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.research_mode import materialize_intraday_research_capability
from app.release.snapshot import build_report_index
from app.storage.duckdb import duckdb_connection
from scripts._ops_cli import load_cli_settings


def main() -> int:
    settings = load_cli_settings()
    probe_date = date(2026, 3, 9)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        existing_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM fact_intraday_research_capability
                WHERE as_of_date = ?
                """,
                [probe_date],
            ).fetchone()[0]
            or 0
        )
    if existing_count > 0:
        run_id = f"intraday_research_capability-existing-{probe_date.isoformat()}"
    else:
        result = materialize_intraday_research_capability(settings, as_of_date=probe_date)
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            build_report_index(settings, connection=connection)
        run_id = result.run_id
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        counts = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM vw_latest_intraday_research_capability) AS capability_count,
                (SELECT COUNT(*) FROM fact_intraday_entry_decision WHERE session_date <= ?) AS raw_count,
                (SELECT COUNT(*) FROM fact_intraday_adjusted_entry_decision WHERE session_date <= ?) AS adjusted_count,
                (SELECT COUNT(*) FROM fact_intraday_meta_decision WHERE session_date <= ?) AS meta_count,
                (SELECT COUNT(*) FROM vw_intraday_decision_lineage WHERE session_date <= ?) AS lineage_count
            """,
            [probe_date, probe_date, probe_date, probe_date],
        ).fetchone()
    print(
        "Intraday research mode smoke completed. "
        f"run_id={run_id} capabilities={int(counts[0] or 0)} raw={int(counts[1] or 0)} "
        f"adjusted={int(counts[2] or 0)} meta={int(counts[3] or 0)} lineage={int(counts[4] or 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
