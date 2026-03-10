# ruff: noqa: E402

from __future__ import annotations

import sys

PROJECT_ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.providers.krx.monitoring import collect_krx_validation_summary
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from scripts._krx_cli import load_cli_settings


def main() -> int:
    settings = load_cli_settings()
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        summary = collect_krx_validation_summary(settings, connection=connection)

    print(
        "KRX live configuration validation completed. "
        f"status={summary.status} checks={summary.check_count} "
        f"errors={summary.error_count} warnings={summary.warning_count} "
        f"health={summary.provider_health_status}"
    )
    for issue in summary.issues:
        print(f"[{issue.severity}] {issue.code}: {issue.message}")
    return 0 if summary.error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
