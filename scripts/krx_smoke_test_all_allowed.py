# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys

PROJECT_ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ops.common import JobStatus
from app.ops.runtime import JobRunContext
from app.providers.krx.monitoring import run_krx_smoke_tests
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from scripts._krx_cli import base_parser, load_cli_settings, resolve_cli_as_of_date


def build_parser() -> argparse.ArgumentParser:
    parser = base_parser("Run live KRX smoke tests for all approved services.")
    parser.add_argument("--allow-empty", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_cli_settings()
    as_of_date = resolve_cli_as_of_date(settings, args.as_of_date)
    service_slugs = list(settings.providers.krx.allowed_services)

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name="krx_smoke_test_all_allowed",
            as_of_date=as_of_date,
            notes="KRX smoke test for all allowed services.",
        ) as job:
            frame = run_krx_smoke_tests(
                settings,
                service_slugs=service_slugs,
                as_of_date=as_of_date,
                connection=connection,
                run_id=job.run_id,
                allow_empty=args.allow_empty,
            )
            success_count = int((frame["status"] == JobStatus.SUCCESS).sum())
            degraded = frame[frame["status"] != JobStatus.SUCCESS]
            if not degraded.empty:
                job.mark_degraded(
                    f"{len(degraded)} KRX services fell back during smoke test."
                )
            print(
                "KRX smoke test completed for all allowed services. "
                f"as_of_date={as_of_date.isoformat()} total={len(frame)} "
                f"success={success_count} degraded={len(degraded)}"
            )
            for row in frame.itertuples(index=False):
                print(
                    f"- {row.service_slug}: status={row.status} http_status={row.http_status} "
                    f"row_count={int(row.row_count)} fallback_used={bool(row.fallback_used)} "
                    f"fallback_reason={row.fallback_reason or '-'}"
                )
            return 0 if degraded.empty else 1


if __name__ == "__main__":
    raise SystemExit(main())
