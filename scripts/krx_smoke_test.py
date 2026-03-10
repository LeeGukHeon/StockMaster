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
from app.providers.krx.registry import canonicalize_krx_service_slugs
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from scripts._krx_cli import base_parser, load_cli_settings, resolve_cli_as_of_date


def build_parser() -> argparse.ArgumentParser:
    parser = base_parser("Run a live KRX smoke test for one approved service.")
    parser.add_argument("--service-slug", required=True)
    parser.add_argument("--allow-empty", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_cli_settings()
    as_of_date = resolve_cli_as_of_date(settings, args.as_of_date)
    service_slug = canonicalize_krx_service_slugs([args.service_slug])[0]

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name="krx_smoke_test",
            as_of_date=as_of_date,
            notes=f"KRX smoke test for {service_slug}.",
        ) as job:
            frame = run_krx_smoke_tests(
                settings,
                service_slugs=[service_slug],
                as_of_date=as_of_date,
                connection=connection,
                run_id=job.run_id,
                allow_empty=args.allow_empty,
            )
            row = frame.iloc[0]
            if row["status"] != JobStatus.SUCCESS:
                job.mark_degraded(f"{service_slug} smoke test fell back: {row['fallback_reason']}")
            print(
                "KRX smoke test completed. "
                f"service_slug={service_slug} as_of_date={as_of_date.isoformat()} "
                f"status={row['status']} http_status={row['http_status']} "
                f"row_count={int(row['row_count'])} fallback_used={bool(row['fallback_used'])}"
            )
            if row["fallback_reason"]:
                print(f"fallback_reason={row['fallback_reason']}")
            return 0 if row["status"] == JobStatus.SUCCESS else 1


if __name__ == "__main__":
    raise SystemExit(main())
