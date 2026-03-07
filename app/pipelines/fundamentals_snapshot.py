from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.domain.fundamentals.account_normalizer import materialize_fundamentals_row
from app.domain.fundamentals.materializer import candidate_disclosures, statement_basis_order
from app.providers.dart.client import DartProvider
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from ._helpers import load_symbol_frame, write_json_payload


@dataclass(slots=True)
class FundamentalsSnapshotSyncResult:
    run_id: str
    as_of_date: date
    requested_symbol_count: int
    row_count: int
    skipped_symbol_count: int
    unmatched_corp_code_count: int
    failed_symbol_count: int
    artifact_paths: list[str]
    notes: str


def upsert_fundamentals_snapshot(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("fundamentals_snapshot_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_fundamentals_snapshot
        WHERE (as_of_date, symbol) IN (
            SELECT as_of_date, symbol
            FROM fundamentals_snapshot_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_fundamentals_snapshot (
            as_of_date,
            symbol,
            fiscal_year,
            report_code,
            revenue,
            operating_income,
            net_income,
            roe,
            debt_ratio,
            operating_margin,
            source_doc_id,
            source,
            disclosed_at,
            statement_basis,
            report_name,
            currency,
            accounting_standard,
            source_notes_json,
            ingested_at
        )
        SELECT
            as_of_date,
            symbol,
            fiscal_year,
            report_code,
            revenue,
            operating_income,
            net_income,
            roe,
            debt_ratio,
            operating_margin,
            source_doc_id,
            source,
            disclosed_at,
            statement_basis,
            report_name,
            currency,
            accounting_standard,
            source_notes_json,
            ingested_at
        FROM fundamentals_snapshot_stage
        """
    )
    connection.unregister("fundamentals_snapshot_stage")


def _raw_financial_path(settings: Settings, *, symbol: str, disclosed_date: date, file_name: str):
    return (
        settings.paths.raw_dir
        / "dart"
        / "financials"
        / f"disclosed_date={disclosed_date.isoformat()}"
        / f"symbol={symbol}"
        / file_name
    )


def sync_fundamentals_snapshot(
    settings: Settings,
    *,
    as_of_date: date,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    force: bool = False,
    dry_run: bool = False,
    dart_provider: DartProvider | None = None,
) -> FundamentalsSnapshotSyncResult:
    ensure_storage_layout(settings)
    owns_provider = dart_provider is None
    provider = dart_provider or DartProvider(settings)

    with activate_run_context("sync_fundamentals_snapshot", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["dart_list", "dart_fnlttSinglAcntAll", "dim_symbol"],
                notes=f"Materialize fundamentals snapshot for {as_of_date.isoformat()}",
            )
            try:
                symbol_frame = load_symbol_frame(
                    connection,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                )
                requested_symbol_count = len(symbol_frame)
                unmatched_corp_code_count = 0
                failed_symbol_count = 0
                skipped_symbol_count = 0

                existing_symbols: set[str] = set()
                if not force:
                    existing_symbols = {
                        str(row[0]).zfill(6)
                        for row in connection.execute(
                            """
                            SELECT symbol
                            FROM fact_fundamentals_snapshot
                            WHERE as_of_date = ?
                            """,
                            [as_of_date],
                        ).fetchall()
                    }

                if dry_run:
                    notes = (
                        f"Dry run only. as_of_date={as_of_date.isoformat()} "
                        f"symbols={requested_symbol_count} skipped_existing={len(existing_symbols)}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return FundamentalsSnapshotSyncResult(
                        run_id=run_context.run_id,
                        as_of_date=as_of_date,
                        requested_symbol_count=requested_symbol_count,
                        row_count=0,
                        skipped_symbol_count=len(existing_symbols),
                        unmatched_corp_code_count=0,
                        failed_symbol_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                output_rows: list[dict[str, object]] = []
                artifact_paths: list[str] = []

                for row in symbol_frame.itertuples(index=False):
                    symbol = str(row.symbol).zfill(6)
                    corp_code = str(row.dart_corp_code).strip() if row.dart_corp_code else ""
                    if symbol in existing_symbols:
                        skipped_symbol_count += 1
                        continue
                    if not corp_code:
                        unmatched_corp_code_count += 1
                        continue

                    try:
                        disclosures_snapshot = provider.fetch_regular_disclosures(
                            corp_code=corp_code,
                            start_date=as_of_date - timedelta(days=800),
                            end_date=as_of_date,
                        )
                        disclosures = candidate_disclosures(
                            disclosures_snapshot.frame,
                            as_of_date=as_of_date,
                        )
                        if disclosures.empty:
                            skipped_symbol_count += 1
                            continue

                        selected_row = None
                        selected_payload = None
                        selected_disclosure = None
                        selected_basis = None

                        for disclosure in disclosures.to_dict(orient="records"):
                            for basis in statement_basis_order():
                                financial_snapshot = provider.fetch_financial_statement(
                                    corp_code=corp_code,
                                    bsns_year=int(disclosure["fiscal_year"]),
                                    reprt_code=str(disclosure["reprt_code"]),
                                    fs_div=basis,
                                )
                                if financial_snapshot.frame.empty:
                                    continue
                                materialized = materialize_fundamentals_row(
                                    frame=financial_snapshot.frame,
                                    disclosure=disclosure,
                                    as_of_date=as_of_date,
                                    symbol=symbol,
                                    project_root=settings.paths.project_root,
                                    statement_basis=basis,
                                )
                                if materialized is None:
                                    continue
                                selected_row = materialized
                                selected_payload = financial_snapshot.payload
                                selected_disclosure = disclosure
                                selected_basis = basis
                                break
                            if selected_row is not None:
                                break

                        if (
                            selected_row is None
                            or selected_disclosure is None
                            or selected_payload is None
                        ):
                            skipped_symbol_count += 1
                            continue

                        notes_payload = json.loads(str(selected_row["source_notes_json"]))
                        notes_payload["corp_code"] = corp_code
                        notes_payload["report_type_name"] = selected_disclosure["report_type_name"]
                        selected_row["source_notes_json"] = json.dumps(
                            notes_payload,
                            ensure_ascii=False,
                            sort_keys=True,
                        )
                        selected_row["ingested_at"] = now_local(settings.app.timezone)
                        output_rows.append(selected_row)

                        raw_payload = {
                            "disclosure": selected_disclosure,
                            "statement_basis": selected_basis,
                            "financials": selected_payload,
                        }
                        raw_path = _raw_financial_path(
                            settings,
                            symbol=symbol,
                            disclosed_date=selected_disclosure["rcept_dt"],
                            file_name=f"{run_context.run_id}_{selected_basis}.json",
                        )
                        artifact_paths.append(str(write_json_payload(raw_path, raw_payload)))
                    except Exception:
                        failed_symbol_count += 1

                output_frame = pd.DataFrame(output_rows)
                if not output_frame.empty:
                    upsert_fundamentals_snapshot(connection, output_frame)
                    curated_path = write_parquet(
                        output_frame,
                        base_dir=settings.paths.curated_dir,
                        dataset="fundamentals/snapshot",
                        partitions={"as_of_date": as_of_date.isoformat()},
                        filename="fundamentals_snapshot.parquet",
                    )
                    artifact_paths.append(str(curated_path))

                if requested_symbol_count > 0 and output_frame.empty and failed_symbol_count > 0:
                    raise RuntimeError(
                        "No fundamentals rows were materialized from DART for the requested date."
                    )

                notes = (
                    f"Fundamentals snapshot completed. as_of_date={as_of_date.isoformat()}, "
                    f"rows={len(output_frame)}, requested_symbols={requested_symbol_count}, "
                    "skipped="
                    f"{skipped_symbol_count}, corp_code_unmatched={unmatched_corp_code_count}, "
                    f"failed={failed_symbol_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return FundamentalsSnapshotSyncResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    requested_symbol_count=requested_symbol_count,
                    row_count=len(output_frame),
                    skipped_symbol_count=skipped_symbol_count,
                    unmatched_corp_code_count=unmatched_corp_code_count,
                    failed_symbol_count=failed_symbol_count,
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
                    notes=f"Fundamentals snapshot failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                )
                raise
            finally:
                if owns_provider:
                    provider.close()
