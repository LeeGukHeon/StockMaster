from __future__ import annotations

from dataclasses import dataclass

from app.common.run_context import activate_run_context
from app.common.time import now_local, today_local
from app.providers.dart.client import DartProvider
from app.providers.kis.client import KISProvider
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class ProviderSmokeResult:
    run_id: str
    symbol: str
    kis_status: str
    dart_status: str
    corp_code: str | None
    artifact_paths: list[str]


def _lookup_corp_code(connection, symbol: str) -> str | None:
    row = connection.execute(
        "SELECT dart_corp_code FROM dim_symbol WHERE symbol = ?",
        [symbol],
    ).fetchone()
    if row and row[0]:
        return str(row[0])
    return None


def run_provider_smoke_check(
    settings: Settings,
    *,
    symbol: str,
    kis_provider: KISProvider | None = None,
    dart_provider: DartProvider | None = None,
) -> ProviderSmokeResult:
    ensure_storage_layout(settings)
    owns_kis = kis_provider is None
    owns_dart = dart_provider is None
    kis = kis_provider or KISProvider(settings)
    dart = dart_provider or DartProvider(settings)

    with activate_run_context(
        "provider_smoke_check", as_of_date=today_local(settings.app.timezone)
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["kis_quote_probe", "dart_company_overview"],
                notes=f"Provider smoke check for symbol {symbol}",
            )
            try:
                artifact_paths: list[str] = []
                kis_quote = kis.fetch_current_quote(symbol=symbol)
                kis_status = "ok"
                raw_path = kis_quote.get("_raw_path")
                if raw_path:
                    artifact_paths.append(str(raw_path))

                corp_code = _lookup_corp_code(connection, symbol)
                dart_status = "skipped"
                try:
                    if corp_code is None:
                        corp_codes = dart.load_corp_code_map(force=False)
                        matched = corp_codes.loc[corp_codes["stock_code"] == symbol, "corp_code"]
                        corp_code = str(matched.iloc[0]) if not matched.empty else None
                    if corp_code is not None:
                        overview = dart.fetch_company_overview(corp_code=corp_code)
                        raw_path = overview.get("_raw_path")
                        if raw_path:
                            artifact_paths.append(str(raw_path))
                        dart_status = "ok"
                except Exception as exc:
                    dart_status = f"error:{exc}"

                notes = (
                    f"Provider smoke check completed. symbol={symbol}, "
                    f"kis_status={kis_status}, dart_status={dart_status}, corp_code={corp_code}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return ProviderSmokeResult(
                    run_id=run_context.run_id,
                    symbol=symbol,
                    kis_status=kis_status,
                    dart_status=dart_status,
                    corp_code=corp_code,
                    artifact_paths=artifact_paths,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Provider smoke check failed.",
                    error_message=str(exc),
                )
                raise
            finally:
                if owns_kis:
                    kis.close()
                if owns_dart:
                    dart.close()
