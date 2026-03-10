from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd

from app.common.time import now_local, today_local
from app.ops.repository import json_text
from app.providers.base import (
    BaseProvider,
    ProviderHealth,
    ProviderRequestError,
    request_with_retries,
)
from app.providers.krx.registry import (
    KRX_SERVICE_BY_SLUG,
    canonicalize_krx_service_slugs,
    krx_service_definition,
)


@dataclass(frozen=True, slots=True)
class KrxFetchResult:
    service_slug: str
    frame: pd.DataFrame
    source: str
    fallback_used: bool
    fallback_reason: str | None
    http_status: int | None
    latency_ms: int | None
    status: str
    error_class: str | None = None


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        dict_rows = [item for item in payload if isinstance(item, dict)]
        if dict_rows:
            return dict_rows
        return []
    if not isinstance(payload, dict):
        return []

    candidate_keys = (
        "data",
        "items",
        "results",
        "result",
        "output",
        "output1",
        "output2",
        "response",
        "OutBlock_1",
        "OutBlock1",
        "body",
    )
    for key in candidate_keys:
        value = payload.get(key)
        records = _extract_records(value)
        if records:
            return records

    for value in payload.values():
        records = _extract_records(value)
        if records:
            return records
    return []


def _upsert_budget_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    provider_name: str,
    request_budget: int,
    snapshot_ts: datetime,
) -> None:
    date_kst = snapshot_ts.date()
    requests_used = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_external_api_request_log
            WHERE provider_name = ?
              AND CAST(request_ts AS DATE) = ?
            """,
            [provider_name, date_kst],
        ).fetchone()[0]
    )
    usage_ratio = float(requests_used / request_budget) if request_budget else 1.0
    if usage_ratio >= 1.0:
        throttle_state = "BLOCKED"
    elif usage_ratio >= 0.95:
        throttle_state = "FALLBACK_ONLY"
    elif usage_ratio >= 0.80:
        throttle_state = "WARNING"
    else:
        throttle_state = "OK"
    snapshot_id = f"api-budget-{provider_name}-{snapshot_ts.strftime('%Y%m%dT%H%M%S%f')}"
    connection.execute(
        """
        INSERT INTO fact_external_api_budget_snapshot (
            budget_snapshot_id,
            provider_name,
            snapshot_ts,
            date_kst,
            request_budget,
            requests_used,
            usage_ratio,
            throttle_state,
            details_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            snapshot_id,
            provider_name,
            snapshot_ts,
            date_kst,
            request_budget,
            requests_used,
            usage_ratio,
            throttle_state,
            json_text({"provider_name": provider_name}),
            snapshot_ts,
        ],
    )


class KrxProvider(BaseProvider):
    provider_name = "krx"

    def __init__(self, settings, *, timeout: float | None = None) -> None:
        effective_timeout = timeout or settings.providers.krx.request_timeout_seconds
        super().__init__(settings, timeout=effective_timeout)

    def credential_map(self) -> dict[str, str | None]:
        if not self.settings.providers.krx.enabled_live:
            return {}
        return {"api_key": self.settings.providers.krx.api_key}

    @property
    def enabled_live(self) -> bool:
        return bool(self.settings.providers.krx.enabled_live)

    @property
    def allowed_services(self) -> list[str]:
        return canonicalize_krx_service_slugs(self.settings.providers.krx.allowed_services)

    def allowed_service_count(self) -> int:
        return len(self.allowed_services)

    def service_enabled(self, service_slug: str) -> bool:
        normalized = service_slug.strip().lower()
        return self.enabled_live and normalized in set(self.allowed_services)

    def service_url(self, service_slug: str) -> str | None:
        normalized = service_slug.strip().lower()
        return self.settings.providers.krx.service_urls.get(normalized)

    def source_attribution_label(self) -> str:
        return self.settings.providers.krx.source_attribution_label

    def budget_state(
        self,
        connection: duckdb.DuckDBPyConnection | None = None,
        *,
        on_date: date | None = None,
    ) -> dict[str, object]:
        today = on_date or today_local(self.settings.app.timezone)
        if connection is None:
            return {
                "date_kst": today,
                "request_budget": self.settings.providers.krx.daily_request_budget,
                "requests_used": 0,
                "usage_ratio": 0.0,
                "throttle_state": "NO_SNAPSHOT",
            }
        row = connection.execute(
            """
            SELECT request_budget, requests_used, usage_ratio, throttle_state
            FROM vw_latest_external_api_budget_snapshot
            WHERE provider_name = 'krx'
              AND date_kst = ?
            ORDER BY snapshot_ts DESC
            LIMIT 1
            """,
            [today],
        ).fetchone()
        if row is None:
            return {
                "date_kst": today,
                "request_budget": self.settings.providers.krx.daily_request_budget,
                "requests_used": 0,
                "usage_ratio": 0.0,
                "throttle_state": "NO_SNAPSHOT",
            }
        return {
            "date_kst": today,
            "request_budget": int(row[0] or 0),
            "requests_used": int(row[1] or 0),
            "usage_ratio": float(row[2] or 0.0),
            "throttle_state": str(row[3] or "UNKNOWN"),
        }

    def health_check(self) -> ProviderHealth:
        if not self.enabled_live:
            return ProviderHealth(
                provider=self.provider_name,
                configured=bool(self.settings.providers.krx.api_key),
                status="disabled",
                detail="KRX live integration is disabled by configuration.",
            )
        if not self.settings.providers.krx.api_key:
            return ProviderHealth(
                provider=self.provider_name,
                configured=False,
                status="missing_credentials",
                detail="KRX live is enabled but KRX_API_KEY is missing.",
            )
        if not self.allowed_services:
            return ProviderHealth(
                provider=self.provider_name,
                configured=False,
                status="blocked",
                detail="KRX live is enabled but no approved services are configured.",
            )
        configured_urls = sum(
            1 for slug in self.allowed_services if bool(self.service_url(slug))
        )
        return ProviderHealth(
            provider=self.provider_name,
            configured=True,
            status="live_ready" if configured_urls else "partial_configuration",
            detail=(
                f"KRX live enabled. approved={len(self.allowed_services)} "
                f"configured_urls={configured_urls}"
            ),
        )

    def _record_request_log(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        service_slug: str,
        request_ts: datetime,
        run_id: str | None,
        as_of_date: date | None,
        http_status: int | None,
        status: str,
        latency_ms: int | None,
        rows_received: int,
        used_fallback: bool,
        error_code: str | None,
        error_message: str | None,
        endpoint_url: str | None,
    ) -> None:
        request_id = f"api-request-{uuid4().hex}"
        connection.execute(
            """
            INSERT INTO fact_external_api_request_log (
                request_id,
                provider_name,
                service_slug,
                run_id,
                as_of_date,
                request_ts,
                http_status,
                status,
                latency_ms,
                rows_received,
                used_fallback,
                error_code,
                error_message,
                endpoint_url,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                request_id,
                self.provider_name,
                service_slug,
                run_id,
                as_of_date,
                request_ts,
                http_status,
                status,
                latency_ms,
                rows_received,
                used_fallback,
                error_code,
                error_message,
                endpoint_url,
                request_ts,
            ],
        )
        _upsert_budget_snapshot(
            connection,
            provider_name=self.provider_name,
            request_budget=self.settings.providers.krx.daily_request_budget,
            snapshot_ts=request_ts,
        )

    def _record_service_status(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        service_slug: str,
        request_ts: datetime,
        last_smoke_status: str,
        http_status: int | None,
        last_error_class: str | None,
        fallback_mode: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        service = krx_service_definition(service_slug)
        service_status_id = f"krx-status-{service_slug}-{request_ts.strftime('%Y%m%dT%H%M%S%f')}"
        connection.execute(
            """
            INSERT INTO fact_krx_service_status (
                service_status_id,
                service_slug,
                display_name_ko,
                approval_expected,
                enabled_by_env,
                last_smoke_status,
                last_smoke_ts,
                last_success_ts,
                last_http_status,
                last_error_class,
                fallback_mode,
                notes_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                service_status_id,
                service.service_slug,
                service.display_name_ko,
                service.approval_required,
                self.service_enabled(service_slug),
                last_smoke_status,
                request_ts,
                request_ts if last_smoke_status == "SUCCESS" else None,
                http_status,
                last_error_class,
                fallback_mode,
                json_text(details),
                request_ts,
            ],
        )

    def _record_source_attribution(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        as_of_date: date | None,
    ) -> None:
        snapshot_ts = now_local(self.settings.app.timezone)
        rows = [
            ("market_pulse", "krx_market_statistics"),
            ("ops", "krx_live_status"),
            ("health_dashboard", "krx_live_health"),
            ("docs_help", "krx_live_help"),
        ]
        for page_slug, component_slug in rows:
            connection.execute(
                """
                INSERT INTO fact_source_attribution_snapshot (
                    attribution_snapshot_id,
                    snapshot_ts,
                    as_of_date,
                    page_slug,
                    component_slug,
                    source_label,
                    provider_name,
                    active_flag,
                    notes_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    f"source-attribution-{page_slug}-{component_slug}-{snapshot_ts.strftime('%Y%m%dT%H%M%S%f')}",
                    snapshot_ts,
                    as_of_date,
                    page_slug,
                    component_slug,
                    self.source_attribution_label(),
                    self.provider_name,
                    self.enabled_live,
                    json_text({"as_of_date": as_of_date.isoformat() if as_of_date else None}),
                    snapshot_ts,
                ],
            )

    def _budget_guard_state(
        self,
        connection: duckdb.DuckDBPyConnection | None,
    ) -> str:
        if connection is None:
            return "UNKNOWN"
        state = str(self.budget_state(connection).get("throttle_state", "UNKNOWN"))
        return state

    def _normalize_frame(self, service_slug: str, frame: pd.DataFrame) -> pd.DataFrame:
        output = frame.copy()
        rename_map = {
            "isu_cd": "symbol",
            "isu_srt_cd": "symbol",
            "ISU_CD": "symbol",
            "ISU_SRT_CD": "symbol",
            "short_code": "symbol",
            "stock_code": "symbol",
            "symbol_code": "symbol",
            "isu_nm": "company_name",
            "ISU_NM": "company_name",
            "kor_sec_nm": "company_name",
            "company": "company_name",
            "company_nm": "company_name",
            "market": "market_segment",
            "mkt_nm": "market_segment",
            "MKT_NM": "market_segment",
            "mrkt_ctg": "market_segment",
            "sector": "sector",
            "sector_nm": "sector",
            "induty_nm": "industry",
            "industry_nm": "industry",
            "lstg_dt": "listing_date",
            "listing_date": "listing_date",
        }
        for column_name in list(output.columns):
            normalized = column_name.strip()
            if normalized in rename_map:
                output = output.rename(columns={column_name: rename_map[normalized]})
        if "symbol" in output.columns:
            output["symbol"] = output["symbol"].astype(str).str[-6:].str.zfill(6)
        if "listing_date" in output.columns:
            output["listing_date"] = pd.to_datetime(
                output["listing_date"],
                errors="coerce",
            ).dt.date
        output["krx_service_slug"] = service_slug
        output["source"] = "krx_live"
        return output

    def fetch_service_rows(
        self,
        *,
        service_slug: str,
        params: dict[str, Any] | None = None,
        as_of_date: date | None = None,
        run_id: str | None = None,
        connection: duckdb.DuckDBPyConnection | None = None,
        allow_empty: bool = True,
        record_attribution: bool = False,
    ) -> KrxFetchResult:
        normalized_slug = service_slug.strip().lower()
        request_ts = now_local(self.settings.app.timezone)
        endpoint_url = self.service_url(normalized_slug)
        if normalized_slug not in KRX_SERVICE_BY_SLUG:
            raise ValueError(f"Unknown KRX service slug: {service_slug}")
        if not self.enabled_live:
            return KrxFetchResult(
                service_slug=normalized_slug,
                frame=pd.DataFrame(),
                source="krx_disabled",
                fallback_used=True,
                fallback_reason="live_disabled",
                http_status=None,
                latency_ms=None,
                status="BLOCKED",
            )
        if normalized_slug not in set(self.allowed_services):
            return KrxFetchResult(
                service_slug=normalized_slug,
                frame=pd.DataFrame(),
                source="krx_not_allowed",
                fallback_used=True,
                fallback_reason="service_not_allowed",
                http_status=None,
                latency_ms=None,
                status="BLOCKED",
            )
        if not endpoint_url:
            return KrxFetchResult(
                service_slug=normalized_slug,
                frame=pd.DataFrame(),
                source="krx_missing_endpoint",
                fallback_used=True,
                fallback_reason="missing_service_url",
                http_status=None,
                latency_ms=None,
                status="BLOCKED",
            )
        budget_state = self._budget_guard_state(connection)
        if budget_state == "BLOCKED":
            result = KrxFetchResult(
                service_slug=normalized_slug,
                frame=pd.DataFrame(),
                source="krx_budget_blocked",
                fallback_used=True,
                fallback_reason="daily_budget_exhausted",
                http_status=None,
                latency_ms=None,
                status="BLOCKED",
            )
            if connection is not None:
                self._record_request_log(
                    connection,
                    service_slug=normalized_slug,
                    request_ts=request_ts,
                    run_id=run_id,
                    as_of_date=as_of_date,
                    http_status=None,
                    status=result.status,
                    latency_ms=None,
                    rows_received=0,
                    used_fallback=True,
                    error_code="budget_exhausted",
                    error_message=result.fallback_reason,
                    endpoint_url=endpoint_url,
                )
                self._record_service_status(
                    connection,
                    service_slug=normalized_slug,
                    request_ts=request_ts,
                    last_smoke_status=result.status,
                    http_status=None,
                    last_error_class="budget_exhausted",
                    fallback_mode="fallback_only",
                    details={"reason": result.fallback_reason},
                )
            return result

        headers = {
            "Accept": "application/json",
            "AUTH_KEY": str(self.settings.providers.krx.api_key or ""),
        }
        started = datetime.now()
        try:
            response = request_with_retries(
                client=self.client,
                provider_name=self.provider_name,
                logger=self.logger,
                method="GET",
                url=endpoint_url,
                endpoint_label=normalized_slug,
                params=params or {},
                headers=headers,
            )
            latency_ms = int((datetime.now() - started).total_seconds() * 1000)
            payload = response.json()
            records = _extract_records(payload)
            frame = self._normalize_frame(normalized_slug, pd.DataFrame(records))
            if frame.empty and not allow_empty:
                raise ProviderRequestError(
                    self.provider_name,
                    normalized_slug,
                    "Valid response but no rows returned.",
                )
            result = KrxFetchResult(
                service_slug=normalized_slug,
                frame=frame,
                source="krx_live",
                fallback_used=False,
                fallback_reason=None,
                http_status=response.status_code,
                latency_ms=latency_ms,
                status="SUCCESS" if not frame.empty or allow_empty else "FAILED",
            )
            if connection is not None:
                self._record_request_log(
                    connection,
                    service_slug=normalized_slug,
                    request_ts=request_ts,
                    run_id=run_id,
                    as_of_date=as_of_date,
                    http_status=response.status_code,
                    status=result.status,
                    latency_ms=latency_ms,
                    rows_received=len(frame),
                    used_fallback=False,
                    error_code=None,
                    error_message=None,
                    endpoint_url=endpoint_url,
                )
                self._record_service_status(
                    connection,
                    service_slug=normalized_slug,
                    request_ts=request_ts,
                    last_smoke_status=result.status,
                    http_status=response.status_code,
                    last_error_class=None,
                    fallback_mode="primary_live",
                    details={"rows_received": len(frame)},
                )
                if record_attribution:
                    self._record_source_attribution(connection, as_of_date=as_of_date)
            return result
        except (ProviderRequestError, ValueError, json.JSONDecodeError) as exc:
            latency_ms = int((datetime.now() - started).total_seconds() * 1000)
            error_class = exc.__class__.__name__
            if connection is not None:
                self._record_request_log(
                    connection,
                    service_slug=normalized_slug,
                    request_ts=request_ts,
                    run_id=run_id,
                    as_of_date=as_of_date,
                    http_status=getattr(exc, "response", None).status_code
                    if getattr(exc, "response", None) is not None
                    else None,
                    status="DEGRADED_SUCCESS",
                    latency_ms=latency_ms,
                    rows_received=0,
                    used_fallback=True,
                    error_code=error_class,
                    error_message=str(exc),
                    endpoint_url=endpoint_url,
                )
                self._record_service_status(
                    connection,
                    service_slug=normalized_slug,
                    request_ts=request_ts,
                    last_smoke_status="DEGRADED_SUCCESS",
                    http_status=None,
                    last_error_class=error_class,
                    fallback_mode="fallback_only",
                    details={"error": str(exc)},
                )
            return KrxFetchResult(
                service_slug=normalized_slug,
                frame=pd.DataFrame(),
                source="krx_fallback",
                fallback_used=True,
                fallback_reason=str(exc),
                http_status=None,
                latency_ms=latency_ms,
                status="DEGRADED_SUCCESS",
                error_class=error_class,
            )

    def fetch_market_summary(
        self,
        *,
        trading_date: date | None = None,
        connection: duckdb.DuckDBPyConnection | None = None,
        run_id: str | None = None,
    ) -> dict[str, object]:
        service_slug = next(
            (
                slug
                for slug in (
                    "index_krx_daily",
                    "index_kospi_daily",
                    "index_kosdaq_daily",
                )
                if self.service_enabled(slug)
            ),
            None,
        )
        if service_slug is None:
            return self.build_stub_payload("fetch_market_summary", trading_date=trading_date)
        result = self.fetch_service_rows(
            service_slug=service_slug,
            params={
                "trading_date": (
                    trading_date or today_local(self.settings.app.timezone)
                ).isoformat()
            },
            as_of_date=trading_date,
            run_id=run_id,
            connection=connection,
            record_attribution=connection is not None,
        )
        return {
            "provider": self.provider_name,
            "service_slug": service_slug,
            "configured": self.is_configured(),
            "stub": False,
            "fallback_used": result.fallback_used,
            "row_count": len(result.frame),
            "status": result.status,
            "source": result.source,
        }
