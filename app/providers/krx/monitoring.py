from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import duckdb
import pandas as pd

from app.common.time import today_local
from app.providers.krx.client import KrxProvider
from app.providers.krx.registry import KRX_SERVICE_BY_SLUG, KRX_SERVICE_REGISTRY
from app.settings import Settings

REQUIRED_KRX_RELATIONS: tuple[str, ...] = (
    "fact_external_api_request_log",
    "fact_external_api_budget_snapshot",
    "fact_krx_service_status",
    "fact_source_attribution_snapshot",
    "vw_latest_external_api_budget_snapshot",
    "vw_latest_krx_service_status",
    "vw_latest_source_attribution_snapshot",
)


@dataclass(frozen=True, slots=True)
class KrxValidationIssue:
    severity: str
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class KrxValidationSummary:
    issues: tuple[KrxValidationIssue, ...]
    check_count: int
    warning_count: int
    error_count: int
    status: str
    provider_health_status: str
    provider_health_detail: str


def resolve_default_as_of_date(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> date:
    fallback_date = today_local(settings.app.timezone)
    if connection is None:
        return fallback_date
    try:
        row = connection.execute(
            """
            SELECT MAX(trading_date)
            FROM dim_trading_calendar
            WHERE is_trading_day = TRUE
              AND trading_date <= ?
            """,
            [fallback_date],
        ).fetchone()
    except duckdb.Error:
        return fallback_date
    if row and row[0]:
        return row[0]
    return fallback_date


def collect_krx_validation_summary(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> KrxValidationSummary:
    issues: list[KrxValidationIssue] = []
    provider = KrxProvider(settings)
    try:
        health = provider.health_check()
    finally:
        provider.close()

    allowed_set = set(settings.providers.krx.allowed_services)
    registry_set = {service.service_slug for service in KRX_SERVICE_REGISTRY}
    configured_urls = {
        slug: url for slug, url in settings.providers.krx.service_urls.items() if str(url).strip()
    }

    if not settings.providers.krx.enabled_live:
        issues.append(
            KrxValidationIssue(
                severity="ERROR",
                code="krx_live_disabled",
                message="ENABLE_KRX_LIVE=false 상태입니다.",
            )
        )
    if settings.providers.krx.enabled_live and not settings.providers.krx.api_key:
        issues.append(
            KrxValidationIssue(
                severity="ERROR",
                code="krx_api_key_missing",
                message="ENABLE_KRX_LIVE=true 이지만 KRX_API_KEY가 없습니다.",
            )
        )
    if settings.providers.krx.enabled_live and not allowed_set:
        issues.append(
            KrxValidationIssue(
                severity="ERROR",
                code="krx_allowed_services_missing",
                message="승인 서비스 allowlist가 비어 있습니다.",
            )
        )

    unknown_services = sorted(allowed_set - registry_set)
    if unknown_services:
        issues.append(
            KrxValidationIssue(
                severity="ERROR",
                code="krx_unknown_allowed_service",
                message=f"registry에 없는 승인 서비스 slug: {', '.join(unknown_services)}",
            )
        )

    missing_urls = sorted(slug for slug in allowed_set if not configured_urls.get(slug))
    if missing_urls:
        issues.append(
            KrxValidationIssue(
                severity="ERROR",
                code="krx_missing_service_url",
                message=f"실제 endpoint URL이 없는 서비스: {', '.join(missing_urls)}",
            )
        )

    if settings.providers.krx.daily_request_budget <= 0:
        issues.append(
            KrxValidationIssue(
                severity="ERROR",
                code="krx_invalid_budget",
                message="KRX_DAILY_REQUEST_BUDGET는 양수여야 합니다.",
            )
        )
    if settings.providers.krx.request_timeout_seconds <= 0:
        issues.append(
            KrxValidationIssue(
                severity="ERROR",
                code="krx_invalid_timeout",
                message="KRX_REQUEST_TIMEOUT_SECONDS는 양수여야 합니다.",
            )
        )
    if not settings.providers.krx.source_attribution_label.strip():
        issues.append(
            KrxValidationIssue(
                severity="WARNING",
                code="krx_source_label_missing",
                message="KRX 출처 표기 문구가 비어 있습니다.",
            )
        )

    if connection is not None:
        relation_names = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        missing_relations = sorted(
            relation for relation in REQUIRED_KRX_RELATIONS if relation not in relation_names
        )
        if missing_relations:
            issues.append(
                KrxValidationIssue(
                    severity="ERROR",
                    code="krx_missing_relations",
                    message=f"필수 KRX 저장 계약/뷰가 없습니다: {', '.join(missing_relations)}",
                )
            )

    if health.status == "partial_configuration":
        issues.append(
            KrxValidationIssue(
                severity="WARNING",
                code="krx_partial_configuration",
                message=health.detail,
            )
        )
    elif health.status in {"missing_credentials", "blocked", "disabled"}:
        issues.append(
            KrxValidationIssue(
                severity="ERROR",
                code="krx_health_blocked",
                message=health.detail,
            )
        )

    error_count = sum(1 for item in issues if item.severity == "ERROR")
    warning_count = sum(1 for item in issues if item.severity == "WARNING")
    status = "PASS"
    if error_count:
        status = "FAIL"
    elif warning_count:
        status = "WARN"
    return KrxValidationSummary(
        issues=tuple(issues),
        check_count=8,
        warning_count=warning_count,
        error_count=error_count,
        status=status,
        provider_health_status=health.status,
        provider_health_detail=health.detail,
    )


def run_krx_smoke_tests(
    settings: Settings,
    *,
    service_slugs: list[str],
    as_of_date: date,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    allow_empty: bool = False,
) -> pd.DataFrame:
    provider = KrxProvider(settings)
    rows: list[dict[str, Any]] = []
    try:
        for service_slug in service_slugs:
            result = provider.fetch_service_rows(
                service_slug=service_slug,
                as_of_date=as_of_date,
                run_id=run_id,
                connection=connection,
                allow_empty=allow_empty,
                record_attribution=True,
            )
            service = KRX_SERVICE_BY_SLUG[service_slug]
            rows.append(
                {
                    "service_slug": service_slug,
                    "display_name_ko": service.display_name_ko,
                    "status": result.status,
                    "source": result.source,
                    "fallback_used": result.fallback_used,
                    "fallback_reason": result.fallback_reason,
                    "http_status": result.http_status,
                    "latency_ms": result.latency_ms,
                    "row_count": len(result.frame),
                    "as_of_date": as_of_date,
                }
            )
    finally:
        provider.close()
    return pd.DataFrame(rows)
