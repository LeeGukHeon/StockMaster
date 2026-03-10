from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class KrxServiceDefinition:
    service_slug: str
    display_name_ko: str
    category: str
    approval_required: bool = True
    fallback_policy: str = "fallback_to_seed_or_existing"
    expected_usage: str = "reference_or_statistics"
    request_cost_weight: int = 1


KRX_SERVICE_REGISTRY: tuple[KrxServiceDefinition, ...] = (
    KrxServiceDefinition(
        service_slug="stock_kospi_daily_trade",
        display_name_ko="유가증권 일별매매정보",
        category="daily_trade",
        expected_usage="market_statistics",
    ),
    KrxServiceDefinition(
        service_slug="stock_kosdaq_daily_trade",
        display_name_ko="코스닥 일별매매정보",
        category="daily_trade",
        expected_usage="market_statistics",
    ),
    KrxServiceDefinition(
        service_slug="stock_kospi_symbol_master",
        display_name_ko="유가증권 종목기본정보",
        category="symbol_master",
        expected_usage="reference",
    ),
    KrxServiceDefinition(
        service_slug="stock_kosdaq_symbol_master",
        display_name_ko="코스닥 종목기본정보",
        category="symbol_master",
        expected_usage="reference",
    ),
    KrxServiceDefinition(
        service_slug="index_krx_daily",
        display_name_ko="KRX 시리즈 일별시세정보",
        category="index_daily",
        expected_usage="index_statistics",
    ),
    KrxServiceDefinition(
        service_slug="index_kospi_daily",
        display_name_ko="KOSPI 시리즈 일별시세정보",
        category="index_daily",
        expected_usage="index_statistics",
    ),
    KrxServiceDefinition(
        service_slug="index_kosdaq_daily",
        display_name_ko="KOSDAQ 시리즈 일별시세정보",
        category="index_daily",
        expected_usage="index_statistics",
    ),
    KrxServiceDefinition(
        service_slug="etf_daily_trade",
        display_name_ko="ETF 일별매매정보",
        category="etf_daily_trade",
        expected_usage="etf_statistics",
    ),
)

KRX_SERVICE_BY_SLUG: dict[str, KrxServiceDefinition] = {
    item.service_slug: item for item in KRX_SERVICE_REGISTRY
}
KRX_CANONICAL_SERVICE_SLUGS: tuple[str, ...] = tuple(KRX_SERVICE_BY_SLUG)


def canonicalize_krx_service_slugs(values: list[str] | tuple[str, ...] | set[str]) -> list[str]:
    canonical: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        normalized = str(raw_value).strip().lower()
        if not normalized:
            continue
        if normalized not in KRX_SERVICE_BY_SLUG:
            raise ValueError(f"Unknown KRX service slug: {raw_value}")
        if normalized not in seen:
            canonical.append(normalized)
            seen.add(normalized)
    return canonical


def krx_service_url_env_key(service_slug: str) -> str:
    normalized = service_slug.strip().upper().replace("-", "_")
    return f"KRX_SERVICE_URL_{normalized}"


def krx_service_definition(service_slug: str) -> KrxServiceDefinition:
    normalized = service_slug.strip().lower()
    if normalized not in KRX_SERVICE_BY_SLUG:
        raise KeyError(f"Unknown KRX service slug: {service_slug}")
    return KRX_SERVICE_BY_SLUG[normalized]
