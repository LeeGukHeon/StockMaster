from __future__ import annotations

from datetime import date

import pytest

from app.providers import base
from app.providers.krx.registry import (
    KRX_SERVICE_REGISTRY,
    build_krx_request_params,
    canonicalize_krx_service_slugs,
    krx_default_service_urls,
)


def test_krx_registry_contains_all_approved_services() -> None:
    service_slugs = [item.service_slug for item in KRX_SERVICE_REGISTRY]

    assert len(service_slugs) == 8
    assert "etf_daily_trade" in service_slugs
    assert "stock_kospi_symbol_master" in service_slugs


def test_krx_default_service_urls_cover_registry() -> None:
    service_urls = krx_default_service_urls()

    assert set(service_urls) == {item.service_slug for item in KRX_SERVICE_REGISTRY}
    assert service_urls["etf_daily_trade"].endswith("/svc/apis/etp/etf_bydd_trd")


def test_build_krx_request_params_maps_as_of_date_to_bas_dd() -> None:
    params = build_krx_request_params("etf_daily_trade", as_of_date=date(2026, 3, 6))

    assert params == {"basDd": "20260306"}


def test_canonicalize_krx_service_slugs_rejects_unknown_service() -> None:
    with pytest.raises(ValueError):
        canonicalize_krx_service_slugs(["unknown_service"])


def test_krx_request_policy_resolves_per_service_override() -> None:
    policy = base.resolve_provider_request_policy("krx", "etf_daily_trade")

    assert policy.min_interval_seconds == 0.6
    assert policy.retries == 4
