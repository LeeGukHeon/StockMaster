from __future__ import annotations

from datetime import date

import pandas as pd

from app.providers.base import ProviderRequestError
from app.providers.krx.client import KrxProvider
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload


def _enable_krx_live(settings) -> None:
    settings.providers.krx.enabled_live = True
    settings.providers.krx.api_key = "test-krx-key"
    settings.providers.krx.allowed_services = ["etf_daily_trade"]


def test_krx_live_fetch_records_request_budget_and_status(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    _enable_krx_live(settings)

    def _fake_request_with_retries(**_kwargs):
        return _FakeResponse(
            {
                "OutBlock_1": [
                    {
                        "BAS_DD": "20260306",
                        "ISU_CD": "069500",
                        "ISU_NM": "KODEX 200",
                        "TDD_CLSPRC": "35000",
                    }
                ]
            }
        )

    monkeypatch.setattr(
        "app.providers.krx.client.request_with_retries",
        _fake_request_with_retries,
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        provider = KrxProvider(settings)
        try:
            result = provider.fetch_service_rows(
                service_slug="etf_daily_trade",
                as_of_date=date(2026, 3, 6),
                run_id="test-krx-live",
                connection=connection,
                allow_empty=False,
                record_attribution=True,
            )
        finally:
            provider.close()

        assert result.status == "SUCCESS"
        assert result.fallback_used is False
        assert result.http_status == 200
        assert result.frame.loc[0, "symbol"] == "069500"
        assert result.frame.loc[0, "company_name"] == "KODEX 200"

        request_count = connection.execute(
            "SELECT COUNT(*) FROM fact_external_api_request_log WHERE provider_name = 'krx'"
        ).fetchone()[0]
        budget_count = connection.execute(
            "SELECT COUNT(*) FROM fact_external_api_budget_snapshot WHERE provider_name = 'krx'"
        ).fetchone()[0]
        status_count = connection.execute(
            "SELECT COUNT(*) FROM fact_krx_service_status WHERE service_slug = 'etf_daily_trade'"
        ).fetchone()[0]
        attribution_count = connection.execute(
            "SELECT COUNT(*) FROM fact_source_attribution_snapshot WHERE provider_name = 'krx'"
        ).fetchone()[0]

        assert request_count == 1
        assert budget_count == 1
        assert status_count == 1
        assert attribution_count == 4


def test_krx_live_fetch_fallback_records_degraded_status(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    _enable_krx_live(settings)

    def _fake_request_with_retries(**_kwargs):
        raise ProviderRequestError("krx", "etf_daily_trade", "simulated failure")

    monkeypatch.setattr(
        "app.providers.krx.client.request_with_retries",
        _fake_request_with_retries,
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        provider = KrxProvider(settings)
        try:
            result = provider.fetch_service_rows(
                service_slug="etf_daily_trade",
                as_of_date=date(2026, 3, 6),
                run_id="test-krx-fallback",
                connection=connection,
                allow_empty=False,
            )
        finally:
            provider.close()

        assert result.status == "DEGRADED_SUCCESS"
        assert result.fallback_used is True
        assert "simulated failure" in (result.fallback_reason or "")

        latest_status = connection.execute(
            """
            SELECT last_smoke_status, fallback_mode, last_error_class
            FROM vw_latest_krx_service_status
            WHERE service_slug = 'etf_daily_trade'
            """
        ).fetchone()
        latest_request = connection.execute(
            """
            SELECT status, used_fallback, error_code
            FROM vw_latest_external_api_request_log
            WHERE provider_name = 'krx'
              AND service_slug = 'etf_daily_trade'
            """
        ).fetchone()

        assert latest_status == ("DEGRADED_SUCCESS", "fallback_only", "ProviderRequestError")
        assert latest_request == ("DEGRADED_SUCCESS", True, "ProviderRequestError")


def test_krx_live_normalization_handles_duplicate_symbol_like_columns(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    _enable_krx_live(settings)
    provider = KrxProvider(settings)
    try:
        frame = provider._normalize_frame(  # noqa: SLF001
            "stock_kospi_symbol_master",
            pd.DataFrame(
                [
                    {
                        "ISU_CD": "KR7069500007",
                        "ISU_SRT_CD": "069500",
                        "ISU_NM": "KODEX 200",
                    }
                ]
            ),
        )
    finally:
        provider.close()

    assert frame.loc[0, "symbol"] == "069500"
    assert frame.loc[0, "company_name"] == "KODEX 200"
