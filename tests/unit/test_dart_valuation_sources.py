from __future__ import annotations

from types import SimpleNamespace

import httpx

from app.providers.dart.financials import DartFinancialClient


class _NoopLogger:
    def warning(self, *args, **kwargs) -> None:
        return None


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        providers=SimpleNamespace(
            dart=SimpleNamespace(base_url="https://opendart.example", api_key="test-key")
        )
    )


def test_fetch_stock_total_status_preserves_source_fields() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={
                "status": "000",
                "message": "정상",
                "list": [
                    {
                        "rcept_no": "20250331000001",
                        "corp_cls": "Y",
                        "corp_code": "00126380",
                        "corp_name": "삼성전자",
                        "se": "보통주",
                        "istc_totqy": "5,969,782,550",
                        "tesstk_co": "0",
                        "distb_stock_co": "5,969,782,550",
                        "stlm_dt": "2024-12-31",
                    }
                ],
            },
        )

    client = DartFinancialClient(
        _settings(),
        httpx.Client(transport=httpx.MockTransport(handler)),
        _NoopLogger(),
    )

    snapshot = client.fetch_stock_total_status(
        corp_code="00126380",
        bsns_year=2024,
        reprt_code="11011",
    )

    assert seen["path"] == "/api/stockTotqySttus.json"
    assert seen["params"] == {
        "crtfc_key": "test-key",
        "corp_code": "00126380",
        "bsns_year": "2024",
        "reprt_code": "11011",
    }
    assert snapshot.payload["status"] == "000"
    assert list(snapshot.frame.columns) == [
        "rcept_no",
        "corp_cls",
        "corp_code",
        "corp_name",
        "se",
        "istc_totqy",
        "tesstk_co",
        "distb_stock_co",
        "stlm_dt",
    ]
    assert snapshot.frame.loc[0, "se"] == "보통주"
    assert snapshot.frame.loc[0, "istc_totqy"] == "5,969,782,550"
    assert snapshot.frame.loc[0, "distb_stock_co"] == "5,969,782,550"


def test_fetch_financial_indicator_preserves_indicator_rows() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={
                "status": "000",
                "message": "정상",
                "list": [
                    {
                        "corp_code": "00126380",
                        "corp_name": "삼성전자",
                        "bsns_year": "2024",
                        "reprt_code": "11011",
                        "idx_cl_code": "M210000",
                        "idx_cl_nm": "수익성지표",
                        "idx_code": "M211000",
                        "idx_nm": "영업이익률",
                        "idx_val": "12.3",
                    }
                ],
            },
        )

    client = DartFinancialClient(
        _settings(),
        httpx.Client(transport=httpx.MockTransport(handler)),
        _NoopLogger(),
    )

    snapshot = client.fetch_financial_indicator(
        corp_code="00126380",
        bsns_year=2024,
        reprt_code="11011",
        idx_cl_code="M210000",
    )

    assert seen["path"] == "/api/fnlttSinglIndx.json"
    assert seen["params"] == {
        "crtfc_key": "test-key",
        "corp_code": "00126380",
        "bsns_year": "2024",
        "reprt_code": "11011",
        "idx_cl_code": "M210000",
    }
    assert snapshot.payload["status"] == "000"
    assert snapshot.frame.loc[0, "idx_nm"] == "영업이익률"
    assert snapshot.frame.loc[0, "idx_val"] == "12.3"


def test_dart_empty_status_returns_empty_valuation_frames() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "013", "message": "조회된 데이타가 없습니다."})

    client = DartFinancialClient(
        _settings(),
        httpx.Client(transport=httpx.MockTransport(handler)),
        _NoopLogger(),
    )

    stock_total = client.fetch_stock_total_status(
        corp_code="00126380",
        bsns_year=2024,
        reprt_code="11011",
    )
    indicator = client.fetch_financial_indicator(
        corp_code="00126380",
        bsns_year=2024,
        reprt_code="11011",
        idx_cl_code="M210000",
    )

    assert stock_total.frame.empty
    assert indicator.frame.empty
