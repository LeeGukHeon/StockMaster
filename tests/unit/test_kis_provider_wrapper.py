from __future__ import annotations

from app.providers.kis.client import KISProvider


def test_kis_provider_fetch_investor_flow_forwards_optional_args():
    calls: dict[str, object] = {}

    class _StubInvestorFlowClient:
        def fetch_investor_flow(self, **kwargs):
            calls.update(kwargs)
            return {"ok": True}

    provider = KISProvider.__new__(KISProvider)
    provider.investor_flow = _StubInvestorFlowClient()

    result = KISProvider.fetch_investor_flow(
        provider,
        symbol="005930",
        trading_date="2026-03-06",
        market_code="J",
        adjusted_price_flag="1",
        extra_class_code="X",
        persist_probe_artifacts=True,
    )

    assert result == {"ok": True}
    assert calls == {
        "symbol": "005930",
        "trading_date": "2026-03-06",
        "market_code": "J",
        "adjusted_price_flag": "1",
        "extra_class_code": "X",
        "persist_probe_artifacts": True,
    }
