from __future__ import annotations

from app.providers.base import BaseProvider


class KrxProvider(BaseProvider):
    provider_name = "krx"

    def credential_map(self) -> dict[str, str | None]:
        return {"api_key": self.settings.providers.krx.api_key}

    def fetch_market_summary(self, *, trading_date: str | None = None) -> dict[str, object]:
        return self.build_stub_payload("fetch_market_summary", trading_date=trading_date)
