from __future__ import annotations

from app.providers.base import BaseProvider, ProviderHealth


class KISProvider(BaseProvider):
    provider_name = "kis"

    def credential_map(self) -> dict[str, str | None]:
        config = self.settings.providers.kis
        return {
            "app_key": config.app_key,
            "app_secret": config.app_secret,
            "account_no": config.account_no,
            "product_code": config.product_code,
        }

    def health_check(self) -> ProviderHealth:
        health = super().health_check()
        if self.settings.providers.kis.use_mock:
            health = ProviderHealth(
                provider=health.provider,
                configured=health.configured,
                status=health.status,
                detail=f"{health.detail} Mock mode is enabled.",
            )
        return health

    def fetch_symbol_master(self) -> dict[str, object]:
        return self.build_stub_payload("fetch_symbol_master", market="KOSPI,KOSDAQ")

    def fetch_daily_ohlcv(
        self,
        *,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, object]:
        return self.build_stub_payload(
            "fetch_daily_ohlcv",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
