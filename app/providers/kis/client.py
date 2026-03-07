from __future__ import annotations

from app.providers.base import BaseProvider, ProviderHealth

from .auth import KisTokenManager
from .investor_flow import KisInvestorFlowClient
from .market_data import KisMarketDataClient, SymbolMasterSnapshot


class KISProvider(BaseProvider):
    provider_name = "kis"

    def __init__(self, settings, *, timeout: float = 10.0) -> None:
        super().__init__(settings, timeout=timeout)
        self.token_manager = KisTokenManager(settings, self.client, self.logger)
        self.market_data = KisMarketDataClient(
            settings, self.client, self.logger, self.token_manager
        )
        self.investor_flow = KisInvestorFlowClient(
            settings, self.client, self.logger, self.token_manager
        )

    def credential_map(self) -> dict[str, str | None]:
        config = self.settings.providers.kis
        return {
            "app_key": config.app_key,
            "app_secret": config.app_secret,
        }

    def get_access_token(self, *, force_refresh: bool = False):
        return self.token_manager.get_access_token(force_refresh=force_refresh)

    def health_check(self) -> ProviderHealth:
        missing = self.missing_credentials()
        if missing:
            return super().health_check()

        try:
            token = self.get_access_token()
        except Exception as exc:
            return ProviderHealth(
                provider=self.provider_name,
                configured=True,
                status="error",
                detail=str(exc),
            )

        suffix = "mock" if self.settings.providers.kis.use_mock else "prod"
        return ProviderHealth(
            provider=self.provider_name,
            configured=True,
            status="ok",
            detail=(
                f"Token acquired from {token.source} cache for {suffix} environment. "
                f"Expires at {token.expires_at.isoformat()}."
            ),
        )

    def fetch_symbol_master(self, *, as_of_date=None) -> SymbolMasterSnapshot:
        return self.market_data.fetch_symbol_master(as_of_date=as_of_date)

    def fetch_current_quote(self, *, symbol: str) -> dict[str, object]:
        return self.market_data.fetch_current_quote(symbol=symbol)

    def fetch_daily_ohlcv(
        self,
        *,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        return self.market_data.fetch_daily_ohlcv(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def fetch_investor_flow(self, *, symbol: str, trading_date=None):
        return self.investor_flow.fetch_investor_flow(symbol=symbol, trading_date=trading_date)
