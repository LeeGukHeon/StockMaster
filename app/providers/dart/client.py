from __future__ import annotations

import pandas as pd

from app.providers.base import BaseProvider, ProviderHealth

from .company import DartCompanyClient
from .corp_codes import CorpCodesSnapshot, DartCorpCodeClient
from .financials import (
    DartDisclosureSnapshot,
    DartFinancialClient,
    DartFinancialStatementSnapshot,
)


class DartProvider(BaseProvider):
    provider_name = "dart"

    def __init__(self, settings, *, timeout: float = 15.0) -> None:
        super().__init__(settings, timeout=timeout)
        self.corp_codes = DartCorpCodeClient(settings, self.client, self.logger)
        self.company = DartCompanyClient(settings, self.client, self.logger)
        self.financials = DartFinancialClient(settings, self.client, self.logger)

    def credential_map(self) -> dict[str, str | None]:
        return {"api_key": self.settings.providers.dart.api_key}

    def health_check(self) -> ProviderHealth:
        missing = self.missing_credentials()
        if missing:
            return super().health_check()

        try:
            snapshot = self.download_corp_codes(force=False)
        except Exception as exc:
            return ProviderHealth(
                provider=self.provider_name,
                configured=True,
                status="error",
                detail=str(exc),
            )

        source = "cache" if snapshot.cached else "fresh_download"
        return ProviderHealth(
            provider=self.provider_name,
            configured=True,
            status="ok",
            detail=f"corpCode map loaded from {source}. rows={len(snapshot.frame)}",
        )

    def download_corp_codes(self, *, force: bool = False) -> CorpCodesSnapshot:
        return self.corp_codes.download_corp_codes(force=force)

    def load_corp_code_map(self, *, force: bool = False) -> pd.DataFrame:
        return self.download_corp_codes(force=force).frame

    def fetch_corp_codes(self) -> pd.DataFrame:
        return self.load_corp_code_map(force=False)

    def fetch_company_overview(self, *, corp_code: str) -> dict[str, object]:
        return self.company.fetch_company_overview(corp_code=corp_code)

    def fetch_regular_disclosures(
        self,
        *,
        corp_code: str,
        start_date,
        end_date,
        page_count: int = 100,
    ) -> DartDisclosureSnapshot:
        return self.financials.fetch_regular_disclosures(
            corp_code=corp_code,
            start_date=start_date,
            end_date=end_date,
            page_count=page_count,
        )

    def fetch_financial_statement(
        self,
        *,
        corp_code: str,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementSnapshot:
        return self.financials.fetch_financial_statement(
            corp_code=corp_code,
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            fs_div=fs_div,
        )
