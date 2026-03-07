from __future__ import annotations

from app.providers.base import BaseProvider


class DartProvider(BaseProvider):
    provider_name = "dart"

    def credential_map(self) -> dict[str, str | None]:
        return {"api_key": self.settings.providers.dart.api_key}

    def fetch_corp_codes(self) -> dict[str, object]:
        return self.build_stub_payload("fetch_corp_codes")

    def fetch_company_overview(self, *, corp_code: str) -> dict[str, object]:
        return self.build_stub_payload("fetch_company_overview", corp_code=corp_code)
