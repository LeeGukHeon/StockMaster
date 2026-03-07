from __future__ import annotations

import json
from typing import Any

import httpx

from app.common.time import today_local
from app.providers.base import ProviderRequestError, request_with_retries
from app.settings import Settings


class DartCompanyClient:
    def __init__(self, settings: Settings, client: httpx.Client, logger) -> None:
        self.settings = settings
        self.client = client
        self.logger = logger

    def fetch_company_overview(self, *, corp_code: str) -> dict[str, Any]:
        api_key = self.settings.providers.dart.api_key
        if not api_key:
            raise ProviderRequestError("dart", "/api/company.json", "Missing api_key")

        endpoint = "/api/company.json"
        response = request_with_retries(
            client=self.client,
            provider_name="dart",
            logger=self.logger,
            method="GET",
            url=f"{self.settings.providers.dart.base_url.rstrip('/')}{endpoint}",
            endpoint_label=endpoint,
            params={
                "crtfc_key": api_key,
                "corp_code": corp_code,
            },
        )

        payload = response.json()
        if payload.get("status") != "000":
            detail = payload.get("message") or payload.get("status") or "Unknown DART API error."
            raise ProviderRequestError("dart", endpoint, str(detail))

        raw_path = (
            self.settings.paths.raw_dir
            / "dart"
            / "company_overview"
            / f"date={today_local(self.settings.app.timezone).isoformat()}"
            / f"{corp_code}.json"
        )
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload["_raw_path"] = str(raw_path)
        return payload
