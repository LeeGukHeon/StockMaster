from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from app.logging import get_logger
from app.settings import Settings


@dataclass(slots=True)
class ProviderHealth:
    provider: str
    configured: bool
    status: str
    detail: str


class BaseProvider:
    provider_name = "base"

    def __init__(self, settings: Settings, *, timeout: float = 10.0) -> None:
        self.settings = settings
        self.timeout = timeout
        self.client = httpx.Client(timeout=timeout)
        self.logger = get_logger(f"app.providers.{self.provider_name}")

    def credential_map(self) -> dict[str, str | None]:
        return {}

    def missing_credentials(self) -> list[str]:
        return [name for name, value in self.credential_map().items() if not value]

    def is_configured(self) -> bool:
        return not self.missing_credentials()

    def health_check(self) -> ProviderHealth:
        missing = self.missing_credentials()
        if missing:
            detail = f"Missing credentials: {', '.join(missing)}"
            return ProviderHealth(
                provider=self.provider_name,
                configured=False,
                status="missing_credentials",
                detail=detail,
            )
        return ProviderHealth(
            provider=self.provider_name,
            configured=True,
            status="placeholder_ok",
            detail="Provider skeleton is configured. Real API calls are pending.",
        )

    def build_stub_payload(self, operation: str, **params: Any) -> dict[str, Any]:
        return {
            "provider": self.provider_name,
            "operation": operation,
            "configured": self.is_configured(),
            "stub": True,
            "params": params,
        }

    def save_raw_payload(self, payload: dict[str, Any]) -> None:
        self.logger.debug("Skipping raw payload persistence for stub provider.", extra=payload)

    def close(self) -> None:
        self.client.close()
