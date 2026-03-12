from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from app.common.paths import ensure_directory
from app.common.time import get_timezone, utc_now
from app.providers.base import ProviderRequestError, request_with_retries
from app.settings import Settings

KIS_MOCK_BASE_URL = "https://openapivts.koreainvestment.com:29443"


@dataclass(slots=True)
class KisAccessToken:
    access_token: str
    expires_at: datetime
    cache_path: Path
    source: str


def parse_kis_expiry(value: str | None, timezone_name: str) -> datetime:
    if not value:
        return utc_now() + timedelta(hours=6)

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y%m%d%H%M%S",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.replace(tzinfo=get_timezone(timezone_name)).astimezone(timezone.utc)
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return utc_now() + timedelta(hours=6)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=get_timezone(timezone_name))
    return parsed.astimezone(timezone.utc)


class KisTokenManager:
    def __init__(self, settings: Settings, client: httpx.Client, logger) -> None:
        self.settings = settings
        self.client = client
        self.logger = logger
        self._token_lock = threading.Lock()
        self._cached_token: KisAccessToken | None = None

    @property
    def cache_path(self) -> Path:
        return ensure_directory(self.settings.paths.cache_dir / "kis") / "access_token.json"

    @property
    def base_url(self) -> str:
        if self.settings.providers.kis.use_mock:
            return KIS_MOCK_BASE_URL
        return self.settings.providers.kis.base_url.rstrip("/")

    def load_cached_token(self) -> KisAccessToken | None:
        path = self.cache_path
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        access_token = payload.get("access_token")
        expires_at = parse_kis_expiry(payload.get("expires_at"), self.settings.app.timezone)
        if not access_token:
            return None
        if expires_at <= utc_now() + timedelta(minutes=5):
            return None
        return KisAccessToken(
            access_token=access_token,
            expires_at=expires_at,
            cache_path=path,
            source="cache",
        )

    def save_token(self, payload: dict[str, Any]) -> KisAccessToken:
        access_token = payload.get("access_token")
        if not access_token:
            raise ProviderRequestError(
                "kis", "/oauth2/tokenP", "Token response did not include access_token."
            )

        expires_at = parse_kis_expiry(
            payload.get("access_token_token_expired"),
            self.settings.app.timezone,
        )
        record = {
            "access_token": access_token,
            "expires_at": expires_at.isoformat(),
            "cached_at": utc_now().isoformat(),
            "environment": "mock" if self.settings.providers.kis.use_mock else "prod",
        }
        self.cache_path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        token = KisAccessToken(
            access_token=access_token,
            expires_at=expires_at,
            cache_path=self.cache_path,
            source="fresh",
        )
        self._cached_token = token
        return token

    def request_new_token(self) -> KisAccessToken:
        config = self.settings.providers.kis
        if not config.app_key or not config.app_secret:
            missing = [
                name
                for name, value in {
                    "app_key": config.app_key,
                    "app_secret": config.app_secret,
                }.items()
                if not value
            ]
            raise ProviderRequestError(
                "kis", "/oauth2/tokenP", f"Missing credentials: {', '.join(missing)}"
            )

        endpoint = f"{self.base_url}/oauth2/tokenP"
        response = request_with_retries(
            client=self.client,
            provider_name="kis",
            logger=self.logger,
            method="POST",
            url=endpoint,
            endpoint_label="/oauth2/tokenP",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": config.app_key,
                "appsecret": config.app_secret,
            },
        )
        return self.save_token(response.json())

    def get_access_token(self, *, force_refresh: bool = False) -> KisAccessToken:
        with self._token_lock:
            if (
                not force_refresh
                and self._cached_token is not None
                and self._cached_token.expires_at > utc_now() + timedelta(minutes=5)
            ):
                return self._cached_token
            if not force_refresh:
                cached = self.load_cached_token()
                if cached is not None:
                    self._cached_token = cached
                    return cached
            token = self.request_new_token()
            self._cached_token = token
            return token
