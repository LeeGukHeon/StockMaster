from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from app.common.paths import ensure_directory
from app.common.time import today_local, utc_now
from app.logging import get_logger
from app.settings import Settings


@dataclass(slots=True)
class ProviderHealth:
    provider: str
    configured: bool
    status: str
    detail: str


class ProviderRequestError(RuntimeError):
    def __init__(self, provider: str, endpoint: str, detail: str) -> None:
        super().__init__(f"[{provider}] {endpoint}: {detail}")
        self.provider = provider
        self.endpoint = endpoint
        self.detail = detail


@dataclass(frozen=True, slots=True)
class ProviderRequestPolicy:
    min_interval_seconds: float = 0.0
    retries: int = 3
    retry_delay_seconds: float = 0.5
    transport_retry_delay_seconds: float | None = None
    retryable_status_codes: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    @property
    def transport_delay_seconds(self) -> float:
        return self.transport_retry_delay_seconds or self.retry_delay_seconds


_PROVIDER_REQUEST_POLICIES: dict[str, dict[str, ProviderRequestPolicy]] = {
    "kis": {
        "__default__": ProviderRequestPolicy(
            min_interval_seconds=0.15,
            retries=4,
            retry_delay_seconds=0.75,
            transport_retry_delay_seconds=1.0,
        ),
        "/oauth2/tokenP": ProviderRequestPolicy(
            min_interval_seconds=1.0,
            retries=3,
            retry_delay_seconds=1.0,
            transport_retry_delay_seconds=1.5,
        ),
        "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily": ProviderRequestPolicy(
            min_interval_seconds=0.25,
            retries=4,
            retry_delay_seconds=1.0,
            transport_retry_delay_seconds=1.25,
        ),
    },
    "dart": {
        "__default__": ProviderRequestPolicy(
            min_interval_seconds=0.5,
            retries=5,
            retry_delay_seconds=1.0,
            transport_retry_delay_seconds=1.5,
        ),
        "/api/corpCode.xml": ProviderRequestPolicy(
            min_interval_seconds=1.0,
            retries=4,
            retry_delay_seconds=1.25,
            transport_retry_delay_seconds=1.5,
        ),
        "/api/company.json": ProviderRequestPolicy(
            min_interval_seconds=0.75,
            retries=5,
            retry_delay_seconds=1.25,
            transport_retry_delay_seconds=1.75,
        ),
        "/api/list.json": ProviderRequestPolicy(
            min_interval_seconds=0.9,
            retries=5,
            retry_delay_seconds=1.5,
            transport_retry_delay_seconds=2.0,
        ),
        "/api/fnlttSinglAcntAll.json": ProviderRequestPolicy(
            min_interval_seconds=0.9,
            retries=5,
            retry_delay_seconds=1.5,
            transport_retry_delay_seconds=2.0,
        ),
    },
    "naver_news": {
        "__default__": ProviderRequestPolicy(
            min_interval_seconds=0.25,
            retries=4,
            retry_delay_seconds=0.75,
            transport_retry_delay_seconds=1.0,
        ),
        "/v1/search/news.json": ProviderRequestPolicy(
            min_interval_seconds=0.35,
            retries=4,
            retry_delay_seconds=1.0,
            transport_retry_delay_seconds=1.25,
        ),
    },
    "krx": {
        "__default__": ProviderRequestPolicy(
            min_interval_seconds=0.5,
            retries=4,
            retry_delay_seconds=1.0,
            transport_retry_delay_seconds=1.25,
        )
    },
}

_REQUEST_POLICY_LOCK = threading.Lock()
_LAST_REQUEST_TS: dict[tuple[str, str], float] = {}


def _response_detail(response: httpx.Response) -> str:
    try:
        detail = response.text
    except Exception:
        detail = "<response body unavailable>"
    return detail[:500]


def resolve_provider_request_policy(
    provider_name: str,
    endpoint_label: str,
    *,
    retries: int | None = None,
    retry_delay_seconds: float | None = None,
    retryable_status_codes: set[int] | None = None,
) -> ProviderRequestPolicy:
    provider_policies = _PROVIDER_REQUEST_POLICIES.get(provider_name, {})
    base_policy = provider_policies.get("__default__", ProviderRequestPolicy())
    resolved = provider_policies.get(endpoint_label, base_policy)
    return ProviderRequestPolicy(
        min_interval_seconds=resolved.min_interval_seconds,
        retries=retries if retries is not None else resolved.retries,
        retry_delay_seconds=(
            retry_delay_seconds if retry_delay_seconds is not None else resolved.retry_delay_seconds
        ),
        transport_retry_delay_seconds=resolved.transport_retry_delay_seconds,
        retryable_status_codes=frozenset(
            retryable_status_codes
            if retryable_status_codes is not None
            else resolved.retryable_status_codes
        ),
    )


def _wait_for_request_slot(
    provider_name: str,
    endpoint_label: str,
    policy: ProviderRequestPolicy,
) -> None:
    if policy.min_interval_seconds <= 0:
        return
    request_key = (provider_name, endpoint_label)
    with _REQUEST_POLICY_LOCK:
        now = time.monotonic()
        last_ts = _LAST_REQUEST_TS.get(request_key)
        if last_ts is not None:
            wait_seconds = (last_ts + policy.min_interval_seconds) - now
            if wait_seconds > 0:
                time.sleep(wait_seconds)
                now = time.monotonic()
        _LAST_REQUEST_TS[request_key] = now


def request_with_retries(
    *,
    client: httpx.Client,
    provider_name: str,
    logger,
    method: str,
    url: str,
    endpoint_label: str,
    retries: int = 3,
    retry_delay_seconds: float = 0.5,
    retryable_status_codes: set[int] | None = None,
    **request_kwargs: Any,
) -> httpx.Response:
    request_policy = resolve_provider_request_policy(
        provider_name,
        endpoint_label,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        retryable_status_codes=retryable_status_codes,
    )
    retryable = set(request_policy.retryable_status_codes)
    last_detail = "Request did not complete."

    for attempt in range(1, request_policy.retries + 1):
        try:
            _wait_for_request_slot(provider_name, endpoint_label, request_policy)
            response = client.request(method, url, **request_kwargs)
            if response.status_code in retryable and attempt < request_policy.retries:
                last_detail = _response_detail(response)
                logger.warning(
                    "Retrying provider request after retryable status.",
                    extra={
                        "provider": provider_name,
                        "endpoint": endpoint_label,
                        "attempt": attempt,
                        "status_code": response.status_code,
                    },
                )
                time.sleep(request_policy.retry_delay_seconds * attempt)
                continue
            response.raise_for_status()
            return response
        except (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.RemoteProtocolError,
        ) as exc:
            last_detail = str(exc)
            if attempt < request_policy.retries:
                logger.warning(
                    "Retrying provider request after transport error.",
                    extra={
                        "provider": provider_name,
                        "endpoint": endpoint_label,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )
                time.sleep(request_policy.transport_delay_seconds * attempt)
                continue
            raise ProviderRequestError(provider_name, endpoint_label, str(exc)) from exc
        except httpx.HTTPStatusError as exc:
            detail = _response_detail(exc.response)
            raise ProviderRequestError(provider_name, endpoint_label, detail) from exc

    raise ProviderRequestError(provider_name, endpoint_label, last_detail)


class BaseProvider:
    provider_name = "base"

    def __init__(self, settings: Settings, *, timeout: float = 10.0) -> None:
        self.settings = settings
        self.timeout = timeout
        self.client = httpx.Client(timeout=timeout, follow_redirects=True)
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

    def raw_operation_dir(self, operation: str, *, as_of_date: date | None = None) -> Path:
        date_value = as_of_date or today_local(self.settings.app.timezone)
        target = (
            self.settings.paths.raw_dir
            / self.provider_name
            / operation
            / f"date={date_value.isoformat()}"
        )
        return ensure_directory(target)

    def save_raw_json(
        self,
        operation: str,
        payload: dict[str, Any] | list[Any],
        *,
        file_stem: str | None = None,
        as_of_date: date | None = None,
    ) -> Path:
        target_dir = self.raw_operation_dir(operation, as_of_date=as_of_date)
        target_path = target_dir / f"{file_stem or utc_now().strftime('%Y%m%dT%H%M%S')}.json"
        target_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return target_path

    def save_raw_bytes(
        self,
        operation: str,
        payload: bytes,
        *,
        suffix: str,
        file_stem: str | None = None,
        as_of_date: date | None = None,
    ) -> Path:
        target_dir = self.raw_operation_dir(operation, as_of_date=as_of_date)
        target_path = target_dir / f"{file_stem or utc_now().strftime('%Y%m%dT%H%M%S')}{suffix}"
        target_path.write_bytes(payload)
        return target_path

    def save_raw_frame(
        self,
        operation: str,
        frame: pd.DataFrame,
        *,
        file_stem: str | None = None,
        as_of_date: date | None = None,
    ) -> Path:
        target_dir = self.raw_operation_dir(operation, as_of_date=as_of_date)
        target_path = target_dir / f"{file_stem or utc_now().strftime('%Y%m%dT%H%M%S')}.parquet"
        frame.to_parquet(target_path, index=False)
        return target_path

    def save_raw_payload(self, payload: dict[str, Any]) -> Path:
        return self.save_raw_json("stub_payload", payload)

    def request(
        self, method: str, url: str, *, endpoint_label: str, **request_kwargs: Any
    ) -> httpx.Response:
        return request_with_retries(
            client=self.client,
            provider_name=self.provider_name,
            logger=self.logger,
            method=method,
            url=url,
            endpoint_label=endpoint_label,
            **request_kwargs,
        )

    def close(self) -> None:
        self.client.close()
