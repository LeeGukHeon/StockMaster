from __future__ import annotations

import json
from enum import StrEnum

import httpx

DISCORD_JSON_HEADERS = {"Content-Type": "application/json; charset=utf-8"}


class DiscordPublishDecision(StrEnum):
    PUBLISH = "publish"
    SKIP_DRY_RUN = "skip_dry_run"
    SKIP_DISABLED = "skip_disabled"
    SKIP_MISSING_WEBHOOK = "skip_missing_webhook"


def encode_discord_payload(message: dict[str, object]) -> bytes:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def resolve_discord_publish_decision(
    *,
    enabled: bool,
    webhook_url: str | None,
    dry_run: bool,
) -> DiscordPublishDecision:
    if not enabled:
        return DiscordPublishDecision.SKIP_DISABLED
    if dry_run:
        return DiscordPublishDecision.SKIP_DRY_RUN
    if not webhook_url:
        return DiscordPublishDecision.SKIP_MISSING_WEBHOOK
    return DiscordPublishDecision.PUBLISH


def publish_discord_messages(
    webhook_url: str,
    messages: list[dict[str, object]],
    *,
    timeout: float = 10.0,
    client: httpx.Client | None = None,
) -> list[dict[str, object]]:
    owns_client = client is None
    transport = client or httpx.Client(timeout=timeout)
    response_payloads: list[dict[str, object]] = []
    try:
        for index, message in enumerate(messages, start=1):
            response = transport.post(
                webhook_url,
                content=encode_discord_payload(message),
                headers=DISCORD_JSON_HEADERS,
            )
            response.raise_for_status()
            response_payloads.append(
                {
                    "message_index": index,
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                }
            )
    finally:
        if owns_client:
            transport.close()
    return response_payloads
