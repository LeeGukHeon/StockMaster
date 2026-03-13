from __future__ import annotations

import json

import httpx

from app.common.discord import (
    DiscordPublishDecision,
    DISCORD_JSON_HEADERS,
    encode_discord_payload,
    publish_discord_messages,
    resolve_discord_publish_decision,
)


def test_encode_discord_payload_preserves_korean_text() -> None:
    payload = encode_discord_payload({"content": "한글 메시지"})
    decoded = payload.decode("utf-8")
    assert "한글 메시지" in decoded
    assert "\\u" not in decoded


def test_publish_discord_messages_uses_utf8_bytes() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        captured["content"] = request.content.decode("utf-8")
        return httpx.Response(204, request=request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    responses = publish_discord_messages(
        "https://discord.example/webhook",
        [{"content": "장마감 리포트 테스트"}],
        client=client,
    )
    client.close()

    assert responses[0]["status_code"] == 204
    assert captured["headers"]["content-type"] == DISCORD_JSON_HEADERS["Content-Type"]
    assert json.loads(str(captured["content"]))["content"] == "장마감 리포트 테스트"


def test_resolve_discord_publish_decision_covers_skip_and_publish_cases() -> None:
    assert (
        resolve_discord_publish_decision(
            enabled=False,
            webhook_url="https://discord.example/webhook",
            dry_run=False,
        )
        == DiscordPublishDecision.SKIP_DISABLED
    )
    assert (
        resolve_discord_publish_decision(
            enabled=True,
            webhook_url="https://discord.example/webhook",
            dry_run=True,
        )
        == DiscordPublishDecision.SKIP_DRY_RUN
    )
    assert (
        resolve_discord_publish_decision(
            enabled=True,
            webhook_url=None,
            dry_run=False,
        )
        == DiscordPublishDecision.SKIP_MISSING_WEBHOOK
    )
    assert (
        resolve_discord_publish_decision(
            enabled=True,
            webhook_url="https://discord.example/webhook",
            dry_run=False,
        )
        == DiscordPublishDecision.PUBLISH
    )
