from __future__ import annotations

import json

import httpx

from app.common.discord import (
    DISCORD_JSON_HEADERS,
    encode_discord_payload,
    publish_discord_messages,
)


def test_encode_discord_payload_preserves_korean_text() -> None:
    payload = encode_discord_payload({"content": "테스트 메시지 한글"})
    decoded = payload.decode("utf-8")
    assert "테스트 메시지 한글" in decoded
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
