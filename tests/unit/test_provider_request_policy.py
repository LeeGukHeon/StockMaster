from __future__ import annotations

import pytest

from app.providers import base


def test_resolve_provider_request_policy_prefers_endpoint_override() -> None:
    policy = base.resolve_provider_request_policy("dart", "/api/list.json")

    assert policy.min_interval_seconds == 0.9
    assert policy.retries == 5
    assert policy.retry_delay_seconds == 1.5
    assert policy.transport_delay_seconds == 2.0


def test_resolve_provider_request_policy_falls_back_to_provider_default() -> None:
    policy = base.resolve_provider_request_policy("krx", "/unknown-endpoint")

    assert policy.min_interval_seconds == 0.5
    assert policy.retries == 4


def test_wait_for_request_slot_enforces_minimum_spacing(monkeypatch) -> None:
    policy = base.ProviderRequestPolicy(min_interval_seconds=0.5)
    base._LAST_REQUEST_TS.clear()

    monotonic_values = iter([10.0, 10.1, 10.6])
    sleep_calls: list[float] = []

    monkeypatch.setattr(base.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(base.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    base._wait_for_request_slot("dart", "/api/list.json", policy)
    base._wait_for_request_slot("dart", "/api/list.json", policy)

    assert sleep_calls == pytest.approx([0.4])
