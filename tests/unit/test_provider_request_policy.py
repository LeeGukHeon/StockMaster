from __future__ import annotations

import threading
import time

from app.providers import base as provider_base


def test_wait_for_request_slot_does_not_block_other_endpoints() -> None:
    policy = provider_base.ProviderRequestPolicy(min_interval_seconds=0.3)
    provider_base._LAST_REQUEST_TS.clear()
    provider_base._LAST_REQUEST_TS[("kis", "/endpoint-a")] = time.monotonic() + 0.3

    started = threading.Event()

    def hold_endpoint_a() -> None:
        started.set()
        provider_base._wait_for_request_slot("kis", "/endpoint-a", policy)

    worker = threading.Thread(target=hold_endpoint_a)
    worker.start()
    assert started.wait(timeout=1.0)
    time.sleep(0.05)

    other_started_at = time.perf_counter()
    provider_base._wait_for_request_slot("kis", "/endpoint-b", policy)
    other_elapsed = time.perf_counter() - other_started_at

    worker.join(timeout=1.0)
    assert other_elapsed < 0.1
