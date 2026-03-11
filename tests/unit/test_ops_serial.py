from __future__ import annotations

import json
from datetime import timedelta

from app.common.time import now_local
from app.ops.serial import acquire_serial_lock, release_serial_lock, serial_lock_root
from tests._ticket003_support import build_test_settings


def test_acquire_serial_lock_reclaims_same_host_dead_pid(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    lock_dir = serial_lock_root(settings) / "global_write"
    lock_dir.mkdir(parents=True, exist_ok=True)
    acquired_at = now_local(settings.app.timezone) - timedelta(minutes=1)
    (lock_dir / "lock.json").write_text(
        json.dumps(
            {
                "lock_key": "global_write",
                "owner_run_id": None,
                "job_name": "daily_close",
                "pid": 999999,
                "hostname": "same-host",
                "acquired_at": acquired_at.isoformat(),
                "details": {"identity": {"as_of_date": "2026-03-10"}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("app.ops.serial.socket.gethostname", lambda: "same-host")

    def _fake_kill(pid: int, sig: int) -> None:
        raise OSError("process not found")

    monkeypatch.setattr("app.ops.serial.os.kill", _fake_kill)

    handle = acquire_serial_lock(
        settings,
        lock_key="global_write",
        owner_run_id=None,
        job_name="daily_close",
        stale_after_minutes=120,
        details={"identity": {"as_of_date": "2026-03-11"}},
    )
    metadata = json.loads(handle.meta_path.read_text(encoding="utf-8"))
    release_serial_lock(handle)

    assert metadata["job_name"] == "daily_close"
    assert metadata["details"]["identity"]["as_of_date"] == "2026-03-11"
