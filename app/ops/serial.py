from __future__ import annotations

import json
import os
import shutil
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.common.paths import ensure_directory
from app.common.time import now_local
from app.settings import Settings


class SerialLockError(RuntimeError):
    pass


class SerialLockConflictError(SerialLockError):
    def __init__(
        self,
        message: str,
        *,
        lock_key: str,
        owner_run_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.lock_key = lock_key
        self.owner_run_id = owner_run_id
        self.details = details or {}


@dataclass(frozen=True, slots=True)
class SerialLockHandle:
    lock_key: str
    lock_dir: Path
    meta_path: Path
    acquired_at: datetime


def serial_lock_root(settings: Settings) -> Path:
    return ensure_directory(settings.paths.cache_dir / "scheduler_serial_locks")


def _lock_dir(settings: Settings, lock_key: str) -> Path:
    safe_key = str(lock_key).replace("/", "_").replace("\\", "_")
    return serial_lock_root(settings) / safe_key


def _read_lock_metadata(lock_dir: Path) -> dict[str, Any]:
    meta_path = lock_dir / "lock.json"
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _write_lock_metadata(lock_dir: Path, payload: dict[str, Any]) -> Path:
    meta_path = lock_dir / "lock.json"
    meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta_path


def _is_stale(metadata: dict[str, Any], *, stale_after_minutes: int, now_ts: datetime) -> bool:
    hostname = str(metadata.get("hostname") or "")
    pid = metadata.get("pid")
    if hostname and hostname == socket.gethostname() and pid not in (None, "", 0):
        try:
            os.kill(int(pid), 0)
        except (OSError, ValueError):
            return True
    acquired_at = metadata.get("acquired_at")
    if not acquired_at:
        return True
    try:
        acquired_dt = datetime.fromisoformat(str(acquired_at))
    except ValueError:
        return True
    if acquired_dt.tzinfo is None:
        acquired_dt = acquired_dt.astimezone()
    return now_ts - acquired_dt > timedelta(minutes=max(1, int(stale_after_minutes)))


def acquire_serial_lock(
    settings: Settings,
    *,
    lock_key: str,
    owner_run_id: str | None = None,
    job_name: str,
    stale_after_minutes: int = 120,
    details: dict[str, Any] | None = None,
) -> SerialLockHandle:
    lock_dir = _lock_dir(settings, lock_key)
    now_ts = now_local(settings.app.timezone)
    payload = {
        "lock_key": lock_key,
        "owner_run_id": owner_run_id,
        "job_name": job_name,
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "acquired_at": now_ts.isoformat(),
        "details": details or {},
    }
    try:
        lock_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        metadata = _read_lock_metadata(lock_dir)
        if _is_stale(metadata, stale_after_minutes=stale_after_minutes, now_ts=now_ts):
            shutil.rmtree(lock_dir, ignore_errors=True)
            try:
                lock_dir.mkdir(parents=True, exist_ok=False)
            except FileExistsError:
                refreshed_metadata = _read_lock_metadata(lock_dir)
                raise SerialLockConflictError(
                    f"Serial lock is already held for {lock_key}.",
                    lock_key=lock_key,
                    owner_run_id=refreshed_metadata.get("owner_run_id"),
                    details=refreshed_metadata,
                ) from None
        else:
            raise SerialLockConflictError(
                f"Serial lock is already held for {lock_key}.",
                lock_key=lock_key,
                owner_run_id=metadata.get("owner_run_id"),
                details=metadata,
            ) from None
    meta_path = _write_lock_metadata(lock_dir, payload)
    return SerialLockHandle(
        lock_key=lock_key,
        lock_dir=lock_dir,
        meta_path=meta_path,
        acquired_at=now_ts,
    )


def release_serial_lock(handle: SerialLockHandle) -> None:
    if handle.meta_path.exists():
        handle.meta_path.unlink()
    if handle.lock_dir.exists():
        shutil.rmtree(handle.lock_dir, ignore_errors=True)
