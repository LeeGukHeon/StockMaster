from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from shutil import disk_usage


class DiskWatermark(StrEnum):
    NORMAL = "normal"
    WARNING = "warning"
    PRUNE = "prune"
    LIMIT = "limit"


@dataclass(slots=True)
class DiskUsageReport:
    mount_point: Path
    total_gb: float
    used_gb: float
    available_gb: float
    usage_ratio: float
    status: DiskWatermark
    message: str


def classify_usage_ratio(
    usage_ratio: float,
    *,
    warning_ratio: float,
    prune_ratio: float,
    limit_ratio: float,
) -> DiskWatermark:
    if usage_ratio >= limit_ratio:
        return DiskWatermark.LIMIT
    if usage_ratio >= prune_ratio:
        return DiskWatermark.PRUNE
    if usage_ratio >= warning_ratio:
        return DiskWatermark.WARNING
    return DiskWatermark.NORMAL


def format_gb(value: float) -> str:
    return f"{value:.2f} GB"


def build_watermark_message(status: DiskWatermark, usage_ratio: float) -> str:
    percent = f"{usage_ratio:.1%}"
    if status == DiskWatermark.LIMIT:
        return f"Disk usage is {percent}. High-frequency collection should be reduced."
    if status == DiskWatermark.PRUNE:
        return f"Disk usage is {percent}. Pruning should run now."
    if status == DiskWatermark.WARNING:
        return f"Disk usage is {percent}. Monitor storage closely."
    return f"Disk usage is {percent}. Storage is within the normal range."


def measure_disk_usage(
    path: Path,
    *,
    warning_ratio: float,
    prune_ratio: float,
    limit_ratio: float,
) -> DiskUsageReport:
    usage = disk_usage(path)
    total_gb = usage.total / (1024**3)
    used_gb = usage.used / (1024**3)
    available_gb = usage.free / (1024**3)
    usage_ratio = usage.used / usage.total if usage.total else 0.0
    status = classify_usage_ratio(
        usage_ratio,
        warning_ratio=warning_ratio,
        prune_ratio=prune_ratio,
        limit_ratio=limit_ratio,
    )
    return DiskUsageReport(
        mount_point=path.resolve(),
        total_gb=total_gb,
        used_gb=used_gb,
        available_gb=available_gb,
        usage_ratio=usage_ratio,
        status=status,
        message=build_watermark_message(status, usage_ratio),
    )
