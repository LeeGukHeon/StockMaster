from __future__ import annotations

from pathlib import Path
from typing import Mapping

import pandas as pd

from app.common.paths import ensure_directory
from app.common.time import utc_now


def build_partition_path(base_dir: Path, dataset: str, partitions: Mapping[str, str]) -> Path:
    path = base_dir / dataset
    for key, value in partitions.items():
        path /= f"{key}={value}"
    return path


def write_parquet(
    frame: pd.DataFrame,
    *,
    base_dir: Path,
    dataset: str,
    partitions: Mapping[str, str],
    mode: str = "overwrite",
    filename: str | None = None,
) -> Path:
    target_dir = build_partition_path(base_dir, dataset, partitions)
    ensure_directory(target_dir)
    if mode not in {"overwrite", "append"}:
        raise ValueError("mode must be either 'overwrite' or 'append'.")
    if mode == "overwrite":
        for existing in target_dir.glob("*.parquet"):
            existing.unlink()
    output_name = filename or f"{utc_now().strftime('%Y%m%dT%H%M%S')}.parquet"
    output_path = target_dir / output_name
    frame.to_parquet(output_path, index=False)
    return output_path
