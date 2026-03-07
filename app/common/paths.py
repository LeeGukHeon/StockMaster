from __future__ import annotations

from pathlib import Path
from typing import Iterable


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_path(value: str | Path, base_dir: Path) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate.resolve()


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_directories(paths: Iterable[Path]) -> list[Path]:
    created: list[Path] = []
    for path in paths:
        if not path.exists():
            created.append(path)
        ensure_directory(path)
    return created
