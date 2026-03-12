from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.settings import Settings


ARTIFACT_PATH_MARKERS: tuple[str, ...] = (
    "/runtime/data/artifacts/",
    "/data/artifacts/",
    "/runtime/artifacts/",
    "/artifacts/",
    "runtime/data/artifacts/",
    "data/artifacts/",
    "runtime/artifacts/",
    "artifacts/",
)


def artifact_candidate_paths(settings: Settings, path_value: object) -> list[Path]:
    if path_value in (None, ""):
        return []

    raw_text = str(path_value).strip()
    if not raw_text:
        return []

    raw_candidate = Path(raw_text)
    candidates: list[Path] = []
    if raw_candidate.is_absolute():
        candidates.append(raw_candidate)
    else:
        candidates.append(settings.paths.project_root / raw_candidate)
        candidates.append(settings.paths.artifacts_dir / raw_candidate)

    normalized = raw_text.replace("\\", "/")
    for marker in ARTIFACT_PATH_MARKERS:
        if marker not in normalized:
            continue
        suffix = normalized.split(marker, 1)[1].strip("/")
        if suffix:
            candidates.append(settings.paths.artifacts_dir / Path(suffix))
        break

    seen: set[Path] = set()
    unique_candidates: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_candidates.append(resolved)
    return unique_candidates


def resolve_artifact_path(
    settings: Settings,
    path_value: object,
    *,
    must_exist: bool = True,
) -> Path | None:
    candidates = artifact_candidate_paths(settings, path_value)
    if not candidates:
        return None
    if not must_exist:
        return candidates[0]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None
