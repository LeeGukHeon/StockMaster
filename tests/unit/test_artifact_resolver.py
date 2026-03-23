from __future__ import annotations

from app.common.artifacts import artifact_candidate_paths, resolve_artifact_path
from tests._ticket003_support import build_test_settings


def test_resolve_artifact_path_maps_host_runtime_absolute_path(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    runtime_artifact = settings.paths.artifacts_dir / "models" / "alpha" / "model.pkl"
    runtime_artifact.parent.mkdir(parents=True, exist_ok=True)
    runtime_artifact.write_bytes(b"artifact")

    host_runtime_path = "/opt/stockmaster/runtime/data/artifacts/models/alpha/model.pkl"

    resolved = resolve_artifact_path(settings, host_runtime_path)

    assert resolved == runtime_artifact.resolve()


def test_resolve_artifact_path_maps_legacy_project_artifact_path(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    runtime_artifact = settings.paths.artifacts_dir / "reports" / "preview.md"
    runtime_artifact.parent.mkdir(parents=True, exist_ok=True)
    runtime_artifact.write_text("preview", encoding="utf-8")

    legacy_path = settings.paths.project_root / "data" / "artifacts" / "reports" / "preview.md"

    resolved = resolve_artifact_path(settings, str(legacy_path))

    assert resolved == runtime_artifact.resolve()


def test_resolve_artifact_path_resolves_relative_artifact_path(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    report_path = settings.paths.artifacts_dir / "reports" / "sample.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("sample", encoding="utf-8")

    resolved = resolve_artifact_path(settings, "reports/sample.md")

    assert resolved == report_path.resolve()


def test_artifact_candidate_paths_include_runtime_target_for_host_path(tmp_path) -> None:
    settings = build_test_settings(tmp_path)

    candidates = artifact_candidate_paths(
        settings,
        "/opt/stockmaster/runtime/data/artifacts/models/meta/model.pkl",
    )

    assert (settings.paths.artifacts_dir / "models" / "meta" / "model.pkl").resolve() in candidates


def test_resolve_artifact_path_maps_workspace_artifact_path(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    runtime_artifact = settings.paths.artifacts_dir / "reports" / "preview.md"
    runtime_artifact.parent.mkdir(parents=True, exist_ok=True)
    runtime_artifact.write_text("preview", encoding="utf-8")

    workspace_path = "/workspace/data/artifacts/reports/preview.md"

    resolved = resolve_artifact_path(settings, workspace_path)

    assert resolved == runtime_artifact.resolve()
