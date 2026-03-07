from __future__ import annotations

import ast
from pathlib import Path

import app.ui.helpers as ui_helpers


def _helper_imports_from_file(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported_names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module != "app.ui.helpers":
            continue
        imported_names.update(alias.name for alias in node.names)
    return imported_names


def test_ui_pages_only_import_existing_helper_symbols() -> None:
    ui_root = Path(__file__).resolve().parents[2] / "app" / "ui"
    python_files = list(ui_root.rglob("*.py"))
    imported_names: set[str] = set()
    for path in python_files:
        imported_names.update(_helper_imports_from_file(path))

    missing = sorted(name for name in imported_names if not hasattr(ui_helpers, name))
    assert missing == []
