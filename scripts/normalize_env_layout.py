# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _parse_env_lines(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def _needs_krx_label_reset(value: str | None) -> bool:
    if value is None:
        return True
    stripped = value.strip()
    if not stripped:
        return True
    if "?" in stripped:
        return True
    return False


def _skip_key(key: str, current: dict[str, str]) -> bool:
    metadata_enabled = str(current.get("METADATA_DB_ENABLED", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not metadata_enabled and key in {
        "METADATA_DB_POSTGRES_DB",
        "METADATA_DB_POSTGRES_USER",
        "METADATA_DB_POSTGRES_PASSWORD",
    }:
        return True
    return False


def normalize_env_file(*, target_path: Path, template_path: Path) -> None:
    current = _parse_env_lines(target_path)
    template_lines = template_path.read_text(encoding="utf-8").splitlines()
    output_lines: list[str] = []
    for line in template_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output_lines.append(line)
            continue
        key, template_value = line.split("=", 1)
        if _skip_key(key, current):
            continue
        value = current.get(key, template_value)
        if key == "KRX_SOURCE_ATTRIBUTION_LABEL" and _needs_krx_label_reset(value):
            value = "한국거래소 통계정보"
        output_lines.append(f"{key}={value}")
    target_path.write_text("\n".join(output_lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize an env file to the project template layout while preserving values."
    )
    parser.add_argument("--target", required=True)
    parser.add_argument("--template", required=True)
    args = parser.parse_args()

    normalize_env_file(
        target_path=(PROJECT_ROOT / args.target).resolve(),
        template_path=(PROJECT_ROOT / args.template).resolve(),
    )
    print(f"Normalized {args.target} using {args.template}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
