from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml


@dataclass(slots=True)
class NewsQueryTask:
    query: str
    query_bucket: str
    is_market_wide: bool
    symbol: str | None = None
    company_name: str | None = None


def load_query_pack(project_root: Path, *, pack_name: str = "default") -> dict[str, object]:
    config_path = project_root / "config" / "news_queries.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    packs = data.get("packs", {})
    if pack_name not in packs:
        raise KeyError(f"Unknown news query pack: {pack_name}")
    return packs[pack_name]


def build_query_plan(
    *,
    project_root: Path,
    mode: str,
    focus_frame: pd.DataFrame,
    pack_name: str = "default",
) -> list[NewsQueryTask]:
    pack = load_query_pack(project_root, pack_name=pack_name)
    tasks: list[NewsQueryTask] = []

    if mode in {"market_only", "market_and_focus"}:
        for item in pack.get("market", []):
            tasks.append(
                NewsQueryTask(
                    query=str(item["keyword"]).strip(),
                    query_bucket=str(item["bucket"]).strip(),
                    is_market_wide=True,
                )
            )

    if mode in {"market_and_focus", "symbol_list"}:
        for row in focus_frame.itertuples(index=False):
            company_name = str(row.company_name).strip()
            if not company_name:
                continue
            tasks.append(
                NewsQueryTask(
                    query=company_name,
                    query_bucket="focus_symbol",
                    is_market_wide=False,
                    symbol=str(row.symbol).zfill(6),
                    company_name=company_name,
                )
            )

    deduped: list[NewsQueryTask] = []
    seen: set[tuple[str, str, str | None]] = set()
    for task in tasks:
        key = (task.query_bucket, task.query, task.symbol)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(task)
    return deduped
