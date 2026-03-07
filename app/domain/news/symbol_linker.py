from __future__ import annotations

import json
import re
from dataclasses import dataclass

import pandas as pd

_TEXT_NORMALIZE_RE = re.compile(r"[\s\-\_,./()\[\]{}:;!?\"'`~|]+")
_CORP_SUFFIXES = ("(주)", "주식회사")


def normalize_news_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = str(value)
    for token in _CORP_SUFFIXES:
        normalized = normalized.replace(token, "")
    normalized = _TEXT_NORMALIZE_RE.sub("", normalized)
    return normalized.upper()


@dataclass(slots=True)
class SymbolLinkResult:
    symbols: list[str]
    match_method_json: str


def build_alias_index(symbol_frame: pd.DataFrame) -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = {}
    for row in symbol_frame.itertuples(index=False):
        symbol = str(row.symbol).zfill(6)
        company_name = str(row.company_name).strip()
        for alias in {company_name, normalize_news_text(company_name)}:
            alias_key = normalize_news_text(alias)
            if len(alias_key) < 3:
                continue
            aliases.setdefault(alias_key, []).append(symbol)
    return aliases


def link_news_item(
    *,
    symbol_frame: pd.DataFrame,
    title: str,
    snippet: str,
    query_symbol: str | None = None,
    query_company_name: str | None = None,
) -> SymbolLinkResult:
    alias_index = build_alias_index(symbol_frame)
    normalized_text = normalize_news_text(f"{title} {snippet}")
    matches: dict[str, str] = {}

    for alias, symbols in alias_index.items():
        if alias not in normalized_text:
            continue
        unique_symbols = sorted(set(symbols))
        if len(unique_symbols) == 1:
            matches[unique_symbols[0]] = "name_exact"

    if query_symbol and query_company_name:
        query_alias = normalize_news_text(query_company_name)
        if query_alias and query_alias in normalized_text and query_symbol not in matches:
            matches[query_symbol] = "query_context_exact"
        elif query_alias and not matches:
            matches[query_symbol] = "query_context_exact"

    return SymbolLinkResult(
        symbols=sorted(matches),
        match_method_json=json.dumps(matches, ensure_ascii=False, sort_keys=True),
    )
