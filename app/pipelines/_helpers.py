from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from app.common.paths import ensure_directory

_SLUG_RE = re.compile(r"[^A-Za-z0-9_-]+")


def iter_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def write_json_payload(path: Path, payload: dict[str, object] | list[object]) -> Path:
    ensure_directory(path.parent)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return path


def slugify(value: str, *, fallback: str = "query") -> str:
    slug = _SLUG_RE.sub("_", value.strip())
    slug = slug.strip("_")
    return slug[:60] or fallback


def load_symbol_frame(
    connection,
    *,
    symbols: list[str] | None = None,
    market: str = "ALL",
    limit_symbols: int | None = None,
    require_dart: bool = False,
) -> pd.DataFrame:
    explicit_symbols = [symbol.zfill(6) for symbol in symbols or []]
    query = (
        """
        SELECT symbol, company_name, market, dart_corp_code
        FROM dim_symbol
    """
        if explicit_symbols
        else """
        SELECT symbol, company_name, market, dart_corp_code
        FROM vw_universe_active_common_stock
    """
    )

    frame = connection.execute(query).fetchdf()
    if frame.empty:
        return frame

    frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    if explicit_symbols:
        order_map = {symbol: index for index, symbol in enumerate(explicit_symbols)}
        frame = frame.loc[frame["symbol"].isin(explicit_symbols)].copy()
        frame["__order"] = frame["symbol"].map(order_map)
        frame = frame.sort_values(["__order", "symbol"]).drop(columns=["__order"])
    else:
        frame = frame.sort_values("symbol")

    market_filter = market.upper()
    if market_filter != "ALL":
        frame = frame.loc[frame["market"].astype(str).str.upper() == market_filter]

    if require_dart:
        frame = frame.loc[
            frame["dart_corp_code"].notna() & frame["dart_corp_code"].astype(str).ne("")
        ]

    if limit_symbols is not None and limit_symbols > 0:
        frame = frame.head(limit_symbols)

    return frame.reset_index(drop=True)
