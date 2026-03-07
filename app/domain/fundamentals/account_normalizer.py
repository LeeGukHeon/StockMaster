from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

_LABEL_NORMALIZE_RE = re.compile(r"[\s\-\_,./()\[\]{}]")


def load_account_map(project_root: Path) -> dict[str, list[str]]:
    config_path = project_root / "config" / "fundamentals_account_map.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    accounts = data.get("accounts", {})
    return {key: list(value or []) for key, value in accounts.items()}


def normalize_account_label(value: str | None) -> str:
    if not value:
        return ""
    return _LABEL_NORMALIZE_RE.sub("", str(value)).upper()


def parse_numeric(value: object) -> float | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized in {"-", "nan", "NaN", "None"}:
        return None
    negative = normalized.startswith("(") and normalized.endswith(")")
    normalized = normalized.strip("()").replace(",", "")
    try:
        numeric = float(normalized)
    except ValueError:
        return None
    return -numeric if negative else numeric


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    if prepared.empty:
        return prepared
    prepared["account_key"] = prepared["account_nm"].map(normalize_account_label)
    prepared["amount_value"] = prepared["thstrm_amount"].map(parse_numeric)
    prepared["ord_value"] = pd.to_numeric(prepared.get("ord"), errors="coerce")
    return prepared.sort_values(["sj_div", "ord_value"], na_position="last")


def _pick_metric(
    frame: pd.DataFrame,
    *,
    candidates: list[str],
    statement_sections: set[str],
) -> tuple[float | None, str | None]:
    if frame.empty or not candidates:
        return None, None

    candidate_keys = {normalize_account_label(candidate) for candidate in candidates}
    matched = frame.loc[
        frame["sj_div"].isin(statement_sections) & frame["account_key"].isin(candidate_keys)
    ]
    matched = matched.loc[matched["amount_value"].notna()]
    if matched.empty:
        return None, None
    row = matched.iloc[0]
    return float(row["amount_value"]), str(row["account_nm"])


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return float((numerator / denominator) * 100.0)


def materialize_fundamentals_row(
    *,
    frame: pd.DataFrame,
    disclosure: dict[str, Any],
    as_of_date,
    symbol: str,
    project_root: Path,
    statement_basis: str,
) -> dict[str, Any] | None:
    prepared = _prepare_frame(frame)
    if prepared.empty:
        return None

    mapping = load_account_map(project_root)
    revenue, revenue_label = _pick_metric(
        prepared,
        candidates=mapping.get("revenue", []),
        statement_sections={"IS", "CIS"},
    )
    operating_income, operating_income_label = _pick_metric(
        prepared,
        candidates=mapping.get("operating_income", []),
        statement_sections={"IS", "CIS"},
    )
    net_income, net_income_label = _pick_metric(
        prepared,
        candidates=mapping.get("net_income", []),
        statement_sections={"IS", "CIS"},
    )
    equity, equity_label = _pick_metric(
        prepared,
        candidates=mapping.get("equity", []),
        statement_sections={"BS"},
    )
    liabilities, liabilities_label = _pick_metric(
        prepared,
        candidates=mapping.get("liabilities", []),
        statement_sections={"BS"},
    )

    notes = {
        "availability_rule": "disclosed_date_lte_as_of_date",
        "statement_basis": statement_basis,
        "metric_labels": {
            "revenue": revenue_label,
            "operating_income": operating_income_label,
            "net_income": net_income_label,
            "equity": equity_label,
            "liabilities": liabilities_label,
        },
    }

    return {
        "as_of_date": as_of_date,
        "symbol": symbol,
        "fiscal_year": int(disclosure["fiscal_year"]),
        "report_code": disclosure["reprt_code"],
        "revenue": revenue,
        "operating_income": operating_income,
        "net_income": net_income,
        "roe": _safe_ratio(net_income, equity),
        "debt_ratio": _safe_ratio(liabilities, equity),
        "operating_margin": _safe_ratio(operating_income, revenue),
        "source_doc_id": disclosure["rcept_no"],
        "source": "dart_fnlttSinglAcntAll",
        "disclosed_at": pd.Timestamp(disclosure["rcept_dt"]),
        "statement_basis": statement_basis,
        "report_name": disclosure["report_name_clean"],
        "currency": prepared["currency"].dropna().astype(str).iloc[0]
        if prepared["currency"].dropna().any()
        else None,
        "accounting_standard": None,
        "source_notes_json": json.dumps(notes, ensure_ascii=False),
    }
