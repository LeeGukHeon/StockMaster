from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx
import pandas as pd

from app.providers.base import ProviderRequestError, request_with_retries
from app.settings import Settings

REGULAR_REPORT_NAME_TO_CODE = {
    "사업보고서": "11011",
    "반기보고서": "11012",
    "분기보고서": "11013",
    "3분기보고서": "11014",
}

_PREFIX_RE = re.compile(r"^\[[^\]]+\]")
_PERIOD_RE = re.compile(r"\((\d{4})\.(\d{2})\)")


@dataclass(slots=True)
class DartDisclosureSnapshot:
    frame: pd.DataFrame
    payload: dict[str, Any]


@dataclass(slots=True)
class DartFinancialStatementSnapshot:
    frame: pd.DataFrame
    payload: dict[str, Any]


def parse_regular_report_metadata(report_name: str) -> dict[str, object] | None:
    normalized = _PREFIX_RE.sub("", report_name or "").strip()
    base_name = normalized.split("(")[0].strip()
    reprt_code = REGULAR_REPORT_NAME_TO_CODE.get(base_name)
    if not reprt_code:
        return None

    fiscal_year = None
    match = _PERIOD_RE.search(normalized)
    if match:
        fiscal_year = int(match.group(1))

    return {
        "report_name": report_name,
        "report_name_clean": normalized,
        "report_type_name": base_name,
        "reprt_code": reprt_code,
        "fiscal_year": fiscal_year,
    }


class DartFinancialClient:
    def __init__(self, settings: Settings, client: httpx.Client, logger) -> None:
        self.settings = settings
        self.client = client
        self.logger = logger

    @property
    def base_url(self) -> str:
        return self.settings.providers.dart.base_url.rstrip("/")

    def _api_key(self) -> str:
        api_key = self.settings.providers.dart.api_key
        if not api_key:
            raise ProviderRequestError("dart", "/api", "Missing api_key")
        return api_key

    def fetch_regular_disclosures(
        self,
        *,
        corp_code: str,
        start_date: date,
        end_date: date,
        page_count: int = 100,
    ) -> DartDisclosureSnapshot:
        endpoint = "/api/list.json"
        response = request_with_retries(
            client=self.client,
            provider_name="dart",
            logger=self.logger,
            method="GET",
            url=f"{self.base_url}{endpoint}",
            endpoint_label=endpoint,
            params={
                "crtfc_key": self._api_key(),
                "corp_code": corp_code,
                "bgn_de": start_date.strftime("%Y%m%d"),
                "end_de": end_date.strftime("%Y%m%d"),
                "pblntf_ty": "A",
                "page_count": str(page_count),
            },
        )
        payload = response.json()
        status = payload.get("status")
        if status == "013":
            return DartDisclosureSnapshot(frame=pd.DataFrame(), payload=payload)
        if status != "000":
            detail = payload.get("message") or payload.get("status") or "Unknown DART API error."
            raise ProviderRequestError("dart", endpoint, str(detail))

        frame = pd.DataFrame(payload.get("list", []))
        if frame.empty:
            return DartDisclosureSnapshot(frame=frame, payload=payload)

        parsed = frame["report_nm"].map(parse_regular_report_metadata)
        frame = frame.loc[parsed.notna()].copy()
        if frame.empty:
            return DartDisclosureSnapshot(frame=frame, payload=payload)

        metadata = pd.DataFrame(parsed.dropna().tolist(), index=frame.index)
        frame = pd.concat([frame, metadata], axis=1)
        frame["rcept_dt"] = pd.to_datetime(
            frame["rcept_dt"],
            format="%Y%m%d",
            errors="coerce",
        ).dt.date
        frame = frame.dropna(subset=["rcept_dt", "reprt_code", "fiscal_year"])
        frame = frame.sort_values(
            ["rcept_dt", "rcept_no"],
            ascending=[False, False],
        ).reset_index(drop=True)
        return DartDisclosureSnapshot(frame=frame, payload=payload)

    def fetch_financial_statement(
        self,
        *,
        corp_code: str,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ) -> DartFinancialStatementSnapshot:
        endpoint = "/api/fnlttSinglAcntAll.json"
        response = request_with_retries(
            client=self.client,
            provider_name="dart",
            logger=self.logger,
            method="GET",
            url=f"{self.base_url}{endpoint}",
            endpoint_label=endpoint,
            params={
                "crtfc_key": self._api_key(),
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )
        payload = response.json()
        status = payload.get("status")
        if status == "013":
            return DartFinancialStatementSnapshot(frame=pd.DataFrame(), payload=payload)
        if status != "000":
            detail = payload.get("message") or payload.get("status") or "Unknown DART API error."
            raise ProviderRequestError("dart", endpoint, str(detail))

        frame = pd.DataFrame(payload.get("list", []))
        return DartFinancialStatementSnapshot(frame=frame, payload=payload)
