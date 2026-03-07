from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree as ET

import httpx
import pandas as pd

from app.common.paths import ensure_directory
from app.common.time import today_local
from app.providers.base import ProviderRequestError, request_with_retries
from app.settings import Settings


@dataclass(slots=True)
class CorpCodesSnapshot:
    frame: pd.DataFrame
    raw_zip_path: str | None
    cache_path: str
    cached: bool


def parse_corp_code_xml_bytes(xml_bytes: bytes) -> pd.DataFrame:
    root = ET.fromstring(xml_bytes)
    rows: list[dict[str, str | None]] = []
    for node in root.findall(".//list"):
        rows.append(
            {
                "corp_code": (node.findtext("corp_code") or "").strip(),
                "corp_name": (node.findtext("corp_name") or "").strip(),
                "stock_code": (node.findtext("stock_code") or "").strip(),
                "modify_date": (node.findtext("modify_date") or "").strip(),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    stock_code = frame["stock_code"].fillna("").astype(str).str.strip()
    frame["stock_code"] = stock_code.where(stock_code.ne(""), None)
    frame.loc[frame["stock_code"].notna(), "stock_code"] = (
        frame.loc[frame["stock_code"].notna(), "stock_code"].astype(str).str.zfill(6)
    )
    frame["modify_date"] = pd.to_datetime(
        frame["modify_date"], format="%Y%m%d", errors="coerce"
    ).dt.date
    return frame.sort_values(["stock_code", "corp_code"], na_position="last").reset_index(drop=True)


class DartCorpCodeClient:
    def __init__(self, settings: Settings, client: httpx.Client, logger) -> None:
        self.settings = settings
        self.client = client
        self.logger = logger

    @property
    def cache_path(self) -> Path:
        return (
            ensure_directory(self.settings.paths.cache_dir / "dart") / "corp_codes_latest.parquet"
        )

    def load_cached(self) -> CorpCodesSnapshot | None:
        if not self.cache_path.exists():
            return None
        frame = pd.read_parquet(self.cache_path)
        return CorpCodesSnapshot(
            frame=frame,
            raw_zip_path=None,
            cache_path=str(self.cache_path),
            cached=True,
        )

    def download_corp_codes(self, *, force: bool = False) -> CorpCodesSnapshot:
        cached = self.load_cached()
        if cached is not None and not force:
            return cached

        api_key = self.settings.providers.dart.api_key
        if not api_key:
            raise ProviderRequestError("dart", "/api/corpCode.xml", "Missing api_key")

        endpoint = "/api/corpCode.xml"
        response = request_with_retries(
            client=self.client,
            provider_name="dart",
            logger=self.logger,
            method="GET",
            url=f"{self.settings.providers.dart.base_url.rstrip('/')}{endpoint}",
            endpoint_label=endpoint,
            params={"crtfc_key": api_key},
        )

        payload = response.content
        if payload.lstrip().startswith(b"<"):
            detail = payload.decode("utf-8", errors="ignore")[:500]
            raise ProviderRequestError("dart", endpoint, detail)

        run_date = today_local(self.settings.app.timezone)
        raw_zip_path = (
            self.settings.paths.raw_dir
            / "dart"
            / "corp_codes"
            / f"date={run_date.isoformat()}"
            / "corpCode.zip"
        )
        raw_zip_path.parent.mkdir(parents=True, exist_ok=True)
        raw_zip_path.write_bytes(payload)

        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            member_name = archive.namelist()[0]
            xml_bytes = archive.read(member_name)

        frame = parse_corp_code_xml_bytes(xml_bytes)
        frame.to_parquet(self.cache_path, index=False)
        return CorpCodesSnapshot(
            frame=frame,
            raw_zip_path=str(raw_zip_path),
            cache_path=str(self.cache_path),
            cached=False,
        )
