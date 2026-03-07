from __future__ import annotations

import io
import json
import zipfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import httpx
import pandas as pd

from app.common.time import today_local
from app.providers.base import ProviderRequestError, request_with_retries
from app.settings import Settings
from app.storage.parquet_io import write_parquet

from .auth import KisTokenManager

KIS_SYMBOL_MASTER_SPECS: dict[str, dict[str, Any]] = {
    "KOSPI": {
        "url": "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
        "tail_chars": 227,
        "field_widths": [
            2,
            1,
            4,
            4,
            4,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            9,
            5,
            5,
            1,
            1,
            1,
            2,
            1,
            1,
            1,
            2,
            2,
            2,
            3,
            1,
            3,
            12,
            12,
            8,
            15,
            21,
            2,
            7,
            1,
            1,
            1,
            1,
            1,
            9,
            9,
            9,
            5,
            9,
            8,
            9,
            3,
            1,
            1,
            1,
        ],
        "positions": {
            "group_code": 0,
            "sector_code": 2,
            "industry_code": 3,
            "subindustry_code": 4,
            "etp_flag": 12,
            "spac_flag": 19,
            "trading_halt_flag": 34,
            "liquidation_flag": 35,
            "management_flag": 36,
            "market_warning_flag": 37,
            "listing_date_raw": 49,
            "preferred_flag_raw": 54,
        },
    },
    "KOSDAQ": {
        "url": "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
        "tail_chars": 221,
        "field_widths": [
            2,
            1,
            4,
            4,
            4,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            9,
            5,
            5,
            1,
            1,
            1,
            2,
            1,
            1,
            1,
            2,
            2,
            2,
            3,
            1,
            3,
            12,
            12,
            8,
            15,
            21,
            2,
            7,
            1,
            1,
            1,
            1,
            9,
            9,
            9,
            5,
            9,
            8,
            9,
            3,
            1,
            1,
            1,
        ],
        "positions": {
            "group_code": 0,
            "sector_code": 2,
            "industry_code": 3,
            "subindustry_code": 4,
            "etp_flag": 8,
            "spac_flag": 14,
            "trading_halt_flag": 29,
            "liquidation_flag": 30,
            "management_flag": 31,
            "market_warning_flag": 32,
            "listing_date_raw": 44,
            "preferred_flag_raw": 49,
        },
    },
}


@dataclass(slots=True)
class SymbolMasterSnapshot:
    frame: pd.DataFrame
    artifact_paths: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DailyOhlcvProbe:
    frame: pd.DataFrame
    payload: dict[str, Any]
    raw_json_path: str
    raw_parquet_path: str


class KisMarketDataClient:
    def __init__(
        self,
        settings: Settings,
        client: httpx.Client,
        logger,
        token_manager: KisTokenManager,
    ) -> None:
        self.settings = settings
        self.client = client
        self.logger = logger
        self.token_manager = token_manager

    @property
    def base_url(self) -> str:
        return self.token_manager.base_url

    def _slice_fields(self, text: str, widths: list[int]) -> list[str]:
        values: list[str] = []
        cursor = 0
        for width in widths:
            values.append(text[cursor : cursor + width].strip())
            cursor += width
        return values

    def _parse_symbol_master_text(
        self, text: str, *, market: str, source_name: str
    ) -> pd.DataFrame:
        spec = KIS_SYMBOL_MASTER_SPECS[market]
        rows: list[dict[str, Any]] = []
        for raw_line in text.splitlines():
            if not raw_line.strip():
                continue
            left = raw_line[: len(raw_line) - spec["tail_chars"]]
            right = raw_line[-spec["tail_chars"] :]
            fields = self._slice_fields(right, spec["field_widths"])
            rows.append(
                {
                    "symbol": left[0:9].strip().zfill(6),
                    "standard_code": left[9:21].strip(),
                    "company_name": left[21:].strip(),
                    "market": market,
                    "group_code": fields[spec["positions"]["group_code"]],
                    "sector_code": fields[spec["positions"]["sector_code"]],
                    "industry_code": fields[spec["positions"]["industry_code"]],
                    "subindustry_code": fields[spec["positions"]["subindustry_code"]],
                    "etp_flag_raw": fields[spec["positions"]["etp_flag"]],
                    "spac_flag_raw": fields[spec["positions"]["spac_flag"]],
                    "trading_halt_flag_raw": fields[spec["positions"]["trading_halt_flag"]],
                    "liquidation_flag_raw": fields[spec["positions"]["liquidation_flag"]],
                    "management_flag_raw": fields[spec["positions"]["management_flag"]],
                    "market_warning_flag_raw": fields[spec["positions"]["market_warning_flag"]],
                    "listing_date_raw": fields[spec["positions"]["listing_date_raw"]],
                    "preferred_flag_raw": fields[spec["positions"]["preferred_flag_raw"]],
                    "source_file": source_name,
                }
            )
        return pd.DataFrame(rows)

    def _download_master_market(
        self, market: str, *, as_of_date: date
    ) -> tuple[pd.DataFrame, list[str]]:
        spec = KIS_SYMBOL_MASTER_SPECS[market]
        response = request_with_retries(
            client=self.client,
            provider_name="kis",
            logger=self.logger,
            method="GET",
            url=spec["url"],
            endpoint_label=spec["url"],
        )

        zip_name = spec["url"].split("/")[-1]
        raw_zip_path = (
            self.settings.paths.raw_dir
            / "kis"
            / "symbol_master_zip"
            / f"date={as_of_date.isoformat()}"
            / zip_name
        )
        raw_zip_path.parent.mkdir(parents=True, exist_ok=True)
        raw_zip_path.write_bytes(response.content)

        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            member_name = archive.namelist()[0]
            master_text = archive.read(member_name).decode("cp949")

        frame = self._parse_symbol_master_text(master_text, market=market, source_name=member_name)
        parsed_path = (
            self.settings.paths.raw_dir
            / "reference"
            / "symbol_master"
            / f"date={as_of_date.isoformat()}"
            / f"{market.lower()}_symbol_master.parquet"
        )
        parsed_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(parsed_path, index=False)
        return frame, [str(raw_zip_path), str(parsed_path)]

    def fetch_symbol_master(self, *, as_of_date: date | None = None) -> SymbolMasterSnapshot:
        run_date = as_of_date or today_local(self.settings.app.timezone)
        frames: list[pd.DataFrame] = []
        artifacts: list[str] = []
        for market in ("KOSPI", "KOSDAQ"):
            frame, market_artifacts = self._download_master_market(market, as_of_date=run_date)
            frames.append(frame)
            artifacts.extend(market_artifacts)

        combined = (
            pd.concat(frames, ignore_index=True)
            .sort_values(["market", "symbol"])
            .reset_index(drop=True)
        )
        combined_snapshot = write_parquet(
            combined,
            base_dir=self.settings.paths.raw_dir,
            dataset="reference/symbol_master_snapshot",
            partitions={"date": run_date.isoformat()},
            filename="symbol_master_combined.parquet",
        )
        artifacts.append(str(combined_snapshot))
        return SymbolMasterSnapshot(frame=combined, artifact_paths=artifacts)

    def _kis_headers(self, tr_id: str) -> dict[str, str]:
        token = self.token_manager.get_access_token()
        config = self.settings.providers.kis
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token.access_token}",
            "appkey": config.app_key or "",
            "appsecret": config.app_secret or "",
            "tr_id": tr_id,
            "custtype": "P",
        }

    def _ensure_ok(self, payload: dict[str, Any], *, endpoint: str) -> None:
        if payload.get("rt_cd") not in {None, "0"}:
            message = payload.get("msg1") or payload.get("msg_cd") or "Unknown KIS API error."
            raise ProviderRequestError("kis", endpoint, str(message))

    def fetch_current_quote(self, *, symbol: str, market_code: str = "J") -> dict[str, Any]:
        endpoint = "/uapi/domestic-stock/v1/quotations/inquire-price"
        response = request_with_retries(
            client=self.client,
            provider_name="kis",
            logger=self.logger,
            method="GET",
            url=f"{self.base_url}{endpoint}",
            endpoint_label=endpoint,
            headers=self._kis_headers("FHKST01010100"),
            params={
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_INPUT_ISCD": symbol,
            },
        )

        payload = response.json()
        self._ensure_ok(payload, endpoint=endpoint)
        raw_path = (
            self.settings.paths.raw_dir
            / "kis"
            / "current_quote_probe"
            / f"date={today_local(self.settings.app.timezone).isoformat()}"
            / f"{symbol}.json"
        )
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload["_raw_path"] = str(raw_path)
        return payload

    def fetch_daily_ohlcv(
        self,
        *,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
        market_code: str = "J",
    ) -> DailyOhlcvProbe:
        today = today_local(self.settings.app.timezone)
        start_value = start_date or (today - timedelta(days=30)).strftime("%Y%m%d")
        end_value = end_date or today.strftime("%Y%m%d")
        endpoint = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        response = request_with_retries(
            client=self.client,
            provider_name="kis",
            logger=self.logger,
            method="GET",
            url=f"{self.base_url}{endpoint}",
            endpoint_label=endpoint,
            headers=self._kis_headers("FHKST03010100"),
            params={
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_DATE_1": start_value,
                "FID_INPUT_DATE_2": end_value,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
        )

        payload = response.json()
        self._ensure_ok(payload, endpoint=endpoint)
        history = payload.get("output2", [])
        frame = pd.DataFrame(history)

        raw_json_path = (
            self.settings.paths.raw_dir
            / "kis"
            / "daily_ohlcv_probe"
            / f"date={today.isoformat()}"
            / f"{symbol}.json"
        )
        raw_json_path.parent.mkdir(parents=True, exist_ok=True)
        raw_json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        raw_parquet_path = (
            self.settings.paths.raw_dir
            / "kis"
            / "daily_ohlcv_probe"
            / f"date={today.isoformat()}"
            / f"{symbol}.parquet"
        )
        raw_parquet_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(raw_parquet_path, index=False)

        return DailyOhlcvProbe(
            frame=frame,
            payload=payload,
            raw_json_path=str(raw_json_path),
            raw_parquet_path=str(raw_parquet_path),
        )
