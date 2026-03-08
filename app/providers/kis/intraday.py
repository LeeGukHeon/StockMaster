from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx
import pandas as pd

from app.common.time import today_local
from app.providers.base import ProviderRequestError, request_with_retries
from app.settings import Settings

from .auth import KisTokenManager


@dataclass(slots=True)
class IntradayBarsProbe:
    frame: pd.DataFrame
    payload: dict[str, Any]
    raw_json_path: str
    raw_parquet_path: str


@dataclass(slots=True)
class OrderbookSnapshotProbe:
    quote_frame: pd.DataFrame
    expected_trade_frame: pd.DataFrame
    payload: dict[str, Any]
    raw_json_path: str
    quote_parquet_path: str
    expected_trade_parquet_path: str


class KisIntradayClient:
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

    def fetch_intraday_bars(
        self,
        *,
        symbol: str,
        query_time: str = "153000",
        include_past: bool = True,
        market_code: str = "J",
        session_date: date | None = None,
    ) -> IntradayBarsProbe:
        endpoint = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        response = request_with_retries(
            client=self.client,
            provider_name="kis",
            logger=self.logger,
            method="GET",
            url=f"{self.base_url}{endpoint}",
            endpoint_label=endpoint,
            headers=self._kis_headers("FHKST03010200"),
            params={
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_HOUR_1": query_time,
                "FID_PW_DATA_INCU_YN": "Y" if include_past else "N",
                "FID_ETC_CLS_CODE": "",
            },
        )
        payload = response.json()
        self._ensure_ok(payload, endpoint=endpoint)
        frame = pd.DataFrame(payload.get("output2") or [])
        run_date = session_date or today_local(self.settings.app.timezone)

        raw_dir = (
            self.settings.paths.raw_dir
            / "kis"
            / "intraday_bar_1m_probe"
            / f"date={run_date.isoformat()}"
        )
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_json_path = raw_dir / f"{symbol}.json"
        raw_json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        raw_parquet_path = raw_dir / f"{symbol}.parquet"
        frame.to_parquet(raw_parquet_path, index=False)
        return IntradayBarsProbe(
            frame=frame,
            payload=payload,
            raw_json_path=str(raw_json_path),
            raw_parquet_path=str(raw_parquet_path),
        )

    def fetch_orderbook_snapshot(
        self,
        *,
        symbol: str,
        market_code: str = "J",
        session_date: date | None = None,
    ) -> OrderbookSnapshotProbe:
        endpoint = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
        response = request_with_retries(
            client=self.client,
            provider_name="kis",
            logger=self.logger,
            method="GET",
            url=f"{self.base_url}{endpoint}",
            endpoint_label=endpoint,
            headers=self._kis_headers("FHKST01010200"),
            params={
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_INPUT_ISCD": symbol,
            },
        )
        payload = response.json()
        self._ensure_ok(payload, endpoint=endpoint)
        run_date = session_date or today_local(self.settings.app.timezone)
        raw_dir = (
            self.settings.paths.raw_dir
            / "kis"
            / "orderbook_snapshot_probe"
            / f"date={run_date.isoformat()}"
        )
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_json_path = raw_dir / f"{symbol}.json"
        raw_json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        quote_frame = pd.DataFrame([payload.get("output1") or {}])
        expected_trade_frame = pd.DataFrame([payload.get("output2") or {}])
        quote_parquet_path = raw_dir / f"{symbol}_quote.parquet"
        expected_trade_parquet_path = raw_dir / f"{symbol}_expected_trade.parquet"
        quote_frame.to_parquet(quote_parquet_path, index=False)
        expected_trade_frame.to_parquet(expected_trade_parquet_path, index=False)
        return OrderbookSnapshotProbe(
            quote_frame=quote_frame,
            expected_trade_frame=expected_trade_frame,
            payload=payload,
            raw_json_path=str(raw_json_path),
            quote_parquet_path=str(quote_parquet_path),
            expected_trade_parquet_path=str(expected_trade_parquet_path),
        )
