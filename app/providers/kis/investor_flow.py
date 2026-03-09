from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from app.common.time import today_local
from app.providers.base import ProviderRequestError, request_with_retries
from app.settings import Settings

from .auth import KisTokenManager

INVESTOR_FLOW_TR_ID = "FHPTJ04160001"
INVESTOR_FLOW_ENDPOINT = "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"


@dataclass(slots=True)
class InvestorFlowProbe:
    frame: pd.DataFrame
    payload: dict[str, Any]
    raw_json_path: str | None
    raw_parquet_path: str | None


class KisInvestorFlowClient:
    def __init__(
        self,
        settings: Settings,
        client,
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

    def fetch_investor_flow(
        self,
        *,
        symbol: str,
        trading_date: date | None = None,
        market_code: str = "J",
        adjusted_price_flag: str = "",
        extra_class_code: str = "",
        persist_probe_artifacts: bool = False,
    ) -> InvestorFlowProbe:
        requested_date = trading_date or today_local(self.settings.app.timezone)
        response = request_with_retries(
            client=self.client,
            provider_name="kis",
            logger=self.logger,
            method="GET",
            url=f"{self.base_url}{INVESTOR_FLOW_ENDPOINT}",
            endpoint_label=INVESTOR_FLOW_ENDPOINT,
            headers=self._kis_headers(INVESTOR_FLOW_TR_ID),
            params={
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_DATE_1": requested_date.strftime("%Y%m%d"),
                "FID_ORG_ADJ_PRC": adjusted_price_flag,
                "FID_ETC_CLS_CODE": extra_class_code,
            },
        )
        payload = response.json()
        payload["_response_headers"] = dict(response.headers)
        self._ensure_ok(payload, endpoint=INVESTOR_FLOW_ENDPOINT)

        output2 = payload.get("output2")
        output1 = payload.get("output1")
        if isinstance(output2, list) and output2:
            frame = pd.DataFrame(output2)
        elif isinstance(output2, dict):
            frame = pd.DataFrame([output2])
        elif isinstance(output1, list) and output1:
            frame = pd.DataFrame(output1)
        elif isinstance(output1, dict):
            frame = pd.DataFrame([output1])
        else:
            frame = pd.DataFrame()

        raw_json_path = None
        raw_parquet_path = None
        if persist_probe_artifacts:
            raw_dir = (
                self.settings.paths.raw_dir
                / "kis"
                / "investor_flow_probe"
                / f"trading_date={requested_date.isoformat()}"
                / f"symbol={symbol}"
            )
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_json_path = raw_dir / "payload.json"
            raw_json_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            raw_parquet_path = raw_dir / "payload.parquet"
            frame.to_parquet(raw_parquet_path, index=False)

        return InvestorFlowProbe(
            frame=frame,
            payload=payload,
            raw_json_path=str(raw_json_path) if raw_json_path is not None else None,
            raw_parquet_path=str(raw_parquet_path) if raw_parquet_path is not None else None,
        )
