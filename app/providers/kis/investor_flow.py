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
INVESTOR_FLOW_DATE_KEYS = ("stck_bsop_date", "bsop_date", "base_dt", "date")


@dataclass(slots=True)
class InvestorFlowProbe:
    frame: pd.DataFrame
    payload: dict[str, Any]
    raw_json_path: str | None
    raw_parquet_path: str | None


def _coerce_probe_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _row_date_token(row: dict[str, Any]) -> str | None:
    for key in INVESTOR_FLOW_DATE_KEYS:
        raw_value = row.get(key)
        if raw_value in {None, ""}:
            continue
        if isinstance(raw_value, date):
            return raw_value.strftime("%Y%m%d")
        token = str(raw_value).strip()
        if len(token) == 8 and token.isdigit():
            return token
        digits = "".join(ch for ch in token if ch.isdigit())
        if len(digits) >= 8:
            return digits[:8]
    return None


def _select_requested_probe_rows(payload: dict[str, Any], *, requested_date: date) -> list[dict[str, Any]]:
    requested_token = requested_date.strftime("%Y%m%d")
    for output_key in ("output2", "output1"):
        rows = _coerce_probe_rows(payload.get(output_key))
        if not rows:
            continue
        date_tokens = [_row_date_token(row) for row in rows]
        if any(token is not None for token in date_tokens):
            return [
                row
                for row, token in zip(rows, date_tokens, strict=False)
                if token == requested_token
            ]
        return rows[-1:]
    return []


def _build_probe_frame(payload: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for output_key in ("output2", "output1"):
        rows = _coerce_probe_rows(payload.get(output_key))
        if rows:
            break
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


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

        selected_rows = _select_requested_probe_rows(payload, requested_date=requested_date)
        frame = pd.DataFrame(selected_rows) if selected_rows else pd.DataFrame()

        raw_json_path = None
        raw_parquet_path = None
        if persist_probe_artifacts:
            full_probe_frame = _build_probe_frame(payload)
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
            full_probe_frame.to_parquet(raw_parquet_path, index=False)

        return InvestorFlowProbe(
            frame=frame,
            payload=payload,
            raw_json_path=str(raw_json_path) if raw_json_path is not None else None,
            raw_parquet_path=str(raw_parquet_path) if raw_parquet_path is not None else None,
        )
