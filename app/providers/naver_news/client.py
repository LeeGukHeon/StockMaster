from __future__ import annotations

import re
from typing import Any

from app.providers.base import BaseProvider, ProviderHealth, ProviderRequestError

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(value: str | None) -> str:
    if not value:
        return ""
    return _HTML_TAG_RE.sub("", value).replace("&quot;", '"').replace("&amp;", "&").strip()


class NaverNewsProvider(BaseProvider):
    provider_name = "naver_news"

    def credential_map(self) -> dict[str, str | None]:
        config = self.settings.providers.naver_news
        return {
            "client_id": config.client_id,
            "client_secret": config.client_secret,
        }

    def health_check(self) -> ProviderHealth:
        missing = self.missing_credentials()
        if missing:
            return super().health_check()

        try:
            payload = self.search_news(query="KOSPI", limit=1, start=1)
        except Exception as exc:
            return ProviderHealth(
                provider=self.provider_name,
                configured=True,
                status="error",
                detail=str(exc),
            )

        return ProviderHealth(
            provider=self.provider_name,
            configured=True,
            status="ok",
            detail=f"Search API reachable. total={payload.get('total', 0)}",
        )

    def search_news(
        self,
        *,
        query: str,
        limit: int = 10,
        start: int = 1,
        sort: str = "date",
    ) -> dict[str, Any]:
        if not self.is_configured():
            raise ProviderRequestError("naver_news", "/v1/search/news.json", "Missing credentials")

        endpoint = "/v1/search/news.json"
        response = self.request(
            "GET",
            f"{self.settings.providers.naver_news.base_url.rstrip('/')}{endpoint}",
            endpoint_label=endpoint,
            headers={
                "X-Naver-Client-Id": self.settings.providers.naver_news.client_id or "",
                "X-Naver-Client-Secret": self.settings.providers.naver_news.client_secret or "",
            },
            params={
                "query": query,
                "display": min(max(limit, 1), 100),
                "start": min(max(start, 1), 1000),
                "sort": sort,
            },
        )
        payload = response.json()
        items = payload.get("items", [])
        for item in items:
            item["title_plain"] = strip_html(item.get("title"))
            item["description_plain"] = strip_html(item.get("description"))
        return payload
