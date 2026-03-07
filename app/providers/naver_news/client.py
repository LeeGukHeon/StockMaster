from __future__ import annotations

from app.providers.base import BaseProvider


class NaverNewsProvider(BaseProvider):
    provider_name = "naver_news"

    def credential_map(self) -> dict[str, str | None]:
        config = self.settings.providers.naver_news
        return {
            "client_id": config.client_id,
            "client_secret": config.client_secret,
        }

    def search_news(self, *, query: str, limit: int = 10) -> dict[str, object]:
        return self.build_stub_payload("search_news", query=query, limit=limit)
