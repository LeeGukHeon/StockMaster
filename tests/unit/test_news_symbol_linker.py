from __future__ import annotations

import json

import pandas as pd

from app.domain.news.symbol_linker import link_news_item


def test_link_news_item_matches_exact_company_name():
    symbol_frame = pd.DataFrame(
        [
            {"symbol": "005930", "company_name": "삼성전자"},
            {"symbol": "000660", "company_name": "SK하이닉스"},
        ]
    )

    result = link_news_item(
        symbol_frame=symbol_frame,
        title="삼성전자 반도체 투자 확대",
        snippet="메모리 업황 개선",
    )

    assert result.symbols == ["005930"]
    assert json.loads(result.match_method_json) == {"005930": "name_exact"}
