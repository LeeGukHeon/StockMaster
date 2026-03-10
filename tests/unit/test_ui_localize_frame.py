from __future__ import annotations

import pandas as pd

from app.ui.helpers import localize_frame


def test_localize_frame_formats_percent_like_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "expected_excess_return": 0.0123,
                "lower_band": -0.0345,
                "target_weight": 0.25,
                "execution_rate": 0.6,
                "final_selection_rank_pct": 0.125,
                "final_selection_value": 0.87,
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.loc[0, "예상 초과수익률"] == "1.23%"
    assert localized.loc[0, "하단 밴드"] == "-3.45%"
    assert localized.loc[0, "목표 비중"] == "25.00%"
    assert localized.loc[0, "실행 비율"] == "60.00%"
    assert localized.loc[0, "선택 상위 비율"] == "12.50%"
    assert localized.loc[0, "최종 선택 점수"] == 0.87
