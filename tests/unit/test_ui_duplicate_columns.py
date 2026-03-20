from __future__ import annotations

import pandas as pd

from app.ui.helpers import localize_frame


def test_localize_frame_deduplicates_duplicate_columns_after_label_translation() -> None:
    frame = pd.DataFrame(
        [["005930", "005930"]],
        columns=["symbol", "symbol"],
    )

    localized = localize_frame(frame)

    assert list(localized.columns) == ["종목코드", "종목코드 (2)"]
