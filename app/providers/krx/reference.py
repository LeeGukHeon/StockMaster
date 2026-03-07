from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.settings import Settings


class KrxReferenceAdapter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def seed_path(self) -> Path:
        return self.settings.paths.project_root / "config" / "seeds" / "symbol_master_seed.csv"

    def load_seed_fallback(self) -> pd.DataFrame:
        if not self.seed_path.exists():
            return pd.DataFrame(
                columns=["symbol", "sector", "industry", "market_segment", "source_note"]
            )
        frame = pd.read_csv(self.seed_path, dtype={"symbol": str})
        if frame.empty:
            return frame
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
        return frame
