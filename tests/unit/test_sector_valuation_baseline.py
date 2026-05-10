from __future__ import annotations

import pandas as pd

from app.domain.valuation.peers import build_sector_valuation_baseline


def _peer_frame(peer_count: int = 8) -> pd.DataFrame:
    rows = [
        {
            "symbol": "000000",
            "sector": "Tech",
            "industry": "Semi",
            "per": 9.0,
            "pbr": 0.9,
            "is_common_stock": True,
        }
    ]
    for idx in range(peer_count):
        rows.append(
            {
                "symbol": f"000{idx + 1:03d}",
                "sector": "Tech",
                "industry": "Semi",
                "per": float(idx + 10),
                "pbr": float(idx + 1),
                "is_common_stock": True,
                "is_etf": False,
                "is_etn": False,
                "is_spac": False,
                "is_reit": False,
                "is_delisted": False,
                "is_trading_halt": False,
                "is_management_issue": False,
            }
        )
    return pd.DataFrame(rows)


def test_uses_sector_peer_group_and_excludes_target() -> None:
    baseline = build_sector_valuation_baseline(
        _peer_frame(8),
        target_symbol="000000",
        sector="Tech",
        industry="Semi",
    )

    assert baseline.group_type == "sector"
    assert baseline.peer_count == 8
    assert baseline.valid_pbr_count == 8
    assert baseline.median_pbr == 4.5
    assert baseline.pbr_percentile == 0.0


def test_excludes_unsupported_security_rows() -> None:
    frame = _peer_frame(8)
    frame.loc[1, "is_etf"] = True

    baseline = build_sector_valuation_baseline(frame, target_symbol="000000", sector="Tech")

    assert baseline.peer_count == 7
    assert "insufficient_peer_sample" in baseline.reasons


def test_falls_back_to_industry_when_sector_is_too_thin() -> None:
    frame = _peer_frame(8)
    frame.loc[5:, "sector"] = "Other"

    baseline = build_sector_valuation_baseline(
        frame,
        target_symbol="000000",
        sector="Tech",
        industry="Semi",
    )

    assert baseline.group_type == "industry"
    assert baseline.peer_count == 8


def test_low_metric_coverage_withholds() -> None:
    frame = _peer_frame(8)
    frame.loc[1:5, "per"] = None

    baseline = build_sector_valuation_baseline(frame, target_symbol="000000", sector="Tech")

    assert baseline.per_coverage < 0.60
    assert "low_peer_metric_coverage" in baseline.reasons
