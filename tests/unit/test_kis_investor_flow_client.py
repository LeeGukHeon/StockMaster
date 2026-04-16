from __future__ import annotations

from datetime import date

from app.providers.kis.investor_flow import _select_requested_probe_rows


def test_select_requested_probe_rows_prefers_requested_trading_date() -> None:
    payload = {
        "output2": [
            {"stck_bsop_date": "20260414", "frgn_ntby_qty": "100"},
            {"stck_bsop_date": "20260415", "frgn_ntby_qty": "200"},
            {"stck_bsop_date": "20260416", "frgn_ntby_qty": "300"},
        ]
    }

    rows = _select_requested_probe_rows(payload, requested_date=date(2026, 4, 15))

    assert rows == [{"stck_bsop_date": "20260415", "frgn_ntby_qty": "200"}]


def test_select_requested_probe_rows_returns_empty_when_dated_rows_do_not_match() -> None:
    payload = {
        "output2": [
            {"stck_bsop_date": "20260414", "frgn_ntby_qty": "100"},
            {"stck_bsop_date": "20260413", "frgn_ntby_qty": "200"},
        ],
        "output1": {"stck_prpr": "0"},
    }

    rows = _select_requested_probe_rows(payload, requested_date=date(2026, 4, 15))

    assert rows == []


def test_select_requested_probe_rows_keeps_last_row_when_payload_has_no_date_key() -> None:
    payload = {
        "output2": [
            {"frgn_ntby_qty": "100"},
            {"frgn_ntby_qty": "250"},
        ]
    }

    rows = _select_requested_probe_rows(payload, requested_date=date(2026, 4, 15))

    assert rows == [{"frgn_ntby_qty": "250"}]
