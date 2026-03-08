from __future__ import annotations

from app.audit.contracts import CONTRACTS, CONTRACTS_BY_NAME, get_ticket_checklist


def test_contract_registry_contains_representative_tables() -> None:
    expected = {
        "dim_symbol",
        "fact_daily_ohlcv",
        "fact_prediction",
        "fact_intraday_final_action",
        "fact_portfolio_nav_snapshot",
        "fact_latest_app_snapshot",
        "fact_ui_data_freshness_snapshot",
    }
    assert expected.issubset(CONTRACTS_BY_NAME.keys())
    assert len(CONTRACTS) >= 30


def test_intraday_final_action_contract_is_view() -> None:
    contract = CONTRACTS_BY_NAME["fact_intraday_final_action"]
    assert contract.object_type == "view"
    assert "final_action" in contract.required_columns
    assert contract.unique_key == (
        "session_date",
        "symbol",
        "horizon",
        "checkpoint_time",
        "ranking_version",
    )


def test_ticket_checklist_covers_t000_to_t013() -> None:
    tickets = {item.ticket_id for item in get_ticket_checklist()}
    assert tickets == {
        "T000",
        "T001",
        "T002",
        "T003",
        "T004",
        "T005",
        "T006",
        "T007",
        "T008",
        "T009",
        "T010",
        "T011",
        "T012",
        "T013",
    }
