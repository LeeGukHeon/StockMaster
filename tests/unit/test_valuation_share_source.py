from __future__ import annotations

from app.domain.valuation.share_source import select_share_source


def test_selects_common_stock_row_when_preferred_rows_coexist() -> None:
    selection = select_share_source(
        [
            {
                "se": "우선주",
                "istc_totqy": "10",
                "tesstk_co": "0",
                "distb_stock_co": "9",
                "stlm_dt": "2024-12-31",
            },
            {
                "se": "보통주",
                "istc_totqy": "100",
                "tesstk_co": "5",
                "distb_stock_co": "95",
                "stlm_dt": "2024-12-31",
            },
        ]
    )

    assert selection.available
    assert selection.share_count == 100.0
    assert selection.denominator_basis == "issued_shares"
    assert selection.selected_se == "보통주"
    assert selection.istc_totqy == 100.0
    assert selection.tesstk_co == 5.0
    assert selection.distb_stock_co == 95.0
    assert selection.stlm_dt == "2024-12-31"
    assert selection.share_basis_pass


def test_uses_aggregate_row_only_with_withhold_reason() -> None:
    selection = select_share_source(
        [
            {
                "se": "합계",
                "istc_totqy": "1,000",
                "tesstk_co": "0",
                "distb_stock_co": "900",
                "stlm_dt": "2024-12-31",
            }
        ]
    )

    assert selection.share_count == 1000.0
    assert selection.denominator_basis == "issued_shares"
    assert "share_basis_aggregate" in selection.reasons
    assert not selection.share_basis_pass


def test_distributed_stock_only_records_float_based_withhold_reason() -> None:
    selection = select_share_source(
        [
            {
                "se": "보통주",
                "istc_totqy": "-",
                "tesstk_co": "0",
                "distb_stock_co": "900",
                "stlm_dt": "2024-12-31",
            }
        ]
    )

    assert selection.share_count == 900.0
    assert selection.denominator_basis == "distributed_stock"
    assert "share_denominator_float_based" in selection.reasons
    assert not selection.share_basis_pass
    assert selection.lineage()["denominator_basis"] == "distributed_stock"


def test_ambiguous_rows_make_share_count_unavailable() -> None:
    selection = select_share_source(
        [
            {"se": "종류주식A", "istc_totqy": "100"},
            {"se": "종류주식B", "istc_totqy": "200"},
        ]
    )

    assert not selection.available
    assert selection.reasons == ("ambiguous_share_class",)


def test_mixed_preferred_common_without_separable_common_count_withholds_labels() -> None:
    selection = select_share_source(
        [
            {"se": "우선주", "istc_totqy": "50"},
            {"se": "종류주식", "istc_totqy": "100"},
        ]
    )

    assert not selection.available
    assert "ambiguous_share_class" in selection.reasons
