from scripts.backfill_core_research_data import _is_empty_news_backfill_error


def test_is_empty_news_backfill_error_matches_expected_message():
    exc = RuntimeError("No news metadata rows were materialized for the requested signal date.")
    assert _is_empty_news_backfill_error(exc) is True


def test_is_empty_news_backfill_error_ignores_other_runtime_errors():
    exc = RuntimeError("Provider request failed with 500.")
    assert _is_empty_news_backfill_error(exc) is False
