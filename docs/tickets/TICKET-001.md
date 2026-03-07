# TICKET-001

Provider activation and reference-data sync.

Implemented in this ticket:

- KIS auth/token cache, symbol master download, quote probe, and daily OHLCV probe
- DART corpCode download/cache/parse and company overview probe
- `dim_symbol` upsert and `vw_universe_active_common_stock`
- `dim_trading_calendar` upsert with weekend/public-holiday/override logic
- Streamlit Home/Ops summaries for universe, calendar, and provider health

Still out of scope:

- Full OHLCV backfill
- Financial statement bulk ingestion
- News ingestion
- Ranking, evaluation, and report generation
