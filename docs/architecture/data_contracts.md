# Data Contracts

## `dim_symbol`

Primary key: `symbol`

Columns introduced in TICKET-001:

- `symbol`
- `company_name`
- `market`
- `market_segment`
- `sector`
- `industry`
- `listing_date`
- `security_type`
- `is_common_stock`
- `is_preferred_stock`
- `is_etf`
- `is_etn`
- `is_spac`
- `is_reit`
- `is_delisted`
- `is_trading_halt`
- `is_management_issue`
- `status_flags`
- `dart_corp_code`
- `dart_corp_name`
- `source`
- `as_of_date`
- `updated_at`

## `vw_universe_active_common_stock`

Filters:

- `market in ('KOSPI', 'KOSDAQ')`
- `is_common_stock = true`
- `is_etf = false`
- `is_etn = false`
- `is_spac = false`
- `is_reit = false`
- `is_delisted = false`

## `dim_trading_calendar`

Primary key: `trading_date`

Columns introduced in TICKET-001:

- `trading_date`
- `is_trading_day`
- `market_session_type`
- `weekday`
- `is_weekend`
- `is_public_holiday`
- `holiday_name`
- `source`
- `source_confidence`
- `is_override`
- `prev_trading_date`
- `next_trading_date`
- `updated_at`
