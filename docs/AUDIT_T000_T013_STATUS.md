# AUDIT T000-T013 STATUS

- generated_at: `2026-03-09T02:12:53.085070+09:00`
- env: `local`
- duckdb_path: `D:\MyApps\StockMaster\data\marts\main.duckdb`
- PASS: `74`
- WARN: `0`
- FAIL: `0`

## contract

| status | priority | target | summary | remediation |
| --- | --- | --- | --- | --- |
| PASS | INFO | dim_symbol | 1 row per symbol / rows=4317 / duplicates=0 | none |
| PASS | INFO | dim_trading_calendar | 1 row per trading_date / rows=730 / duplicates=0 | none |
| PASS | INFO | fact_daily_ohlcv | 1 row per trading_date x symbol / rows=200 / duplicates=0 | none |
| PASS | INFO | fact_fundamentals_snapshot | 1 row per as_of_date x symbol / rows=235 / duplicates=0 | none |
| PASS | INFO | fact_news_item | 1 row per canonical news_id / rows=588 / duplicates=0 | none |
| PASS | INFO | fact_feature_snapshot | 1 row per as_of_date x symbol x feature_name / rows=211075 / duplicates=0 | none |
| PASS | INFO | fact_forward_return_label | 1 row per as_of_date x symbol x horizon / rows=303000 / duplicates=0 | none |
| PASS | INFO | fact_market_regime_snapshot | 1 row per as_of_date x market_scope / rows=12 / duplicates=0 | none |
| PASS | INFO | fact_investor_flow | 1 row per trading_date x symbol / rows=1096 / duplicates=0 | none |
| PASS | INFO | fact_prediction | 1 row per as_of_date x symbol x horizon x prediction_version / rows=40600 / duplicates=0 | none |
| PASS | INFO | fact_ranking | 1 row per as_of_date x symbol x horizon x ranking_version / rows=36350 / duplicates=0 | none |
| PASS | INFO | fact_selection_outcome | 1 row per selection_date x symbol x horizon x ranking_version / rows=36350 / duplicates=0 | none |
| PASS | INFO | fact_evaluation_summary | 1 row per summary_date x window_type x horizon x ranking_version x segment_type x segment_value / rows=108 / duplicates=0 | none |
| PASS | INFO | fact_calibration_diagnostic | 1 row per diagnostic_date x horizon x ranking_version x bin_type x bin_value / rows=11 / duplicates=0 | none |
| PASS | INFO | fact_model_training_run | 1 row per training_run_id / rows=18 / duplicates=0 | none |
| PASS | INFO | fact_model_member_prediction | 1 row per training_run_id x as_of_date x symbol x horizon x prediction_role x member_name / rows=31300 / duplicates=0 | none |
| PASS | INFO | fact_intraday_candidate_session | 1 row per session_date x symbol x horizon x ranking_version / rows=240 / duplicates=0 | none |
| PASS | INFO | fact_intraday_final_action | 1 row per session_date x symbol x horizon x checkpoint_time x ranking_version / rows=1200 / duplicates=0 | none |
| PASS | INFO | fact_intraday_meta_prediction | 1 row per session_date x symbol x horizon x checkpoint_time x ranking_version / rows=1200 / duplicates=0 | none |
| PASS | INFO | fact_intraday_meta_decision | 1 row per session_date x symbol x horizon x checkpoint_time x ranking_version / rows=1200 / duplicates=0 | none |
| PASS | INFO | fact_intraday_active_meta_model | 1 row per active_meta_model_id / rows=8 / duplicates=0 | none |
| PASS | INFO | fact_portfolio_target_book | 1 row per as_of_date x execution_mode x symbol / rows=15156 / duplicates=0 | none |
| PASS | INFO | fact_portfolio_rebalance_plan | 1 row per as_of_date x execution_mode x symbol / rows=15156 / duplicates=0 | none |
| PASS | INFO | fact_portfolio_position_snapshot | 1 row per snapshot_date x execution_mode x symbol / rows=6 / duplicates=0 | none |
| PASS | INFO | fact_portfolio_nav_snapshot | 1 row per snapshot_date x execution_mode x portfolio_policy_id x portfolio_policy_version / rows=6 / duplicates=0 | none |
| PASS | INFO | fact_job_run | 1 row per run_id / rows=49 / duplicates=0 | none |
| PASS | INFO | fact_job_step_run | 1 row per step_run_id / rows=78 / duplicates=0 | none |
| PASS | INFO | fact_health_snapshot | 1 row per snapshot_at x health_scope x component_name x metric_name / rows=105 / duplicates=0 | none |
| PASS | INFO | fact_latest_app_snapshot | 1 row per snapshot_id, canonical latest resolved by vw_latest_app_snapshot / rows=3 / duplicates=0 | none |
| PASS | INFO | fact_latest_report_index | 1 row per report_index_id, canonical latest resolved by report_type / rows=140 / duplicates=0 | none |
| PASS | INFO | fact_release_candidate_check | 1 row per release_candidate_check_id, canonical latest resolved by check_name / rows=146 / duplicates=0 | none |
| PASS | INFO | fact_ui_data_freshness_snapshot | 1 row per snapshot_ts x page_name x dataset_name / rows=57 / duplicates=0 | none |

## latest_layer

| status | priority | target | summary | remediation |
| --- | --- | --- | --- | --- |
| PASS | INFO | vw_latest_app_snapshot | vw_latest_app_snapshot rows=1 | none |
| PASS | INFO | vw_latest_report_index | vw_latest_report_index rows=13 | none |
| PASS | INFO | vw_latest_release_candidate_check | vw_latest_release_candidate_check rows=24 | none |
| PASS | INFO | vw_latest_ui_data_freshness_snapshot | vw_latest_ui_data_freshness_snapshot rows=19 | none |
| PASS | INFO | fact_latest_app_snapshot | canonical latest uniqueness duplicate_groups=0 | none |
| PASS | INFO | fact_latest_report_index | canonical latest uniqueness duplicate_groups=0 | none |
| PASS | INFO | fact_release_candidate_check | canonical latest uniqueness duplicate_groups=0 | none |
| PASS | INFO | fact_ui_data_freshness_snapshot | canonical latest uniqueness duplicate_groups=0 | none |
| PASS | INFO | fact_latest_app_snapshot | source consistency mismatches=0 | none |
| PASS | INFO | fact_latest_app_snapshot | active meta ids invalid=0 | none |
| PASS | INFO | active_intraday_policy_id | active_intraday_policy_id consistency=ok | none |
| PASS | INFO | active_portfolio_policy_id | active_portfolio_policy_id consistency=ok | none |
| PASS | INFO | active_ops_policy_id | active_ops_policy_id consistency=ok | none |
| PASS | INFO | fact_ui_data_freshness_snapshot | weekend/holiday false critical rows=0 | none |

## artifact_integrity

| status | priority | target | summary | remediation |
| --- | --- | --- | --- | --- |
| PASS | INFO | daily_discord_preview | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | daily_research_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | evaluation_postmortem_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | evaluation_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | intraday_meta_model_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | intraday_monitor_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | intraday_policy_research_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | intraday_postmortem_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | intraday_summary_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | ops_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | portfolio_report | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | release_candidate_checklist | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | t000_t013 | preview_exists=True payload_exists=True cleanup_safe=True | none |
| PASS | INFO | fact_latest_report_index | duplicate report_key groups=0 | none |

## ticket_coverage

| status | priority | target | summary | remediation |
| --- | --- | --- | --- | --- |
| PASS | INFO | T000 | Foundation / bootstrap / UI skeleton | none |
| PASS | INFO | T001 | Universe / calendar / provider activation | none |
| PASS | INFO | T002 | Core research ingestion | none |
| PASS | INFO | T003 | Feature store / labels / explanatory ranking | none |
| PASS | INFO | T004 | Flow / selection v1 / discord report | none |
| PASS | INFO | T005 | Postmortem evaluation / calibration | none |
| PASS | INFO | T006 | ML alpha v1 / selection v2 | none |
| PASS | INFO | T007 | Intraday candidate assist | none |
| PASS | INFO | T008 | Regime-aware intraday comparison | none |
| PASS | INFO | T009 | Intraday policy calibration framework | none |
| PASS | INFO | T010 | Intraday meta-model overlay | none |
| PASS | INFO | T011 | Integrated portfolio layer | none |
| PASS | INFO | T012 | Ops hardening / health dashboard | none |
| PASS | INFO | T013 | Final workflow / dashboard / release polish | none |
