# StockMaster

StockMaster is a Korea-focused personal stock research platform for post-market analysis, explanatory ranking, reporting, and retrospective evaluation.

Implemented through TICKET-017:

- foundation, settings, logging, bootstrap, and disk guard
- provider activation for KIS, DART, and Naver News
- reference data sync for `dim_symbol` and `dim_trading_calendar`
- core research ingestion for `fact_daily_ohlcv`, `fact_fundamentals_snapshot`, and `fact_news_item`
- feature store, forward return labels, market regime snapshot, and explanatory ranking v0
- investor flow ingestion, selection engine v1, calibrated proxy prediction bands, and Discord EOD report draft
- selection outcome freezing, cohort/rolling evaluation summaries, calibration diagnostics, and Discord postmortem draft
- ML alpha model v1, model-aware uncertainty/disagreement, and selection engine v2
- intraday candidate assist engine v1 with candidate-only 1m bars, trade summary, quote summary, and deterministic timing decisions
- intraday postmortem, regime-aware adjusted timing, strategy comparison, and timing calibration
- intraday policy calibration, walk-forward tuning, ablation analysis, recommendation registry, and manual active-policy freeze/rollback
- intraday policy meta-model / ML timing classifier v1 with panel-specific training, threshold calibration, bounded overlay scoring, and manual active-meta freeze/rollback
- integrated long-only portfolio candidate, allocation, rebalance, NAV, evaluation, and reporting layer
- ops stability layer with job/step metadata, dependency readiness, health snapshots, disk watermark tracking, recovery queue, active lock management, ops policy registry, and Health Dashboard
- final workflow polish with latest snapshot, report index, release candidate checks, Docs/Help, and Korean UI vocabulary
- OCI deployment assets with server compose, nginx reverse proxy, backup/runbook, and external access checklist
- DB contract audit / latest layer integrity checks / artifact reference validation
- host systemd timer based scheduler orchestration with serial lock discipline, scheduled news sync, daily close/evaluation, weekly candidate generation, and maintenance automation
- Streamlit Today, Ops, Health Dashboard, Research, Leaderboard, Market Pulse, Stock Workbench, Evaluation, Intraday Console, Portfolio Studio, Portfolio Evaluation, and Docs/Help pages

Out of scope:

- auto-trading, order routing, and execution
- full news article storage
- online-learning / RL intraday policy
- deep-learning / transformer / RL meta timing overlay
- full-market raw tick or websocket archival

The platform contains explanatory ranking, ML-assisted selection, and deterministic intraday timing support. It is still not an execution engine.

## Repository layout

```text
app/                application packages
config/             yaml configuration and local examples
data/               runtime storage roots (.gitkeep only)
docs/               architecture notes and ticket docs
scripts/            cli entrypoints
tests/              unit and integration tests
```

## Local setup

1. Create a virtual environment and install dependencies.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

2. Prepare environment variables.

```powershell
Copy-Item .env.example .env
```

3. Fill the provider credentials you actually have.

Required for live ingestion:

- `KIS_APP_KEY`
- `KIS_APP_SECRET`
- `DART_API_KEY`
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`

Used only when the related capability is enabled:

- `KIS_ACCOUNT_NO`
- `KIS_PRODUCT_CODE`
- `KIS_USE_MOCK`
- `KRX_API_KEY`
- `DISCORD_REPORT_ENABLED`
- `DISCORD_WEBHOOK_URL`

Notes:

- `KIS_USE_MOCK=false` uses the production KIS host.
- `KRX_API_KEY` is required only when `ENABLE_KRX_LIVE=true`.
- `ENABLE_KRX_LIVE=true` activates approved KRX OPEN API services only.
- `KRX_ALLOWED_SERVICES` must contain only the approved canonical service slugs.
- `DISCORD_REPORT_ENABLED=false` keeps Discord in render-only mode. Live publish requires both `DISCORD_REPORT_ENABLED=true` and `DISCORD_WEBHOOK_URL`.
- `DISCORD_WEBHOOK_URL` is optional for preview and dry-run workflows.

## OCI / server deployment

TICKET-014 adds a server-facing deployment bundle under `deploy/` and `scripts/server/`.

Key assets:

- `deploy/docker-compose.server.yml`
- `deploy/nginx/default.conf`
- `deploy/env/.env.server.example`
- `deploy/systemd/stockmaster-compose.service`
- `scripts/server/start_server.sh`
- `scripts/server/stop_server.sh`
- `scripts/server/restart_server.sh`
- `scripts/server/tail_server_logs.sh`
- `scripts/server/smoke_test_server.sh`
- `scripts/server/check_public_access.sh`
- `scripts/server/backup_server_data.sh`
- `scripts/server/print_runtime_info.sh`
- `docs/RUNBOOK_SERVER_OPERATIONS.md`
- `docs/SCHEDULER_AUTOMATION.md`
- `docs/METADATA_HOST_WORKER_VALIDATION.md`

Local vs server:

- local uses `.env`, direct Streamlit access, and optional source bind mounts
- server uses `deploy/env/.env.server`, nginx reverse proxy, persistent runtime volumes, restart policies, and explicit smoke/backup scripts
- local can expose `8501`; server should expose only `80` through nginx and keep `8501` internal

Server quick start:

```bash
cp deploy/env/.env.server.example deploy/env/.env.server
bash scripts/server/start_server.sh
bash scripts/server/smoke_test_server.sh
```

The server stack expects:

- code checkout under `/opt/stockmaster/app`
- runtime data under `/opt/stockmaster/runtime`
- backups under `/opt/stockmaster/backups`
- Docker Engine + Docker Compose plugin installed on the OCI instance
- canonical ops/dev/server runbook in `docs/RUNBOOK_SERVER_OPERATIONS.md`

## Scheduler automation

TICKET-017 adds host `systemd timer` based automation for server operation. Production scheduling is intentionally outside Streamlit and outside in-app APScheduler.

Key scheduler assets:

- `deploy/systemd/stockmaster-scheduler@.service`
- `deploy/systemd/stockmaster-ops-maintenance.timer`
- `deploy/systemd/stockmaster-news-morning.timer`
- `deploy/systemd/stockmaster-intraday-assist.timer`
- `deploy/systemd/stockmaster-news-after-close.timer`
- `deploy/systemd/stockmaster-evaluation.timer`
- `deploy/systemd/stockmaster-daily-close.timer`
- `deploy/systemd/stockmaster-daily-audit-lite.timer`
- `deploy/systemd/stockmaster-weekly-training.timer`
- `deploy/systemd/stockmaster-weekly-calibration.timer`
- `scripts/server/install_scheduler_units.sh`
- `scripts/server/uninstall_scheduler_units.sh`
- `scripts/server/status_scheduler_units.sh`
- `scripts/server/run_scheduler_job_host.sh`
- `docs/SCHEDULER_AUTOMATION.md`
- `docs/RUNBOOK_SERVER_OPERATIONS.md`

Initial schedule:

- ops maintenance: daily `02:30`
- morning news sync: Mon-Fri `08:30`
- intraday assist: Mon-Fri `08:55-15:15`, every 5 minutes
- after-close news sync: Mon-Fri `16:10`
- evaluation bundle: Mon-Fri `16:20`
- daily close bundle: Mon-Fri `18:40`
- daily audit lite: Mon-Fri `19:05`
- weekly training candidate: Sat `03:30`
- weekly calibration: Sat `06:30`

Automation rules:

- non-trading day => self-skip
- already completed identity => idempotent skip
- lock occupied => skip/defer
- missing upstream readiness => blocked/degraded
- date semantics are split explicitly:
  - `calendar_day`: morning news sync, after-close news sync, daily audit lite, ops maintenance
  - `trading_day`: intraday assist, evaluation bundle, daily close bundle
  - `hybrid`: weekly training candidate and weekly calibration run on calendar schedule but resolve the latest trading-day inputs internally
- weekly retrain/calibration results are generated automatically but never auto-applied
- active model/policy changes remain manual via UI compare-and-confirm or explicit freeze scripts

## Intraday research mode

TICKET-018 turns the existing intraday stack into a default-on research layer for
`server` / research-like environments.

Behavior:

- intraday assist, regime-adjusted policy, and meta-model overlay stay candidate-only
- raw action, adjusted action, and meta-model output are all persisted
- same-exit comparison and intraday lineage are queryable from the UI and DuckDB views
- intraday summary / postmortem / policy / meta-model reports are generated as research artifacts
- all intraday outputs remain explicitly non-trading and non-ordering

Safety boundaries that remain in force:

- no broker integration
- no automatic order execution
- no automatic policy promotion
- no automatic meta-model promotion

Useful commands:

```powershell
python scripts/validate_intraday_research_mode.py --as-of-date 2026-03-09
python scripts/smoke_intraday_research_mode.py
```

## KRX live integration

TICKET-019 / TICKET-020 activate KRX OPEN API as a live-first, fallback-second source for
exchange reference and market statistics.

Current approved canonical services:

- `stock_kospi_daily_trade`
- `stock_kosdaq_daily_trade`
- `stock_kospi_symbol_master`
- `stock_kosdaq_symbol_master`
- `index_krx_daily`
- `index_kospi_daily`
- `index_kosdaq_daily`
- `etf_daily_trade`

Rules:

- `ENABLE_KRX_LIVE=false` keeps KRX fully disabled
- `ENABLE_KRX_LIVE=true` requires `KRX_API_KEY`
- only services listed in `KRX_ALLOWED_SERVICES` are callable
- live failure never makes the whole platform fatal; it falls back to seed/existing paths where available
- KIS remains the operational intraday source
- KRX-driven statistics carry the source attribution label `한국거래소 통계정보`

Useful commands:

```powershell
python scripts/validate_krx_live_configuration.py
python scripts/krx_smoke_test.py --service-slug etf_daily_trade --as-of-date 2026-03-06
python scripts/krx_smoke_test_all_allowed.py --as-of-date 2026-03-06
python scripts/render_krx_service_status_report.py --as-of-date 2026-03-06 --dry-run
```

## Initial bootstrap and reference data

Run these first on a new machine:

```powershell
python scripts/bootstrap.py
python scripts/sync_trading_calendar.py --start 2025-01-01 --end 2026-12-31
python scripts/sync_universe.py
python scripts/provider_smoke_check.py --symbol 005930
```

## Core ingestion flow

TICKET-002 research ingestion:

```powershell
python scripts/sync_daily_ohlcv.py --date 2026-03-06 --limit-symbols 50
python scripts/sync_fundamentals_snapshot.py --as-of-date 2026-03-06 --limit-symbols 50
python scripts/sync_news_metadata.py --date 2026-03-06 --mode market_and_focus --limit-symbols 50
python scripts/sync_investor_flow.py --trading-date 2026-03-06 --limit-symbols 50
python scripts/backfill_core_research_data.py --start 2026-03-02 --end 2026-03-06 --limit-symbols 50
```

`python scripts/run_daily_pipeline.py` now orchestrates:

- OHLCV sync
- fundamentals snapshot sync
- news metadata sync
- investor flow sync
- feature store build
- market regime snapshot build
- explanatory ranking materialization
- selection engine v1 materialization
- proxy prediction calibration when enough labeled history exists
- Discord preview render on every run
- Discord publish only when `DISCORD_REPORT_ENABLED=true`

Forward labels and validation remain separate because they depend on future trading days. Proxy calibration is attempted inside the daily pipeline, but it is skipped gracefully until enough labeled history exists.

## TICKET-003 execution flow

Single-date build flow:

```powershell
python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100
python scripts/build_forward_labels.py --start 2026-03-02 --end 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/build_market_regime_snapshot.py --as-of-date 2026-03-06
python scripts/materialize_explanatory_ranking.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/validate_explanatory_ranking.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5
```

## TICKET-004 execution flow

Investor flow ingestion:

```powershell
python scripts/sync_investor_flow.py --trading-date 2026-03-06 --limit-symbols 100
python scripts/backfill_investor_flow.py --start 2026-02-17 --end 2026-03-06 --limit-symbols 100
```

Selection engine, proxy bands, and Discord:

```powershell
python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100
python scripts/materialize_selection_engine_v1.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/calibrate_proxy_prediction_bands.py --start 2026-01-05 --end 2026-03-06 --horizons 1 5
python scripts/render_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
python scripts/validate_selection_engine_v1.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5
```

Backfill note:

- `backfill_investor_flow.py` is date-loop orchestration over `sync_investor_flow.py`
- selection validation is meaningful only on dates where `fact_forward_return_label.label_available_flag = TRUE`
- calibrated proxy bands are attached to the latest available selection snapshot at or before the calibration end date
- `publish_discord_eod_report.py` always renders preview artifacts first, then optionally sends one or more Discord messages depending on payload length

## TICKET-005 execution flow

Outcome freezing, evaluation summary, calibration, and postmortem:

```powershell
python scripts/materialize_selection_outcomes.py --selection-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/backfill_selection_outcomes.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/materialize_prediction_evaluation.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --rolling-windows 20 60
python scripts/materialize_calibration_diagnostics.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --bin-count 10
python scripts/render_postmortem_report.py --evaluation-date 2026-03-13 --horizons 1 5 --dry-run
python scripts/publish_discord_postmortem_report.py --evaluation-date 2026-03-13 --horizons 1 5 --dry-run
python scripts/validate_evaluation_pipeline.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5
```

`python scripts/run_evaluation.py` now orchestrates:

- frozen selection outcome materialization
- cohort and rolling evaluation summary generation
- calibration diagnostic materialization
- postmortem preview render on every run
- postmortem Discord publish only when `DISCORD_REPORT_ENABLED=true`
- evaluation consistency validation

Selection snapshot freeze principle:

- evaluation never reuses today's score to judge yesterday's pick
- `fact_selection_outcome` freezes `fact_ranking` and `fact_prediction` fields at selection time
- realized outcome is then attached from `fact_forward_return_label`
- historical prediction snapshots are append-only backfills when missing; existing prediction rows are not overwritten during evaluation

UI:

```powershell
streamlit run app/ui/Home.py
```

Historical validation note:

- validation only works on dates where both `fact_ranking` and `fact_forward_return_label` exist
- the latest as-of date often has no available label yet because future closes are not known
- to validate a window, materialize historical feature/regime/ranking snapshots first

PowerShell example for historical ranking snapshots:

```powershell
foreach ($d in "2026-03-02","2026-03-03","2026-03-04","2026-03-05","2026-03-06") {
  python scripts/build_feature_store.py --as-of-date $d --limit-symbols 100
  python scripts/build_market_regime_snapshot.py --as-of-date $d
  python scripts/materialize_explanatory_ranking.py --as-of-date $d --horizons 1 5 --limit-symbols 100
}
python scripts/validate_explanatory_ranking.py --start 2026-03-02 --end 2026-03-06 --horizons 1 5
```

## Feature groups

Current `fact_feature_snapshot` groups:

- `price_trend`
- `volatility_risk`
- `liquidity_turnover`
- `fundamentals_quality`
- `value_safety`
- `news_catalyst`
- `investor_flow`
- `data_quality`

Representative features:

- price/trend: `ret_3d`, `ret_5d`, `ret_20d`, `ma5_over_ma20`, `ma20_over_ma60`, `drawdown_20d`
- volatility/risk: `realized_vol_20d`, `gap_abs_avg_20d`, `hl_range_1d`, `max_loss_20d`
- turnover/liquidity: `volume_ratio_1d_vs_20d`, `turnover_z_5_20`, `adv_20`, `adv_60`
- fundamentals/quality: `roe_latest`, `debt_ratio_latest`, `operating_margin_latest`, `days_since_latest_report`
- value/safety: `earnings_yield_proxy`, `low_debt_preference_proxy`, `profitability_support_proxy`
- news: `news_count_3d`, `distinct_publishers_3d`, `latest_news_age_hours`, `positive_catalyst_count_3d`
- investor flow: `foreign_net_value_ratio_1d`, `foreign_net_value_ratio_5d`, `institution_net_value_ratio_5d`, `smart_money_flow_ratio_20d`, `flow_alignment_score`, `flow_coverage_flag`
- data quality: `stale_price_flag`, `missing_key_feature_count`, `data_confidence_score`

## Feature calculation overview

- price and trend features come from `fact_daily_ohlcv`
- turnover features use `turnover_value` and fall back to `close * volume`
- fundamentals features come from the latest eligible DART snapshot at or before `as_of_date`
- news features use metadata only and conservative symbol linking
- investor flow features use KIS daily investor trend payloads normalized into `fact_investor_flow`
- if investor flow coverage is missing, raw flow features stay `NULL`; the pipeline does not zero-fill missing source coverage
- data quality features score whether price, fundamentals, and news are present and current
- all feature rows are materialized into `fact_feature_snapshot`
- cross-sectional `rank_pct` and `zscore` are computed within market (`KOSPI`, `KOSDAQ`)

## Investor flow coverage and null handling

- source: KIS `investor-trade-by-stock-daily`
- storage contract: `fact_investor_flow`
- coverage rule: if the requested trading date is missing or KIS does not return a usable flow field, the curated row is skipped and downstream flow features remain `NULL`
- the only explicit fill is `flow_coverage_flag`, which is `1.0` when same-day flow exists and `0.0` when it does not
- missing coverage is handled as neutral/unknown in scoring, then reflected through uncertainty and implementation penalties rather than fake zero flow

## Label definition

`fact_forward_return_label` uses explicit next-open logic:

- D+1: next trading day open -> same day close
- D+5: next trading day open -> close of the fifth trading session in the holding window
- baseline: same-market equal-weight average forward return
- excess label: symbol forward return minus same-market baseline

This is intentionally next-open based because a post-market research decision cannot assume same-day close execution without look-ahead bias.

## Realized outcome definitions

Pre-cost evaluation uses the same next-open logic as the label layer:

- realized return: `next trading day open -> future close`
- realized excess return: realized return minus same-market equal-weight baseline
- D+1 evaluation date: exit close of the next trading session
- D+5 evaluation date: exit close of the fifth trading session in the holding window

Band diagnostics use:

- `in_band`: realized excess return is between `lower_band` and `upper_band`
- `above_upper`: realized excess return is above `upper_band`
- `below_lower`: realized excess return is below `lower_band`
- `band_missing`: no frozen proxy band existed for that selection snapshot

## Regime state rules

`fact_market_regime_snapshot` is built for:

- `KR_ALL`
- `KOSPI`
- `KOSDAQ`

Current v0 regime states:

- `panic`
- `risk_off`
- `neutral`
- `risk_on`
- `euphoria`

Inputs used by the rule engine:

- breadth up ratio
- median symbol return 1d
- median symbol return 5d
- 20d market realized volatility
- turnover burst z-score
- 20d new high ratio
- 20d new low ratio

## Explanatory score v0

Active components:

- `trend_momentum_score`
- `turnover_participation_score`
- `quality_score`
- `value_safety_score`
- `news_catalyst_score`
- `regime_fit_score`
- `risk_penalty_score`

Reserved components:

- `flow_score`

`flow_score` is explicitly marked as reserved, not faked.

Weights differ by horizon:

- D+1 favors short-term trend, turnover, and news
- D+5 shifts more weight toward medium-term trend, quality, and value/safety

Stored outputs:

- `final_selection_value`
- `top_reason_tags_json`
- `risk_flags_json`
- `eligibility_notes_json`
- `explanatory_score_json`

## Selection engine v1

Selection engine v1 is a separate layer from explanatory ranking v0.

Active components:

- `trend_momentum_score`
- `turnover_participation_score`
- `quality_score`
- `value_safety_score`
- `news_catalyst_score`
- `flow_score`
- `regime_fit_score`
- `risk_penalty_score`
- `uncertainty_proxy_score`
- `implementation_penalty_score`

Reserved / nullable components:

- `disagreement_score = null` when no model-disagreement source exists

Definitions:

- `flow_score`: uses investor flow ratios, smart-money flow, and alignment between foreign/institution vs individual flow
- `uncertainty_proxy_score`: higher when realized volatility, gap behavior, or missing flow/data coverage suggests unstable outcomes
- `implementation_penalty_score`: higher when liquidity is thin, turnover is weak, price staleness exists, or flow coverage is missing

Difference from explanatory ranking v0:

- v0 is a human-readable inspection score with reserved `flow_score`
- v1 activates `flow_score` and adds uncertainty / implementation frictions to form a pragmatic selection layer
- neither v0 nor v1 is an ML alpha model

## Calibrated proxy prediction bands

`fact_prediction` stores:

- `expected_excess_return`
- `lower_band`
- `median_band`
- `upper_band`

These are calibrated historical proxies, not model forecasts.

Current rule:

- bucket latest selection rows by score decile
- look up historical excess-return distribution for the same horizon and score bucket
- prefer same-market calibration where available, otherwise fall back to `KR_ALL`
- store quartile-like proxy bands and sample size in `fact_prediction`

## Postmortem evaluation and calibration

`fact_selection_outcome` stores:

- frozen selection snapshot fields from `fact_ranking`
- frozen proxy prediction bands from `fact_prediction`
- realized return and realized excess return from `fact_forward_return_label`
- `band_status`, `prediction_error`, and outcome maturity state

`fact_evaluation_summary` stores:

- cohort rows by `selection_date`
- rolling rows such as `rolling_20d` and `rolling_60d`
- separate summaries for `selection_engine_v1` and `explanatory_ranking_v0`

Comparison rule:

- selection engine v1 and explanatory ranking v0 are evaluated separately
- comparison is done by aligning the same selection window and horizon, then comparing summary deltas

Rolling evaluation window:

- `rolling_20d`: latest 20 selection dates available in the requested range
- `rolling_60d`: latest 60 selection dates available in the requested range

Calibration diagnostics:

- only frozen rows with proxy bands participate
- `overall` rows show aggregate coverage and bias
- `expected_return_bin` rows show whether higher expected-return buckets realized in a monotonic order

Postmortem dry-run and publish:

```powershell
python scripts/render_postmortem_report.py --evaluation-date 2026-03-13 --horizons 1 5 --dry-run
python scripts/publish_discord_postmortem_report.py --evaluation-date 2026-03-13 --horizons 1 5 --dry-run
```

- render always writes preview artifacts under `data/artifacts/postmortem/...`
- publish failure is downgraded to a warning so the broader evaluation pipeline does not fail

## ML alpha model v1

ML alpha model v1 is the first supervised excess-return layer. It is a pragmatic,
sklearn-only baseline, not a claim of production-grade predictive power.

Purpose:

- learn symbol-level `excess_forward_return` from the curated feature store
- feed `selection_engine_v2` with a distinct alpha signal that is separate from explanatory ranking
- keep uncertainty, disagreement, and fallback rows explicit instead of hiding weak coverage

Limits:

- no LightGBM, XGBoost, SHAP, or deep-learning dependency is required on the default path
- no random-shuffle CV is used; splits are date-aware only
- evaluation remains pre-cost
- model outputs are point estimates plus calibrated residual bands, not structural probabilistic forecasts

### Excess-return label definition

The label definition remains the same as TICKET-003:

- entry return anchor: next trading-day open after `as_of_date`
- exit return anchor: future close on the D+1 or D+5 trading day
- raw forward return = `future_close / next_open - 1`
- excess forward return = raw forward return minus same-market equal-weight baseline for the same horizon

`next open -> future close` stays explicit because it better matches a realistic
post-close research workflow than a same-close-to-close shortcut.

### Train, validation, and OOF rules

- build the supervised dataset from `fact_feature_snapshot` joined to `fact_forward_return_label`
- train separate horizons for `D+1` and `D+5`
- use time-aware date splits only
- use the last `validation_days` distinct dates as validation, not random samples
- if history is thin, keep training but mark fallback reasons in the model registry
- `scripts/backfill_alpha_oof_predictions.py` rolls `train_end_date` over a historical range and stores validation predictions as time-aware OOF-style rows

### Base model family and ensemble weighting

Current member models:

- `ElasticNetCV` linear baseline, with `ElasticNet` fallback when the split is too narrow
- `HistGradientBoostingRegressor` boosting baseline
- `ExtraTreesRegressor` bagged-tree baseline

Ensemble weighting:

- compute validation `mae`
- use positive `corr` as a dampener when available
- convert inverse-error scores into normalized ensemble weights
- store weights in `fact_model_training_run.ensemble_weight_json`

### Uncertainty v1 and disagreement v1

`uncertainty_score`:

- derived from validation residual calibration
- each prediction is mapped into a residual bucket using validation-time predicted value ranges
- use expected absolute residual for the matched bucket
- convert that quantity into a percentile-like score for the current inference batch

`disagreement_score`:

- computed from the cross-member spread of `elasticnet`, `hist_gbm`, and `extra_trees`
- current implementation uses member prediction standard deviation
- convert the spread into a percentile-like score for the current inference batch

Uncertainty and disagreement are not the same thing:

- uncertainty measures calibrated residual instability
- disagreement measures cross-model spread

### Fallback policy

Fallback is explicit and stored row by row.

Current order:

1. use the latest successful `alpha_model_v1` artifact for the requested horizon
2. if no alpha artifact exists, fall back to `proxy_prediction_band_v1`
3. if neither alpha nor proxy prediction exists, emit a null alpha-prediction row with `fallback_flag = true`

`fallback_flag` and `fallback_reason` remain visible in `fact_prediction`,
`fact_selection_outcome`, and the UI.

### Selection engine v2

Selection engine v2 is different from explanatory ranking v0 and selection engine v1.

- v0: human-readable explanatory inspection layer
- v1: rule-based selection layer with active flow and implementation frictions
- v2: selection layer that adds a supervised alpha core plus model-aware penalties

Current v2 structure:

- positive side: `alpha_core_score`, `flow_score`, `trend_momentum_score`, `quality_score`, `value_safety_score`, `regime_fit_score`, `news_catalyst_score`
- penalty side: `risk_penalty_score`, `uncertainty_score`, `disagreement_score`, `implementation_penalty_score`, fallback penalty

High-level formula:

- `final_selection_value = weighted_positive_components - weighted_penalties`

The explanatory layer is still preserved in `explanatory_score_json`; it is not treated as a substitute for the ML alpha core.

### Model registry and artifact layout

Registry tables:

- `fact_model_training_run`
- `fact_model_member_prediction`
- `fact_model_metric_summary`

Key stored fields:

- model/version metadata
- train and validation windows
- feature count and ensemble weights
- validation metrics
- validation and inference member predictions
- fallback flags and reasons

Representative artifacts:

- training dataset parquet
- pickled sklearn artifact per horizon
- model validation summary
- selection engine comparison summary
- model diagnostic markdown report

### TICKET-006 commands

```powershell
python scripts/build_model_training_dataset.py --train-end-date 2026-03-06 --horizons 1 5 --min-train-days 120
python scripts/train_alpha_model_v1.py --train-end-date 2026-03-06 --horizons 1 5 --min-train-days 120 --validation-days 20
python scripts/backfill_alpha_oof_predictions.py --start-train-end-date 2026-02-14 --end-train-end-date 2026-03-06 --horizons 1 5 --limit-models 3
python scripts/materialize_alpha_predictions_v1.py --as-of-date 2026-03-06 --horizons 1 5
python scripts/materialize_selection_engine_v2.py --as-of-date 2026-03-06 --horizons 1 5
python scripts/validate_alpha_model_v1.py --as-of-date 2026-03-06 --horizons 1 5
python scripts/compare_selection_engines.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5
python scripts/render_model_diagnostic_report.py --train-end-date 2026-03-06 --horizons 1 5 --dry-run
```

## TICKET-007 intraday candidate assist

Intraday candidate session concept:

- the intraday layer is downstream of `selection_engine_v2`
- session candidates follow `selection_date -> next trading day session_date`
- this layer adjusts entry timing for the already-selected candidate list; it does not replace end-of-day selection
- this is not automated trading and it does not send orders

Candidate-only storage strategy:

- only the candidate session universe is stored
- there is no full-market intraday sweep
- no raw websocket/tick packet long-term archive is kept
- summary tables focus on candidate-only 1m bars, trade summary, quote summary, signal snapshots, and deterministic actions

Data storage principles:

- `fact_intraday_bar_1m`: candidate-only 1-minute bars
- `fact_intraday_trade_summary`: checkpoint-level execution-strength and activity summary
- `fact_intraday_quote_summary`: checkpoint-level orderbook summary, nullable when quote data is unavailable
- `fact_intraday_signal_snapshot`: rule-based signal family snapshot
- `fact_intraday_entry_decision`: deterministic action at each checkpoint
- `fact_intraday_timing_outcome`: naive-open versus timing-layer postmortem

Signal families:

- gap/opening quality
- VWAP and micro-trend
- relative volume and activity
- orderbook imbalance and spread
- execution strength
- risk, friction, and shock

Actions:

- `ENTER_NOW`: timing layer supports immediate entry
- `WAIT_RECHECK`: candidate stays alive but needs the next checkpoint
- `AVOID_TODAY`: timing layer vetoes the day
- `DATA_INSUFFICIENT`: data quality is too weak to support a timing call

Default checkpoints:

- `09:05`
- `09:15`
- `09:30`
- `10:00`
- `11:00`

Fallback and signal-quality policy:

- quote summary can stay null; that lowers signal quality and orderbook score instead of being zero-filled
- trade summary uses a 1m-bar proxy when direct trade summary is unavailable
- future sessions or missing same-day bars materialize explicit `unavailable` rows
- `DATA_INSUFFICIENT` stays visible in the decision table and UI

TTL and storage policy:

- candidate-only intraday parquet goes under `data/curated/intraday/...`
- raw live KIS intraday probes go under `data/raw/kis/...`
- proxy-generated intraday bars are marked `source=proxy_daily_ohlcv`
- retention follows existing intraday and orderbook retention settings in `config/retention.yaml`

Collector example:

```powershell
python scripts/run_intraday_candidate_collector.py --session-date 2026-03-09 --horizons 1 5 --poll-seconds 15 --dry-run
```

### TICKET-007 commands

```powershell
python scripts/materialize_intraday_candidate_session.py --selection-date 2026-03-06 --horizons 1 5 --max-candidates 30
python scripts/backfill_intraday_candidate_bars.py --session-date 2026-03-09 --horizons 1 5
python scripts/backfill_intraday_candidate_trade_summary.py --session-date 2026-03-09 --horizons 1 5
python scripts/backfill_intraday_candidate_quote_summary.py --session-date 2026-03-09 --horizons 1 5
python scripts/materialize_intraday_signal_snapshots.py --session-date 2026-03-09 --checkpoint 09:30 --horizons 1 5
python scripts/materialize_intraday_entry_decisions.py --session-date 2026-03-09 --checkpoint 09:30 --horizons 1 5
python scripts/evaluate_intraday_timing_layer.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5
python scripts/render_intraday_monitor_report.py --session-date 2026-03-09 --checkpoint 09:30 --dry-run
```

## TICKET-008 intraday postmortem and regime-aware comparison

Intraday market context:

- checkpoint-level market snapshot built only from the active candidate set plus candidate-only bars, trade summaries, and quote summaries
- captures breadth, candidate return dispersion, relative activity, spread, execution strength, shock proxy, and source coverage
- stored in `fact_intraday_market_context_snapshot`

Regime families:

- `PANIC_OPEN`
- `WEAK_RISK_OFF`
- `NEUTRAL_CHOP`
- `HEALTHY_TREND`
- `OVERHEATED_GAP_CHASE`
- `DATA_WEAK`

Adjustment profiles:

- `DEFENSIVE`
- `NEUTRAL`
- `SELECTIVE_RISK_ON`
- `GAP_CHASE_GUARD`
- `DATA_WEAK_GUARD`

Raw timing vs adjusted timing:

- raw timing comes from TICKET-007 deterministic checkpoint decisions in `fact_intraday_entry_decision`
- adjusted timing keeps the raw row frozen and writes regime-aware action changes into `fact_intraday_adjusted_entry_decision`
- `DATA_INSUFFICIENT -> ENTER_NOW` is forbidden
- `AVOID_TODAY -> ENTER_NOW` is not allowed in the default implementation

Selection v2 and intraday timing coupling:

- stock picking remains downstream of `selection_engine_v2`
- the intraday layer only changes entry timing for the already-selected candidate set
- there is no independent intraday stock picking universe

Strategy IDs:

- `SEL_V2_OPEN_ALL`
- `SEL_V2_TIMING_RAW_FIRST_ENTER`
- `SEL_V2_TIMING_ADJ_FIRST_ENTER`
- `SEL_V2_TIMING_ADJ_0930_ONLY`
- `SEL_V2_TIMING_ADJ_1000_ONLY`

Same-exit comparison rule:

- all strategy results reuse the same exit date and exit price from `fact_forward_return_label`
- only the entry timing changes
- this keeps the comparison centered on timing edge instead of mixing different exit policies

No-entry and skip diagnostics:

- `no_entry` rows are stored and evaluated, not dropped
- `skip_saved_loss_flag` tracks cases where skipping avoided a negative open-baseline result
- `missed_winner_flag` tracks cases where skipping missed a positive open-baseline result

Candidate-only storage and data-quality fallback:

- candidate-only intraday storage remains mandatory
- no full-market intraday sweep is added
- weak quote/trade coverage lowers signal quality and can push the adjusted layer into `WAIT_RECHECK`, `AVOID_TODAY`, or `DATA_INSUFFICIENT`
- market context, adjusted decisions, strategy results, and timing calibration all preserve explicit reason codes or quality flags

Dry-run and publish:

```powershell
python scripts/render_intraday_postmortem_report.py --session-date 2026-03-09 --horizons 1 5 --dry-run
python scripts/publish_discord_intraday_postmortem.py --session-date 2026-03-09 --horizons 1 5 --dry-run
```

- render always writes preview artifacts under `data/artifacts/intraday_postmortem/...`
- publish is optional and downgraded to a warning on failure

Current known limitations:

- the intraday timing layer is deterministic and rule-based, not ML-optimized
- future session dates can legitimately materialize sparse bars and `DATA_INSUFFICIENT` outcomes
- evaluation is still pre-cost and does not simulate execution slippage beyond entry-timing proxies

### TICKET-008 commands

```powershell
python scripts/materialize_intraday_market_context_snapshots.py --session-date 2026-03-09 --checkpoints 09:05 09:15 09:30 10:00 11:00
python scripts/materialize_intraday_regime_adjustments.py --session-date 2026-03-09 --checkpoints 09:05 09:15 09:30 10:00 11:00 --horizons 1 5
python scripts/materialize_intraday_adjusted_entry_decisions.py --session-date 2026-03-09 --checkpoint 09:30 --horizons 1 5
python scripts/materialize_intraday_decision_outcomes.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5
python scripts/evaluate_intraday_strategy_comparison.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5 --cutoff 11:00
python scripts/materialize_intraday_timing_calibration.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5
python scripts/render_intraday_postmortem_report.py --session-date 2026-03-09 --horizons 1 5 --dry-run
python scripts/publish_discord_intraday_postmortem.py --session-date 2026-03-09 --horizons 1 5 --dry-run
python scripts/validate_intraday_strategy_pipeline.py --session-date 2026-03-09 --horizons 1 5
```

## TICKET-009 intraday policy calibration and experiment framework

Intraday policy calibration purpose:

- tune the deterministic intraday timing layer without changing the stock universe or exit logic
- use only matured same-exit intraday outcomes as tuning input
- separate research recommendations from the active production policy registry

Matured-only tuning and same-exit rule:

- calibration, walk-forward, ablation, and recommendation scoring read only matured intraday outcomes
- raw and adjusted intraday decisions remain frozen; TICKET-009 does not overwrite them
- policy comparison still reuses the same exit date and exit price from the existing forward-label baseline

Policy templates and search space:

- templates: `BASE_DEFAULT`, `DEFENSIVE_LIGHT`, `DEFENSIVE_STRONG`, `RISK_ON_LIGHT`, `GAP_GUARD_STRICT`, `FRICTION_GUARD_STRICT`, `COHORT_GUARD_STRICT`, `FULL_BALANCED`
- scopes: `GLOBAL`, `HORIZON`, `HORIZON_CHECKPOINT`, `HORIZON_REGIME_CLUSTER`, `HORIZON_CHECKPOINT_REGIME_FAMILY`
- search space version `pcal_v1` expands the base templates across horizon, checkpoint, and regime-aware scopes
- parameters cover threshold deltas, confidence/signal/execution gates, uncertainty/spread/friction/gap/cohort/shock penalties, data-weak guard strength, and rank cap

Objective function overview:

- `objective_score` combines mean realized excess return, mean timing edge vs open, hit rate, execution rate, skip-saved-loss rate, and stability score
- it penalizes missed-winner rate, left-tail proxy, and manual-review-required candidates
- stability is derived from session-level dispersion of realized excess return, not a learned policy model

Walk-forward split rules:

- supported modes: `ANCHORED_WALKFORWARD`, `ROLLING_WALKFORWARD`
- default split example: train `40` sessions, validation `10`, test `10`, step `5`
- if sample is too small for a proper test window, recommendation logic can fall back to validation/all-style evidence and mark manual review when needed

Regime cluster and fallback structure:

- `RISK_OFF`: `PANIC_OPEN`, `WEAK_RISK_OFF`
- `NEUTRAL`: `NEUTRAL_CHOP`
- `RISK_ON`: `HEALTHY_TREND`, `OVERHEATED_GAP_CHASE`
- `DATA_WEAK`: `DATA_WEAK`
- family-level sample shortages can fall back to cluster scope, then horizon scope, then global scope, and the fallback source is stored explicitly

Recommendation vs active policy:

- recommendations are research outputs stored in `fact_intraday_policy_selection_recommendation`
- active policy state is stored separately in `fact_intraday_active_policy`
- auto-promotion is forbidden; activation happens only through explicit CLI freeze
- rollback is also explicit and recorded separately from recommendation generation

Freeze and rollback examples:

```powershell
python scripts/freeze_intraday_active_policy.py --as-of-date 2026-03-20 --promotion-type MANUAL_FREEZE --source latest_recommendation --note "Promote after review"
python scripts/rollback_intraday_active_policy.py --as-of-date 2026-03-24 --horizons 1 5 --note "Rollback due to weak execution stability"
```

Research report and Discord summary:

```powershell
python scripts/render_intraday_policy_research_report.py --as-of-date 2026-03-20 --horizons 1 5 --dry-run
python scripts/publish_discord_intraday_policy_summary.py --as-of-date 2026-03-20 --horizons 1 5 --dry-run
```

- render always writes preview artifacts under `data/artifacts/intraday_policy/...`
- publish is optional, dry-run safe, and warning-tolerant on failure

Current known limitations:

- policy tuning is still deterministic template search; there is no intraday ML/RL/online-learning policy
- promotion remains manual and conservative by design
- weak or sparse matured samples can force fallback scope usage and `manual_review_required_flag`
- candidate-only storage and same-exit comparison remain mandatory; no full-market intraday sweep is introduced

### TICKET-009 commands

```powershell
python scripts/materialize_intraday_policy_candidates.py --search-space-version pcal_v1 --horizons 1 5 --checkpoints 09:05 09:15 09:30 10:00 11:00 --scopes GLOBAL HORIZON HORIZON_CHECKPOINT HORIZON_REGIME_CLUSTER
python scripts/run_intraday_policy_calibration.py --start-session-date 2026-01-05 --end-session-date 2026-03-20 --horizons 1 5 --checkpoints 09:05 09:15 09:30 10:00 11:00 --objective-version ip_obj_v1 --split-version wf_40_10_10_step5 --search-space-version pcal_v1
python scripts/run_intraday_policy_walkforward.py --start-session-date 2026-01-05 --end-session-date 2026-03-20 --mode rolling --train-sessions 40 --validation-sessions 10 --test-sessions 10 --step-sessions 5 --horizons 1 5
python scripts/evaluate_intraday_policy_ablation.py --start-session-date 2026-01-05 --end-session-date 2026-03-20 --horizons 1 5 --base-policy-source latest_recommendation
python scripts/materialize_intraday_policy_recommendations.py --as-of-date 2026-03-20 --horizons 1 5 --minimum-test-sessions 10
python scripts/freeze_intraday_active_policy.py --as-of-date 2026-03-20 --promotion-type MANUAL_FREEZE --source latest_recommendation --note "Promote after review"
python scripts/rollback_intraday_active_policy.py --as-of-date 2026-03-24 --horizons 1 5 --note "Rollback due to weak execution stability"
python scripts/render_intraday_policy_research_report.py --as-of-date 2026-03-20 --horizons 1 5 --dry-run
python scripts/publish_discord_intraday_policy_summary.py --as-of-date 2026-03-20 --horizons 1 5 --dry-run
python scripts/validate_intraday_policy_framework.py --as-of-date 2026-03-20 --horizons 1 5
```

## TICKET-010 intraday policy meta-model / ML timing classifier v1

TICKET-010 adds a bounded ML overlay on top of the active intraday policy. It does not replace:

- selection engine v2 candidate generation
- the active deterministic intraday policy
- hard guards such as `AVOID_TODAY` or `DATA_INSUFFICIENT`

The overlay only operates on matured intraday snapshots and only on admissible panels:

- `ENTER_PANEL`
- `WAIT_PANEL`

Class system:

- `ENTER_PANEL`: `KEEP_ENTER`, `DOWNGRADE_WAIT`, `DOWNGRADE_AVOID`
- `WAIT_PANEL`: `KEEP_WAIT`, `UPGRADE_ENTER`, `DOWNGRADE_AVOID`

Base model family:

- `LogisticRegression`
- `HistGradientBoostingClassifier`
- `ExtraTreesClassifier`

The ensemble is a conservative sklearn-only soft-voting stack. Probability calibration uses sigmoid-style per-class calibration. Uncertainty and disagreement are proxy measures built from confidence margin and member dispersion, not Bayesian estimates.

### Matured-only tuning and as-of discipline

- training and evaluation use only matured same-exit intraday outcomes
- live scoring reads only already-materialized intraday snapshots for that checkpoint
- the meta-model cannot create independent actions outside the tuned policy action space
- `AVOID_TODAY` and `DATA_INSUFFICIENT` are never upward-overridden

### Final action rule

- adjusted `ENTER_NOW` can only become `ENTER_NOW`, `WAIT_RECHECK`, or `AVOID_TODAY`
- adjusted `WAIT_RECHECK` can only become `WAIT_RECHECK`, `ENTER_NOW`, or `AVOID_TODAY`
- adjusted `AVOID_TODAY` / `DATA_INSUFFICIENT` stay as-is
- low confidence, low margin, high uncertainty, high disagreement, missing model, or missing artifact all fall back to the tuned policy action and store a fallback reason

### Recommendation, freeze, and rollback

- training output is stored in the generic model registry with `model_domain = intraday_meta`
- active meta-model state is separate from training output
- promotion is manual only
- rollback is explicit and scope-safe

### TICKET-010 commands

```powershell
python scripts/build_intraday_meta_training_dataset.py --start-session-date 2026-03-03 --end-session-date 2026-03-09 --horizons 1 5
python scripts/validate_intraday_meta_dataset.py --start-session-date 2026-03-03 --end-session-date 2026-03-09 --horizons 1 5
python scripts/train_intraday_meta_models.py --train-end-date 2026-03-09 --horizons 1 5 --start-session-date 2026-03-03 --validation-sessions 1
python scripts/run_intraday_meta_walkforward.py --start-session-date 2026-03-03 --end-session-date 2026-03-09 --mode rolling --train-sessions 3 --validation-sessions 1 --test-sessions 1 --step-sessions 1 --horizons 1 5
python scripts/calibrate_intraday_meta_thresholds.py --as-of-date 2026-03-09 --horizons 1 5
python scripts/evaluate_intraday_meta_models.py --start-session-date 2026-03-03 --end-session-date 2026-03-09 --horizons 1 5
python scripts/freeze_intraday_active_meta_model.py --as-of-date 2026-03-09 --source latest_training --note \"Freeze after review\" --horizons 1 5
python scripts/materialize_intraday_meta_predictions.py --session-date 2026-03-09 --horizons 1 5
python scripts/materialize_intraday_final_actions.py --session-date 2026-03-09 --horizons 1 5
python scripts/render_intraday_meta_model_report.py --as-of-date 2026-03-09 --horizons 1 5 --dry-run
python scripts/publish_discord_intraday_meta_summary.py --as-of-date 2026-03-09 --horizons 1 5 --dry-run
python scripts/validate_intraday_meta_model_framework.py --as-of-date 2026-03-09 --horizons 1 5
```

## TICKET-011 integrated portfolio / capital allocation / risk budget

TICKET-011 adds a deterministic long-only portfolio proposal layer downstream of selection engine v2 and the intraday timing overlay.

Execution modes:

- `OPEN_ALL`: enter all eligible names without timing gating
- `TIMING_ASSISTED`: gate new entries and adds with intraday final action, while hold/trim/exit still follow deterministic portfolio rules

Policy configs:

- [balanced_long_only_v1.yaml](/d:/MyApps/StockMaster/config/portfolio_policies/balanced_long_only_v1.yaml)
- [defensive_long_only_v1.yaml](/d:/MyApps/StockMaster/config/portfolio_policies/defensive_long_only_v1.yaml)

Allocation flow:

1. Build `fact_portfolio_candidate` from selection v2, alpha bands, flow, regime, and intraday final action.
2. Compute `effective_alpha_long` and `risk_scaled_conviction`.
3. Rank candidates with explicit tie-break order:
   `current_holding_flag DESC -> candidate_rank ASC -> symbol ASC`
4. Allocate deterministic target weights with regime-aware cash, single-name cap, sector cap, KOSDAQ cap, turnover cap, and liquidity cap.
5. Materialize rebalance actions in this order:
   `EXIT -> TRIM -> HOLD -> ADD -> BUY_NEW -> SKIP -> NO_ACTION`
6. Build position snapshots and NAV snapshots.
7. Evaluate policy performance for `OPEN_ALL`, `TIMING_ASSISTED`, and an equal-weight comparison baseline.

Weight calculation:

- `effective_alpha_long` starts from primary-horizon alpha
- tactical alpha, lower band, flow, and regime fit can add support
- uncertainty, disagreement, and implementation penalty reduce conviction
- `risk_scaled_conviction = effective_alpha_long / max(volatility_proxy, vol_floor)`
- target weights are normalized from positive conviction only and then clipped by cap-aware iterative allocation
- fractional shares are forbidden and residual cash is tracked explicitly

Policy constraints:

- long-only only
- no leverage / no margin / no short / no derivatives
- gross exposure cannot exceed 100%
- negative weights are forbidden
- hold hysteresis reduces churn
- turnover overflow becomes waitlist / `SKIP`, not forced entry

TICKET-011 commands:

```powershell
python scripts/freeze_active_portfolio_policy.py --as-of-date 2026-03-06 --policy-config-path config/portfolio_policies/balanced_long_only_v1.yaml
python scripts/build_portfolio_candidate_book.py --as-of-date 2026-03-06
python scripts/validate_portfolio_candidate_book.py --as-of-date 2026-03-06
python scripts/materialize_portfolio_target_book.py --as-of-date 2026-03-06
python scripts/materialize_portfolio_rebalance_plan.py --as-of-date 2026-03-06
python scripts/materialize_portfolio_position_snapshots.py --as-of-date 2026-03-06
python scripts/materialize_portfolio_nav.py --start-date 2026-03-03 --end-date 2026-03-09
python scripts/run_portfolio_walkforward.py --start-as-of-date 2026-03-03 --end-as-of-date 2026-03-06
python scripts/evaluate_portfolio_policies.py --start-date 2026-03-03 --end-date 2026-03-09
python scripts/render_portfolio_report.py --as-of-date 2026-03-06 --dry-run
python scripts/publish_discord_portfolio_summary.py --as-of-date 2026-03-06 --dry-run
python scripts/validate_portfolio_framework.py --as-of-date 2026-03-06
```

Stored contracts:

- `fact_portfolio_policy_registry`
- `fact_portfolio_candidate`
- `fact_portfolio_target_book`
- `fact_portfolio_rebalance_plan`
- `fact_portfolio_position_snapshot`
- `fact_portfolio_nav_snapshot`
- `fact_portfolio_constraint_event`
- `fact_portfolio_evaluation_summary`

Portfolio UI:

- `포트폴리오 스튜디오`: active policy, target holdings, waitlist, blocked names, constraint summary
- `포트폴리오 평가`: NAV, drawdown, turnover, holding count, policy comparison
- `운영`: active portfolio policy registry, latest target/rebalance/nav/evaluation run state, rollback/report status

Current known limitations:

- this is still a proposal layer, not an execution system
- the equal-weight comparison is a simple deterministic baseline, not a full optimizer benchmark
- `TIMING_ASSISTED` only gates new entry and add actions
- DuckDB remains effectively single-writer, so portfolio batch runs are safest when executed sequentially

### Stored contracts

- `fact_model_training_run` extended for `model_domain = intraday_meta`
- `fact_model_metric_summary` stores panel/class/overlay diagnostics
- `fact_intraday_meta_prediction`
- `fact_intraday_meta_decision`
- `fact_intraday_active_meta_model`

### Current known limitations

- the overlay is still a conservative sklearn baseline, not a deep or sequential intraday model
- probability calibration is sigmoid-style and intentionally simple
- uncertainty/disagreement are proxy scores, not probabilistic guarantees
- the final action layer is bounded by the active policy and hard guards by design

## TICKET-012 operational stability / batch recovery / disk guard / health dashboard

TICKET-012 adds a protective ops layer around the existing research, evaluation, intraday,
meta-model, and portfolio pipelines. It does not redesign alpha, selection, or portfolio logic.

Run and step metadata:

- `fact_job_run` stores job-level status, trigger type, lineage, lock key, policy reference, and error summary
- `fact_job_step_run` stores step order, status, records/artifacts, retry count, skip reason, and per-step error text
- statuses supported: `SUCCESS`, `PARTIAL_SUCCESS`, `DEGRADED_SUCCESS`, `SKIPPED`, `BLOCKED`, `FAILED`
- trigger types supported: `MANUAL`, `SCHEDULED`, `RECOVERY`, `VALIDATION`, `DRY_RUN`
- run lineage keeps `root_run_id`, `parent_run_id`, and `recovery_of_run_id`

Dependency and health materialization:

- `fact_pipeline_dependency_state` stores pipeline-level readiness for research, post-close, evaluation, and maintenance bundles
- `fact_health_snapshot` stores layered health metrics instead of one boolean
- current health summary includes failed-run counts, active/stale locks, open alerts, disk usage ratio, and latest successful report/evaluation/portfolio outputs
- `fact_alert_event` stores visible warnings instead of relying on stdout-only behavior

Disk watermark and retention policy:

- warn watermark: `70%`
- cleanup watermark: `80%`
- emergency watermark: `90%`
- cleanup is allowlist-driven and protected-prefix aware
- sufficient-mode default retention keeps only re-creatable high-volume support data on a rolling window:
  - raw API payload/cache: `14d`
  - intraday `bar_1m`: `30d`
  - intraday `trade_summary`: `45d`
  - intraday `quote_summary`: `21d`
  - report cache/artifacts: `21d`
  - logs: `30d`
- curated core data, predictions, evaluations, and portfolio snapshots are not auto-deleted
- retention supports both dry-run and actual-run tracking through `fact_retention_cleanup_run`
- disk events are written to `fact_disk_watermark_event`

Recovery and locking:

- duplicate execution guard uses `fact_active_lock`
- stale lock release is explicit and script-driven
- failed or blocked runs can be queued through `fact_recovery_action`
- recovery creates new runs and preserves lineage; it never overwrites the original failed run
- active ops behavior is resolved from `fact_active_ops_policy` or safe default YAML config

Ops policy configs:

- [default_ops_policy.yaml](/d:/MyApps/StockMaster/config/ops/default_ops_policy.yaml)
- [conservative_ops_policy.yaml](/d:/MyApps/StockMaster/config/ops/conservative_ops_policy.yaml)
- [local_dev_ops_policy.yaml](/d:/MyApps/StockMaster/config/ops/local_dev_ops_policy.yaml)

TICKET-012 commands:

```powershell
python scripts/freeze_active_ops_policy.py --as-of-at 2026-03-08T18:00:00 --policy-config-path config/ops/default_ops_policy.yaml --promotion-type MANUAL_FREEZE --note "Initial default ops policy"
python scripts/check_pipeline_dependencies.py --as-of-date 2026-03-08
python scripts/materialize_health_snapshots.py --as-of-date 2026-03-08
python scripts/run_daily_research_pipeline.py --as-of-date 2026-03-08 --dry-run
python scripts/run_daily_post_close_bundle.py --as-of-date 2026-03-08 --dry-run
python scripts/run_daily_evaluation_bundle.py --as-of-date 2026-03-08 --dry-run
python scripts/run_ops_maintenance_bundle.py --as-of-date 2026-03-08 --dry-run
python scripts/enforce_retention_policies.py --as-of-date 2026-03-08 --dry-run
python scripts/cleanup_disk_watermark.py --as-of-date 2026-03-08 --dry-run
python scripts/rotate_and_compress_logs.py --as-of-date 2026-03-08 --dry-run
python scripts/summarize_storage_usage.py --as-of-date 2026-03-08
python scripts/reconcile_failed_runs.py --as-of-date 2026-03-08
python scripts/recover_incomplete_runs.py --as-of-date 2026-03-08 --dry-run
python scripts/force_release_stale_lock.py --as-of-date 2026-03-08
python scripts/render_ops_report.py --as-of-date 2026-03-08 --dry-run
python scripts/publish_discord_ops_alerts.py --as-of-date 2026-03-08 --dry-run
python scripts/validate_health_framework.py --as-of-date 2026-03-08
python scripts/validate_ops_framework.py --as-of-date 2026-03-08
```

Dashboard coverage:

- `Health Dashboard`: overall health summary, recent runs, failed steps, dependency readiness, disk watermark, cleanup history, active locks, recovery queue, alerts, and latest successful outputs
- `Ops`: keeps the broader research/intraday/policy/portfolio operational view while Health Dashboard focuses on TICKET-012 reliability state

Current known limitations:

- DuckDB is still effectively single-writer, so ops bundles and validation scripts should run sequentially
- recovery routing is deterministic and rule-based; it does not infer business-safe replay for every possible future pipeline
- Discord ops alert publish is optional and should be dry-run first
- cleanup defaults stay conservative by design; the framework prefers false negatives over accidental deletion

## TICKET-013 final workflow / dashboard / report polish

TICKET-013 closes the user-facing workflow layer. The main rule is that the UI should
read existing materialized outputs instead of recomputing heavy logic on page load.

New presentation contracts:

- `fact_latest_app_snapshot`
- `fact_latest_report_index`
- `fact_release_candidate_check`
- `fact_ui_data_freshness_snapshot`

New release / report scripts:

```powershell
python scripts/render_daily_research_report.py --as-of-date 2026-03-06 --dry-run
python scripts/render_portfolio_report.py --as-of-date 2026-03-06 --dry-run
python scripts/render_evaluation_report.py --as-of-date 2026-03-06 --dry-run
python scripts/render_intraday_summary_report.py --session-date 2026-03-09 --dry-run
python scripts/build_report_index.py
python scripts/build_ui_freshness_snapshot.py
python scripts/build_latest_app_snapshot.py --as-of-date 2026-03-06
python scripts/validate_page_contracts.py
python scripts/validate_report_artifacts.py
python scripts/validate_navigation_integrity.py
python scripts/validate_release_candidate.py --as-of-date 2026-03-08
python scripts/render_release_candidate_checklist.py --as-of-date 2026-03-08 --dry-run
```

TICKET-013 page structure:

- `오늘`
- `시장 현황`
- `리더보드`
- `포트폴리오`
- `포트폴리오 평가`
- `장중 콘솔`
- `사후 평가`
- `종목 분석`
- `리서치 랩`
- `운영`
- `헬스 대시보드`
- `문서 / 도움말`

TICKET-013 docs:

- [docs/USER_GUIDE.md](/d:/MyApps/StockMaster/docs/USER_GUIDE.md)
- [docs/WORKFLOW_DAILY.md](/d:/MyApps/StockMaster/docs/WORKFLOW_DAILY.md)
- [docs/GLOSSARY.md](/d:/MyApps/StockMaster/docs/GLOSSARY.md)
- [docs/KNOWN_LIMITATIONS.md](/d:/MyApps/StockMaster/docs/KNOWN_LIMITATIONS.md)
- [docs/REPORTS_AND_PAGES.md](/d:/MyApps/StockMaster/docs/REPORTS_AND_PAGES.md)

TICKET-016 audit docs:

- [docs/AUDIT_T000_T013_STATUS.md](/d:/MyApps/StockMaster/docs/AUDIT_T000_T013_STATUS.md)
- [docs/DB_CONTRACT_MATRIX.md](/d:/MyApps/StockMaster/docs/DB_CONTRACT_MATRIX.md)
- [docs/GAP_REMEDIATION_BACKLOG.md](/d:/MyApps/StockMaster/docs/GAP_REMEDIATION_BACKLOG.md)
- [docs/CASE_RUNBOOK_T000_T013.md](/d:/MyApps/StockMaster/docs/CASE_RUNBOOK_T000_T013.md)

Presentation rules:

- page-specific stale / warning states come from `fact_ui_data_freshness_snapshot`
- current truth badges come from `fact_latest_app_snapshot`
- canonical report center comes from `fact_latest_report_index`
- release candidate checklist is append-only and stored in `fact_release_candidate_check`
- dashboard and reports should agree on the same materialized truth; no page-local recomputation

## Grade rules

Current grade assignment:

- `A`: eligible and top 5%
- `A-`: eligible and top 15%
- `B`: eligible and top 35%, or critical-risk names that still clear the minimum threshold
- `C`: everything else, including ineligible names

Grades are presentation buckets, not probability estimates.

## Tables and views added through TICKET-012

Tables:

- `fact_investor_flow`
- `fact_feature_snapshot`
- `fact_forward_return_label`
- `fact_market_regime_snapshot`
- `fact_ranking`
- `fact_prediction`
- `fact_selection_outcome`
- `fact_evaluation_summary`
- `fact_calibration_diagnostic`
- `fact_model_training_run`
- `fact_model_member_prediction`
- `fact_model_metric_summary`
- `fact_intraday_candidate_session`
- `fact_intraday_bar_1m`
- `fact_intraday_trade_summary`
- `fact_intraday_quote_summary`
- `fact_intraday_signal_snapshot`
- `fact_intraday_entry_decision`
- `fact_intraday_timing_outcome`
- `fact_intraday_market_context_snapshot`
- `fact_intraday_regime_adjustment`
- `fact_intraday_adjusted_entry_decision`
- `fact_intraday_strategy_result`
- `fact_intraday_strategy_comparison`
- `fact_intraday_timing_calibration`
- `fact_intraday_policy_experiment_run`
- `fact_intraday_policy_candidate`
- `fact_intraday_policy_evaluation`
- `fact_intraday_policy_ablation_result`
- `fact_intraday_policy_selection_recommendation`
- `fact_intraday_active_policy`
- `fact_job_run`
- `fact_job_step_run`
- `fact_pipeline_dependency_state`
- `fact_health_snapshot`
- `fact_disk_watermark_event`
- `fact_retention_cleanup_run`
- `fact_alert_event`
- `fact_recovery_action`
- `fact_active_ops_policy`
- `fact_active_lock`
- `ops_ranking_validation_summary`
- `ops_selection_validation_summary`

Views:

- `vw_latest_investor_flow`
- `vw_feature_snapshot_latest`
- `vw_feature_matrix_latest`
- `vw_latest_forward_return_label`
- `vw_market_regime_latest`
- `vw_ranking_latest`
- `vw_prediction_latest`
- `vw_latest_model_training_run`
- `vw_latest_model_member_prediction`
- `vw_latest_model_metric_summary`
- `vw_selection_outcome_latest`
- `vw_latest_evaluation_summary`
- `vw_latest_calibration_diagnostic`
- `vw_latest_intraday_candidate_session`
- `vw_latest_intraday_bar_1m`
- `vw_latest_intraday_trade_summary`
- `vw_latest_intraday_quote_summary`
- `vw_latest_intraday_signal_snapshot`
- `vw_latest_intraday_entry_decision`
- `vw_latest_intraday_timing_outcome`
- `vw_latest_intraday_market_context_snapshot`
- `vw_latest_intraday_regime_adjustment`
- `vw_latest_intraday_adjusted_entry_decision`
- `vw_latest_intraday_strategy_result`
- `vw_latest_intraday_strategy_comparison`
- `vw_latest_intraday_timing_calibration`
- `vw_latest_intraday_policy_experiment_run`
- `vw_latest_intraday_policy_candidate`
- `vw_latest_intraday_policy_evaluation`
- `vw_latest_intraday_policy_ablation_result`
- `vw_latest_intraday_policy_selection_recommendation`
- `vw_latest_intraday_active_policy`
- `vw_latest_job_run`
- `vw_latest_job_step_run`
- `vw_latest_pipeline_dependency_state`
- `vw_latest_health_snapshot`
- `vw_latest_disk_watermark_event`
- `vw_latest_retention_cleanup_run`
- `vw_latest_alert_event`
- `vw_latest_recovery_action`
- `vw_latest_active_ops_policy`
- `vw_latest_active_lock`
- `vw_latest_ranking_validation_summary`
- `vw_latest_selection_validation_summary`

## Raw, curated, and artifact outputs

Representative paths:

```text
data/curated/features/as_of_date=YYYY-MM-DD/feature_snapshot.parquet
data/curated/features/as_of_date=YYYY-MM-DD/feature_matrix.parquet
data/curated/market/investor_flow/trading_date=YYYY-MM-DD/investor_flow.parquet
data/curated/labels/as_of_date=YYYY-MM-DD/forward_return_labels.parquet
data/curated/regime/as_of_date=YYYY-MM-DD/market_regime_snapshot.parquet
data/curated/model/training_dataset/train_end_date=YYYY-MM-DD/alpha_training_dataset.parquet
data/curated/ranking/as_of_date=YYYY-MM-DD/horizon=N/explanatory_ranking.parquet
data/curated/ranking/as_of_date=YYYY-MM-DD/horizon=N/ranking_version=selection_engine_v1/selection_engine_v1.parquet
data/curated/ranking/as_of_date=YYYY-MM-DD/horizon=N/ranking_version=selection_engine_v2/selection_engine_v2.parquet
data/curated/prediction/as_of_date=YYYY-MM-DD/proxy_prediction_band.parquet
data/curated/prediction/as_of_date=YYYY-MM-DD/prediction_version=alpha_prediction_v1/alpha_prediction_v1.parquet
data/curated/evaluation/selection_outcomes/selection_date=YYYY-MM-DD/ranking_version=.../horizon=N/selection_outcomes.parquet
data/curated/evaluation/summary/start_selection_date=YYYY-MM-DD/end_selection_date=YYYY-MM-DD/evaluation_summary.parquet
data/curated/evaluation/calibration_diagnostics/start_selection_date=YYYY-MM-DD/end_selection_date=YYYY-MM-DD/calibration_diagnostics.parquet
data/curated/intraday/candidate_session/session_date=YYYY-MM-DD/horizon=N/ranking_version=selection_engine_v2/candidate_session.parquet
data/curated/intraday/bar_1m/session_date=YYYY-MM-DD/bar_1m.parquet
data/curated/intraday/trade_summary/session_date=YYYY-MM-DD/trade_summary.parquet
data/curated/intraday/quote_summary/session_date=YYYY-MM-DD/quote_summary.parquet
data/curated/intraday/signal_snapshot/session_date=YYYY-MM-DD/checkpoint=HHMM/signal_snapshot.parquet
data/curated/intraday/entry_decision/session_date=YYYY-MM-DD/checkpoint=HHMM/entry_decision.parquet
data/curated/intraday/timing_outcome/session_date=YYYY-MM-DD/horizon=N/timing_outcome.parquet
data/curated/intraday/market_context/session_date=YYYY-MM-DD/market_context_snapshot.parquet
data/curated/intraday/regime_adjustment/session_date=YYYY-MM-DD/regime_adjustment.parquet
data/curated/intraday/adjusted_entry_decision/session_date=YYYY-MM-DD/checkpoint=HHMM/adjusted_entry_decision.parquet
data/curated/intraday/strategy_result/session_date=YYYY-MM-DD/strategy_result.parquet
data/curated/intraday/strategy_comparison/end_session_date=YYYY-MM-DD/strategy_comparison.parquet
data/curated/intraday/timing_calibration/window_end_date=YYYY-MM-DD/timing_calibration.parquet
data/curated/intraday/policy_candidates/search_space_version=pcal_v1/policy_candidates.parquet
data/curated/intraday/policy_evaluation/window_end_date=YYYY-MM-DD/policy_evaluation.parquet
data/curated/intraday/policy_ablation/end_session_date=YYYY-MM-DD/policy_ablation.parquet
data/curated/intraday/policy_recommendation/recommendation_date=YYYY-MM-DD/policy_recommendation.parquet
data/curated/intraday/active_policy/effective_from_date=YYYY-MM-DD/active_policy.parquet
data/artifacts/model/training/train_end_date=YYYY-MM-DD/horizon=N/alpha_model_v1.pkl
data/artifacts/model_validation/<run_id>.md
data/artifacts/selection_engine_comparison/<run_id>.md
data/artifacts/model_diagnostics/train_end_date=YYYY-MM-DD/<run_id>/model_diagnostic_report.md
data/artifacts/intraday_monitor/session_date=YYYY-MM-DD/<run_id>/intraday_monitor_preview.md
data/artifacts/intraday_postmortem/session_date=YYYY-MM-DD/<run_id>/intraday_postmortem_preview.md
data/artifacts/intraday_policy/as_of_date=YYYY-MM-DD/<run_id>/intraday_policy_research_report.md
data/artifacts/ops/storage_usage/storage_usage_summary.json
data/artifacts/ops/report/as_of_date=YYYY-MM-DD/<run_id>/ops_report_preview.md
data/artifacts/ops/report/as_of_date=YYYY-MM-DD/<run_id>/ops_report_payload.json
data/artifacts/validation/ranking/start_date=YYYY-MM-DD/end_date=YYYY-MM-DD/ranking_validation_summary.parquet
data/artifacts/validation/selection_engine_v1/start_date=YYYY-MM-DD/end_date=YYYY-MM-DD/selection_validation_summary.parquet
data/artifacts/discord/as_of_date=YYYY-MM-DD/<run_id>/discord_preview.md
data/artifacts/postmortem/evaluation_date=YYYY-MM-DD/<run_id>/postmortem_preview.md
```

## News query pack adjustment

The default query pack lives in [config/news_queries.yaml](/d:/MyApps/StockMaster/config/news_queries.yaml).

How to adjust:

1. Edit `packs.default.market`
2. Add or remove `bucket` / `keyword` pairs
3. Rerun `scripts/sync_news_metadata.py`

Operational note:

- `market_and_focus` without explicit symbols caps the focus subset to avoid unbounded query fan-out

## Fundamentals availability rule

Current rule is conservative and date-only:

- a DART filing is eligible only if `rcept_dt <= as_of_date`
- the latest eligible regular filing wins
- future filings are excluded from historical snapshots

## Discord dry-run and publish

Use dry-run first:

```powershell
python scripts/render_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
```

Live publish:

```powershell
python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06
```

Operational behavior:

- render always writes preview artifacts under `data/artifacts/discord/...`
- publish always renders preview artifacts first and then sends one or more Discord messages if needed
- live publish requires `DISCORD_REPORT_ENABLED=true`
- publish failure is recorded as a warning note and does not fail the broader daily pipeline
- only metadata and selection summary are sent; no full article body is stored or transmitted

## Known limitations

- explanatory ranking v0 still keeps `flow_score` reserved by design
- selection engine v1 is not an ML alpha model; its prediction bands are calibrated historical proxies only
- ML alpha model v1 is still a shallow-history sklearn baseline, not a fully tuned production stack
- `expected_excess_return`, `lower_band`, `median_band`, and `upper_band` in `alpha_prediction_v1` come from calibrated residual logic, not a full probabilistic model
- `disagreement_score` is model-spread based only; it is not a richer committee or Bayesian uncertainty estimate
- fallback rows can still appear when no historical alpha artifact exists for a requested date/horizon
- `market_cap` depends on upstream availability and is not yet independently validated
- many 20d and 60d features will be null on shallow history windows
- symbol linking for news is intentionally conservative and may leave `symbol_candidates = []`
- investor flow coverage can be partial and stays null instead of being zero-filled
- intraday quote summary is allowed to remain null for historical or future sessions; the timing layer lowers signal quality instead of fabricating quote depth
- historical/future intraday 1m data can fall back to proxy or unavailable rows depending on session-date coverage
- no full article body storage
- validation is sparse unless historical ranking snapshots and forward labels have already been materialized
- evaluation is pre-cost only; there is still no transaction-cost simulator
- future-dated postmortem runs can render empty previews if the requested evaluation date has not matured yet

## Validation and lint

Run tests:

```powershell
python -m pytest
```

Run lint:

```powershell
python -m ruff check .
```

## UI checkpoints

After `streamlit run app/ui/Home.py`, verify:

- Home shows reference data, research freshness, feature/ranking snapshot, and validation summary
- Ops shows version tracking, flow/prediction coverage, model training summary, validation status, experiment runs, active-policy registry state, rollback history, and policy publish/report status
- Health Dashboard shows overall health summary, recent job runs, failed steps, dependency readiness, disk watermark, cleanup history, active locks, recovery queue, alerts, and latest successful outputs
- Research shows feature store, flow, regime, labels, selection validation, and intraday policy lab outputs
- Research also shows intraday meta-model training rows, calibration summary, confusion matrix, feature importance, and overlay comparison
- Leaderboard compares explanatory ranking v0, selection engine v1, and selection engine v2
- Market Pulse shows regime + flow breadth + latest top selections
- Stock Workbench shows one symbol across features, alpha predictions, flow history, prices, frozen outcomes, and news metadata
- Research also acts as the policy lab and shows walk-forward evidence, ablation deltas, recommendations, and policy-report previews
- Evaluation shows outcome cohorts, rolling summaries, calibration diagnostics, selection-engine comparison, intraday strategy comparison, policy walk-forward/ablation evidence, and policy-only vs meta-overlay comparison
- Intraday Console shows candidate session coverage, checkpoint health, raw vs tuned actions, active policy trace, ML class probabilities, final action, and fallback
- Portfolio Studio shows active portfolio policy, target holdings, waitlist, blocked names, and constraint summary
- Portfolio Evaluation shows NAV, drawdown, turnover, holding count, and policy comparison
- Ops also shows active meta-model registry, latest meta training/scoring runs, fallback traces, and rollback history

## Docker

Copy `.env.example` to `.env`, then run:

```powershell
docker compose up --build
```

The container starts the Streamlit dashboard on `http://localhost:8501`.
