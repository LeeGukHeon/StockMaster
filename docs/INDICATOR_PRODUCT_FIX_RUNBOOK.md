# Indicator Product Fix Runbook

## Goal
Run the leading-indicator product path end to end for:
- `/즉석종목분석`
- `/내일종목추천` D+1
- `/내일종목추천` D+5

The runbook targets these predictive specs:
- `alpha_lead_d1_v1`
- `alpha_swing_d5_v1`

During the D+1 repair lane:
- `alpha_lead_d1_v1` may remain blocked and D+1 can stay on the fallback baseline
- `alpha_swing_d5_v1` is treated as a preserved D+5 control lane unless you explicitly widen `--freeze-horizons`

## Precondition

Before running the bundle, the local/server DuckDB must have same-day OHLCV coverage for every training candidate date that the bundle will try to build feature snapshots for.

Typical failure shape:

```text
Feature store cannot build a market-wide snapshot because same-day OHLCV is missing for trading date 2026-02-27.
Missing feature-snapshot source dates for bundle: 2026-02-27.
```

If this happens, sync/fill the missing OHLCV date first before retrying the bundle.

Example recovery command:

```bash
python3 scripts/sync_daily_ohlcv.py \
  --date 2026-02-27 \
  --market ALL \
  --force
```

## 1. Registry rematerialization

```bash
python3 scripts/materialize_alpha_model_specs.py
```

Expected:
- `dim_alpha_model_spec` contains lifecycle metadata
- only `alpha_lead_d1_v1`, `alpha_swing_d5_v1` remain `active_candidate_flag=TRUE`

## 2. Train only the new active predictive specs

```bash
python3 scripts/train_alpha_candidate_models.py \
  --train-end-date 2026-04-15 \
  --horizons 1 5 \
  --model-spec-ids alpha_lead_d1_v1 alpha_swing_d5_v1
```

Use shorter windows / limited symbols only for smoke work:

```bash
python3 scripts/train_alpha_candidate_models.py \
  --train-end-date 2026-04-15 \
  --horizons 1 5 \
  --model-spec-ids alpha_lead_d1_v1 alpha_swing_d5_v1 \
  --min-train-days 5 \
  --validation-days 2 \
  --limit-symbols 4
```

## 3. End-to-end indicator-product bundle

This single command rematerializes the new registry rows, trains the requested specs, materializes prediction/selection/shadow outputs, builds the canonical selection-gap scorecard, runs validation, and only freezes the horizons explicitly requested via `--freeze-horizons`.

For the D+1 repair lane, use `--freeze-horizons 1` so that:
- D+1 can be blocked and held on the fallback baseline when matured evidence is too thin
- D+5 remains on its current active model as a preserved control lane
- the bundle output can still emit `blocked_freeze_model_spec_ids` / `freeze_block_reasons` without silently changing D+5
- `--backfill-shadow-history` walks the full selection-date range instead of materializing shadow rows for only the final `as_of_date`

If you later want a full multi-horizon activation pass, widen the command explicitly with `--freeze-horizons 1 5`.

```bash
python3 scripts/run_alpha_indicator_product_bundle.py \
  --train-end-date 2026-04-15 \
  --as-of-date 2026-04-15 \
  --shadow-start-selection-date 2026-03-16 \
  --shadow-end-selection-date 2026-04-15 \
  --horizons 1 5 \
  --model-spec-ids alpha_lead_d1_v1 alpha_swing_d5_v1 \
  --backfill-shadow-history \
  --rolling-windows 20 60 \
  --freeze-horizons 1
```

Smoke variant:

```bash
python3 scripts/run_alpha_indicator_product_bundle.py \
  --train-end-date 2026-03-06 \
  --as-of-date 2026-03-06 \
  --shadow-start-selection-date 2026-03-06 \
  --shadow-end-selection-date 2026-03-06 \
  --horizons 1 5 \
  --model-spec-ids alpha_lead_d1_v1 alpha_swing_d5_v1 \
  --backfill-shadow-history \
  --min-train-days 5 \
  --validation-days 2 \
  --limit-symbols 4 \
  --rolling-windows 20 60 \
  --freeze-horizons 1
```

## 4. Explicit validation rerun

```bash
python3 scripts/validate_alpha_model_v1.py \
  --as-of-date 2026-04-15 \
  --horizons 1 5
```

Look for these checks in the validation markdown/json artifact:
- `selection_gap_top5_drag_h1_rolling_20`
- `selection_gap_top5_drag_h1_rolling_60`
- `selection_gap_top5_drag_h5_rolling_20`
- `selection_gap_top5_drag_h5_rolling_60`

Interpretation:
- `pass`: drag is inside the allowed degradation band
- `warn`: missing scorecard row or insufficient matured history
- `fail`: reserved for hard validation violations

## 5. Canonical selection-gap scorecard only

Use this when you already trust `fact_alpha_shadow_selection_outcome` and only want to refresh gap metrics.

```bash
python3 scripts/materialize_alpha_shadow_selection_gap_scorecard.py \
  --start-selection-date 2026-03-16 \
  --end-selection-date 2026-04-15 \
  --horizons 1 5 \
  --model-spec-ids alpha_lead_d1_v1 alpha_swing_d5_v1 \
  --rolling-windows 20 60 \
  --skip-outcome-refresh
```

Canonical source of truth:
- table: `fact_alpha_shadow_selection_gap_scorecard`

Important encoded semantics:
- raw top-slice source = shadow prediction expected-excess-return-desc top5 within `(selection_date, horizon, model_spec_id)`
- hit-rate formula = share of matured rows with `realized_excess_return > 0.0`
- insufficient history = scorecard row present with `insufficient_history_flag=TRUE`

## 6. Discord/read-store refresh

```bash
python3 scripts/materialize_discord_bot_read_store.py \
  --as-of-date 2026-04-15
```

Expected:
- `/내일종목추천` snapshots contain D+1/D+5 serving lineage
- `stock_summary` snapshot contains the enriched serving payload used by `/즉석종목분석`

## 7. Ops/report refresh

Discord EOD:

```bash
python3 scripts/render_discord_eod_report.py \
  --as-of-date 2026-04-15 \
  --dry-run
```

Release candidate checklist:

```bash
python3 scripts/render_release_candidate_checklist.py \
  --as-of-date 2026-04-15 \
  --dry-run
```

Expected report labels:
- `active serving spec`
- `legacy comparison baseline`
- `fallback baseline`

Expected new visibility:
- `선택 드래그 점검`
- latest rolling20 selection-gap summary

## 8. What to inspect after rerun

### Serving
- D+1 active model should either:
  - remain the fallback baseline if `blocked_freeze_model_spec_ids` contains `alpha_lead_d1_v1`, or
  - switch to `alpha_lead_d1_v1` only after the D+1 gate passes
- D+5 active model should remain unchanged during D+1-only repair runs (`--freeze-horizons 1`)
- `/즉석종목분석` output should include:
  - why now
  - signal decomposition
  - risk flags
  - invalidation conditions
  - quote/news basis

### Scorecard
- `fact_alpha_shadow_selection_gap_scorecard`
- verify:
  - `drag_vs_raw_top5`
  - `selected_top5_mean_realized_excess_return`
  - `report_candidates_mean_realized_excess_return`
  - `selected_top5_hit_rate`
  - `insufficient_history_flag`

### Validation
- active candidate gates should appear for D+1 and D+5
- no missing lifecycle metadata for active specs

## 9. Server execution notes

Recommended server order:
1. pull/deploy code
2. `python3 scripts/materialize_alpha_model_specs.py`
3. `python3 scripts/run_alpha_indicator_product_bundle.py ... --backfill-shadow-history`
4. `python3 scripts/materialize_discord_bot_read_store.py --as-of-date ...`
5. `python3 scripts/render_release_candidate_checklist.py --as-of-date ... --dry-run`
6. `python3 scripts/render_discord_eod_report.py --as-of-date ... --dry-run`
7. `scripts/server/verify_indicator_product_bundle_host.sh ...`

Convenience host wrapper:

```bash
scripts/server/run_indicator_product_bundle_host.sh 2026-04-15 2026-04-15 2026-03-16 2026-04-15
```

The host wrapper defaults to D+1-only freeze mode (`freeze-horizons=1`).  
To widen serving changes explicitly, pass extra horizons:

```bash
scripts/server/run_indicator_product_bundle_host.sh 2026-04-15 2026-04-15 2026-03-16 2026-04-15 1 5
```

The host wrapper now also runs `scripts/server/verify_indicator_product_bundle_host.sh` automatically to:
- prove preserved horizons (for the D+1 repair lane, D+5) did not drift
- confirm same-window comparator evidence exists for `alpha_recursive_expanding_v1` and `alpha_topbucket_h1_rolling_120_v1`
- surface the latest validation + report artifact directories

If you are transporting local artifacts instead of pulling directly, apply them first:

```bash
scripts/server/apply_indicator_product_fix_artifacts.sh \
  /path/to/indicator-product-fix-2026-04-20.bundle \
  /path/to/indicator-product-fix-working-tree-2026-04-20.tar.gz \
  a14695bd7836f51f6f26b0b3808219d24b4e1d7271060ff79099f768ab96f02b \
  01073d8003b0cb57287e5aca753bad1fd42f442d7d6a0f25669f756ea9c69f0a
```

Do not judge success from training completion alone. The minimum acceptance evidence is:
- D+1 freeze was either explicitly blocked with fallback retained or explicitly allowed by gate
- D+5 active serving state stayed unchanged during D+1-only repair mode
- shadow backfill actually covered the requested selection-date window (not just the terminal `as_of_date`)
- selection-gap scorecard materialized
- validation gate rows present
- Discord/release reports show active/comparison/fallback roles
