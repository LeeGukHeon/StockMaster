# D+5-Centric Single-Model Redesign Review

This document is the tracked-repo review companion to `.omx/plans/ralplan-d5-centric-single-model-redesign-2026-04-21.md`.
The plan itself stays under `.omx/plans/` (read-only / ignored for team-worker delivery), so the brownfield review findings and execution guardrails are recorded here in versioned documentation.

## Brownfield review findings

1. **Keep `alpha_swing_d5_v1` intact as the H5 control lane.**
   - `app/ml/constants.py` currently exposes `alpha_swing_d5_v1` as the active D+5 top5-binary challenger.
   - Add `alpha_swing_d5_v2`, but do not mutate `alpha_swing_d5_v1` in place or the redesign loses comparator discipline.

2. **Do not widen the generic `top5_binary` scoring branch.**
   - `app/selection/engine_v2.py` currently routes every `top5_binary` spec through the same `SELECTION_V2_TOP5_FOCUS_WEIGHTS` path.
   - The redesign must add a model-spec-specific override (for example `SELECTION_V2_D5_PRIMARY_WEIGHTS`) that activates only for `alpha_swing_d5_v2`.
   - A generic weight change would silently alter D+1 and other top5-binary lanes.

3. **Do not reuse the D+1 freeze/comparator helper for H5 promotion.**
   - `app/ml/indicator_product.py` is currently centered on D+1 thresholds and `LEGACY_H1_COMPARATOR_MODEL_SPEC_ID`.
   - The D+5 redesign needs H5-aware comparator evidence instead of piggybacking on the existing H1-centric freeze logic.

4. **Validation must compare explicit H5 controls, not only drag-based scorecards.**
   - `app/ml/validation.py` currently emits generic selection-gap drag checks.
   - The redesign needs additional D+5 comparator checks against `alpha_swing_d5_v1` and `alpha_recursive_expanding_v1`, plus explicit bucket pass/warn/fail checks for the D+5 lane.

5. **Bucket robustness belongs in the evaluation-summary surface.**
   - `app/evaluation/alpha_shadow.py` already owns summary materialization and `app/ml/shadow_report.py` already reads `fact_alpha_shadow_evaluation_summary`.
   - Add bucket labels and bucket rows there.
   - Do **not** bolt D+5 bucket logic onto the selection-gap scorecard alone or the final report renderer alone.

## Frozen execution contract

### Challenger spec
Add `alpha_swing_d5_v2` with this frozen contract:

- `estimation_scheme='rolling'`
- `rolling_window_days=250`
- `member_names=('elasticnet', 'hist_gbm')`
- `feature_groups=('price_trend','volatility_risk','liquidity_turnover','investor_flow','news_catalyst','fundamentals_quality','value_safety','data_quality')`
- `target_variant='top5_binary'`
- `training_target_variant='top5_binary'`
- `validation_primary_metric_name='top5_mean_excess_return'`
- `promotion_primary_loss_name='loss_top5'`
- `allowed_horizons=(5,)`
- `active_candidate_flag=True`
- `lifecycle_role='active_candidate'`
- `lifecycle_fallback_flag=False`

### D+1 boundary

- D+1 is **not** the primary training target for this lane.
- D+1 may appear only as:
  - feature input
  - diagnostic output
  - auxiliary interpretation in the final comparison memo

### Comparator lock

- Primary D+5 comparator: `alpha_swing_d5_v1`
- Secondary D+5 non-inferiority comparator: `alpha_recursive_expanding_v1` at H5
- Auxiliary D+1 interpretation comparators:
  - `alpha_recursive_expanding_v1` at H1
  - `alpha_topbucket_h1_rolling_120_v1` at H1

### Bucket source of truth

Bucket labels must come from a wide feature frame joined to `fact_alpha_shadow_selection_outcome` on:
- `selection_date = as_of_date`
- `symbol`

Canonical bucket definitions:

- `bucket_continuation`: `ret_10d > 0.03` and `dist_from_20d_high <= 0.15`
- `bucket_reversal_recovery`: `drawdown_20d <= -0.08` and `residual_ret_5d > 0`
- `bucket_crowded_risk`: `turnover_z_5_20 >= 1.5` or `news_burst_share_1d >= 0.30` or `realized_vol_20d >= 0.05`

## File-level implementation guardrails

| Surface | Required change | Do not do |
| --- | --- | --- |
| `app/ml/constants.py` | Add `alpha_swing_d5_v2` as a new D+5 challenger spec and keep `alpha_swing_d5_v1` frozen as the control lane. | Do not retune `alpha_swing_d5_v1` in place. |
| `app/selection/engine_v2.py` | Add a model-spec-specific D+5-primary weight override for `alpha_swing_d5_v2`. | Do not change the generic `top5_binary` branch for every spec. |
| `app/evaluation/alpha_shadow.py` | Materialize bucket-labeled D+5 evidence into `fact_alpha_shadow_evaluation_summary`. | Do not hide bucket derivation in the markdown renderer only. |
| `app/ml/validation.py` | Add explicit H5 comparator and bucket checks gated on `focus_model_spec_id == "alpha_swing_d5_v2"` (or equivalent explicit parameter). | Do not rely on selection-gap drag checks alone. |
| `app/ml/indicator_product.py` | Load comparator evidence in an H5-aware way for this lane. | Do not reuse the current D+1/H1 helper assumptions as the D+5 decision rule. |
| `scripts/check_alpha_indicator_product_readiness.py`, `scripts/run_alpha_indicator_product_bundle.py`, `scripts/server/run_indicator_product_bundle_host.sh`, `scripts/server/verify_indicator_product_bundle_host.sh` | Keep bundle/readiness/host verification aligned with the new H5 comparator contract. | Do not leave server verification hard-coded to the legacy D+1-centric assumptions. |

## Canonical verification handoff

Minimum local verification before OCI rerun:

```bash
python3 -m compileall \
  app/ml/constants.py \
  app/ml/training.py \
  app/ml/inference.py \
  app/ml/validation.py \
  app/ml/indicator_product.py \
  app/selection/engine_v2.py \
  app/evaluation/alpha_shadow.py \
  app/ml/shadow_report.py \
  scripts/materialize_alpha_shadow_evaluation.py \
  scripts/render_alpha_shadow_comparison_report.py
```

Also require:
- targeted `pytest` coverage for the touched spec/training/inference/selection/evaluation paths
- local commit + server HEAD parity confirmation
- canonical OCI rerun with:
  - `train_end_date=2026-04-15`
  - `as_of_date=2026-04-15`
  - `shadow_start_selection_date=2026-03-16`
  - `shadow_end_selection_date=2026-04-15`

Post-rerun report generation commands:

```bash
python scripts/materialize_alpha_shadow_evaluation.py \
  --start-selection-date 2026-03-16 \
  --end-selection-date 2026-04-15 \
  --horizons 1 5 \
  --rolling-windows 20 60

python scripts/render_alpha_shadow_comparison_report.py \
  --start-selection-date 2026-03-16 \
  --end-selection-date 2026-04-15 \
  --horizons 1 5
```

Decision gate for the challenger lane:
- D+5 rolling-20 `>= +0.25%p` vs `alpha_swing_d5_v1`
- D+5 cohort `>= +0.20%p` vs `alpha_swing_d5_v1`
- no worse than `-0.25%p` vs `alpha_recursive_expanding_v1` H5 on cohort top5
- wins `2/3` buckets with no catastrophic bucket loss

## Review summary

The redesign is feasible in the current codebase, but only if it stays bounded:
- new challenger spec instead of mutating the existing D+5 control lane
- model-spec-specific H5 ranking override instead of a generic top5-binary change
- H5-aware comparator and bucket validation instead of reusing D+1-centric helpers
