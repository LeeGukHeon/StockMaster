# Selection Raw Drag D5 Review

This document is the tracked-repo review companion to `.omx/plans/ralplan-selection-raw-drag-d5-2026-04-22.md`.
The plan itself stays under `.omx/plans/` (read-only / ignored for team-worker delivery), so the brownfield review findings and execution guardrails are recorded here in versioned documentation.

## Brownfield review findings

1. **Keep the D5 lane isolated through the model-spec-specific weight override.**
   - `app/selection/engine_v2.py` already carries a dedicated `SELECTION_V2_D5_PRIMARY_WEIGHTS` table that activates only for `alpha_swing_d5_v2`.
   - The review supports the plan's isolation rule: do not retune the generic `top5_binary` branch unless the wider blast radius is explicitly re-approved.

2. **`report_candidates` is still a compatibility surface, not a distinct drag diagnostic.**
   - In `app/selection/engine_v2.py`, `top5_binary` models still resolve `report_candidate_flag` from the same final-selection ordering and a fixed top-5 cut.
   - In `app/evaluation/alpha_shadow.py`, the evaluation layer reconstructs `report_candidates` from the same ordered `final_selection_value` rows.
   - That means the plan is correct to remove `report_candidates` from primary success criteria unless a truly separate diagnostic is introduced.

3. **The current validation path already carries the D5-specific drag and floor gates; keep it aligned with the frozen baseline.**
   - `app/ml/validation.py` already adds manifest-backed `d5_primary_drag_improvement_*` checks, `d5_primary_selected_top5_floor_*` checks, comparator checks versus `alpha_swing_d5_v1` / `alpha_recursive_expanding_v1`, and the three D5 bucket checks.
   - The review guardrail is therefore maintenance-oriented: preserve those D5-specific checks and update them together if the frozen baseline manifest or success thresholds change.
   - Do not let a later refactor collapse this lane back to the generic selection-gap warning thresholds only.

4. **Downstream compatibility consumers still read `report_candidates`, so the output contract must stay backward-compatible unless scope widens.**
   - `app/ml/indicator_product.py` still blocks or warns on missing / underperforming `report_candidates` rows.
   - `app/reports/discord_eod.py` still prints `report_candidates` alongside selected-top5 and drag metrics for user-facing summaries.
   - `app/release/reporting.py` is already drag-first, which is good, but the broader reporting stack still expects the existing summary columns to stay present.
   - Practical guardrail: keep the row/column contract stable in phase 1-3, even if success criteria stop treating `report_candidates` as decisive.

5. **The D+1 auxiliary section is already on the safer summary-backed path; do not regress it.**
   - `app/ml/shadow_report.py` now renders the D+1 auxiliary interpretation directly from summary rows for the H1 comparator models instead of relying on H5-only pairwise joins.
   - The plan should preserve this separation as a regression guardrail rather than treating it as a still-open design gap.

6. **`tests/unit/test_selection_engine_v2_overlay.py` remains non-authoritative until repaired.**
   - The file still references `engine_v2._compute_crowding_penalty_score` / `engine_v2._augment_reason_tags` without importing an `engine_v2` module binding.
   - The named D5 unit/integration tests in the PRD and test spec should remain the frozen gate until this overlay test is repaired.

## File-level execution guardrails

| Surface | Required handling | Do not do |
| --- | --- | --- |
| `app/selection/engine_v2.py` | Preserve the D5-v2-specific override path and keep any drag-reduction tuning explicitly tied to `alpha_swing_d5_v2`. | Do not silently retune the generic `top5_binary` branch for every model. |
| `app/evaluation/alpha_shadow.py` | Keep `report_candidates` explicitly documented as a compatibility/reporting slice derived from the same ranking surface unless a new diagnostic path is added. | Do not claim drag-attribution independence from `report_candidates` while it is still cut from the same ordering. |
| `app/ml/validation.py` | Preserve the existing manifest-backed D5 drag-improvement and selected-top5-floor checks alongside the comparator and bucket checks. | Do not regress this lane back to the generic drag warning thresholds only. |
| `app/ml/shadow_report.py` | Preserve the separate D+1 auxiliary summary section and keep the D5 bucket/comparator sections centered on H5 rows. | Do not collapse D+1 auxiliary interpretation back into H5-only pairwise joins. |
| `app/ml/indicator_product.py`, `app/reports/discord_eod.py`, `app/release/reporting.py` | Treat them as compatibility consumers that must continue receiving stable summary columns/rows during this lane. | Do not remove or rename `report_candidates` output fields without an explicit scope expansion and matching test updates. |
| `tests/unit/test_selection_engine_v2_overlay.py` | Either repair it in a dedicated follow-up or keep it excluded from the authoritative gate. | Do not advertise it as a frozen verification gate in the current lane. |

## Canonical verification handoff

Minimum local verification for the implementation lane should keep the plan's named commands, plus an explicit note that the overlay test remains a known non-gate until repaired:

```bash
python3 -m compileall \
  app/ml/constants.py \
  app/selection/engine_v2.py \
  app/ml/validation.py \
  app/evaluation/alpha_shadow.py \
  app/ml/shadow_report.py

python3 -m pytest \
  tests/unit/test_alpha_model_specs.py::test_alpha_swing_d5_v2_matches_frozen_contract \
  tests/unit/test_indicator_product.py -q

python3 -m pytest \
  tests/integration/test_alpha_shadow_pipeline.py::test_materialize_alpha_shadow_evaluation_summary_emits_d5_bucket_rows_for_swing_v2 \
  tests/integration/test_alpha_shadow_comparison_report.py::test_render_alpha_shadow_comparison_report_includes_d5_focus_sections \
  tests/integration/test_alpha_model_v1.py::test_run_alpha_indicator_product_bundle_d5_focus_enforces_comparator_lock -q
```

Additional review-time reminder:
- treat `tests/unit/test_selection_engine_v2_overlay.py` as a known broken/non-authoritative test until its imports are repaired.
- keep the baseline manifest and the D5-specific validation thresholds in sync; if one changes, the other must move with it.

## Review summary

The assigned ralplan is directionally sound and already matches several current-code guardrails, especially D5-v2 isolation, the manifest-backed D5 validation checks, and the summary-backed D+1 auxiliary report path. The main brownfield risks to keep explicit are: `report_candidates` still being a compatibility-derived slice instead of an independent diagnostic, and downstream consumers still expecting that compatibility surface to remain stable while the lane is being tuned.
