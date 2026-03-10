# CODEX SECOND-PHASE INSTRUCTION — TICKET-018 Intraday Full Enablement (Research Mode)

You are working inside the StockMaster repository at the project root.

## Read first
Before changing code, read and follow these documents in the repository root:

- KR_Stock_Research_Platform_v1_Implementation_Spec.md
- TICKET_007_Intraday_Candidate_Assist_Engine.md
- TICKET_008_Intraday_Postmortem_Regime_Aware_Strategy_Comparison.md
- TICKET_009_Policy_Calibration_Regime_Tuning_Experiment_Ablation.md
- TICKET_010_Policy_Meta_Model_ML_Timing_Classifier_v1.md
- TICKET_017_Automation_Scheduler_Timer_Orchestration.md
- TICKET_018_Intraday_Full_Enable_Research_Mode.md

## Goal
Implement TICKET-018.

This is **not** a new intraday engine.
This is a **full enablement / operationalization / research-mode default-on** ticket for the existing intraday stack.

The user explicitly said:
- they are not going to follow these intraday outputs for actual buying right now
- enabling all intraday research functionality is acceptable

So:
- turn intraday research features ON by default in research/server mode
- keep no-order / no-auto-promotion safety boundaries in place

## What must be true after implementation
- intraday scheduler path is active in research mode
- candidate-only intraday jobs persist raw action, adjusted action, and meta-model output
- Intraday Console is no longer a narrow or partially hidden view; it should surface all research layers
- Evaluation / Research / Stock Workbench / Health views expose intraday diagnostics
- same-exit comparison is visible and queryable
- postmortem and summary artifacts exist
- UI labels intraday outputs as research / non-trading
- no auto-ordering
- no automatic policy/model promotion

## Required implementation direction
1. Add or normalize capability flags / registry for intraday research features.
2. Default-enable them in research/server mode.
3. Ensure scheduler/bundle execution persists all three decision layers:
   - raw policy
   - regime-adjusted policy
   - meta-model overlay
4. Make lineage queryable from decision rows back to selection / ranking / portfolio inputs.
5. Expose all intraday layers in the UI.
6. Add or update validation / smoke scripts for intraday research mode.
7. Update docs/help pages.

## Keep these constraints
- Candidate-only intraday principle stays in force.
- Do not add broker integration.
- Do not add auto-execution.
- Do not automatically activate a newly trained policy or meta-model.
- Do not widen raw intraday data storage to full market long-term retention.

## Deliverables
Implement the code, tests, docs, and scripts needed to satisfy TICKET-018.
At the end, report back with:
1. added/modified file list
2. key behavior changes
3. exact validation commands run
4. pass/fail results
5. remaining known limitations
6. whether commit/push was performed
