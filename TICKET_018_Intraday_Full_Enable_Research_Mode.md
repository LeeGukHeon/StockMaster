# TICKET-018 — Intraday Full Enablement (Research Mode, Non-Trading)

## 1. Purpose

StockMaster already has the intraday feature stack split across the earlier tickets:

- TICKET-007: candidate-only intraday assist engine
- TICKET-008: intraday postmortem and regime-aware comparison
- TICKET-009: policy calibration / regime tuning / experiment management
- TICKET-010: policy meta-model / ML timing classifier v1
- TICKET-017: server scheduler / timer / orchestration

This ticket exists to **fully turn on the intraday stack in research mode** now that the user explicitly stated:

- they do **not** intend to follow these signals for actual purchases right now
- it is acceptable to **enable all intraday research features**
- they still want the system to remain **non-trading / no order routing**

The goal is not to invent a new intraday engine.
The goal is to **operationalize, expose, and default-enable** the existing intraday feature set so that the system continuously collects data, evaluates policies, displays diagnostics, and produces research outputs.

---

## 2. Core principle

Intraday is still treated as a **downstream execution/timing research layer** for candidates that came from the prior close selection process.

It is **not** a separate universe-wide stock picker.

The canonical flow remains:

1. prior close selection / portfolio candidate generation
2. next trading day intraday monitoring on candidate set only
3. raw policy action generation
4. adjusted policy action generation
5. meta-model overlay output generation
6. postmortem / comparison / calibration data accumulation

---

## 3. Required operating mode

### 3.1 Mode to enable
Enable the full intraday stack in **research mode**:

- intraday collection ON
- intraday assist timer ON
- raw policy evaluation ON
- adjusted policy evaluation ON
- meta-model prediction ON
- intraday postmortem ON
- comparison tables / diagnostics ON
- research-oriented UI panels ON
- optional Discord research summary ON

### 3.2 Hard prohibitions that remain in force
The following remain disabled by design:

- no broker order placement
- no auto-buy / auto-sell
- no active portfolio mutation directly from intraday signal
- no automatic promotion of new intraday policy
- no automatic promotion of new intraday meta-model
- no automatic replacement of close-based selection engine

---

## 4. What “full enablement” means in practice

This ticket means the system should expose and persist all three layers below for each eligible candidate:

### A. Raw timing policy layer
Examples:
- ENTER_NOW
- WAIT_RECHECK
- AVOID_TODAY
- DATA_INSUFFICIENT

### B. Regime-aware adjusted layer
Examples:
- raw action
- adjusted action
- adjustment profile
- regime family
- adjustment reason

### C. Meta-model overlay layer
Examples:
- keep / downgrade / bounded-upgrade style decision
- confidence / uncertainty / disagreement
- policy override value estimate
- panel probability output

All three layers must be visible, auditable, and attributable to the same candidate / decision point.

---

## 5. Scope

## In scope

### 5.1 Feature flags and default behavior
Create or normalize runtime flags so that **research profile defaults to enabled** for all intraday features.

Recommended toggles:
- `ENABLE_INTRADAY_ASSIST=true`
- `ENABLE_INTRADAY_POSTMORTEM=true`
- `ENABLE_INTRADAY_POLICY_ADJUSTMENT=true`
- `ENABLE_INTRADAY_META_MODEL=true`
- `ENABLE_INTRADAY_RESEARCH_REPORTS=true`
- `ENABLE_INTRADAY_DISCORD_SUMMARY=true|false` (configurable, but supported)
- `ENABLE_INTRADAY_WRITEBACK=true`

If a profile system already exists, define a canonical research profile where all intraday features are ON.

### 5.2 Candidate sourcing
Candidate-only principle remains in force.

The intraday engine should read from:
- latest active selection run
- latest active target book / portfolio candidate set
- optionally top-N ranked candidates if portfolio target book is not present

Define a deterministic candidate acquisition rule and document it.

### 5.3 Intraday data collection
Ensure the scheduler and runtime path collect/store candidate-level intraday data required by T007~T010:

- 1-minute bars
- execution / trade summary features
- quote summary features (if available from current provider path)
- liquidity / spread / gap / reversal proxies
- event timestamps
- action timestamps

Retention must remain candidate-only and TTL-governed.

### 5.4 Action generation and persistence
For each decision timestamp, persist:
- raw action
- raw reason codes
- raw scores / thresholds
- adjusted action
- adjusted reason codes
- regime family
- active policy id
- active meta-model id (if any)
- uncertainty / disagreement diagnostics
- action source lineage

### 5.5 UI activation
The following UI surfaces must be fully active and non-placeholder:

- Intraday Console
- Evaluation
- Stock Workbench intraday tab/section
- Research page intraday comparison area
- Ops / Health views for scheduler and intraday job status

The user must be able to answer these questions visually:
- what were today’s intraday candidates?
- what did raw policy say?
- what did adjusted policy say?
- what did meta-model say?
- what happened next?
- did timing add value over simple open entry?

### 5.6 Daily research outputs
Generate at least these research artifacts:

- intraday summary report
- intraday postmortem report
- raw vs adjusted vs meta-model comparison summary
- action distribution report
- missed winner / saved loss / false avoid summary

### 5.7 Notification layer
Support an intraday research summary notification path.

Recommended behavior:
- midday research summary (optional)
- end-of-day intraday summary (preferred)
- publish only concise research summary, not noisy minute-by-minute spam

### 5.8 Ops and health
Add/confirm monitoring for:
- timer execution count
- skip reasons
- lock contention
- candidate count
- missing data rate
- non-trading-day self-skip
- stale data conditions
- policy / meta-model activity state

---

## 6. Non-goals

This ticket does **not**:
- add broker integration
- add auto-order execution
- promote intraday ML policy automatically
- replace selection engine with intraday model
- broaden intraday data collection to all symbols
- store full-market raw quote streams long-term

---

## 7. Detailed implementation requirements

### 7.1 Runtime / scheduler
Confirm that T017 scheduler registry exposes all intraday jobs as active research jobs.

Required service availability:
- morning news
- after-close news
- daily close
- evaluation
- intraday assist
- optional intraday research reporting hook
- optional intraday Discord summary hook

### 7.2 Feature/state registry
Add one canonical intraday capability registry that records:
- feature slug
- enabled flag
- rollout mode
- blocking dependency
- current active policy id
- current active meta-model id
- report availability
- last successful run
- last degraded run
- last skip reason

### 7.3 Decision lineage contract
For each intraday decision row, ensure traceability back to:
- originating selection run
- originating ranking row
- originating target book row (if present)
- policy used
- meta-model used
- market regime snapshot used

### 7.4 Comparison contract
Implement or normalize a same-exit comparison framework for:
- OPEN_ALL baseline
- RAW_POLICY first-enter logic
- ADJUSTED_POLICY first-enter logic
- META_MODEL overlay result

### 7.5 Research-mode UX
The UI must clearly mark intraday outputs as:

- research / timing assist
- non-ordering
- not auto-executed
- comparison-oriented

Use explicit badges or callouts instead of hiding complexity.

### 7.6 Default-on configuration
In server/research mode, all intraday features should come up enabled by default without requiring manual per-page toggles.

### 7.7 Graceful degradation
If part of the stack is unavailable:
- raw action can still run
- adjusted action can still run without meta-model
- postmortem can still run on available layers
- UI must show degradation honestly

---

## 8. Suggested files / modules to add or update

This ticket does not mandate exact file names, but the implementation should likely touch areas like:

- `app/intraday/...`
- `app/ops/...`
- `app/ui/pages/07_Intraday_Console.py`
- `app/ui/pages/06_Evaluation.py`
- `app/ui/pages/05_Stock_Workbench.py`
- `app/ui/pages/02_Placeholder_Research.py`
- `app/ui/pages/10_Health_Dashboard.py`
- `scripts/run_intraday_assist_bundle.py`
- `scripts/render_intraday_summary_report.py`
- `scripts/render_intraday_postmortem_report.py` (if missing)
- `scripts/validate_intraday_research_mode.py`

If an existing placeholder file exists, convert it rather than duplicating it.

---

## 9. Acceptance criteria

This ticket is done only when all of the following are true:

1. intraday scheduler path is enabled in research mode by default
2. candidate-only intraday runs occur automatically on trading days
3. raw action, adjusted action, and meta-model output are all persisted and queryable
4. Intraday Console shows current candidates and all available decision layers
5. Evaluation / Research pages show same-exit comparison results
6. postmortem artifacts are generated without manual hand assembly
7. all outputs are clearly marked non-trading / research-only
8. no code path performs auto-ordering or auto-promotion
9. health/ops pages show intraday scheduler status and recent outcomes
10. validation and smoke scripts pass

---

## 10. Validation and smoke tests

Required checks:
- scheduler registry validation
- intraday capability registry validation
- intraday lineage validation
- same-exit comparison validation
- UI page contract validation
- navigation integrity validation
- research report artifact validation
- smoke run for one mocked or recent trading-day candidate set

At minimum produce one script that prints a concise pass/fail summary for intraday full enablement.

---

## 11. Operator notes

Since the user explicitly said they are **not** going to trade directly off the intraday outputs right now, it is acceptable to be aggressive in enabling:
- more diagnostics
- more comparison tables
- more model outputs
- more report visibility

However, that is **not** permission to remove safety boundaries.
Keep:
- no auto execution
- no auto deployment
- bounded overlays
- honest degraded status reporting

---

## 12. Definition of done summary

“Full enablement” for this ticket means:

- all intraday research layers are ON
- all of them run automatically
- all of them are visible in the UI
- all of them are recorded for later evaluation
- none of them can auto-trade or auto-promote
