# StockMaster OMX Workflow Addendum

This repository uses OMX workflow lanes aggressively. The rules below are **mandatory preflight gates** for any future `deep-interview -> ralplan -> ralph` handoff.

## 1) Stage-gate artifacts are mandatory

### Before leaving `$deep-interview`
Do not hand off until the current lane has a concrete artifact that captures all of:
- task statement
- desired outcome
- bounded scope
- success criteria
- non-goals
- constraints / decision boundaries
- authoritative evidence sources / baseline artifact

A later lane must not infer these from chat history alone.

### Before leaving `$ralplan`
Do not hand off to execution until all required planning artifacts exist and are current:
- `prd-*.md`
- `test-spec-*.md`
- the approved `ralplan-*.md` handoff artifact
- explicit baseline / comparator definitions
- canonical run recipe
- acceptance / fail gates
- rollout / preserve / fallback rules when server execution can affect live paths

If any of these are missing, stale, or contradictory, stop and repair the plan first.

### Before entering `$ralph`
Execution is blocked until the operator verifies that the approved PRD + test spec + handoff artifact exist, are the latest artifacts for the lane, and are specific enough to map directly into code and verification steps.

## 2) Stale mode / stale command cleanup is mandatory between lanes

Before switching modes or starting a new lane:
- inspect active OMX/session state
- cancel/terminalize stale prior modes first
- clear stale `skill-active` residue if it survives cancellation
- do not begin a new `deep-interview`, `ralplan`, or `ralph` lane while an unrelated stale mode marker is still active

Required principle:
- **clean state first, then transition**
- never stack a new lane on top of stale `ralph`, `ralplan`, or `skill-active` residue

If `omx cancel` leaves residue, clear the matching session-scoped state and any legacy root compatibility state before proceeding.

## 3) Plan/code/server parity is a hard gate

Before any server rerun or OCI execution:
1. freeze the plan contract as an explicit checklist
   - formulas
   - lookup timing / source of truth
   - band / threshold construction
   - horizon / model scope
2. verify code matches that checklist locally
3. run local verification
4. create a git commit that records the exact validated code
5. prefer push + server pull; if deployment uses another mechanism, a GitHub commit must still exist for the exact logic being executed
6. verify server parity before execution:
   - local commit SHA
   - remote commit SHA / `git rev-parse HEAD`
   - remote dirty state / `git status --short`
7. only then run the server job

Never treat “the job ran” as proof that “the approved plan ran.”

## 4) Result validation and contract validation are separate

A lane is not complete unless both are true:
- **result validation:** the tests / reruns / metrics finished and were interpreted
- **contract validation:** the executed code actually matched the approved plan artifact

Do not close a lane, write a success/failure conclusion, or escalate to the next redesign stage if contract validation has not been checked explicitly.

## 5) OCI DuckDB lock rule remains in force

If the OCI runtime DuckDB is locked by an active StockMaster writer:
- do **not** create temporary snapshot copies
- do **not** run ad-hoc live DB inspection queries
- monitor by process status and log tails only
- inspect the DB only after the writer exits

## 6) Preferred transition checklist

Use this order every time:
1. stale-state cleanup
2. artifact existence check
3. artifact completeness check
4. plan -> code conformance check
5. git commit / server parity check
6. local verification
7. server execution
8. result validation
9. contract validation
10. only then close or escalate the lane
