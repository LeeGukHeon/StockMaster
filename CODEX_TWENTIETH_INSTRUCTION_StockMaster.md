# CODEX SECOND-PHASE INSTRUCTION — TICKET-019 KRX LIVE OPEN API Activation

You are working inside the StockMaster repository at the project root.

## Read first
Before coding, read these documents from the repository root:

- KR_Stock_Research_Platform_v1_Implementation_Spec.md
- TICKET_001_Universe_Calendar_Provider_Activation.md
- TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md
- TICKET_004_Flow_Selection_Engine_Discord_Report.md
- TICKET_012_Operational_Stability_Batch_Recovery_Disk_Guard_Monitoring_Health_Dashboard.md
- TICKET_016_DB_Audit_Integration_Checklist_Gap_Remediation_000_013.md
- TICKET_019_KRX_Live_API_Activation_and_Market_Data_Integration.md

## Goal
Implement TICKET-019.

The user now has KRX API approval.
Today, `KRX_API_KEY` exists largely as forward-compatibility and the real ingestion path still behaves like fallback/stub-first.
Change that so KRX live integration becomes real and operationally useful.

## Critical constraints
- Do **not** assume that one auth key automatically authorizes every KRX service.
- Do **not** assume that KRX OPEN API replaces KIS real-time / intraday feeds.
- Do **not** break existing working flows if KRX live is disabled or partially approved.
- Keep a service-by-service capability registry and fallback path.
- Preserve personal / non-commercial posture and source attribution requirements in docs/help.

## Required implementation direction
1. Normalize config:
   - `ENABLE_KRX_LIVE`
   - `KRX_API_KEY`
   - approved service allowlist
2. Turn `app/providers/krx/client.py` into a real, typed live client.
3. Add service capability registry and health visibility.
4. Activate KRX live first for Priority A paths:
   - universe/reference/master enrichment
   - market classification
   - trading calendar if approved
   - market-wide basic stats
5. Then wire Priority B paths if feasible without destabilizing current code:
   - short sell stats
   - investor stats
   - issue/breadth stats
6. Keep explicit fallback behavior when a service is unavailable/unapproved.
7. Add validation/smoke scripts and docs.

## Important non-goals
- No attempt to replace KIS as the operational intraday provider.
- No streaming feed architecture from KRX OPEN API.
- No commercial redistribution assumptions.

## Deliverables
Implement code, tests, scripts, docs, and UI health/help updates needed for TICKET-019.

At the end, report back with:
1. file list added/changed
2. which KRX service families are now live
3. which paths still fallback
4. validation commands and results
5. known limitations
6. whether commit/push was performed
