# TICKET-019 — KRX LIVE OPEN API Activation and Market Data Integration

## 1. Purpose

`KRX_API_KEY` currently exists as a forward-compatibility setting, but the live KRX integration path has not been activated in the real ingestion pipeline.

The user now stated that KRX API approval has been completed.
This ticket exists to convert the current placeholder / seed-fallback posture into a **real, production-usable KRX OPEN API integration** wherever the approved services make sense for StockMaster.

This ticket should **not** assume that KRX OPEN API replaces every other provider.
It should activate KRX where KRX is the better or intended source, while preserving current working providers and fallback behavior.

---

## 2. Official constraints that must shape the implementation

The implementation must respect the official KRX OPEN API model:

1. usage requires a Data Marketplace account, authentication key issuance, and then **separate per-API service application/approval**
2. it is possible to have an auth key but still receive authorization errors for a service that was not separately approved
3. auth key usage has a lifetime / renewal cycle
4. the OPEN API is positioned around KRX statistical information interfaces
5. real-time distribution products are a separate KRX data distribution product family
6. request budgets and attribution / use restrictions exist
7. the application must remain personal / non-commercial unless the user later obtains a broader compliant license posture

Therefore:

- do not assume one global key unlocks all services
- do not assume KRX OPEN API is the same thing as a real-time streaming market-data feed
- do not replace KIS real-time/intraday feed with KRX OPEN API just because a key now exists
- build service-by-service capability detection and fallback

---

## 3. High-level objective

After this ticket:

- `ENABLE_KRX_LIVE=true` should activate real KRX client usage
- approved KRX services should be callable through a typed provider layer
- current seed fallback/reference-only behavior should become a fallback path, not the primary path
- data should carry source attribution (`krx_live`, `krx_seed`, `kis`, `dart`, `naver`, etc.)
- the system should clearly distinguish:
  - KRX OPEN API statistical/reference data
  - existing broker-provided quote / intraday data
  - existing news/fundamental sources

---

## 4. Recommended source-of-truth role for KRX in StockMaster

Use KRX LIVE first where applicable for:
- exchange reference/master data
- trading calendar / market day logic if approved service supports it
- market breadth / issue statistics
- investor-by-symbol or market-wide investor stats (if approved)
- short-selling / market action statistics (if approved)
- market status / exchange classification data
- EOD-level exchange-originating stats that are more appropriate from KRX than from broker-derived endpoints

Continue using other providers where they are still the correct source:
- KIS for operational intraday / quote / timing feed paths
- DART for filings / financial statements
- Naver news or current news path for news metadata

---

## 5. Scope

## In scope

### 5.1 Configuration model
Add/normalize env/config handling for:

- `ENABLE_KRX_LIVE=true|false`
- `KRX_API_KEY=...`
- `KRX_APPROVED_SERVICE_SLUGS=...` (comma-separated or equivalent)
- `KRX_BASE_URL=...` if needed
- optional request budget / throttle settings
- optional attribution toggle / legal banner toggle

Rules:
- `KRX_API_KEY` remains optional when `ENABLE_KRX_LIVE=false`
- `KRX_API_KEY` becomes required when `ENABLE_KRX_LIVE=true`
- service use must be gated by approved-service awareness

### 5.2 Real client implementation
Replace stub-only posture with a real KRX client abstraction that supports:
- auth header / auth parameter handling
- typed request helper
- retry with bounded backoff
- timeout handling
- structured error mapping
- rate-limit aware behavior
- explicit handling of 401/403/404/429 and malformed payloads
- request / response audit logging without leaking secrets

### 5.3 Service catalog and capability registry
Create a capability registry for KRX services:
- service slug
- service group
- approved flag
- enabled flag
- tested flag
- last success ts
- last auth failure ts
- last schema mismatch ts
- current fallback mode

This should allow StockMaster to know:
- which KRX services are approved
- which are safe to call
- which are currently falling back

### 5.4 Provider activation points
Activate real KRX usage in the relevant ingestion/materialization paths.

Priority activation order:

#### Priority A
- universe / reference data
- symbol master enrichment
- market classification / market segments
- trading calendar logic if supported by approved services
- market-wide basic statistics

#### Priority B
- short-selling related statistics
- investor activity / investor-by-symbol stats
- market action / measures / caution statistics
- issue / breadth statistics

#### Priority C
- additional exchange-originating end-of-day research stats
- optional cross-check / audit against existing provider values

### 5.5 Fallback contract
If a service is:
- not approved
- temporarily unavailable
- structurally mismatched
- over quota
- non-compliant for the intended use

then the pipeline must:
- fall back safely to existing provider or seed path where applicable
- record degraded status explicitly
- avoid hard failing the entire platform unless the service is designated critical

### 5.6 Storage and source attribution
Persist clear provenance:
- provider name
- service slug
- pull time
- request version if needed
- schema version
- fallback indicator

Do not silently mix KRX live data and seed fallback without labeling.

### 5.7 UI / documentation / attribution
Where KRX-driven statistics are surfaced, add source attribution and any required usage labeling.

Also add a docs/help section explaining:
- what KRX live integration is powering
- what still comes from KIS/DART/news providers
- what happens when a service is not approved or fails

### 5.8 Ops / health
Add KRX-specific health visibility:
- auth key present / absent
- approved service count
- success / failure by service
- quota/budget consumption
- last 401 / 403 / 429
- fallback occurrences
- schema drift warnings

---

## 6. Non-goals

This ticket does **not**:
- replace all KIS functionality
- turn KRX OPEN API into a streaming feed handler
- add commercial redistribution support
- widen app usage beyond compliant personal / non-commercial posture
- auto-consume every visible service in the KRX catalog

---

## 7. Required implementation details

### 7.1 Compatibility-first rollout
Do not break current working paths just because KRX is now available.

Rollout sequence:
1. enable KRX live client
2. validate service-by-service
3. promote selected KRX services to primary source
4. keep fallback for all activated paths

### 7.2 Service-by-service whitelisting
The implementation must use an explicit allowlist of approved services rather than “call whatever exists”.

### 7.3 Error semantics
Map common cases:
- unauthorized key
- key present but service not approved
- quota exceeded or throttled
- temporary service issue
- schema mismatch
- empty but valid response
- business-day no-data response

### 7.4 Data contract alignment
For each activated KRX-backed dataset, define:
- grain
- unique key
- required columns
- freshness expectation
- rerun semantics
- fallback semantics

This should align with the audit/contract work already implemented.

### 7.5 Integration with existing tickets
The following areas should directly benefit:
- universe sync
- calendar logic / freshness logic
- market pulse
- leaderboard support features
- risk/breadth context
- postmortem analytics where exchange-level stats help
- ops health dashboards

### 7.6 Legal/compliance posture
Do not bury this.
The docs and UI help must make it clear that:
- KRX data usage is governed by KRX terms
- third-party redistribution is restricted
- screens using KRX statistics may require attribution text
- the current platform is personal / non-commercial in scope unless explicitly relicensed later

---

## 8. Suggested file/module impact

Likely areas to touch:
- `app/settings.py`
- `app/providers/krx/client.py`
- `app/providers/krx/reference.py`
- `app/providers/krx/...` additional modules as needed
- `app/ingestion/universe_sync.py`
- `app/ingestion/...` KRX-backed materializers
- `app/storage/...`
- `app/ui/pages/04_Market_Pulse.py`
- `app/ui/pages/10_Health_Dashboard.py`
- `app/ui/pages/11_Docs_Help.py`
- `scripts/validate_krx_live_integration.py`
- `scripts/smoke_krx_live_services.py`
- `docs/KRX_LIVE_INTEGRATION.md`

Use the repository’s current patterns; do not force a parallel architecture.

---

## 9. Acceptance criteria

This ticket is complete only when all of the following are true:

1. `ENABLE_KRX_LIVE=true` activates real KRX client usage
2. service approval is handled per service, not assumed globally
3. at least the Priority A activation paths use live KRX where approved
4. clear fallback behavior exists when services are unavailable or unapproved
5. all KRX-backed tables/paths record source attribution
6. Ops/Health pages expose KRX live status and failures
7. docs explain the live integration and compliance constraints
8. validation and smoke scripts pass
9. current non-KRX flows continue to work if KRX live is disabled
10. no code path incorrectly treats KRX OPEN API as a replacement for broker real-time feed

---

## 10. Validation / smoke tests

Required:
- config validation with `ENABLE_KRX_LIVE=false`
- config validation with `ENABLE_KRX_LIVE=true`
- one or more approved live service smoke calls
- unauthorized/unapproved service handling test
- fallback handling test
- source attribution verification
- UI contract validation for KRX health/help presence

If live services cannot be hit in CI/local, provide dependency-injected mock coverage plus a manual smoke script for the server environment.

---

## 11. Definition of done summary

This ticket is done when KRX transitions from:

- placeholder config
- stub client
- seed fallback reference path

to:

- real approved-service-aware live provider
- selective primary-source activation
- explicit provenance and fallback
- operational health visibility
- compliant personal-use documentation
