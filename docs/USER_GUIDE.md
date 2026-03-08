# StockMaster User Guide

## Purpose
StockMaster is a Korean stock research platform focused on post-close analysis, explainable ranking, intraday timing assistance, portfolio proposal, and operational monitoring.

It is not an auto-trading product.

## Core Workflow
1. Start on `오늘` to confirm current truth, active policies, freshness, and latest report links.
2. Check `시장 현황` for regime, breadth, volatility, and narrative context.
3. Review `리더보드` for top-ranked names and risk/uncertainty flags.
4. Move to `포트폴리오` and `포트폴리오 평가` for target book, rebalance, holdings, NAV, and execution-mode comparison.
5. Use `장중 콘솔` during the session to review raw action, adjusted action, meta overlay, and final action.
6. Use `사후 평가` and `종목 분석` for matured outcomes, miss reasons, and postmortem trace.
7. Use `리서치 랩` for model, policy, and calibration diagnostics.
8. Use `운영` and `헬스 대시보드` to inspect failures, locks, alerts, retention, and recovery status.
9. Use `문서 / 도움말` for glossary, workflow, latest report index, and release-candidate checks.

## What The Badges Mean
- `성공`: latest materialized output is present and within expected freshness.
- `저하`: output exists, but quality, completeness, or freshness needs caution.
- `경고`: attention required, but not necessarily blocking.
- `치명`: blocking or near-blocking operational state.

## Ground Rules
- Do not treat displayed actions as execution instructions.
- Use existing materialized outputs as the source of truth.
- If a page shows `stale`, `degraded`, or missing artifact warnings, treat them as real system state, not cosmetic messages.
