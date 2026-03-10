# TICKET-020 — KRX 승인 후 Live Enablement / Smoke / Integration Activation

## 목적
KRX OPEN API 활용신청이 승인된 후, StockMaster에서 승인된 서비스만 안전하게 활성화한다. 이 티켓은 **인증 성공 여부**만 보는 수준이 아니라, **서비스별 smoke test**, **요청 예산 관리**, **fallback 유지**, **UI 출처 표시**, **운영 가드**까지 포함한다.

본 티켓은 KRX OPEN API의 공식 이용 흐름인 **인증키 신청 → 서비스 활용 신청 → 관리자 승인 후 이용**을 전제로 한다. 승인 전에는 `ENABLE_KRX_LIVE=false` 상태를 유지해야 한다. 또한 KRX 약관상 **비상업적 목적 사용**, **제3자 제공 금지**, **화면상 “한국거래소 통계정보” 표시**, **하루 10,000회 제한**, **인증키 이용기간 1년** 조건을 반영해야 한다.

## 현재 전제
사용자가 아래 8개 서비스에 대해 활용신청을 완료했다.

1. 유가증권 일별매매정보
2. 코스닥 일별매매정보
3. 유가증권 종목기본정보
4. 코스닥 종목기본정보
5. KRX 시리즈 일별시세정보
6. KOSPI 시리즈 일별시세정보
7. KOSDAQ 시리즈 일별시세정보
8. ETF 일별매매정보

중요: **실제 활성화는 승인 확인 후에만** 한다. 승인되지 않은 서비스는 registry에 남겨도 호출 금지 상태여야 한다.

---

## 범위

### 이번 티켓에 포함
- KRX live feature flag 활성화 경로 구현
- 승인 서비스 registry / allowlist 구현
- 승인 서비스별 smoke test 스크립트 구현
- KRX reference/market integration을 seed fallback 위에서 live 우선 구조로 변경
- KRX 호출량 카운터 및 일일 예산 가드 구현
- 실패 시 fallback-to-seed / fallback-to-existing behavior 유지
- UI / Docs / Report에 KRX 출처 표기 반영
- Ops / Health / Docs 페이지에 KRX live 상태 노출

### 이번 티켓에 포함하지 않음
- KRX 비승인 서비스 호출 시도
- KRX를 실시간 호가/체결 primary source로 교체
- 자동 주문 / 자동매매 기능
- 외부 공개 서비스로의 재배포 기능
- KRX raw payload 장기 대량 저장

---

## 구현 원칙

### 1. 승인 서비스만 명시적으로 활성화
`.env`에 다음 계층을 둔다.

- `ENABLE_KRX_LIVE=false|true`
- `KRX_API_KEY=`
- `KRX_ALLOWED_SERVICES=` (comma-separated slug list)
- `KRX_DAILY_REQUEST_BUDGET=` (default conservative)
- `KRX_REQUEST_TIMEOUT_SECONDS=`
- `KRX_SOURCE_ATTRIBUTION_LABEL=한국거래소 통계정보`

규칙:
- `ENABLE_KRX_LIVE=false` 이면 KRX 호출 금지
- `ENABLE_KRX_LIVE=true` 이더라도 `KRX_ALLOWED_SERVICES`에 없는 서비스는 호출 금지
- 승인 서비스 slug는 코드 registry에서 canonical 하게 관리

### 2. KRX는 reference/statistics 우선
현재 구조상 KIS는 실시간/장중 primary source로 유지한다.
KRX는 다음 영역에서 우선 사용한다.

- 종목기본정보 보강
- 일별매매정보 기준값 보강
- 지수/시장 통계 기준값 보강
- ETF 일별 흐름 보강

### 3. fallback 유지
실패 시 동작 순서:
1. KRX live 호출 시도
2. 실패/timeout/권한 오류 → structured warning 기록
3. 기존 seed/reference fallback 사용
4. run status는 `DEGRADED_SUCCESS` 또는 `BLOCKED` 규칙에 맞춰 기록

### 4. deterministic + auditable
모든 KRX 호출은 다음을 남겨야 한다.
- `service_slug`
- `request_ts`
- `response_status`
- `http_status`
- `rows_received`
- `latency_ms`
- `used_fallback`
- `run_id`
- `as_of_date`

### 5. 요청 예산 보수적으로 운용
KRX 약관상 1일 10,000회 제한이 있으므로 v1에서는 훨씬 보수적으로 잡는다.
권장 default:
- `KRX_DAILY_REQUEST_BUDGET=1000`

임계치:
- 80% 초과 시 warning
- 95% 초과 시 non-critical KRX fetch skip
- 100% 도달 시 fallback only

---

## 승인 후 즉시 해야 할 작업

### A. 설정 반영
`env/.env.server` 또는 서버 `.env`에 아래 반영

```env
ENABLE_KRX_LIVE=true
KRX_API_KEY=...
KRX_ALLOWED_SERVICES=stock_kospi_daily_trade,stock_kosdaq_daily_trade,stock_kospi_symbol_master,stock_kosdaq_symbol_master,index_krx_daily,index_kospi_daily,index_kosdaq_daily,etf_daily_trade
KRX_DAILY_REQUEST_BUDGET=1000
KRX_REQUEST_TIMEOUT_SECONDS=20
KRX_SOURCE_ATTRIBUTION_LABEL=한국거래소 통계정보
```

실제 slug 이름은 Codex가 registry에서 canonical 하게 정의한다. 사람이 읽는 한글명과 slug mapping 문서를 남긴다.

### B. 승인 서비스 registry 구현
예상 파일:
- `app/providers/krx/registry.py`
- `docs/KRX_SERVICE_REGISTRY.md`

필드 예시:
- `service_slug`
- `display_name_ko`
- `category`
- `approval_required=true`
- `enabled_by_env`
- `fallback_policy`
- `expected_usage`
- `request_cost_weight`

### C. smoke test 구현
예상 스크립트:
- `scripts/krx_smoke_test.py`
- `scripts/krx_smoke_test_all_allowed.py`

테스트 내용:
- API key 존재
- `ENABLE_KRX_LIVE=true`
- 승인 서비스별 최소 1회 샘플 호출
- HTTP 상태 / payload shape / row count / parse success 확인
- 실패 원인 분류
  - 인증 문제
  - 비승인 서비스
  - timeout
  - payload schema drift
  - empty result

### D. live integration 연결
기존 seed fallback adapter를 live-first/fallback-second 구조로 변경한다.
대상 예시:
- `app/providers/krx/reference.py`
- `app/ingestion/universe_sync.py`
- 시장 지수 적재 bundle
- ETF reference/statistics 적재 bundle

규칙:
- Universe / calendar / symbol master는 KRX live 성공 시 live 사용
- 실패 시 seed fallback 사용
- 결과 provenance를 저장

### E. 출처 표기 반영
아래 위치에 KRX attribution을 추가한다.
- Market Pulse footer
- Leaderboard footnote (KRX 데이터가 사용된 경우만)
- Docs / Help 페이지
- report bundle metadata

문구 기본값:
- `일부 지표는 한국거래소 통계정보를 사용하여 구성되었습니다.`

---

## 스키마 / 저장 계약
가능하면 기존 `ops` / `serving` 계층에 추가한다.

### 신규 권장 테이블
- `ops.fact_external_api_request_log`
- `ops.fact_external_api_budget_snapshot`
- `ops.fact_krx_service_status`
- `serving.fact_source_attribution_snapshot`

#### ops.fact_external_api_request_log
컬럼 예시:
- `request_id`
- `provider_name` (`krx`)
- `service_slug`
- `run_id`
- `as_of_date`
- `request_ts`
- `http_status`
- `status`
- `latency_ms`
- `rows_received`
- `used_fallback`
- `error_code`
- `error_message`

#### ops.fact_external_api_budget_snapshot
컬럼 예시:
- `provider_name`
- `snapshot_ts`
- `date_kst`
- `request_budget`
- `requests_used`
- `usage_ratio`
- `throttle_state`

#### ops.fact_krx_service_status
컬럼 예시:
- `service_slug`
- `display_name_ko`
- `approval_expected`
- `enabled_by_env`
- `last_smoke_status`
- `last_smoke_ts`
- `last_success_ts`
- `last_http_status`
- `last_error_class`

#### serving.fact_source_attribution_snapshot
컬럼 예시:
- `as_of_date`
- `page_slug`
- `component_slug`
- `source_label`
- `provider_name`
- `active_flag`

---

## UI / 문서 요구사항

### Ops 페이지
- KRX live enabled 여부
- allowed services 수
- last smoke result
- request usage today
- fallback events today

### Health Dashboard
- `KRX_LIVE_OK`, `KRX_LIVE_DEGRADED`, `KRX_LIVE_BLOCKED`
- approval/service mismatch 경고

### Docs / Help
- 승인 서비스 registry 표
- 현재 live on/off 상태
- KRX 출처 및 이용 제약 안내

### README / 운영 문서
- 승인 전 / 승인 후 설정 차이
- smoke test 실행법
- rollback 방법

---

## 스크립트 / CLI 요구사항

최소 구현:
- `python scripts/krx_smoke_test.py --service-slug <slug>`
- `python scripts/krx_smoke_test_all_allowed.py`
- `python scripts/validate_krx_live_configuration.py`
- `python scripts/render_krx_service_status_report.py`

권장 추가:
- `python scripts/run_krx_reference_refresh.py`
- `python scripts/run_krx_index_refresh.py`

---

## 완료 기준
- 승인된 8개 서비스가 registry에 canonical slug로 등록되어 있다.
- `ENABLE_KRX_LIVE=true` 상태에서 승인 서비스 smoke test가 통과한다.
- universe/reference/index/ETF 경로에서 KRX live 우선, fallback 유지가 동작한다.
- Ops/Health/Docs에 KRX 상태가 노출된다.
- KRX request budget이 집계되고 경고/차단 동작이 있다.
- 리포트/화면에 KRX 출처 표기가 들어간다.
- KRX live 실패 시 시스템 전체가 멈추지 않고 `DEGRADED_SUCCESS` 또는 fallback으로 유지된다.

---

## 검증 명령 예시

```bash
python scripts/validate_krx_live_configuration.py
python scripts/krx_smoke_test_all_allowed.py
python scripts/render_krx_service_status_report.py
python -m pytest -q tests/unit/test_krx_registry.py tests/integration/test_krx_live_framework.py
```

---

## 하지 말아야 할 것
- 승인되지 않은 KRX 서비스 추정 호출
- KRX 데이터를 제3자 공개 서비스처럼 재배포
- 출처 표기 누락
- budget guard 없이 무차별 호출
- KRX live 실패를 fatal error로 처리
- 장중 실시간 KIS 경로를 KRX로 무리하게 대체

---

## Codex 구현 포인트
Codex는 먼저 루트의 기존 문서들과 T019를 읽고, 현재 KRX stub 구조를 확인한 뒤 아래를 수행한다.

1. service registry 도입
2. env/validation 강화
3. smoke test 및 status report 구현
4. live-first/fallback-second integration 연결
5. Ops / Health / Docs 노출
6. tests / docs / validation scripts 업데이트

구현 완료 후에는 **승인된 8개 서비스 기준으로 실제 smoke 결과**, **fallback 동작 결과**, **요청 예산 snapshot 결과**를 요약 보고해야 한다.
