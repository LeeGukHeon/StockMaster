# KRX Live Integration

KRX live integration is used for exchange-originating reference and daily market statistics.
It does not replace the existing broker intraday path.

## 목적

- 승인된 KRX OPEN API만 안전하게 호출
- symbol master / index statistics / ETF daily trade를 live-first로 사용
- 실패 시 seed 또는 기존 provider 경로로 안전하게 fallback
- 요청 예산, 상태, 출처 표기를 운영 화면에서 추적

## 활성화 조건

- `ENABLE_KRX_LIVE=true`
- `KRX_API_KEY` 존재
- `KRX_ALLOWED_SERVICES`에 승인된 canonical slug 명시

예시:

```env
ENABLE_KRX_LIVE=true
KRX_API_KEY=...
KRX_ALLOWED_SERVICES=stock_kospi_daily_trade,stock_kosdaq_daily_trade,stock_kospi_symbol_master,stock_kosdaq_symbol_master,index_krx_daily,index_kospi_daily,index_kosdaq_daily,etf_daily_trade
KRX_DAILY_REQUEST_BUDGET=1000
KRX_REQUEST_TIMEOUT_SECONDS=20
KRX_SOURCE_ATTRIBUTION_LABEL=한국거래소 통계정보
```

## 사용 원칙

- 승인되지 않은 서비스는 호출하지 않음
- live 호출 실패는 시스템 전체 fatal로 만들지 않음
- KIS intraday / quote 경로는 그대로 유지
- raw 전시장 장기 저장으로 넓히지 않음
- 단일 `main.duckdb` write discipline 유지

## 현재 live-first 적용 영역

- universe/reference enrichment
- symbol master 보강
- KRX/KOSPI/KOSDAQ index daily statistics
- ETF daily trade statistics

## fallback 규칙

다음 경우는 fallback 또는 degraded로 처리한다.

- `ENABLE_KRX_LIVE=false`
- API key 없음
- allowlist에 없는 서비스
- service URL 누락
- timeout / 401 / 403 / 429 / malformed payload
- empty but valid payload

fallback이 발생해도 아래는 남긴다.

- request log
- budget snapshot
- service status
- source attribution snapshot

## 요청 예산

보수적 기본값:

- `KRX_DAILY_REQUEST_BUDGET=1000`

예산 상태:

- 80% 이상: `WARNING`
- 95% 이상: `FALLBACK_ONLY`
- 100% 이상: `BLOCKED`

## 출처 표기

KRX 기반 통계는 다음 문구를 사용한다.

`한국거래소 통계정보`

노출 위치:

- Ops
- Health Dashboard
- Docs / Help
- KRX status report

## 검증 명령

```powershell
python scripts/validate_krx_live_configuration.py
python scripts/krx_smoke_test.py --service-slug etf_daily_trade --as-of-date 2026-03-06
python scripts/krx_smoke_test_all_allowed.py --as-of-date 2026-03-06
python scripts/render_krx_service_status_report.py --as-of-date 2026-03-06 --dry-run
```

## known limitation

- KRX OPEN API는 실시간 체결/호가 feed가 아니다
- intraday trading feed primary source는 여전히 KIS다
- 승인 서비스 외 호출은 intentionally blocked 상태로 유지한다
