# TICKET-001 — 종목마스터 + 거래일 캘린더 + 공식 Provider 실가동

- 문서 목적: TICKET-000 foundation 이후, Codex가 바로 이어서 구현할 **첫 실데이터 연동 작업**의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
  - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
  - `TICKET_000_Foundation_and_First_Work_Package.md`
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- 전제 상태: TICKET-000의 foundation이 최소 실행 가능한 상태여야 함
- 우선순위: 최상
- 기대 결과: “실제 API 키를 연결하면 종목 유니버스와 거래일 캘린더가 채워지고, KIS/DART의 기초 조회가 동작하는 상태”

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **Provider stub을 실제 통신 가능한 provider로 올리고**, 국내주식 리서치 플랫폼의 가장 중요한 기준 데이터인 **종목 유니버스(dim_symbol)** 와 **거래일 캘린더(dim_trading_calendar)** 를 실제로 채우는 작업이다.

즉, 이번 티켓의 핵심은 아래 두 가지다.

1. 더 이상 “빈 뼈대”가 아니라, **공식 데이터 소스와 연결되는 기초 수집기**를 만든다.
2. 이후 TICKET-002부터 일봉/재무/뉴스 적재 파이프라인을 대량 구현할 수 있도록, **기준 마스터 데이터와 수집 계약(contract)** 을 확정한다.

---

## 2. 이번 티켓에서 반드시 끝내야 하는 것

### 2.1 종목마스터(dim_symbol) 실채움
다음이 가능한 상태를 만든다.

- 국내주식 기준 종목 유니버스를 수집한다.
- 종목별 시장구분, 보통주 여부, ETF/ETN/스팩/우선주/리츠 등 분류 플래그를 정규화한다.
- DART corp_code를 가능한 범위에서 매핑한다.
- DuckDB의 `dim_symbol` 을 upsert 방식으로 갱신한다.
- 기본 연구용 필터 뷰 `vw_universe_active_common_stock` 를 만든다.

### 2.2 거래일 캘린더(dim_trading_calendar) 실채움
다음이 가능한 상태를 만든다.

- 지정한 날짜 범위에 대해 거래일 캘린더를 생성/갱신한다.
- `prev_trading_date`, `next_trading_date` 를 채운다.
- source, confidence, override 여부를 남긴다.
- 미래 휴장일은 override 파일로 보정 가능하게 만든다.

### 2.3 KIS provider 실가동
다음이 가능한 상태를 만든다.

- App Key / App Secret 기반 인증과 토큰 재사용이 동작한다.
- health check가 된다.
- 샘플 종목 1개에 대해 일봉 또는 현재가의 “읽기 전용” 조회가 된다.
- raw payload를 짧게 저장할 수 있다.
- 실패 시 run manifest와 로그에 오류가 남는다.

### 2.4 DART provider 실가동
다음이 가능한 상태를 만든다.

- API key 기반 호출이 된다.
- corpCode 전체 파일을 내려받아 cache/parsing 할 수 있다.
- 종목코드/회사명 기준으로 corp_code 매핑이 가능하다.
- 샘플 종목 1개에 대해 기업개황 또는 기초 재무조회 probe가 된다.

### 2.5 Ops/UI 가시성 강화
다음이 가능해야 한다.

- Ops 페이지에서 최근 universe sync run 결과가 보인다.
- 종목 수, 시장별 종목 수, DART 매핑 성공 수, 마지막 캘린더 업데이트 범위가 보인다.
- provider health 상태가 최소한 pass/fail로 보인다.

---

## 3. 이번 티켓의 범위와 비범위

### 3.1 이번 티켓의 범위
- KIS provider 인증/기초 조회
- DART provider 인증/corpCode/기초 조회
- KRX 또는 공식 참조소스 기반 종목마스터 정규화 구조
- dim_symbol, dim_trading_calendar 실적재
- universe sync script
- trading calendar sync script
- provider smoke check script
- raw + curated 저장 규칙 확정
- Ops 화면 기본 확장
- 관련 테스트 작성

### 3.2 이번 티켓의 비범위
이번 티켓에서는 아래를 완성하지 않는다.

- 전 종목 일봉 대량 적재
- 전 종목 재무 스냅샷 대량 적재
- 뉴스 적재 파이프라인
- 피처 엔지니어링
- 랭킹 엔진
- Discord 전송 로직 완성
- 장중 체결/호가 수집

즉, 이번 티켓은 **“기준 데이터 + provider activation”** 까지다.

---

## 4. Codex가 작업 시작 전에 반드시 확인할 것

Codex는 작업 시작 전에 아래 순서를 따른다.

1. 루트 경로 `D:\MyApps\StockMaster` 를 기준으로 현재 저장소 상태를 확인한다.
2. 아래 문서를 먼저 읽는다.
   - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
   - `TICKET_000_Foundation_and_First_Work_Package.md`
   - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
3. TICKET-000이 아직 불완전하다면, **이번 티켓 수행에 직접 필요한 blocking issue만 보완**한다.
4. 기존 기획 문서와 foundation 파일 구조를 불필요하게 뒤집지 않는다.
5. 새로운 파일은 기존 `app/`, `config/`, `scripts/`, `tests/`, `docs/` 구조 안에 추가한다.

---

## 5. 이번 티켓의 설계 원칙

### 5.1 공식 소스 우선
- KIS, OpenDART, KRX 계열 공식 소스를 우선 사용한다.
- 웹 페이지 HTML 스크래핑을 기본축으로 삼지 않는다.
- 다만 KRX 참조 데이터의 자동화 경로가 당장 완전히 안정적이지 않으면, **공식 소스 adapter + 수동 seed fallback** 구조를 허용한다.

### 5.2 읽기 전용만 구현
- 주문/정정/취소/실주문 관련 기능은 금지한다.
- provider는 읽기 전용 기능만 활성화한다.

### 5.3 idempotent 실행
- 동일 날짜/동일 범위 sync를 반복 실행해도 데이터가 깨지지 않아야 한다.
- upsert key를 명시한다.

### 5.4 나중 확장을 염두에 둔 정규화
- 당장 전 종목 적재를 다 구현하지 않아도 된다.
- 대신 symbol master와 calendar schema는 이후 feature/prediction/evaluation이 의존할 수 있게 안정적으로 잡는다.

---

## 6. 종목마스터(dim_symbol) 요구사항

## 6.1 수집 목표
최종적으로 `dim_symbol` 에 아래 성격의 정보가 들어가야 한다.

- 6자리 종목코드 기준의 국내 상장 종목 식별자
- 회사명
- 시장 구분 (KOSPI / KOSDAQ 등)
- 섹터/업종
- 상장일
- 증권종류 분류 플래그
- 활성/상장폐지/거래정지/관리 플래그 가능 영역
- DART corp_code 매핑
- KIS 조회용 심볼값/코드값
- 최신 갱신 시각

## 6.2 최소 컬럼 계약
최소 컬럼은 아래를 기준으로 한다.

- `symbol`
- `company_name`
- `market`
- `market_segment`
- `sector`
- `industry`
- `listing_date`
- `security_type`
- `is_common_stock`
- `is_preferred_stock`
- `is_etf`
- `is_etn`
- `is_spac`
- `is_reit`
- `is_delisted`
- `is_trading_halt`
- `is_management_issue`
- `status_flags`
- `dart_corp_code`
- `dart_corp_name`
- `source`
- `as_of_date`
- `updated_at`

## 6.3 기본 연구용 유니버스 뷰
Codex는 아래 view를 생성한다.

### `vw_universe_active_common_stock`
기본 필터 의도:
- `market in ('KOSPI', 'KOSDAQ')`
- `is_common_stock = true`
- `is_etf = false`
- `is_etn = false`
- `is_spac = false`
- `is_reit = false`
- `is_delisted = false`

주의:
- `is_trading_halt`, `is_management_issue` 는 source가 안정적으로 제공되지 않으면 컬럼은 두되 null 허용 가능하다.
- “필터 가능한 구조”를 만드는 것이 목적이며, 완벽한 정책 플래그 확보를 이번 티켓의 완료 조건으로 두지는 않는다.

## 6.4 DART corp_code 매핑 정책
- DART corpCode 전체 파일을 내려받아 cache한다.
- stock_code가 존재하는 경우 이를 최우선 매핑 키로 사용한다.
- 종목코드 미일치 시 회사명 기반 보조 매핑을 허용하되, fuzzy match는 보수적으로 처리한다.
- 불확실한 매핑은 강제로 붙이지 말고 null로 남긴다.
- 매핑 결과에는 `match_method` 또는 동등한 메타데이터를 남겨도 된다.

---

## 7. 거래일 캘린더(dim_trading_calendar) 요구사항

## 7.1 목적
장후 리포트, D+1/D+5 평가, prev/next trading date 계산의 기준 테이블을 만든다.

## 7.2 최소 컬럼 계약
- `trading_date`
- `is_trading_day`
- `market_session_type`
- `weekday`
- `is_weekend`
- `is_public_holiday`
- `holiday_name`
- `source`
- `source_confidence`
- `is_override`
- `prev_trading_date`
- `next_trading_date`
- `updated_at`

## 7.3 구현 원칙
- 최초 구현은 **주말 + 한국 공휴일 + override 파일** 기반으로 생성해도 된다.
- 가능하다면 KRX/공식 시장 캘린더 소스를 adapter로 붙인다.
- 단, 확인되지 않은 웹 페이지 하드스크래핑을 강하게 고정하지 않는다.
- override 파일을 통해 거래소 특수휴장일을 수동 보정할 수 있게 한다.

## 7.4 설정 파일
다음 파일 중 최소 1개를 둔다.

- `config/trading_calendar_overrides.csv`
- 또는 `config/trading_calendar_overrides.example.csv`

필드 예시:
- `date`
- `is_trading_day`
- `holiday_name`
- `note`

## 7.5 캘린더 생성 스크립트
Codex는 아래와 유사한 CLI를 제공한다.

```bash
python scripts/sync_trading_calendar.py --start 2025-01-01 --end 2026-12-31
```

동작 요구:
- 범위 생성/업데이트
- prev/next trading date 계산
- run manifest 기록
- 성공/실패 로그 기록

---

## 8. KIS provider 실구현 요구사항

## 8.1 최소 구현 목표
이번 티켓에서 KIS는 “실주문”이 아니라 **인증 + 읽기 전용 시세 probe** 까지만 구현한다.

## 8.2 필수 구현 요소
- 토큰 발급/갱신 로직
- expiry-aware token cache
- 공통 header builder
- 공통 request wrapper (retry, timeout, error mapping)
- `health_check()`
- 샘플 종목에 대한 읽기 전용 조회 1~2개

## 8.3 최소 public interface 예시
- `get_access_token()`
- `health_check()`
- `fetch_daily_ohlcv(symbol, start_date, end_date)` 또는 동등 기능
- `fetch_current_quote(symbol)` 또는 동등 기능

## 8.4 운영 요구사항
- secrets는 `.env` 에서만 읽는다.
- 토큰을 git에 저장하지 않는다.
- 토큰 cache가 필요하면 `data/cache/` 아래에 두고 gitignore 대상이어야 한다.
- 실패 시 provider 이름, endpoint, status code, error body 요약을 로그/manifest notes에 남긴다.

## 8.5 샘플 검증 종목
기본 smoke test 종목은 아래를 기본값으로 둔다.
- `005930` (예: 삼성전자)

단, 심볼 하드코딩은 설정으로 override 가능하게 만든다.

---

## 9. DART provider 실구현 요구사항

## 9.1 최소 구현 목표
이번 티켓에서 DART는 **corpCode 매핑 + 기업개황/기초 재무 probe** 까지만 구현한다.

## 9.2 필수 구현 요소
- API key 로딩
- corpCode zip 다운로드
- xml 파싱 및 정규화
- stock_code → corp_code 매핑 저장
- `health_check()`
- 샘플 종목 기초 조회

## 9.3 최소 public interface 예시
- `download_corp_codes(force=False)`
- `load_corp_code_map()`
- `health_check()`
- `fetch_company_overview(corp_code)` 또는 동등 기능
- `fetch_latest_financial_probe(corp_code, bsns_year=None, reprt_code=None)` 또는 동등 기능

## 9.4 raw 저장
- corpCode raw zip/xml은 `data/raw/dart/corp_codes/` 또는 동등 위치에 저장한다.
- 반복 다운로드를 피하기 위해 cache-aware 하게 구현한다.

---

## 10. KRX / 참조데이터 adapter 요구사항

## 10.1 역할
이번 티켓에서 KRX 계층의 역할은 “종목마스터 참조정보를 얻는 공식 adapter” 다.

## 10.2 구현 원칙
- 우선 공식 경로를 시도한다.
- 만약 인증/접근조건 때문에 자동화 구현이 당장 완전하지 않다면, **adapter interface + seed fallback** 구조를 만든다.
- 이 경우 seed는 버전 관리 가능한 예시 파일 형태로 둔다.

## 10.3 seed fallback 허용 형태
예시 파일:
- `config/seeds/symbol_master_seed.example.csv`

실제 로컬 사용자는 필요 시 다음처럼 복사하여 넣을 수 있다.
- `config/seeds/symbol_master_seed.csv`

주의:
- example 파일은 git에 포함 가능
- 실제 seed는 선택적으로 사용
- seed는 provider 부재 시 개발용 fallback일 뿐, 운영 기준 source of truth라는 의미는 아니다.

---

## 11. 저장 규칙

## 11.1 raw 저장
이번 티켓에서 생성되는 raw 저장 예시:

```text
data/raw/kis/health_check/date=YYYY-MM-DD/*.json
data/raw/kis/daily_ohlcv_probe/date=YYYY-MM-DD/*.parquet
data/raw/dart/corp_codes/date=YYYY-MM-DD/*.zip
data/raw/dart/company_overview/date=YYYY-MM-DD/*.json
data/raw/reference/symbol_master/date=YYYY-MM-DD/*.parquet
```

## 11.2 curated / semantic 저장
- `dim_symbol` 은 DuckDB dimension table로 유지
- `dim_trading_calendar` 는 DuckDB dimension table로 유지
- 필요 시 정규화 결과 parquet snapshot도 함께 저장 가능

## 11.3 upsert key
- `dim_symbol`: `symbol`
- `dim_trading_calendar`: `trading_date`

---

## 12. 이번 티켓에서 생성하거나 보강해야 하는 파일

아래는 권장 최소 파일 목록이다. Codex는 저장소 상황에 맞게 세부 이름을 조정할 수 있지만, 역할은 유지해야 한다.

### 12.1 provider 구현
- `app/providers/kis/auth.py`
- `app/providers/kis/market_data.py`
- `app/providers/dart/corp_codes.py`
- `app/providers/dart/company.py`
- `app/providers/krx/reference.py`

### 12.2 ingestion / normalization
- `app/ingestion/universe_sync.py`
- `app/ingestion/calendar_sync.py`
- `app/reference/symbol_normalizer.py`
- `app/reference/dart_mapper.py`

### 12.3 scripts
- `scripts/sync_universe.py`
- `scripts/sync_trading_calendar.py`
- `scripts/provider_smoke_check.py`

### 12.4 config
- `config/universe_filters.yaml` (선택이지만 권장)
- `config/trading_calendar_overrides.example.csv`
- `config/seeds/symbol_master_seed.example.csv` (fallback 필요 시)

### 12.5 tests
- `tests/unit/test_symbol_normalizer.py`
- `tests/unit/test_trading_calendar.py`
- `tests/unit/test_dart_corp_codes.py`
- `tests/integration/test_sync_universe.py`
- `tests/integration/test_provider_smoke.py`

### 12.6 docs
- `docs/tickets/TICKET-001.md`
- `docs/architecture/data_contracts.md` 또는 동등 문서

---

## 13. UI / Ops 보강 요구사항

기존 Home / Ops 화면을 크게 뒤엎을 필요는 없지만, 최소 아래 정보가 보여야 한다.

### 13.1 Home 또는 Ops에 표시할 것
- `dim_symbol` 총 종목 수
- 시장별 종목 수(KOSPI/KOSDAQ)
- `dart_corp_code` 매핑 성공 수
- 마지막 universe sync 시각
- trading calendar 범위 (min/max date)
- provider health 상태

### 13.2 있으면 좋은 것
- symbol filter summary
- active common stock count
- 최근 실패한 provider call summary

---

## 14. README 보강 요구사항

README 또는 동등 문서에 아래를 추가한다.

- KIS / DART 키를 어디에 넣는지
- `.env.example` 의 신규 필드 설명
- universe sync 실행 방법
- trading calendar sync 실행 방법
- provider smoke check 실행 방법
- seed fallback 사용 방법 (있는 경우)

---

## 15. 실행 예시

아래 예시는 Codex가 실제로 맞춰줘야 할 실행 UX다.

```bash
cp .env.example .env
python scripts/bootstrap.py
python scripts/sync_trading_calendar.py --start 2025-01-01 --end 2026-12-31
python scripts/sync_universe.py
python scripts/provider_smoke_check.py --symbol 005930
streamlit run app/ui/Home.py
```

Windows 환경을 고려한다면 README에 PowerShell 예시도 함께 적는다.

---

## 16. 완료 기준 (Definition of Done)

이번 티켓은 아래를 모두 만족해야 완료다.

1. TICKET-000 foundation이 깨지지 않는다.
2. `.env` 기반으로 KIS/DART 설정을 읽을 수 있다.
3. `sync_trading_calendar.py` 로 지정 범위 캘린더가 채워진다.
4. `sync_universe.py` 로 `dim_symbol` 이 채워진다.
5. `vw_universe_active_common_stock` 가 생성된다.
6. DART corp code cache/download/parsing 이 동작한다.
7. KIS provider가 토큰 발급 후 읽기 전용 probe 1개 이상 수행한다.
8. DART provider가 기업개황 또는 기초 재무 probe 1개 이상 수행한다.
9. 각 작업이 run manifest를 남긴다.
10. raw payload 또는 raw snapshot이 지정 경로에 저장된다.
11. Ops/UI 에 유니버스/캘린더/health 요약이 보인다.
12. 최소 단위/통합 테스트가 통과한다.
13. README가 신규 실행 흐름을 설명한다.

---

## 17. 절대 하지 말아야 할 것

1. 주문/자동매매 기능을 추가하지 말 것
2. 전 종목 일봉 대량 backfill까지 범위를 키우지 말 것
3. 전 종목 뉴스 적재를 이번 티켓에 끼워 넣지 말 것
4. KRX/공식 소스 접근이 애매하다고 해서 바로 비공식 스크래핑을 기본축으로 삼지 말 것
5. corp_code를 불확실한 fuzzy match로 무리하게 붙이지 말 것
6. 종목마스터를 단순 CSV 한 장으로 끝내고 메타데이터/플래그를 버리지 말 것
7. run manifest 없이 sync script를 만들지 말 것
8. provider 오류를 삼키고 지나가지 말 것

---

## 18. 다음 티켓 예고

이번 티켓 다음은 아래 흐름이 자연스럽다.

- TICKET-002: 일봉 / 재무 / 뉴스 메타데이터 적재 파이프라인
- TICKET-003: 피처 스토어 v1
- TICKET-004: 랭킹 엔진 v1

즉, 이번 티켓의 성공 조건은 **“기준 마스터와 provider 기반이 생겨서 TICKET-002로 자연스럽게 넘어갈 수 있는 상태”** 다.

---

## 19. Codex에게 바로 붙여넣을 실행 지시문

```text
당신은 StockMaster 저장소의 TICKET-001을 구현하는 엔지니어입니다.
루트 경로는 D:\MyApps\StockMaster 이고, 먼저 아래 문서를 읽으세요.
- KR_Stock_Research_Platform_v1_Implementation_Spec.md
- TICKET_000_Foundation_and_First_Work_Package.md
- CODEX_FIRST_INSTRUCTION_StockMaster.md

이번 작업 목표는 provider stub을 실제 공식 provider로 올리고, dim_symbol / dim_trading_calendar 를 실채우는 것입니다.

반드시 구현할 것:
- KIS provider: 인증, 토큰 캐시, health check, 읽기 전용 시세 probe
- DART provider: corpCode 다운로드/파싱, corp_code 매핑, company overview 또는 기초 재무 probe
- 종목마스터 정규화 및 dim_symbol upsert
- vw_universe_active_common_stock 생성
- 거래일 캘린더 생성 및 dim_trading_calendar upsert
- scripts/sync_universe.py
- scripts/sync_trading_calendar.py
- scripts/provider_smoke_check.py
- raw 저장 + run manifest 기록
- Ops/UI에 유니버스/캘린더/provider health 요약 표시
- 관련 테스트 및 README 보강

중요 제약:
- 자동매매/주문 금지
- 전 종목 대량 일봉/재무/뉴스 적재까지 범위를 확장하지 말 것
- idempotent upsert 구조 유지
- 공식 소스 우선, 필요 시 seed fallback은 허용하되 운영 기준 source of truth로 혼동하지 말 것
- 불확실한 DART 매핑은 null로 남길 것

완료 후 실행 예시가 README에 있어야 하고,
bootstrap → trading calendar sync → universe sync → provider smoke check → streamlit 확인 흐름이 재현 가능해야 합니다.
```
