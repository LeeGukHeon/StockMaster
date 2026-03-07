# TICKET-002 — 일봉 OHLCV + 재무 스냅샷 + 뉴스 메타데이터 적재 파이프라인

- 문서 목적: TICKET-001 이후, Codex가 바로 이어서 구현할 **첫 연구용 핵심 데이터 적재 파이프라인**의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
  - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
  - `TICKET_000_Foundation_and_First_Work_Package.md`
  - `TICKET_001_Universe_Calendar_Provider_Activation.md`
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation이 실행 가능해야 함
  - TICKET-001의 `dim_symbol`, `dim_trading_calendar`, KIS/DART provider activation이 완료되어 있어야 함
- 우선순위: 최상
- 기대 결과: “실제 연구/리포트에 쓸 수 있는 핵심 데이터 3축(시세, 재무, 뉴스)이 매일/백필 방식으로 적재되고, 이후 피처/랭킹/평가 티켓으로 바로 넘어갈 수 있는 상태”

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **국내주식 리서치 플랫폼의 첫 실전 데이터 레이어를 완성하는 작업**이다.

즉, 이번 티켓의 목표는 아래 세 가지를 안정적으로 만드는 것이다.

1. **전 종목 장후 일봉 데이터**가 적재된다.
2. **누수 없는 재무 스냅샷**이 적재된다.
3. **뉴스 메타데이터와 심볼 후보 연결 정보**가 적재된다.

이 세 가지가 채워져야 다음 티켓에서 피처 생성, 설명용 점수, 랭킹 엔진, 평가 리포트로 넘어갈 수 있다.

---

## 2. 이번 티켓에서 반드시 끝내야 하는 것

### 2.1 일봉 OHLCV 적재 파이프라인
다음이 가능한 상태를 만든다.

- `vw_universe_active_common_stock` 기준 전 종목 또는 지정 종목군에 대해 일봉 시세를 적재한다.
- 지정한 거래일 1일 또는 날짜 범위 백필이 가능하다.
- 결과를 DuckDB `fact_daily_ohlcv`와 Parquet 파티션에 반영한다.
- 동일 날짜/동일 종목 재실행 시 중복 없이 idempotent 하게 갱신된다.
- raw payload를 보존하고, curated 결과와 run manifest를 남긴다.

### 2.2 재무 스냅샷 적재 파이프라인
다음이 가능한 상태를 만든다.

- DART 매핑이 완료된 종목에 대해 공식 재무 데이터를 수집한다.
- 보고서 코드(분기/반기/3분기/사업보고서)와 공시 식별자를 보존한다.
- **누수 방지 원칙**에 따라 `as_of_date` 기준 최신 사용 가능 재무 스냅샷을 만들 수 있어야 한다.
- `fact_fundamentals_snapshot` 에 정규화된 결과가 들어가야 한다.
- 보고서 중복/정정/연결/별도 기준 선택 규칙이 명시되어야 한다.

### 2.3 뉴스 메타데이터 적재 파이프라인
다음이 가능한 상태를 만든다.

- 뉴스 본문 전문 저장 없이, 제목/언론사/링크/발행시각/요약 snippet/태그/심볼후보 중심으로 적재한다.
- 시장 공통 쿼리와 종목 중심 쿼리를 함께 지원한다.
- 심볼 매칭은 보수적으로 처리하고, 오탐 방지 규칙을 둔다.
- `fact_news_item` 에 적재되며, 중복 기사 판정 규칙이 있어야 한다.
- 매일 장후 보고용 “최근 뉴스 묶음”으로 활용할 수 있는 수준의 freshness/tagging 이 가능해야 한다.

### 2.4 Ops/UI 가시성 강화
다음이 가능해야 한다.

- Ops 화면에서 최근 OHLCV 적재 상태, 재무 적재 상태, 뉴스 적재 상태를 확인할 수 있다.
- 최근 적재 거래일, 커버리지, 에러 건수, 최근 뉴스 개수, DART 매핑 누락 종목 수 등을 볼 수 있다.
- Placeholder/Research 페이지에서 최소한 최신 데이터가 들어왔는지 사람이 확인 가능한 수준으로 보여준다.

### 2.5 배치/스크립트 엔트리포인트
다음 스크립트가 동작해야 한다.

- `scripts/sync_daily_ohlcv.py`
- `scripts/sync_fundamentals_snapshot.py`
- `scripts/sync_news_metadata.py`
- `scripts/backfill_core_research_data.py`

---

## 3. 이번 티켓의 범위와 비범위

### 3.1 이번 티켓의 범위
- 전 종목 장후 일봉 적재
- 재무 데이터 수집 및 스냅샷화
- 뉴스 메타데이터 수집 및 심볼 후보 연결
- raw 저장 규칙 강화
- DuckDB / Parquet curated 적재
- run manifest 기록
- 데이터 품질 검증
- Ops/UI 상태 표시
- README 및 실행 가이드 갱신
- 관련 테스트 작성

### 3.2 이번 티켓의 비범위
이번 티켓에서는 아래를 완성하지 않는다.

- 피처 엔지니어링 전체 구현
- 예측 모델 학습
- 실제 종목 랭킹/등급 산출
- 뉴스 요약 LLM 파이프라인
- 뉴스 클러스터링 고도화
- 장중 1분봉/체결/호가 수집
- Discord 전송 최종 포맷 완성
- D+1/D+5 평가 엔진 완성

즉, 이번 티켓은 **“핵심 연구 데이터 레이어 구축”** 까지다.

---

## 4. Codex가 작업 시작 전에 반드시 확인할 것

Codex는 작업 시작 전에 아래 순서를 따른다.

1. 루트 경로 `D:\MyApps\StockMaster` 를 기준으로 현재 저장소 상태를 확인한다.
2. 아래 문서를 먼저 읽는다.
   - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
   - `TICKET_000_Foundation_and_First_Work_Package.md`
   - `TICKET_001_Universe_Calendar_Provider_Activation.md`
   - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
   - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
3. TICKET-001이 불완전하다면, **이번 티켓 수행에 직접 필요한 blocking issue만 보완**한다.
4. 기존 파일 구조와 foundation 설계를 불필요하게 뒤집지 않는다.
5. 새로운 파일은 기존 `app/`, `config/`, `scripts/`, `tests/`, `docs/` 구조 안에 추가한다.

---

## 5. 이번 티켓의 설계 원칙

### 5.1 공식 소스 우선
- 시세는 KIS 공식 provider를 기본으로 한다.
- 재무는 OpenDART 공식 provider를 기본으로 한다.
- 뉴스는 Naver Search API 기반 메타데이터 적재를 기본으로 한다.
- pykrx, HTML 스크래핑, 비공식 크롤링은 기본 경로로 삼지 않는다.
- 다만 일부 필드가 공식 소스로 즉시 확보되지 않으면, **필수 컬럼은 유지하되 null 허용 또는 명시적 proxy 계산**을 허용한다.

### 5.2 장후 연구 시스템 전제
이 플랫폼은 장후 리포트가 핵심이다.

- `as_of_date` 는 **장 마감 후 연구 시점**을 의미한다.
- 재무/뉴스 스냅샷은 “그날 장 마감 후 리포트 생성 시점까지 이용 가능한 정보”를 기준으로 해야 한다.
- intraday 초저지연 체결 판단 로직은 이번 티켓의 범위가 아니다.

### 5.3 idempotent + backfill friendly
- 날짜 범위를 주어 과거 데이터를 백필할 수 있어야 한다.
- 동일 파라미터 재실행 시 중복 적재가 되지 않아야 한다.
- 실행 범위, 종목 수, 적재 row 수, 실패 건수는 run manifest 에 남긴다.

### 5.4 누수 방지
- 재무는 **공시 공개 시각/공개일자 기준**으로 availability 를 판정해야 한다.
- 뉴스는 **발행시각 기준**으로 signal date 를 판정해야 한다.
- 미래 데이터를 과거 `as_of_date` 에 붙여 넣는 구조를 만들면 안 된다.

### 5.5 설명 가능성 확보
- 이후 피처/랭킹 티켓에서 “왜 그 종목이 올라왔는지” 설명하려면, 이번 티켓에서 source 문맥을 잃어버리면 안 된다.
- 따라서 raw 경로, source document id, query keyword, match method, notes 는 가능한 범위에서 보존한다.

---

## 6. 일봉 OHLCV 적재 요구사항

## 6.1 목적
`fact_daily_ohlcv` 는 향후 아래 모든 티켓의 기초가 된다.

- 추세/모멘텀 피처
- 거래대금/회전율 피처
- D+1/D+5 수익률 라벨
- 종목 카드 차트
- 시장 breadth / 급등락 탐지

즉, 이번 티켓의 OHLCV 적재는 **단순 보관용이 아니라, 이후 모델과 리포트의 기준 데이터**다.

## 6.2 수집 범위
기본 수집 범위는 아래와 같다.

- 대상: `vw_universe_active_common_stock`
- 시장: KOSPI, KOSDAQ
- 기본 빈도: 일봉
- 실행 모드:
  - 단일 거래일 적재
  - 날짜 범위 백필
  - 지정 종목 subset 적재
  - 개발용 limit-symbols 적재

## 6.3 최소 컬럼 계약
DuckDB `fact_daily_ohlcv` 최소 컬럼은 아래를 유지한다.

- `trading_date`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `turnover_value`
- `market_cap`
- `source`
- `ingested_at`

필수 원칙:
- 최소 컬럼 계약은 깨지지 않아야 한다.
- 필요한 경우 비파괴적으로 보조 컬럼을 추가해도 된다.
  - 예: `value_proxy_flag`, `price_adjustment_flag`, `currency`, `source_notes_json`

## 6.4 소스/필드 처리 원칙
- 종가, 시가, 고가, 저가, 거래량은 공식 소스 값을 우선 사용한다.
- `turnover_value` 가 소스에 없으면 null 허용 또는 명시적 proxy 계산을 허용한다.
- `market_cap` 이 안정적으로 확보되지 않으면 우선 null 허용 가능하다. 단, README와 manifest notes에 이유를 남긴다.
- 숫자 파싱 실패 시 문자열로 우겨 넣지 말고 실패 처리한다.

## 6.5 파티셔닝/적재 경로 권장안
예시:

```text
data/raw/kis/daily_ohlcv/trading_date=2026-03-06/symbol=005930/*.json
data/curated/market/daily_ohlcv/trading_date=2026-03-06/*.parquet
```

원칙:
- raw 는 provider 원문 payload 중심
- curated 는 분석 가능한 정규화 row 중심
- DuckDB 는 curated parquet 를 기준으로 merge/upsert 하거나 외부 테이블로 읽을 수 있어야 한다

## 6.6 upsert key / 중복 규칙
- 논리 primary key: `(trading_date, symbol)`
- 동일 키가 다시 들어오면 최신 적재분으로 대체하거나 deterministic merge 한다.
- 동일 거래일 데이터가 여러 source note 를 가질 수 있어도 최종 row 는 1건이어야 한다.

## 6.7 품질 검증 규칙
최소한 아래 검증은 구현한다.

- `high >= max(open, close)`
- `low <= min(open, close)`
- `volume >= 0`
- `open/high/low/close > 0` (거래정지/비정상 데이터 예외는 notes 처리 가능)
- 중복 키 없음
- universe 대상 대비 커버리지 산출

## 6.8 스크립트 요구사항
`scripts/sync_daily_ohlcv.py` 는 최소 아래 옵션을 지원한다.

- `--date YYYY-MM-DD`
- `--start YYYY-MM-DD --end YYYY-MM-DD`
- `--symbols 005930,000660`
- `--limit-symbols 100`
- `--market KOSPI|KOSDAQ|ALL`
- `--force`
- `--dry-run`

행동 원칙:
- `--date` 또는 `--start/--end` 중 하나는 필수
- 거래일 캘린더를 참고해 비거래일은 skip 또는 명확한 메시지 처리
- dry-run 시 row 수/대상 종목 수/예상 raw 파일 경로만 보여줘도 된다

---

## 7. 재무 스냅샷 적재 요구사항

## 7.1 목적
`fact_fundamentals_snapshot` 은 향후 아래 피처의 기초다.

- 재무 퀄리티
- 밸류/안전성
- 수익성/부채/마진
- 설명용 종목 카드 재무 요약

이 데이터는 **정확성보다도 누수 방지와 기준 통일이 더 중요**하다.

## 7.2 기본 입력 전제
- `dim_symbol.dart_corp_code` 가 채워져 있어야 한다.
- DART provider 는 TICKET-001에서 활성화되어 있어야 한다.
- corp_code 미매핑 종목은 이번 티켓에서 무리하게 추정하지 말고 skip + 로그 기록한다.

## 7.3 최소 컬럼 계약
DuckDB `fact_fundamentals_snapshot` 최소 컬럼은 아래를 유지한다.

- `as_of_date`
- `symbol`
- `fiscal_year`
- `report_code`
- `revenue`
- `operating_income`
- `net_income`
- `roe`
- `debt_ratio`
- `operating_margin`
- `source_doc_id`
- `source`
- `ingested_at`

권장 추가 컬럼(비파괴적 확장 허용):
- `disclosed_at`
- `fs_div` 또는 `statement_basis`
- `report_name`
- `currency`
- `accounting_standard`
- `source_notes_json`

## 7.4 연결/별도 선택 규칙
- 연결재무(`CFS`)를 우선한다.
- 연결이 없을 때만 별도재무(`OFS`)를 fallback 한다.
- 어떤 기준을 사용했는지 컬럼 또는 notes 로 남긴다.

## 7.5 보고서 코드 처리 원칙
- 사업보고서/분기보고서/반기보고서/3분기보고서를 모두 고려한다.
- raw `report_code` 는 보존한다.
- 사람 읽기 쉬운 `report_name` 을 추가해도 된다.
- 정정 공시가 존재하면 **최신 유효 공시를 우선**하되, 원본 식별자(`source_doc_id`)는 남긴다.

## 7.6 누수 방지의 핵심 정의
이번 프로젝트에서 `as_of_date` 는 **장 마감 후 연구 시점 기준 스냅샷 날짜**다.

따라서 다음을 따른다.

- 공시가 `as_of_date` 의 장 마감 후 리포트 생성 cutoff 이전에 공개되었다면, 해당 `as_of_date` 스냅샷에 포함할 수 있다.
- cutoff 이후에 공개된 공시는 다음 signal date 로 넘긴다.
- 과거 백테스트에서도 동일 원칙을 유지한다.

Codex는 최소한 아래 중 하나를 명시적으로 구현해야 한다.

1. **날짜 단위 conservative rule**
   - 공개일(`disclosed_date`) 기준으로, 같은 날짜는 포함 가능
   - 시각 단위는 우선 단순화
2. **시각 단위 rule**
   - `signal_cutoff_time_local` 설정을 두고 공개시각까지 반영

이번 티켓에서는 1번을 먼저 구현해도 되지만, 코드 구조는 2번으로 확장 가능해야 한다.

## 7.7 스냅샷화 전략
중요: DART 원천 데이터와 `fact_fundamentals_snapshot` 을 같은 개념으로 취급하면 안 된다.

권장 구조:

1. raw DART payload 저장
2. 필요하면 intermediate/staging 정규화 테이블 생성
3. 특정 `as_of_date` 에 대해 **그 시점까지 공개된 최신 유효 재무 row** 를 골라 snapshot materialization

즉, Codex는 아래 둘 중 하나를 구현할 수 있다.

- 직접 `fact_fundamentals_snapshot` 을 materialize 하는 파이프라인
- 혹은 `stg_dart_financials` + materializer 조합

단, 최종적으로는 `fact_fundamentals_snapshot` 이 채워져야 한다.

## 7.8 정규화 계산 원칙
최소 계산 항목:
- `revenue`
- `operating_income`
- `net_income`
- `roe`
- `debt_ratio`
- `operating_margin`

원칙:
- 소스가 직접 제공하면 그대로 사용 가능
- 직접 제공하지 않으면 정규화된 계정 매핑을 통해 계산할 수 있다
- 계산 근거가 불명확하면 억지 계산을 하지 말고 null 처리

권장: 계정명/표준계정코드 매핑은 별도 config 또는 정규화 모듈로 분리한다.

예시 파일:
- `config/fundamentals_account_map.yaml`
- `app/domain/fundamentals/account_normalizer.py`

## 7.9 upsert key / 중복 규칙
핵심적으로는 아래 개념을 지켜야 한다.

- 하나의 `(as_of_date, symbol)` 에 대해 최종 스냅샷 row 는 1건이어야 한다.
- 같은 종목에 대해 동일 `as_of_date` 재실행 시 deterministic 해야 한다.
- source document 교체나 정정공시 반영이 있으면 더 최신 유효 row 로 재계산 가능해야 한다.

## 7.10 스크립트 요구사항
`scripts/sync_fundamentals_snapshot.py` 는 최소 아래 옵션을 지원한다.

- `--as-of-date YYYY-MM-DD`
- `--start YYYY-MM-DD --end YYYY-MM-DD`
- `--symbols 005930,000660`
- `--limit-symbols 100`
- `--force`
- `--dry-run`

권장 추가 옵션:
- `--materialize-only`
- `--fetch-only`

---

## 8. 뉴스 메타데이터 적재 요구사항

## 8.1 목적
`fact_news_item` 은 향후 아래 기능의 기초가 된다.

- 장후 주요 이슈 요약
- 종목 카드 최근 뉴스
- 뉴스 촉매 피처
- 장세/테마 분위기 파악

이번 티켓에서 중요한 것은 **뉴스를 많이 모으는 것**보다 **중복 없이, 저작권 문제 없이, 심볼 연결 가능한 메타데이터를 남기는 것**이다.

## 8.2 저장 원칙
반드시 아래를 따른다.

- 기사 본문 전문 저장 금지
- 제목/언론사/원문 링크/발행시각/snippet/검색쿼리 중심 저장
- query plan 과 symbol linking 결과를 보존
- 뉴스 원문 페이지를 다시 스크래핑하는 기본 경로 금지

## 8.3 최소 컬럼 계약
DuckDB `fact_news_item` 최소 컬럼은 아래를 유지한다.

- `news_id`
- `published_at`
- `symbol_candidates`
- `query_keyword`
- `title`
- `publisher`
- `link`
- `snippet`
- `tags_json`
- `catalyst_score`
- `sentiment_score`
- `freshness_score`
- `source`
- `ingested_at`

권장 추가 컬럼:
- `canonical_link`
- `match_method_json`
- `query_bucket`
- `is_market_wide`
- `source_notes_json`

## 8.4 뉴스 쿼리 전략
이번 티켓에서는 적어도 아래 두 계층을 지원한다.

### A. 시장 공통 쿼리
시장 분위기/섹터/거시 이슈를 잡기 위한 고정 쿼리 묶음.

예시는 하드코딩하지 말고 config 로 분리한다.

예시 범주:
- 코스피 / 코스닥
- 외국인 순매수 / 기관 순매수
- 반도체 / 2차전지 / 바이오 / AI
- 금리 / 환율 / 유가 / 공매도 / 정책 / 수출

권장 파일:
- `config/news_queries.yaml`

### B. 종목 중심 쿼리
종목별 회사명 기반 쿼리.

원칙:
- 초기 기본 모드는 “전 종목 풀쿼리”가 아니라 **선택 가능한 focus universe** 로 제한해도 된다.
- focus universe 기본 추천:
  - 최근 거래대금 상위
  - 사용자 지정 watchlist
  - `--symbols` 직접 지정
- 회사명과 심볼 코드를 모두 무조건 섞지 말고, 오탐이 적은 방식으로 보수적 구성

즉, 이번 티켓의 기본 목표는 **뉴스 수집 가능 구조를 만드는 것**이지, 모든 종목 모든 기사를 완벽히 긁는 것이 아니다.

## 8.5 심볼 연결(symbol linking) 원칙
가장 중요한 주의사항이다.

- 회사명 exact/normalized match 를 우선한다.
- 지나치게 공격적인 fuzzy matching 금지
- 동음이의, 일반명사, 그룹명/브랜드명은 오탐 가능성이 높으므로 보수적으로 처리
- 확신이 없으면 `symbol_candidates = []` 를 허용한다.
- 가능한 경우 `match_method_json` 에 exact/alias/query-context 정도를 남긴다.

권장 구현:
- 법인명 정규화 함수 (`(주)`, `주식회사`, 공백/특수문자 정리)
- alias dictionary (보수적)
- query bucket 별 linking 규칙 차등 적용

## 8.6 중복 기사 판정 규칙
아래 중 하나 이상을 사용해 dedupe 한다.

- canonical link 우선
- link 정규화 후 hash
- `(title, publisher, published_at)` 해시

원칙:
- 같은 기사가 여러 쿼리에서 잡혀도 `fact_news_item` 최종 row 는 1건이어야 한다.
- 다만 어떤 쿼리로 잡혔는지는 query metadata 에 남겨도 된다.

## 8.7 간단 태깅/점수화
이번 티켓에서 고급 NLP는 하지 않는다. 대신 최소한 아래 수준은 허용한다.

- `freshness_score`: 발행 후 경과시간 기반 간단 점수
- `tags_json`: 키워드 사전 기반 태깅
- `catalyst_score`: 실적/수주/규제/합병/공급계약/리콜/소송/감자/유상증자 등 이벤트 키워드 기반 규칙 점수
- `sentiment_score`: 우선 null 허용 또는 매우 단순 규칙 기반

중요:
- 이 값들은 **최종 투자판단 점수**가 아니다.
- 나중 피처 티켓에서 재계산/보완 가능해야 한다.

## 8.8 signal date 판정 원칙
뉴스도 누수 방지를 위해 signal date 기준을 명확히 해야 한다.

권장 규칙:
- 발행시각이 `as_of_date` 장후 cutoff 이전이면 해당 `as_of_date` 묶음에 포함
- cutoff 이후면 다음 signal date 로 넘김
- 이번 티켓에서 시각 단위가 어렵다면, 날짜 단위 conservative rule 로 시작 가능

## 8.9 스크립트 요구사항
`scripts/sync_news_metadata.py` 는 최소 아래 옵션을 지원한다.

- `--date YYYY-MM-DD`
- `--start YYYY-MM-DD --end YYYY-MM-DD`
- `--symbols 005930,000660`
- `--limit-symbols 100`
- `--mode market_only|market_and_focus|symbol_list`
- `--force`
- `--dry-run`

권장 추가 옵션:
- `--query-pack default`
- `--max-items-per-query 50`

---

## 9. 저장 구조 / 파티셔닝 요구사항

Codex는 최소한 아래 경로 원칙을 만족해야 한다.

### 9.1 raw
```text
data/raw/kis/daily_ohlcv/trading_date=YYYY-MM-DD/symbol=XXXXXX/*.json
data/raw/dart/financials/disclosed_date=YYYY-MM-DD/symbol=XXXXXX/*.json
data/raw/naver_news/fetch_date=YYYY-MM-DD/query_bucket=.../*.json
```

### 9.2 curated
```text
data/curated/market/daily_ohlcv/trading_date=YYYY-MM-DD/*.parquet
data/curated/fundamentals/snapshot/as_of_date=YYYY-MM-DD/*.parquet
data/curated/news/items/signal_date=YYYY-MM-DD/*.parquet
```

### 9.3 marts / helper views
권장 뷰 또는 보조 테이블:
- `vw_latest_daily_ohlcv`
- `vw_latest_fundamentals_snapshot`
- `vw_news_recent_market`
- `vw_news_recent_by_symbol`

이번 티켓에서 모든 뷰가 필수는 아니지만, 최소한 사람이 검증하기 쉬운 헬퍼 뷰는 제공하는 것이 좋다.

---

## 10. run manifest / 품질 로그 요구사항

각 파이프라인 실행은 `ops_run_manifest` 에 남아야 한다.

최소한 아래가 기록되어야 한다.

- `run_type`
  - `sync_daily_ohlcv`
  - `sync_fundamentals_snapshot`
  - `sync_news_metadata`
  - `backfill_core_research_data`
- `as_of_date` 또는 range 정보
- provider/source 정보
- 대상 종목 수
- 적재 row 수
- skip 수
- 실패 수
- raw artifact 경로
- curated artifact 경로
- notes / warnings

권장 추가 항목:
- coverage ratio
- unmatched dart corp_code count
- unmatched news symbol count
- deduped news count

---

## 11. UI / Ops 요구사항

## 11.1 Ops 페이지
최소한 아래 요약 카드/표를 추가한다.

- 최근 OHLCV 적재 거래일 / row 수 / universe 커버리지
- 최근 재무 스냅샷 `as_of_date` / 적재 종목 수 / corp_code 누락 수
- 최근 뉴스 적재 signal date / 기사 수 / dedupe 후 row 수 / symbol match 비율
- 최근 실패 run 5건

## 11.2 Placeholder/Research 페이지
최소한 사람이 아래를 눈으로 확인할 수 있어야 한다.

- 최신 거래일 상위 일부 종목의 OHLCV 샘플
- 최신 재무 스냅샷 샘플
- 최신 뉴스 메타데이터 샘플

아직 예쁜 최종 리포트가 아니어도 된다.
핵심은 **“실제 데이터가 들어왔는지 확인 가능한 개발자용 연구 화면”** 이다.

---

## 12. 테스트 요구사항

최소한 아래 테스트를 추가한다.

### 12.1 단위 테스트
- OHLCV 정규화/검증 테스트
- 재무 계정 매핑/정규화 테스트
- 뉴스 dedupe 테스트
- 뉴스 symbol linking 테스트
- 날짜/cutoff 해석 테스트

### 12.2 통합 테스트
- `sync_daily_ohlcv.py` smoke test
- `sync_fundamentals_snapshot.py` smoke test
- `sync_news_metadata.py` smoke test
- DuckDB 적재 후 row 존재 검증

원칙:
- 실제 외부 API 호출을 강제하는 테스트만 두지 말고, fixture/mock 기반 테스트도 충분히 둔다.
- 네트워크가 없어도 핵심 정규화 로직 테스트가 돌아야 한다.

---

## 13. 이번 티켓에서 Codex가 추가해도 좋은 파일 예시

아래는 예시이며, exact naming 은 저장소 구조에 맞게 조정 가능하다.

### 13.1 config
- `config/news_queries.yaml`
- `config/fundamentals_account_map.yaml`

### 13.2 app domain / pipeline
- `app/pipelines/daily_ohlcv.py`
- `app/pipelines/fundamentals_snapshot.py`
- `app/pipelines/news_metadata.py`
- `app/domain/fundamentals/account_normalizer.py`
- `app/domain/fundamentals/materializer.py`
- `app/domain/news/query_plan.py`
- `app/domain/news/symbol_linker.py`
- `app/domain/news/dedupe.py`
- `app/domain/validation/market_data.py`

### 13.3 scripts
- `scripts/sync_daily_ohlcv.py`
- `scripts/sync_fundamentals_snapshot.py`
- `scripts/sync_news_metadata.py`
- `scripts/backfill_core_research_data.py`

### 13.4 tests
- `tests/unit/test_ohlcv_validation.py`
- `tests/unit/test_fundamentals_normalization.py`
- `tests/unit/test_news_dedupe.py`
- `tests/unit/test_news_symbol_linker.py`
- `tests/integration/test_sync_daily_ohlcv.py`
- `tests/integration/test_sync_fundamentals_snapshot.py`
- `tests/integration/test_sync_news_metadata.py`

---

## 14. 완료 기준 (Definition of Done)

아래가 재현 가능해야 이번 티켓은 완료로 본다.

1. foundation + provider activation 상태에서 아래 명령이 실행 가능하다.

```bash
python scripts/sync_daily_ohlcv.py --date 2026-03-06 --limit-symbols 50
python scripts/sync_fundamentals_snapshot.py --as-of-date 2026-03-06 --limit-symbols 50
python scripts/sync_news_metadata.py --date 2026-03-06 --mode market_and_focus --limit-symbols 50
```

2. 전체 백필용 오케스트레이션이 가능하다.

```bash
python scripts/backfill_core_research_data.py --start 2026-03-02 --end 2026-03-06 --limit-symbols 50
```

3. DuckDB에서 아래 수준의 결과 확인이 가능하다.
- `fact_daily_ohlcv` row 존재
- `fact_fundamentals_snapshot` row 존재
- `fact_news_item` row 존재
- 중복 키가 없어야 함

4. Streamlit에서 최신 적재 상태가 보인다.

```bash
streamlit run app/ui/Home.py
```

5. README에 아래가 업데이트되어야 한다.
- 필요한 환경변수
- 각 스크립트 실행 방법
- 백필 방법
- 뉴스 query pack 조정 방법
- 재무 snapshot availability 규칙 설명
- known limitations

---

## 15. Codex가 절대 하지 말아야 하는 것

- 자동매매/주문/정정/취소 기능 추가
- 뉴스 본문 전문 저장을 기본 경로로 추가
- 불확실한 심볼 매칭을 공격적으로 강행
- 재무 snapshot 에 미래 공시를 과거 날짜에 붙이는 구조 생성
- TICKET-003 이후 범위(피처 전체/랭킹 전체)를 섞어서 과도하게 확장
- 기존 문서 원본을 삭제/이동/개명

---

## 16. 권장 구현 순서

Codex는 아래 순서를 권장한다.

1. `fact_daily_ohlcv` 스키마 및 pipeline 구현
2. OHLCV raw/curated 적재 + 검증 + smoke test
3. DART financial raw + 정규화 + snapshot materializer 구현
4. `fact_fundamentals_snapshot` 적재 + 테스트
5. 뉴스 query config + fetch + dedupe + symbol linking 구현
6. `fact_news_item` 적재 + 테스트
7. `backfill_core_research_data.py` 작성
8. Ops/UI 표시 보강
9. README 정리

이 순서가 가장 자연스럽다.

---

## 17. 이번 티켓이 끝난 뒤 다음 티켓으로 넘어갈 준비 상태

이번 티켓이 끝나면 다음 티켓에서는 아래가 가능해야 한다.

- trend / momentum / liquidity / flow / quality / value / news / risk / regime 피처 생성
- D+1 / D+5 라벨 생성
- 설명용 점수 구조와 랭킹 엔진 구현
- Market Pulse / Leaderboard / Stock Workbench 초안 고도화

즉, 이번 티켓의 산출물은 **TICKET-003의 피처/랭킹 구현을 위한 연구 데이터 기반**이다.

