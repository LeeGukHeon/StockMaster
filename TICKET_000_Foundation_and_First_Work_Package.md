# TICKET-000 — Foundation & First Work Package

- 문서 목적: Codex가 **바로 구현을 시작할 수 있는 수준**으로 첫 작업 범위와 산출물을 명확하게 정의한다.
- 문서 버전: v1.0
- 전제 문서: `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- 우선순위: 최상
- 난이도: 중상
- 기대 결과: “동작하는 뼈대 + 확장 가능한 기반” 확보

---

## 1. 이번 티켓의 목표

이번 티켓의 목표는 **모든 기능을 완성하는 것**이 아니다.

이번 티켓 하나로 아래 상태를 만든다.

> “리포지토리를 실행하면 앱 골격이 떠 있고,
> 데이터 저장 구조와 설정 구조가 준비되어 있으며,
> Provider 인터페이스, 배치 골격, DuckDB 부트스트랩, run manifest, Ops 기초 화면이 동작하는 상태”

즉, 이 티켓은 **플랫폼의 기초 공사**다.

---

## 2. 이 티켓에서 반드시 끝내야 하는 것

### 2.1 프로젝트 뼈대
- Python 프로젝트 초기화
- 의존성 관리 설정 (`pyproject.toml`)
- 패키지 구조 생성
- `README.md` 기본 작성
- `Makefile` 또는 동등한 개발 명령 집합 제공

### 2.2 실행 환경
- `Dockerfile`
- `docker-compose.yml`
- `.env.example`
- 로컬 실행 명령 확정
- 기본 볼륨 마운트 구성

### 2.3 설정 시스템
- `.env` + YAML/typed settings 조합
- 설정 로딩 실패 시 명확한 에러
- 환경별 (`local`, `prod`) 분기 가능한 구조

### 2.4 로깅/운영 뼈대
- structured logging
- run_id 생성기
- execution context
- 디스크 사용량 체크 유틸
- 보관정책 로더

### 2.5 저장소 뼈대
- `data/raw`, `data/curated`, `data/marts`, `data/cache`, `data/logs`, `data/artifacts`
- 디렉터리 자동 생성
- DuckDB 파일 생성/초기화
- 기본 dimension / ops 테이블 생성

### 2.6 Provider 인터페이스
아직 모든 API 호출을 완성할 필요는 없지만, 아래 인터페이스는 확정한다.
- `BaseProvider`
- `KISProvider`
- `DartProvider`
- `KrxProvider`
- `NaverNewsProvider`

각 provider는 최소한 다음 형태를 가진다.
- 인증/세션 초기화
- health check
- fetch 메서드(아직 stub 가능)
- raw 저장 훅
- 에러 처리 훅

### 2.7 배치/스케줄러 뼈대
- 수집 작업을 실행할 job runner
- daily pipeline runner 골격
- evaluation runner 골격
- APScheduler 또는 cron-compatible 엔트리포인트
- idempotent run 구조

### 2.8 Streamlit 기본 UI
최소 페이지 3개
- Home
- Ops
- Placeholder (Market Pulse or Leaderboard 자리)

Home에서는 아래를 보여준다.
- 프로젝트 이름
- 최근 run 상태
- 데이터 루트 경로
- 저장공간 사용량
- 아직 구현되지 않은 모듈 목록

Ops에서는 아래를 보여준다.
- 최근 `ops_run_manifest`
- 디스크 사용량
- 최근 에러/경고 placeholder

---

## 3. 이번 티켓에서 하지 않는 것

이번 티켓에서는 아래를 완성하지 않아도 된다.
- 실제 KIS/DART/네이버 전체 API 구현
- 예측 모델 학습
- 실제 종목 랭킹
- 뉴스 클러스터링
- Discord 메시지 포맷 완성
- 종목 상세 페이지 완성
- D+1/D+5 평가 계산 완성

단, **이 기능들을 나중에 바로 붙일 수 있는 구조**는 만들어야 한다.

---

## 4. 산출물 목록 (파일/기능 단위)

Codex는 최소 아래 산출물을 만든다.

### 4.1 최상위
- `README.md`
- `pyproject.toml`
- `.env.example`
- `Dockerfile`
- `docker-compose.yml`
- `Makefile`

### 4.2 설정/공통
- `app/settings.py`
- `config/settings.yaml`
- `config/logging.yaml`
- `config/retention.yaml`
- `app/logging.py`
- `app/common/run_context.py`
- `app/common/paths.py`
- `app/common/time.py`
- `app/common/disk.py`

### 4.3 저장/DB
- `app/storage/bootstrap.py`
- `app/storage/duckdb.py`
- `app/storage/parquet_io.py`
- `app/storage/manifests.py`

### 4.4 Providers
- `app/providers/base.py`
- `app/providers/kis/client.py`
- `app/providers/dart/client.py`
- `app/providers/krx/client.py`
- `app/providers/naver_news/client.py`

### 4.5 배치/작업자
- `app/scheduler/jobs.py`
- `scripts/bootstrap.py`
- `scripts/run_daily_pipeline.py`
- `scripts/run_evaluation.py`
- `scripts/prune_storage.py`

### 4.6 UI
- `app/ui/Home.py`
- `app/ui/pages/01_Ops.py`
- `app/ui/pages/02_Placeholder_Research.py`

### 4.7 테스트
- `tests/unit/test_settings.py`
- `tests/unit/test_run_context.py`
- `tests/unit/test_disk_guard.py`
- `tests/integration/test_bootstrap.py`

---

## 5. 상세 구현 요구사항

## 5.1 Python / 의존성
권장:
- Python 3.11+
- pandas
- duckdb
- pyarrow
- pydantic-settings or equivalent
- streamlit
- plotly
- apscheduler
- httpx
- tenacity
- loguru or structlog (택1)
- pytest

원칙:
- 의존성은 너무 과하게 넣지 않는다.
- 지금 꼭 필요 없는 대형 ML 프레임워크는 늦춰도 된다.
- 다만 LightGBM 등은 향후 추가가 쉬운 구조면 된다.

## 5.2 설정 로딩
`settings.py`는 다음을 만족해야 한다.
- `.env` 로드
- 기본값과 override 처리
- 필수 키 누락 시 명확한 예외
- `local`, `prod` 분기 가능
- 경로를 모두 `Path` 객체로 정규화

필수 설정 객체 예시:
- app metadata
- timezone
- data paths
- provider secrets
- retention windows
- storage thresholds
- discord settings

## 5.3 로깅
반드시 아래 필드를 로그에 포함할 수 있어야 한다.
- timestamp
- level
- module
- run_id
- run_type
- message
- extra json

로그 출력 위치:
- 콘솔
- `data/logs/app.log`

## 5.4 run manifest
`ops_run_manifest`는 이번 티켓에서 반드시 usable 상태여야 한다.

필수 기능:
- run_id 생성
- run start 기록
- run end 기록
- status = running / success / failed
- notes / artifact path / error message 기록

향후 모든 job은 이 manifest를 남겨야 하므로, 초기에 공통 유틸로 만든다.

## 5.5 DuckDB bootstrap
`bootstrap.py`가 최초 실행 시 아래를 수행해야 한다.
- 데이터 디렉터리 생성
- DuckDB DB 파일 생성
- dimension / ops table 생성
- 샘플 데이터 또는 빈 테이블 초기화
- bootstrap run manifest 기록

최소 생성 대상 테이블:
- `dim_symbol`
- `dim_trading_calendar`
- `ops_run_manifest`
- `ops_disk_usage_log`

## 5.6 Provider 인터페이스
`BaseProvider` 예시 책임:
- config 보관
- session/client 생성
- `health_check()`
- `fetch_*()` 메서드들의 공통 예외 처리
- raw payload optional save

각 provider는 지금은 완전 구현이 아니어도 다음이 있어야 한다.
- 생성 가능
- 환경변수 읽기 가능
- health check placeholder 동작
- 최소 1개 stub fetch 메서드

예시:
- KIS: `fetch_symbol_master()`, `fetch_daily_ohlcv()`
- DART: `fetch_corp_codes()`, `fetch_company_overview()`
- KRX: `fetch_market_summary()`
- Naver: `search_news()`

## 5.7 스토리지 헬퍼
`parquet_io.py`는 다음 역할을 가진다.
- 날짜 파티션 경로 생성
- DataFrame을 Parquet로 저장
- overwrite / append 정책 지원
- 저장된 파일 경로 반환

`manifests.py`는 다음 역할을 가진다.
- run manifest insert/update
- artifact path append
- error capture helper

## 5.8 디스크 가드
`disk.py`는 다음 기능을 가져야 한다.
- 현재 경로 기준 디스크 사용량 계산
- warning/prune/limit 임계치 판정
- 사람이 읽기 쉬운 메시지 반환
- 추후 `prune_storage.py`에서 재사용 가능

## 5.9 Streamlit UI 골격
### Home 페이지 요구사항
표시 항목:
- 서비스 제목: KR Stock Research Platform v1
- 현재 환경(local/prod)
- DuckDB 경로
- Data root 경로
- 디스크 사용량 카드
- 최근 run 10개
- 구현 상태 체크리스트

### Ops 페이지 요구사항
표시 항목:
- 최근 bootstrap/daily/evaluation run 테이블
- 디스크 워터마크 상태
- 경고 placeholder
- “향후 표시 예정” 영역

### Research Placeholder 페이지
표시 항목:
- 향후 Market Pulse/Leaderboard가 들어올 자리
- 현재는 mock cards 또는 empty state

UI는 너무 조잡하지 않게 구성한다.
- `st.metric`
- `st.dataframe`
- `st.status` 또는 유사 상태 뱃지
- 구분 섹션과 적절한 spacing 사용

---

## 6. 구현 단계 제안 (Codex 실행 순서)

Codex는 아래 순서대로 구현하는 것이 좋다.

### Step 1 — Repository bootstrap
- pyproject
- package init
- README
- env example
- Makefile

### Step 2 — Settings & logging
- settings loader
- logging config
- run context 유틸

### Step 3 — Storage bootstrap
- 디렉터리 생성
- DuckDB 연결
- 테이블 생성
- bootstrap script

### Step 4 — Provider skeletons
- BaseProvider
- 4개 provider stub
- health check

### Step 5 — Scheduler skeleton
- daily / evaluation runner
- manifest 연동

### Step 6 — Streamlit skeleton
- Home
- Ops
- Placeholder page

### Step 7 — Tests
- settings test
- bootstrap integration test
- disk guard test

---

## 7. 완료 기준 (Definition of Done)

이번 티켓은 아래를 모두 만족해야 완료다.

1. `cp .env.example .env` 후 기본 로컬 실행 가능
2. `python scripts/bootstrap.py` 실행 시 데이터 디렉터리와 DuckDB가 생성됨
3. `ops_run_manifest`에 bootstrap 실행 기록이 남음
4. `streamlit run app/ui/Home.py` 또는 동등한 엔트리포인트로 UI 실행 가능
5. Home / Ops / Placeholder 페이지가 열림
6. 최근 run 목록이 UI에 표시됨
7. 디스크 사용량이 UI에 표시됨
8. provider stub 객체 생성 및 health check 가능
9. 최소 테스트가 통과함
10. README에 실행 방법이 명확히 적혀 있음

---

## 8. 절대 하지 말아야 할 것

1. 아직 요구되지 않은 자동매매/주문 기능을 넣지 말 것
2. 전 종목 틱 저장 구조를 먼저 만들지 말 것
3. UI만 먼저 화려하게 만들고 내부 구조를 부실하게 두지 말 것
4. settings를 여기저기서 중복 파싱하지 말 것
5. run manifest 없이 job을 실행하지 말 것
6. raw/curated/artifacts 경로를 뒤섞지 말 것
7. 하드코딩된 절대 경로를 쓰지 말 것
8. 단일 giant script로 모든 것을 구현하지 말 것

---

## 9. Codex가 생성해주면 좋은 추가 문서

가능하면 아래도 함께 생성한다.
- `docs/architecture/overview.md`
- `docs/decisions/ADR-0001-stack.md`
- `docs/decisions/ADR-0002-storage-layout.md`
- `docs/tickets/TICKET-000.md` (현재 문서 사본)

---

## 10. 다음 티켓 예고 (참고)

이번 티켓 다음은 보통 아래 순서다.
- TICKET-001: 종목마스터 + 거래일 캘린더 + KIS/DART 기초 수집
- TICKET-002: 일봉/재무/뉴스 적재 파이프라인
- TICKET-003: 피처 스토어 v1
- TICKET-004: 랭킹 엔진 v1
- TICKET-005: Market Pulse / Leaderboard UI
- TICKET-006: Discord 전송 + D+1 평가

이번 티켓은 어디까지나 그 기반이다.

---

## 11. Codex에게 직접 전달할 실행 지시문

아래 문장을 그대로 Codex에 붙여넣어도 된다.

```text
당신은 KR Stock Research Platform v1의 foundation을 구현하는 엔지니어입니다.
먼저 구현 확정 방향서를 기준으로 프로젝트 뼈대를 만드세요.
이번 작업의 목표는 완성품이 아니라, 확장 가능한 기반을 만드는 것입니다.

반드시 구현할 것:
- Python 3.11+ 프로젝트 초기화
- pyproject.toml
- Dockerfile / docker-compose.yml
- .env.example
- settings loader
- structured logging
- run_id / run manifest
- data 디렉터리 bootstrap
- DuckDB bootstrap (dim_symbol, dim_trading_calendar, ops_run_manifest, ops_disk_usage_log)
- provider base class 및 KIS/DART/KRX/Naver provider stub
- scripts/bootstrap.py, scripts/run_daily_pipeline.py, scripts/run_evaluation.py, scripts/prune_storage.py
- Streamlit Home/Ops/Placeholder 페이지
- 최소 단위/통합 테스트

중요 제약:
- 자동매매/주문 기능은 넣지 말 것
- 전 종목 틱/호가 장기 저장 구조를 우선하지 말 것
- 모든 job은 run manifest를 남길 것
- data/raw, data/curated, data/marts, data/cache, data/logs, data/artifacts 구조를 지킬 것
- settings와 path, retention, thresholds는 하드코딩 대신 설정으로 뺄 것

완료 후 README에 로컬 실행 방법, bootstrap 방법, Streamlit 실행 방법, 테스트 실행 방법을 적으세요.
```

