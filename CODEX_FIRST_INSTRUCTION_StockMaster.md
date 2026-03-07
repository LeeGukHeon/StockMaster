# Codex 첫 전달용 지시서 — StockMaster 루트 기준

문서 목적: Codex가 **D:\\MyApps\\StockMaster** 루트 폴더를 기준으로, 이미 배치된 기획 문서를 읽고 **첫 번째 구현 작업**을 오해 없이 시작하도록 만드는 실행 지시서다.

이 문서는 “설명 문서”가 아니라 **즉시 실행 가능한 작업 지시문**이다.

---

## 0. 작업 컨텍스트

현재 프로젝트 루트는 아래와 같다.

- 루트 경로: `D:\MyApps\StockMaster`

루트 폴더에는 이미 아래 문서가 존재한다고 가정한다.

- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `CODEX_HANDOFF_BRIEF.md`

Codex는 **이 3개 문서를 루트 기준 source of truth로 간주**하고 작업을 시작해야 한다.

이 문서의 목표는 첫 번째 작업에서 다음 상태를 만드는 것이다.

> “프로젝트 저장소가 깔끔하게 초기화되어 있고,
> 설정/로그/저장소/Provider/UI의 골격이 있으며,
> `.gitignore`까지 포함된 안전한 기본 개발 기반이 마련된 상태”

---

## 1. Codex가 먼저 이해해야 하는 고정 전제

다음 전제는 이미 확정되었으므로, Codex는 별도 질문 없이 그대로 따른다.

### 1.1 제품 방향
- 프로젝트는 **국내주식 개인 리서치 플랫폼**이다.
- 자동매매가 아니라 **장후 분석 / 종목 랭킹 / 리포트 / 사후 평가**가 핵심이다.
- v1은 **국내주식(KOSPI/KOSDAQ)** 만 다룬다.
- 미국주식/나스닥은 후속 확장 범위다.

### 1.2 기술 방향
- 개발은 로컬에서 시작하지만, 구조는 **Oracle Cloud Linux + Docker** 배포를 고려한다.
- 저장공간은 **실사용 80GB 전제**다.
- 데이터는 **공식 API 우선**이다.
- 합법적 접근만 사용한다.
- 뉴스 본문 전문 대량 저장/재배포는 하지 않는다.
- 전 종목 틱/호가 장기 저장 구조를 지금 우선 구현하지 않는다.

### 1.3 모델 방향
- 사용자에게 보이는 점수는 설명용 계층이다.
- 실제 선별 철학은 다음을 반영하는 방향이다.
  - D+1 / D+5 초과수익률 기대값
  - 예측 불확실성
  - 모델 간 불일치
  - 거래비용/체결 가능성
  - 장세(regime) 적합성
- 하지만 이번 첫 작업에서는 **모델 구현이 아니라 foundation 구축**이 목적이다.

---

## 2. 이번 첫 작업의 목표

이번 첫 작업의 목표는 “기능 완성”이 아니다.

이번 첫 작업의 목표는 아래 4가지를 만드는 것이다.

1. **저장소 기본 구조 정리**
2. **`.gitignore`를 포함한 안전한 개발 환경 정리**
3. **애플리케이션/배치/스토리지/설정의 foundation 생성**
4. **이후 티켓들이 바로 이어질 수 있는 실행 가능한 skeleton 확보**

즉 이번 작업은 다음 한 줄로 요약된다.

> “프로젝트 루트 정리 + `.gitignore` 생성 + Python/Docker/Streamlit/DuckDB 기반 skeleton 구현”

---

## 3. Codex가 작업 시작 전에 반드시 할 일

Codex는 실제 파일 생성 전에 아래 순서를 따른다.

1. 루트 폴더 `D:\MyApps\StockMaster`를 기준으로 현재 파일 목록을 확인한다.
2. 아래 3개 문서를 먼저 읽는다.
   - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
   - `TICKET_000_Foundation_and_First_Work_Package.md`
   - `CODEX_HANDOFF_BRIEF.md`
3. 위 문서와 충돌하지 않도록 **첫 구현 범위를 foundation에 한정**한다.
4. 기존 문서를 삭제/이동/개명하지 않는다.
5. 루트 폴더를 기준으로 프로젝트 구조를 정리하되, 향후 코드와 문서가 섞이지 않게 배치한다.

중요:
- 기존 기획 문서는 **보존**한다.
- 구조를 새로 잡더라도, 기획 문서는 `docs/`로 옮기지 말고 **현재 루트 위치를 우선 유지**한다.
- 필요하면 사본을 `docs/`에 둘 수는 있지만, 원본은 건드리지 않는다.

---

## 4. 이번 첫 작업의 최우선 산출물

Codex는 이번 첫 작업에서 최소 아래 산출물을 만들어야 한다.

### 4.1 루트 기본 파일
- `README.md`
- `.gitignore`
- `.env.example`
- `pyproject.toml`
- `Dockerfile`
- `docker-compose.yml`
- `Makefile`

### 4.2 설정 및 공통 모듈
- `app/settings.py`
- `config/settings.yaml`
- `config/logging.yaml`
- `config/retention.yaml`
- `app/logging.py`
- `app/common/run_context.py`
- `app/common/paths.py`
- `app/common/time.py`
- `app/common/disk.py`

### 4.3 저장/DB 관련 모듈
- `app/storage/bootstrap.py`
- `app/storage/duckdb.py`
- `app/storage/parquet_io.py`
- `app/storage/manifests.py`

### 4.4 Provider skeleton
- `app/providers/base.py`
- `app/providers/kis/client.py`
- `app/providers/dart/client.py`
- `app/providers/krx/client.py`
- `app/providers/naver_news/client.py`

### 4.5 배치 및 스크립트
- `app/scheduler/jobs.py`
- `scripts/bootstrap.py`
- `scripts/run_daily_pipeline.py`
- `scripts/run_evaluation.py`
- `scripts/prune_storage.py`

### 4.6 UI skeleton
- `app/ui/Home.py`
- `app/ui/pages/01_Ops.py`
- `app/ui/pages/02_Placeholder_Research.py`

### 4.7 테스트
- `tests/unit/test_settings.py`
- `tests/unit/test_run_context.py`
- `tests/unit/test_disk_guard.py`
- `tests/integration/test_bootstrap.py`

### 4.8 문서 보강
가능하면 아래도 함께 만든다.
- `docs/architecture/overview.md`
- `docs/decisions/ADR-0001-stack.md`
- `docs/decisions/ADR-0002-storage-layout.md`
- `docs/tickets/TICKET-000.md`

---

## 5. `.gitignore`는 이번 첫 작업에 반드시 포함한다

Codex는 이번 첫 작업에서 `.gitignore`를 **직접 생성**해야 한다.

이 `.gitignore`는 다음 목적을 충족해야 한다.

1. Python 프로젝트 일반 산출물 무시
2. 가상환경 무시
3. `.env` 및 비밀키 무시
4. IDE/OS 잡파일 무시
5. 대용량 데이터/로그/캐시/아티팩트 무시
6. DuckDB/Parquet/임시 리포트/학습 산출물 무시
7. 다만 디렉터리 구조 유지를 위한 `.gitkeep`은 허용

### 5.1 `.gitignore`에 반드시 포함할 범주

#### Python
- `__pycache__/`
- `*.py[cod]`
- `.pytest_cache/`
- `.mypy_cache/`
- `.ruff_cache/`
- `.coverage`
- `htmlcov/`
- `dist/`
- `build/`
- `*.egg-info/`

#### Virtual environments
- `.venv/`
- `venv/`
- `env/`
- `.python-version`

#### Secrets / env
- `.env`
- `.env.*`
- `!.env.example`
- `secrets/`
- `*.pem`
- `*.key`
- `*.p12`

#### IDE / OS
- `.idea/`
- `.vscode/`
- `.DS_Store`
- `Thumbs.db`
- `desktop.ini`

#### Logs / temp
- `*.log`
- `logs/`
- `tmp/`
- `temp/`
- `.cache/`

#### Data lake / generated artifacts
- `data/raw/**`
- `data/curated/**`
- `data/marts/**`
- `data/cache/**`
- `data/logs/**`
- `data/artifacts/**`
- `!data/**/.gitkeep`

#### DB / analytic files
- `*.duckdb`
- `*.duckdb.wal`
- `*.parquet`
- `*.feather`
- `*.arrow`
- `*.csv`
- `*.tsv`

#### Streamlit / notebook / misc
- `.streamlit/secrets.toml`
- `.ipynb_checkpoints/`

#### ML / report artifacts
- `models/`
- `checkpoints/`
- `reports/generated/`
- `artifacts/`

중요:
- `.gitignore`는 너무 공격적으로 써서 **기획 문서까지 제외하면 안 된다.**
- 루트의 `.md` 기획 문서는 추적 대상에 남겨야 한다.

---

## 6. 권장 루트 구조

Codex는 아래 구조를 기준으로 저장소를 잡는다.

```text
D:\MyApps\StockMaster
├─ KR_Stock_Research_Platform_v1_Implementation_Spec.md
├─ TICKET_000_Foundation_and_First_Work_Package.md
├─ CODEX_HANDOFF_BRIEF.md
├─ README.md
├─ .gitignore
├─ .env.example
├─ pyproject.toml
├─ Dockerfile
├─ docker-compose.yml
├─ Makefile
├─ config/
├─ docs/
├─ scripts/
├─ app/
│  ├─ common/
│  ├─ providers/
│  ├─ scheduler/
│  ├─ storage/
│  └─ ui/
├─ tests/
└─ data/
   ├─ raw/
   ├─ curated/
   ├─ marts/
   ├─ cache/
   ├─ logs/
   └─ artifacts/
```

주의사항:
- `data/` 하위는 Git에 내용물을 올리지 않는다.
- 필요하면 각 디렉터리에 `.gitkeep`만 둔다.
- 코드와 생성 데이터가 섞이지 않게 유지한다.

---

## 7. 구현 시 지켜야 할 핵심 규칙

### 7.1 절대 하지 말 것
- 자동매매/주문 기능 추가 금지
- 전 종목 틱/호가 장기저장 구조 우선 구현 금지
- giant script 하나에 모든 기능 몰아넣기 금지
- 하드코딩된 절대 경로 남발 금지
- settings를 여러 파일에서 제각각 파싱하는 구조 금지
- run manifest 없이 배치 실행 금지
- UI만 만들고 내부 구조를 비워두는 식의 겉치레 구현 금지

### 7.2 반드시 지킬 것
- 경로는 `Path` 기반으로 정규화
- Windows 로컬 개발 + Linux 배포 둘 다 고려
- 설정은 `.env` + YAML + typed settings 기반
- 모든 배치 작업은 `run_id`를 갖고 `ops_run_manifest`에 기록
- DuckDB + Parquet 구조를 foundation 단계에서 반영
- UI와 배치/워커 로직 분리

---

## 8. 이번 작업에서 구현해야 하는 기능 수준

이번 첫 작업에서는 **완전한 데이터 수집 기능**이 아니라, 아래 수준이면 된다.

### 8.1 Settings
- `.env` 로드 가능
- 필수 설정 누락 시 명확한 오류
- `local` / `prod` 환경 분기 가능
- Path 정규화 가능

### 8.2 Logging
- structured logging 동작
- 콘솔 + 파일 출력 가능
- `run_id`, `run_type` 포함 가능

### 8.3 Storage bootstrap
- `data/` 하위 디렉터리 자동 생성
- DuckDB 파일 자동 생성
- 최소 테이블 생성
  - `dim_symbol`
  - `dim_trading_calendar`
  - `ops_run_manifest`
  - `ops_disk_usage_log`

### 8.4 Provider skeleton
각 provider는 지금은 stub이어도 좋다. 다만 아래는 가능해야 한다.
- 객체 생성
- 설정 주입
- health check placeholder
- 최소 1개 stub fetch 메서드

예시:
- KIS: `fetch_symbol_master()`, `fetch_daily_ohlcv()`
- DART: `fetch_corp_codes()`, `fetch_company_overview()`
- KRX: `fetch_market_summary()`
- Naver News: `search_news()`

### 8.5 Scheduler / scripts
- `bootstrap.py` 실행 가능
- `run_daily_pipeline.py` 실행 가능
- `run_evaluation.py` 실행 가능
- `prune_storage.py` 실행 가능
- 현재는 skeleton이어도 되지만 manifest 기록은 남아야 함

### 8.6 UI skeleton
- Streamlit Home / Ops / Placeholder 페이지 동작
- Home에서 최근 run, data root, DuckDB path, 디스크 사용량 표시
- Ops에서 run manifest와 디스크 워터마크 표시
- Placeholder 페이지에 향후 Market Pulse / Leaderboard 자리 표시

---

## 9. 완료 기준 (Definition of Done)

Codex는 아래가 충족될 때 이번 첫 작업을 완료로 본다.

1. 루트에 `.gitignore`가 생성되어 있다.
2. `.gitignore`가 비밀키/가상환경/데이터/로그/아티팩트를 적절히 제외한다.
3. `pyproject.toml`, `Dockerfile`, `docker-compose.yml`, `.env.example`, `Makefile`가 존재한다.
4. `python scripts/bootstrap.py` 실행 시 데이터 디렉터리와 DuckDB가 생성된다.
5. `ops_run_manifest`에 bootstrap 기록이 남는다.
6. `streamlit run app/ui/Home.py`로 UI가 열린다.
7. Home / Ops / Placeholder 페이지가 보인다.
8. provider stub이 생성 가능하고 health check placeholder가 동작한다.
9. 최소 테스트가 통과한다.
10. `README.md`에 로컬 실행법, bootstrap, Streamlit 실행법, 테스트 실행법이 적혀 있다.

---

## 10. Codex가 최종 보고 시 반드시 포함해야 하는 내용

Codex는 첫 작업을 마친 뒤, 최종 응답에 아래를 포함해야 한다.

1. **생성/수정한 파일 목록**
2. **루트 디렉터리 트리 요약**
3. **`.gitignore`에 어떤 범주를 넣었는지 설명**
4. **로컬 실행 방법**
   - 환경 변수 준비
   - bootstrap 실행
   - Streamlit 실행
   - 테스트 실행
5. **현재는 skeleton이고 후속 티켓에서 구현될 부분**
6. **가정한 사항/남은 TODO**

중요:
- “완성되지 않은 기능”을 완성된 것처럼 말하지 않는다.
- stub과 placeholder는 명확히 구분해서 보고한다.

---

## 11. Codex에게 바로 전달할 실제 지시문

아래 문구를 그대로 첫 메시지로 전달하면 된다.

```text
작업 루트는 D:\MyApps\StockMaster 입니다.
이 루트에는 아래 3개 문서가 이미 있습니다.
- KR_Stock_Research_Platform_v1_Implementation_Spec.md
- TICKET_000_Foundation_and_First_Work_Package.md
- CODEX_HANDOFF_BRIEF.md

이 3개 문서를 먼저 읽고 source of truth로 삼아 주세요.
기존 문서는 삭제/이동/개명하지 마세요.

이번 첫 작업의 목표는 기능 완성이 아니라 foundation 구축입니다.
반드시 먼저 해야 할 일은 아래와 같습니다.

1) 루트에 .gitignore 생성
2) Python/Docker/Streamlit/DuckDB 기반 저장소 skeleton 생성
3) settings / logging / run manifest / storage bootstrap 구현
4) provider skeleton 구현
5) scripts/bootstrap.py, scripts/run_daily_pipeline.py, scripts/run_evaluation.py, scripts/prune_storage.py 생성
6) Streamlit Home / Ops / Placeholder 페이지 생성
7) 최소 테스트 작성
8) README 정리

중요 제약:
- 자동매매/주문 기능 금지
- 전 종목 틱/호가 장기 저장 구조 우선 구현 금지
- settings는 중앙집중식으로 관리
- 모든 배치 작업은 run manifest 기록 필수
- data/raw, data/curated, data/marts, data/cache, data/logs, data/artifacts 구조 준수
- 경로 하드코딩 금지
- Windows 로컬 개발과 Linux 배포 둘 다 고려

.gitignore에는 최소 아래 범주가 반드시 포함되어야 합니다.
- Python 캐시/빌드 산출물
- 가상환경
- .env 및 secret 파일
- IDE/OS 잡파일
- logs, tmp, cache
- data/raw, data/curated, data/marts, data/cache, data/logs, data/artifacts
- *.duckdb, *.parquet, *.feather, *.arrow, *.csv, *.tsv
- models, checkpoints, generated reports
단, .env.example과 기획용 .md 문서는 추적 대상에 남겨 주세요.

완료 후에는 아래를 보고해 주세요.
- 생성/수정한 파일 목록
- 루트 트리 요약
- .gitignore 설명
- bootstrap / Streamlit / test 실행 방법
- 현재 skeleton인 부분과 후속 TODO
```

---

## 12. 추가 메모

이번 첫 작업은 프로젝트 성공에 매우 중요하다.

이번 단계에서 가장 중요한 것은 화려한 결과물이 아니라 아래다.
- 구조가 깨끗한가
- 설정이 일관적인가
- 저장소 레이어가 확장 가능한가
- run manifest 기반으로 운영 추적이 가능한가
- 이후 KIS/DART/Naver/KRX 연결을 무리 없이 붙일 수 있는가

즉, 이번 작업은 “겉모양”보다 **토대의 품질**이 핵심이다.
