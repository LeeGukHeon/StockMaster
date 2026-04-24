# DuckDB Analytics And Metadata Store Split

## 1. 목적

StockMaster의 핵심 목표는 아래 세 가지다.

- 한국 주식 데이터가 매일 누적되면서
- 모델/선정 로직이 주기적으로 재학습되고
- 시간이 갈수록 더 나은 종목 선별 정확도를 갖는 것

이 목표 자체는 `DuckDB + Parquet + 배치 학습` 구조와 잘 맞는다.
문제는 지금 `분석 저장소`와 `운영 메타데이터 저장소`가 같은 DuckDB 파일에 섞여 있다는 점이다.

현재 장애 패턴은 대부분 아래 두 계층이 분리되지 않아 생긴다.

- 분석 계층:
  - 일봉, 재무, 뉴스, feature, prediction, ranking, evaluation
- 운영 계층:
  - 스케줄러 상태, active lock, 실패 복구, latest snapshot, UI freshness, release check

이 문서의 목표는 다음이다.

- DuckDB는 `분석용 단일-writer 배치 저장소`로 유지한다.
- 운영 메타데이터는 별도 저장소로 분리한다.
- UI는 운영 메타데이터는 metadata DB에서, 대용량 분석 조회는 DuckDB snapshot/read-only에서 읽는다.
- 기존 데이터와 폴더 구조를 최대한 재활용하면서 다운타임과 재적재 비용을 줄인다.

## 2. 현재 상태 요약

### 2.1 현재 런타임 구조

- 코드:
  - `/opt/stockmaster/app`
- 런타임 데이터:
  - `/opt/stockmaster/runtime/data/raw`
  - `/opt/stockmaster/runtime/data/curated`
  - `/opt/stockmaster/runtime/data/marts/main.duckdb`
  - `/opt/stockmaster/runtime/artifacts`
- 배포:
  - Docker Compose
  - `app` Streamlit 컨테이너
  - `nginx` 컨테이너
  - host systemd timer + service

### 2.2 현재 실측 용량

2026-03-11 기준 OCI 서버 실측:

- root filesystem:
  - 총 175G
  - 사용 81G
  - 여유 94G
- 핵심 경로:
  - `main.duckdb`: `3.2G`
  - `data/marts`: `3.2G`
  - `artifacts`: `13G`
  - `data/cache`: `2.5M`

### 2.3 결론

- 현재 여유 용량 `94G`면 단기 운영에는 충분하다.
- 즉시 병목은 `용량`이 아니라 `동시 접근 구조`다.
- 가장 빨리 커지는 건 DuckDB보다 `artifacts`다.

## 3. 왜 현재 구조가 자꾸 깨지는가

### 3.1 DuckDB는 본질적으로 단일 writer 지향

DuckDB는 분석 쿼리에는 매우 강하지만, 여러 독립 프로세스가 같은 DB 파일에 계속 write하거나,
서로 다른 연결 모드로 자주 붙는 운영 DB로는 부적합하다.

현재는 아래가 하나의 파일을 같이 건드린다.

- Streamlit app
- scheduler bundle
- 수동 CLI
- recovery / maintenance
- latest snapshot / report index builder

### 3.2 충돌 유형

현재 장애는 크게 세 종류다.

1. read-only vs read-write config conflict
- 같은 파일에 이미 read-write 연결이 있는데 다른 경로가 read-only로 붙으려다 실패

2. writer lock conflict
- app 컨테이너 또는 다른 bundle이 writer를 잡고 있는데 새 batch가 붙으려다 실패

3. stale 운영 상태
- 실패한 컨테이너/세션이 `RUNNING` row, `fact_active_lock`, serial lock 디렉터리를 남김

### 3.3 구조적 원인

- `run_scheduler_job.sh`가 running app 컨테이너에 `exec`로 들어가 scheduler를 실행
- 운영 메타데이터도 DuckDB에 저장
- UI latest snapshot / freshness도 DuckDB에 write
- 실패한 run 정리가 예외 상황마다 항상 완결되지 않음

## 4. 목표 아키텍처

## 4.1 원칙

1. DuckDB는 분석 저장소로만 사용한다.
2. 운영 메타데이터는 metadata DB로 분리한다.
3. 배치 writer는 host scheduler 하나만 가진다.
4. UI는 DuckDB에 write하지 않는다.
5. 수동 실행도 scheduler와 동일한 단일 진입점으로만 수행한다.

## 4.2 권장 구성

- `DuckDB`
  - 역할: 대용량 분석 fact / feature / ranking / evaluation / report payload source
  - 접근 정책:
    - batch writer 1개
    - UI는 snapshot/read-only

- `Postgres`
  - 역할: 운영 메타데이터 저장소
  - 접근 정책:
    - app read
    - scheduler read/write
    - recovery/maintenance read/write

- `Parquet / artifacts`
  - 역할:
    - raw/curated 산출물
    - model artifact
    - report preview/payload

- `host systemd scheduler`
  - 역할:
    - 모든 batch orchestration
    - timeout / kill / restart / journald logging

## 4.3 프로세스 구조

- `nginx`:
  - Docker 유지
- `app`:
  - Docker 유지
  - DuckDB snapshot/read-only only
  - metadata DB read
- `scheduler-worker`:
  - host Python venv 권장
  - Docker 제거 권장
  - DuckDB write + metadata DB write

## 5. 어떤 테이블을 어디에 둘 것인가

## 5.1 DuckDB에 남길 것

이들은 대용량 분석/집계/모델링 데이터이므로 DuckDB 유지가 맞다.

- 기준/시장 데이터
  - `dim_symbol`
  - `dim_trading_calendar`
  - `fact_daily_ohlcv`
  - `fact_fundamentals_snapshot`
  - `fact_news_item`
  - `fact_investor_flow`
  - `fact_market_regime_snapshot`

- feature / label / ranking / prediction
  - `fact_feature_snapshot`
  - `fact_forward_return_label`
  - `fact_ranking`
  - `fact_prediction`
  - `fact_selection_outcome`
  - `fact_evaluation_summary`
  - `fact_calibration_diagnostic`

- ML / alpha / intraday / portfolio 분석 테이블
  - `fact_model_training_run`
  - `fact_model_metric_summary`
  - `fact_alpha_*`
  - `fact_intraday_*`
  - `fact_portfolio_*`

- 분석용 뷰
  - `vw_latest_evaluation_summary`
  - `vw_latest_calibration_diagnostic`
  - `vw_feature_matrix_latest`
  - 기타 ranking/evaluation/intraday/portfolio 조회 뷰

## 5.2 Postgres로 옮길 것

이들은 운영 메타데이터이며, 자주 작은 단위로 갱신되고, 락/상태 전이가 중요하다.

- 실행/단계 메타데이터
  - `ops_run_manifest`
  - `fact_job_run`
  - `fact_job_step_run`

- 락/복구/정책
  - `fact_active_lock`
  - `fact_recovery_action`
  - `fact_active_ops_policy`

- 운영 health / retention / alert
  - `fact_pipeline_dependency_state`
  - `fact_health_snapshot`
  - `fact_disk_watermark_event`
  - `fact_retention_cleanup_run`
  - `fact_alert_event`

- latest / release / UI freshness
  - `fact_latest_app_snapshot`
  - `fact_latest_report_index`
  - `fact_release_candidate_check`
  - `fact_ui_data_freshness_snapshot`

- scheduler 상태
  - 파일 lock / `scheduler_state/*.json` / `scheduler_serial_locks/*`
  - 최종적으로 metadata DB 테이블로 대체

## 5.3 이유

- 이 테이블들은 크기가 작다.
- 동시 접근/상태 전이가 중요하다.
- 웹 UI와 scheduler가 동시에 만지기 쉽다.
- Postgres의 row-level lock / transaction / connection semantics가 더 적합하다.

## 6. 기존 데이터 재활용 가능성

## 6.1 결론

가능하다. 재다운로드/재적재를 처음부터 다시 할 필요는 없다.

### 그대로 재활용 가능한 것

- `raw`
- `curated`
- `artifacts`
- `main.duckdb` 안의 분석 테이블 대부분

### 옮겨야 하는 것

- 운영 메타데이터 테이블만 DuckDB에서 Postgres로 1회 migration

### 다시 생성하면 되는 것

- latest snapshot
- report index
- UI freshness
- release candidate check

이들은 소규모 derived metadata라 필요하면 재계산이 더 안전하다.

## 6.2 권장 재활용 전략

1. DuckDB 분석 테이블은 유지
2. Postgres schema 생성
3. DuckDB의 운영 테이블만 export/import
4. latest/release/freshness는 새 구조에서 재생성

## 7. 목표 폴더 구조

현재 폴더 구조는 크게 틀리지 않는다. 다만 운영 메타데이터를 DuckDB에서 떼어낼 수 있게 의도를 명확히 해야 한다.

권장 구조:

```text
runtime/
  data/
    raw/
    curated/
    marts/
      main.duckdb
      snapshots/
  artifacts/
  logs/
    app/
    scheduler/
  backups/

deploy/
  docker-compose.server.yml
  docker-compose.metadata.yml

app/
  storage/
    duckdb.py
    metadata_store.py
    metadata_postgres.py
  ops/
    repository.py
    runtime.py
    scheduler.py
```

## 8. 용량 계획

## 8.1 현재 기준

- DuckDB 3.2G
- artifacts 13G
- free 94G

## 8.2 예상

- DuckDB는 일봉/재무/평가 위주이면 수개월~1년은 충분히 버틴다.
- 장기적으로 더 빨리 커질 가능성이 큰 건:
  - artifacts
  - intraday parquet
  - model artifact 누적본

## 8.3 권장 운영 기준

- `artifacts` retention 강제
- intraday raw/summary retention 별도
- DuckDB는 월간 compact/backup 체크
- free disk 25% 이하로 내려가면 경고

## 9. 단계별 구현 계획

## Phase 0. 즉시 안정화

목표:
- 지금 같은 writer 충돌과 stale state를 먼저 줄인다.

포함:
- read-only snapshot fallback
- active lock dead owner auto-heal
- serial lock dead pid auto-heal
- stale RUNNING row cleanup
- Discord/평가 용어 한국어화

상태:
- 이번 턴에서 이미 1차 반영

## Phase 1. metadata store abstraction 도입

목표:
- 운영 메타데이터 접근을 DuckDB 직결 코드에서 분리

작업:
- `MetadataStore` 인터페이스 도입
- `DuckdbMetadataStore` 구현
- `PostgresMetadataStore` 초안 구현
- `app.ops.repository`, `app.storage.manifests`, latest snapshot builders를 인터페이스 뒤로 숨김

완료 기준:
- 운영 메타데이터 코드가 DuckDB SQL에 직접 묶이지 않음

## Phase 2. Postgres 도입

목표:
- 운영 메타데이터 write를 Postgres로 이전

작업:
- dependency 추가:
  - `psycopg[binary]`
- settings 추가:
  - `METADATA_DB_URL`
  - `METADATA_DB_ENABLED`
- schema migration script 작성
- metadata repository를 Postgres backend로 전환

완료 기준:
- `fact_job_run`, `fact_active_lock`, `fact_recovery_action` 등이 Postgres에 저장됨

## Phase 3. scheduler host worker 분리

목표:
- scheduler가 더 이상 app 컨테이너에 `exec`로 들어가지 않게 함

작업:
- OCI host Python venv 설치
- host systemd service가 `python scripts/run_scheduled_bundle.py` 직접 실행
- `run_scheduler_job.sh` / systemd unit / runbook 수정
- app Docker는 UI 전용으로 유지

완료 기준:
- scheduler 실행 중 app 컨테이너 생사와 무관하게 batch가 끝까지 실행됨

## Phase 4. UI read path 정리

목표:
- UI는 metadata DB + DuckDB snapshot만 사용

작업:
- latest app snapshot / report index / health summary는 metadata DB에서 읽음
- 대용량 분석 테이블만 DuckDB snapshot에서 읽음
- app startup bootstrap write 제거

완료 기준:
- UI가 writer lock 충돌의 원인이 되지 않음

## Phase 5. historical cleanup and backfill

목표:
- 현재 남은 dirty historical rows 정리

작업:
- stale `RUNNING` / orphan recovery action 정리 script
- 기존 scheduler state 파일을 metadata DB로 마이그레이션
- old failed runs 정리 기준 수립

완료 기준:
- 운영 테이블에 의미 없는 `RUNNING` / dead lock / old orphan 상태가 없음

## 10. 정합성 규칙

아래 규칙은 반드시 강제한다.

1. 분석 fact는 DuckDB append/replace grain을 유지한다.
2. 운영 상태는 Postgres transaction 안에서 기록한다.
3. `run_id`는 전 계층에서 공통 키로 유지한다.
4. 운영 메타데이터 삭제 대신 terminal status 전환을 기본으로 한다.
5. UI latest snapshot은 metadata DB 기준 truth를 보여준다.
6. 수동 실행도 scheduler 단일 진입점을 통한다.

## 11. 실패 복구 규칙

### batch가 죽었을 때

- systemd timeout 또는 process exit 감지
- metadata store에서 run status를 terminal 상태로 전환
- DB active lock 해제
- serial lock 해제
- recovery action 생성 여부 판단

### 서버 재부팅 후

- startup health check에서 stale lock scan
- orphan RUNNING row cleanup
- 필요한 bundle만 재실행

## 12. 사용자-facing 용어 원칙

현재 가장 큰 문제 중 하나는 Discord/평가 화면이 기술 용어 중심이라는 점이다.

앞으로 원칙:

- `D+1`, `D+5` 대신
  - `1거래일`
  - `5거래일`
- `postmortem`
  - `사후 점검`
  - `사후 평가`
- `proxy`
  - `참고값`
  - `통계 기반 참고 범위`
- `hit rate`
  - `수익 플러스 비율`
  - 필요 시 `적중률` 병기

기술 용어가 꼭 필요할 때만 괄호로 뒤에 붙인다.

## 13. 이번 턴 기준 구현 반영 항목

이번 턴에서 이미 반영한 것:

- DuckDB read-only lock/config conflict 시 snapshot fallback
- active lock owner run 종료/죽은 pid 자동 회수
- serial lock dead pid 자동 회수
- stale RUNNING row 자동 실패 처리
- ops maintenance에서 stale RUNNING cleanup
- Discord 장마감/사후 평가 메시지 한국어화
- `D+1/D+5` 일부 UI 문구를 `1거래일/5거래일` 중심 표현으로 교체

즉, 이 문서는 “앞으로 할 일”만이 아니라 “이미 착수된 운영 안정화의 기준 문서”다.

## 14. 최종 판단

- 모델 개발/배치 분석 저장소로는 DuckDB를 계속 써도 충분하다.
- 지금 장애를 만든 건 DuckDB 자체보다 `운영 메타데이터까지 DuckDB에 몰아넣은 구조`다.
- 따라서 최종 정답은:
  - DuckDB 제거가 아니라
  - `DuckDB 분석 저장소 유지 + metadata store 분리 + host scheduler worker 단일화`

이 방향이면 기존 데이터는 거의 그대로 재활용하면서도,
운영 충돌 문제를 구조적으로 줄일 수 있다.
