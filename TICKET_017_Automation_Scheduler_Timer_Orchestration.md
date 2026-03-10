# TICKET-017 — Automation Scheduler / Timer / Orchestration Baseline

## 1. Goal

Server 운영 기준으로 StockMaster의 배치 실행을 자동화한다. 본 티켓은 **장후 추천 자동 생성**, **사후 평가 자동 실행**, **주간 학습 후보 생성**, **운영 점검/정리 작업 자동화**를 포함한다.

핵심 목표는 아래 4개다.

1. 사람이 매일 수동으로 스크립트를 치지 않아도 장후 추천 결과가 자동으로 생성될 것
2. DuckDB single-writer 전제를 깨지 않도록 모든 write job이 순차 실행될 것
3. 모델은 자동으로 "후보"까지 만들되, production 배포는 사람 승인 없이는 바뀌지 않을 것
4. system reboot / container restart / 일시적 장애 후에도 재가동과 재실행 규칙이 명확할 것

---

## 2. Scope

### 포함
- 서버용 scheduler 방식 확정
- systemd service + timer 단위 설계
- Docker Compose 기반 실행 방식 확정
- daily / evaluation / intraday / weekly-train / ops-maintenance 작업 스케줄 정의
- serial execution guard
- lock / stale-run / timeout / retry 정책
- scheduler runbook, 설치 스크립트, health check 요구사항
- UI/Ops에 scheduler 상태 노출

### 제외
- 자동 주문/자동매매
- production model 자동 교체
- 다중 서버 분산 스케줄러
- Kubernetes, Celery, Airflow 같은 대형 orchestration 도입
- 실시간 초저지연 event streaming 재설계

---

## 3. Architecture Decision

### 최종 원칙

StockMaster 서버 운영에서는 **앱 내부 스케줄러를 주 스케줄러로 쓰지 않는다.**

#### primary scheduler
- **systemd timer**

#### command execution target
- **docker compose exec -T app ...**
- 또는 필요 시 **docker compose run --rm runner ...**

#### in-app scheduler 사용 원칙
- APScheduler는 개발/테스트 또는 별도 단일 runner 프로세스에서만 허용
- Streamlit UI 프로세스 내부에 background scheduler를 넣지 않는다
- 다중 app replica / restart 시 중복 실행 가능성이 있는 구조는 금지

### 이유
1. 서버 재부팅 이후 자동 복구가 명확해야 한다.
2. DuckDB single-writer 특성 때문에 외부에서 순차 실행 제어가 필요하다.
3. systemd는 unit/timer 상태와 journald 로그를 통해 운영 추적이 쉽다.
4. 스케줄 정의, enable/disable, 장애 복구, 수동 재실행이 단순하다.

---

## 4. Required Job Families

### A. Daily close pipeline
목적:
- 장후 데이터 materialization
- 피처 스냅샷 생성
- selection / portfolio / report / latest snapshot 반영
- Discord daily report publish

실행 논리:
1. trading-day 여부 확인
2. upstream readiness 확인
3. market / news / fundamentals materialization
4. feature snapshot / regime snapshot
5. alpha inference / selection engine / portfolio engine
6. daily report bundle render
7. latest app snapshot / report index rebuild
8. audit-lite / freshness snapshot 갱신
9. Discord publish

권장 시각:
- **평일 18:40 Asia/Seoul**
- 단, 실제 거래일/휴장일 판단은 스크립트 내부에서 `dim_trading_calendar` 기준으로 최종 확인

### B. Daily evaluation pipeline
목적:
- 이전 selection / portfolio / intraday timing의 matured outcome 평가
- postmortem 리포트 생성

권장 시각:
- **평일 16:20 Asia/Seoul**
- 마감 데이터 반영을 위해 장 종료 후 약간의 버퍼 확보

### C. Intraday candidate assist pipeline
목적:
- 전일 selection 후보군의 당일 장중 timing assist 갱신

권장 방식:
- 1분마다 전체 시장을 돌지 말고 **candidate-only** 범위 유지
- 5분 주기부터 시작

권장 스케줄:
- 평일 08:55, 09:05~15:15 사이 5분 주기
- 또는 장중 long-running service 1개 + internal loop (후보군 수가 늘면 이 방식 고려)

현재 v1 권장:
- **systemd timer 기반 5분 주기 oneshot job**

### D. Weekly training candidate pipeline
목적:
- 새 학습 데이터셋 materialization
- walk-forward / validation / retrain candidate 생성
- comparison report 생성
- production model은 바꾸지 않음

권장 시각:
- **토요일 03:30 Asia/Seoul** 또는 **일요일 04:00 Asia/Seoul**
- 시장이 닫힌 시간에 무겁게 실행

출력:
- candidate model artifact
- metrics / comparison / recommendation
- active model 자동 교체 금지

### E. Weekly calibration / ablation pipeline
목적:
- policy calibration
- regime parameter tuning
- ablation 결과 업데이트

권장 시각:
- weekly training 직후

### F. Ops maintenance pipeline
목적:
- retention cleanup
- disk watermark check
- orphan artifact check
- stale lock cleanup
- lightweight integrity check

권장 시각:
- 매일 02:30

### G. Daily audit-lite pipeline
목적:
- latest layer consistency
- artifact reference integrity
- freshness / active ID sanity

권장 시각:
- daily close pipeline 종료 직후

---

## 5. Schedule Matrix (initial baseline)

모든 시간대는 **Asia/Seoul** 기준이다.

| job | cron-like schedule | purpose | blocking class |
|---|---|---|---|
| ops_maintenance | daily 02:30 | cleanup, lock cleanup, disk check | exclusive |
| weekly_training_candidate | Sat 03:30 | retrain candidate generation | exclusive-heavy |
| weekly_calibration | Sat 06:30 | policy/regime calibration | exclusive-heavy |
| evaluation_close | Mon-Fri 16:20 | matured outcome evaluation | exclusive |
| daily_close | Mon-Fri 18:40 | next-day recommendation generation | exclusive |
| daily_audit_lite | Mon-Fri 19:05 | latest/report/artifact sanity checks | exclusive |
| intraday_assist | Mon-Fri 08:55-15:15 every 5 min | timing assist | exclusive-light |
| release_smoke_optional | Sun 09:00 | server smoke / page smoke / docs check | optional |

### important
- `exclusive` / `exclusive-heavy` / `exclusive-light` 모두 **동시에 write 실행되면 안 된다**
- 실제로는 전부 단일 serial runner를 통과해야 한다
- 거래일이 아니면 `daily_close`, `evaluation_close`, `intraday_assist`는 self-skip 한다

---

## 6. Serial Execution Model

### 절대 원칙
- `main.duckdb` write job은 동시 실행 금지
- scheduler는 여러 개 timer를 가지더라도, 실제 작업 진입은 단일 lock discipline을 거쳐야 한다

### required mechanism

#### lock file / advisory lock
예시:
- `/var/lib/stockmaster/locks/global_write.lock`
- `/var/lib/stockmaster/locks/intraday.lock`

#### policy
- global write lock이 잡혀 있으면 다른 write job은 `SKIPPED_LOCKED` 또는 `DEFERRED` 처리
- intraday는 light class지만 역시 global write 진행 중이면 skip/requeue
- heavy job(training/calibration)은 daily close / evaluation과 겹치면 안 된다

### stale lock handling
- lock owner pid/container/run_id 기록
- max age 초과 시 stale 판단
- stale cleanup은 ops maintenance만 수행

---

## 7. Orchestration Entry Points

Codex는 개별 스크립트가 많더라도 아래처럼 **번들 runner** 를 추가해야 한다.

### required scripts
- `scripts/run_daily_close_bundle.py`
- `scripts/run_evaluation_bundle.py`
- `scripts/run_intraday_assist_bundle.py`
- `scripts/run_weekly_training_bundle.py`
- `scripts/run_weekly_calibration_bundle.py`
- `scripts/run_ops_maintenance_bundle.py`
- `scripts/run_daily_audit_lite_bundle.py`
- `scripts/ops/run_serial_job.py`

### runner contract
입력 예시:
- `--job daily_close`
- `--as-of-date YYYY-MM-DD` (optional, default resolved)
- `--dry-run`
- `--force`
- `--skip-discord`
- `--skip-heavy-train`

출력:
- structured stdout
- run_id
- exit code
- ops tables 반영

---

## 8. systemd Design

### required units
- `stockmaster-daily-close.service`
- `stockmaster-daily-close.timer`
- `stockmaster-evaluation.service`
- `stockmaster-evaluation.timer`
- `stockmaster-intraday-assist.service`
- `stockmaster-intraday-assist.timer`
- `stockmaster-weekly-training.service`
- `stockmaster-weekly-training.timer`
- `stockmaster-weekly-calibration.service`
- `stockmaster-weekly-calibration.timer`
- `stockmaster-ops-maintenance.service`
- `stockmaster-ops-maintenance.timer`
- `stockmaster-daily-audit-lite.service`
- `stockmaster-daily-audit-lite.timer`

### service pattern
- `Type=oneshot`
- explicit working directory
- docker compose command execution
- timeout 지정
- non-zero exit on failed bundle
- stdout/stderr journald 수집

### timer pattern
- `OnCalendar=` 사용
- `Persistent=true` 사용 권장
- timezone은 서버 로컬 timezone 또는 unit env로 명확화
- 너무 정밀할 필요 없는 jobs는 randomized delay 허용 가능

---

## 9. Docker / Compose Requirements

### compose principle
- core `app` service는 항상 올라와 있음
- optional `runner` profile은 필요 시 지원
- 스케줄러는 컨테이너 내부가 아니라 host systemd 에서 docker compose command를 호출

### acceptable execution patterns

#### pattern A (preferred initially)
`docker compose exec -T app python scripts/run_daily_close_bundle.py`

장점:
- 이미 떠 있는 app 환경 재사용
- 간단함

주의:
- app container가 살아 있어야 함
- command는 non-interactive 모드(`-T`)로 실행

#### pattern B (optional later)
`docker compose --profile runner run --rm runner python scripts/run_daily_close_bundle.py`

장점:
- 실행마다 clean process
- long-running app와 분리

주의:
- start cost 증가
- volume/env mount 일치 필수

---

## 10. Auto-Learning Policy

### daily automatic
- inference / selection / portfolio / report 는 자동 실행

### weekly automatic
- retraining candidate generation 은 자동 실행 가능
- calibration / ablation 은 자동 실행 가능

### explicitly forbidden
- active production model 자동 교체
- active portfolio policy 자동 교체
- active intraday meta model 자동 교체

### required deployment flow
1. weekly train candidate 생성
2. metrics/report 생성
3. recommendation 생성
4. 사람 승인
5. active model/policy registry update

즉, **자동 학습 후보 생성은 허용하되 자동 배포는 금지**

---

## 11. Trading Calendar Rules

### hard requirement
Mon-Fri timer만으로는 충분하지 않다. 한국 휴장일/임시휴장/대체공휴일이 있으므로 각 bundle은 내부에서 `dim_trading_calendar` 를 확인해야 한다.

### required behaviors
- non-trading day면 `SKIPPED_NON_TRADING_DAY`
- already materialized면 idempotent no-op 또는 `SKIPPED_ALREADY_DONE`
- missing upstream readiness면 `DEGRADED_SUCCESS` 또는 `BLOCKED`

---

## 12. Job State / Exit Semantics

### standardized statuses
- `SUCCEEDED`
- `FAILED`
- `PARTIAL_SUCCESS`
- `DEGRADED_SUCCESS`
- `BLOCKED`
- `SKIPPED`
- `SKIPPED_NON_TRADING_DAY`
- `SKIPPED_ALREADY_DONE`
- `SKIPPED_LOCKED`

### exit code principle
- success-like status: exit 0
- failure/blocking requiring operator action: non-zero

---

## 13. Observability Requirements

UI/Ops에서 아래를 보여야 한다.

### scheduler health widgets
- next scheduled runs
- last successful run per job
- last failed run per job
- last duration
- current lock state
- current active timers/services expectation
- server local time / timezone

### documentation/help
- timer map
- manual rerun commands
- skip rules
- holiday handling 설명

### logs
- journald + application log path 둘 다 보존

---

## 14. Deliverables

### code
- serial runner
- bundle runners
- schedule-aware helper utilities
- lock utilities
- job registration metadata
- systemd unit templates
- install/uninstall helper scripts

### docs
- scheduler architecture doc
- server runbook
- failure playbook
- manual rerun cheatsheet

### ops
- timer status validation script
- scheduler smoke script
- manual catch-up script

---

## 15. Acceptance Criteria

1. 서버 재부팅 후 timer가 자동 복구된다.
2. `daily_close` 가 장후 자동 실행되어 다음날 추천 리포트를 생성한다.
3. `evaluation_close` 가 자동 실행되어 matured outcome을 기록한다.
4. `weekly_training_candidate` 가 자동 실행되어 candidate artifact와 comparison report를 만든다.
5. active model/policy는 자동으로 바뀌지 않는다.
6. 동시에 두 write job이 들어와도 둘 다 write 하지 않고 lock discipline에 따라 하나만 수행된다.
7. non-trading day에는 daily/intraday/evaluation jobs가 self-skip 한다.
8. Ops/Health/Docs UI에서 scheduler 상태를 확인할 수 있다.
9. manual rerun command가 문서화되어 있다.
10. install/runbook만 보고 다른 사람이 재설정 가능하다.

---

## 16. Out of Scope Follow-up

이 티켓 이후 필요 시 후속으로 분리 가능:
- scheduler HA
- distributed queue
- dedicated runner container profile 고도화
- model approval UI
- calendar-based catch-up orchestration
