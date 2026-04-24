# StockMaster 통합 운영 · 배포 · 테스트 매뉴얼

이 문서는 **현재 운영 중인 StockMaster 전체 코드베이스의 단일 기준 문서**다.
분산되어 있던 배포/운영/백업/scheduler 문서를 하나로 합쳤고, 실제 코드와 서버 운영 상태에
맞지 않는 레거시 설명은 제거했다.

대상 독자:

- 현재 운영 서버를 유지보수하는 운영자
- 로컬에서 기능을 개발하고 서버에 반영하는 개발자
- 배포/백업/스케줄/테스트를 처음 인계받는 다음 컨텍스트

문서 범위:

- 시스템 구조
- 서버 접속 방법
- 배포 방식
- 스케줄러/운영 흐름
- 데이터 저장소/보존 규칙
- 장애 대응
- **누락 없는 테스트 절차**

---

## 1. 현재 운영 진실

### 1.1 핵심 제품 정의

- 사용자 표면의 핵심은 **Discord bot**이다.
- 공개 웹 대시보드는 현재 운영 진입점이 아니다.
- H5 메인 추천 모델은 **`alpha_swing_d5_v2` 단일 주축**이다.
- 이전 D5 1세대 control lane은 **운영 경로/서버 데이터/registry/runtime에서 제거되었다.**
- H5 baseline comparator는 `alpha_recursive_expanding_v1`이다.

### 1.2 서버 기준 경로

- 코드 checkout: `/opt/stockmaster/app`
- runtime root: `/opt/stockmaster/runtime`
- backup root: `/opt/stockmaster/backups`
- 분석 저장소: `/opt/stockmaster/runtime/data/marts/main.duckdb`
- 운영 메타데이터 저장소: metadata Postgres (`127.0.0.1:5433`)
- host worker venv: `/opt/stockmaster/worker-venv`

### 1.3 현재 운영 표면

- 자동 알림: Discord webhook
- 사용자 조회: Discord slash command
- 서버 앱/컨테이너: metadata 중심 server stack
- 배치 실행: host `systemd timer` + host worker python

---

## 2. 시스템 구조 요약

### 2.1 큰 그림

StockMaster는 다음 4축으로 나뉜다.

1. **수집/적재**
   - OHLCV, 재무, 뉴스, 수급, 달력, 유니버스
2. **모델/선정**
   - feature store
   - market regime
   - alpha 학습/추론
   - selection engine
3. **평가/리포트**
   - evaluation
   - postmortem
   - discord EOD / brief / ops report
4. **운영**
   - scheduler
   - lock / recovery / retention / health
   - metadata Postgres

### 2.2 저장소 분리 원칙

- **Writer store**: `main.duckdb`
  - 수집, 학습, 평가, shadow, 포트폴리오 산출
- **Metadata store**: Postgres
  - 운영 메타데이터
  - Discord bot read-store snapshot

즉:

- 분석 데이터는 DuckDB
- 운영 메타데이터와 bot snapshot은 Postgres

### 2.3 사용자 조회 원칙

- 사용자 조회는 `main.duckdb` 직접 읽기보다 **bot read-store**를 우선한다.
- 배치 중에도 Discord bot 응답이 가능한 이유가 이 분리 때문이다.

---

## 3. 코드베이스 빠른 맵

### 3.1 가장 먼저 보는 파일

- 설정: `app/settings.py`
- 분석 저장소: `app/storage/duckdb.py`
- metadata 저장소: `app/storage/metadata_postgres.py`
- 스케줄 job catalog: `app/ops/scheduler.py`
- 배치 번들 orchestration: `app/ops/bundles.py`
- scheduler service 실행 진입: `scripts/run_scheduled_bundle.py`
- 서버 host scheduler wrapper: `scripts/server/run_scheduler_job_host.sh`

### 3.2 모델/추천 관련 핵심 파일

- 모델 상수/스펙: `app/ml/constants.py`
- 학습: `app/ml/training.py`
- 추론: `app/ml/inference.py`
- active registry freeze/rollback: `app/ml/active.py`
- auto-promotion: `app/ml/promotion.py`
- validation: `app/ml/validation.py`
- selection engine v2: `app/selection/engine_v2.py`
- D5/H5 indicator-product lane: `app/ml/indicator_product.py`

### 3.3 Discord 관련 핵심 파일

- bot service: `app/discord_bot/service.py`
- live analysis: `app/discord_bot/live_analysis.py`
- live recalc: `app/discord_bot/live_recalc.py`
- read-store materialization: `app/discord_bot/read_store.py`
- EOD report rendering/publish: `app/reports/discord_eod.py`

### 3.4 운영/복구 관련 핵심 파일

- ops maintenance: `app/ops/maintenance.py`
- health snapshot: `app/ops/health.py`
- lock runtime: `app/ops/locks.py`, `app/ops/runtime.py`
- ops validation: `app/ops/validation.py`

---

## 4. 서버 접속 방법

### 4.1 SSH 접속

현재 운영 서버는 다음 형식을 기준으로 접속한다.

```bash
ssh -i ~/.ssh/oci_oracle_stockmaster ubuntu@168.107.44.206
```

주의:

- 개인 키 경로는 **로컬 비공개 저장소**에 있어야 하며 repo에 넣지 않는다.
- 가능하면 `~/.ssh/config`에 alias를 만들어 쓴다.

예시:

```sshconfig
Host stockmaster-oci
    HostName 168.107.44.206
    User ubuntu
    IdentityFile ~/.ssh/oci_oracle_stockmaster
```

이후:

```bash
ssh stockmaster-oci
```

### 4.2 접속 후 가장 먼저 확인할 것

```bash
cd /opt/stockmaster/app
git rev-parse HEAD
git status --short
bash scripts/server/print_runtime_info.sh
```

---

## 5. 서버 배포 방식

### 5.1 신규 서버 구축

전제:

- Ubuntu 서버
- Docker Engine + Docker Compose plugin 설치
- `/opt/stockmaster/app`, `/opt/stockmaster/runtime`, `/opt/stockmaster/backups` 준비

env 작성:

```bash
cd /opt/stockmaster/app
cp deploy/env/.env.server.example deploy/env/.env.server
```

필수 채워야 하는 항목:

- KIS
- DART
- NAVER NEWS
- Discord webhook / bot token
- metadata Postgres 계정/포트

핵심 env:

- `APP_ENV=server`
- `APP_TIMEZONE=Asia/Seoul`
- `STOCKMASTER_PROJECT_ROOT=/opt/stockmaster/app`
- `STOCKMASTER_RUNTIME_ROOT=/opt/stockmaster/runtime`
- `STOCKMASTER_BACKUP_ROOT=/opt/stockmaster/backups`
- `METADATA_DB_ENABLED=true`
- `METADATA_DB_BACKEND=postgres`
- `METADATA_DB_HOST_PORT=5433`

최초 기동:

```bash
cd /opt/stockmaster/app
bash scripts/server/start_server.sh
bash scripts/server/smoke_test_server.sh
```

`start_server.sh`가 하는 일:

1. runtime 디렉터리 생성
2. metadata DB 기동 및 readiness 대기
3. metadata schema bootstrap
4. 필요 시 DuckDB metadata → Postgres initial migration
5. `scripts/bootstrap.py` 실행
6. server stack 기동
7. smoke test

### 5.2 Discord bot 서비스 설치

```bash
cd /opt/stockmaster/app
bash scripts/server/install_discord_bot_service.sh
```

설치 후 확인:

```bash
systemctl is-active stockmaster-discord-bot.service
```

### 5.3 스케줄러 설치

```bash
cd /opt/stockmaster/app
bash scripts/server/install_scheduler_units.sh
```

확인:

```bash
bash scripts/server/status_scheduler_units.sh
```

### 5.4 Day-2 배포

일반적인 코드 반영 절차:

```bash
cd /opt/stockmaster/app
git pull --ff-only origin main
bash scripts/server/smoke_test_server.sh
```

모델/registry/reports를 건드렸다면 추가로:

```bash
python3 scripts/materialize_alpha_model_specs.py
python3 scripts/materialize_discord_bot_read_store.py --as-of-date YYYY-MM-DD
python3 scripts/render_discord_eod_report.py --as-of-date YYYY-MM-DD --dry-run
sudo systemctl restart stockmaster-discord-bot.service
```

### 5.5 공개 접근 방식

현재 서버 정책:

- 외부 공개 포트는 `80`
- Streamlit 대시보드는 공개 진입점이 아니다
- 사용자 접근은 Discord bot이 기본이다

`scripts/server/check_public_access.sh`는 현재 의도적으로:

- “공개 dashboard 없음”
- “Discord bot 사용”

기준을 안내한다.

---

## 6. 스케줄러 / 타이머 운영

### 6.1 실행 경로

실제 실행 순서:

1. `systemd timer`
2. `stockmaster-scheduler@.service`
3. `scripts/server/run_scheduler_job_host.sh`
4. `/opt/stockmaster/worker-venv/bin/python`
5. `scripts/run_scheduled_bundle.py`

즉:

- scheduler는 app 컨테이너 내부 `docker exec` 기준이 아니다
- **host worker**가 기준이다

### 6.2 타이머 파일

- `deploy/systemd/stockmaster-ops-maintenance.timer`
- `deploy/systemd/stockmaster-news-morning.timer`
- `deploy/systemd/stockmaster-intraday-assist.timer`
- `deploy/systemd/stockmaster-news-after-close.timer`
- `deploy/systemd/stockmaster-evaluation.timer`
- `deploy/systemd/stockmaster-daily-close.timer`
- `deploy/systemd/stockmaster-daily-audit-lite.timer`
- `deploy/systemd/stockmaster-daily-overlay-refresh.timer`
- `deploy/systemd/stockmaster-docker-build-cache-cleanup.timer`
- `deploy/systemd/stockmaster-weekly-training.timer`
- `deploy/systemd/stockmaster-weekly-calibration.timer`
- `deploy/systemd/stockmaster-weekly-policy-research.timer`

### 6.3 현재 권장 운영 구성

일일 핵심:

- `news-morning`
- `news-after-close`
- `evaluation`
- `daily-close`
- `ops-maintenance`

주의:

- `daily-overlay-refresh`
- `daily-audit-lite`

는 **daily-close 성공 후 체인으로 따라가는 구조**가 있으므로,
별도 backstop timer를 항상 켜는지 여부는 중복 실행 정책과 함께 판단한다.

### 6.4 주요 schedule

- ops maintenance: 매일 `02:30`
- morning news: 평일 `08:30`
- intraday assist: 평일 장중 5분 간격
- after-close news: 평일 `16:10`
- evaluation: 평일 `16:20`
- daily-close: 평일 `17:30`
- daily-overlay-refresh: 평일 `21:30`
- weekly-training: 토요일 `03:30`
- weekly-calibration: 토요일 `10:00`
- weekly-policy-research: 토요일 `14:00`

### 6.5 상태 확인

```bash
systemctl list-timers 'stockmaster-*.timer' --all
systemctl --no-pager --full status stockmaster-scheduler@ops-maintenance.service
bash scripts/server/status_scheduler_units.sh
```

### 6.6 수동 실행

예시:

```bash
sudo -E bash scripts/server/run_scheduler_job_host.sh news-morning --dry-run
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-close --as-of-date 2026-04-23 --force --skip-discord
sudo -E bash scripts/server/run_scheduler_job_host.sh evaluation --as-of-date 2026-04-23 --force
```

### 6.7 상태 코드 해석

- `SUCCESS`: 정상 완료
- `PARTIAL_SUCCESS`: 일부 정리/보조 동작 포함 성공
- `DEGRADED_SUCCESS`: 핵심은 성공, 선택적 step 일부 실패
- `SKIPPED_ALREADY_DONE`: 같은 identity 재실행 스킵
- `SKIPPED_NON_TRADING_DAY`: 비거래일 정상 스킵
- `SKIPPED_LOCKED`: 다른 writer가 이미 잡고 있어 정상 스킵
- `BLOCKED`: upstream/readiness/disk 등 구조적 차단
- `FAILED`: 실제 예외

---

## 7. 일일 운영 흐름

### 7.1 장 시작 전

- `news-morning`
  - 야간/아침 뉴스 메타데이터 수집
  - Discord morning brief

### 7.2 장 마감 직후

- `news-after-close`
  - 장중/장후 뉴스 재수집
  - close brief

### 7.3 평가

- `evaluation`
  - matured outcome 집계
  - postmortem / evaluation summary
  - Discord postmortem surface 갱신

### 7.4 추천/Serving

- `daily-close`
  - feature / ranking / selection / model training
  - H5 active는 `alpha_swing_d5_v2`
  - Discord EOD / read-store 갱신

### 7.5 후속

- 성공 시 chain:
  - `daily-close` → `daily-overlay-refresh`
  - `daily-overlay-refresh` → `daily-audit-lite`

### 7.6 운영 유지보수

- `ops-maintenance`
  - health snapshot
  - stale lock / failed run cleanup
  - retention / disk watermark / model artifact cleanup

---

## 8. 데이터 저장소와 보존 규칙

### 8.1 저장소

- 분석 DB: `runtime/data/marts/main.duckdb`
- metadata DB: Postgres
- artifacts: `runtime/artifacts`
- logs: `runtime/logs`
- raw/cache/curated: `runtime/data/...`

### 8.2 retention 기본값

- raw API: `14일`
- intraday 5m: `45일`
- intraday 1m: `30일`
- orderbook summary: `21일`
- report cache: `21일`
- logs: `30일`

### 8.3 ops policy 핵심

- warning ratio: `0.70`
- cleanup ratio: `0.80`
- emergency ratio: `0.90`
- model artifact keep latest per group: `2`

### 8.4 보호 규칙

정리 대상 allowlist:

- `data/raw`
- `data/cache`
- `data/logs`
- `data/artifacts`
- `data/curated/intraday/bar_1m`
- `data/curated/intraday/trade_summary`
- `data/curated/intraday/quote_summary`

보호 prefix:

- `data/curated`
- `data/marts`

즉:

- 핵심 marts/curated 본체는 보호
- 오래된 raw/cache/log/artifact 중심으로 정리
- active model artifact는 별도 보호 + latest keep 규칙 적용

### 8.5 수동 정리

```bash
python3 scripts/enforce_retention_policies.py --dry-run
python3 scripts/cleanup_disk_watermark.py
python3 scripts/materialize_health_snapshots.py
```

---

## 9. 백업 / 복구

### 9.1 백업

```bash
bash scripts/server/backup_server_data.sh
```

백업 전 권장:

```bash
systemctl stop stockmaster-discord-bot.service
# 필요 시 scheduler timer 일시 중지
```

### 9.2 복구 기본 순서

1. app/bot/timer 중지
2. `/opt/stockmaster/runtime` 복원
3. 필요 시 metadata DB 복원
4. `bash scripts/server/start_server.sh`
5. `bash scripts/server/smoke_test_server.sh`
6. `bash scripts/server/status_scheduler_units.sh`

---

## 10. 누락 없는 테스트 방법

테스트는 **레이어별로 전부** 봐야 한다.
하나만 돌리고 끝내면 운영 누락이 생긴다.

### 10.1 공통 기본

모든 변경 공통:

```bash
ruff check .
pytest
```

### 10.2 scheduler / ops / timer 변경 시

```bash
python3 scripts/validate_scheduler_framework.py
python3 scripts/smoke_scheduler_bundles.py
python3 scripts/validate_ops_framework.py --as-of-date 2026-04-23
python3 scripts/validate_health_framework.py --as-of-date 2026-04-23
```

서버에서 추가:

```bash
bash scripts/server/status_scheduler_units.sh
sudo -E bash scripts/server/run_scheduler_job_host.sh news-morning --dry-run
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-close --as-of-date 2026-04-23 --dry-run --force --skip-discord
sudo -E bash scripts/server/run_scheduler_job_host.sh evaluation --as-of-date 2026-04-23 --dry-run --force
```

### 10.3 모델 / selection / 리포트 변경 시

```bash
python3 scripts/validate_alpha_model_v1.py --as-of-date 2026-04-23 --horizons 1 5
python3 scripts/validate_release_candidate.py --as-of-date 2026-04-23
python3 scripts/render_discord_eod_report.py --as-of-date 2026-04-23 --dry-run
python3 scripts/materialize_discord_bot_read_store.py --as-of-date 2026-04-23
```

필요 시:

```bash
python3 scripts/materialize_alpha_model_specs.py
python3 scripts/run_alpha_auto_promotion.py --as-of-date 2026-04-23 --horizons 1 5
```

### 10.4 서버/배포 변경 시

```bash
bash scripts/server/smoke_test_server.sh
bash scripts/server/check_public_access.sh
systemctl is-active stockmaster-discord-bot.service
```

### 10.5 배포 전 최소 게이트

다음 5개를 모두 만족해야 배포 완료로 본다.

1. `ruff check .`
2. `pytest`
3. 관련 validate/smoke script
4. 서버 dry-run 또는 smoke
5. render/read-store/output surface 직접 확인

### 10.6 추천 변경 시 최종 수동 체크

- H1 active model 확인
- H5 active model 확인
- Discord EOD dry-run 미리보기 확인
- read-store 갱신 후 bot snapshot 확인
- scheduler/timer 상태 확인

---

## 11. 운영 장애 대응

### 11.1 writer 충돌

- `SKIPPED_LOCKED`는 즉시 장애로 단정하지 않는다
- 먼저 active writer 존재 여부 확인

```bash
pgrep -af \"run_scheduler_job_host\\.sh|run_scheduled_bundle\\.py\"
```

### 11.2 stale lock 의심

```bash
python3 scripts/force_release_stale_lock.py
python3 scripts/materialize_health_snapshots.py
```

### 11.3 disk watermark

```bash
python3 scripts/cleanup_disk_watermark.py
python3 scripts/materialize_health_snapshots.py
df -h /opt/stockmaster/runtime
```

### 11.4 Discord bot 문제

```bash
sudo systemctl restart stockmaster-discord-bot.service
systemctl is-active stockmaster-discord-bot.service
python3 scripts/materialize_discord_bot_read_store.py --as-of-date 2026-04-23
```

### 11.5 모델 표면 이상

```bash
python3 scripts/materialize_alpha_model_specs.py
python3 scripts/validate_alpha_model_v1.py --as-of-date 2026-04-23 --horizons 1 5
python3 scripts/render_discord_eod_report.py --as-of-date 2026-04-23 --dry-run
```

---

## 12. 운영자가 기억해야 할 현재 제품 계약

- 사용자 표면은 **Discord bot 중심**
- H5 메인 모델은 **`alpha_swing_d5_v2`**
- D5 v1은 **운영에서 제거됨**
- H5 baseline comparator는 `alpha_recursive_expanding_v1`
- metadata split은 기본값
- scheduler는 host worker 기준
- public dashboard는 기본 진입점이 아님

---

## 13. 관련 보조 문서

이 문서 하나로 운영은 가능해야 한다.
다만 배경 설계가 필요하면 아래 문서를 참고한다.

- `docs/design/overview.md`
- `docs/design/DISCORD_BOT_READ_STORE_ARCHITECTURE.md`
- `docs/design/DUCKDB_ANALYTICS_AND_METADATA_STORE_SPLIT.md`
- `docs/operations/METADATA_HOST_WORKER_VALIDATION.md`
- `docs/user/USER_GUIDE.md`
- `docs/user/WORKFLOW_DAILY.md`
