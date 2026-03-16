# StockMaster 통합 런북

이 문서는 `2026-03-12` 기준 StockMaster의 단일 운영 기준 문서다.

목적:

- 다음 컨텍스트가 지금과 같은 수준으로 바로 유지보수, 개발, 서버 운영을 이어갈 수 있게 한다.
- 서버 접속, 코드 구조, 로컬 개발, 배포, 스케줄러, 데이터 저장소, 장애 대응을 한 문서에 묶는다.
- 이전의 분산된 배포/백업/외부접속/scheduler update 문서를 대체한다.

이 문서를 먼저 보고, 더 깊은 설계 배경이 필요할 때만 아래 참조 문서로 내려간다.

- 구조 배경: [overview.md](d:/MyApps/StockMaster/docs/architecture/overview.md)
- DuckDB/metadata split 배경: [DUCKDB_ANALYTICS_AND_METADATA_STORE_SPLIT.md](d:/MyApps/StockMaster/docs/architecture/DUCKDB_ANALYTICS_AND_METADATA_STORE_SPLIT.md)
- 스케줄러 개요: [SCHEDULER_AUTOMATION.md](d:/MyApps/StockMaster/docs/SCHEDULER_AUTOMATION.md)
- metadata/host worker 검증 체크: [METADATA_HOST_WORKER_VALIDATION.md](d:/MyApps/StockMaster/docs/METADATA_HOST_WORKER_VALIDATION.md)
- 사용자 관점 화면 설명: [USER_GUIDE.md](d:/MyApps/StockMaster/docs/USER_GUIDE.md)

## 1. 현재 운영 진실

- 로컬 작업 경로: `D:\MyApps\StockMaster`
- OCI 서버 코드 경로: `/opt/stockmaster/app`
- OCI 서버 runtime 경로: `/opt/stockmaster/runtime`
- OCI 서버 backup 경로: `/opt/stockmaster/backups`
- 분석 저장소: `/opt/stockmaster/runtime/data/marts/main.duckdb`
- 운영 메타데이터 저장소: Postgres `metadata_db`, loopback `127.0.0.1:5433`
- 앱 제공 방식: Docker `app` + `nginx` + `metadata_db`
- 배치 실행 방식: host `systemd timer` + `stockmaster-scheduler@.service` + `/opt/stockmaster/worker-venv/bin/python`
- 장중 분위기 UI: 장중 컨텍스트가 있으면 실시간 축 우선, 없으면 `전일 종가 기준` fallback을 명시한다.

중요한 운영 원칙:

- 서버 기본은 `DuckDB 분석 저장소 + Postgres 운영 메타데이터` split이다.
- UI 앱 컨테이너는 읽기 중심이다. 실제 write job은 host worker가 담당한다.
- scheduler 충돌은 `SKIPPED_LOCKED`가 정상일 수 있다. 무조건 장애로 해석하지 않는다.
- recovery run은 Discord에 stale 시장 요약을 다시 보내지 않는다.
- `ops-maintenance`는 Docker build cache와 오래된 model artifact를 자동 정리한다.

## 2. 서버 접속

현재 운영 PC 기준:

- 접속 배치 파일: `C:\Users\Administrator\Desktop\connect_oci.bat`
- SSH 키 폴더: `C:\Users\Administrator\Desktop\OCI_SSH_KEY`
- 현재 접속 계정/호스트: `ubuntu@168.107.44.206`

권장:

- 먼저 `connect_oci.bat` 내용을 확인하고, 그 안의 현재 키 경로/서버 주소를 기준으로 접속한다.
- Windows PowerShell에서 `ssh` PATH가 안 잡힐 수 있으므로 `C:\Windows\System32\OpenSSH\ssh.exe`를 직접 써도 된다.

직접 접속 예시:

```powershell
C:\Windows\System32\OpenSSH\ssh.exe `
  -i C:\Users\Administrator\Desktop\OCI_SSH_KEY\ssh-key-2026-03-05.key `
  ubuntu@168.107.44.206
```

## 3. 서버 구성

### 3.1 Docker / reverse proxy

- Compose 파일: `deploy/docker-compose.server.yml`
- nginx 설정: `deploy/nginx/default.conf`
- 서버 env 예시: `deploy/env/.env.server.example`
- server start/stop/restart:
  - `scripts/server/start_server.sh`
  - `scripts/server/stop_server.sh`
  - `scripts/server/restart_server.sh`
- smoke/public check:
  - `scripts/server/smoke_test_server.sh`
  - `scripts/server/check_public_access.sh`
- runtime info / logs / backup:
  - `scripts/server/print_runtime_info.sh`
  - `scripts/server/tail_server_logs.sh`
  - `scripts/server/backup_server_data.sh`

외부 노출 규칙:

- 외부 공개 포트는 `80`
- Streamlit 내부 포트는 `8501`
- `8501`은 외부 직접 공개 대상이 아니다

### 3.2 Scheduler / systemd

- service template: `deploy/systemd/stockmaster-scheduler@.service`
- timer 파일:
  - `deploy/systemd/stockmaster-ops-maintenance.timer`
  - `deploy/systemd/stockmaster-news-morning.timer`
  - `deploy/systemd/stockmaster-intraday-assist.timer`
  - `deploy/systemd/stockmaster-news-after-close.timer`
  - `deploy/systemd/stockmaster-evaluation.timer`
  - `deploy/systemd/stockmaster-daily-close.timer`
  - `deploy/systemd/stockmaster-daily-audit-lite.timer`
  - `deploy/systemd/stockmaster-daily-overlay-refresh.timer`
  - `deploy/systemd/stockmaster-weekly-training.timer`
  - `deploy/systemd/stockmaster-weekly-calibration.timer`
  - `deploy/systemd/stockmaster-weekly-policy-research.timer`
- scheduler helper:
  - `scripts/server/install_scheduler_units.sh`
  - `scripts/server/uninstall_scheduler_units.sh`
  - `scripts/server/status_scheduler_units.sh`
  - `scripts/server/run_scheduler_job_host.sh`

실제 실행 경로:

1. `systemd timer`
2. `stockmaster-scheduler@.service`
3. `scripts/server/run_scheduler_job_host.sh`
4. `/opt/stockmaster/worker-venv/bin/python`
5. `scripts/run_scheduled_bundle.py`

`scripts/server/run_scheduler_job.sh`는 호환용 wrapper다. 새 작업 기준은 `run_scheduler_job_host.sh`다.

## 4. 코드베이스 로우레벨 맵

다음 컨텍스트가 제일 자주 보는 경로만 먼저 적는다.

### 4.1 핵심 진입점

- 설정: `app/settings.py`
- DuckDB 저장소: `app/storage/duckdb.py`
- Postgres metadata 저장소: `app/storage/metadata_postgres.py`
- bootstrap: `scripts/bootstrap.py`
- 최신 snapshot/materialization: `app/release/snapshot.py`
- scheduler job catalog: `app/scheduler/jobs.py`
- ops bundle orchestration: `app/ops/bundles.py`
- ops maintenance: `app/ops/maintenance.py`
- UI helper/data loading: `app/ui/helpers.py`
- UI component skin/rendering: `app/ui/components.py`

### 4.2 기능별 패키지

- `app/ingestion/`
  - 유니버스, 캘린더, OHLCV, 재무, 뉴스, 수급 적재
- `app/features/`
  - feature store와 builder
- `app/regime/`
  - 일간 시장국면 분류
- `app/ranking/`
  - explanatory ranking
- `app/ml/`
  - alpha 학습, 추론, promotion, validation
- `app/selection/`
  - selection engine v1/v2
- `app/intraday/`
  - 장중 candidate assist, context, regime adjustment, meta overlay
- `app/portfolio/`
  - 후보군, 목표북, 리밸런스, NAV, 평가, 리포트
- `app/reports/`
  - Discord EOD/brief/postmortem/portfolio 등
- `app/ops/`
  - health, locks, retention, recovery, policy, runtime
- `app/ui/`
  - Streamlit pages

### 4.3 UI에서 자주 고치는 파일

- 홈: `app/ui/Home.py`
- 시장현황: `app/ui/pages/04_Market_Pulse.py`
- 종목분석: `app/ui/pages/05_Stock_Workbench.py`
- 장중콘솔: `app/ui/pages/07_Intraday_Console.py`
- 운영/헬스: `app/ui/pages/01_Ops.py`, `app/ui/pages/10_Health_Dashboard.py`
- 문서/도움말: `app/ui/pages/11_Docs_Help.py`

### 4.4 테스트 관례

- 단위 테스트: `tests/unit`
- 통합 테스트: `tests/integration`
- UI 포맷/표시 관련:
  - `tests/unit/test_ui_localize_frame.py`
  - `tests/unit/test_ui_market_mood.py`
  - `tests/unit/test_stock_workbench_live.py`
- 운영 관련:
  - `tests/unit/test_ops_retention.py`
  - `tests/integration/test_ops_framework.py`
  - `tests/integration/test_scheduler_framework.py`

## 5. 로컬 개발 기준

### 5.1 환경 준비

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .[dev]
Copy-Item .env.example .env
```

### 5.2 자주 쓰는 명령

```powershell
python -m pytest tests/unit/test_ui_localize_frame.py -q
python -m pytest tests/unit/test_stock_workbench_live.py -q
python -m pytest tests/unit/test_ops_retention.py -q
python -m py_compile app/ui/helpers.py app/ui/components.py
rg -n "pattern" app docs scripts tests
```

### 5.3 로컬과 서버 차이

- 로컬 `.env` 기본은 metadata split이 아닐 수 있다
- 서버 `.env.server` 기본은 `METADATA_DB_ENABLED=true`, `METADATA_DB_BACKEND=postgres`
- 서버 scheduler는 Docker 안이 아니라 host worker에서 돈다
- 서버 runtime data는 `/opt/stockmaster/runtime`에 있고, repo checkout 바깥 persistent 경로다

## 6. 서버 신규 구축

### 6.1 기본 전제

- Ubuntu 22.04/24.04
- Docker Engine + Compose plugin 설치
- 코드 checkout: `/opt/stockmaster/app`
- runtime root: `/opt/stockmaster/runtime`
- backup root: `/opt/stockmaster/backups`

### 6.2 서버 env 작성

```bash
cd /opt/stockmaster/app
cp deploy/env/.env.server.example deploy/env/.env.server
```

필수 확인:

- `APP_BASE_URL`
- `PUBLIC_PORT`
- `KIS_APP_KEY`, `KIS_APP_SECRET`
- `DART_API_KEY`
- `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
- `METADATA_DB_ENABLED=true`
- `METADATA_DB_BACKEND=postgres`
- `METADATA_DB_POSTGRES_DB=stockmaster_meta`
- `METADATA_DB_POSTGRES_USER=stockmaster`
- `METADATA_DB_POSTGRES_PASSWORD=...`
- `METADATA_DB_HOST_PORT=5433`

### 6.3 최초 기동

```bash
cd /opt/stockmaster/app
bash scripts/server/start_server.sh
bash scripts/server/smoke_test_server.sh
```

`start_server.sh`가 하는 일:

1. runtime 디렉터리 생성
2. 필요 시 Docker image build
3. metadata DB 기동과 readiness 대기
4. metadata schema bootstrap
5. Postgres target이 비어 있을 때만 DuckDB metadata migration
6. `scripts/bootstrap.py` 실행
7. app/nginx/metadata_db 기동
8. smoke test

### 6.4 공개 점검

```bash
cd /opt/stockmaster/app
bash scripts/server/check_public_access.sh http://YOUR_PUBLIC_IP
```

OCI/OS 방화벽 이슈가 있으면:

```bash
sudo iptables -C INPUT -p tcp --dport 80 -j ACCEPT 2>/dev/null || \
  sudo iptables -I INPUT 4 -p tcp --dport 80 -j ACCEPT
sudo netfilter-persistent save
```

## 7. 서버 일상 운영

### 7.1 기본 확인

```bash
cd /opt/stockmaster/app
git branch --show-current
git rev-parse --short HEAD
git status --short
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml ps
curl -fsS http://127.0.0.1/healthz
curl -fsS http://127.0.0.1/readyz
systemctl list-timers 'stockmaster-*.timer' --all
systemctl --failed --no-pager
bash scripts/server/print_runtime_info.sh
```

### 7.2 재배포 표준 절차

1. 로컬 변경 테스트
2. commit / push
3. 서버 접속
4. `/opt/stockmaster/app`에서 `git pull --ff-only origin main`
5. `FORCE_BUILD=true bash scripts/server/start_server.sh`
6. smoke test 확인

실행 예시:

```bash
cd /opt/stockmaster/app
git pull --ff-only origin main
FORCE_BUILD=true bash scripts/server/start_server.sh
```

### 7.3 서비스 제어

```bash
cd /opt/stockmaster/app
bash scripts/server/start_server.sh
bash scripts/server/stop_server.sh
bash scripts/server/restart_server.sh
bash scripts/server/tail_server_logs.sh
```

## 8. 스케줄러 운영

### 8.1 상태 확인

```bash
cd /opt/stockmaster/app
bash scripts/server/status_scheduler_units.sh
systemctl list-timers 'stockmaster-*.timer' --all
journalctl -u 'stockmaster-*.service' -n 200 --no-pager
```

### 8.2 설치/재설치

```bash
cd /opt/stockmaster/app
bash scripts/server/ensure_scheduler_worker_venv.sh
sudo -E bash scripts/server/install_scheduler_units.sh
```

### 8.3 수동 실행

```bash
cd /opt/stockmaster/app
sudo -E bash scripts/server/run_scheduler_job_host.sh news-morning --dry-run
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-close --dry-run --force --skip-discord
sudo systemctl start stockmaster-scheduler@evaluation.service
```

### 8.4 현재 운영 시간표

- `02:30` ops maintenance
- `08:30` morning news
- `08:55-15:15` intraday assist, 5분 간격
- `16:10` after-close news / close brief
- `16:20` evaluation
- `18:40` daily close
- `21:30` daily overlay refresh
- `05:30` daily audit lite
- `04:30` docker build cache cleanup
- `토 03:30` weekly training
- `토 10:00` weekly calibration
- `토 14:00` weekly policy research

### 8.5 상태 해석

- `SUCCESS`, `PARTIAL_SUCCESS`, `DEGRADED_SUCCESS`
  - 정상 완료 범주
- `SKIPPED_NON_TRADING_DAY`
  - 휴장일 정상 스킵
- `SKIPPED_ALREADY_DONE`
  - idempotent 스킵
- `SKIPPED_LOCKED`
  - 다른 writer가 있어 정상 스킵일 수 있음
- `BLOCKED`
  - readiness, disk policy, upstream 부족
- `FAILED`
  - 예외 또는 실제 장애

## 9. 데이터 저장소와 운영 메타데이터

### 9.1 현재 역할 분리

- DuckDB
  - OHLCV, feature, ranking, prediction, intraday snapshot, portfolio 등 분석 데이터
- Postgres metadata
  - `fact_job_run`, `fact_job_step_run`, `fact_active_lock`, `fact_recovery_action`, `fact_alert_event`, latest snapshot 계열

### 9.2 중요한 경로

- DuckDB: `/opt/stockmaster/runtime/data/marts/main.duckdb`
- server env 기준 runtime artifacts: `/opt/stockmaster/runtime/artifacts`
- metadata DB loopback: `127.0.0.1:5433`

### 9.3 주의할 점

- 레거시 artifact 경로가 `/opt/stockmaster/app/data/artifacts/...`로 남아 있을 수 있다
- UI helper는 현재 `resolve_ui_artifact_path()`로 runtime artifacts 경로 fallback을 지원한다
- model artifact cleanup은 active training run과 최신 group만 보존한다

## 10. 장 분위기 / 시장국면 해석

현재 기준:

- 장중에는 `latest_intraday_market_context_frame()` 기반 장중 분위기를 우선 쓴다
- 장중 데이터가 약하면 `장중 데이터 보강 중`으로 보류한다
- 장중 데이터가 없을 때만 `latest_regime_frame()` 기반 전일 종가 레짐을 보여준다
- fallback일 때는 반드시 `3월 11일 종가 기준`처럼 날짜를 표시한다

즉:

- `장 분위기`는 장중 컨텍스트
- `일간 시장국면`은 장마감 snapshot

둘을 같은 값처럼 쓰면 안 된다.

## 11. 로그 / 장애 대응

### 11.1 기본 로그

```bash
cd /opt/stockmaster/app
bash scripts/server/tail_server_logs.sh
journalctl -u 'stockmaster-*.service' -n 200 --no-pager
journalctl -u stockmaster-scheduler@daily-close.service -f
```

### 11.2 흔한 장애 원인

- `healthz` 실패
  - nginx 또는 app 컨테이너 문제
- `readyz` 실패
  - app 내부 readiness 실패
- `SKIPPED_LOCKED`
  - 실제 writer 선점 가능성, 즉시 장애로 단정 금지
- `BLOCKED`
  - dependency/disk/readiness 확인
- DuckDB artifact 경로 not found
  - 레거시 artifact 경로 가능성, `resolve_ui_artifact_path()` 확인
- 화면에 raw enum 노출
  - `app/ui/helpers.py`의 `UI_VALUE_LABELS`, `format_ui_value()`, `localize_frame()` 확인

### 11.3 복구 관련 명령

```bash
cd /opt/stockmaster/app
python scripts/force_release_stale_lock.py --lock-name scheduler_global_write
python scripts/recover_incomplete_runs.py --limit 5
python scripts/reconcile_failed_runs.py --limit 20
```

먼저 `ops-maintenance` 최신 실행 이력을 본다. 이미 stale 상태를 정리했을 수 있다.

## 12. 백업 / 복구

### 12.1 백업

```bash
cd /opt/stockmaster/app
bash scripts/server/backup_server_data.sh
bash scripts/server/backup_server_data.sh --dry-run
```

백업 기본 위치:

- `/opt/stockmaster/backups`

### 12.2 복구

1. 서비스 중지

```bash
cd /opt/stockmaster/app
bash scripts/server/stop_server.sh
```

2. 백업 해제

```bash
mkdir -p /tmp/stockmaster-restore
tar -xzf /opt/stockmaster/backups/stockmaster-backup-YYYYmmddTHHMMSSZ.tgz -C /tmp/stockmaster-restore
```

3. 필요한 경로만 복구

```bash
rsync -av /tmp/stockmaster-restore/runtime/ /opt/stockmaster/runtime/
rsync -av /tmp/stockmaster-restore/deploy/ /opt/stockmaster/app/deploy/
cp /tmp/stockmaster-restore/.env.server /opt/stockmaster/app/deploy/env/.env.server
```

4. 재기동

```bash
cd /opt/stockmaster/app
bash scripts/server/start_server.sh
```

복구 전 주의:

- 현재 상태도 별도 백업 후 복구하는 것이 안전하다
- `.env.server` secret이 최신 운영값과 맞는지 다시 확인한다
- destructive migration은 백업 없이 진행하지 않는다

## 13. 로우레벨 점검용 SQL / 명령

### 13.1 Postgres metadata

```bash
cd /opt/stockmaster/app
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml exec -T metadata_db \
  psql -U stockmaster -d stockmaster_meta
```

```sql
select status, job_name, as_of_date, started_at, finished_at, run_id
from fact_job_run
order by started_at desc
limit 20;

select lock_name, job_name, owner_run_id, status, acquired_at, released_at, release_reason
from fact_active_lock
order by acquired_at desc
limit 20;
```

### 13.2 디스크 / 용량

```bash
df -h
du -sh /opt/stockmaster/runtime/*
docker system df
```

지금까지 자주 큰 항목:

- Docker build cache
- `/opt/stockmaster/runtime/artifacts/models`
- `/opt/stockmaster/runtime/data/raw`
- `/opt/stockmaster/runtime/data/marts/main.duckdb`

## 14. 문서 정책

운영 기준:

- 운영 절차는 이 문서를 단일 기준으로 본다.
- 배포/백업/외부접속/scheduler 절차를 따로 분산 복제하지 않는다.
- 설계 배경은 architecture 문서에 둔다.
- 사용자 설명은 `USER_GUIDE.md`와 `REPORTS_AND_PAGES.md`에 둔다.
- update 메모 성격 문서는 새 기준 문서에 흡수하고 삭제한다.

이번 문서가 대체한 범주:

- OCI 신규 구축 메모
- 외부 접속 체크리스트
- 백업/복구 별도 문서
- scheduler server runbook
- metadata/host worker update 메모
## Dashboard Access Control

If you need direct access from your phone, keep the dashboard publicly reachable on port `80` and protect it with dashboard credentials instead of loopback-only binding.

```bash
# in deploy/env/.env.server
PUBLIC_BIND_HOST=0.0.0.0
PUBLIC_PORT=80
DASHBOARD_ACCESS_ENABLED=true
DASHBOARD_ACCESS_USERNAME=stockmaster
DASHBOARD_ACCESS_PASSWORD=change_this_dashboard_password
```

Change `DASHBOARD_ACCESS_PASSWORD` before starting the stack.
