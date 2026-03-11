# 서버 운영 통합 런북

이 문서는 2026-03-11 기준 StockMaster OCI 서버의 단일 운영 기준 문서입니다.

- 1차 구축 절차: [DEPLOY_OCI.md](d:/MyApps/StockMaster/docs/DEPLOY_OCI.md)
- 구조 배경: [DUCKDB_ANALYTICS_AND_METADATA_STORE_SPLIT.md](d:/MyApps/StockMaster/docs/architecture/DUCKDB_ANALYTICS_AND_METADATA_STORE_SPLIT.md)
- 스케줄러 요약: [SCHEDULER_AUTOMATION.md](d:/MyApps/StockMaster/docs/SCHEDULER_AUTOMATION.md)
- 메타데이터 검증 절차: [METADATA_HOST_WORKER_VALIDATION.md](d:/MyApps/StockMaster/docs/METADATA_HOST_WORKER_VALIDATION.md)

기존 `*_UPDATE.md` 성격 문서는 이 문서로 통합했습니다.

## 1. 서버 접속

- Windows 관리자 PC 기준 접속 파일: `C:\Users\Administrator\Desktop\connect_oci.bat`
- SSH 키 폴더: `C:\Users\Administrator\Desktop\OCI_SSH_KEY`
- 현재 스크립트 기준 대상 서버: `ubuntu@168.107.44.206`

접속 정보가 바뀌었을 수 있으므로, 직접 SSH 명령을 복사하기보다 먼저 바탕화면 `connect_oci.bat` 내용을 확인하는 것을 기준으로 합니다.

## 2. 현재 운영 기준

- 앱 코드: `/opt/stockmaster/app`
- 런타임 루트: `/opt/stockmaster/runtime`
- 백업 루트: `/opt/stockmaster/backups`
- 분석 저장소: `/opt/stockmaster/runtime/data/marts/main.duckdb`
- 운영 메타데이터 저장소: Docker `metadata_db`, loopback `127.0.0.1:5433`
- host worker venv: `/opt/stockmaster/worker-venv`

현재 프로세스 구조:

- Docker: `app`, `nginx`, `metadata_db`
- systemd: `stockmaster-compose.service`
- scheduler: host `systemd timer` + `stockmaster-scheduler@.service`
- scheduler 실제 실행 경로:
  - `run_scheduler_job_host.sh`
  - `/opt/stockmaster/worker-venv/bin/python`
  - `scripts/run_scheduled_bundle.py`

로컬과 서버 기준 차이:

- 로컬 `.env` 기본값은 `METADATA_DB_ENABLED=false`, `METADATA_DB_BACKEND=duckdb`
- 서버 `.env.server`는 `METADATA_DB_ENABLED=true`, `METADATA_DB_BACKEND=postgres`
- 서버 UI는 metadata Postgres + DuckDB snapshot/read-only 조합을 기준으로 동작

## 3. 기본 점검 명령

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

일상 운영 체크 순서:

1. `docker compose ... ps`
2. `healthz`, `readyz`
3. `systemctl list-timers 'stockmaster-*.timer' --all`
4. `systemctl --failed --no-pager`
5. `bash scripts/server/tail_server_logs.sh`

## 4. 서버 시작 / 중지 / 재시작

```bash
cd /opt/stockmaster/app
bash scripts/server/start_server.sh
bash scripts/server/stop_server.sh
bash scripts/server/restart_server.sh
bash scripts/server/tail_server_logs.sh
```

재부팅 후 확인:

```bash
sudo systemctl status stockmaster-compose.service
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml ps
```

이미지 재빌드까지 포함하려면:

```bash
cd /opt/stockmaster/app
FORCE_BUILD=true bash scripts/server/start_server.sh
```

## 5. 코드 업데이트 / 롤백

업데이트 순서:

1. `bash scripts/server/backup_server_data.sh`
2. 코드 반영
3. `bash scripts/server/ensure_scheduler_worker_venv.sh`
4. systemd 자산이 바뀌었으면 `sudo -E bash scripts/server/install_scheduler_units.sh`
5. `FORCE_BUILD=true bash scripts/server/start_server.sh`
6. `bash scripts/server/smoke_test_server.sh`
7. `bash scripts/server/check_public_access.sh http://YOUR_PUBLIC_IP`

롤백 순서:

1. 이전 커밋 또는 이전 배포본으로 코드 복원
2. `FORCE_BUILD=true bash scripts/server/start_server.sh`
3. 필요하면 최신 백업에서 데이터만 복구

## 6. 자동 스케줄러 운영

기본 명령:

```bash
cd /opt/stockmaster/app
bash scripts/server/status_scheduler_units.sh
sudo -E bash scripts/server/install_scheduler_units.sh
bash scripts/server/uninstall_scheduler_units.sh
```

수동 실행:

```bash
cd /opt/stockmaster/app
sudo -E bash scripts/server/run_scheduler_job_host.sh news-morning --dry-run
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-close --dry-run --force --skip-discord
sudo systemctl start stockmaster-scheduler@evaluation.service
```

`scripts/server/run_scheduler_job.sh`는 deprecated wrapper이고, 실제로는 `run_scheduler_job_host.sh`로 위임됩니다.

현재 timer 기준:

| job | service slug | schedule |
|---|---|---|
| ops maintenance | `ops-maintenance` | daily 02:30 |
| morning news | `news-morning` | Mon-Fri 08:30 |
| intraday assist | `intraday-assist` | Mon-Fri 08:55-15:15 every 5 min |
| after-close news | `news-after-close` | Mon-Fri 16:10 |
| after-close brief | same run | Mon-Fri 16:10 | 장마감 직후 브리핑만 발행 |
| evaluation | `evaluation` | Mon-Fri 16:20 |
| daily close | `daily-close` | Mon-Fri 18:40 |
| daily audit lite | `daily-audit-lite` | Mon-Fri 19:05 |
| weekly training candidate | `weekly-training` | Sat 03:30 |
| weekly calibration | `weekly-calibration` | Sat 06:30 |

락 해석 기준:

- serial lock 충돌: `SKIPPED_LOCKED`
- active DB lock 충돌: scheduler 레이어에서는 `SKIPPED_LOCKED` 정상 스킵으로 처리
- 실제 `fact_job_run`에는 내부 시도 이력이 `BLOCKED`로 남을 수 있음
- readiness 부족, 디스크 emergency watermark, upstream 부족 같은 구조적 차단은 `BLOCKED`
- 예외/회귀는 `FAILED`

즉 `scheduler_global_write`가 이미 잡혀 있을 때의 스킵은 정상 운영 범주이며, 바로 장애로 취급하지 않습니다.

메시지 운영 기준:

- `16:10`은 최종 추천이 아니라 장마감 직후 브리핑
- 최종 추천 종목 Discord 발행은 `18:40 daily-close` 완료 이후만 허용

## 7. 메타데이터 / Postgres 운영

필수 서버 env 키:

- `METADATA_DB_ENABLED=true`
- `METADATA_DB_BACKEND=postgres`
- `METADATA_DB_SCHEMA=stockmaster_meta`
- `METADATA_DB_POSTGRES_DB=stockmaster_meta`
- `METADATA_DB_POSTGRES_USER=stockmaster`
- `METADATA_DB_POSTGRES_PASSWORD=...`
- `METADATA_DB_HOST_PORT=5433`

기본 검증:

```bash
cd /opt/stockmaster/app
python3 scripts/bootstrap_metadata_store.py
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml run --rm app python scripts/migrate_duckdb_metadata_to_postgres.py --truncate-first
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml run --rm app python scripts/build_report_index.py
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml run --rm app python scripts/build_ui_freshness_snapshot.py
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml run --rm app python scripts/build_latest_app_snapshot.py --as-of-date 2026-03-11
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml run --rm app python scripts/validate_release_candidate.py --as-of-date 2026-03-11
```

Postgres 직접 확인:

```bash
cd /opt/stockmaster/app
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml exec -T metadata_db psql -U stockmaster -d stockmaster_meta
```

예시 조회:

```sql
select status, job_name, as_of_date, started_at, finished_at, run_id
from stockmaster_meta.fact_job_run
order by started_at desc
limit 20;

select lock_name, job_name, owner_run_id, status, acquired_at, released_at, release_reason
from stockmaster_meta.fact_active_lock
order by created_at desc
limit 20;
```

## 8. 로그 / 장애 대응

컨테이너 로그:

```bash
cd /opt/stockmaster/app
bash scripts/server/tail_server_logs.sh
```

journald:

```bash
journalctl -u 'stockmaster-*.service' -n 200 --no-pager
journalctl -u stockmaster-scheduler@daily-close.service -f
```

자주 보는 장애 분류:

- `healthz` 실패: nginx 또는 호스트 포트 문제
- `readyz` 실패: upstream app 부팅 실패 또는 Streamlit 문제
- 외부 접속 실패: OCI ingress / 공인 IP / OS 방화벽 / 포트 매핑 문제
- `SKIPPED_LOCKED`: 실제 writer가 이미 돌고 있으면 정상
- `BLOCKED`: upstream 부족, 정책 차단, 디스크 한계 등 점검 필요
- `FAILED`: 예외 stack trace 확인 필요

복구 보조 스크립트:

```bash
cd /opt/stockmaster/app
python scripts/force_release_stale_lock.py --lock-name scheduler_global_write
python scripts/recover_incomplete_runs.py --limit 5
python scripts/reconcile_failed_runs.py --limit 20
```

`ops-maintenance`가 먼저 stale 상태를 정리하는 경우가 많으므로, 수동 강제 조치 전에 최근 maintenance 실행 이력부터 확인합니다.

## 9. 외부 공개 점검

```bash
cd /opt/stockmaster/app
bash scripts/server/check_public_access.sh http://YOUR_PUBLIC_IP
```

Oracle Ubuntu 기본 iptables가 `80/tcp`를 막는 경우:

```bash
sudo iptables -C INPUT -p tcp --dport 80 -j ACCEPT 2>/dev/null || \
  sudo iptables -I INPUT 4 -p tcp --dport 80 -j ACCEPT
sudo netfilter-persistent save
```

## 10. 문서 운영 원칙

- 서버 운영 기준은 이 문서를 우선한다.
- `DEPLOY_OCI.md`는 신규 서버 구축 절차 용도로만 본다.
- `SCHEDULER_*_UPDATE.md`, `DEPLOY_OCI_METADATA_UPDATE.md`는 아카이브 메모로 유지한다.
- 구조 배경과 의사결정은 architecture 문서를 본다.
