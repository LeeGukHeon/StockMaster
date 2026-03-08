# 자동 스케줄러 서버 설치 / 운영 Runbook

## 설치 자산

- `deploy/systemd/stockmaster-scheduler@.service`
- `deploy/systemd/stockmaster-ops-maintenance.timer`
- `deploy/systemd/stockmaster-news-morning.timer`
- `deploy/systemd/stockmaster-intraday-assist.timer`
- `deploy/systemd/stockmaster-news-after-close.timer`
- `deploy/systemd/stockmaster-evaluation.timer`
- `deploy/systemd/stockmaster-daily-close.timer`
- `deploy/systemd/stockmaster-daily-audit-lite.timer`
- `deploy/systemd/stockmaster-weekly-training.timer`
- `deploy/systemd/stockmaster-weekly-calibration.timer`
- `scripts/server/install_scheduler_units.sh`
- `scripts/server/uninstall_scheduler_units.sh`
- `scripts/server/status_scheduler_units.sh`
- `scripts/server/run_scheduler_job.sh`

## 설치 순서

서버 경로 기준:

```bash
cd /opt/stockmaster/app
bash scripts/server/install_scheduler_units.sh
bash scripts/server/status_scheduler_units.sh
```

설치 스크립트는:

- systemd unit/timer 파일 복사
- `daemon-reload`
- timer enable/start

를 순서대로 수행합니다.

## 제거 순서

```bash
cd /opt/stockmaster/app
bash scripts/server/uninstall_scheduler_units.sh
```

## 상태 확인

```bash
cd /opt/stockmaster/app
bash scripts/server/status_scheduler_units.sh
systemctl list-timers 'stockmaster-*.timer' --all
```

## 수동 재실행

개별 bundle을 scheduler 경로 그대로 수동 실행하려면:

```bash
cd /opt/stockmaster/app
sudo systemctl start stockmaster-scheduler@news-morning.service
sudo systemctl start stockmaster-scheduler@daily-close.service
sudo systemctl start stockmaster-scheduler@weekly-training.service
```

직접 host shell에서 서비스 wrapper를 호출할 수도 있습니다.

```bash
cd /opt/stockmaster/app
bash scripts/server/run_scheduler_job.sh news-morning --dry-run
bash scripts/server/run_scheduler_job.sh daily-close --dry-run --skip-discord
```

## service / timer 목록

| job | service slug | schedule |
|---|---|---|
| ops maintenance | `ops-maintenance` | daily 02:30 |
| morning news | `news-morning` | Mon-Fri 08:30 |
| intraday assist | `intraday-assist` | Mon-Fri 08:55-15:15 every 5 min |
| after-close news | `news-after-close` | Mon-Fri 16:10 |
| evaluation | `evaluation` | Mon-Fri 16:20 |
| daily close | `daily-close` | Mon-Fri 18:40 |
| daily audit lite | `daily-audit-lite` | Mon-Fri 19:05 |
| weekly training candidate | `weekly-training` | Sat 03:30 |
| weekly calibration | `weekly-calibration` | Sat 06:30 |

## lock discipline

T17은 host timer가 여러 개여도 DB write는 하나씩만 통과시키는 구조입니다.

1. systemd timer가 service를 호출
2. service는 `scripts/server/run_scheduler_job.sh`를 실행
3. shell script는 `docker compose exec -T app python scripts/run_scheduled_bundle.py ...`를 호출
4. `run_scheduled_bundle.py`는 serial file lock + scheduler state를 확인
5. 실제 bundle 함수는 `JobRunContext` / DB active lock까지 거칩니다

즉 lock은 2단입니다.

- host/file lock: scheduler 중복 실행 방지
- DB active lock: write job 중복 실행 방지

## skip / blocked 규칙

- non-trading day: `SKIPPED_NON_TRADING_DAY`
- same identity already done: `SKIPPED_ALREADY_DONE`
- serial lock occupied: `SKIPPED_LOCKED`
- readiness 부족: `BLOCKED`
- optional step 일부 실패: `DEGRADED_SUCCESS`

## journald / 로그 확인

```bash
journalctl -u 'stockmaster-*.service' -n 200 --no-pager
journalctl -u stockmaster-scheduler@daily-close.service -f
```

컨테이너 로그:

```bash
cd /opt/stockmaster/app
bash scripts/server/tail_server_logs.sh
```

## smoke / validation

```bash
cd /opt/stockmaster/app
docker compose --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml ps
python scripts/validate_scheduler_framework.py
python scripts/smoke_scheduler_bundles.py
curl http://127.0.0.1/healthz
curl http://127.0.0.1/readyz
```

로컬 Windows에서 검증할 때는 Streamlit이 `main.duckdb`를 잡고 있으면 write bundle smoke가 잠길 수 있습니다. 이 경우에는 Streamlit을 잠깐 내린 뒤 smoke를 실행하고 다시 올리는 편이 안전합니다.

## 장애 시 우선 확인 순서

1. timer가 enable 상태인지
2. 해당 service가 최근 실행됐는지
3. `fact_job_run` / `fact_job_step_run`에 `BLOCKED` 또는 `FAILED`가 찍혔는지
4. scheduler state JSON과 active lock이 남아 있는지
5. upstream readiness가 부족한지
6. disk watermark / cleanup / alert 상태가 문제인지

## 주의

- Streamlit UI는 scheduler를 직접 실행하지 않습니다.
- 자동 학습/보정은 후보 생성까지만 수행합니다.
- active policy / active meta-model은 UI 또는 CLI의 수동 freeze로만 바꿉니다.
- DuckDB single-writer 특성상 timer를 더 늘리기 전에 lock discipline을 먼저 유지해야 합니다.
