# 자동 스케줄러 서버 운영 런북

이 문서는 scheduler 운용만 따로 빠르게 볼 때 쓰는 축약본입니다.
전체 서버 운영 절차는 [RUNBOOK_SERVER_OPERATIONS.md](d:/MyApps/StockMaster/docs/RUNBOOK_SERVER_OPERATIONS.md)를 우선합니다.

## 현재 설치 자산

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
- `scripts/server/run_scheduler_job_host.sh`
- `scripts/server/run_scheduler_job.sh`

## 현재 실행 경로

1. `systemd timer`
2. `stockmaster-scheduler@.service`
3. `run_scheduler_job_host.sh`
4. `/opt/stockmaster/worker-venv/bin/python`
5. `scripts/run_scheduled_bundle.py`

`run_scheduler_job.sh`는 deprecated wrapper입니다.

## 상태 확인

```bash
cd /opt/stockmaster/app
bash scripts/server/status_scheduler_units.sh
systemctl list-timers 'stockmaster-*.timer' --all
systemctl --failed --no-pager
```

## 설치 / 재설치

```bash
cd /opt/stockmaster/app
bash scripts/server/ensure_scheduler_worker_venv.sh
sudo -E bash scripts/server/install_scheduler_units.sh
```

## 수동 실행

```bash
cd /opt/stockmaster/app
sudo systemctl start stockmaster-scheduler@news-morning.service
sudo systemctl start stockmaster-scheduler@daily-close.service
sudo -E bash scripts/server/run_scheduler_job_host.sh evaluation --dry-run --force
```

## lock 규칙

- serial lock 충돌: `SKIPPED_LOCKED`
- active lock 충돌: scheduler 레이어에서는 `SKIPPED_LOCKED`
- readiness / disk policy / upstream 부족: `BLOCKED`
- 예외: `FAILED`

즉 active writer가 이미 있을 때는 systemd 실패보다 정상 스킵 해석이 우선입니다.

## 로그 확인

```bash
journalctl -u 'stockmaster-*.service' -n 200 --no-pager
journalctl -u stockmaster-scheduler@intraday-assist.service -f
```

```bash
cd /opt/stockmaster/app
bash scripts/server/tail_server_logs.sh
```
