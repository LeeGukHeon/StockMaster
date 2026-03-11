# Scheduler Server Runbook Update

이 문서는 기존 scheduler runbook의 host worker 전환 보조 메모였습니다.
현재는 아래 문서들로 내용이 통합되었습니다.

- [RUNBOOK_SERVER_OPERATIONS.md](d:/MyApps/StockMaster/docs/RUNBOOK_SERVER_OPERATIONS.md)
- [SCHEDULER_SERVER_RUNBOOK.md](d:/MyApps/StockMaster/docs/SCHEDULER_SERVER_RUNBOOK.md)

현재 기준 요약:

- scheduler 경로는 `systemd timer -> stockmaster-scheduler@.service -> run_scheduler_job_host.sh`
- host worker는 `/opt/stockmaster/worker-venv/bin/python`으로 `scripts/run_scheduled_bundle.py`를 실행
- `run_scheduler_job.sh`는 deprecated wrapper
- single-writer 충돌은 정상 스킵(`SKIPPED_LOCKED`) 해석을 우선한다
