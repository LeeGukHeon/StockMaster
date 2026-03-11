# Scheduler Host Worker Update

이 문서는 2026-03 host worker 전환 당시의 보조 메모였습니다.
현재 운영 기준은 아래 문서로 통합했습니다.

- [RUNBOOK_SERVER_OPERATIONS.md](d:/MyApps/StockMaster/docs/RUNBOOK_SERVER_OPERATIONS.md)
- [SCHEDULER_SERVER_RUNBOOK.md](d:/MyApps/StockMaster/docs/SCHEDULER_SERVER_RUNBOOK.md)
- [METADATA_HOST_WORKER_VALIDATION.md](d:/MyApps/StockMaster/docs/METADATA_HOST_WORKER_VALIDATION.md)

현재 핵심만 요약하면:

- scheduler는 `run_scheduler_job_host.sh`를 사용한다.
- 실행 프로세스는 `/opt/stockmaster/worker-venv` Python이다.
- metadata split 사용 시 worker는 loopback `127.0.0.1:5433` 경로로 Postgres에 접근한다.
