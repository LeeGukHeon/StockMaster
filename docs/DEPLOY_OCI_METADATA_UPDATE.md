# DEPLOY OCI Metadata Update

이 문서는 metadata split / host worker 전환 당시의 보조 메모였습니다.
현재 운영 기준은 아래 문서로 통합되었습니다.

- [DEPLOY_OCI.md](d:/MyApps/StockMaster/docs/DEPLOY_OCI.md)
- [RUNBOOK_SERVER_OPERATIONS.md](d:/MyApps/StockMaster/docs/RUNBOOK_SERVER_OPERATIONS.md)

현재 핵심만 요약하면:

- 서버는 `DuckDB 분석 저장소 + Postgres 운영 메타데이터 저장소` 구조를 사용합니다.
- `metadata_db`는 Docker 서비스로 올라오고 loopback `127.0.0.1:5433`로 접근합니다.
- `start_server.sh`는 metadata split enabled 시 `metadata_db` readiness 이후 bootstrap을 수행합니다.
