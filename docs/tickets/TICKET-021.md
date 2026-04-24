# TICKET-021

Metadata store split 1차 이행.

목표:

- 운영 메타데이터용 Postgres 저장소를 실제 배포에 연결한다.
- DuckDB 분석 저장소는 유지한다.
- 기존 DuckDB 운영 메타데이터를 Postgres로 복제 가능한 상태로 만든다.

완료 기준:

- `metadata_db` 서비스가 서버에서 기동된다.
- metadata schema bootstrap이 가능하다.
- DuckDB 운영 메타데이터를 Postgres로 migrate 할 수 있다.
- 로컬은 Postgres 없이도 계속 동작한다.

세부 작업:

- settings에 metadata backend 설정 추가
- Postgres schema DDL 추가
- metadata bootstrap / migration 스크립트 추가
- server compose / env / start script 보강
- 서버 실 migration 검증

현재 상태:

- 1차 완료

진행 메모:

- metadata schema/bootstrap/migration 구현
- 서버 `metadata_db` 기동 확인
- 서버 `.env.server` 기준 `METADATA_DB_ENABLED=true`, `METADATA_DB_BACKEND=postgres` 운영 반영
- 운영 메타데이터 latest rebuild 및 최근 run 기록 Postgres 적재 확인
- 로컬 `.env`는 계속 `METADATA_DB_ENABLED=false`, `METADATA_DB_BACKEND=duckdb`로 유지
- 운영 기준 문서는 `docs/STOCKMASTER_UNIFIED_MANUAL_KO.md`로 통합
