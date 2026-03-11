# TICKET-022

운영 메타데이터 dual-write 및 read 전환.

목표:

- 핵심 운영 메타데이터를 DuckDB와 Postgres에 동시에 기록한다.
- UI/운영 화면이 Postgres metadata를 우선 읽고 DuckDB로 fallback 한다.

완료 기준:

- `ops_run_manifest`, `fact_job_run`, `fact_job_step_run`, `fact_active_lock`, `fact_recovery_action`가 dual-write 된다.
- Home/Ops/Health/운영 상태 관련 helper가 metadata 우선 읽기를 사용한다.
- 로컬에서는 metadata disabled 상태로 fallback 동작한다.

세부 작업:

- manifests / ops repository dual-write
- 일부 UI helper metadata 우선 읽기
- metadata latest view 보강
- read path 회귀 테스트

현재 상태:

- 진행 중

진행 메모:

- manifests / job run dual-write 반영
- helper 일부 metadata 우선 읽기 반영

