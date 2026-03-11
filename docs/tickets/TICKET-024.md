# TICKET-024

운영성 hardening 및 stale state 자동 복구.

목표:

- stale `RUNNING` row, active lock, serial lock 잔류 문제를 구조적으로 줄인다.
- 실패 run이 후속 작업을 계속 막는 현상을 줄인다.

완료 기준:

- read-only lock conflict 시 snapshot fallback
- owner run 종료/죽은 pid 감지 시 active/serial lock 자동 회수
- stale `RUNNING` row 자동 실패 처리
- ops maintenance에서 orphan run 정리

세부 작업:

- duckdb snapshot fallback
- lock auto-heal
- runtime stale row cleanup
- maintenance cleanup_stale_job_runs
- 회귀 테스트

현재 상태:

- 1차 완료

진행 메모:

- stale `RUNNING` row / active lock / serial lock auto-heal 로직 반영
- 서버 metadata 이력에서 `STALE_RELEASED`, stale cleanup 흔적 확인
- ops maintenance와 host scheduler 경로 기준으로 운영 해석 문서화
