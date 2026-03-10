`D:\MyApps\StockMaster` 루트에서 작업하세요.

루트에 있는 기존 문서들을 모두 먼저 읽고, 특히 아래 문서를 source of truth로 사용하세요.
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_014_OCI_Deployment_External_Access_Runbook.md`
- `TICKET_015_Backtest_WalkForward_Validation_Research_Lab.md`
- `TICKET_016_DB_Audit_Integration_Checklist_Gap_Remediation_000_013.md`
- `TICKET_017_Automation_Scheduler_Timer_Orchestration.md`

이번 작업은 **TICKET-017만** 구현합니다.

목표:
- 서버 운영 기준의 자동 스케줄링을 구현합니다.
- 장후 추천 자동 생성, 장후 평가 자동 실행, 장중 후보군 보조 실행, 주간 학습 후보 생성, 운영 maintenance를 자동화합니다.
- 단, active model/policy 자동 교체는 절대 구현하지 않습니다.

반드시 지켜야 할 원칙:
1. `main.duckdb` single-writer discipline 유지
2. UI(Streamlit) 안에 background scheduler 넣지 말 것
3. host systemd timer + docker compose command 실행 구조를 우선 구현할 것
4. trading calendar self-skip 규칙을 bundle 내부에 넣을 것
5. 모든 write job은 serial runner/lock discipline을 거칠 것
6. 자동 학습은 candidate generation까지만, production auto-promote 금지

구현해야 할 것:
- bundle runner 스크립트 추가
  - `scripts/run_daily_close_bundle.py`
  - `scripts/run_evaluation_bundle.py`
  - `scripts/run_intraday_assist_bundle.py`
  - `scripts/run_weekly_training_bundle.py`
  - `scripts/run_weekly_calibration_bundle.py`
  - `scripts/run_ops_maintenance_bundle.py`
  - `scripts/run_daily_audit_lite_bundle.py`
- 공통 serial job wrapper / lock helper 추가
- scheduler/systemd 관련 install assets 추가
  - 예: `deploy/systemd/*.service`, `deploy/systemd/*.timer`
  - 예: install/uninstall/status helper shell scripts
- Ops/Health/Docs UI에 scheduler 상태/수동 실행 방법/다음 실행 시간/최근 결과 노출
- README 및 docs 갱신
- scheduler smoke/validation 스크립트 추가

실행 방식 기본값:
- systemd timer가 host에서 `docker compose exec -T app python ...` 형태로 호출
- 필요 시 runner profile은 optional 로만 구현

권장 초기 스케줄:
- ops maintenance: daily 02:30
- evaluation: Mon-Fri 16:20
- daily close: Mon-Fri 18:40
- daily audit lite: Mon-Fri 19:05
- intraday assist: Mon-Fri 08:55-15:15 every 5 min
- weekly training candidate: Sat 03:30
- weekly calibration: Sat 06:30

필수 예외 처리:
- non-trading day → skip
- lock occupied → skip/defer
- already completed → idempotent skip
- missing upstream readiness → blocked/degraded

하지 말아야 할 것:
- Celery/Airflow/Kubernetes 도입
- Streamlit 내부 scheduler 도입
- DB 파일 분리
- auto-order / auto-trade 기능 추가
- auto-promote / auto-activate model or policy

완료 기준:
- scheduler bundle이 로컬/서버에서 수동 실행 가능
- systemd unit/timer 템플릿이 설치 가능
- serial runner가 실제 동시 write를 막음
- Ops/Health/Docs에서 scheduler 상태가 보임
- 문서만 보고 타인이 install/enable/manual-rerun 가능
- 관련 테스트/ruff/smoke 통과

작업 후 보고에는 아래를 포함하세요.
1. 추가/수정 파일 목록
2. bundle별 실행 진입점 요약
3. systemd unit/timer 목록과 스케줄 요약
4. lock discipline 설명
5. 테스트/검증 결과
6. known limitation
7. 커밋/푸시 여부
