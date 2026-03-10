# Codex 열다섯 번째 전달용 지시서 — TICKET-014 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 앱 코어/대시보드/ops를 깨지 않는 선에서 **TICKET-014** 를 진행하세요.

반드시 먼저 읽을 문서:
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `TICKET_001_Universe_Calendar_Provider_Activation.md`
- `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
- `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
- `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
- `TICKET_005_Postmortem_Evaluation_Calibration_Report.md`
- `TICKET_006_ML_Alpha_Uncertainty_Disagreement_Selection_v2.md`
- `TICKET_007_Intraday_Candidate_Assist_Engine.md`
- `TICKET_008_Intraday_Postmortem_Regime_Aware_Strategy_Comparison.md`
- `TICKET_009_Policy_Calibration_Regime_Tuning_Experiment_Ablation.md`
- `TICKET_010_Policy_Meta_Model_ML_Timing_Classifier_v1.md`
- `TICKET_011_Integrated_Portfolio_Capital_Allocation_Risk_Budget.md`
- `TICKET_012_Operational_Stability_Batch_Recovery_Disk_Guard_Monitoring_Health_Dashboard.md`
- `TICKET_013_Final_User_Workflow_Dashboard_Report_Polish.md`
- `TICKET_014_OCI_Deployment_External_Access_Runbook.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
- `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
- `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
- `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
- `CODEX_EIGHTH_INSTRUCTION_StockMaster.md`
- `CODEX_NINTH_INSTRUCTION_StockMaster.md`
- `CODEX_TENTH_INSTRUCTION_StockMaster.md`
- `CODEX_ELEVENTH_INSTRUCTION_StockMaster.md`
- `CODEX_TWELFTH_INSTRUCTION_StockMaster.md`
- `CODEX_THIRTEENTH_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTEENTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **OCI 배포 / 외부접속 / Docker Compose 운영 / reverse proxy / runbook** 완성입니다.

반드시 구현할 것:
- server용 compose profile (`docker-compose.server.yml` 또는 동등)
- server용 env example
- local/server environment 분리
- reverse proxy 구성 (`nginx` 또는 `caddy` 권장)
- app bind `0.0.0.0`
- health / readiness / smoke test
- persistent data/artifacts/logs/backups layout
- restart 정책
- server start/stop/restart/logs helper script
- backup script / restore 문서
- OCI 배포 runbook 문서
- external access checklist 문서
- README 갱신

권장 파일:
- `deploy/docker-compose.server.yml`
- `deploy/nginx/default.conf`
- `deploy/env/.env.server.example`
- `deploy/systemd/stockmaster-compose.service`
- `scripts/server/start_server.sh`
- `scripts/server/stop_server.sh`
- `scripts/server/restart_server.sh`
- `scripts/server/tail_server_logs.sh`
- `scripts/server/smoke_test_server.sh`
- `scripts/server/check_public_access.sh`
- `scripts/server/backup_server_data.sh`
- `docs/DEPLOY_OCI.md`
- `docs/RUNBOOK_SERVER_OPERATIONS.md`
- `docs/BACKUP_AND_RESTORE.md`
- `docs/EXTERNAL_ACCESS_CHECKLIST.md`

중요 제약:
- 이번 티켓은 실제 OCI 콘솔 클릭을 대신하지 않음
- secret 실값을 저장소에 넣지 말 것
- 모든 포트를 외부에 다 열지 말 것
- 로컬 개발 설정을 서버 기본값으로 그대로 쓰지 말 것
- reverse proxy 없이 debug 노출을 기본값으로 삼지 말 것
- backup 없이 destructive migration 금지
- model/selection/portfolio core logic 을 이번 티켓에서 건드리지 말 것

세부 요구:
- server compose 는 production-like restart 정책과 healthcheck 를 가져야 함
- app 은 `0.0.0.0` 과 고정 포트로 bind 되어야 함
- reverse proxy 는 외부 포트와 app upstream 을 연결해야 함
- health path / smoke path 확인 가능해야 함
- data/artifacts/logs/backups 경로가 명확해야 함
- `.env.server.example` 에 필요한 환경변수 설명이 있어야 함
- OCI runbook 에 public IP / network rule / SSH / deploy / check 절차가 있어야 함
- 외부접속 체크리스트에 브라우저/포트/방화벽/보안규칙/로그 확인 순서가 있어야 함
- local 과 server 환경 차이를 문서화할 것

완료 후 반드시 남길 것:
- 추가/수정 파일 목록
- local vs server 차이 요약
- server compose 구조 요약
- reverse proxy 요약
- backup/restore 흐름 요약
- OCI 수동 단계 목록
- known limitation
- 실제 배포 시 주의사항

주의:
- 이번 티켓은 “서버에 올릴 수 있게 만드는 것”이 목표입니다.
- Codex가 직접 OCI를 생성하지 못하더라도, 사람이 그대로 따라 하면 배포할 수 있도록 자산과 문서를 완성하세요.