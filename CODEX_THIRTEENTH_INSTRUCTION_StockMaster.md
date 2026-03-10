# Codex 열세 번째 전달용 지시서 — TICKET-012 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine / evaluation / ML alpha / intraday assist / intraday postmortem / calibration / meta model / portfolio layer 를 깨지 않는 선에서 **TICKET-012** 를 진행하세요.

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

이번 작업의 목표는 **운영 안정화 / 배치 복구 / 디스크 가드 / 모니터링 / Health Dashboard** 를 구현하는 것입니다.

반드시 구현할 것:
- 공통 job run / step run metadata layer
- `fact_job_run` 또는 동등 저장 계약
- `fact_job_step_run` 또는 동등 저장 계약
- dependency readiness materialization
- `fact_pipeline_dependency_state` 또는 동등 저장 계약
- health snapshot materialization
- `fact_health_snapshot` 또는 동등 저장 계약
- disk watermark event tracking
- `fact_disk_watermark_event` 또는 동등 저장 계약
- retention cleanup run tracking
- `fact_retention_cleanup_run` 또는 동등 저장 계약
- alert event tracking
- `fact_alert_event` 또는 동등 저장 계약
- recovery action tracking
- `fact_recovery_action` 또는 동등 저장 계약
- active ops policy registry
- `fact_active_ops_policy` 또는 동등 저장 계약
- lock manager / duplicate execution guard
- stale lock release path
- recovery / reconcile framework
- `scripts/run_daily_research_pipeline.py`
- `scripts/run_daily_post_close_bundle.py`
- `scripts/run_daily_evaluation_bundle.py`
- `scripts/run_ops_maintenance_bundle.py`
- `scripts/materialize_health_snapshots.py`
- `scripts/check_pipeline_dependencies.py`
- `scripts/render_ops_report.py`
- `scripts/validate_health_framework.py`
- `scripts/validate_ops_framework.py`
- `scripts/enforce_retention_policies.py`
- `scripts/cleanup_disk_watermark.py`
- `scripts/rotate_and_compress_logs.py`
- `scripts/summarize_storage_usage.py`
- `scripts/reconcile_failed_runs.py`
- `scripts/recover_incomplete_runs.py`
- `scripts/force_release_stale_lock.py`
- `scripts/freeze_active_ops_policy.py`
- `scripts/rollback_active_ops_policy.py`
- 가능하면 `scripts/publish_discord_ops_alerts.py`
- `config/ops/default_ops_policy.yaml`
- 가능하면 `config/ops/conservative_ops_policy.yaml`
- 가능하면 `config/ops/local_dev_ops_policy.yaml`
- Ops Dashboard / Health Dashboard 구현 또는 기존 Ops 화면 대폭 확장
- README 갱신
- 관련 테스트 작성

중요 제약:
- 이번 티켓은 새로운 alpha / selection / portfolio 수익 최적화 티켓이 아님
- 기존 ranking / portfolio / evaluation / intraday logic 을 재설계하지 말 것
- 실패를 숨기지 말고 명시적으로 저장할 것
- recovery run 은 overwrite 가 아니라 새 run 으로 남길 것
- destructive cleanup 은 allowlist 기반이어야 함
- curated core data / predictions / evaluations / portfolio snapshots 자동 삭제 금지
- UI 진입만으로 무거운 bundle 자동 실행 금지
- 동일 job 중복 실행 방지 필요
- active ops policy 가 없어도 safe default / dry-run 검증은 가능해야 함
- disk 사용량 80GB 예산을 강하게 반영할 것
- emergency watermark 시 신규 고빈도 적재를 제한할 수 있어야 함
- stdout 로그만으로 상태 관리하지 말 것
- silent success 금지

세부 요구:
- status 체계는 최소 아래를 지원할 것
  - `SUCCESS`
  - `PARTIAL_SUCCESS`
  - `DEGRADED_SUCCESS`
  - `SKIPPED`
  - `BLOCKED`
  - `FAILED`
- trigger type 은 최소 아래를 지원할 것
  - `MANUAL`
  - `SCHEDULED`
  - `RECOVERY`
  - `VALIDATION`
  - `DRY_RUN`
- run lineage 는 최소 아래를 추적할 것
  - `root_run_id`
  - `parent_run_id`
  - `recovery_of_run_id`
- disk watermark 기본 정책은 최소 아래를 지원할 것
  - warn 70%
  - cleanup 80%
  - emergency 90%
- retention 은 dry-run / actual-run 모두 지원할 것
- stale lock timeout 과 force release path 를 둘 것
- failed/stale/blocked run 을 recovery queue 로 볼 수 있게 할 것
- dependency readiness 는 pipeline 별로 materialize 할 것
- latest successful daily report / evaluation / portfolio target book / nav snapshot 등을 health에 반영할 것
- recent runs / step failures / alerts / disk / cleanup history / locks / stale pipelines 가 Ops 화면에서 보여야 함
- run bundles 는 explicit 스크립트로 분리할 것
- log rotation / compression 을 지원할 것
- 가능하면 Discord ops alert 는 dry-run / publish 둘 다 지원할 것

저장 계약 최소 요구:
- job run
- step run
- pipeline dependency state
- health snapshot
- disk watermark event
- retention cleanup run
- alert event
- recovery action
- active ops policy

권장 구현 방향:
- 공통 `JobRunContext`, `StepRunContext`, `LockManager`, `OpsPolicyResolver`, `HealthRepository` 류의 계층으로 분리
- 기존 스크립트에 흩어진 run manifest 로직을 공통 서비스로 승격
- recovery run 은 failure lineage 를 남기고 parent/root 관계를 추적
- cleanup 은 “잘 지우는 것”보다 “잘못 안 지우는 것”을 우선
- health snapshot 은 단일 숫자보다 다층 상태를 저장
- 최근 24h / 7d failed run count, stale pipeline count, active lock count 를 health 요약에 포함
- storage usage 는 top path usage 와 artifact class 기준 cleanup eligibility 를 보여줄 것

UI 최소 요구:
- Overall Health Summary
- Recent Runs
- Step Failure Explorer
- Dependency Readiness
- Disk Usage / Watermark
- Retention & Cleanup History
- Active Locks
- Recovery Queue
- Alerts
- Latest successful output per pipeline

완료 후 반드시 남길 것:
- 추가/수정 파일 목록
- 새 config 파일 목록
- run/step metadata 구조 요약
- dependency / health 계산 요약
- disk watermark / retention 정책 요약
- recovery / lock 흐름 요약
- Dashboard 구성 요약
- known limitation
- 다음 티켓으로 넘길 메모

주의:
- 이번 티켓의 목적은 기능 추가가 아니라 운영 안정화입니다.
- 실패를 잘 기록하고, 재실행을 안전하게 만들고, 디스크를 예측 가능하게 관리하는 것이 핵심입니다.
- cleanup 자동화보다 보호적 기본값이 더 중요합니다.
