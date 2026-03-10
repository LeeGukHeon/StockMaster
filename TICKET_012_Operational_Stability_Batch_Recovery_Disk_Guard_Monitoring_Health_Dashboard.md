# TICKET-012 — 운영 안정화 / 배치 복구 / 디스크 가드 / 모니터링 / Health Dashboard

- 문서 목적: TICKET-011 이후, Codex가 바로 이어서 구현할 **운영 안정화 계층**의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
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
- 전제 상태:
  - TICKET-000 ~ TICKET-011 까지의 foundation / data / ranking / evaluation / ML alpha / intraday assist / intraday postmortem / calibration / meta model / portfolio engine 계층이 존재
  - 시스템은 이미 “연구 가능” 단계에 진입했으나, 아직 **운영 실패 감지 / 재실행 안전성 / 디스크 가드 / 보관 정책 집행 / 헬스 대시보드**가 완전하지 않음
- 우선순위: 최상
- 기대 결과: **StockMaster가 단일 사용자 연구/리포트 플랫폼을 넘어, 배치 실패·데이터 부족·디스크 압박·부분 성공·중복 실행 상황에서도 상태를 정확히 드러내고 복구 가능한 운영형 시스템이 되는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **“기능이 있는 시스템”을 “계속 돌릴 수 있는 시스템”으로 바꾸는 작업** 이다.

이번 티켓에서 Codex가 만들어야 하는 것은 단순 로그 추가가 아니다. 아래 다섯 가지가 동시에 성립해야 한다.

1. 어떤 파이프라인이 언제 어떤 입력으로 실행되었는지 추적 가능해야 한다.
2. 실패가 나면 어디서 왜 실패했는지, 재시도/복구 가능한지 즉시 보이게 해야 한다.
3. 디스크 사용량과 보관 정책이 자동으로 관리되어야 한다.
4. UI에서 **건강 상태 / 실패 상태 / 마지막 정상 실행 시점 / 수집 지연 / 경고 임계치**를 볼 수 있어야 한다.
5. 재실행이 idempotent 하고 안전해야 하며, 실패한 런을 덮어쓰지 말고 **새 복구 런**으로 남겨야 한다.

즉 이번 티켓은 **Ops Layer / Reliability Layer / Health Layer** 를 구현하는 티켓이다.

---

## 2. 이번 티켓의 핵심 원칙

### 2.1 운영 티켓은 새로운 예측 기능을 만드는 티켓이 아니다
이번 티켓은 alpha 모델, selection engine, portfolio logic 을 개선하는 티켓이 아니다.

반드시 아래를 지킨다.

- ranking 로직 재설계 금지
- ML 모델 구조 개편 금지
- intraday timing 정책 변경 금지
- portfolio policy 수익률 개선 실험 금지

이번 티켓의 목적은 기존 결과를 **안정적으로 계산·기록·복구·관찰** 가능하게 만드는 것이다.

### 2.2 실패를 숨기지 말고 명시적으로 저장해야 한다
운영 시스템은 “조용히 실패”하면 안 된다.

반드시 아래를 따른다.

- 실패한 런을 DB/Parquet 메타 테이블에 남긴다.
- step 단위 상태를 남긴다.
- partial success 를 별도 상태로 표현한다.
- degraded mode / skipped due to dependency / blocked due to lock 를 구분한다.
- 단순 stdout 로그에만 의존하지 않는다.

### 2.3 destructive action 은 보수적으로, 핵심 연구 산출물은 보존적으로 다뤄야 한다
디스크 가드와 보관 정책은 중요하지만, 핵심 데이터 훼손은 더 위험하다.

따라서 반드시 아래를 따른다.

- raw/curated/model/evaluation 핵심 산출물은 무조건 자동 삭제하지 않는다.
- 자동 삭제 대상은 cache / temp / stale raw / rendered report cache / old logs / short TTL intraday derived artifacts 를 우선한다.
- 삭제 전 dry-run / summary / watermark reason 을 남긴다.
- destructive cleanup 은 policy 와 allowlist 기반이어야 한다.
- 사용량이 높다고 해서 핵심 fact tables 를 truncate 하면 안 된다.

### 2.4 배치 재실행은 overwrite 가 아니라 new run 이어야 한다
실패한 런을 고치기 위해 같은 기록을 덮어쓰면 나중에 분석이 불가능해진다.

원칙:
- run_id 는 immutable
- recovery_run_id 는 새로운 런으로 발급
- parent_run_id / root_run_id / recovery_of_run_id 추적
- 재실행 이유(reason)를 저장
- 원본 실패 런은 FAILED 상태로 남긴다

### 2.5 UI 는 상태를 보여주는 도구이지, 대형 배치를 몰래 실행하는 버튼이 되어서는 안 된다
Streamlit UI 는 이미 탐색형 UI다.

이번 티켓에서도 아래를 지킨다.

- 페이지 진입 시 무거운 materialization 자동 실행 금지
- “최근 상태 조회” 중심
- explicit button 이 있어도 기본은 dry-run / health check
- destructive cleanup / rollback / recovery 는 보호된 명시적 액션으로 제한

### 2.6 운영 정책과 연구 정책은 분리해야 한다
Selection policy, intraday policy, portfolio policy 와 별도로 **ops policy** 가 있어야 한다.

예:
- disk watermark thresholds
- retention TTL
- retry count
- backoff seconds
- alert severity rules
- stale-run threshold
- health check window
- lock timeout
- cleanup allowlist

### 2.7 80GB 예산을 강하게 반영해야 한다
이 프로젝트는 저장공간이 무한하지 않다.

따라서 운영 계층은 반드시 아래를 감안해야 한다.

- 70% 경고
- 80% cleanup candidate 강화
- 90% emergency guard
- short TTL intraday cache 정리
- old rendered chart/report cache 정리
- 오래된 raw provider cache 정리
- log rotation / compression
- 새 고빈도 적재 전 디스크 여유 체크

### 2.8 dependency-aware orchestration 이 필요하다
배치는 서로 독립적이지 않다.

예:
- universe / calendar가 비어 있으면 price ingestion 이 정상 완료될 수 없다.
- OHLCV가 없으면 feature store가 build 되면 안 된다.
- matured labels 가 없으면 일부 evaluation 은 skip 되어야 한다.
- intraday meta model 이 없더라도 selection v2 / portfolio open-all 은 돌아가야 한다.

즉 step 간 dependency를 명시하고, “왜 스킵되었는지”를 상태로 남겨야 한다.

### 2.9 graceful degradation 을 지원해야 한다
모든 외부 소스가 항상 완벽할 수는 없다.

따라서 이번 티켓에서는 아래를 구분해야 한다.

- SUCCESS
- PARTIAL_SUCCESS
- DEGRADED_SUCCESS
- SKIPPED
- BLOCKED
- FAILED

예:
- 뉴스 API 장애가 있어도 price + feature + ranking 까지 가능하면 `DEGRADED_SUCCESS`
- intraday data 가 없더라도 open-all portfolio 는 `DEGRADED_SUCCESS`
- active policy 미설정으로 특정 레이어가 실행 불가하면 `BLOCKED`

### 2.10 Health는 단일 숫자가 아니라 다층 상태여야 한다
Health Dashboard 는 녹색/빨간색 하나로 끝나면 안 된다.

최소한 아래 레이어가 있어야 한다.

- storage health
- ingestion freshness health
- batch success health
- dependency health
- model availability health
- report readiness health
- alert backlog health

---

## 3. 이번 티켓에서 반드시 끝내야 하는 것

### 3.1 Standardized run orchestration metadata
기존 run manifest 를 확장해 **운영 공통 메타 계약**을 만든다.

최소 포함:
- `run_id`
- `root_run_id`
- `parent_run_id`
- `recovery_of_run_id`
- `job_name`
- `job_group`
- `run_mode`
- `as_of_date`
- `requested_at`
- `started_at`
- `ended_at`
- `status`
- `status_reason`
- `trigger_type` (`MANUAL`, `SCHEDULED`, `RECOVERY`, `VALIDATION`, `DRY_RUN`)
- `requested_by`
- `policy_ids_json`
- `input_snapshot_ref`
- `output_summary_json`
- `error_class`
- `error_message_truncated`
- `host_name`
- `pid`
- `lock_key`
- `code_version`
- `config_hash`

### 3.2 Step-level execution tracking
각 job 내부 step 을 추적할 수 있어야 한다.

예:
- `load_universe`
- `fetch_prices`
- `materialize_features`
- `score_selection`
- `render_report`
- `publish_discord`

최소 포함:
- `run_id`
- `step_id`
- `step_name`
- `step_order`
- `dependency_step_names_json`
- `started_at`
- `ended_at`
- `status`
- `records_in`
- `records_out`
- `artifact_count`
- `warning_count`
- `error_count`
- `retry_attempt`
- `skip_reason`
- `input_freshness_status`
- `notes_json`

### 3.3 Job lock / duplicate execution guard
중복 실행으로 데이터가 꼬이지 않게 해야 한다.

최소 요구:
- job-level lock key
- stale lock timeout
- forced unlock script
- lock acquisition failure 기록
- UI에서 current lock 상태 확인 가능
- same `job_name + as_of_date + run_mode` 중복 실행 방지
- 단, 명시적 `RECOVERY` run 은 허용 가능

### 3.4 Retry / backoff / recovery framework
실패한 배치를 다시 돌릴 수 있게 해야 한다.

최소 요구:
- retryable vs non-retryable error 구분
- step-level retry count
- exponential 또는 fixed backoff 지원
- recovery candidate 탐지
- 실패한 run 재실행 스크립트
- step 재실행 또는 whole-job 재실행 모두 지원 가능하면 좋음
- recovery 실행 이유와 parent linkage 저장

### 3.5 Dependency state materialization
각 파이프라인의 upstream 충족 여부를 materialize 한다.

예:
- 오늘 유니버스 존재 여부
- 최신 OHLCV cutoff 충족 여부
- 오늘 feature snapshot 존재 여부
- active selection model 존재 여부
- active portfolio policy 존재 여부
- mature labels sufficient 여부

출력:
- dependency name
- expected state
- actual state
- severity
- ready flag
- stale duration
- blocking job names

### 3.6 Health snapshot materialization
주기적으로 health snapshot 을 만든다.

최소 health 분야:
- storage usage
- curated data freshness
- raw data freshness
- model artifact freshness
- last successful daily report
- last successful portfolio target book
- last successful evaluation
- alert backlog
- lock backlog
- failed run count recent 24h / 7d

### 3.7 Disk watermark guard
이 티켓의 중요한 deliverable 중 하나다.

반드시 지원:
- disk usage percent 수집
- thresholds: warning / cleanup / emergency
- current top directories usage 요약
- cleanup candidate 추천
- emergency mode 에서 신규 고빈도 적재 제한
- cleanup history 저장
- cleanup dry-run
- cleanup 실제 실행
- cleanup 결과 before/after usage 기록

권장 임계값 기본값:
- warn: 70%
- cleanup: 80%
- emergency: 90%

### 3.8 Retention policy executor
데이터 보관 정책을 코드로 집행할 수 있어야 한다.

최소 TTL 정책 후보:
- logs: 14~30일
- rendered charts / temp reports: 7~30일
- provider raw cache: 7~30일
- intraday candidate-only detailed cache: 7~30일
- stale dry-run artifacts: 7~14일

주의:
- curated OHLCV / fundamentals / features / labels / predictions / evaluation / target book / nav snapshot 자동 삭제 금지
- destructive cleanup 은 allowlist 기반

### 3.9 System health / ops alert renderer
운영 상태를 사람이 한 번에 이해할 수 있어야 한다.

최소 출력:
- critical alerts
- warning alerts
- failed jobs
- stale pipelines
- near-full disk
- lock contention
- missing active policy
- missing model artifact
- report not produced today

가능하면:
- Discord ops alert dry-run / publish
- HTML ops summary report

### 3.10 Health Dashboard / Ops Dashboard
기존 Ops 화면을 운영 대시보드 수준으로 확장한다.

최소 화면:
- Overall Health Summary
- Recent Runs
- Step Failure Table
- Dependency Readiness
- Disk Usage & Watermark
- Retention / Cleanup History
- Active Locks
- Recovery Queue / Failed Runs
- Latest Reports / Artifacts
- Last Successful per pipeline

### 3.11 Daily orchestration entrypoint
수동 스크립트가 많아지면 운영이 어려워진다.

최소 요구:
- `run_daily_research_pipeline.py`
- `run_daily_post_close_bundle.py`
- `run_daily_evaluation_bundle.py`
- `run_ops_maintenance_bundle.py`

각 entrypoint 는 내부적으로 여러 step 을 orchestration 하고, run/step 메타를 남겨야 한다.

### 3.12 Validation & reconciliation scripts
운영 티켓에서는 검증 도구가 매우 중요하다.

최소 요구:
- `validate_health_framework.py`
- `reconcile_failed_runs.py`
- `recover_incomplete_runs.py`
- `enforce_retention_policies.py`
- `cleanup_disk_watermark.py`
- `render_ops_report.py`

---

## 4. 디렉터리 / 설정 / 산출물 요구사항

### 4.1 권장 디렉터리
아래와 유사한 구조를 허용한다.

```text
config/
  ops/
    default_ops_policy.yaml
    conservative_ops_policy.yaml
    local_dev_ops_policy.yaml

artifacts/
  reports/
    ops/
  health/
  locks/
  cleanup/
  validation/

logs/
  app/
  jobs/
  ops/

state/
  locks/
  recovery/
  alerts/
```

프로젝트 기존 구조와 다르더라도 동일 의미를 달성하면 된다.

### 4.2 ops policy config
최소한 아래를 설정에서 제어 가능해야 한다.

- disk thresholds
- retention TTL by artifact class
- retry count
- backoff base seconds
- stale lock timeout
- stale pipeline thresholds
- alert severity mapping
- cleanup allowlist
- emergency ingestion guard rules
- job concurrency rules
- log rotation / compression policy
- report freshness SLAs

### 4.3 health classification config
가능하면 아래를 분리한다.

- freshness expectations by dataset
- required outputs by time window
- critical vs warning dependency
- dashboard ordering
- alert suppression window

---

## 5. 데이터 / 메타 저장 계약

아래 이름은 권장 계약이다. 기존 네이밍과 다르더라도 의미는 동일해야 한다.

### 5.1 `fact_job_run`
역할:
- job 실행 단위 메타 저장

핵심 컬럼 예시:
- `run_id`
- `root_run_id`
- `parent_run_id`
- `recovery_of_run_id`
- `job_name`
- `job_group`
- `run_mode`
- `as_of_date`
- `status`
- `status_reason`
- `trigger_type`
- `requested_by`
- `started_at`
- `ended_at`
- `duration_seconds`
- `warning_count`
- `error_count`
- `retry_count_total`
- `policy_refs_json`
- `config_hash`
- `code_version`
- `host_name`
- `pid`
- `lock_key`
- `output_summary_json`
- `error_class`
- `error_message_truncated`
- `created_at`

### 5.2 `fact_job_step_run`
역할:
- job 내부 step 추적

핵심 컬럼 예시:
- `run_id`
- `step_id`
- `step_name`
- `step_order`
- `status`
- `skip_reason`
- `dependency_status`
- `started_at`
- `ended_at`
- `duration_seconds`
- `records_in`
- `records_out`
- `artifact_count`
- `warning_count`
- `error_count`
- `retry_attempt`
- `is_retried`
- `notes_json`
- `created_at`

### 5.3 `fact_pipeline_dependency_state`
역할:
- 특정 시점 파이프라인별 dependency readiness 저장

핵심 컬럼 예시:
- `snapshot_ts`
- `as_of_date`
- `pipeline_name`
- `dependency_name`
- `required_flag`
- `actual_flag`
- `severity`
- `readiness_status`
- `staleness_minutes`
- `details_json`

### 5.4 `fact_health_snapshot`
역할:
- 종합 health 스냅샷

핵심 컬럼 예시:
- `snapshot_ts`
- `as_of_date`
- `overall_health_status`
- `storage_health_status`
- `ingestion_health_status`
- `model_health_status`
- `report_health_status`
- `ops_health_status`
- `disk_used_gb`
- `disk_total_gb`
- `disk_used_pct`
- `failed_runs_24h`
- `failed_runs_7d`
- `critical_alert_count`
- `warning_alert_count`
- `active_lock_count`
- `stale_pipeline_count`
- `details_json`

### 5.5 `fact_disk_watermark_event`
역할:
- 디스크 임계치 도달 및 정리 이벤트 기록

핵심 컬럼 예시:
- `event_id`
- `snapshot_ts`
- `used_pct_before`
- `used_pct_after`
- `watermark_level`
- `action_type`
- `is_dry_run`
- `freed_bytes`
- `top_paths_json`
- `cleanup_targets_json`
- `status`
- `notes_json`

### 5.6 `fact_retention_cleanup_run`
역할:
- TTL / cleanup 실행 이력 저장

핵심 컬럼 예시:
- `cleanup_run_id`
- `started_at`
- `ended_at`
- `policy_id`
- `artifact_class`
- `target_path`
- `candidate_file_count`
- `deleted_file_count`
- `freed_bytes`
- `is_dry_run`
- `status`
- `notes_json`

### 5.7 `fact_alert_event`
역할:
- health / ops alert 이벤트 저장

핵심 컬럼 예시:
- `alert_id`
- `event_ts`
- `severity`
- `category`
- `alert_code`
- `title`
- `message`
- `entity_type`
- `entity_key`
- `status`
- `dedupe_key`
- `suppressed_flag`
- `resolved_at`
- `details_json`

### 5.8 `fact_recovery_action`
역할:
- recovery / rollback / forced unlock 등 운영 개입 이력 저장

핵심 컬럼 예시:
- `recovery_action_id`
- `event_ts`
- `action_type`
- `operator`
- `target_run_id`
- `new_run_id`
- `reason`
- `status`
- `details_json`

### 5.9 `fact_active_ops_policy`
역할:
- 현재 활성 ops policy 저장

핵심 컬럼 예시:
- `policy_id`
- `policy_name`
- `version`
- `config_hash`
- `activated_at`
- `activated_by`
- `is_active`
- `notes`

---

## 6. Job / pipeline taxonomy 제안

아래 taxonomy 는 예시다. 같은 의미를 달성하면 네이밍은 조금 달라도 된다.

### 6.1 job groups
- `INGESTION`
- `FEATURES`
- `RANKING`
- `REPORTING`
- `EVALUATION`
- `INTRADAY`
- `PORTFOLIO`
- `OPS`
- `VALIDATION`

### 6.2 대표 job names
- `daily_universe_refresh`
- `daily_ohlcv_ingestion`
- `daily_fundamental_snapshot`
- `daily_news_metadata_ingestion`
- `daily_feature_materialization`
- `daily_selection_v2_scoring`
- `daily_discord_report_render_publish`
- `daily_evaluation_postmortem`
- `daily_portfolio_target_book`
- `daily_portfolio_nav`
- `intraday_candidate_assist`
- `intraday_meta_overlay`
- `ops_health_snapshot`
- `ops_retention_cleanup`
- `ops_disk_watermark_cleanup`
- `ops_recovery_reconcile`

---

## 7. Recovery / retry / reconciliation 상세 원칙

### 7.1 retryable vs non-retryable 예시
retryable:
- 일시적 네트워크 장애
- provider timeout
- file lock contention
- transient DuckDB write conflict
- Discord publish timeout

non-retryable:
- 필수 config 누락
- schema mismatch
- required upstream dataset absent
- parsing contract 붕괴
- invalid policy config

### 7.2 recovery eligibility
다음과 같은 경우 recovery 후보로 본다.

- FAILED 상태
- PARTIAL_SUCCESS 이지만 critical step 실패
- RUNNING 이 오래 지속되어 stale 로 판정
- LOCKED 상태가 lock timeout 초과
- report 미생성으로 downstream BLOCKED 발생

### 7.3 reconciliation 목적
reconciliation 은 “왜 오늘 상태가 안 맞는지”를 찾는 도구다.

예:
- feature run success 인데 ranking run 없음
- target book 이 있는데 nav snapshot 없음
- report artifact 는 있는데 corresponding run metadata 없음
- failed run이 있는데 alert 미생성
- active lock 이 있는데 실제 프로세스는 없음

---

## 8. Disk guard / retention 정책 상세 요구사항

### 8.1 artifact class 분류
최소한 아래 정도는 분류해야 한다.

- `LOG_APP`
- `LOG_JOB`
- `TMP_REPORT`
- `TMP_CHART`
- `RAW_PROVIDER_CACHE`
- `RAW_INTRADAY_CACHE`
- `CURATED_INTRADAY_DERIVED`
- `VALIDATION_ARTIFACT`
- `OPS_REPORT`
- `MODEL_ARTIFACT`
- `CORE_CURATED_DATA`

### 8.2 cleanup 우선순위
기본 우선순위 예시:
1. `TMP_CHART`
2. `TMP_REPORT`
3. `VALIDATION_ARTIFACT`
4. `RAW_PROVIDER_CACHE`
5. `RAW_INTRADAY_CACHE`
6. `LOG_JOB`
7. `LOG_APP`
8. `CURATED_INTRADAY_DERIVED`

자동 cleanup 금지:
- `CORE_CURATED_DATA`
- `MODEL_ARTIFACT` (정책상 명시되지 않았다면)
- `fact_*` 메타 저장소
- 핵심 evaluation / portfolio snapshot

### 8.3 emergency mode 동작 예시
disk > emergency threshold 라면:
- 신규 intraday candidate detailed capture 비활성화 또는 축소
- rendered chart generation 최소화
- non-essential validation artifact 생성 중지
- warning/critical alert 생성
- ops dashboard 에 emergency badge 노출
- cleanup bundle 실행 권장 또는 자동 실행(정책 허용 시)

### 8.4 top-path usage visibility
대시보드에서 아래 정도는 보여야 한다.
- path
- size_gb
- file_count
- artifact_class
- ttl_policy
- cleanup_eligible

---

## 9. UI / Dashboard 요구사항

### 9.1 Ops Dashboard 메인
필수 카드:
- overall health
- disk used %
- latest daily report status
- latest evaluation status
- latest portfolio target book status
- failed runs 24h
- critical alerts
- active locks
- stale pipelines

### 9.2 Recent Runs 테이블
필수 컬럼:
- run_id
- job_name
- as_of_date
- status
- started_at
- duration
- trigger_type
- recovery_of_run_id
- warning_count
- error_count

### 9.3 Step Failure Explorer
필수 컬럼:
- run_id
- step_name
- status
- retry_attempt
- skip_reason
- error_count
- notes summary

### 9.4 Dependency Readiness 화면
필수 항목:
- pipeline_name
- dependency_name
- readiness_status
- required_flag
- staleness
- blocking reason

### 9.5 Disk & Retention 화면
필수 항목:
- current usage
- thresholds
- top paths
- recent cleanup history
- dry-run candidate summary
- emergency mode 여부

### 9.6 Recovery Queue 화면
필수 항목:
- failed / stale / blocked runs
- recovery eligibility
- suggested action
- already attempted recovery count
- link to parent/root run chain

### 9.7 Alerts 화면
필수 항목:
- severity
- category
- title
- opened_at
- resolved_at
- status
- dedupe_key
- entity link

---

## 10. 구현해야 할 스크립트 / 모듈

아래 이름은 권장안이다. 의미가 같다면 약간 달라도 된다.

### 10.1 orchestration / run control
- `scripts/run_daily_research_pipeline.py`
- `scripts/run_daily_post_close_bundle.py`
- `scripts/run_daily_evaluation_bundle.py`
- `scripts/run_ops_maintenance_bundle.py`

### 10.2 health / monitoring
- `scripts/materialize_health_snapshots.py`
- `scripts/check_pipeline_dependencies.py`
- `scripts/render_ops_report.py`
- `scripts/publish_discord_ops_alerts.py` (가능하면)
- `scripts/validate_health_framework.py`

### 10.3 retention / disk
- `scripts/enforce_retention_policies.py`
- `scripts/cleanup_disk_watermark.py`
- `scripts/rotate_and_compress_logs.py`
- `scripts/summarize_storage_usage.py`

### 10.4 recovery / control plane
- `scripts/reconcile_failed_runs.py`
- `scripts/recover_incomplete_runs.py`
- `scripts/force_release_stale_lock.py`
- `scripts/freeze_active_ops_policy.py`
- `scripts/rollback_active_ops_policy.py`

### 10.5 framework validation
- `scripts/validate_ops_framework.py`

---

## 11. 로그 / 알림 / 관측성 요구사항

### 11.1 structured logging
가능하면 JSON-friendly structured logging 을 사용한다.

최소 필드:
- ts
- level
- run_id
- step_name
- job_name
- as_of_date
- event_type
- message
- context

### 11.2 log rotation
최소 요구:
- app/job log 분리
- size 또는 time 기반 rotation
- compression
- TTL
- rotation 결과도 ops metric 으로 반영 가능하면 좋음

### 11.3 alert dedupe
같은 문제로 경고가 폭주하지 않도록 한다.

최소:
- `dedupe_key`
- suppression window
- reopen if resolved 후 재발
- critical repeated failure escalation 가능하면 좋음

### 11.4 Discord ops alert
가능하면 아래 두 단계 모두 지원:
- dry-run preview
- publish

메시지 예시 범주:
- disk emergency
- daily report missing
- repeated failed run
- stale lock
- stale pipeline critical
- active policy missing

---

## 12. 테스트 요구사항

최소 테스트 범위:

1. run / step 상태 전이 테스트
2. retry / backoff 판단 테스트
3. stale lock 판정 테스트
4. dependency readiness 계산 테스트
5. disk watermark 분류 테스트
6. retention allowlist / denylist 테스트
7. cleanup dry-run vs actual-run 차이 테스트
8. health snapshot 집계 테스트
9. alert dedupe 테스트
10. recovery linkage(parent/root/recovery_of) 테스트
11. UI helper / repository layer smoke test
12. daily bundle orchestration 의 dry-run smoke test

가능하면 fixture 기반 테스트를 작성한다.

---

## 13. 완료 기준 (Definition of Done)

아래가 모두 만족되어야 완료로 본다.

1. 운영 공통 run / step 메타 계약이 코드와 저장소에 반영되어 있다.
2. 중복 실행 방지 lock 과 stale lock 해제 도구가 있다.
3. failure / partial / blocked / degraded 상태가 구분 저장된다.
4. health snapshot 이 materialize 된다.
5. dependency readiness 가 materialize 된다.
6. disk watermark guard 가 동작한다.
7. retention policy dry-run / actual-run 이 동작한다.
8. top-path usage 요약이 나온다.
9. ops dashboard 에 recent runs / failures / dependencies / disk / alerts / cleanup history / locks 가 보인다.
10. recovery / reconcile 스크립트가 있다.
11. ops policy freeze / rollback 이 가능하다.
12. README 와 운영 실행 예시가 업데이트된다.
13. 기존 ranking / portfolio / evaluation 기능이 깨지지 않는다.
14. UI 진입만으로 무거운 배치가 실행되지 않는다.
15. destructive cleanup 이 핵심 curated data 를 지우지 않는다.

---

## 14. 이번 티켓에서 하지 말아야 할 것

- 자동매매 기능 추가
- 새로운 alpha model 도입
- selection engine 재설계
- portfolio policy 수익률 최적화 실험
- 실시간 전종목 저장 확대
- 전체 데이터 저장 구조 대수술
- 핵심 curated data 자동 삭제
- 운영 실패를 stdout 로그만으로 처리
- “실패했지만 일단 성공 처리” 같은 silent success

---

## 15. Codex 구현 팁 / 권장 방향

### 15.1 우선순위
가장 먼저 다음 순서로 잡는 것이 좋다.

1. run/step metadata repository
2. lock manager
3. dependency checker
4. health snapshot materializer
5. disk/retention engine
6. recovery/reconcile scripts
7. ops dashboard
8. daily bundles wiring
9. alert renderer / publisher

### 15.2 기존 코드와의 연결
기존 각 스크립트에 흩어진 run manifest logic 이 있다면 공통 유틸/서비스로 끌어올린다.

예:
- `JobRunContext`
- `StepRunContext`
- `LockManager`
- `HealthRepository`
- `OpsPolicyResolver`

### 15.3 가장 중요한 설계 포인트
- “실패를 기록하지 않는 편의성”보다 “실패를 잘 드러내는 구조”를 우선한다.
- “cleanup 잘 되는 것”보다 “잘못 지우지 않는 것”을 우선한다.
- “자동 복구 많이 하는 것”보다 “복구 이유와 linkage 가 남는 것”을 우선한다.

---

## 16. 산출물 정리 시 Codex가 마지막에 남겨야 할 내용

작업 종료 후 Codex 는 반드시 아래를 요약해서 남겨야 한다.

- 추가/수정 파일 목록
- 새 config 파일 목록
- run/step metadata 계약 요약
- health snapshot 계산 요약
- disk watermark / retention 정책 요약
- recovery 흐름 요약
- dashboard 구성 요약
- known limitation
- 운영상 주의사항
- 다음 티켓(TICKET-013)으로 넘길 메모

---

## 17. 다음 티켓과의 연결 의도

이번 티켓이 끝나면 시스템은 기능적으로 꽤 완성된다.  
다음 티켓은 자연스럽게 **최종 사용자 워크플로우 / 리포트 경험 / Research-to-Decision UI polishing / release candidate 수준 마감** 으로 이어지게 된다.

즉 TICKET-013 은 “무엇을 더 계산할까”보다,  
**지금까지 만든 것을 한눈에 쓰고 판단할 수 있는 최종 사용 흐름으로 정리하는 티켓** 이 된다.
