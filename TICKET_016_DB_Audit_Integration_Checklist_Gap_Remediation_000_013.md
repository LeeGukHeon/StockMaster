# TICKET-016 — DB 개선 / TICKET-000~013 통합 점검 체크리스트 / 케이스별 개선방안

- 문서 목적: TICKET-013 완료 시점의 StockMaster를 기준으로, **TICKET-000~013 전 구간을 관통하는 DB·저장 계약·운영 lineage·UI/report consistency** 를 한 번에 감사하고, 남은 구조적 리스크를 제거하기 위한 **통합 점검·보강 티켓**의 범위와 완료 기준을 명확히 정의한다.
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
  - `TICKET_012_Operational_Stability_Batch_Recovery_Disk_Guard_Monitoring_Health_Dashboard.md`
  - `TICKET_013_Final_User_Workflow_Dashboard_Report_Polish.md`
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
  - `USER_MANUAL_KR_StockMaster.md`
  - `DATA_TRAINING_GUIDE_KR_StockMaster.md`
- 전제 상태:
  - TICKET-013 완료 보고 기준, release/report/snapshot/UI 계층은 동작하며 **스키마 불일치까지 정리된 상태**다.
  - 현재 release candidate check 결과는 **critical 0**, warning은 **주말 기준 freshness 경고**가 남아 있다.
  - `fact_latest_app_snapshot`, `fact_latest_report_index`, `fact_release_candidate_check`, `fact_ui_data_freshness_snapshot` 이 canonical current-state layer 로 도입되었다.
  - Streamlit 응답 200, 주요 리포트 렌더링/validation 스크립트 smoke 는 성공했다.
- 우선순위: 최상
- 기대 결과: **DB 계약, 파이프라인 grain, run lineage, latest snapshot/report index, freshness 판단, active policy/model/book 참조, artifact 참조 무결성, UI/report/source consistency 가 한 번의 감사 티켓으로 정리되고, 이후 OCI 배포(TICKET-014)와 연구 백테스트(TICKET-015)를 믿고 진행할 수 있는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **“TICKET-000~013까지 쌓인 시스템이 기능적으로는 돌아가지만, DB/저장 계약/lineage/최신 상태 표면화/검증 규칙이 장기 운영 기준으로 정말 견고한지”를 전면 감사하고 보강하는 티켓**이다.

핵심은 새 기능 추가가 아니다. 아래 다섯 가지를 반드시 만족시키는 것이다.

1. 모든 핵심 테이블이 **정확한 grain(행의 의미)** 과 **유일키/중복 규칙** 을 가진다.
2. 모든 핵심 산출물이 **어느 run / step / policy / model / as_of_date / market_date** 에서 나왔는지 역추적 가능하다.
3. 최신 snapshot / report index / freshness / release candidate 가 **서로 다른 truth source를 보지 않고**, 같은 기준으로 정렬된다.
4. rerun, late-arriving data, revised data, partial success, degraded success, weekend freshness 같은 **실운영 케이스**에서 데이터가 깨지지 않는다.
5. Codex가 점검 결과를 **문서 + 자동 검증 스크립트 + 필요한 마이그레이션/수정 코드**로 남겨, 이후 티켓이 같은 문제를 재발시키지 못하게 한다.

즉 이번 티켓은 **Audit Layer / Contract Hardening Layer / Data Integrity Layer / Gap Remediation Layer** 를 구현하는 티켓이다.

---

## 2. 이 티켓의 위치

이 티켓은 번호상 TICKET-016이지만 성격상 **TICKET-013 직후의 통합 점검 티켓**으로 취급한다.

의미상 위치는 아래와 같다.

- TICKET-013 까지: 기능/UX/운영/리포트 골격 완성
- **이번 티켓**: 그 위에 쌓인 DB·저장·latest layer·lineage·검증 규칙을 전면 감사하고 보강
- TICKET-014: OCI 배포/외부 접근
- TICKET-015: 연구 백테스트/워크포워드 검증

즉, 이번 티켓은 **배포 전 신뢰도 확보**와 **연구 결과 신뢰도 확보**를 위한 중간 검증문서다.

---

## 3. 이번 티켓의 범위

### 3.1 포함

1. **TICKET-000~013 전 구간 DB/저장 계약 감사**
   - dim/fact/release/ops 테이블 전반
   - raw/curated/marts/artifacts/logs 디렉터리와 DB 참조 관계
   - DuckDB materialized table, Parquet 경로, artifact path 의 정합성

2. **핵심 테이블 grain / key / lineage / freshness 규칙 확정**
   - natural key / surrogate key / run key / snapshot key 구분
   - rerun/upsert/idempotency 규칙 문서화 및 자동 검증
   - latest snapshot/report index/freshness tables contract 확정

3. **TICKET-000~013 checklist 기반 통합 점검표 작성**
   - 티켓별 점검 포인트
   - 실패 시 위험도
   - remediation action
   - owner-level TODO 분해

4. **케이스별 개선방안(runbook style) 작성 및 반영**
   - weekend/holiday stale false warning
   - duplicate run / duplicate row / conflicting latest snapshot
   - report artifact missing / broken path
   - DART 정정/늦게 도착한 데이터
   - intraday session partial write
   - policy/model/portfolio active id mismatch
   - disk cleanup 와 artifact reference 충돌

5. **자동 감사 스크립트 및 contract validation 보강**
   - schema diff
   - uniqueness/grain validation
   - lineage/reference validation
   - release/report/latest/freshness consistency validation
   - ticket coverage checklist validation

6. **문서화**
   - audit 결과 문서
   - DB contract matrix
   - unresolved issue backlog
   - remediation priority list

### 3.2 제외

- 새로운 alpha 모델 추가
- selection logic 수익률 개선 실험
- intraday policy/meta model 재학습 로직 고도화
- portfolio policy 수익률 최적화
- OCI 인프라 배포 작업 자체
- full walk-forward backtest 구현 자체

이번 티켓은 **신기능 티켓이 아니라 감사/보강 티켓**이다.

---

## 4. 이번 티켓의 핵심 원칙

### 4.1 “현재 돌아간다”와 “장기적으로 믿을 수 있다”는 다르다
이번 티켓은 단순 smoke success 에 만족하면 안 된다.

반드시 아래를 확인한다.

- 같은 데이터를 두 번 적재했을 때 결과가 같은가
- 동일 as_of_date 의 rerun 이 latest layer 를 뒤엎지 않는가
- partial/degraded 상태가 snapshot/report/ui 에 숨겨지지 않는가
- active policy/model/book 참조가 latest snapshot 과 실제 source table 에서 일치하는가
- artifact path 가 DB에만 있고 파일은 없는 상태가 존재하지 않는가

### 4.2 모든 핵심 테이블은 grain 과 key 가 문서화되어야 한다
각 테이블마다 최소한 아래 다섯 개를 반드시 갖춘다.

- row grain
- required columns
- uniqueness key
- rerun/upsert rule
- lineage source / downstream consumer

### 4.3 latest layer 는 “집계 결과”가 아니라 “정식 계약”으로 관리해야 한다
`fact_latest_app_snapshot`, `fact_latest_report_index`, `fact_ui_data_freshness_snapshot`, `fact_release_candidate_check` 는 임시 편의 테이블이 아니라 **정식 current-truth surface** 다.

따라서 아래가 필요하다.

- source of truth 정의
- rebuild rule 정의
- stale 허용 조건 정의
- conflicting row 차단
- UI/report script 와 동일 참조 경로 사용

### 4.4 DuckDB single-writer 현실을 전제로 설계해야 한다
이번 티켓은 “언젠가 병렬로 잘 돌 수도 있다”를 가정하지 않는다.

반드시 아래를 따른다.

- single writer 전제를 유지
- write-heavy step 의 순차 실행 기준 확인
- stale lock / duplicate execution / retry replay 에 대한 방어
- long transaction / broken artifact write / half-committed latest snapshot 방지

### 4.5 audit 결과는 반드시 자동 재현 가능해야 한다
문서만 추가하면 안 된다.

반드시 아래를 제공한다.

- 코드로 실행 가능한 audit script
- failure 를 재현하는 최소 테스트 또는 fixture
- remediation 전/후 비교 가능한 validation

---

## 5. 이번 티켓에서 Codex가 반드시 만들어야 하는 산출물

### 5.1 문서

최소 아래 문서를 만든다.

- `docs/AUDIT_T000_T013_STATUS.md`
  - 티켓별 pass/warn/fail 상태
  - 근거 파일/스크립트/테이블
  - 즉시 수정 완료 항목과 보류 항목 구분

- `docs/DB_CONTRACT_MATRIX.md`
  - 핵심 테이블별 grain / key / required columns / lineage / rerun rule / downstream

- `docs/GAP_REMEDIATION_BACKLOG.md`
  - P0/P1/P2 개선 backlog
  - 각 이슈의 위험도, 영향 범위, 수정 방향

- `docs/CASE_RUNBOOK_T000_T013.md`
  - 케이스별 탐지 방법, 원인, 우선 확인 위치, 복구 절차

### 5.2 코드/스크립트

최소 아래 스크립트 또는 동등 기능을 구현한다.

- `scripts/audit_t000_t013_integrity.py`
- `scripts/validate_db_contracts.py`
- `scripts/validate_latest_layer_consistency.py`
- `scripts/validate_artifact_reference_integrity.py`
- `scripts/validate_ticket_coverage_checklist.py`
- `scripts/render_audit_summary_report.py`

필요하면 보조 모듈을 추가한다.

- `app/audit/contracts.py`
- `app/audit/checks.py`
- `app/audit/reporting.py`
- `app/audit/remediation.py`

### 5.3 테스트

최소 아래 테스트 또는 동등 범위를 추가한다.

- ticket coverage checklist smoke
- latest snapshot uniqueness/integrity test
- report index artifact existence test
- rerun duplicate prevention test
- weekend freshness classification test
- active policy/model/book consistency test
- broken lineage detection test

---

## 6. DB 개선 우선순위

이번 티켓은 아래 DB 개선 우선순위를 따라야 한다.

### 6.1 P0 — 당장 깨지면 신뢰도가 무너지는 것

1. **핵심 테이블 grain 불명확 / 중복 허용 문제**
2. **latest snapshot / report index conflicting row 문제**
3. **artifact path 는 있는데 파일이 없는 참조 문제**
4. **active policy/model/portfolio id mismatch 문제**
5. **rerun 후 같은 as_of_date 에 row duplication 또는 stale row 잔존 문제**
6. **weekend/holiday 에 stale warning 이 과잉 발생하는 freshness 로직 문제**

### 6.2 P1 — 지금 당장 안 깨져도 운영 중 계속 비용을 만드는 것

1. source_system / provider_name / schema_version / data_version 부재
2. run lineage 부족으로 원인 추적이 어려운 문제
3. partial/degraded 와 success 가 충분히 구분되지 않는 문제
4. news dedup / symbol mapping drift / revised fundamentals 반영 규칙 미흡
5. intraday session partial write 와 close completeness 검증 부족

### 6.3 P2 — 확장 전에 정리하면 좋은 것

1. 테이블/컬럼 naming inconsistency
2. enum 값 정규화 부족
3. docs/help/glossary 와 DB contract 용어 불일치
4. compact/retention/cleanup 실행 후 참조 무결성 검증 부족

---

## 7. 공통 DB 계약 보강 요구

이번 섹션은 TICKET-000~013 전체에 공통으로 적용하는 DB 개선 요구다.

### 7.1 공통 메타 컬럼 표준화
핵심 fact/release/ops 테이블은 가능한 범위에서 아래 표준 메타 컬럼을 가진다.

- `run_id`
- `root_run_id`
- `parent_run_id`
- `as_of_date`
- `market_date` 또는 `trade_date` 또는 `session_date` (테이블 의미에 맞게)
- `created_ts`
- `updated_ts`
- `source_system`
- `provider_name`
- `schema_version`
- `status`
- `payload_hash` 또는 동등한 change detection 필드

모든 테이블에 다 강제할 필요는 없지만, **핵심 curated/release/ops tables** 에는 일관되게 적용한다.

### 7.2 시간 의미 정규화
아래 시간을 혼용하지 않도록 규칙을 확정한다.

- `as_of_date`: 이 시점에서 알고 있었던 정보 기준의 연구/판단 날짜
- `market_date` / `trade_date`: 실제 거래일
- `session_date`: intraday session 소속 날짜
- `available_ts`: 외부 데이터가 시스템에 실제 도착/확인된 시각
- `generated_ts`: 리포트/산출물이 생성된 시각
- `snapshot_ts`: latest layer 를 materialize 한 시각

### 7.3 grain / uniqueness / rerun rule registry
각 테이블마다 아래를 registry 형태로 남긴다.

- primary business grain
- expected unique key columns
- duplicate detection SQL
- rerun merge policy
- destructive overwrite 허용 여부

### 7.4 lineage / reference integrity
반드시 아래 참조를 검증할 수 있어야 한다.

- `fact_latest_app_snapshot` → latest run ids / active policy ids / latest report bundle
- `fact_latest_report_index` → `artifact_path` 실제 존재 여부
- `fact_prediction` / `fact_ranking` → upstream feature/model/policy lineage
- `fact_portfolio_target_book` / `fact_portfolio_rebalance_plan` / `fact_portfolio_position_snapshot` 간 연결
- `fact_release_candidate_check` / `fact_ui_data_freshness_snapshot` 가 실제 화면이 읽는 동일 소스를 참조하는지

### 7.5 enum / status 표준화
아래 상태 값들은 공통 enum registry 또는 동등한 표준 관리 방식을 가진다.

- run status
- step status
- report status
- stale/warning severity
- intraday action
- portfolio execution mode
- model/policy activation status

### 7.6 latest layer rebuild 가능성
최신 상태를 나타내는 테이블은 **원천 fact/release/ops data 로부터 재구성 가능**해야 한다.

즉,
- latest table 은 authoritative summary 이지만,
- 원천 lineages 가 존재해야 하고,
- rebuild script 로 다시 만들 수 있어야 한다.

### 7.7 reference-safe cleanup
cleanup/retention 이 아래 참조를 끊으면 안 된다.

- active/latest 에서 가리키는 artifact
- 가장 최근 release candidate checklist 의 증빙 report
- 평가/포트폴리오/리포트 center 에 노출되는 canonical bundle

---

## 8. TICKET-000~013 통합 점검 체크리스트

아래 체크리스트는 **Codex가 실제 코드/DB/문서를 열어 보고 PASS/WARN/FAIL 을 채워야 하는 감사 목록**이다.

각 항목마다 아래를 남긴다.

- 상태: PASS / WARN / FAIL
- 근거: 테이블 / 스크립트 / 테스트 / 문서 경로
- 위험도: P0 / P1 / P2
- 조치: fixed now / backlog / accept

### 8.1 TICKET-000 — Foundation

점검 항목:
- 프로젝트 구조와 data 디렉터리 계층이 구현 문서와 일치하는가
- `.env.example`, settings loader, logging, run context, disk guard bootstrap 이 실제 동작하는가
- `ops_run_manifest` 또는 동등 구조가 이후 티켓과 충돌 없이 살아 있는가
- DuckDB bootstrap 이 멱등적(idempotent) 인가
- 최초 실행 시 필요한 테이블/디렉터리가 누락 없이 생성되는가

개선 포인트:
- bootstrapping 과정에서 스키마 버전/초기화 버전을 함께 기록
- DB init 과 directory init 을 분리하되, 하나의 health report 로 묶기

### 8.2 TICKET-001 — Universe / Calendar / Provider Activation

점검 항목:
- `dim_symbol` 의 unique grain 이 안정적인가
- ticker rename / delisting / market reclassification 을 감당할 필드가 있는가
- `dim_trading_calendar` 가 weekend/holiday 를 명시적으로 구분하는가
- provider health / auth failure 가 metadata 로 남는가
- symbol master 와 provider symbol mapping drift 를 탐지할 수 있는가

개선 포인트:
- `dim_symbol` 에 effective date 구간 또는 status history 대응 여지를 남김
- freshness 판단에서 `dim_trading_calendar` 를 적극적으로 사용

### 8.3 TICKET-002 — Daily OHLCV / Fundamentals / News Metadata

점검 항목:
- `fact_daily_ohlcv` unique grain 이 `(symbol, market_date, price_type)` 또는 동등 규칙으로 안정적인가
- rerun 시 중복 row 가 남지 않는가
- `fact_fundamentals_snapshot` 이 report/publication/as_of 의미를 혼동하지 않는가
- 정정공시/revised financials 를 반영할 수 있는 컬럼이 있는가
- `fact_news_item` dedup 규칙이 query duplication 을 막는가
- news 와 symbol mapping 실패가 silent drop 되지 않는가

개선 포인트:
- fundamentals 원천 raw 와 curated snapshot 을 분리해 버전 추적 강화
- news dedup key 를 `provider article id > canonical URL > normalized title+published_ts` 순으로 정규화

### 8.4 TICKET-003 — Feature Store / Labels / Explanatory Ranking

점검 항목:
- `fact_feature_snapshot` 이 strictly as_of discipline 을 지키는가
- `fact_forward_return_label` 이 next open → future close 규칙을 일관되게 지키는가
- `fact_market_regime_snapshot` 이 market_date / as_of_date 를 혼동하지 않는가
- `fact_ranking` 의 explanatory score 구성요소가 lineage 로 남는가
- ranking 재생성 시 이전 결과를 덮어쓰지 않고 run 기준 구분되는가

개선 포인트:
- label 생성 시 trading calendar gap/holiday/휴장일 케이스를 자동 검증
- ranking explanation JSON schema 를 중앙에서 관리

### 8.5 TICKET-004 — Flow / Selection v1 / Discord Report

점검 항목:
- `fact_investor_flow` grain 과 지연 허용 규칙이 명확한가
- `fact_prediction` 이 band/expected value/proxy uncertainty 를 재현 가능하게 저장하는가
- `fact_ranking` 의 selection status 와 publish 대상이 섞이지 않는가
- Discord dry-run 과 publish 가 명확히 구분되는가
- report artifact 와 published state 가 일치하는가

개선 포인트:
- published report bundle id 와 ranking cohort id 를 분리해 기록
- selection cohort snapshot 을 immutable 하게 저장

### 8.6 TICKET-005 — Postmortem / Evaluation / Calibration

점검 항목:
- `fact_selection_outcome` 이 selection 당시 snapshot 과 matured outcome 을 정확히 연결하는가
- `fact_evaluation_summary` 가 same-exit 기준을 유지하는가
- `fact_calibration_diagnostic` 가 band coverage/bias/monotonicity 를 run 기준으로 보존하는가
- 평가 대상 cohort 누락/중복이 없는가
- postmortem report 와 DB summary 가 같은 cohort 를 참조하는가

개선 포인트:
- evaluation join key 를 명시적 cohort key 로 표준화
- not-matured / partially matured / matured 상태 분리를 강화

### 8.7 TICKET-006 — ML Alpha / Uncertainty / Disagreement / Selection v2

점검 항목:
- `fact_model_training_run`, `fact_model_metric_summary`, `fact_model_member_prediction`, `fact_model_feature_importance` lineage 가 완전한가
- 학습 데이터셋 버전과 prediction 결과가 연결되는가
- OOF / validation / live-like prediction 이 명시적으로 구분되는가
- `fact_prediction` 과 `fact_ranking` 에 model version / ensemble version / uncertainty source 가 남는가
- fallback prediction 이 normal prediction 으로 오해되지 않는가

개선 포인트:
- dataset fingerprint / feature set fingerprint 를 명시적으로 저장
- active model registry 와 archived model registry 구분

### 8.8 TICKET-007 — Intraday Candidate Assist Engine

점검 항목:
- candidate-only 저장 원칙이 지켜지는가
- `fact_intraday_candidate_session` 이 session open/close completeness 를 표현하는가
- `fact_intraday_bar_1m`, `fact_intraday_quote_summary`, `fact_intraday_signal_snapshot`, `fact_intraday_entry_decision`, `fact_intraday_final_action` grain 이 명확한가
- disconnect / partial write / end-of-session finalize 누락을 탐지할 수 있는가
- action 이 path dependent 한지, replay 가능성이 있는지 명확한가

개선 포인트:
- session finalization validation 추가
- intraday raw summary 와 final action 을 분리 저장

### 8.9 TICKET-008 — Intraday Postmortem / Regime-Aware Comparison

점검 항목:
- raw vs adjusted action 비교가 same-exit 기준을 지키는가
- `fact_intraday_strategy_comparison` / `fact_intraday_strategy_result` 가 동일 cohort 를 참조하는가
- `fact_intraday_regime_adjustment` 가 실제 적용된 adjustment profile 을 남기는가
- skip saved loss / missed winner / timing edge metrics 가 재현 가능한가

개선 포인트:
- comparison unit key 를 종목/세션/selection cohort 기준으로 고정
- regime adjustment 적용 전후 값을 모두 저장

### 8.10 TICKET-009 — Policy Calibration / Regime Tuning / Experiment Ablation

점검 항목:
- `fact_intraday_policy_experiment_run`, `fact_intraday_policy_candidate`, `fact_intraday_policy_evaluation`, `fact_intraday_policy_ablation_result`, `fact_intraday_active_policy` lineage 가 살아 있는가
- recommendation 과 activation 이 분리되어 있는가
- freeze / rollback / active policy switch 가 audit trail 로 남는가
- matured-only 원칙이 experiment layer 에도 유지되는가

개선 포인트:
- recommendation reason / reject reason 을 구조화 컬럼으로 관리
- policy registry 와 active policy table 의 중복 truth source 제거

### 8.11 TICKET-010 — Policy Meta-Model / ML Timing Classifier v1

점검 항목:
- `fact_intraday_meta_prediction`, `fact_intraday_meta_decision`, `fact_intraday_active_meta_model` lineage 가 완전한가
- KEEP / DOWNGRADE / 제한적 UPGRADE bounded overlay 원칙이 데이터에도 드러나는가
- ENTER/WAIT panel 분리 학습의 데이터 누수 가능성이 없는가
- hard guard override 금지 규칙이 decision layer 에서 깨지지 않는가

개선 포인트:
- meta decision 에 raw policy / adjusted policy / meta override 의 3층 구조를 명시적으로 저장
- meta model fallback reason 저장 강화

### 8.12 TICKET-011 — Integrated Portfolio / Capital Allocation / Risk Budget

점검 항목:
- `fact_portfolio_policy_registry`, `fact_portfolio_candidate`, `fact_portfolio_target_book`, `fact_portfolio_rebalance_plan`, `fact_portfolio_position_snapshot`, `fact_portfolio_nav_snapshot`, `fact_portfolio_constraint_event`, `fact_portfolio_evaluation_summary` 연결이 완전한가
- target book → rebalance plan → position snapshot → nav snapshot 흐름이 끊기지 않는가
- OPEN_ALL / TIMING_ASSISTED execution mode 가 명시적으로 남는가
- constraint hit 와 실제 조정 결과가 모두 저장되는가

개선 포인트:
- position continuity validation 추가
- portfolio와 selection/intraday lineage key 를 하나의 book_run_id 체계로 정리

### 8.13 TICKET-012 — Operational Stability / Recovery / Disk Guard / Monitoring

점검 항목:
- `fact_job_run`, `fact_job_step_run`, `fact_pipeline_dependency_state`, `fact_health_snapshot`, `fact_alert_event`, `fact_recovery_action`, `fact_disk_watermark_event`, `fact_retention_cleanup_run` 등 ops lineage 가 일관적인가
- root/parent/recovery lineage 가 실제로 이어지는가
- duplicate execution / stale lock / retry replay 방지가 구현되었는가
- cleanup 이 reference-safe 한가
- health dashboard 와 실제 DB status 가 같은 truth source 를 쓰는가

개선 포인트:
- stale lock auto-heal 전에 human-readable evidence 남기기
- cleanup allowlist / denylist 를 코드와 문서 모두에 명시

### 8.14 TICKET-013 — Final Workflow / Dashboard / Report Polish

점검 항목:
- `fact_latest_app_snapshot` 이 단 한 row 또는 canonical latest row 규칙을 지키는가
- `fact_latest_report_index` 의 artifact_path / report_type / run_id / status 가 canonical report center 로 충분한가
- `fact_release_candidate_check` 와 `fact_ui_data_freshness_snapshot` 이 실제 페이지가 쓰는 동일 data source 를 검증하는가
- page badge / glossary / stale banner 와 underlying DB status 가 모순되지 않는가
- T013에서 수정된 snapshot/report schema mismatch 가 재발하지 않도록 contract test 가 있는가

개선 포인트:
- freshness 계산에 `dim_trading_calendar` 를 적용해 weekend false warning 감축
- latest layer canonicalization test 강화
- docs/help 페이지와 DB contract glossary 연결 강화

---

## 9. 케이스별 개선방안

이번 섹션은 “실제 운영에서 가장 현실적으로 터질 케이스”를 기준으로 한 runbook 성격의 개선안이다.

### CASE-01 — 주말/휴장일에 stale 경고가 과하게 뜬다

증상:
- 토/일/공휴일에도 최신 market materialization 이 이전 거래일이라는 이유만으로 stale 경고가 뜬다.

원인 후보:
- freshness 계산이 wall-clock 기준이고 trading calendar aware 하지 않음
- market_date 와 generated_ts 만 보고 판단

개선:
- `dim_trading_calendar` 기반으로 “다음 정상 거래일 전까지 허용되는 freshness window” 도입
- page별 dataset마다 `expected_update_calendar` 와 `max_business_gap` 정의
- warning level 을 `INFO / EXPECTED_NON_TRADING / LATE / STALE` 로 세분화

완료 기준:
- 정상적인 주말에는 `warning` 대신 `expected_non_trading` 또는 equivalent 표시
- 실제 지연과 휴장일을 UI가 구분해서 보여줌

### CASE-02 — rerun 후 같은 as_of_date 데이터가 중복된다

증상:
- 같은 날짜의 ranking/prediction/report/index row 가 여러 벌 남아 latest surface 와 충돌

원인 후보:
- natural grain 이 아닌 insert-only append
- rerun merge key 불명확
- latest rebuild 시 obsolete rows 배제 실패

개선:
- 테이블별 unique grain registry 작성
- rerun mode 를 `append_new_run`, `replace_same_run`, `rebuild_latest_only` 중 명시
- duplicate detection SQL + auto-fail validation 추가

완료 기준:
- 동일 rerun 에서 canonical latest row 가 1개만 남음
- 중복이 발생하면 validation 이 FAIL 처리

### CASE-03 — partial/degraded success 인데 UI 는 정상처럼 보인다

증상:
- upstream data 가 일부 부족했는데 Home/Leaderboard/report center 는 정상처럼 보임

원인 후보:
- latest snapshot 이 run status 를 축약하면서 detail loss 발생
- report index status 와 source step status 연결 부족

개선:
- latest snapshot 에 source completeness fields 추가
- report index/status calculation 이 upstream run summary 를 반영하도록 보강
- “data complete / partial / degraded” badge 표준화

완료 기준:
- partial/degraded 상태가 report, snapshot, UI badge 에 일관되게 보임

### CASE-04 — DB에는 artifact_path 가 있는데 실제 파일은 없다

증상:
- report center 에 링크는 보이지만 artifact 파일이 삭제되었거나 생성 실패

원인 후보:
- artifact write 실패 후 DB insert 성공
- cleanup 이 active artifact 참조를 무시하고 삭제

개선:
- artifact write → fsync/exists 검증 → DB commit 순서로 정리
- cleanup allowlist 에 latest/published artifacts 보존
- `validate_artifact_reference_integrity.py` 추가

완료 기준:
- report index 에 등록된 canonical artifact 는 모두 실제 파일 존재
- 누락 시 release candidate FAIL

### CASE-05 — DART 정정/후행 도착 데이터 때문에 과거 스냅샷 의미가 흔들린다

증상:
- 당시에는 없던 수정 재무 정보가 과거 as_of_date snapshot 에 섞여 들어감

원인 후보:
- fundamentals snapshot 이 publication/availability 시점을 제대로 반영하지 않음
- revised data 와 original snapshot 이 분리 저장되지 않음

개선:
- `available_ts`, `source_report_id`, `revision_flag` 또는 동등 구조 추가
- as_of discipline validation 강화
- historical snapshot 은 immutable, revised materialization 은 별도 run 으로 남김

완료 기준:
- 과거 snapshot 재현 시 당시 이용 가능 정보만 사용됨

### CASE-06 — news dedup 이 불안정해 같은 뉴스가 여러 번 보인다

증상:
- 다른 검색 쿼리로 잡힌 동일 기사가 여러 row 로 남음

원인 후보:
- normalized title 만으로는 dedup 부족
- canonical URL 처리 미흡

개선:
- provider article id / canonical URL / normalized title+publisher+published_ts 계층형 dedup key 사용
- mapping confidence 저장

완료 기준:
- 동일 기사 중복 노출이 실사용 기준에서 억제됨

### CASE-07 — active policy/model/portfolio id 가 latest snapshot 과 실제 active registry 에서 다르다

증상:
- Home 은 A 정책이 active 라고 보이는데 실제 selection/meta/portfolio 테이블은 다른 id 사용

원인 후보:
- active registry 업데이트 순서 불일치
- latest snapshot rebuild 시 source race condition

개선:
- active id update 를 single commit boundary 또는 순차 write contract 로 고정
- `validate_latest_layer_consistency.py` 에 active id cross-check 추가

완료 기준:
- latest snapshot 과 source registry 가 항상 동일

### CASE-08 — intraday session 이 중간에 끊겨 final action 이 없다

증상:
- 1분봉/quote summary 는 있는데 session close/final action 이 비어 있음

원인 후보:
- disconnect / graceful finalize 미실행
- candidate session close completeness check 부재

개선:
- session end reconciliation step 추가
- `OPEN`, `PARTIAL`, `FINALIZED`, `ABANDONED` 상태 표준화
- close missing case 를 Ops/Intraday Console 에 노출

완료 기준:
- active session 이 영구 중간상태로 남지 않음

### CASE-09 — cleanup 이 reference 를 끊는다

증상:
- disk cleanup 뒤에 report center, docs/help, evaluation drill-down 링크가 깨짐

원인 후보:
- reference-safe cleanup 규칙 부재
- latest/published/active artifact 보호 미흡

개선:
- cleanup 전에 protected artifact manifest 구성
- delete 대상과 referenced artifact 를 diff 검증
- cleanup run 결과를 `fact_retention_cleanup_run` 에 증빙

완료 기준:
- cleanup 이후도 latest/published report center 무결성 유지

### CASE-10 — schema mismatch 가 조용히 재발한다

증상:
- T013에서 한 번 정리된 report/snapshot/UI schema mismatch 가 새 변경에서 다시 발생

원인 후보:
- 중앙 schema registry 부재
- UI expected columns 와 report builder output columns 가 분리 관리

개선:
- release/report/latest layer 에 대한 중앙 contract registry 추가
- contract test 를 CI/smoke 에 넣기
- page contract / report artifact / navigation integrity validation 을 release gate 로 유지

완료 기준:
- mismatch 는 런타임보다 validation 단계에서 먼저 잡힘

---

## 10. 이번 티켓에서 요구하는 구체 구현 방향

### 10.1 중앙 DB contract registry 추가
형태는 자유지만 아래 정보는 반드시 있어야 한다.

- table_name
- layer (`dim` / `fact` / `ops` / `release` / `artifact`)
- grain description
- unique key columns
- required columns
- optional columns
- status enum / critical enum (필요 시)
- rerun policy
- lineage inputs
- lineage outputs
- audit severity

### 10.2 audit engine 은 “문서 파서”가 아니라 “실제 상태 검사기”여야 한다
즉 아래를 읽어야 한다.

- DuckDB schema / row count / duplicate query
- artifact file existence
- selected report/index/snapshot rows
- active registry rows
- freshness snapshot rows
- release candidate rows

### 10.3 결과는 표준 status 로 요약한다
권장 상태:
- `PASS`
- `WARN`
- `FAIL`
- `NOT_APPLICABLE`

각 결과에는 반드시 아래를 남긴다.
- check_id
- check_name
- status
- severity
- evidence
- recommended_action

### 10.4 즉시 수정 가능한 것과 backlog 로 남길 것을 분리한다
이번 티켓은 모든 문제를 다 고치는 것이 아니라,

- 지금 바로 고쳐야 하는 구조적 결함은 수정
- 리스크는 크지 않지만 설계 개선이 필요한 것은 backlog 로 남김

이 두 가지를 분리해야 한다.

---

## 11. 완료 기준 (Definition of Done)

아래를 모두 만족해야 이번 티켓 완료로 본다.

### 11.1 문서
- `docs/AUDIT_T000_T013_STATUS.md` 존재
- `docs/DB_CONTRACT_MATRIX.md` 존재
- `docs/GAP_REMEDIATION_BACKLOG.md` 존재
- `docs/CASE_RUNBOOK_T000_T013.md` 존재

### 11.2 자동 감사
- `scripts/audit_t000_t013_integrity.py` 실행 가능
- `scripts/validate_db_contracts.py` 실행 가능
- `scripts/validate_latest_layer_consistency.py` 실행 가능
- `scripts/validate_artifact_reference_integrity.py` 실행 가능
- `scripts/validate_ticket_coverage_checklist.py` 실행 가능
- `scripts/render_audit_summary_report.py` 실행 가능

### 11.3 필수 검증
최소 아래 검증이 통과하거나, WARN/FAIL 이면 근거와 remediation 이 문서화되어 있어야 한다.

- canonical latest snapshot uniqueness
- canonical report index integrity
- artifact existence for latest/published reports
- weekend/holiday freshness classification
- active policy/model/portfolio snapshot consistency
- duplicate row detection for major curated tables
- lineage/reference integrity for core release tables
- rerun idempotency for at least one representative pipeline

### 11.4 결과물의 품질
- TICKET-000~013 항목별 PASS/WARN/FAIL 이 한 문서에서 보인다.
- 어떤 FAIL 이 남아 있으면, 왜 남았고 언제 고칠지 backlog 에 명시되어 있다.
- 이번 티켓 결과를 기반으로 TICKET-014/015 진행 전 점검이 가능하다.

---

## 12. 하지 말아야 할 것

- 새로운 매수 신호/예측 점수/포트폴리오 정책을 몰래 추가하지 말 것
- 지금 있는 테이블을 큰 이유 없이 새 이름으로 바꾸지 말 것
- audit 를 위해 canonical historical data 를 파괴적으로 덮어쓰지 말 것
- “현재 smoke 가 되니 괜찮다”는 식으로 grain/duplication/lineage 문제를 넘기지 말 것
- 문서만 만들고 자동 검증을 빼지 말 것

---

## 13. Codex에 대한 구현 지침

Codex는 이번 티켓에서 아래 순서로 작업한다.

1. 루트 문서(TICKET-000~013, 구현 스펙, 사용자 가이드, 데이터 학습 가이드) 재독
2. 현재 repo 의 핵심 테이블/스크립트/페이지/리포트 빌더 inventory 작성
3. DB contract matrix 초안 생성
4. audit script / validation script 구현
5. 실제 실행해서 PASS/WARN/FAIL 수집
6. 즉시 수정 가능한 P0/P1 문제 수정
7. audit summary / backlog / case runbook 문서 작성
8. release candidate / ops / docs 페이지에 필요한 최소 링크 추가

---

## 14. 권장 파일 배치

아래는 권장 배치다. 이름은 바뀌어도 되지만 역할은 유지한다.

- `app/audit/contracts.py`
- `app/audit/checks.py`
- `app/audit/reporting.py`
- `app/audit/remediation.py`
- `scripts/audit_t000_t013_integrity.py`
- `scripts/validate_db_contracts.py`
- `scripts/validate_latest_layer_consistency.py`
- `scripts/validate_artifact_reference_integrity.py`
- `scripts/validate_ticket_coverage_checklist.py`
- `scripts/render_audit_summary_report.py`
- `docs/AUDIT_T000_T013_STATUS.md`
- `docs/DB_CONTRACT_MATRIX.md`
- `docs/GAP_REMEDIATION_BACKLOG.md`
- `docs/CASE_RUNBOOK_T000_T013.md`
- `tests/unit/test_db_contract_registry.py`
- `tests/integration/test_audit_t000_t013_framework.py`

---

## 15. 최종 기대 상태

이번 티켓이 끝나면 StockMaster는 아래 상태가 되어야 한다.

- “기능이 많지만 내부 계약이 불안한 시스템”이 아니라,
- **“무엇이 최신이고, 어디서 왔고, 어느 티켓의 어떤 계약을 따르며, 어떤 케이스에서 어떻게 깨질 수 있고, 그때 무엇을 보면 되는지까지 정리된 시스템”** 이 되어야 한다.

이 상태가 되면 그 다음 OCI 배포(TICKET-014)와 연구 백테스트(TICKET-015)를 훨씬 덜 불안하게 진행할 수 있다.
