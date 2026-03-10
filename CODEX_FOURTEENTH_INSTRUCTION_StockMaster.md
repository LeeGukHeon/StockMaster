# Codex 열네 번째 전달용 지시서 — TICKET-013 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine / evaluation / ML alpha / intraday assist / postmortem / calibration / meta model / portfolio layer / ops hardening 을 깨지 않는 선에서 **TICKET-013** 을 진행하세요.

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

이번 작업의 목표는 **최종 사용자 워크플로우 정리 / 보고서·대시보드 마감 / release candidate polish** 입니다.

반드시 구현할 것:
- 앱 첫 화면 `Home` 또는 `Today`
- current truth / latest snapshot / active policy badges
- page navigation 재정리
- `Market Pulse`
- `Leaderboard`
- `Portfolio`
- `Intraday Console`
- `Evaluation`
- `Stock Workbench`
- `Research Lab`
- `Ops`
- `Docs` 또는 `Help`
- 공통 vocabulary / glossary resolver
- 공통 status badge / warning banner / provenance footer
- canonical report center / latest report index
- `fact_latest_app_snapshot` 또는 동등 저장 계약
- `fact_latest_report_index` 또는 동등 저장 계약
- `fact_release_candidate_check` 또는 동등 저장 계약
- `fact_ui_data_freshness_snapshot` 또는 동등 저장 계약
- `scripts/build_latest_app_snapshot.py`
- `scripts/build_report_index.py`
- `scripts/build_ui_freshness_snapshot.py`
- `scripts/render_daily_research_report.py`
- `scripts/render_portfolio_report.py`
- `scripts/render_evaluation_report.py`
- `scripts/render_intraday_summary_report.py`
- `scripts/render_release_candidate_checklist.py`
- `scripts/validate_page_contracts.py`
- `scripts/validate_report_artifacts.py`
- `scripts/validate_navigation_integrity.py`
- `scripts/validate_release_candidate.py`
- `docs/USER_GUIDE.md`
- `docs/WORKFLOW_DAILY.md`
- `docs/GLOSSARY.md`
- `docs/KNOWN_LIMITATIONS.md`
- 가능하면 `docs/REPORTS_AND_PAGES.md`
- README 갱신
- 관련 테스트 작성

중요 제약:
- 이번 티켓은 배포 티켓이 아님
- alpha / selection / intraday / portfolio core logic 을 재설계하지 말 것
- UI 진입 시 heavy job 자동실행 금지
- UI용 숫자를 별도로 재계산하지 말고 existing materialized outputs 를 우선 사용
- page 마다 제각각 vocabulary / badge semantics 쓰지 말 것
- broken link / missing artifact / stale state 를 숨기지 말 것
- hardcoded absolute path 남발 금지
- report 와 dashboard 가 서로 다른 truth 를 보여주면 안 됨
- PDF 중심으로 시간을 쓰지 말 것; HTML/Markdown 중심으로 정리할 것
- OCI 배포/외부접속은 다음 티켓으로 넘길 것

세부 요구:
- `Home/Today` 에서 현재 `as_of_date`, latest daily bundle, latest evaluation, latest intraday, active policy badges, top actionable names, critical alerts, report links 를 볼 수 있어야 함
- `Market Pulse` 는 regime, breadth, volatility, news clusters, narrative summary 를 보여야 함
- `Leaderboard` 는 rank / symbol / grade / selection value / expected alpha / uncertainty / disagreement / implementation penalty / flow score / band / flags 와 filters 를 제공해야 함
- `Portfolio` 는 target book / rebalance / holdings / NAV / exposure / cash / execution mode compare 를 보여야 함
- `Intraday Console` 은 raw action vs adjusted action, timing edge, stale warnings 를 보여야 함
- `Evaluation` 은 D+1/D+5 matured summary, band coverage, calibration, miss reason, model/policy compare 를 보여야 함
- `Stock Workbench` 는 why-in / why-not / reports / decisions / postmortem history 를 보여야 함
- `Research Lab` 은 advanced/technical compare 를 접을 수 있는 형태로 제공할 것
- `Ops` 와 사용자 페이지가 링크로 연결되어야 함
- report center 에서 latest/history/status/run/artifact path 를 찾을 수 있어야 함
- glossary / help / daily workflow docs 를 앱 안 혹은 docs 로 접근 가능해야 함
- release candidate checklist 를 실행 가능해야 함

권장 구현 방향:
- repository / assembler / view-model / page-rendering 분리
- 공통 component module 로 badge/banner/footer/card/table resolver 정리
- current truth snapshot 을 materialize 해서 Home 로딩을 가볍게 유지
- table formatting / percent formatting / technical columns toggle 제공
- empty / stale / partial / degraded state 전용 helper 를 둘 것
- environment footer 에 app version / refresh time / run id / active policy ids 를 넣을 것
- report index 를 단일 resolver 로 관리할 것

UI 최소 요구:
- 빠른 첫 화면 이해 가능
- 페이지 간 drill-down 자연스러움
- 동일 개념의 라벨 통일
- stale/degraded 상태 배너 명확
- 보고서 링크/이력 찾기 쉬움
- 숫자보다 narrative summary card 도 일부 제공

완료 후 반드시 남길 것:
- 추가/수정 파일 목록
- 페이지 구조 요약
- current truth snapshot 구조
- report index 구조
- glossary / status badge 체계
- stale/degraded 처리 방식
- release candidate checklist 결과
- known limitation
- 다음 OCI deployment 티켓으로 넘길 메모

주의:
- 이번 티켓은 기능 추가보다 “사용자 경험을 마감하는 티켓”입니다.
- 지금까지 만든 결과를 사용자가 빠르고 정확하게 이해할 수 있게 하는 것이 핵심입니다.
- 다음 티켓에서 OCI 배포와 외부접속을 다룰 예정이므로, 이번 티켓은 그 전 단계의 앱 마감까지 하세요.