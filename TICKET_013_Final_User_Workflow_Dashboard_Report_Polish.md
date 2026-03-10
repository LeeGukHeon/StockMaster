# TICKET-013 — 최종 사용자 워크플로우 정리 / 보고서·대시보드 마감 / Release Candidate Polish

- 문서 목적: TICKET-012 이후, Codex가 바로 이어서 구현할 **최종 사용자 관점의 daily workflow 정리, Streamlit 대시보드 마감, 보고서 UX 정리, Release Candidate 수준 polish** 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
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
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001 universe/calendar/provider activation 완료
  - TICKET-002 core research data ingestion 완료
  - TICKET-003 feature store / labels / explanatory ranking v0 완료
  - TICKET-004 flow layer / selection engine v1 / Discord 장후 리포트 초안 완료
  - TICKET-005 postmortem / evaluation / calibration diagnostic 완료
  - TICKET-006 ML alpha model v1 / uncertainty-disagreement / selection engine v2 완료
  - TICKET-007 intraday candidate assist engine v1 완료
  - TICKET-008 intraday postmortem / regime-aware comparison / strategy comparison 완료
  - TICKET-009 intraday policy calibration / tuning / ablation / freeze-rollback 프레임 완료
  - TICKET-010 intraday policy meta-model / ML timing classifier v1 완료
  - TICKET-011 integrated portfolio / capital allocation / risk budget 완료
  - TICKET-012 operational stability / batch recovery / disk guard / monitoring / health dashboard 완료
- 우선순위: 최상
- 기대 결과: **사용자가 앱을 열었을 때 “오늘 무엇을 봐야 하고, 무엇이 완료되었고, 어떤 종목/포트폴리오/리포트가 지금 기준값인지”를 혼동 없이 이해할 수 있는 release candidate 수준의 최종 사용자 흐름이 완성된 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **지금까지 만든 research / prediction / intraday / portfolio / ops 계층을 하나의 사용자 경험으로 묶어, 실제로 매일 쓰는 앱처럼 마감하는 작업** 이다.

핵심은 아래 다섯 가지다.

1. “데이터는 쌓였는데 어디서 뭘 봐야 하는지 모르겠다” 상태를 끝낸다.
2. page / card / badge / glossary / report naming 을 통일한다.
3. daily workflow 를 장후 기준, 다음날 장중 기준, 사후평가 기준으로 명확히 정리한다.
4. Streamlit UI 와 exported report / Discord 리포트가 서로 같은 vocabulary 를 쓰게 만든다.
5. Release Candidate 수준에서 **읽기 쉬움 / drill-down / 상태 가시성 / 문서화 / 사용 순서** 를 마감한다.

즉 이번 티켓은 새로운 예측 엔진이 아니라 **“지금 있는 기능이 제대로 전달되도록 만드는 마감 티켓”** 이다.

---

## 2. 이번 티켓의 핵심 원칙

### 2.1 이번 티켓은 UI/UX / information architecture / report polish 티켓이다
이번 티켓은 alpha model, selection logic, portfolio logic 자체를 다시 설계하는 티켓이 아니다.

해야 하는 것:
- 기존 결과를 더 잘 보여주기
- 사용 흐름 정리하기
- 상태/용어 통일하기
- 보고서 품질 올리기
- 페이지 간 drill-down 정리하기
- read-only 중심의 탐색 흐름 강화하기

하면 안 되는 것:
- 모델 성능 튜닝이 이번 티켓의 본업이 되는 것
- selection v2 를 v3 로 갈아엎는 것
- intraday policy 구조를 재설계하는 것
- 포트폴리오 정책을 새로 학습식으로 바꾸는 것
- 무거운 신규 데이터 파이프라인을 이번 티켓에 얹는 것

### 2.2 canonical user flow 를 먼저 고정하고, 각 페이지는 그 흐름을 지원해야 한다
이 앱의 canonical user flow 는 최소 아래 4단계여야 한다.

1. **장후 체크**
   - 오늘 데이터 적재/분석이 완료되었는지 확인
   - Market Pulse 확인
   - Leaderboard / Portfolio Plan 확인
   - Daily Research Report 확인
2. **다음 세션 시작 전/초반**
   - Intraday Console 로 후보군 상태 확인
   - `OPEN_ALL` vs `TIMING_ASSISTED` 실행 제안 확인
3. **사후평가**
   - Evaluation / Postmortem 에서 전일 추천과 실제 결과 비교
   - calibration / band coverage / miss reason 확인
4. **운영상태**
   - Ops / Health 에서 실패, 부분실패, stale, disk 상태 확인

모든 페이지는 이 흐름 중 하나 이상을 직접 지원해야 한다.

### 2.3 “오늘 기준값”이 무엇인지 명확해야 한다
앱 어디를 보든 아래 질문에 즉답할 수 있어야 한다.

- 현재 앱이 기준으로 삼는 `as_of_date` 는 언제인가
- 오늘 장후 bundle 이 성공했는가
- 현재 활성 `selection engine`, `timing policy`, `portfolio policy`, `ops policy` 는 무엇인가
- 현재 Leaderboard / Portfolio / Evaluation 이 어느 run 을 기준으로 하는가
- 현재 보는 숫자가 `raw`, `adjusted`, `published`, `dry-run`, `active-policy` 중 무엇인가

즉 **current truth card / status banner / active policy badges / run lineage** 가 매우 중요하다.

### 2.4 용어는 한 번 정하면 끝까지 같아야 한다
지금까지 여러 티켓에서 생긴 용어를 UI에서 통일해야 한다.

최소 glossary 대상:
- `as_of_date`
- `selection engine v1/v2`
- `explanatory ranking`
- `selection value`
- `uncertainty`
- `disagreement`
- `raw action`
- `adjusted action`
- `intraday meta decision`
- `OPEN_ALL`
- `TIMING_ASSISTED`
- `target book`
- `rebalance plan`
- `position snapshot`
- `NAV snapshot`
- `matured outcome`
- `coverage`
- `bias`
- `monotonicity`
- `dry-run`
- `published`

서로 다른 페이지에서 같은 개념을 다른 이름으로 보여주면 안 된다.

### 2.5 heavy job 는 화면 진입 시 자동 실행되면 안 된다
TICKET-012 의 운영 안정화 원칙을 유지한다.

UI는 조회와 탐색 중심이어야 하고,
- data ingestion
- model training
- backfill
- full evaluation bundle
- retention cleanup
- recovery run

같은 무거운 작업은 UI 진입 시 자동으로 돌지 않아야 한다.

UI에서 허용 가능한 것:
- lightweight refresh
- materialized view read
- latest artifact load
- explicit manual trigger button (있더라도 안전장치/확인 포함)
- dry-run preview

### 2.6 보고서와 대시보드는 같은 이야기여야 한다
리포트, 대시보드, Discord 메시지, exported artifact 는 숫자가 다르면 안 된다.

반드시 아래가 지켜져야 한다.

- same data contract
- same run reference
- same active policy badges
- same rank / grade / band semantics
- same naming
- same caution flags

즉 UI용 숫자와 Discord용 숫자가 따로 계산되지 않도록 해야 한다.

### 2.7 v1 release candidate 는 “보기 예쁨”보다 “빠르게 이해됨”이 더 중요하다
이번 티켓의 품질 기준은 미적인 장식보다 아래에 둔다.

- 상태가 한눈에 들어옴
- 클릭 동선이 짧음
- 같은 정보가 중복되지 않음
- drill-down 이 자연스러움
- 실패/주의 상태가 숨지 않음
- 페이지 로딩이 과도하게 무겁지 않음
- 모바일에서 완벽할 필요는 없지만 데스크탑 기준 가독성이 좋아야 함

### 2.8 export artifact 는 HTML 중심으로 통일하고, PDF는 필수가 아니다
이번 단계의 기본 export 는 HTML/Markdown 중심이 맞다.

필수:
- daily research report html
- portfolio summary html
- postmortem report html
- ops report html

선택:
- markdown export
- JSON sidecar
- printable summary

이번 티켓에서 PDF 렌더러에 시간을 많이 쓰지 않는다.

### 2.9 release note / known limitation / user guide 가 같이 있어야 한다
이번 티켓이 완료되면 실제 사용을 위한 최소 문서가 있어야 한다.

최소 필요 문서:
- `README.md` 갱신
- `docs/USER_GUIDE.md`
- `docs/WORKFLOW_DAILY.md`
- `docs/GLOSSARY.md`
- `docs/KNOWN_LIMITATIONS.md`
- 가능하면 `docs/REPORTS_AND_PAGES.md`

### 2.10 이번 티켓은 “배포” 티켓이 아니다
외부접속/클라우드 운영은 다음 티켓으로 분리한다.

이번 티켓에서 할 일:
- 외부접속 가능성을 고려한 설정 정리
- base URL / host / port / environment badge 준비
- health/status/footer 정보 정리

이번 티켓에서 하지 않을 일:
- OCI instance 생성
- reverse proxy / TLS / domain 연결
- public ingress 개방
- systemd / cloud init 실배포 고정
- production secret 배치

---

## 3. 이번 티켓에서 반드시 끝내야 하는 것

### 3.1 최상위 navigation / information architecture 정리
Streamlit 멀티페이지 구조를 release candidate 수준으로 정리한다.

권장 최상위 페이지:
- `Home` 또는 `Today`
- `Market Pulse`
- `Leaderboard`
- `Portfolio`
- `Intraday Console`
- `Evaluation`
- `Stock Workbench`
- `Research Lab`
- `Ops`
- `Docs` 또는 `Help`

각 페이지 역할:
- **Home/Today**: 오늘 상태 한 화면 요약
- **Market Pulse**: 시장 분위기, breadth, 변동성, 뉴스 클러스터
- **Leaderboard**: 상위 후보, 등급, band, filters
- **Portfolio**: target book, rebalance plan, holdings, NAV
- **Intraday Console**: 후보군 action/timing 상태
- **Evaluation**: D+1/D+5 결과, postmortem, calibration
- **Stock Workbench**: 개별 심볼 deep dive
- **Research Lab**: model/policy/ablation/comparison
- **Ops**: health, runs, alerts, disk, recovery
- **Docs/Help**: glossary, workflow, usage hints

### 3.2 Home/Today 페이지 완성
이 페이지는 앱을 열었을 때 가장 먼저 보는 페이지여야 한다.

반드시 포함:
- current `as_of_date`
- latest successful daily bundle status
- latest evaluation status
- latest intraday status (있을 경우)
- active policy badges
  - selection engine
  - timing policy
  - timing meta model
  - portfolio policy
  - ops policy
- top 5 actionable names
- portfolio summary snapshot
- market regime snapshot
- critical alerts / degraded state banner
- today’s report links
- latest Discord publish state
- quick links to Leaderboard / Portfolio / Evaluation / Ops

### 3.3 Market Pulse 마감
기존 market summary 를 한 페이지에서 이해 가능하게 정리한다.

필수 섹션:
- market regime banner
- breadth / advance-decline / hit rate
- realized volatility / turbulence proxy
- style / market split (KOSPI / KOSDAQ)
- flow summary (가능한 범위)
- major news clusters
- “today’s read” narrative summary
- caution flags

필수 drill-down:
- cluster → related symbols
- symbol → Stock Workbench
- regime badge → regime details panel

### 3.4 Leaderboard 마감
Leaderboard 는 실전적으로 가장 많이 보는 페이지다.

필수 요구:
- active universe count
- current filtering state 표시
- default sort = active selection value
- columns:
  - rank
  - symbol / name
  - market / sector
  - grade
  - selection value
  - expected alpha D+1/D+5
  - uncertainty
  - disagreement
  - implementation penalty
  - flow score
  - explanatory score
  - band (q25/q50/q75 또는 equivalent)
  - status flags
- filters:
  - market
  - sector
  - grade
  - regime family
  - tradability / liquidity
  - portfolio-eligible
  - current holding / not holding
- compare mode:
  - explanatory ranking vs selection v2
  - open_all vs timing_assisted relevance badge
- click-through:
  - row → Stock Workbench
  - symbol action drawer → related reports / latest postmortem

### 3.5 Portfolio 페이지 마감
Portfolio 페이지는 target book 과 실제 추천 실행 관점이 명확해야 한다.

필수 섹션:
- active portfolio policy
- latest target book summary
- cash weight / invested weight / count
- concentration bars
- sector / market exposure
- new entries / holds / trims / exits
- `OPEN_ALL` vs `TIMING_ASSISTED` execution comparison
- latest rebalance plan
- latest position snapshot
- latest NAV snapshot
- performance since recent checkpoints
- portfolio caution flags

필수 표:
- target positions
- holdings continuation
- blocked or capped positions
- residual cash and reasons
- rebalance deltas

### 3.6 Intraday Console 마감
장중 페이지는 과장 없이 “보조 레이어”라는 점이 잘 보여야 한다.

필수 요구:
- active timing policy/meta model badges
- tracked candidate count
- raw action vs adjusted action 분리 표시
- first-enter recommendation
- wait / avoid 이유 코드
- open reference 대비 timing edge
- last update time
- stale data warning
- quick compare to base `OPEN_ALL`

### 3.7 Evaluation 페이지 마감
사후평가는 이 플랫폼의 신뢰도를 만드는 핵심 페이지다.

필수 섹션:
- latest matured windows
- D+1 / D+5 summary
- hit rate
- average realized alpha
- top/bottom decile result
- portfolio-level realized summary
- band coverage / calibration panels
- miss reason decomposition
- model/policy comparison panels
- latest postmortem report links

필수 drill-down:
- date → selection snapshot
- symbol → prediction vs realized path
- band diagnostics → detailed calibration table

### 3.8 Stock Workbench 마감
개별 종목 화면은 데이터가 많더라도 “하나의 종목을 오늘 살펴보는 화면”이어야 한다.

필수 섹션:
- header summary card
- current rank / grade / portfolio eligibility
- expected alpha / uncertainty / disagreement
- price history / feature snapshot
- news cluster summary
- latest reports where this symbol appeared
- recent selection / timing / portfolio decisions
- postmortem history
- risk flags
- why-in / why-not panel
- recommended watch items

### 3.9 Research Lab 마감
연구자는 필요하지만 일반 사용자도 길을 잃으면 안 된다.

필수 요구:
- default hidden / expandable advanced blocks 허용
- active model registry view
- recent experiment / ablation summary
- selection v1/v2 비교
- timing raw/adjusted/meta compare
- portfolio policy compare
- calibration summary
- model artifact references
- noisy technical table 는 접을 수 있게 구성

### 3.10 Ops 페이지와 사용자 페이지 연결
Ops 는 별도 고급 탭이지만 사용자 경험과 연결되어야 한다.

예:
- Home 에 “현재 degraded” 배너가 뜨면 Ops 해당 섹션으로 이동 가능
- Leaderboard 에 data stale warning 이 뜨면 dependency state 링크
- Portfolio 에 missing NAV snapshot 경고가 뜨면 recovery 큐 링크

즉 사용자 페이지와 Ops 페이지가 단절되면 안 된다.

### 3.11 canonical report center / artifact navigation
리포트를 한 곳에서 찾을 수 있어야 한다.

필수:
- latest daily research report
- latest portfolio report
- latest evaluation report
- latest intraday report (있으면)
- latest ops report
- report history list
- report status (`DRY_RUN`, `PUBLISHED`, `FAILED`, `PARTIAL_SUCCESS`)
- artifact path / generation time / run id

### 3.12 공통 badge / status / color semantics 통일
세부 디자인 자유는 주되, 상태 semantics 는 통일한다.

예시:
- green 계열 = healthy / eligible / active / success
- amber 계열 = caution / wait / partial / degraded
- red 계열 = failed / blocked / avoid / hard risk
- blue/neutral = info / baseline / dry-run / reference

중요한 것은 색상 자체보다 semantics consistency 다.

### 3.13 environment footer / run provenance / version footer
모든 주요 페이지 하단 또는 side area 에 최소 아래를 보여야 한다.

- environment (`local`, `staging`, `prod-like`)
- app version / git commit (가능하면)
- latest refresh time
- latest materialized run id
- active policy ids
- data freshness indicator

### 3.14 docs/help 내장 페이지
앱 안에 최소한 아래를 볼 수 있어야 한다.

- “이 앱은 무엇을 보여주는가”
- “매일 어떤 순서로 보면 되는가”
- “selection / timing / portfolio / evaluation 의 차이는 무엇인가”
- “grade / band / uncertainty 는 무엇을 뜻하는가”
- “현재 known limitation 은 무엇인가”

### 3.15 release candidate checklist 화면 또는 문서
최종 마감용 checklist 를 남긴다.

최소 항목:
- daily bundle success
- evaluation bundle success
- ops health pass
- disk under threshold
- active policy resolved
- report generation pass
- critical page render pass
- drill-down integrity pass
- glossary/docs present
- no broken artifact link

---

## 4. 구현 상세 요구

### 4.1 라우팅 / 페이지 구성
가능하면 페이지별 모듈 구조를 명확히 나눈다.

예시:
- `app/pages/00_Home.py`
- `app/pages/10_Market_Pulse.py`
- `app/pages/20_Leaderboard.py`
- `app/pages/30_Portfolio.py`
- `app/pages/40_Intraday_Console.py`
- `app/pages/50_Evaluation.py`
- `app/pages/60_Stock_Workbench.py`
- `app/pages/70_Research_Lab.py`
- `app/pages/80_Ops.py`
- `app/pages/90_Docs.py`

공통 컴포넌트는 별도 분리:
- badges
- summary cards
- report link cards
- status banners
- tables
- charts
- provenance footer

### 4.2 view model / presentation layer 분리
UI 코드가 데이터 계약을 직접 여기저기 때리지 않도록 한다.

권장 분리:
- repository / query layer
- service / assembler layer
- view model layer
- page rendering layer

즉 “DuckDB 쿼리 + dataframe 가공 + streamlit 출력”이 페이지마다 뒤섞이지 않게 한다.

### 4.3 canonical vocabulary module
가능하면 아래를 코드 레벨에서 관리한다.
- display label constants
- status mapping
- grade order
- action order
- tooltip text
- glossary mapping
- warning reason text

### 4.4 artifact link resolver
report/artifact 링크 규칙을 통일하는 resolver 를 둔다.

지원 대상:
- html report
- markdown report
- json summary
- discord publish artifact
- run manifest
- validation result

### 4.5 empty state / stale state / degraded state 처리
진짜 사용성은 정상 상태보다 이상 상태에서 갈린다.

반드시 지원:
- no data yet
- stale data
- partially published
- dry-run only
- degraded_success
- missing dependency
- recovery in progress

각 경우에:
- 무슨 일이 일어났는지
- 사용자가 어디로 가야 하는지
- 다음 액션이 무엇인지
가 보이도록 한다.

### 4.6 테이블 UX
최소한 아래를 고려한다.
- default sort
- fixed key columns
- numeric formatting
- percentage formatting
- run id / policy id compact display
- csv export (가능하면)
- row detail expanders
- hidden technical columns 토글

### 4.7 차트 UX
차트는 많아도 되지만 불필요하게 무거우면 안 된다.

원칙:
- 한 페이지에 차트 수 과도하지 않게
- 같은 차트를 중복해서 여러 번 그리지 않기
- hover / tooltips 는 유용할 때만
- plotly/altair 등 기존 스택에 맞추되 일관성 유지

### 4.8 narrative summary blocks
숫자만 보여주면 사용자 피로도가 높다.

권장:
- auto-generated narrative summary card
- “오늘의 읽을거리”
- “이 포트폴리오가 왜 이렇게 구성되었는가”
- “이번 평가에서 무엇이 맞고 틀렸는가”
- “현재 운영상 주의점은 무엇인가”

### 4.9 보고서 렌더링 정리
최소 아래 스크립트 또는 동등 기능이 있어야 한다.

- `scripts/render_daily_research_report.py`
- `scripts/render_portfolio_report.py`
- `scripts/render_evaluation_report.py`
- `scripts/render_intraday_summary_report.py`
- `scripts/render_release_candidate_checklist.py`
- `scripts/build_report_index.py`

이미 유사 기능이 있으면 중복 생성보다 정리/통합이 우선이다.

### 4.10 설정 파일
가능하면 아래 설정을 둔다.

- `config/app/display.yaml`
- `config/app/navigation.yaml`
- `config/app/glossary.yaml`
- `config/app/reporting.yaml`
- `config/app/environment.yaml`

---

## 5. 저장 계약 / materialization 요구

이번 티켓은 새 모델 테이블보다 **presentation-friendly materialization** 이 중요하다.

최소 필요:
- `fact_latest_app_snapshot`
- `fact_latest_report_index`
- `fact_release_candidate_check`
- `fact_ui_data_freshness_snapshot`

또는 동등한 저장 계약.

### 5.1 `fact_latest_app_snapshot`
목적:
- Home/Today 페이지의 빠른 로딩과 current truth 제공

최소 필드 예시:
- `snapshot_ts`
- `as_of_date`
- `latest_daily_bundle_run_id`
- `latest_evaluation_run_id`
- `latest_intraday_run_id`
- `latest_portfolio_run_id`
- `active_selection_policy_id`
- `active_timing_policy_id`
- `active_meta_model_id`
- `active_portfolio_policy_id`
- `active_ops_policy_id`
- `health_status`
- `market_regime_family`
- `top_actionable_symbol_list_json`
- `latest_report_bundle_id`

### 5.2 `fact_latest_report_index`
목적:
- report center 및 artifact navigation 제공

최소 필드 예시:
- `report_type`
- `as_of_date`
- `generated_ts`
- `status`
- `run_id`
- `artifact_path`
- `artifact_format`
- `published_flag`
- `dry_run_flag`
- `summary_json`

### 5.3 `fact_release_candidate_check`
목적:
- 주요 페이지/리포트/상태를 한 번에 검증

필드 예시:
- `check_ts`
- `environment`
- `check_name`
- `status`
- `severity`
- `detail_json`
- `recommended_action`

### 5.4 `fact_ui_data_freshness_snapshot`
목적:
- 페이지별 stale 상태 판단

필드 예시:
- `snapshot_ts`
- `page_name`
- `dataset_name`
- `latest_available_ts`
- `freshness_seconds`
- `stale_flag`
- `warning_level`

---

## 6. CLI / 스크립트 요구

최소한 아래 스크립트 또는 동등 기능을 구현한다.

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

---

## 7. 테스트 요구

최소 테스트 범위:
- page data assembler test
- latest snapshot builder test
- report index builder test
- glossary resolver test
- status badge mapping test
- report artifact existence / integrity test
- navigation contract test
- stale/degraded empty-state rendering helper test
- release candidate checklist validator test

---

## 8. 하지 말아야 할 것

- 이번 티켓에서 배포 자동화까지 밀어 넣지 말 것
- 모델 재학습 체계를 다시 뒤집지 말 것
- 데이터 파이프라인 본질을 크게 변경하지 말 것
- UI에서 무거운 작업 자동실행 금지
- page마다 제각각 이름/포맷/색상 쓰지 말 것
- portfolio / evaluation 숫자를 UI에서 다시 독자 계산하지 말 것
- broken link / missing artifact 를 조용히 무시하지 말 것
- status 를 숨기지 말 것
- “예쁘지만 이해 안 되는 UI” 금지
- hardcoded 경로 남발 금지

---

## 9. 완료 기준

아래를 만족하면 이번 티켓은 완료로 본다.

1. 앱 첫 화면이 `Home/Today` 로 정리되어 있고 current truth 를 보여준다.
2. Market Pulse / Leaderboard / Portfolio / Intraday Console / Evaluation / Stock Workbench / Research Lab / Ops / Docs 흐름이 명확하다.
3. 주요 페이지가 동일한 vocabulary 와 badge semantics 를 사용한다.
4. report center 에서 최신 리포트와 이력을 찾을 수 있다.
5. release candidate checklist 가 실행 가능하고 상태를 남긴다.
6. glossary / user guide / workflow / known limitations 문서가 존재한다.
7. stale / degraded / partial states 가 사용자에게 명확히 보인다.
8. heavy job 가 UI 진입 시 자동 실행되지 않는다.
9. README 가 갱신되어 “어떤 순서로 앱을 쓸지” 설명한다.
10. 다음 티켓(OCI 배포/외부접속)으로 넘길 준비가 된 상태다.

---

## 10. Codex가 작업 후 반드시 남겨야 할 보고

- 추가/수정 파일 목록
- 페이지 구조 요약
- 공통 vocabulary / badge / status 체계 요약
- report center / artifact index 구조 요약
- Home/Today current truth 구성 요약
- empty/stale/degraded 상태 처리 방식 요약
- release candidate checklist 결과 요약
- known limitation
- OCI deployment ticket 로 넘길 준비 메모

---

## 11. 다음 티켓 연결 메모

다음 티켓은 **OCI 배포 / 외부접속 / public URL / reverse proxy / 보안 규칙 / 운영 런북** 에 관한 티켓으로 분리한다.

이번 티켓이 끝나면 다음 티켓에서 다룰 것:
- Oracle Cloud Infrastructure compute instance 실배포
- public IP / NSG / ingress port 설계
- Docker Compose 기반 서버 구동
- reverse proxy / optional domain / TLS
- Streamlit bind address / environment config
- boot persistence / restart policy / backup / restore / runbook

이번 티켓은 그 전 단계로서 **앱의 사용자 경험과 아티팩트 구조를 마감하는 역할** 까지만 가진다.