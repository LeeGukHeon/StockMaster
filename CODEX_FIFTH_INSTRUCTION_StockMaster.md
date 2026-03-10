# Codex 다섯 번째 전달용 지시서 — TICKET-004 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / explanatory ranking 을 깨지 않는 선에서 **TICKET-004** 를 진행하세요.

반드시 먼저 읽을 문서:
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `TICKET_001_Universe_Calendar_Provider_Activation.md`
- `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
- `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
- `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **수급(flow) 활성화 + selection engine v1 + Discord 장후 리포트 초안** 을 만드는 것입니다.

반드시 구현할 것:
- `fact_investor_flow` 또는 동등한 curated 저장 계약 구축
- investor flow raw payload 저장 및 run manifest 기록
- `scripts/sync_investor_flow.py`
- `scripts/backfill_investor_flow.py`
- feature store 에 flow feature 추가
- TICKET-003의 reserved `flow_score` 를 active component 로 승격
- `uncertainty_proxy_score` 및 `implementation_penalty_score` 도입
- selection engine v1 구축
- `scripts/materialize_selection_engine_v1.py`
- selection 결과를 `fact_ranking` 에 저장하거나 동등한 저장 계약 구현
- calibrated proxy prediction band 생성
- `fact_prediction` 또는 동등한 저장 계약에 결과 저장
- `scripts/calibrate_proxy_prediction_bands.py`
- Discord payload renderer 구현
- `scripts/render_discord_eod_report.py`
- Discord webhook publisher 구현 (`dry-run` 필수)
- `scripts/publish_discord_eod_report.py`
- selection validation 스크립트 구현
- `scripts/validate_selection_engine_v1.py`
- Leaderboard / Market Pulse / Stock Workbench / Ops 화면 확장
- README 갱신
- 관련 테스트 작성

중요 제약:
- 이번 티켓에서 ML alpha model 을 학습하거나 예측하는 척하지 말 것
- `disagreement_score` 는 없으면 null 로 둘 것
- `expected_excess_return`, `lower_band`, `median_band`, `upper_band` 가 필요하면 calibrated proxy 임을 README에 분명히 적을 것
- flow source coverage 가 부족하면 0으로 땜질하지 말 것
- explanatory ranking v0 를 깨지 말 것
- selection engine v1 과 explanatory ranking v0 의 차이를 문서화할 것
- Discord publish 실패가 전체 배치 실패로 번지지 않게 할 것
- 뉴스 본문 전문 저장/전송 금지
- aggressive over-engineering 금지

이번 작업 완료의 핵심 기준:
1. `python scripts/sync_investor_flow.py --trading-date 2026-03-06 --limit-symbols 100`
2. `python scripts/backfill_investor_flow.py --start 2026-02-17 --end 2026-03-06 --limit-symbols 100`
3. `python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100`
4. `python scripts/materialize_selection_engine_v1.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100`
5. `python scripts/calibrate_proxy_prediction_bands.py --start 2026-01-05 --end 2026-03-06 --horizons 1 5`
6. `python scripts/render_discord_eod_report.py --as-of-date 2026-03-06 --dry-run`
7. `python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06 --dry-run`
8. `python scripts/validate_selection_engine_v1.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5`
9. `streamlit run app/ui/Home.py`

README에는 최소한 아래를 적어 주세요.
- investor flow source coverage 와 null 처리 규칙
- flow feature 목록
- flow score 계산 개요
- uncertainty proxy / implementation penalty 정의
- selection engine v1 계산 개요
- calibrated proxy band 생성 규칙
- explanatory ranking v0 와 selection engine v1 의 차이
- Discord dry-run / publish 사용법
- 현재 known limitations

작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- investor flow 적재/백필 실행 순서
- selection 결과 확인 방법
- proxy band 확인 방법
- Discord payload preview 확인 방법
- 아직 남은 TODO
- TICKET-005 진입 전 주의사항

---
