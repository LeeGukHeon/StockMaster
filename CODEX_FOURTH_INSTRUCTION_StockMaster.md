# Codex 네 번째 전달용 지시서 — TICKET-003 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion 을 깨지 않는 선에서 **TICKET-003** 를 진행하세요.

반드시 먼저 읽을 문서:
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `TICKET_001_Universe_Calendar_Provider_Activation.md`
- `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
- `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **연구 엔진의 첫 완성형 계층** 을 만드는 것입니다.

반드시 구현할 것:
- `fact_feature_snapshot` 기반 feature store 구축
- wide feature materialization 또는 equivalent access layer 구축
- 가격/추세/변동성/거래대금/재무/뉴스/데이터품질 feature 생성
- `fact_forward_return_label` 또는 동등한 label table 구축
- D+1 / D+5 label 생성 규칙 구현 (기본은 next open → future close)
- same-market equal-weight baseline 기반 excess return label 생성
- `fact_market_regime_snapshot` 구축
- regime state 분류 (`panic`, `risk_off`, `neutral`, `risk_on`, `euphoria`)
- explanatory ranking v0 구현
- `fact_ranking` 적재
- `top_reason_tags_json`, `risk_flags_json`, `explanatory_score_json` 저장
- `scripts/build_feature_store.py`
- `scripts/build_forward_labels.py`
- `scripts/build_market_regime_snapshot.py`
- `scripts/materialize_explanatory_ranking.py`
- `scripts/validate_explanatory_ranking.py`
- Leaderboard / Research / Ops 화면 확장
- run manifest 기록
- README 갱신
- 관련 테스트 작성

중요 제약:
- 이번 티켓에서 ML alpha model 학습/예측까지 확장하지 말 것
- uncertainty / disagreement 를 임의치로 흉내내지 말 것
- flow score 는 아직 준비되지 않았으면 reserved 처리할 것
- label 정의를 모호하게 두지 말고 next open 기준을 명시적으로 구현할 것
- 점수는 explanatory layer 이며 predictive engine 이 아님을 README에 분명히 적을 것
- aggressive over-engineering 금지
- 공식 source 및 existing curated layer 우선

이번 작업 완료의 핵심 기준:
1. `python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100`
2. `python scripts/build_forward_labels.py --start 2026-03-02 --end 2026-03-06 --horizons 1 5 --limit-symbols 100`
3. `python scripts/build_market_regime_snapshot.py --as-of-date 2026-03-06`
4. `python scripts/materialize_explanatory_ranking.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100`
5. `python scripts/validate_explanatory_ranking.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5`
6. `streamlit run app/ui/Home.py`

README에는 최소한 아래를 적어 주세요.
- feature group 목록
- 핵심 feature 계산 개요
- D+1 / D+5 label 정의
- 왜 next open 기준인지
- regime state 규칙
- explanatory score v0 의 active/reserved component
- grade 부여 규칙
- 현재 known limitations

작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- 실행 순서
- feature/label/ranking 확인 방법
- validation 결과 확인 방법
- 아직 남은 TODO
- TICKET-004 진입 전 주의사항

---

