# 장중 리서치 모드

## 개요

TICKET-018부터 StockMaster의 장중 스택은 `research/server` 환경에서 기본 활성화됩니다.
이 모드는 **연구용 / 비매매용**입니다.

- 장중 후보군 보조
- 원시 정책 판단
- 장세 조정 정책 판단
- 메타모델 오버레이
- 사후평가 / 비교 / 요약 리포트

는 자동으로 수집되고 기록되지만, 아래는 계속 금지됩니다.

- 자동 주문
- 자동 매수/매도
- 자동 정책 승격
- 자동 메타모델 승격

## 기본 원칙

- 장중은 항상 전일 selection / portfolio candidate의 하위 계층입니다.
- 전 종목 장중 수집은 하지 않습니다.
- 후보군에 대해서만 1분봉, 체결 요약, 호가 요약, 정책 판단을 저장합니다.
- Streamlit은 기본적으로 read-only입니다.
- write는 scheduler bundle 또는 명시적 수동 승인 버튼에서만 허용합니다.

## 세 가지 의사결정 계층

장중 후보군마다 아래 세 계층이 모두 기록됩니다.

1. 원시 정책 계층
   - `ENTER_NOW`
   - `WAIT_RECHECK`
   - `AVOID_TODAY`
   - `DATA_INSUFFICIENT`

2. 장세 조정 계층
   - 원시 판단을 장중 market context와 regime family로 보정
   - adjustment profile과 reason code를 함께 저장

3. 메타모델 오버레이 계층
   - 정책을 대체하지 않고 bounded overlay만 수행
   - confidence margin, uncertainty, disagreement를 기록
   - `AVOID_TODAY` / `DATA_INSUFFICIENT` 상향 override는 금지

## 연구 모드 기본 활성화 항목

`server` 환경에서는 아래 capability가 기본 `ON`입니다.

- `intraday_assist`
- `intraday_policy_adjustment`
- `intraday_meta_model`
- `intraday_postmortem`
- `intraday_research_reports`
- `intraday_writeback`

선택적 항목:

- `intraday_discord_summary`

`local` 환경에서는 기본 `OFF`이며, 필요한 경우 명시적으로 켤 수 있습니다.

## 스케줄러와 자동 실행

장중 리서치 경로는 TICKET-017 systemd timer와 bundle을 그대로 사용합니다.

- 평일 08:55~15:15, 5분 간격: `run_intraday_assist_bundle.py`
- 장후 평가 bundle에서 intraday postmortem 생성
- 주간 training bundle에서 intraday meta-model artifact 생성
- 주간 calibration bundle에서 intraday policy research report 생성

자동 실행되지만 자동 반영되지 않는 것:

- retrain candidate 결과
- calibration 결과

절대 자동 반영되지 않는 것:

- active intraday policy 교체
- active intraday meta-model 교체

## 수동 승인 반영

Research 화면에서는 아래 흐름으로만 반영할 수 있습니다.

1. before / after 비교 수치 확인
2. 운영자 체크박스 확인
3. 수동 버튼 클릭

이 버튼은 예외적 write 경로이며, 자동 승격이 아닙니다.

## 데이터 lineage

`vw_intraday_decision_lineage`를 통해 장중 의사결정 행은 아래로 추적됩니다.

- originating selection date
- ranking run
- candidate session run
- raw decision run
- adjusted decision run
- meta decision run
- prediction row
- portfolio target row
- market regime snapshot

## UI에서 보는 위치

- `장중 콘솔`
  - 후보군, 원시 판단, 조정 판단, 메타모델, 최종 행동
  - same-exit 비교
  - lineage 테이블

- `사후 평가`
  - 장중 postmortem
  - timing calibration
  - same-exit comparison

- `종목 분석`
  - 종목별 intraday timeline
  - raw / adjusted / meta / final action

- `리서치 랩`
  - intraday capability 상태
  - policy/meta before-after 비교
  - 수동 승인 반영

- `헬스 대시보드`
  - intraday capability registry
  - same-exit summary
  - intraday diagnostics freshness

## 리포트 산출물

장중 research mode는 최소 아래 아티팩트를 생성합니다.

- intraday summary report
- intraday postmortem report
- intraday policy research report
- intraday meta model report

이들은 `fact_latest_report_index`와 release/report center에서 함께 조회됩니다.

## degraded 동작

일부 계층이 비어도 전체는 연구용으로 계속 동작할 수 있습니다.

- raw 판단만 있는 경우: adjusted/meta 없이도 표시
- adjusted만 있는 경우: meta 없이도 표시
- meta 없음: raw + adjusted만으로 same-exit 비교 유지
- 일부 report 없음: capability registry에 degraded 상태로 표시

## known limitation

- 장중은 여전히 candidate-only 저장이므로 시장 전체 intraday breadth 연구에는 제한이 있습니다.
- 메타모델은 bounded overlay이며 독립 stock picking이 아닙니다.
- 뉴스 본문 전문 저장은 하지 않습니다.
- 장중 결과는 연구용이며 실제 주문 시스템과 연결되지 않습니다.
