# StockMaster 데이터 학습 가이드 (한글)

## 1. 문서 목적

이 문서는 StockMaster의 **데이터 학습/검증/재학습 운영 방식**을 정리한 한글 가이드입니다.

대상 독자:
- 직접 모델을 돌리거나 실험을 관리하는 사용자
- Codex나 개발자에게 구현 방향을 전달해야 하는 사람
- 점수, alpha model, uncertainty, disagreement, timing meta model, portfolio policy 를 연구하는 사람

이 문서는 “모델만 어떻게 학습시키는가”가 아니라 아래 전체를 다룹니다.

- 어떤 데이터를 학습에 써야 하는가
- 누수를 어떻게 막는가
- 어떤 라벨을 써야 하는가
- 검증은 어떻게 해야 하는가
- 어떤 지표로 평가해야 하는가
- 언제 재학습하고 언제 freeze 해야 하는가

---

## 2. 가장 중요한 원칙

## 2.1 미래 누수 금지
StockMaster의 데이터 학습은 무엇보다 **future leakage 방지**가 최우선입니다.

금지 예시:
- 미래 재무 수치를 과거 판단에 사용
- 정정공시 이후 값으로 과거 feature 재생성
- 미래 뉴스 요약을 과거 시점에 사용
- test 구간 평균/표준편차로 train 스케일링
- 장 마감 후 계산된 값을 장중 의사결정에 사용

### 실무 규칙
- 모든 학습 샘플은 `as_of_date` 기준으로 생성
- 모든 feature 는 그 날짜까지 관측 가능한 값만 포함
- 모든 label 은 미래 구간에 대해 별도 생성
- train/validation/test 는 시간 순서 보존

---

## 3. 학습 대상 레이어

StockMaster의 학습/정책 대상은 크게 다섯 레이어입니다.

### 3.1 Explanatory ranking
사용자에게 보기 쉽게 설명하는 점수 체계입니다.
학습 자체보다는 rule-based 구성에 가깝고, 주로 **설명 계층** 역할을 합니다.

### 3.2 Alpha model
핵심 기대 초과수익률 예측 모델입니다.

예:
- D+1 excess return
- D+5 excess return

### 3.3 Uncertainty / Disagreement
예측값만이 아니라 **얼마나 불확실한지**, **모델들이 얼마나 의견이 갈리는지**를 추정합니다.

### 3.4 Intraday timing layer
장중 후보군의 진입 타이밍 보조 모델 또는 정책입니다.

### 3.5 Portfolio policy
최종적으로 어떤 종목을 어떤 비중으로 담을지 결정하는 정책입니다.
이 레이어는 순수 ML이 아닐 수도 있습니다.

---

## 4. 데이터 소스

StockMaster는 공식 데이터 우선 원칙을 사용합니다.

주요 소스 예시:
- 시세/분봉/체결/호가 요약
- 재무/공시
- 뉴스 메타데이터
- 시장 요약/수급/섹터/레짐 관련 데이터

### 권장 저장 구분
- `raw/`: provider 응답 원본 또는 거의 원본
- `curated/`: 정리되고 join 가능한 데이터셋
- `feature/`: 학습용 피처 스냅샷
- `label/`: 미래 수익률/성과 라벨
- `artifact/`: 모델, 리포트, manifest, tuning 결과

---

## 5. 샘플 단위 정의

보통 학습 샘플 1개는 아래를 의미합니다.

- 단위: `(as_of_date, symbol)`
- 입력: 해당 날짜까지 사용 가능한 feature
- 타깃: 다음날 또는 5영업일 뒤의 결과

### 권장 라벨
- `label_excess_ret_d1`
- `label_excess_ret_d5`
- `label_up_prob_d1` (선택)
- `label_hit_threshold_d5` (선택)

핵심은 **절대수익률보다 초과수익률(excess return)** 을 우선하는 것입니다.

---

## 6. 피처 카테고리

권장 피처 묶음은 아래와 같습니다.

### 6.1 가격/추세
- 최근 수익률
- 이동평균 거리
- 변동성
- 고점/저점 대비 위치
- 갭 정보

### 6.2 거래/유동성
- 거래대금
- 회전율
- ADV(평균 거래대금)
- 거래량 급증 여부

### 6.3 수급/플로우
- 외국인/기관 순매수 추세
- 프로그램 매매 요약
- 최근 수급 기울기

### 6.4 재무/퀄리티
- 매출/이익 성장률
- 수익성
- 부채 관련 지표
- 현금흐름 안정성

### 6.5 밸류/안전성
- 가치 관련 비율
- 변동성/낙폭 기반 안전성
- 급등/급락 리스크 플래그

### 6.6 뉴스/이벤트
- 최근 뉴스 건수
- 긍/부정 촉매 요약
- 공시 이벤트 플래그
- 테마/키워드 군집

### 6.7 시장/레짐
- market breadth
- realized vol
- 섹터 쏠림도
- 외부 충격 proxy
- risk-off / risk-on 상태

---

## 7. 학습 데이터셋 생성 규칙

### 7.1 `feature snapshot`
하루 단위로, 가능한 한 장후 기준으로 스냅샷을 만든다.

예:
- `fact_feature_snapshot`
- `fact_market_regime_snapshot`
- `fact_ranking`

### 7.2 `label snapshot`
future open/close 규칙에 맞춰 라벨을 생성한다.

예:
- next open → next close
- next open → 5th trading day close

### 7.3 학습용 머지 규칙
학습 시에는 반드시 아래 키로 정렬/머지한다.

- `as_of_date`
- `symbol`
- `horizon`
- 필요 시 `universe_id`

---

## 8. 권장 검증 방식

## 8.1 기본: walk-forward validation
무작위 셔플 K-fold 대신 **시간 순서 기반 walk-forward** 를 사용합니다.

권장 예시:
- train: 과거 24개월
- validation: 최근 3개월
- test: 다음 1개월
- 한 칸씩 앞으로 이동

## 8.2 보조: time-aware split
모델 내부 개발 초기에는 time-aware split 도 사용할 수 있습니다.

### 왜 random split 을 피하나요?
시계열에서 random split 은 미래 정보를 간접적으로 train에 섞어 넣을 수 있어 성능이 과대평가됩니다.

---

## 9. 모델별 권장 학습 구조

## 9.1 Alpha model v1
권장 시작점:
- ElasticNetCV
- HistGradientBoostingRegressor
- RandomForestRegressor 또는 ExtraTreesRegressor
- horizon 분리(D+1, D+5)
- 앙상블 평균 또는 가중 평균

### 운영 팁
- 먼저 sklearn-only 버전으로 시작
- 성능보다 재현성을 우선
- feature importance 는 참고만 하고 과신 금지

## 9.2 Uncertainty
가능한 시작점:
- OOF residual 기반 volatility proxy
- fold별 예측 분산
- prediction interval calibration

## 9.3 Disagreement
가능한 시작점:
- 모델 간 예측값 표준편차
- rank disagreement
- directional disagreement

## 9.4 Timing meta model
정책을 완전히 대체하는 모델이 아니라, 기존 timing policy 위에 얹는 bounded overlay 로 운영합니다.

---

## 10. 하이퍼파라미터 탐색

### 권장 순서
1. 기본 파라미터로 baseline 생성
2. walk-forward 기준 baseline 확정
3. 제한된 탐색 범위로 tuning 수행
4. validation 기준 추천안 생성
5. test 구간에서 최종 확인
6. 성과가 불안정하면 freeze 또는 rollback

### 실무 원칙
- tuning 은 test 구간을 보지 않고 수행
- 지나치게 넓은 탐색 공간 금지
- trial 수보다 실험 규율이 중요
- tuning 결과는 artifact 와 manifest 로 남긴다

---

## 11. 평가 지표

정확도 하나로 보지 않습니다.

### 핵심 지표
- top-k net excess return
- hit rate
- drawdown
- turnover
- calibration coverage
- uncertainty bucket monotonicity
- disagreement slice spread
- regime별 성과

### alpha model 전용
- rank IC
- decile spread
- q25/q50/q75 realized mapping

### timing 전용
- open 대비 edge
- saved loss
- missed winner

### portfolio 전용
- daily NAV
- max drawdown
- exposure path
- cap hit frequency

---

## 12. 재학습 주기 권장안

### 기본 권장
- D+1 alpha model: 주 1회 또는 2주 1회
- D+5 alpha model: 주 1회 또는 월 2회
- timing meta model: 충분한 표본 쌓인 뒤에만 갱신
- policy calibration: 월 1회 또는 레짐 급변 시 추가 점검

### 재학습보다 더 중요한 것
- 최근 postmortem 확인
- calibration 붕괴 여부 확인
- 특정 레짐 편향 확인
- 최근 한 달 성과 드리프트 확인

---

## 13. freeze / rollback 기준

다음 중 하나면 freeze 또는 rollback 검토:

- 최근 1~2개월 top-k net performance 급락
- uncertainty calibration 붕괴
- 특정 레짐에서 지속적 오작동
- turnover 폭증
- portfolio cap hit 과다
- intraday meta overlay가 raw policy보다 일관되게 나쁨

### 좋은 운영 습관
새 모델을 곧바로 기본값으로 승격하지 말고,
항상 **baseline vs candidate 비교표**를 남기세요.

---

## 14. 초보자용 추천 실험 순서

1. explanatory ranking v0 결과 점검
2. selection v1 vs v2 비교
3. alpha model D+1 baseline 생성
4. alpha model D+5 baseline 생성
5. uncertainty / disagreement 추가
6. timing raw vs adjusted 비교
7. portfolio policy 비교
8. walk-forward backtest 수행
9. calibration 진단
10. 실제 운영 반영 여부 결정

---

## 15. 하지 말아야 할 것

- 정확도만 보고 모델 선택하지 말 것
- 랜덤 셔플 검증을 main result 로 쓰지 말 것
- 최신 데이터 전체로 fit 후 과거 성과를 재평가하지 말 것
- feature engineering 단계에서 미래 집계를 섞지 말 것
- tuning 을 너무 자주 해서 test 구간을 사실상 학습하지 말 것
- 결과가 좋아 보인다는 이유만으로 레짐 조건을 사후적으로 덧칠하지 말 것

---

## 16. 실전 학습 작업 예시

### 예시 1. D+1 alpha model 업데이트
1. latest curated data 확인
2. feature snapshot 생성
3. matured label update
4. walk-forward train/validation/test 실행
5. top-k / net-of-cost / calibration 확인
6. baseline 대비 개선 여부 판단
7. artifact 저장
8. 승격 또는 보류 결정

### 예시 2. timing meta model 업데이트
1. matured timing outcomes 확보
2. active policy 기준 샘플 정리
3. bounded override 규칙 유지
4. false upgrade / false downgrade 분석
5. open 대비 edge와 saved loss 확인
6. 개선 시에만 제한적 승격

---

## 17. 문서/아티팩트로 꼭 남겨야 하는 것

모든 학습/튜닝/비교 실험은 아래를 남겨야 합니다.

- run_id
- as_of cutoff
- 사용 데이터 구간
- feature set version
- label definition
- model params
- tuning params
- validation/test metrics
- comparison baseline
- artifact path
- 승격 여부

---

## 18. 추천 운영 기준선

초기 운영에서는 아래가 가장 현실적입니다.

- 모델: sklearn-only
- 검증: walk-forward 중심
- tuning: Optuna 또는 제한된 grid/random search
- 결과 채택: top-k net performance + drawdown + calibration 동시 확인
- 승격: 한 번에 하나씩
- rollback: 쉬워야 함

---

## 19. 마지막 조언

데이터 학습은 “성능을 최고로 만드는 작업”이 아니라,
**미래에도 망가지지 않을 가능성이 높은 구조를 고르는 작업**에 가깝습니다.

그래서 StockMaster에서는 아래 순서가 중요합니다.

1. 누수 방지
2. 검증 규율
3. 비용 반영
4. calibration 확인
5. 장세별 견고성 확인
6. 그 다음에 성능 개선

이 순서를 지켜야 학습이 쌓일수록 시스템이 더 강해집니다.

