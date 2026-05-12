# StockMaster 3~5일 보유형 종목 추천 사이클 및 하이브리드 방법론 v3.0

> 작성일: 2026-05-12  
> 목적: StockMaster를 **국내주식 3~5거래일 보유형 종목 추천/분석 엔진**으로 설계한다.  
> 핵심: 장마감 데이터 기준으로 다음 거래일 후보를 만들되, 추천 메시지에는 반드시 **매수 가능 가격 범위**, **최대 허용 진입가**, **추격주의 기준**, **목표권 도달 기준**, **신호 무효 기준**을 함께 표시한다.

---

## 0. 핵심 결론

현재 StockMaster의 기본 흐름은 아래 방향이 맞다.

```text
장 종료
→ 당일 장 데이터 수집
→ 전일/기존 추천 성과 업데이트
→ feature 생성
→ 룰 필터
→ 머신러닝 추론 또는 주기적 학습
→ 하이브리드 점수 계산
→ 다음 거래일 추천 생성
```

다만 현재 사이클이 아래 순서라면 개선이 필요하다.

```text
장마감
→ 뉴스 수집
→ 전날 성과평가
→ 당일 장 데이터 수집
→ 학습
→ 다음날 종목추천
```

권장 순서는 다음이다.

```text
장마감
→ 거래일/장마감 확정 체크
→ 당일 장 데이터 수집
→ 데이터 정합성 검증
→ 시장조치/공시/뉴스 수집
→ feature 생성
→ 기존 추천 성과 업데이트
→ 라벨 업데이트
→ 모델 학습 여부 결정
→ ML 추론
→ 룰 점수 + ML 점수 하이브리드 계산
→ 다음 거래일 추천 생성
→ 추천 메시지 생성
→ 로그/스냅샷 저장
```

가장 중요한 변경점은 두 가지다.

```text
1. 성과평가는 당일 장 데이터 수집 이후에 수행한다.
2. 학습은 아직 결과가 확정되지 않은 추천 데이터를 사용하면 안 된다.
```

---

## 1. 용어 정의

### 1.1 날짜 기준

| 용어 | 의미 |
|---|---|
| T | 신호 생성일. 오늘 장마감일 |
| T+1 | 다음 거래일. 추천 사용일 |
| T+5 | 3~5일 보유형 기준의 최대 평가 종료일 |
| signal_date | 추천 신호가 만들어진 날짜. 보통 T |
| recommend_date | 추천이 실제로 표시되는 날짜. 보통 T+1 |
| signal_close | T일 종가 |
| entry_reference_price | 다음날 매수 가능 여부를 판단할 기준 가격. 기본은 T+1 시가 또는 사용자가 확인하는 현재가 |

### 1.2 점수 기준

StockMaster는 반드시 아래 점수를 분리한다.

```text
signal_score: T일 장마감 기준으로 종목 구조가 얼마나 좋은지 나타내는 점수
entry_policy: T일 장마감 기준으로 계산한 다음날 유효 가격 정책
entry_score: T+1에 실제 가격이 생긴 후, 지금도 들어갈 수 있는지 계산하는 점수
final_score: signal_score, ML 확률, 시장/섹터 상태, 유동성, 리스크를 합친 최종 점수
```

장중 별도 타이머가 없더라도 `entry_policy`는 반드시 생성해야 한다. 즉 추천 메시지에 아래 내용이 포함되어야 한다.

```text
추천 기준가: 85,500원
매수 가능 범위: 85,000~87,500원
최대 허용 진입가: 87,500원
87,500원 초과 시작 시: 추격주의 또는 눌림 대기
90,000원 이상: 목표권 도달, 신규 후보 제외
82,500원 이탈: 신호 무효
```

---

## 2. StockMaster의 최종 운영 사이클

### 2.1 최소 권장 사이클

장중 타이머가 없는 현재 구조에서는 아래처럼 운영한다.

```text
[장마감 이후 T]
1. 장마감 여부 확인
2. 당일 OHLCV/거래대금 수집
3. 시장조치/거래정지/관리종목/투자경고 정보 수집
4. 공시/뉴스 수집
5. 데이터 정합성 검증
6. feature 생성
7. 기존 추천 성과 업데이트
8. 라벨 업데이트
9. 모델 학습 또는 기존 모델 로드
10. ML 추론
11. 룰 기반 점수 계산
12. 하이브리드 점수 계산
13. entry_policy 계산
14. 다음 거래일 추천 메시지 생성
15. 결과 저장
```

### 2.2 이상적 사이클

장중에 별도 판단을 하지 않더라도, 다음날 사용성을 높이려면 최소한 다음 두 단계가 있으면 좋다.

```text
[장마감 이후 T]
→ 추천 생성 및 매수 가능 가격 범위 제공

[다음 거래일 T+1 장 시작 전 또는 장 시작 직후]
→ 현재가/시가 기준으로 추천 상태만 재검증
```

이 두 번째 단계는 새 종목을 다시 추천하는 단계가 아니다. 전날 추천된 종목이 아직 유효한지 표시만 바꾸는 단계다.

```text
VALID
WATCH_CAUTION
CHASE_RISK
TARGET_ZONE_REACHED
INVALIDATED
```

장중 타이머가 아예 없다면, T일 장마감 추천 메시지에 `조건부 유효 기준`을 명확히 적는다.

예시:

```text
내일 시초가가 87,500원 이하일 때만 유효.
시초가가 87,500~90,000원이면 추격주의.
90,000원 이상 시작하면 단기 목표권 도달로 신규 후보 제외.
82,500원 아래로 시작하면 신호 무효.
```

---

## 3. 현재 사이클의 문제점과 개선 방향

### 3.1 현재 순서

```text
장마감
→ 뉴스 수집
→ 전날 성과평가
→ 당일 장 데이터 수집
→ 학습
→ 다음날 종목추천
```

### 3.2 문제점

#### 문제 1. 성과평가가 당일 장 데이터보다 먼저 실행됨

전날 추천의 성과를 보려면 오늘의 고가, 저가, 종가, 거래량이 필요하다. 따라서 성과평가는 반드시 당일 장 데이터 수집 이후에 실행해야 한다.

잘못된 구조:

```text
성과평가
→ 당일 데이터 수집
```

올바른 구조:

```text
당일 데이터 수집
→ 데이터 검증
→ 성과평가
```

#### 문제 2. “전날 성과평가”와 “최종 라벨 확정”을 혼동하기 쉬움

3~5일 보유형에서는 전날 추천이 바로 최종 성공/실패가 되는 게 아니다.

성과평가는 두 종류로 나누어야 한다.

```text
1. 일간 상태 평가
   - 어제 추천이 오늘 유효했는가?
   - 시가가 최대 허용 진입가를 넘었는가?
   - 오늘 목표권에 도달했는가?
   - 오늘 신호 무효 가격을 깼는가?

2. 최종 라벨 평가
   - 추천 후 3~5거래일 동안 목표가와 손절 기준 중 무엇이 먼저 발생했는가?
   - T+5까지 목표가에 도달하지 못했는가?
```

전날 성과평가는 “일간 상태 평가”일 수는 있지만, 머신러닝 학습용 정답으로 바로 쓰면 안 된다.

#### 문제 3. 매일 학습 시 미성숙 라벨이 섞일 위험

예를 들어 T일 밤에 학습할 때, T-1 추천 종목은 아직 5일 결과가 끝나지 않았다. 이 데이터를 성공/실패로 확정하면 라벨 누수가 발생한다.

원칙:

```text
학습에는 결과가 확정된 샘플만 사용한다.
```

예:

```text
T일 학습에 사용 가능한 샘플:
- T-5 이전 추천 중 목표/손절/기간종료 결과가 확정된 샘플

T일 학습에 사용하면 안 되는 샘플:
- T-1, T-2, T-3 추천 중 아직 결과가 끝나지 않은 샘플
```

#### 문제 4. 뉴스는 추천 근거 보조로 쓰고, 핵심 모델 입력은 조심해야 함

뉴스 수집은 단순 정보 알림 또는 리스크 플래그로 쓰는 것이 안전하다. 뉴스 원문을 바로 모델의 핵심 feature로 넣으면 시각 정보, 중복 기사, 과잉반응, 데이터 누수 문제가 생길 수 있다.

권장:

```text
뉴스 수집 → 단순 정보 알림
공시 수집 → 하드 리스크 플래그 또는 감점
뉴스 키워드 → 보조 feature로만 제한적 사용
```

---

## 4. 권장 배치 순서

### 4.1 표준 순서

```text
STEP 00. trading_date 확정
STEP 01. 장마감 데이터 수집
STEP 02. 데이터 정합성 검증
STEP 03. 시장조치/공시/뉴스 수집
STEP 04. feature 생성
STEP 05. 기존 추천 상태 업데이트
STEP 06. ML 라벨 업데이트
STEP 07. 모델 학습 여부 결정
STEP 08. ML 추론
STEP 09. 룰 기반 필터 및 signal_score 계산
STEP 10. 하이브리드 점수 계산
STEP 11. entry_policy 계산
STEP 12. 최종 추천 생성
STEP 13. 리포트/메시지 생성
STEP 14. 스냅샷 및 로그 저장
```

### 4.2 각 단계 설명

#### STEP 00. trading_date 확정

휴장일, 조기폐장, 주말, 공휴일을 먼저 확인한다.

```python
if not is_trading_day(today):
    stop_pipeline("not_trading_day")
```

#### STEP 01. 장마감 데이터 수집

수집 대상:

```text
- 종목별 OHLCV
- 거래대금
- 등락률
- 시가총액
- 지수 OHLCV
- 섹터/업종 지수
- 투자자별 매매동향, 가능하면 선택
- 시장조치 정보
```

핵심 원칙:

```text
추천 생성은 반드시 T일 종가 확정 데이터만 사용한다.
```

#### STEP 02. 데이터 정합성 검증

검증 항목:

```text
- 당일 종가 누락 여부
- 거래량/거래대금 음수 또는 0 이상치 여부
- 고가 >= 저가 여부
- 고가 >= 종가 >= 저가 여부
- 고가 >= 시가 >= 저가 여부
- 수정주가 반영 여부
- 상장폐지/거래정지 종목의 가격 처리 여부
- 전일 대비 비정상 급변 데이터 여부
```

정합성 실패 시:

```text
핵심 시세 데이터 누락 → 해당 종목 후보 제외
시장조치 데이터 누락 → 전체 추천 confidence 낮춤 또는 보수적 제외
재무 데이터 누락 → 재무 점수 하향
공시 데이터 누락 → 뉴스/공시 설명만 생략, 하드 필터는 보수적으로 적용
```

#### STEP 03. 시장조치/공시/뉴스 수집

시장조치:

```text
- 관리종목
- 투자주의환기종목
- 거래정지
- 투자주의
- 투자경고
- 투자위험
- 단기과열
- 불성실공시법인
```

공시:

```text
- 감사의견
- 자본잠식
- 유상증자
- 전환사채/신주인수권부사채
- 최대주주 변경
- 횡령/배임
- 영업정지
- 소송
- 주요 계약 해지
```

뉴스:

```text
- 단순 정보 알림
- 리스크 키워드 탐지
- 테마 설명 보조
```

뉴스는 추천 근거를 보강하는 정보로 사용하되, 뉴스 때문에 하드 필터를 무시하면 안 된다.

#### STEP 04. feature 생성

생성 feature:

```text
가격:
- return_1d
- return_3d
- return_5d
- return_10d
- return_20d
- gap_proxy

이평선:
- ma5
- ma20
- ma60
- ma120
- dist_ma5
- dist_ma20
- dist_ma60
- dist_ma120
- ma20_slope_5
- ma60_slope_20
- ma_alignment_state

거래량:
- volume_today
- turnover_today
- median_volume_20
- median_turnover_20
- vol_rel20
- turnover_rel20
- vol_z20
- volume_contraction_5
- volume_expansion_today

캔들:
- close_loc
- body_ratio
- upper_wick_ratio
- lower_wick_ratio

변동성:
- atr14
- atr_pct
- bb_width
- bb_width_percentile_120

지지/저항:
- high_20
- high_60
- low_20
- nearest_resistance
- nearest_support
- upside_to_resistance
- downside_to_support

시장/섹터:
- index_return_5d
- index_return_20d
- index_ma20_state
- sector_return_5d
- sector_return_20d
- sector_relative_strength

재무/공시:
- debt_ratio
- current_ratio
- equity_positive
- operating_profit_state
- sales_growth_state
- recent_cb_bw_flag
- recent_rights_issue_flag
- audit_risk_flag
```

#### STEP 05. 기존 추천 상태 업데이트

이 단계는 오늘 수집한 장 데이터를 이용해 기존 추천의 상태를 업데이트한다.

상태:

```text
OPEN: 아직 평가 중
BUYABLE: 다음날 가격 기준 유효했음
NOT_BUYABLE_GAP: 시가가 최대 허용 진입가를 초과함
TARGET_1_REACHED: 1차 목표권 도달
TARGET_2_REACHED: 2차 목표권 도달
INVALIDATED: 신호 무효 가격 이탈
TIMEOUT: 5거래일 내 목표 미달
```

중요:

```text
시가가 최대 허용 진입가를 넘어서 NOT_BUYABLE_GAP이 된 종목은 추천 실패로 바로 처리하지 않는다.
```

이 경우는 “좋은 신호였지만 사용자에게 유효한 가격을 주지 않은 종목”이 아니라, “추천 메시지에 적힌 조건을 벗어나 진입 대상이 아니었던 종목”으로 분리한다.

#### STEP 06. ML 라벨 업데이트

라벨은 결과가 확정된 샘플만 업데이트한다.

```text
추천일 T 기준:
- T+1 시가가 max_buy_price 이하였는가?
- T+1~T+5 사이에 target_1 또는 stop_price 중 무엇이 먼저 발생했는가?
- 5거래일 내 둘 다 발생하지 않았다면 timeout 처리
```

라벨 예시:

```text
SUCCESS = 1
- max_buy_price 이내에서 유효했고
- 목표가가 손절가보다 먼저 도달

FAIL = 0
- max_buy_price 이내에서 유효했지만
- 손절가 또는 신호 무효 기준이 목표보다 먼저 발생

TIMEOUT = 0 또는 별도 class
- 5거래일 내 목표 미달

NOT_EXECUTABLE = 학습 라벨에서 제외 또는 별도 모델용
- 다음날 시가가 max_buy_price를 초과
```

#### STEP 07. 모델 학습 여부 결정

매일 전체 재학습은 가능하지만 권장 기본값은 아니다. 추천 구조는 아래가 더 안정적이다.

```text
매일: 데이터 수집 + feature 생성 + ML 추론 + 추천 생성
주 1회 또는 조건 충족 시: 모델 재학습
월 1회: feature 중요도 및 성능 점검
```

그래도 매일 학습을 유지해야 한다면 아래 원칙을 지킨다.

```text
- 결과가 확정된 샘플만 사용
- 최근 1~2년 rolling window 사용
- 동일한 random seed 사용
- 학습 데이터 개수 최소 기준 미달 시 마지막 정상 모델 사용
- 모델 성능이 기준 이하로 떨어지면 배포하지 않음
- 모델 버전과 feature 버전을 저장
```

#### STEP 08. ML 추론

ML은 전체 종목을 바로 예측하지 않는다.

권장 구조:

```text
하드 필터 통과
→ 패턴 후보 분류
→ ML 성공확률 계산
→ 하이브리드 점수에 반영
```

ML이 예측할 값:

```text
P(target_1 도달 before stop within 5 trading days)
```

즉 단순히 D+5 종가 수익률을 맞히는 것이 아니라, **정해진 손절/목표 조건에서 상방이 먼저 나올 확률**을 예측한다.

#### STEP 09. 룰 기반 필터 및 signal_score 계산

하드 필터:

```text
- 관리종목 제외
- 거래정지 제외
- 투자경고/위험 제외
- 단기과열 제외 또는 강한 감점
- 감사의견 문제 제외
- 자본잠식 제외
- 시총 기준 미달 제외
- 평균 거래대금 기준 미달 제외
- 5일/20일 과열 기준 초과 제외
```

패턴 필터:

```text
A. 20일선 눌림 후 재상승형
B. 박스권 압축 후 돌파형
C. 역배열 개선형
```

signal_score 예시:

```text
차트 구조: 25점
거래량 구조: 20점
과열 방지: 15점
상대강도: 10점
지지/저항 손익비: 10점
캔들 품질: 10점
재무 안정성: 10점
```

#### STEP 10. 하이브리드 점수 계산

예시:

```python
final_score = (
    0.40 * rule_score
    + 0.35 * ml_probability_score
    + 0.10 * market_regime_score
    + 0.10 * sector_strength_score
    + 0.05 * liquidity_score
)
```

단, 하드 제외 조건에 걸린 종목은 ML 점수가 높아도 복구하지 않는다.

```python
if hard_filter_failed:
    candidate = False
```

#### STEP 11. entry_policy 계산

추천 메시지에 포함될 핵심 가격 구간을 계산한다.

```text
- signal_close
- stop_price
- target_1
- target_2
- max_buy_price
- chase_warning_price
- target_zone_price
- invalidation_price
```

#### STEP 12. 최종 추천 생성

추천 수는 시장 상태에 따라 달라져야 한다.

```text
시장 강세: Top 5~10
시장 보통: Top 3~5
시장 약세: Top 0~3 또는 관망 메시지
```

#### STEP 13. 리포트/메시지 생성

최종 메시지는 반드시 “종목명”만 주지 말고 가격 조건을 포함한다.

필수 출력:

```text
종목명
추천 기준일
추천 기준가
최대 허용 진입가
매수 가능 범위
추격주의 구간
목표권
신호 무효 기준
추천 사유
리스크 요인
상태 해석
```

#### STEP 14. 스냅샷 및 로그 저장

저장 항목:

```text
- run_id
- trade_date
- data_version
- feature_version
- model_version
- config_version
- candidate_count
- excluded_count_by_reason
- recommendation_count
- top_recommendations
- model_metrics_snapshot
- pipeline_warnings
```

---

## 5. entry_policy 상세 설계

### 5.1 왜 필요한가

장마감 후 추천은 T일 종가 기준으로 만들어진다. 하지만 실제 사용은 T+1에 한다. T+1 시초가가 급등하면 좋은 신호였더라도 이미 늦은 가격일 수 있다.

따라서 추천 메시지는 다음 질문에 답해야 한다.

```text
내일 얼마까지면 아직 유효한가?
얼마 이상이면 추격위험인가?
얼마 이상이면 이미 목표권인가?
어디를 깨면 신호가 틀린 것인가?
```

### 5.2 가격 기준 계산

#### 5.2.1 stop_price

패턴별 stop_price:

```text
눌림목형:
stop_price = min(signal_low, ma20 * 0.985, nearest_support * 0.99)

박스돌파형:
stop_price = breakout_level * 0.98

역배열 개선형:
stop_price = min(signal_low, ma20 * 0.98)
```

공통 제한:

```text
signal_close 대비 손절폭이 5%를 초과하면 후보 점수 하향 또는 제외
```

```python
risk_pct = signal_close / stop_price - 1
if risk_pct > 0.05:
    reject_or_penalize()
```

#### 5.2.2 target_1, target_2

기본값:

```text
target_1 = signal_close * 1.05
target_2 = signal_close * 1.08
```

단, 가까운 저항이 있으면 목표가를 조정한다.

```text
nearest_resistance가 target_1보다 가까우면 target_1을 nearest_resistance 근처로 낮춘다.
```

예:

```python
target_1 = min(signal_close * 1.05, nearest_resistance * 0.995)
target_2 = min(signal_close * 1.08, next_resistance * 0.995)
```

#### 5.2.3 max_buy_price

최대 허용 진입가는 여러 조건 중 가장 보수적인 값을 사용한다.

```python
max_by_signal_move = signal_close * 1.03
max_by_target_zone = signal_close * 1.04
max_by_ma20_dist = ma20 * 1.08
max_by_resistance = nearest_resistance / 1.04
max_by_rr = (target_1 + min_rr * stop_price) / (1 + min_rr)

max_buy_price = min(
    max_by_signal_move,
    max_by_target_zone,
    max_by_ma20_dist,
    max_by_resistance,
    max_by_rr
)
```

기본 권장값:

```text
max_by_signal_move: 신호가 대비 +3%
chase_warning_price: 신호가 대비 +4%
target_zone_price: 신호가 대비 +5~6%
min_rr: 1.5
```

#### 5.2.4 손익비 기반 max_buy_price 공식

손익비 조건:

```text
(target_1 - entry_price) / (entry_price - stop_price) >= min_rr
```

이를 entry_price 기준으로 풀면:

```text
entry_price <= (target_1 + min_rr * stop_price) / (1 + min_rr)
```

따라서:

```python
max_by_rr = (target_1 + min_rr * stop_price) / (1 + min_rr)
```

이 값이 낮게 나오면 해당 종목은 구조가 좋아도 현재 가격에서 손익비가 좋지 않다는 뜻이다.

### 5.3 entry_status 판정

T+1 시가 또는 현재가가 들어오면 아래처럼 상태를 판정한다.

```python
def classify_entry_status(price, policy):
    if price <= 0:
        return "UNKNOWN"

    if price < policy["invalidation_price"]:
        return "INVALIDATED"

    if price <= policy["max_buy_price"]:
        return "BUYABLE"

    if price <= policy["chase_warning_price"]:
        return "WATCH_CAUTION"

    if price <= policy["target_zone_price"]:
        return "CHASE_RISK"

    if price <= policy["extended_price"]:
        return "TARGET_ZONE_REACHED"

    return "EXTENDED"
```

기본 상태표:

| 상태 | 조건 | 표시 |
|---|---|---|
| BUYABLE | 현재가 <= max_buy_price | 매수 가능 범위 |
| WATCH_CAUTION | max_buy_price 초과, +4% 이내 | 가격 부담 증가 |
| CHASE_RISK | 신호가 대비 +4~6% | 추격주의 |
| TARGET_ZONE_REACHED | 신호가 대비 +5~8% | 단기 목표권 도달 |
| EXTENDED | 신호가 대비 +8% 이상 | 신규 후보 제외 |
| INVALIDATED | 신호 무효 가격 이탈 | 후보 제외 |

---

## 6. 추천 메시지 포맷

### 6.1 기본 템플릿

```text
[StockMaster 추천]

종목: {name} ({code})
추천 기준일: {signal_date} 장마감
다음 사용일: {recommend_date}

Signal Score: {signal_score}
ML Probability: {ml_probability}
Final Score: {final_score}
패턴: {pattern_type}

추천 기준가: {signal_close}원
매수 가능 범위: {entry_lower}~{max_buy_price}원
최대 허용 진입가: {max_buy_price}원

추격주의: {chase_warning_price}원 이상
목표권 도달: {target_zone_price}원 이상
신호 무효: {invalidation_price}원 이탈

1차 목표: {target_1}원
2차 목표: {target_2}원
가까운 저항: {nearest_resistance}원
가까운 지지: {nearest_support}원

추천 사유:
- {reason_1}
- {reason_2}
- {reason_3}

주의 요인:
- {risk_1}
- {risk_2}

내일 기준:
- 시초가가 {max_buy_price}원 이하이면 후보 유효
- 시초가가 {chase_warning_price}원 이상이면 추격주의
- 시초가가 {target_zone_price}원 이상이면 목표권 도달로 신규 후보 제외
- 시초가가 {invalidation_price}원 아래이면 신호 무효
```

### 6.2 예시

```text
[StockMaster 추천]

종목: 예시종목 (000000)
추천 기준일: 2026-05-11 장마감
다음 사용일: 2026-05-12

Signal Score: 91
ML Probability: 0.63
Final Score: 87
패턴: 20일선 눌림 후 재상승 + 박스 상단 돌파

추천 기준가: 85,500원
매수 가능 범위: 84,800~87,500원
최대 허용 진입가: 87,500원

추격주의: 89,000원 이상
목표권 도달: 90,000원 이상
신호 무효: 82,500원 이탈

1차 목표: 90,000원
2차 목표: 93,000원
가까운 저항: 93,600원
가까운 지지: 84,000원

추천 사유:
- 20일선 재돌파
- 거래량 동반 회복
- 박스 상단 돌파
- 다음날 갭상승 제한 조건이 명확함

주의 요인:
- 신호일 상승폭이 커서 +5% 이상 추격 시 손익비 악화
- 93,000원대 단기 저항 존재

내일 기준:
- 시초가가 87,500원 이하이면 후보 유효
- 시초가가 89,000원 이상이면 추격주의
- 시초가가 90,000원 이상이면 목표권 도달로 신규 후보 제외
- 시초가가 82,500원 아래이면 신호 무효
```

---

## 7. 머신러닝 방법론

### 7.1 ML의 역할

StockMaster에서 ML은 전체 종목을 처음부터 고르는 역할이 아니다.

```text
룰 기반 필터가 후보군을 만든다.
ML은 후보군 내부에서 성공확률을 재정렬한다.
```

즉 ML이 배우는 것은 아래다.

```text
이미 기본 조건을 통과한 종목들 중에서,
어떤 거래량/이평선/캔들/시장/섹터/재무 조합이
3~5일 안에 목표가를 먼저 달성할 확률이 높은가?
```

### 7.2 권장 모델

1차 권장:

```text
LightGBM
CatBoost
XGBoost
Logistic Regression calibrated model
```

초기에는 복잡한 딥러닝보다 트리 기반 모델을 권장한다.

이유:

```text
- tabular feature에 강함
- feature importance 해석 가능
- 작은 데이터에서도 비교적 안정적
- 학습/추론 속도 빠름
- 룰 기반 feature와 결합하기 좋음
```

### 7.3 학습 대상

전체 종목을 무작정 학습하지 않는다.

```text
학습 universe = 과거 시점에서 동일한 하드 필터와 패턴 필터를 통과했던 종목
```

즉 과거 데이터에서도 현재와 동일한 후보 생성 로직을 적용한다.

잘못된 방식:

```text
전체 종목의 다음 5일 수익률을 학습
```

올바른 방식:

```text
하드 필터 통과 + 차트 패턴 후보 + 거래량 조건 충족 종목만 학습
```

### 7.4 라벨 설계: triple-barrier

단순 D+5 수익률을 라벨로 쓰면 모델이 이미 오른 종목을 좋아하기 쉽다. 따라서 triple-barrier 방식을 사용한다.

기본 라벨:

```text
상방 목표: +5%
하방 기준: -3% 또는 신호 무효 가격
기간 제한: 5거래일
```

라벨 생성:

```python
if next_day_open > max_buy_price:
    label = "NOT_EXECUTABLE"
elif target_price_touched_before_stop:
    label = 1
elif stop_touched_before_target:
    label = 0
elif no_target_until_day_5:
    label = 0
```

중요:

```text
NOT_EXECUTABLE은 실패와 다르다.
```

시초가가 최대 허용 진입가를 초과한 경우는 추천 메시지의 조건상 후보가 아니었으므로, 성공/실패 모델에서는 제외하거나 별도 “갭 리스크 모델”의 학습 데이터로 사용한다.

### 7.5 entry-aware label

추천 시스템의 실제 성능은 “종목이 올랐는가”가 아니라 “제시한 가격 조건 안에서 유효했는가”까지 포함해야 한다.

따라서 라벨은 반드시 entry_policy를 반영한다.

```text
1. T일 장마감에 신호 생성
2. T+1 시가 확인
3. T+1 시가 <= max_buy_price이면 executable
4. executable인 샘플만 목표/손절 순서를 평가
5. T+1 시가 > max_buy_price이면 not_executable
```

이렇게 해야 추천 시스템이 다음 문제를 줄인다.

```text
좋은 종목을 골랐지만 다음날 이미 너무 올라 실제로는 들어가기 어려운 문제
```

### 7.6 학습 feature

ML feature는 아래 그룹으로 구성한다.

```text
가격 모멘텀:
- return_1d
- return_3d
- return_5d
- return_10d
- return_20d

과열 방지:
- dist_ma20
- dist_ma60
- rsi14
- vol_rel20
- return_5d_rank

이평선 구조:
- ma5_above_ma20
- ma20_above_ma60
- ma20_slope_5
- ma60_slope_20
- ma_compression_5_20_60

거래량 구조:
- vol_rel20
- turnover_rel20
- vol_z20
- volume_contraction_5
- up_volume_ratio_5
- down_volume_ratio_5

캔들 품질:
- close_loc
- body_ratio
- upper_wick_ratio
- lower_wick_ratio

변동성:
- atr_pct
- bb_width
- bb_width_percentile_120

지지/저항:
- upside_to_resistance
- downside_to_support
- rr_ratio
- box_width_20

시장/섹터:
- market_regime
- index_above_ma20
- sector_relative_strength_5d
- sector_relative_strength_20d

재무/공시 리스크:
- debt_ratio_bucket
- current_ratio_bucket
- equity_positive
- audit_risk_flag
- recent_cb_bw_flag
- recent_rights_issue_flag
```

### 7.7 패턴별 모델

초기에는 하나의 모델로 시작할 수 있다.

```text
model_global
```

성능이 쌓이면 패턴별 모델을 분리한다.

```text
pullback_model: 20일선 눌림 후 재상승형
breakout_model: 박스권 압축 후 돌파형
recovery_model: 역배열 개선형
```

이유:

```text
- 눌림목형과 돌파형의 성공 조건이 다름
- 거래량 기준도 다름
- 과열 판단 기준도 다름
- 손절 기준도 다름
```

### 7.8 학습 주기

권장 기본값:

```text
매일: 추론
주 1회: 재학습
월 1회: feature/성능 점검
```

현재 구조상 매일 학습을 유지한다면:

```text
매일 학습하되, 학습 데이터는 확정 라벨만 사용
성능 기준 미달 시 마지막 정상 모델 유지
model_version을 날짜별로 저장
```

### 7.9 검증 방식

무작위 train/test split 금지.

권장:

```text
walk-forward validation
purged time-series split
embargo period 적용
```

이유:

```text
주식 데이터는 시간 순서가 중요하고,
인접 날짜 샘플이 서로 강하게 연관되어 있기 때문.
```

검증 예시:

```text
Train: 2023-01 ~ 2024-12
Validation: 2025-01 ~ 2025-03
Test: 2025-04 ~ 2025-06

다음 fold:
Train: 2023-04 ~ 2025-03
Validation: 2025-04 ~ 2025-06
Test: 2025-07 ~ 2025-09
```

### 7.10 평가 지표

일반적인 accuracy만 보면 안 된다.

권장 지표:

```text
Top-K hit rate
Top-K average return
Top-K target-first rate
Top-K stop-first rate
NOT_EXECUTABLE ratio
BUYABLE ratio
CHASE_RISK avoided ratio
average max favorable excursion
average max adverse excursion
calibration error
precision@K
```

실제 추천 프로그램에서 중요한 지표:

```text
추천된 종목 중 다음날 max_buy_price 이내에서 유효했던 비율
유효했던 종목 중 target_1을 stop보다 먼저 달성한 비율
추천 후 이미 목표권 도달로 바뀐 종목 비율
추격주의 회피율
```

---

## 8. 룰 기반 필터 요약

### 8.1 하드 제외 필터

```text
관리종목 제외
거래정지 제외
투자경고/위험 제외
단기과열 제외 또는 감점
감사의견 문제 제외
자본잠식 제외
불성실공시법인 제외 또는 감점
스팩/우선주/ETF/ETN 제외, 일반 종목 추천 기준
신규상장 60거래일 이내 제외, 별도 전략 없을 때
```

### 8.2 유동성 필터

```text
현재가 >= 1,000원
시가총액 >= 1,000억 원
20일 평균 거래대금 >= 20억 원
당일 거래대금 >= 20억 원
20일 평균 거래량 >= 5만 주
```

보수형:

```text
시가총액 >= 2,000억 원
20일 평균 거래대금 >= 50억 원
```

### 8.3 재무 리스크 필터

```text
자본총계 > 0
부채비율 <= 300%
유동비율 >= 80%
감사의견 정상
자본잠식 아님
최근 대규모 유상증자/CB/BW 감점
최대주주 변경 잦음 감점
횡령/배임 공시 제외
```

재무는 좋은 종목을 찾기보다 위험 종목을 제거하는 용도로 쓴다.

### 8.4 차트 구조 필터

```text
close > ma20
ma20_slope_5 >= 0
ma60_slope_20 >= -0.005
-0.01 <= close / ma20 - 1 <= 0.06
return_5d <= 0.10~0.12
return_20d <= 0.25~0.30
```

### 8.5 거래량 필터

```text
vol_rel20 = volume_today / median(volume_20)
turnover_rel20 = turnover_today / median(turnover_20)
vol_z20 = zscore(log(volume_today), log(volume_20))
```

권장:

```text
1.3 <= vol_rel20 <= 3.5
turnover_rel20 >= 1.3
vol_z20 >= 0.8
vol_rel20 > 5.0은 과열 위험
```

패턴별:

```text
눌림목형: vol_rel20 1.2~2.5
박스돌파형: vol_rel20 1.7~4.0
역배열 개선형: vol_rel20 1.5~3.5
```

### 8.6 캔들 필터

```text
close_loc = (close - low) / (high - low)
body_ratio = abs(close - open) / (high - low)
upper_wick_ratio = (high - max(open, close)) / (high - low)
```

권장:

```text
close_loc >= 0.65
body_ratio >= 0.35
upper_wick_ratio <= 0.30
```

제외/감점:

```text
vol_rel20 >= 2.0
upper_wick_ratio >= 0.45
close_loc <= 0.55
```

---

## 9. 성과평가 설계

### 9.1 성과평가를 세 종류로 분리

#### A. 추천 생성 품질

```text
추천된 종목이 구조적으로 타당했는가?
signal_score가 높은 종목이 실제로 좋은 결과를 냈는가?
```

#### B. 가격 유효성 품질

```text
다음날 시가가 max_buy_price 이내였는가?
추천 메시지의 매수 가능 범위가 현실적이었는가?
NOT_EXECUTABLE 비율이 너무 높지 않은가?
```

#### C. 결과 품질

```text
BUYABLE이었던 종목 중 target_1이 stop보다 먼저 나왔는가?
5거래일 내 수익 구간에 도달했는가?
```

### 9.2 추천 결과 상태

```text
PENDING: 아직 결과 확정 전
BUYABLE: 다음날 유효 가격 범위 안에 들어옴
NOT_EXECUTABLE: 다음날 시가가 max_buy_price 초과
SUCCESS_T1: 1차 목표가 손절보다 먼저 도달
SUCCESS_T2: 2차 목표가 손절보다 먼저 도달
FAIL_STOP: 손절 또는 신호 무효 먼저 발생
TIMEOUT: 5거래일 내 목표 미달
INVALID_DATA: 데이터 불완전
```

### 9.3 성과평가 pseudo-code

```python
def update_recommendation_outcome(rec, daily_bars):
    # rec: T일 추천 스냅샷
    # daily_bars: T+1 ~ 현재까지의 일봉

    first_day = daily_bars[0]

    if first_day.open > rec.max_buy_price:
        rec.entry_status = "NOT_EXECUTABLE"
        # 성공/실패 라벨에는 넣지 않거나 별도 갭 리스크 통계로 처리
        return rec

    rec.entry_status = "BUYABLE"
    entry_price = first_day.open

    for day in daily_bars[:5]:
        hit_target = day.high >= rec.target_1
        hit_stop = day.low <= rec.stop_price

        if hit_target and hit_stop:
            # 일봉만 있을 때는 보수적으로 처리한다.
            # 고가/저가 발생 순서를 모르면 실패 또는 불확실로 분류한다.
            rec.outcome = "AMBIGUOUS"
            return rec

        if hit_target:
            rec.outcome = "SUCCESS_T1"
            return rec

        if hit_stop:
            rec.outcome = "FAIL_STOP"
            return rec

    rec.outcome = "TIMEOUT"
    return rec
```

일봉만 있는 경우 같은 날 목표가와 손절가가 모두 터치되면 순서를 알 수 없다. 이 경우 보수적으로 처리한다.

권장:

```text
AMBIGUOUS는 학습에서 제외하거나 실패로 처리
분봉 데이터가 있으면 실제 선후관계 확인
```

---

## 10. 추천 데이터 스키마

### 10.1 recommendation_snapshot

```json
{
  "run_id": "20260511_postclose_001",
  "signal_date": "2026-05-11",
  "recommend_date": "2026-05-12",
  "code": "000000",
  "name": "예시종목",
  "pattern_type": "PULLBACK_BREAKOUT",
  "signal_close": 85500,
  "signal_high": 87000,
  "signal_low": 82000,
  "ma5": 84200,
  "ma20": 83000,
  "ma60": 80500,
  "ma120": 85000,
  "vol_rel20": 2.1,
  "turnover_rel20": 1.8,
  "rsi14": 61,
  "nearest_support": 84000,
  "nearest_resistance": 93600,
  "stop_price": 82500,
  "target_1": 90000,
  "target_2": 93000,
  "max_buy_price": 87500,
  "chase_warning_price": 89000,
  "target_zone_price": 90000,
  "extended_price": 93000,
  "invalidation_price": 82500,
  "signal_score": 91,
  "rule_score": 88,
  "ml_probability": 0.63,
  "final_score": 87,
  "market_regime": "NORMAL_UP",
  "sector_strength_rank": 18,
  "entry_policy_status": "WAIT_FOR_NEXT_DAY_PRICE",
  "reasons": [
    "20일선 회복",
    "거래량 동반 돌파",
    "박스 상단 돌파"
  ],
  "risks": [
    "신호일 상승폭 큼",
    "상단 저항 가까움"
  ],
  "model_version": "lgbm_202605_week2",
  "feature_version": "feature_v3"
}
```

### 10.2 daily_outcome_update

```json
{
  "signal_date": "2026-05-11",
  "recommend_date": "2026-05-12",
  "code": "000000",
  "t_plus_n": 1,
  "open": 85200,
  "high": 93600,
  "low": 85000,
  "close": 90600,
  "entry_status": "BUYABLE",
  "outcome_status": "TARGET_1_REACHED",
  "entry_price_used": 85200,
  "return_to_close": 0.059,
  "return_to_high": 0.099,
  "notes": [
    "시초가가 max_buy_price 이하",
    "당일 1차 목표 도달"
  ]
}
```

---

## 11. Codex 구현 지침

### 11.1 권장 디렉터리 구조

```text
stockmaster/
  config/
    stockmaster.yml
    filters.yml
    ml.yml
    scheduler.yml

  data/
    raw/
    processed/
    features/
    models/
    reports/
    logs/

  src/
    calendar/
      trading_calendar.py

    ingestion/
      eod_prices.py
      market_actions.py
      disclosures.py
      news.py
      fundamentals.py

    validation/
      data_quality.py
      point_in_time.py

    features/
      price_features.py
      volume_features.py
      candle_features.py
      trend_features.py
      support_resistance.py
      market_regime.py
      financial_features.py
      build_feature_matrix.py

    filters/
      hard_filters.py
      liquidity_filters.py
      financial_filters.py
      pattern_filters.py
      overheat_filters.py

    scoring/
      rule_score.py
      signal_score.py
      entry_policy.py
      hybrid_score.py
      status_classifier.py

    ml/
      labels.py
      dataset.py
      train.py
      predict.py
      calibration.py
      validation.py
      model_registry.py

    evaluation/
      recommendation_outcomes.py
      metrics.py
      backtest_report.py

    reports/
      recommendation_message.py
      daily_summary.py

    pipeline/
      post_close_cycle.py
      recommendation_cycle.py
      evaluation_cycle.py

  tests/
    test_entry_policy.py
    test_no_future_leakage.py
    test_label_generation.py
    test_filters.py
    test_pipeline_order.py
```

### 11.2 main pipeline pseudo-code

```python
def run_post_close_cycle(trade_date: str) -> None:
    run_id = create_run_id(trade_date)

    assert is_trading_day(trade_date)
    assert is_after_market_close(trade_date)

    # 1. 수집
    price_data = collect_eod_prices(trade_date)
    index_data = collect_index_data(trade_date)
    sector_data = collect_sector_data(trade_date)
    market_actions = collect_market_actions(trade_date)
    disclosures = collect_disclosures(trade_date)
    news = collect_news(trade_date)
    fundamentals = load_latest_point_in_time_fundamentals(trade_date)

    # 2. 검증
    validate_price_data(price_data)
    validate_market_actions(market_actions)
    validate_point_in_time(fundamentals, trade_date)

    # 3. feature 생성
    feature_matrix = build_features(
        trade_date=trade_date,
        price_data=price_data,
        index_data=index_data,
        sector_data=sector_data,
        fundamentals=fundamentals,
        market_actions=market_actions,
        disclosures=disclosures,
    )

    # 4. 기존 추천 성과 업데이트
    update_existing_recommendation_outcomes(trade_date, price_data)

    # 5. 라벨 업데이트
    update_matured_labels(trade_date)

    # 6. 모델 학습 여부 결정
    if should_train_model(trade_date):
        train_result = train_model_with_matured_labels(trade_date)
        if train_result.passed_quality_gate:
            register_model(train_result)
        else:
            use_last_good_model()

    # 7. 후보 생성
    candidates = apply_hard_filters(feature_matrix)
    candidates = apply_pattern_filters(candidates)
    candidates = apply_overheat_filters(candidates)

    # 8. ML 추론
    model = load_active_model()
    candidates = predict_ml_probability(model, candidates)

    # 9. 점수 계산
    candidates = calculate_signal_score(candidates)
    candidates = calculate_hybrid_score(candidates)

    # 10. entry_policy 계산
    candidates = calculate_entry_policy(candidates)

    # 11. 최종 추천
    recommendations = select_top_recommendations(candidates)

    # 12. 메시지 생성
    report = build_recommendation_report(recommendations)

    # 13. 저장
    save_recommendation_snapshot(run_id, recommendations)
    save_report(run_id, report)
    save_pipeline_log(run_id)
```

### 11.3 entry_policy 함수 pseudo-code

```python
def calculate_entry_policy(row, config):
    signal_close = row["close"]
    signal_low = row["low"]
    ma20 = row["ma20"]
    nearest_support = row["nearest_support"]
    nearest_resistance = row["nearest_resistance"]
    pattern_type = row["pattern_type"]

    if pattern_type == "PULLBACK":
        stop_price = min(signal_low, ma20 * 0.985, nearest_support * 0.99)
    elif pattern_type == "BREAKOUT":
        stop_price = row["breakout_level"] * 0.98
    elif pattern_type == "RECOVERY":
        stop_price = min(signal_low, ma20 * 0.98)
    else:
        stop_price = min(signal_low, ma20 * 0.98)

    target_1_raw = signal_close * 1.05
    target_2_raw = signal_close * 1.08

    if nearest_resistance and nearest_resistance > signal_close:
        target_1 = min(target_1_raw, nearest_resistance * 0.995)
        target_2 = min(target_2_raw, nearest_resistance * 1.02)
    else:
        target_1 = target_1_raw
        target_2 = target_2_raw

    min_rr = config["min_rr"]

    max_by_signal_move = signal_close * config["max_entry_move_from_signal"]
    max_by_target_zone = signal_close * config["max_target_zone_move"]
    max_by_ma20_dist = ma20 * config["max_dist_ma20_entry"]
    max_by_rr = (target_1 + min_rr * stop_price) / (1 + min_rr)

    if nearest_resistance and nearest_resistance > signal_close:
        max_by_resistance = nearest_resistance / (1 + config["min_upside_to_resistance"])
    else:
        max_by_resistance = signal_close * 1.03

    max_buy_price = min(
        max_by_signal_move,
        max_by_target_zone,
        max_by_ma20_dist,
        max_by_resistance,
        max_by_rr,
    )

    return {
        "stop_price": round_to_tick(stop_price),
        "target_1": round_to_tick(target_1),
        "target_2": round_to_tick(target_2),
        "max_buy_price": round_to_tick(max_buy_price),
        "chase_warning_price": round_to_tick(signal_close * 1.04),
        "target_zone_price": round_to_tick(signal_close * 1.05),
        "extended_price": round_to_tick(signal_close * 1.08),
        "invalidation_price": round_to_tick(stop_price),
    }
```

### 11.4 상태 분류 함수 pseudo-code

```python
def classify_next_day_status(open_price, policy):
    if open_price < policy["invalidation_price"]:
        return "INVALIDATED"
    if open_price <= policy["max_buy_price"]:
        return "BUYABLE"
    if open_price <= policy["chase_warning_price"]:
        return "WATCH_CAUTION"
    if open_price <= policy["target_zone_price"]:
        return "CHASE_RISK"
    if open_price <= policy["extended_price"]:
        return "TARGET_ZONE_REACHED"
    return "EXTENDED"
```

---

## 12. 설정 파일 예시

```yaml
stockmaster:
  holding_period_days: 5
  top_k_default: 5

scheduler:
  post_close_cycle:
    enabled: true
    description: "장마감 이후 추천 생성 사이클"
  next_day_revalidation:
    enabled: false
    description: "장중 타이머가 없으면 false. 대신 추천 메시지에 가격 조건 표시"

universe:
  min_price: 1000
  min_market_cap: 100000000000
  min_avg_turnover_20: 2000000000
  exclude_etf_etn: true
  exclude_spac: true
  exclude_preferred: true
  exclude_new_listing_days: 60

hard_filters:
  exclude_management: true
  exclude_trading_halt: true
  exclude_investment_warning: true
  exclude_investment_risk: true
  exclude_short_term_overheated: true
  exclude_capital_impairment: true
  exclude_bad_audit_opinion: true

financial:
  max_debt_ratio: 300
  min_current_ratio: 80
  require_positive_equity: true
  recent_cb_bw_penalty: true
  recent_rights_issue_penalty: true

technical:
  ma_periods: [5, 20, 60, 120]
  max_return_5d: 0.12
  max_return_20d: 0.30
  max_dist_ma20: 0.10
  max_dist_ma60: 0.20
  rsi14_min: 45
  rsi14_max: 72
  atr_pct_max: 0.10

volume:
  vol_rel20_min: 1.3
  vol_rel20_max: 3.5
  turnover_rel20_min: 1.3
  vol_z20_min: 0.8
  overheat_vol_rel20: 5.0

candle:
  close_loc_min: 0.65
  body_ratio_min: 0.35
  upper_wick_ratio_max: 0.30

entry_policy:
  max_entry_move_from_signal: 1.03
  chase_warning_move_from_signal: 1.04
  target_zone_move_from_signal: 1.05
  extended_move_from_signal: 1.08
  max_target_zone_move: 1.04
  max_dist_ma20_entry: 1.08
  min_upside_to_resistance: 0.04
  min_rr: 1.5
  max_risk_pct: 0.05

ml:
  enabled: true
  model_type: "lightgbm"
  train_schedule: "weekly"
  daily_train_allowed: false
  use_only_matured_labels: true
  label_target_pct: 0.05
  label_stop_pct: 0.03
  label_horizon_days: 5
  exclude_not_executable_from_success_model: true
  validation_method: "walk_forward"
  min_training_samples: 3000
  use_last_good_model_on_failure: true

scoring:
  rule_weight: 0.40
  ml_weight: 0.35
  market_regime_weight: 0.10
  sector_strength_weight: 0.10
  liquidity_weight: 0.05
```

---

## 13. 테스트 체크리스트

### 13.1 파이프라인 순서 테스트

```text
[ ] 당일 장 데이터 수집이 성과평가보다 먼저 실행되는가?
[ ] 라벨 업데이트가 학습보다 먼저 실행되는가?
[ ] 학습은 확정 라벨만 사용하는가?
[ ] 추천 생성은 T일 데이터까지만 사용하는가?
[ ] 추천 결과가 run_id, model_version, feature_version과 함께 저장되는가?
```

### 13.2 데이터 누수 테스트

```text
[ ] T일 feature에 T+1 이후 가격이 들어가지 않는가?
[ ] 재무제표는 실제 공시일 이후부터만 반영되는가?
[ ] 공시는 공시 시각 이후부터만 반영되는가?
[ ] 추천일 이후의 목표/손절 결과가 feature에 섞이지 않는가?
[ ] 종목 필터가 과거 백테스트 시점에도 동일하게 적용되는가?
```

### 13.3 entry_policy 테스트

```text
[ ] max_buy_price가 target_1보다 낮은가?
[ ] max_buy_price 기준 손익비가 min_rr 이상인가?
[ ] stop_price가 signal_close보다 낮은가?
[ ] target_1이 signal_close보다 높은가?
[ ] 저항이 너무 가까우면 max_buy_price가 낮아지는가?
[ ] 신호가 대비 +5% 이상이면 TARGET_ZONE_REACHED로 바뀌는가?
```

### 13.4 추천 메시지 테스트

```text
[ ] 추천 기준가가 표시되는가?
[ ] 최대 허용 진입가가 표시되는가?
[ ] 추격주의 가격이 표시되는가?
[ ] 목표권 도달 가격이 표시되는가?
[ ] 신호 무효 가격이 표시되는가?
[ ] 추천 사유와 리스크가 함께 표시되는가?
```

### 13.5 ML 테스트

```text
[ ] NOT_EXECUTABLE 샘플이 성공/실패 라벨에 섞이지 않는가?
[ ] walk-forward validation을 사용하는가?
[ ] 모델 성능이 기준 이하일 때 마지막 정상 모델을 사용하는가?
[ ] feature importance가 저장되는가?
[ ] 확률 보정 결과가 저장되는가?
[ ] Top-K 기준 성능이 저장되는가?
```

---

## 14. 운영 로그 예시

```text
[2026-05-11 15:45] POST_CLOSE_CYCLE_START
[2026-05-11 15:47] EOD_PRICE_INGEST_DONE: 2,600 issues
[2026-05-11 15:48] DATA_VALIDATION_DONE: warnings=3
[2026-05-11 15:50] MARKET_ACTIONS_DONE
[2026-05-11 15:53] DISCLOSURE_NEWS_DONE
[2026-05-11 15:57] FEATURES_DONE: 2,431 valid issues
[2026-05-11 16:00] OUTCOME_UPDATE_DONE: matured=421, pending=84
[2026-05-11 16:02] LABEL_UPDATE_DONE: success=173, fail=248, not_executable=61
[2026-05-11 16:03] MODEL_LOAD_DONE: lgbm_202605_week2
[2026-05-11 16:05] FILTER_DONE: hard_pass=812, pattern_pass=37
[2026-05-11 16:06] ML_PREDICT_DONE
[2026-05-11 16:07] HYBRID_SCORE_DONE
[2026-05-11 16:08] ENTRY_POLICY_DONE
[2026-05-11 16:09] RECOMMENDATION_DONE: top_k=5
[2026-05-11 16:10] REPORT_SAVED
[2026-05-11 16:10] POST_CLOSE_CYCLE_END
```

---

## 15. 공식 데이터 소스 기준

StockMaster는 아래 공식/준공식 데이터 흐름을 우선한다.

```text
KRX Data Marketplace:
- 종목별 시세
- 지수 시세
- 종목 기본 정보
- 시장조치/지정 정보
- 거래대금/거래량

KRX KIND:
- 상장공시
- 투자유의 관련 정보
- 관리종목/거래정지/불성실공시 등 확인

DART/OpenDART:
- 정기보고서
- 주요사항보고서
- 재무제표
- 감사의견
- 유상증자/CB/BW/최대주주 변경 등 공시
```

참고 URL:

```text
KRX Data Marketplace: https://data.krx.co.kr/
KRX KIND: https://kind.krx.co.kr/
DART: https://dart.fss.or.kr/
OpenDART: https://opendart.fss.or.kr/
English OpenDART: https://engopendart.fss.or.kr/
```

---

## 16. 최종 구현 원칙

StockMaster v3.0은 아래 원칙을 따른다.

```text
1. 장마감 후 추천은 T일 종가 확정 데이터만 사용한다.
2. 성과평가는 반드시 당일 장 데이터 수집 이후 수행한다.
3. ML 학습은 확정 라벨만 사용한다.
4. 전체 종목을 ML이 직접 고르지 않는다.
5. 룰 필터로 후보군을 만들고 ML은 후보군 내부의 순위를 조정한다.
6. 추천 메시지는 종목명만 제공하지 않는다.
7. 반드시 최대 허용 진입가, 추격주의 가격, 목표권 가격, 신호 무효 가격을 제공한다.
8. 다음날 시초가가 최대 허용 진입가를 넘으면 추천 실패가 아니라 NOT_EXECUTABLE로 분리한다.
9. NOT_EXECUTABLE 비율은 별도 관리한다.
10. 모델 성능은 accuracy가 아니라 Top-K, target-first, buyable ratio, not-executable ratio로 평가한다.
11. 공시/재무 정보는 point-in-time 원칙을 지킨다.
12. 데이터가 불완전하면 추천 수를 줄이거나 confidence를 낮춘다.
```

---

## 17. 한 줄 요약

```text
StockMaster의 이상적 구조는 장마감 후 확정 데이터로 신호를 만들고, 다음날 실제 가격 변동을 고려할 수 있도록 “얼마까지 유효한 추천인지”를 함께 제공하는 하이브리드 추천 엔진이다.
```
