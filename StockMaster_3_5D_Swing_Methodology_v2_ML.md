# StockMaster 3~5일 보유형 종목 추천 방법론 v2.0

> 목적: StockMaster를 **국내주식 3~5거래일 보유형 종목 추천/분석 엔진**으로 설계한다.  
> 핵심 원칙: 전체 종목을 모델이 바로 예측하지 않는다. 먼저 신뢰도 높은 룰 기반 필터로 위험·과열·비유동·부실 종목을 제거하고, 남은 후보만 머신러닝으로 재정렬한다.

---

## 0. v2.0에서 반드시 반영할 개선 사항

v1.0의 핵심은 `룰 기반 필터 → 후보 점수화 → 추천`이었다. v2.0에서는 실제 사용 품질을 높이기 위해 아래 개선을 추가한다.

### 0.1 추천 점수와 현재 진입 가능 점수 분리

하루 전 신호가 좋았다고 해서 다음 날에도 같은 가격에서 유효한 것은 아니다. 따라서 StockMaster는 반드시 아래 두 점수를 분리한다.

```text
signal_score: 신호일 종가 기준으로 산출한 종목 선정 점수
entry_score: 사용자가 확인하는 현재 가격 기준의 유효성 점수
```

예를 들어 신호일 종가가 85,500원이고 다음 날 현재가가 90,600원이면, 종목 선정 자체는 성공일 수 있으나 현재 가격 기준으로는 이미 1차 목표권에 접근했을 수 있다. 그러면 StockMaster는 해당 종목을 계속 단순 추천으로 보여주면 안 되고 상태를 변경해야 한다.

```text
좋은 종목 선정 ≠ 지금도 신규 후보로 적합
```

### 0.2 추천 이후 가격 재검증

신호일 다음 날 가격이 이미 많이 상승했다면 추격 위험으로 표시한다.

```python
price_move_from_signal = current_price / signal_close - 1

if price_move_from_signal >= 0.06:
    entry_status = "TARGET_ZONE_REACHED"
elif price_move_from_signal >= 0.04:
    entry_status = "CHASE_RISK"
else:
    entry_status = "VALID"
```

기본 해석:

| 조건 | 상태 | 의미 |
|---|---|---|
| 신호가 대비 +0~3% | VALID | 현재도 후보 유효 |
| 신호가 대비 +3~4% | WATCH_CAUTION | 좋지만 가격 부담 증가 |
| 신호가 대비 +4~6% | CHASE_RISK | 추격 위험, 신규 후보 점수 하향 |
| 신호가 대비 +6% 이상 | TARGET_ZONE_REACHED | 3~5일 전략의 1차 목표권 도달 가능성 |
| 주요 지지선 이탈 | INVALIDATED | 신호 무효 |

### 0.3 목표권 도달 상태 추가

3~5일 보유형 전략에서 +4~6%는 이미 의미 있는 구간이다. 따라서 신호 이후 가격이 빠르게 +5% 이상 움직였으면 추천 상태를 바꾼다.

```python
target_1 = signal_close * 1.05
target_2 = signal_close * 1.08

if current_price >= target_2:
    entry_status = "EXTENDED"
elif current_price >= target_1:
    entry_status = "TARGET_ZONE_REACHED"
```

상태별 표시 예시:

```text
VALID: 현재 후보 유효
WATCH_CAUTION: 후보 유효하지만 가격 부담 있음
CHASE_RISK: 신호 이후 많이 상승, 눌림 대기 우선
TARGET_ZONE_REACHED: 단기 목표권 도달, 신규 후보 제외 가능
INVALIDATED: 핵심 기준선 이탈, 후보 제외
```

### 0.4 저항까지 거리 체크 강화

신호가 좋아도 바로 위에 저항이 가까우면 손익비가 나빠진다.

```python
upside_to_resistance = nearest_resistance / current_price - 1

if upside_to_resistance < 0.03:
    entry_score -= 20
elif upside_to_resistance < 0.05:
    entry_score -= 10
```

3~5일 보유 후보는 최소한 아래 조건을 만족해야 한다.

```text
가장 가까운 주요 저항까지 거리 >= 4%
예상 손실 기준 대비 기대 상승 공간 >= 1.5배
```

### 0.5 추천 상태 전환 로직 추가

StockMaster는 단순히 `추천/비추천`만 출력하지 말고, 추천 이후 상태를 추적해야 한다.

```text
SIGNAL_CREATED
→ VALID
→ WATCH_CAUTION
→ CHASE_RISK
→ TARGET_ZONE_REACHED
→ EXTENDED
→ INVALIDATED
```

상태 전환 예시:

```python
if current_price < invalidation_price:
    status = "INVALIDATED"
elif current_price >= signal_close * 1.08:
    status = "EXTENDED"
elif current_price >= signal_close * 1.05:
    status = "TARGET_ZONE_REACHED"
elif current_price >= signal_close * 1.04:
    status = "CHASE_RISK"
elif current_price >= signal_close * 1.03:
    status = "WATCH_CAUTION"
else:
    status = "VALID"
```

---

## 1. StockMaster 전체 구조

### 1.1 처리 흐름

```text
전체 종목 수집
→ 데이터 정합성 검사
→ 하드 제외 필터
→ 유동성 필터
→ 재무/공시 리스크 필터
→ 시장/섹터 환경 필터
→ 차트 구조 필터
→ 거래량/캔들 트리거 필터
→ 과열 방지 필터
→ 룰 기반 signal_score 산출
→ 머신러닝 후보 랭킹
→ 현재 가격 기반 entry_score 재계산
→ 최종 추천 상태 산출
→ 설명 생성
```

### 1.2 가장 중요한 설계 원칙

```text
1. 위험 종목 제거가 수익 후보 탐색보다 우선이다.
2. 머신러닝은 전체 종목을 직접 예측하지 않고, 필터 통과 후보만 재정렬한다.
3. 신호일 점수와 현재 가격 점수를 분리한다.
4. 추천 이후 가격이 이미 목표권에 도달했으면 신규 후보로 취급하지 않는다.
5. 모든 feature는 해당 시점에 이미 알 수 있는 정보만 사용한다.
6. 하드 제외 필터는 모델 점수로 절대 복구하지 않는다.
```

---

## 2. 데이터 소스 기준

### 2.1 권장 데이터

| 데이터 | 사용 목적 |
|---|---|
| 일별 OHLCV | 이동평균, 수익률, 거래량, 캔들, 변동성 계산 |
| 거래대금 | 유동성 필터, 실제 후보 안정성 판단 |
| 시장조치 정보 | 관리종목, 투자주의, 투자경고, 투자위험, 단기과열, 거래정지 제외 |
| 공시 정보 | 감사의견, 자본잠식, 유상증자, CB/BW, 최대주주 변경, 횡령/배임 등 확인 |
| 재무제표 | 부채비율, 유동비율, 자본총계, 매출, 이익, 현금흐름 계산 |
| 지수/섹터 데이터 | 시장 레짐, 섹터 상대강도 계산 |

### 2.2 공식 참고 링크

- KRX Data Marketplace: `https://data.krx.co.kr/`
- KRX KIND: `https://kind.krx.co.kr/`
- DART: `https://dart.fss.or.kr/`
- OpenDART: `https://opendart.fss.or.kr/`

### 2.3 데이터 정합성 규칙

보수적으로 처리한다.

```text
일봉 누락 → 후보 제외
거래량/거래대금 누락 → 후보 제외
시장조치 데이터 누락 → 후보 제외 또는 낮은 신뢰도 표시
최근 재무제표 누락 → 재무 점수 하향
감사의견 정보 누락 → 후보 제외 또는 강한 패널티
수정주가 불일치 → 해당 종목 feature 재계산 보류
```

### 2.4 Point-in-Time 원칙

머신러닝 및 점수화에서 가장 중요한 규칙이다.

```text
신호일 t의 feature는 t일 장마감 시점까지 확인 가능한 정보만 사용한다.
라벨은 t+1~t+5 구간 결과로 만든다.
분기 재무제표는 공시일 이후부터만 사용한다.
공시 이벤트도 공시 시각 이후부터만 반영한다.
```

잘못된 예:

```text
2025년 3월 말 재무제표를 2025년 3월 31일부터 사용
```

올바른 예:

```text
2025년 3월 말 재무제표가 실제 공시된 날짜 이후부터 사용
```

---

## 3. 하드 제외 필터

아래 조건은 모델이 아무리 높은 점수를 줘도 후보에서 제외한다.

| 항목 | 처리 |
|---|---|
| 관리종목 | 제외 |
| 거래정지 | 제외 |
| 정리매매 | 제외 |
| 투자경고 | 제외 |
| 투자위험 | 제외 |
| 단기과열종목 | 기본 제외. 별도 옵션에서만 허용 |
| 감사의견 비적정/한정/의견거절 | 제외 |
| 자본잠식 | 제외 |
| 상장폐지 실질심사 관련 | 제외 |
| 불성실공시법인 | 제외 또는 강한 패널티 |
| 스팩, 우선주, ETN, 레버리지/인버스 상품 | 일반 종목 추천에서는 제외 |
| 신규상장 60거래일 이내 | 별도 IPO 모드가 아니면 제외 |

구현 예시:

```python
def pass_hard_filters(row):
    return (
        row.is_common_stock
        and not row.is_management_issue
        and not row.is_trading_halt
        and not row.is_delisting_process
        and not row.is_investment_warning
        and not row.is_investment_risk
        and not row.is_short_term_overheated
        and row.audit_opinion_normal
        and not row.capital_impairment
        and not row.recent_serious_disclosure_risk
    )
```

---

## 4. 유동성 필터

3~5일 보유형 후보는 유동성이 낮으면 추천 신뢰도가 떨어진다.

### 4.1 기본 조건

| 항목 | 최소 기준 | 권장 기준 |
|---|---:|---:|
| 현재가 | 1,000원 이상 | 2,000원 이상 |
| 시가총액 | 1,000억 원 이상 | 2,000억 원 이상 |
| 20일 평균 거래대금 | 20억 원 이상 | 50억 원 이상 |
| 당일 거래대금 | 20억 원 이상 | 30억 원 이상 |
| 20일 평균 거래량 | 5만 주 이상 | 10만 주 이상 |

### 4.2 구현 기준

```python
liquidity_pass = (
    close >= 1000
    and market_cap >= 100_000_000_000
    and avg_turnover_20 >= 2_000_000_000
    and turnover_today >= 2_000_000_000
    and avg_volume_20 >= 50_000
)
```

더 보수적인 모드:

```python
liquidity_pass_strict = (
    close >= 2000
    and market_cap >= 200_000_000_000
    and avg_turnover_20 >= 5_000_000_000
    and turnover_today >= 3_000_000_000
)
```

---

## 5. 재무제표 및 공시 리스크 필터

재무제표는 단기 상승을 직접 예측하기 위한 용도보다 **위험 종목 제거** 용도로 사용한다.

```text
좋은 회사 선별용보다는 위험 회사 제거용으로 우선 사용한다.
```

### 5.1 공통 제외 조건

| 항목 | 기준 |
|---|---|
| 자본총계 | 0 이하 제외 |
| 자본잠식 | 제외 |
| 부채비율 | 300% 초과 제외. 금융업은 별도 기준 |
| 유동비율 | 80% 미만 강한 패널티, 60% 미만 제외 검토 |
| 감사의견 | 비적정/한정/의견거절 제외 |
| 최근 2~3년 연속 영업손실 | 일반 기업은 패널티 또는 제외 |
| 최근 대규모 유상증자 | 강한 패널티 |
| CB/BW 잦은 발행 | 강한 패널티 |
| 최대주주 변경 잦음 | 강한 패널티 |
| 횡령/배임 공시 | 제외 |

### 5.2 일반 제조/서비스 기업 기준

```python
financial_pass_general = (
    equity > 0
    and debt_ratio <= 300
    and current_ratio >= 80
    and audit_opinion_normal
    and not capital_impairment
    and revenue_ttm >= 50_000_000_000
)
```

가점 요소:

```text
최근 분기 매출 YoY 증가
최근 분기 영업이익 흑자
최근 2년 연속 영업이익 흑자
영업현금흐름 양호
부채비율 100% 미만
유동비율 150% 이상
```

### 5.3 바이오/성장주 기준

바이오/성장주는 영업손실이 흔하므로 일반 제조업 기준을 그대로 적용하면 안 된다. 대신 현금 소진 리스크와 증자 리스크를 본다.

```python
financial_pass_growth = (
    equity > 0
    and audit_opinion_normal
    and not capital_impairment
    and cash_runway_ratio >= 1.5
    and not recent_large_dilution_event
)
```

```python
cash_runway_ratio = cash_and_equivalents / abs(operating_cash_flow_ttm)
```

단, `operating_cash_flow_ttm`이 양수면 `cash_runway_ratio`는 충분한 것으로 처리할 수 있다.

### 5.4 재무 점수

재무 점수는 전체 점수의 10~15%만 반영한다.

| 조건 | 점수 |
|---|---:|
| 부채비율 100% 미만 | +3 |
| 부채비율 100~200% | +2 |
| 부채비율 200~300% | +0 |
| 유동비율 150% 이상 | +2 |
| 유동비율 100~150% | +1 |
| 최근 분기 매출 YoY 증가 | +2 |
| 최근 분기 영업이익 흑자 | +2 |
| 최근 2년 연속 영업이익 흑자 | +2 |
| 최근 대규모 유상증자/CB/BW | -3~-5 |
| 최대주주 변경 잦음 | -5 |
| 감사의견 문제 | 제외 |

---

## 6. 시장/섹터 환경 필터

개별 종목이 좋아도 시장과 섹터가 약하면 3~5일 성공률이 낮아진다.

### 6.1 시장 레짐

```python
index_ma20 = rolling_mean(index_close, 20)
index_ma60 = rolling_mean(index_close, 60)
index_ma20_slope_5 = index_ma20 / index_ma20.shift(5) - 1

market_regime_ok = (
    index_close > index_ma20
    and index_ma20_slope_5 >= 0
)
```

상태별 추천 강도:

| 시장 상태 | 처리 |
|---|---|
| 지수 > 20일선, 20일선 상승 | 정상 |
| 지수 > 20일선, 20일선 하락 | 추천 수 축소 |
| 지수 < 20일선, 20일선 상승 | 선별 추천 |
| 지수 < 20일선, 20일선 하락 | 후보 수 제한, 보수 모드 |
| 지수 급락 + 거래대금 증가 | 대부분 제외 |

### 6.2 섹터 상대강도

```python
sector_rs_5 = sector_return_5d - market_return_5d
sector_rs_20 = sector_return_20d - market_return_20d

sector_ok = (
    sector_rs_5 > 0
    or sector_rs_20 > 0
)
```

더 강한 조건:

```python
sector_rank_20d <= 0.30
```

즉 20일 성과 기준 상위 30% 섹터를 우선한다.

---

## 7. 차트 구조 필터

StockMaster의 기본 이동평균은 5, 20, 60, 120일 단순이평이다.

```python
ma5 = sma(close, 5)
ma20 = sma(close, 20)
ma60 = sma(close, 60)
ma120 = sma(close, 120)
```

### 7.1 기울기 계산

```python
ma20_slope_5 = ma20 / ma20.shift(5) - 1
ma60_slope_20 = ma60 / ma60.shift(20) - 1
ma120_slope_20 = ma120 / ma120.shift(20) - 1
```

### 7.2 기본 제외 구조

아래 구조는 3~5일 추천 후보로 부적합하다.

```python
weak_downtrend = (
    close < ma20
    and ma5 < ma20 < ma60 < ma120
    and ma20_slope_5 < 0
    and ma60_slope_20 < 0
)
```

### 7.3 과열 이격 제외

```python
dist_ma20 = close / ma20 - 1
dist_ma60 = close / ma60 - 1

if dist_ma20 > 0.10:
    reject_or_penalize()
if dist_ma60 > 0.20:
    reject_or_penalize()
```

추천 구간:

```text
dist_ma20: -1% ~ +6%
dist_ma60: 0% ~ +12%
```

---

## 8. 거래량 필터

거래량은 평균보다 중앙값을 우선 사용한다. 평균은 한 번의 급등 거래량에 의해 왜곡될 수 있다.

### 8.1 기본 계산

```python
median_volume_20 = rolling_median(volume, 20)
median_volume_60 = rolling_median(volume, 60)
median_turnover_20 = rolling_median(turnover, 20)

vol_rel20 = volume / median_volume_20
vol_rel60 = volume / median_volume_60
turnover_rel20 = turnover / median_turnover_20

vol_z20 = (log(volume) - rolling_mean(log(volume), 20)) / rolling_std(log(volume), 20)
```

### 8.2 거래량 기준

| 상황 | 기준 |
|---|---|
| 눌림목 재상승 | `1.2 <= vol_rel20 <= 2.5` |
| 박스 돌파 | `1.7 <= vol_rel20 <= 4.0` |
| 회복형 반전 | `1.5 <= vol_rel20 <= 3.5` |
| 약함 | `vol_rel20 < 1.2` |
| 과열 가능 | `vol_rel20 > 5.0` |
| 위험 | `vol_rel20 > 5.0` + 장대 윗꼬리 |

기본 추천 조건:

```python
volume_trigger = (
    1.3 <= vol_rel20 <= 3.5
    and turnover_rel20 >= 1.3
    and 0.8 <= vol_z20 <= 2.8
)
```

돌파형 조건:

```python
breakout_volume_trigger = (
    1.7 <= vol_rel20 <= 4.0
    and turnover_rel20 >= 1.5
    and vol_z20 >= 1.0
)
```

### 8.3 거래량 수축 후 증가

선행형 후보에 유용하다.

```python
volume_contraction_then_expansion = (
    rolling_mean(volume, 5) <= median_volume_20 * 0.8
    and volume >= median_volume_20 * 1.5
)
```

해석:

```text
최근 며칠간 조용함
→ 오늘 의미 있는 수급 증가
→ 초기 움직임일 가능성
```

### 8.4 거래량 실린 음봉 제외

```python
bad_distribution = (
    close < open
    and daily_return <= -0.03
    and vol_rel20 >= 1.5
    and close < ma20
)
```

이 조건은 매도 압력이 강한 구조로 해석한다.

---

## 9. 캔들 품질 필터

### 9.1 기본 계산

```python
price_range = high - low
close_loc = (close - low) / price_range
upper_wick_ratio = (high - max(open, close)) / price_range
lower_wick_ratio = (min(open, close) - low) / price_range
body_ratio = abs(close - open) / price_range
```

`price_range`가 0이면 해당 캔들 feature는 결측 처리한다.

### 9.2 좋은 캔들

```python
good_candle = (
    close_loc >= 0.65
    and body_ratio >= 0.35
    and upper_wick_ratio <= 0.30
)
```

해석:

```text
종가가 당일 범위 상단
몸통이 충분함
윗꼬리가 과도하지 않음
```

### 9.3 나쁜 캔들

```python
bad_upper_wick = (
    vol_rel20 >= 2.0
    and upper_wick_ratio >= 0.45
    and close_loc <= 0.55
)
```

해석:

```text
거래량은 늘었지만 위에서 밀림
단기 매물 출회 가능성
```

---

## 10. 보조지표 필터

보조지표는 메인 신호가 아니라 과열/약세 확인용으로 사용한다.

### 10.1 RSI

```python
rsi14 = RSI(close, 14)
rsi5 = RSI(close, 5)
```

추천 구간:

```text
45 <= RSI14 <= 65: 가장 선호
65 < RSI14 <= 70: 강하지만 과열 주의
RSI14 > 72: 감점 또는 제외
RSI5 > 85: 단기 과열 감점
```

### 10.2 MACD

MACD는 후행성이 있으므로 단독 신호로 사용하지 않는다.

좋은 조건:

```python
macd_hist_improving = (
    macd_hist > macd_hist.shift(1)
    and macd_hist.shift(1) > macd_hist.shift(2)
)
```

### 10.3 Bollinger Band

```python
bb_mid = sma(close, 20)
bb_std = rolling_std(close, 20)
bb_upper = bb_mid + 2 * bb_std
bb_lower = bb_mid - 2 * bb_std
bb_width = (bb_upper - bb_lower) / bb_mid
bb_width_rank_120 = percentile_rank(bb_width, window=120)
```

선호 조건:

```python
bb_compression_breakout = (
    bb_width_rank_120 <= 0.40
    and close > bb_mid
    and vol_rel20 >= 1.5
)
```

### 10.4 ATR

```python
atr14 = ATR(high, low, close, 14)
atr_pct = atr14 / close
```

추천 구간:

```text
0.02 <= atr_pct <= 0.08
```

감점/제외:

```text
atr_pct > 0.12: 변동성 과다
```

---

## 11. 과열 방지 필터

모델이 이미 많이 오른 종목을 추천하는 문제를 줄이기 위해 강하게 적용한다.

### 11.1 수익률 과열

```python
ret_1d = close / close.shift(1) - 1
ret_3d = close / close.shift(3) - 1
ret_5d = close / close.shift(5) - 1
ret_20d = close / close.shift(20) - 1
```

기본 제외/감점:

```text
ret_3d > 12%: 감점 또는 제외
ret_5d > 15%: 제외 검토
ret_20d > 40%: 제외 검토
```

추천 기본 조건:

```python
not_overheated = (
    ret_5d <= 0.12
    and ret_20d <= 0.30
    and dist_ma20 <= 0.10
    and dist_ma60 <= 0.20
    and rsi14 <= 72
    and vol_rel20 <= 5.0
)
```

보수 모드:

```python
not_overheated_strict = (
    ret_5d <= 0.10
    and ret_20d <= 0.25
    and dist_ma20 <= 0.08
    and rsi14 <= 70
)
```

### 11.2 거래량 과열

```python
overheated_volume = (
    vol_rel20 > 5.0
    and ret_1d > 0.08
)
```

특히 아래 조건은 강하게 제외한다.

```python
exhaustion_risk = (
    vol_rel20 > 5.0
    and ret_1d > 0.10
    and upper_wick_ratio > 0.35
)
```

---

## 12. 추천 패턴 정의

StockMaster는 후보를 패턴별로 분류한다. 패턴은 설명 생성과 ML feature로 모두 사용한다.

### 12.1 A패턴: 20일선 눌림 후 재상승형

가장 안정적인 기본 패턴이다.

```python
pattern_pullback_resume = (
    close > ma20
    and ma20_slope_5 > 0
    and ma60_slope_20 >= -0.005
    and -0.01 <= dist_ma20 <= 0.06
    and ret_5d <= 0.10
    and ret_20d <= 0.25
    and close > ma5
    and volume_trigger
    and good_candle
    and 45 <= rsi14 <= 70
)
```

설명:

```text
상승 추세가 완전히 훼손되지 않음
20일선 부근에서 눌림 후 재상승
거래량이 다시 증가
과열 이격이 크지 않음
```

### 12.2 B패턴: 박스권 압축 후 첫 돌파형

후행 추천 문제를 줄이는 데 가장 중요하다.

```python
high_20 = rolling_max(high, 20)
low_20 = rolling_min(low, 20)
box_width_20 = high_20 / low_20 - 1

ma_compression_5_20_60 = (
    max(ma5, ma20, ma60) / min(ma5, ma20, ma60) - 1
)

pattern_box_breakout = (
    0.05 <= box_width_20 <= 0.18
    and ma_compression_5_20_60 <= 0.08
    and close > high_20.shift(1) * 1.005
    and breakout_volume_trigger
    and close_loc >= 0.70
    and upper_wick_ratio <= 0.30
    and ret_5d <= 0.12
)
```

설명:

```text
가격이 일정 기간 압축됨
이평선 간격이 좁아짐
첫 수급 증가와 함께 박스 상단 돌파
너무 과열된 상태는 아님
```

### 12.3 C패턴: 역배열 개선형

반등 초입 후보지만 리스크가 높으므로 낮은 가중치를 둔다.

```python
ma5_cross_ma20_up = ma5 > ma20 and ma5.shift(1) <= ma20.shift(1)

pattern_reversal_recovery = (
    close > ma20
    and ma5_cross_ma20_up
    and ma20_slope_5 > -0.003
    and 1.5 <= vol_rel20 <= 3.5
    and 40 <= rsi14 <= 60
    and good_candle
)
```

단 아래 조건이면 제외한다.

```python
bad_reversal = (
    ma5 < ma20 < ma60 < ma120
    and close < ma20
)
```

### 12.4 D패턴: 회복형 돌파 + 현재 가격 재검증

v2.0에서 추가하는 패턴이다. 코스메카코리아 같은 사례를 일반화하기 위한 구조다.

```python
pattern_recovery_breakout = (
    close > ma20
    and close > ma120
    and close > recent_pivot_high * 1.005
    and ma60_slope_20 >= -0.01
    and 1.5 <= vol_rel20 <= 4.0
    and close_loc >= 0.65
    and upper_wick_ratio <= 0.35
)
```

하지만 다음 날 현재 가격에서 반드시 재검증한다.

```python
revalidate_recovery_breakout = (
    current_price / signal_close - 1 <= 0.04
    and upside_to_resistance >= 0.04
    and current_price >= breakout_level
)
```

해석:

```text
신호일 패턴은 좋지만, 다음 날 이미 +5~6% 오른 상태라면 신규 후보로는 점수 하향한다.
```

---

## 13. 지지/저항 및 손익비 필터

### 13.1 주요 가격대 계산

```python
resistance_20 = rolling_max(high, 20)
resistance_60 = rolling_max(high, 60)
support_20 = rolling_min(low, 20)

nearest_resistance = min(
    r for r in [resistance_20, resistance_60, recent_pivot_high]
    if r > current_price
)

nearest_support = max(
    s for s in [ma20, recent_pivot_low, breakout_level]
    if s < current_price
)
```

### 13.2 위쪽 공간

```python
upside_to_resistance = nearest_resistance / current_price - 1
```

기준:

```text
>= 6%: 좋음
4~6%: 가능
3~4%: 보통 이하
< 3%: 신규 후보 감점 또는 제외
```

### 13.3 기준선 이탈 가격

```python
invalidation_price = min(signal_low, ma20 * 0.985, breakout_level * 0.98)
```

패턴별 기준:

| 패턴 | 기준선 |
|---|---|
| 20일선 눌림 후 재상승 | 신호일 저가 또는 20일선 하단 |
| 박스 돌파 | 돌파 기준선 하단 |
| 회복형 돌파 | 회복 기준선 또는 20일선 |
| 역배열 개선 | 20일선 재이탈 |

### 13.4 손익비

```python
risk_distance = current_price / invalidation_price - 1
reward_distance = nearest_resistance / current_price - 1
reward_risk_ratio = reward_distance / risk_distance
```

추천 조건:

```text
risk_distance <= 5%
reward_risk_ratio >= 1.5
```

보수 기준:

```text
risk_distance <= 4%
reward_risk_ratio >= 1.8
```

---

## 14. 룰 기반 점수화

하드 필터를 통과한 종목만 점수화한다.

### 14.1 점수 구성

| 항목 | 배점 |
|---|---:|
| 차트 구조 | 25 |
| 거래량 구조 | 20 |
| 과열 방지 | 15 |
| 상대강도/시장환경 | 10 |
| 지지/저항 손익비 | 10 |
| 캔들 품질 | 10 |
| 재무 안정성 | 10 |
| 합계 | 100 |

### 14.2 차트 구조 25점

| 조건 | 점수 |
|---|---:|
| close > ma20 | +5 |
| ma20_slope_5 > 0 | +5 |
| ma60_slope_20 >= 0 | +5 |
| ma5 > ma20 또는 ma5 상향 전환 | +5 |
| dist_ma20 -1%~+6% | +5 |

### 14.3 거래량 20점

| 조건 | 점수 |
|---|---:|
| vol_rel20 1.3~3.5 | +6 |
| turnover_rel20 >= 1.3 | +4 |
| 최근 거래량 수축 후 증가 | +5 |
| 상승일 거래량 > 하락일 거래량 | +3 |
| vol_z20 0.8~2.8 | +2 |

### 14.4 과열 방지 15점

| 조건 | 점수 |
|---|---:|
| ret_5d <= 10% | +5 |
| ret_20d <= 25% | +5 |
| dist_ma20 <= 8% | +3 |
| RSI14 <= 70 | +2 |

### 14.5 현재 가격 entry_score

v2.0에서 추가하는 현재 가격 점수다.

| 조건 | 점수 조정 |
|---|---:|
| 신호가 대비 +0~3% | +10 |
| 신호가 대비 +3~4% | +0 |
| 신호가 대비 +4~6% | -15 |
| 신호가 대비 +6% 이상 | -30 |
| 저항까지 거리 < 3% | -20 |
| 저항까지 거리 3~5% | -10 |
| 기준선 이탈 | 후보 제외 |
| 손익비 < 1.2 | 후보 제외 또는 -30 |
| 손익비 1.2~1.5 | -15 |
| 손익비 >= 1.5 | +10 |

---

## 15. 머신러닝 설계 개요

### 15.1 머신러닝의 역할

StockMaster에서 머신러닝은 다음 역할을 한다.

```text
전체 종목에서 아무거나 예측하는 역할 X
룰 기반 필터를 통과한 후보 중 우선순위를 정하는 역할 O
```

추천 구조:

```text
Hard Filter
→ Rule Candidate Pattern
→ Rule Score
→ ML Re-ranker
→ Current Price Revalidation
→ Final Score / Status
```

### 15.2 왜 이렇게 해야 하는가

단순히 다음 5일 수익률을 예측하면 모델은 아래 종목을 좋아하기 쉽다.

```text
이미 급등한 종목
거래량이 이미 폭발한 종목
뉴스가 이미 반영된 종목
단기 과열이 큰 종목
```

따라서 머신러닝은 **위험한 후보를 제거하는 필터**가 아니라, **필터 통과 후보의 성공 가능성을 정렬하는 도구**로 제한한다.

### 15.3 추천 모델 구조

권장 구조:

```text
Stage 1: 룰 기반 후보 생성
Stage 2: ML target-first 확률 예측
Stage 3: ML risk 예측
Stage 4: 현재 가격 기반 entry_score 재계산
Stage 5: 최종 점수 결합
```

---

## 16. 머신러닝 라벨 설계

### 16.1 단순 D+5 수익률 라벨의 문제

나쁜 라벨 예시:

```python
label = future_close_5d / close - 1
```

문제:

```text
이미 오른 종목의 추가 상승을 과대평가할 수 있음
중간에 큰 하락이 있었는지 반영하지 못함
손익비가 나쁜 후보를 좋게 볼 수 있음
실제 3~5일 관찰 목적과 맞지 않음
```

### 16.2 Triple Barrier 라벨 권장

가장 권장하는 방식은 triple barrier다.

```text
상방 목표: +5%
하방 기준: -3%
기간 제한: 5거래일
```

라벨 정의:

```python
upper_barrier = reference_price * 1.05
lower_barrier = reference_price * 0.97
max_horizon = 5
```

```text
t+1~t+5 안에 upper_barrier를 먼저 터치 → label = 1
t+1~t+5 안에 lower_barrier를 먼저 터치 → label = 0
둘 다 터치하지 않음 → label = neutral 또는 0.5
```

구현 예시:

```python
def triple_barrier_label(df, idx, upper=0.05, lower=0.03, horizon=5):
    ref = df.loc[idx, "close"]
    upper_price = ref * (1 + upper)
    lower_price = ref * (1 - lower)

    for step in range(1, horizon + 1):
        row = df.iloc[idx + step]
        hit_up = row["high"] >= upper_price
        hit_down = row["low"] <= lower_price

        if hit_up and hit_down:
            # 같은 날 둘 다 닿은 경우 보수적으로 하방 우선 처리한다.
            return 0
        if hit_up:
            return 1
        if hit_down:
            return 0

    return 0.5
```

주의:

```text
같은 날 상방/하방 둘 다 터치한 경우 실제 순서를 알 수 없으면 보수적으로 실패 처리한다.
```

### 16.3 보조 라벨

ML 학습에는 여러 라벨을 함께 만든다.

| 라벨 | 목적 |
|---|---|
| target_first_label | +5%가 -3%보다 먼저 도달했는가 |
| max_return_5d | 5일 내 최대 상승률 |
| max_drawdown_5d | 5일 내 최대 불리한 변동 |
| close_return_5d | 5일 후 종가 수익률 |
| time_to_target | 목표 도달까지 걸린 일수 |
| neutral_label | 애매한 구간 구분 |

### 16.4 추천용 최종 목적함수

확률만 보지 말고 기대효용을 사용한다.

```python
expected_utility = (
    p_target_first * expected_reward
    - (1 - p_target_first) * expected_risk
    - cost_penalty
    - volatility_penalty
    - overheat_penalty
)
```

기본값:

```python
expected_reward = min(upside_to_resistance, 0.08)
expected_risk = min(risk_distance, 0.05)
cost_penalty = 0.003
volatility_penalty = max(0, atr_pct - 0.08) * 0.5
overheat_penalty = max(0, dist_ma20 - 0.08) * 0.8
```

---

## 17. 머신러닝 Feature 설계

모든 feature는 신호일 장마감 기준으로 계산한다.

### 17.1 가격/추세 Feature

```text
ret_1d, ret_3d, ret_5d, ret_10d, ret_20d
close / ma5 - 1
close / ma20 - 1
close / ma60 - 1
close / ma120 - 1
ma5 / ma20 - 1
ma20 / ma60 - 1
ma60 / ma120 - 1
ma20_slope_5
ma60_slope_20
ma120_slope_20
```

### 17.2 이평선 구조 Feature

```text
ma_alignment_code
ma_compression_5_20_60
ma_compression_20_60_120
is_close_above_ma20
is_close_above_ma60
is_ma5_cross_ma20_up
is_ma20_cross_ma60_up
```

`ma_alignment_code` 예시:

```text
3: ma5 > ma20 > ma60 > ma120
2: ma5 > ma20 and close > ma20
1: close > ma20 but alignment mixed
0: mixed/neutral
-1: close < ma20
-2: ma5 < ma20 < ma60 < ma120
```

### 17.3 거래량 Feature

```text
vol_rel20
vol_rel60
turnover_rel20
vol_z20
avg_volume_5 / median_volume_20
up_day_volume_ratio_5
down_day_volume_ratio_5
volume_contraction_then_expansion
```

### 17.4 캔들 Feature

```text
close_loc
body_ratio
upper_wick_ratio
lower_wick_ratio
is_good_candle
is_bad_upper_wick
is_distribution_day
```

### 17.5 변동성 Feature

```text
atr_pct
bb_width
bb_width_rank_120
realized_vol_5
realized_vol_20
high_low_range_pct
```

### 17.6 지지/저항 Feature

```text
upside_to_resistance
risk_distance
reward_risk_ratio
breakout_level_distance
nearest_support_distance
nearest_resistance_distance
```

### 17.7 상대강도 Feature

```text
stock_return_5d - market_return_5d
stock_return_20d - market_return_20d
sector_return_5d - market_return_5d
sector_return_20d - market_return_20d
sector_rank_20d
```

### 17.8 재무/공시 Feature

```text
debt_ratio
current_ratio
revenue_growth_yoy
operating_profit_positive
operating_cash_flow_positive
cash_runway_ratio
recent_dilution_event_count
recent_major_disclosure_risk
financial_risk_score
```

### 17.9 패턴 Feature

```text
pattern_pullback_resume
pattern_box_breakout
pattern_reversal_recovery
pattern_recovery_breakout
pattern_count
primary_pattern_code
rule_signal_score
```

### 17.10 현재 가격 재검증 Feature

이 feature는 신호일 학습용과 현재 상태 표시용을 분리한다.

학습용:

```text
signal_close_based_score
upside_to_resistance_at_signal
risk_distance_at_signal
```

현재 상태용:

```text
current_price / signal_close - 1
current_price / breakout_level - 1
current_upside_to_resistance
current_reward_risk_ratio
entry_status_code
entry_score
```

---

## 18. Feature Leakage 방지

### 18.1 금지 feature

아래 feature는 절대 학습 입력으로 넣지 않는다.

```text
future_return_1d
future_return_5d
future_high_5d
future_low_5d
target_first_label 계산 과정에서 나온 미래 hit 정보
공시일 이전의 재무제표 정보
수정주가 처리 오류로 미래 액면분할이 과거에 반영된 비정상 feature
```

### 18.2 날짜 기준 분할

랜덤 분할은 금지한다.

나쁜 방식:

```python
train_test_split(data, shuffle=True)
```

좋은 방식:

```text
2019~2022 train
2023 validation
2024 test
2025 out-of-sample
```

또는 walk-forward:

```text
Train: 2019~2021 → Validate: 2022 → Test: 2023Q1
Train: 2019~2022 → Validate: 2023Q1 → Test: 2023Q2
Train: 2019~2023Q1 → Validate: 2023Q2 → Test: 2023Q3
...
```

### 18.3 Purging / Embargo

3~5일 라벨은 기간이 겹치므로 검증 데이터 근처의 학습 샘플을 제거한다.

```text
라벨 horizon = 5거래일
검증 구간 직전 최소 5거래일 학습 샘플 제거
검증 구간 직후 최소 5거래일 embargo 적용
```

---

## 19. 모델 선택

### 19.1 1순위: LightGBM / CatBoost / XGBoost

추천 이유:

```text
표 형식 데이터에 강함
비선형 관계를 잘 잡음
feature importance/SHAP 해석 가능
결측 처리와 범주형 feature 처리가 상대적으로 편함
학습 속도가 빠름
```

기본 권장:

```text
1차: LightGBM binary classifier
2차: CatBoost classifier
3차: Logistic Regression baseline
```

### 19.2 반드시 유지할 Baseline

ML 모델이 좋아 보이더라도 항상 비교 기준을 유지한다.

```text
Rule-only score
Logistic regression
Simple momentum baseline
Random top-k baseline
```

ML이 rule-only보다 명확히 낫지 않으면 운영 반영하지 않는다.

### 19.3 추천하지 않는 초기 접근

초기 버전에서는 아래 접근을 피한다.

```text
딥러닝으로 일봉 전체 시퀀스 예측
전체 종목 대상 end-to-end 예측
뉴스/커뮤니티 텍스트까지 바로 결합
복잡한 강화학습 구조
```

이유:

```text
데이터 누수 통제가 어렵고 설명력이 떨어짐
운영 안정성 검증이 어려움
후행 모멘텀 과적합 가능성이 큼
```

---

## 20. 학습 데이터 구성

### 20.1 샘플 단위

```text
1개 샘플 = 종목 1개 × 날짜 1개
```

단, 전체 종목 전체 날짜를 바로 학습하지 않는다.

권장:

```text
하드 필터 통과
유동성 필터 통과
최소 하나의 차트 패턴 후보 조건 통과
```

이 후보군만 ML 학습 샘플로 사용한다.

### 20.2 Neutral 처리

Triple barrier에서 5일 내 상방/하방 모두 터치하지 않은 샘플은 애매하다.

옵션 A:

```text
neutral을 제외하고 0/1 이진분류
```

옵션 B:

```text
neutral = 0으로 처리하되 sample_weight 낮춤
```

옵션 C:

```text
3-class classification: fail / neutral / success
```

초기 추천:

```text
옵션 B 또는 C
```

### 20.3 Sample Weight

아래 조건으로 가중치를 조정한다.

```python
sample_weight = 1.0

if avg_turnover_20 < 5_000_000_000:
    sample_weight *= 0.8

if market_regime_bad:
    sample_weight *= 0.8

if label == 0.5:
    sample_weight *= 0.5

if pattern_box_breakout:
    sample_weight *= 1.1
```

목적:

```text
신뢰도 낮은 샘플의 영향 감소
애매한 neutral 샘플 영향 감소
중요 패턴 학습 강화
```

---

## 21. ML 학습 파이프라인

### 21.1 전체 파이프라인

```text
1. load_ohlcv()
2. load_market_actions()
3. load_financials_point_in_time()
4. build_universe()
5. calculate_features()
6. apply_hard_filters()
7. detect_patterns()
8. calculate_rule_score()
9. create_triple_barrier_labels()
10. split_walk_forward()
11. train_baseline_models()
12. train_gbdt_models()
13. calibrate_probabilities()
14. evaluate_top_k()
15. save_model_artifacts()
```

### 21.2 학습 의사코드

```python
def build_training_dataset(asof_dates):
    rows = []
    for date in asof_dates:
        universe = load_universe(date)
        ohlcv = load_ohlcv_until(date)
        financials = load_financials_point_in_time(date)
        market_actions = load_market_actions(date)

        features = calculate_features(universe, ohlcv, financials, market_actions, date)
        features = apply_hard_filters(features)
        features = apply_liquidity_filters(features)
        features = detect_patterns(features)
        features = features[features.any_pattern_pass]
        features["rule_score"] = calculate_rule_score(features)
        features["label"] = create_triple_barrier_labels(features, date)
        rows.append(features)

    return concat(rows)
```

### 21.3 모델 학습 의사코드

```python
train_df, valid_df, test_df = walk_forward_split(dataset)

X_train = train_df[feature_cols]
y_train = train_df["target_first_label"]
w_train = train_df["sample_weight"]

model = LightGBMClassifier(
    n_estimators=500,
    learning_rate=0.03,
    max_depth=5,
    num_leaves=31,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_samples=50,
    reg_alpha=0.1,
    reg_lambda=1.0,
)

model.fit(
    X_train,
    y_train,
    sample_weight=w_train,
    eval_set=[(valid_df[feature_cols], valid_df["target_first_label"])],
    callbacks=[early_stopping(50)],
)
```

### 21.4 확률 보정

모델 확률은 그대로 믿지 않는다. 반드시 calibration을 수행한다.

```text
Platt scaling
Isotonic regression
Validation 기간별 calibration curve 확인
```

```python
calibrated_model = CalibratedClassifierCV(model, method="isotonic", cv="prefit")
calibrated_model.fit(valid_X, valid_y)
```

---

## 22. ML 평가 기준

일반 accuracy는 중요도가 낮다. 추천 엔진은 상위 후보 품질이 중요하다.

### 22.1 핵심 평가 지표

| 지표 | 의미 |
|---|---|
| Top-K Hit Rate | 상위 K개 중 target-first 성공 비율 |
| Precision@K | 추천 상위 K개의 성공률 |
| Avg Forward Return@K | 상위 K개의 5일 평균 수익률 |
| Median Return@K | 평균 왜곡 방지 |
| Max Adverse Excursion@K | 추천 후 최대 불리한 변동 |
| Target-before-Stop Ratio | +5%가 -3%보다 먼저 나온 비율 |
| Calibration Error | 확률 예측 신뢰도 |
| Pattern-wise Performance | 패턴별 성능 |
| Regime-wise Performance | 시장 상태별 성능 |

### 22.2 반드시 분리해서 평가할 구간

```text
상승장
횡보장
하락장
코스닥 강세장
코스닥 약세장
거래대금 증가장
거래대금 감소장
```

### 22.3 패턴별 평가

각 패턴은 따로 평가한다.

```text
A. 20일선 눌림 후 재상승형
B. 박스권 압축 후 첫 돌파형
C. 역배열 개선형
D. 회복형 돌파형
```

출력 예시:

| 패턴 | Precision@10 | Avg Return 5D | MAE | 추천 수 |
|---|---:|---:|---:|---:|
| A | 0.57 | +2.1% | -2.4% | 1,230 |
| B | 0.61 | +2.8% | -2.8% | 840 |
| C | 0.44 | +0.9% | -3.7% | 620 |
| D | 0.53 | +1.8% | -3.0% | 710 |

C패턴이 낮으면 비중을 줄이고, B패턴이 높으면 선행 후보 가중치를 올린다.

### 22.4 Rule-only와 비교

ML은 반드시 rule-only보다 나아야 한다.

```text
Rule-only Top 10 성공률: 52%
ML-rerank Top 10 성공률: 58%
```

이런 식으로 개선이 검증되어야 한다.

개선이 없으면:

```text
ML을 후보 필터로 쓰지 말고 설명 보조/리스크 보조로만 사용한다.
```

---

## 23. 최종 점수 결합

최종 추천 점수는 룰 점수, ML 점수, 현재 가격 점수를 결합한다.

```python
final_score = (
    0.40 * rule_signal_score
    + 0.35 * ml_probability_score
    + 0.25 * entry_score
)
```

### 23.1 ML 확률 점수 변환

```python
ml_probability_score = min(max((p_target_first - 0.40) / 0.30 * 100, 0), 100)
```

해석:

```text
p_target_first 0.40 이하는 0점에 가깝게
p_target_first 0.70 이상은 높은 점수
```

### 23.2 최종 상태 결정

```python
if hard_filter_failed:
    final_status = "REJECTED"
elif invalidated:
    final_status = "INVALIDATED"
elif entry_status == "TARGET_ZONE_REACHED":
    final_status = "TARGET_ZONE_REACHED"
elif final_score >= 80 and entry_status == "VALID":
    final_status = "HIGH_CONFIDENCE"
elif final_score >= 70 and entry_status in ["VALID", "WATCH_CAUTION"]:
    final_status = "CANDIDATE"
elif entry_status == "CHASE_RISK":
    final_status = "CHASE_RISK"
else:
    final_status = "WATCHLIST"
```

---

## 24. 추천 결과 출력 스키마

StockMaster의 결과는 설명 가능해야 한다.

```json
{
  "symbol": "241710",
  "name": "코스메카코리아",
  "signal_date": "YYYY-MM-DD",
  "signal_close": 85500,
  "current_price": 90600,
  "pattern_tags": ["RECOVERY_BREAKOUT", "MA20_RECLAIM", "VOLUME_EXPANSION"],
  "signal_score": 84.5,
  "entry_score": 48.0,
  "ml_probability_target_first": 0.61,
  "expected_utility": 0.018,
  "final_score": 72.4,
  "status": "CHASE_RISK",
  "key_levels": {
    "breakout_level": 87500,
    "nearest_support": 85500,
    "nearest_resistance": 93600,
    "invalidation_price": 84200
  },
  "risk_flags": [
    "SIGNAL_PRICE_ALREADY_UP_5PCT",
    "NEAR_RESISTANCE"
  ],
  "positive_reasons": [
    "20일선 회복",
    "거래량 증가",
    "박스 상단 돌파"
  ],
  "negative_reasons": [
    "신호가 대비 현재가 상승폭 큼",
    "1차 저항까지 거리 축소"
  ]
}
```

---

## 25. 설명 생성 규칙

설명은 사용자가 왜 추천됐는지, 왜 현재는 주의인지 이해할 수 있어야 한다.

### 25.1 설명 템플릿

```text
[종목명]은 신호일 기준 [패턴명] 조건을 충족했습니다.
주요 근거는 [이평선 구조], [거래량], [캔들], [섹터/시장]입니다.
다만 현재가는 신호가 대비 [x]% 상승하여 [entry_status] 상태입니다.
가까운 저항은 [가격] 부근이며, 기준선은 [가격] 부근입니다.
따라서 현재 상태는 [최종 상태]로 분류합니다.
```

### 25.2 상태별 문구

| 상태 | 설명 |
|---|---|
| HIGH_CONFIDENCE | 신호와 현재 가격 모두 양호 |
| CANDIDATE | 조건은 대체로 충족하지만 일부 점검 필요 |
| WATCH_CAUTION | 후보는 유효하나 가격 부담이 일부 있음 |
| CHASE_RISK | 신호 이후 이미 많이 상승해 신규 후보 신뢰도 하락 |
| TARGET_ZONE_REACHED | 3~5일 기준 목표권에 도달했을 가능성 |
| INVALIDATED | 기준선 이탈로 신호 무효 |
| REJECTED | 하드 필터 또는 리스크 필터로 제외 |

---

## 26. 코스메카코리아 사례를 일반화한 개선 로직

아래는 특정 종목에 종속되지 않는 일반 규칙이다.

### 26.1 신호일에는 좋은 후보일 수 있음

다음 조건이면 신호일 후보로 인정한다.

```text
주요 이평선 회복
거래량 증가
박스 상단 또는 최근 고점 돌파
윗꼬리 과도하지 않음
하드 필터 통과
```

### 26.2 다음 날 현재 가격은 다시 평가해야 함

```python
move = current_price / signal_close - 1

if move <= 0.03:
    entry_score_add = 10
elif move <= 0.04:
    entry_score_add = 0
elif move <= 0.06:
    entry_score_add = -15
else:
    entry_score_add = -30
```

### 26.3 현재가가 이미 1차 목표권이면 상태 변경

```python
if current_price >= signal_close * 1.05:
    status = "TARGET_ZONE_REACHED"
```

### 26.4 저항이 가까우면 신규 후보 점수 하향

```python
if upside_to_resistance < 0.04:
    entry_score -= 10
if upside_to_resistance < 0.03:
    entry_score -= 20
```

### 26.5 신호는 성공, 현재 신규 후보는 주의가 가능해야 함

StockMaster는 아래 두 판단을 동시에 할 수 있어야 한다.

```text
신호일 추천은 적절했다.
하지만 현재 가격에서는 추격 위험이 있다.
```

이 구분이 없으면 사용자는 좋은 신호를 늦게 확인하고도 같은 품질의 후보로 오해할 수 있다.

---

## 27. Codex 구현 지시사항

### 27.1 추천 디렉터리 구조

```text
stockmaster/
  data/
    ohlcv_loader.py
    market_action_loader.py
    financial_loader.py
    sector_loader.py
  features/
    technical_features.py
    volume_features.py
    candle_features.py
    financial_features.py
    relative_strength_features.py
  filters/
    hard_filters.py
    liquidity_filters.py
    financial_filters.py
    overheat_filters.py
  patterns/
    pullback_resume.py
    box_breakout.py
    reversal_recovery.py
    recovery_breakout.py
  scoring/
    rule_score.py
    entry_score.py
    final_score.py
    explanation.py
  ml/
    labels.py
    dataset.py
    split.py
    train.py
    calibrate.py
    evaluate.py
    infer.py
  validation/
    backtest_like_evaluation.py
    leakage_checks.py
    data_quality_checks.py
  config/
    filters.yaml
    scoring.yaml
    ml.yaml
```

### 27.2 핵심 함수

```python
def calculate_technical_features(df):
    pass


def apply_hard_filters(df):
    pass


def detect_candidate_patterns(df):
    pass


def calculate_rule_signal_score(df):
    pass


def create_triple_barrier_labels(df, upper=0.05, lower=0.03, horizon=5):
    pass


def train_ml_ranker(train_df, valid_df, feature_cols):
    pass


def calculate_entry_score(signal_row, current_price):
    pass


def determine_recommendation_status(row):
    pass


def generate_explanation(row):
    pass
```

### 27.3 `entry_score.py` 구현 예시

```python
def calculate_entry_score(
    signal_close: float,
    current_price: float,
    nearest_resistance: float | None,
    invalidation_price: float | None,
    base_score: float = 50.0,
) -> dict:
    score = base_score
    flags = []

    move = current_price / signal_close - 1

    if move >= 0.08:
        score -= 35
        status = "EXTENDED"
        flags.append("EXTENDED_FROM_SIGNAL")
    elif move >= 0.06:
        score -= 30
        status = "TARGET_ZONE_REACHED"
        flags.append("TARGET_ZONE_REACHED")
    elif move >= 0.04:
        score -= 15
        status = "CHASE_RISK"
        flags.append("CHASE_RISK")
    elif move >= 0.03:
        status = "WATCH_CAUTION"
        flags.append("PRICE_MOVED_FROM_SIGNAL")
    else:
        score += 10
        status = "VALID"

    if invalidation_price is not None and current_price < invalidation_price:
        return {
            "entry_score": 0,
            "entry_status": "INVALIDATED",
            "flags": ["INVALIDATED_BY_PRICE"],
        }

    if nearest_resistance is not None and nearest_resistance > current_price:
        upside = nearest_resistance / current_price - 1
        if upside < 0.03:
            score -= 20
            flags.append("NEAR_RESISTANCE_LT_3PCT")
        elif upside < 0.05:
            score -= 10
            flags.append("NEAR_RESISTANCE_LT_5PCT")
    else:
        flags.append("NO_CLEAR_RESISTANCE")

    score = max(0, min(100, score))

    return {
        "entry_score": score,
        "entry_status": status,
        "flags": flags,
    }
```

### 27.4 `labels.py` 구현 예시

```python
def create_triple_barrier_label_for_symbol(
    symbol_df,
    upper: float = 0.05,
    lower: float = 0.03,
    horizon: int = 5,
):
    labels = []
    n = len(symbol_df)

    for i in range(n):
        if i + horizon >= n:
            labels.append(None)
            continue

        ref = symbol_df.iloc[i]["close"]
        up = ref * (1 + upper)
        down = ref * (1 - lower)
        label = 0.5
        hit_day = None

        for step in range(1, horizon + 1):
            row = symbol_df.iloc[i + step]
            hit_up = row["high"] >= up
            hit_down = row["low"] <= down

            if hit_up and hit_down:
                label = 0
                hit_day = step
                break
            if hit_up:
                label = 1
                hit_day = step
                break
            if hit_down:
                label = 0
                hit_day = step
                break

        labels.append({
            "target_first_label": label,
            "hit_day": hit_day,
        })

    return labels
```

---

## 28. YAML 설정 예시

```yaml
universe:
  min_price: 1000
  min_market_cap: 100000000000
  min_avg_turnover_20: 2000000000
  min_turnover_today: 2000000000
  exclude_new_listing_days: 60

hard_filters:
  exclude_management_issue: true
  exclude_trading_halt: true
  exclude_investment_warning: true
  exclude_investment_risk: true
  exclude_short_term_overheated: true
  exclude_capital_impairment: true
  require_normal_audit_opinion: true

financial:
  max_debt_ratio_general: 300
  min_current_ratio_general: 80
  min_revenue_ttm_general: 50000000000
  min_cash_runway_growth: 1.5

technical:
  ma_windows: [5, 20, 60, 120]
  max_dist_ma20: 0.10
  max_dist_ma60: 0.20
  preferred_dist_ma20_min: -0.01
  preferred_dist_ma20_max: 0.06
  max_ret_5d: 0.12
  max_ret_20d: 0.30

volume:
  min_vol_rel20: 1.3
  max_vol_rel20: 3.5
  min_turnover_rel20: 1.3
  min_vol_z20: 0.8
  max_vol_z20: 2.8
  breakout_min_vol_rel20: 1.7
  breakout_max_vol_rel20: 4.0

candle:
  min_close_loc: 0.65
  min_body_ratio: 0.35
  max_upper_wick_ratio: 0.30
  bad_upper_wick_ratio: 0.45

entry_revalidation:
  watch_caution_from_signal: 0.03
  chase_risk_from_signal: 0.04
  target_zone_from_signal: 0.06
  extended_from_signal: 0.08
  min_upside_to_resistance: 0.04
  min_reward_risk_ratio: 1.5
  max_risk_distance: 0.05

ml:
  label:
    upper_barrier: 0.05
    lower_barrier: 0.03
    horizon: 5
    same_day_both_hit: "down_first"
  model:
    type: "lightgbm"
    n_estimators: 500
    learning_rate: 0.03
    max_depth: 5
    num_leaves: 31
    subsample: 0.8
    colsample_bytree: 0.8
    min_child_samples: 50
    reg_alpha: 0.1
    reg_lambda: 1.0
  scoring:
    rule_weight: 0.40
    ml_weight: 0.35
    entry_weight: 0.25
```

---

## 29. 테스트 기준

### 29.1 단위 테스트

필수 테스트:

```text
이동평균 계산 정확성
vol_rel20 계산 정확성
캔들 close_loc 계산 정확성
하드 필터 제외 동작
과열 필터 제외 동작
패턴 감지 동작
triple barrier 라벨 동작
entry_status 전환 동작
최종 점수 결합 동작
```

### 29.2 누수 검사

```python
def test_no_future_features(feature_df):
    forbidden_cols = [
        "future_return_5d",
        "future_high_5d",
        "future_low_5d",
        "target_first_label",
    ]
    for col in forbidden_cols:
        assert col not in model_feature_cols
```

### 29.3 추천 상태 테스트

```text
신호가 대비 +2% → VALID
신호가 대비 +3.5% → WATCH_CAUTION
신호가 대비 +5% → CHASE_RISK 또는 TARGET_ZONE_REACHED 설정 확인
신호가 대비 +7% → TARGET_ZONE_REACHED
기준선 이탈 → INVALIDATED
저항까지 2% → entry_score 감점
```

---

## 30. 운영 판단 기준

### 30.1 추천 후보 기준

```text
하드 필터 통과
유동성 필터 통과
재무/공시 리스크 통과
최소 하나의 패턴 통과
rule_signal_score >= 70
p_target_first >= 0.50
entry_status in [VALID, WATCH_CAUTION]
final_score >= 70
```

### 30.2 높은 신뢰도 후보 기준

```text
rule_signal_score >= 80
p_target_first >= 0.58
entry_score >= 70
final_score >= 80
reward_risk_ratio >= 1.5
upside_to_resistance >= 4%
```

### 30.3 제외 기준

```text
entry_status in [CHASE_RISK, TARGET_ZONE_REACHED, EXTENDED] 이고 신규 후보 화면일 경우 제외 또는 별도 상태 표시
하드 필터 실패
기준선 이탈
저항까지 거리 3% 미만
손익비 1.2 미만
vol_rel20 > 5 + 윗꼬리 과다
ret_5d > 15% + dist_ma20 > 10%
```

---

## 31. 가장 중요한 결론

StockMaster v2.0의 핵심은 다음이다.

```text
1. 룰 기반 필터로 위험·과열·부실·비유동 종목을 먼저 제거한다.
2. 차트 패턴은 20일선 눌림 후 재상승, 박스 압축 후 돌파, 회복형 돌파를 중심으로 한다.
3. 거래량은 평균보다 중앙값 대비 비율과 z-score를 함께 사용한다.
4. 머신러닝은 전체 종목 예측이 아니라 필터 통과 후보의 재정렬에 사용한다.
5. 라벨은 단순 5일 수익률이 아니라 triple barrier를 사용한다.
6. 신호일 점수와 현재 가격 기준 entry_score를 분리한다.
7. 신호 이후 이미 +4~6% 움직인 종목은 추격 위험 또는 목표권 도달 상태로 바꾼다.
8. 모델 점수는 하드 필터를 절대 override하지 못한다.
9. 평가는 random split이 아니라 walk-forward, purged split, top-k 성능으로 한다.
10. 최종 출력은 점수뿐 아니라 이유, 위험 플래그, 주요 가격대, 상태를 함께 제공한다.
```

한 줄 요약:

```text
StockMaster는 “이미 오른 종목을 맞히는 모델”이 아니라, “위험한 후보를 제거하고, 아직 손익비가 살아 있는 초기 수급 후보를 신뢰도 순으로 정렬하는 시스템”이어야 한다.
```
