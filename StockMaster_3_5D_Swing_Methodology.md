# StockMaster 3~5일 보유형 종목 추천 방법론 v1.0

> 목적: StockMaster를 **국내주식 3~5거래일 보유형 종목 추천/분석 엔진**으로 설계한다.  
> 핵심 원칙: 모델이 전체 종목에서 바로 고르게 하지 않는다. 먼저 룰 기반 거름망으로 위험·과열·비유동·부실 종목을 제거하고, 남은 후보만 점수화/랭킹한다.

---

## 0. 설계 철학

### 0.1 문제 정의

기존 학습형 추천 시스템은 다음 문제가 생기기 쉽다.

- 이미 많이 오른 종목을 선호한다.
- 거래량 폭발 후 늦은 위치를 추천한다.
- 단기 수익률 라벨을 학습하면서 과열 모멘텀을 좋은 패턴으로 오해한다.
- 재무/공시 리스크가 있는 종목도 차트만 좋으면 추천할 수 있다.
- 시장 약세 구간에서도 동일한 기준으로 후보를 낸다.

따라서 StockMaster는 다음 구조로 설계한다.

```text
전체 종목
→ 하드 제외 필터
→ 유동성 필터
→ 재무/공시 리스크 필터
→ 시장/섹터 환경 필터
→ 차트 구조 필터
→ 거래량/캔들 트리거 필터
→ 과열 방지 필터
→ 점수화
→ 후보 랭킹
→ 설명 생성
```

### 0.2 가장 중요한 원칙

```text
추천할 종목을 찾기 전에, 추천하면 안 되는 종목을 먼저 제거한다.
```

3~5일 보유형 후보의 핵심은 다음이다.

```text
아직 과열되지 않았고,
하락 리스크가 제한적이며,
최근 수급이 처음 들어오기 시작했고,
20일선 또는 박스권 구조상 위로 열려 있는 종목.
```

---

## 1. 데이터 소스 기준

### 1.1 우선 사용 데이터

| 데이터 종류 | 권장 소스 | 사용 목적 |
|---|---|---|
| 일별 OHLCV | KRX 또는 신뢰 가능한 국내 시세 데이터 | 이동평균, 거래량, 캔들, 수익률 계산 |
| 거래대금 | KRX 또는 시세 데이터 | 유동성 필터 |
| 시장조치 | KRX/KIND | 관리종목, 투자주의, 투자경고, 투자위험, 단기과열, 거래정지 등 제외 |
| 공시 | DART/OpenDART | 감사의견, 자본잠식, 유상증자, CB/BW, 최대주주 변경, 횡령/배임 등 확인 |
| 재무제표 | DART/OpenDART | 부채비율, 유동비율, 자본총계, 매출, 이익, 현금흐름 계산 |
| 지수/섹터 | KRX 또는 내부 분류 | 시장 레짐, 섹터 상대강도 계산 |

### 1.2 공식 참고 링크

- KRX Data Marketplace: https://data.krx.co.kr/
- KRX KIND 투자유의사항: https://kind.krx.co.kr/
- DART: https://dart.fss.or.kr/
- OpenDART: https://opendart.fss.or.kr/

### 1.3 데이터 신뢰도 규칙

데이터가 불완전하면 보수적으로 처리한다.

```text
시장조치 데이터 누락 → 후보 제외
일봉 데이터 누락 → 후보 제외
최근 재무제표 누락 → 재무 점수 하향 또는 후보 제외
감사의견 확인 불가 → 후보 제외
상장일 확인 불가 → 후보 제외
공시 리스크 확인 불가 → 후보 제외
```

StockMaster는 애매한 종목을 억지로 추천하지 않는다.

---

## 2. 기본 용어와 계산식

### 2.1 가격/거래량 기본값

각 종목 `s`, 기준일 `t`에 대해 다음 값을 계산한다.

```python
open_t
high_t
low_t
close_t
volume_t
turnover_t = close_t * volume_t
```

거래대금 데이터가 별도로 있으면 별도 필드를 우선 사용한다.

```python
turnover_t = official_turnover_t if available else close_t * volume_t
```

### 2.2 이동평균

```python
ma5   = SMA(close, 5)
ma20  = SMA(close, 20)
ma60  = SMA(close, 60)
ma120 = SMA(close, 120)
```

### 2.3 이동평균 기울기

```python
ma20_slope_5  = ma20_t / ma20_t_minus_5 - 1
ma60_slope_20 = ma60_t / ma60_t_minus_20 - 1
ma120_slope_20 = ma120_t / ma120_t_minus_20 - 1
```

권장 해석:

| 값 | 의미 |
|---|---|
| `ma20_slope_5 > 0` | 단기 스윙 추세 양호 |
| `ma60_slope_20 >= -0.005` | 중기 추세가 크게 무너지지 않음 |
| `ma60_slope_20 < -0.02` | 중기 추세 악화 |

### 2.4 이격도

```python
dist_ma5   = close_t / ma5 - 1
dist_ma20  = close_t / ma20 - 1
dist_ma60  = close_t / ma60 - 1
dist_ma120 = close_t / ma120 - 1
```

3~5일 보유형에서 가장 중요한 값은 `dist_ma20`이다.

권장 범위:

```text
좋은 후보: -1% <= dist_ma20 <= +6%
주의:      +6% < dist_ma20 <= +10%
제외:      dist_ma20 > +10%
```

### 2.5 수익률

```python
ret1  = close_t / close_t_minus_1 - 1
ret3  = close_t / close_t_minus_3 - 1
ret5  = close_t / close_t_minus_5 - 1
ret10 = close_t / close_t_minus_10 - 1
ret20 = close_t / close_t_minus_20 - 1
```

과열 방지 기본값:

```text
ret3  > +12% → 제외 또는 강한 감점
ret5  > +15% → 제외
ret10 > +25% → 제외
ret20 > +40% → 제외
```

보수적 기본값:

```text
ret5  <= +10%
ret20 <= +25%
```

### 2.6 거래량 상대값

거래량은 평균보다 중앙값을 우선 사용한다. 급등일 하나가 평균을 왜곡할 수 있기 때문이다.

```python
median_volume_20 = median(volume over last 20 trading days excluding today if needed)
median_volume_60 = median(volume over last 60 trading days)
median_turnover_20 = median(turnover over last 20 trading days)

vol_rel20 = volume_t / median_volume_20
vol_rel60 = volume_t / median_volume_60
turnover_rel20 = turnover_t / median_turnover_20
```

로그 거래량 z-score도 같이 사용한다.

```python
log_vol_t = log(volume_t)
vol_z20 = (log_vol_t - mean(log(volume over last 20 days))) / std(log(volume over last 20 days))
```

권장 해석:

| 조건 | 해석 |
|---|---|
| `vol_rel20 < 1.0` | 아직 수급 약함 |
| `1.3 <= vol_rel20 <= 3.5` | 3~5일 후보에 적합한 수급 증가 |
| `1.7 <= vol_rel20 <= 4.0` | 박스 돌파형에 적합 |
| `vol_rel20 > 5.0` | 과열 또는 뉴스 추격 위험 |

### 2.7 캔들 위치

```python
range_t = high_t - low_t
close_loc = (close_t - low_t) / range_t
upper_wick_ratio = (high_t - max(open_t, close_t)) / range_t
lower_wick_ratio = (min(open_t, close_t) - low_t) / range_t
body_ratio = abs(close_t - open_t) / range_t
```

`range_t == 0`이면 캔들 필터 계산을 생략하고 신뢰도 점수를 낮춘다.

좋은 캔들 기준:

```text
close_loc >= 0.65
body_ratio >= 0.35
upper_wick_ratio <= 0.30
```

나쁜 캔들 기준:

```text
vol_rel20 >= 2.0
upper_wick_ratio >= 0.45
close_loc <= 0.55
```

해석:

```text
거래량은 터졌지만 종가가 위에서 버티지 못한 구조.
3~5일 후보에서는 강한 감점 또는 제외.
```

---

## 3. 하드 제외 필터

하드 제외 필터에 걸린 종목은 점수 계산 없이 제거한다.

### 3.1 종목 유형 필터

기본 StockMaster 추천 대상은 보통주 중심이다.

```python
universe_type_pass = (
    is_common_stock
    and not is_preferred_stock
    and not is_etf
    and not is_etn
    and not is_elw
    and not is_spac
    and not is_reit
)
```

단, 별도 전략에서 ETF나 리츠를 다루고 싶으면 분리된 모듈로 만든다. 기본 종목 랭킹에 섞지 않는다.

### 3.2 시장조치 필터

다음 항목은 제외한다.

```python
market_risk_pass = (
    not is_management_issue
    and not is_trading_halt
    and not is_delisting_review
    and not is_unfaithful_disclosure
    and not is_investment_warning
    and not is_investment_risk
    and not is_short_term_overheated
)
```

`투자주의`는 모드에 따라 처리한다.

```python
if strict_mode:
    exclude if is_investment_caution
else:
    penalty if is_investment_caution
```

권장 기본값:

```python
strict_mode = True
```

고신뢰 추천에서는 `투자주의`도 제외하는 것을 기본으로 한다.

### 3.3 상장 기간 필터

신규 상장 종목은 변동성이 크고 과거 데이터가 부족하다.

```python
ipo_age_pass = listed_trading_days >= 60
```

보수적 기본값:

```python
listed_trading_days >= 120
```

신규 상장 전용 분석은 별도 모듈로 분리한다.

---

## 4. 유동성 필터

3~5일 보유형 추천에서는 유동성이 매우 중요하다. 유동성이 낮으면 차트가 좋아도 실제 후보로 부적합하다.

### 4.1 기본 유동성 조건

```python
liquidity_pass = (
    close_t >= 1000
    and market_cap >= 100_000_000_000
    and avg_turnover_20 >= 2_000_000_000
    and median_turnover_20 >= 1_500_000_000
    and avg_volume_20 >= 50_000
)
```

보수적 설정:

```python
liquidity_pass_strict = (
    close_t >= 2000
    and market_cap >= 200_000_000_000
    and avg_turnover_20 >= 5_000_000_000
    and median_turnover_20 >= 3_000_000_000
    and avg_volume_20 >= 100_000
)
```

### 4.2 권장 기본값

처음에는 아래 기준을 사용한다.

```yaml
price_min_krw: 1000
market_cap_min_krw: 100000000000
avg_turnover_20_min_krw: 2000000000
median_turnover_20_min_krw: 1500000000
avg_volume_20_min: 50000
```

더 안정적인 후보만 원하면 다음 기준을 사용한다.

```yaml
price_min_krw: 2000
market_cap_min_krw: 200000000000
avg_turnover_20_min_krw: 5000000000
median_turnover_20_min_krw: 3000000000
avg_volume_20_min: 100000
```

### 4.3 계좌 규모 대비 유동성 안전장치

추천 결과에 `liquidity_note`를 추가한다.

```python
liquidity_capacity_ratio = user_reference_amount / turnover_t
```

권장 해석:

| 값 | 해석 |
|---|---|
| `<= 0.002` | 매우 양호 |
| `0.002 ~ 0.005` | 양호 |
| `0.005 ~ 0.010` | 주의 |
| `> 0.010` | 후보 제외 권장 |

사용자 금액을 받지 않는 경우 기본 기준금액을 300만 원 또는 500만 원으로 둔다.

```yaml
reference_amount_krw: 3000000
max_liquidity_capacity_ratio: 0.005
```

---

## 5. 재무제표/공시 리스크 필터

재무제표는 3~5일 수익률을 직접 맞히기 위한 지표가 아니다. StockMaster에서는 주로 위험 종목 제거용으로 사용한다.

```text
재무제표의 역할 = 좋은 종목 선별보다, 위험한 종목 배제.
```

### 5.1 공통 하드 제외

다음 조건은 업종과 무관하게 제외한다.

```python
financial_hard_pass = (
    equity > 0
    and not capital_impairment
    and audit_opinion_is_normal
    and not going_concern_warning
    and not recent_embezzlement_or_breach_of_trust
)
```

### 5.2 일반 기업 기준

일반 제조/서비스/소비재/IT 기업의 기본 기준이다.

```python
general_financial_pass = (
    equity > 0
    and debt_ratio <= 300
    and current_ratio >= 80
    and revenue_ttm >= 30_000_000_000
    and audit_opinion_is_normal
    and not capital_impairment
)
```

보수적 기준:

```python
general_financial_pass_strict = (
    equity > 0
    and debt_ratio <= 200
    and current_ratio >= 100
    and revenue_ttm >= 50_000_000_000
    and operating_profit_ttm > 0
    and audit_opinion_is_normal
    and not capital_impairment
)
```

### 5.3 성장주/바이오/연구개발형 기업 기준

바이오와 연구개발형 기업은 영업손실만으로 일괄 제외하면 후보가 지나치게 줄어든다. 대신 현금 여력과 자본 안정성을 본다.

```python
growth_financial_pass = (
    equity > 0
    and audit_opinion_is_normal
    and not capital_impairment
    and cash_and_equivalents > 0
    and not recent_embezzlement_or_breach_of_trust
)
```

가능하면 현금 소진 여력을 계산한다.

```python
if operating_cash_flow_ttm < 0:
    cash_runway_months = cash_and_equivalents / abs(operating_cash_flow_ttm) * 12
else:
    cash_runway_months = 999
```

권장 기준:

```text
cash_runway_months >= 12 → 통과
cash_runway_months 6~12 → 감점
cash_runway_months < 6 → 제외 또는 강한 감점
```

### 5.4 금융업 예외

은행, 보험, 증권 등 금융업은 부채비율과 유동비율이 일반 기업과 다르게 해석된다.

금융업은 다음 필터를 별도로 사용한다.

```python
financial_sector_pass = (
    equity > 0
    and audit_opinion_is_normal
    and not capital_impairment
    and not recent_major_bad_disclosure
)
```

금융업의 재무 점수는 부채비율 대신 다음 지표를 우선한다.

```text
자본총계 증가 여부
순이익 안정성
연체/부실 관련 공시
배당 안정성
감사의견
```

### 5.5 공시 리스크 감점

최근 공시는 점수화한다.

| 공시/이벤트 | 기간 | 처리 |
|---|---:|---|
| 횡령/배임 | 최근 2년 | 제외 |
| 감사의견 비적정/한정/의견거절 | 최근 보고서 | 제외 |
| 자본잠식 | 최근 보고서 | 제외 |
| 대규모 유상증자 | 최근 180일 | 강한 감점 또는 제외 |
| CB/BW 발행 | 최근 180일 | 감점 |
| 최대주주 변경 | 최근 180일 | 감점 |
| 불성실공시법인 | 현재 지정 | 제외 |
| 소송/제재 중대 공시 | 최근 180일 | 감점 또는 제외 |

기본 감점 예시:

```python
if recent_paid_in_capital_increase_180d:
    financial_score -= 5
if recent_cb_bw_180d:
    financial_score -= 3
if recent_major_shareholder_change_180d:
    financial_score -= 5
if recent_lawsuit_or_sanction_180d:
    financial_score -= 3
```

---

## 6. 시장/섹터 환경 필터

3~5일 보유형 후보는 시장 환경의 영향을 크게 받는다. 시장이 약하면 추천 수를 줄이고 기준을 더 엄격하게 한다.

### 6.1 시장 레짐 계산

KOSPI, KOSDAQ 각각 계산한다.

```python
index_ma5 = SMA(index_close, 5)
index_ma20 = SMA(index_close, 20)
index_ma60 = SMA(index_close, 60)

index_ma20_slope_5 = index_ma20_t / index_ma20_t_minus_5 - 1
```

시장 상태:

```python
market_strong = index_close_t > index_ma20 and index_ma20_slope_5 > 0
market_neutral = index_close_t > index_ma20 or index_ma20_slope_5 > 0
market_weak = index_close_t < index_ma20 and index_ma20_slope_5 < 0
```

### 6.2 시장 상태별 후보 수

```yaml
max_candidates_when_market_strong: 15
max_candidates_when_market_neutral: 8
max_candidates_when_market_weak: 3
```

시장 약세일 때는 다음 조건을 강화한다.

```text
시장 약세 시:
- score_threshold +5점
- vol_rel20 하한 +0.2
- ret5 과열 기준 더 엄격 적용
- 재무 리스크 감점 확대
- 역배열 개선형 후보 비중 축소
```

### 6.3 섹터 상대강도

종목의 섹터가 시장보다 강한지 확인한다.

```python
sector_ret5 = sector_index_close_t / sector_index_close_t_minus_5 - 1
sector_ret20 = sector_index_close_t / sector_index_close_t_minus_20 - 1
market_ret5 = market_index_close_t / market_index_close_t_minus_5 - 1
market_ret20 = market_index_close_t / market_index_close_t_minus_20 - 1

sector_rs_5 = sector_ret5 - market_ret5
sector_rs_20 = sector_ret20 - market_ret20
```

좋은 조건:

```text
sector_rs_5 > 0
sector_rs_20 > 0
sector_rank_20 <= 상위 30%
```

점수화:

```python
if sector_rs_5 > 0: sector_score += 3
if sector_rs_20 > 0: sector_score += 4
if sector_rank_20 <= 0.30: sector_score += 3
```

---

## 7. 차트 구조 필터

StockMaster의 핵심 패턴은 세 가지다.

```text
A. 20일선 눌림 후 재상승형
B. 박스권 압축 후 첫 돌파형
C. 역배열 개선형
```

우선순위:

```text
A > B > C
```

C는 보조 후보로만 사용한다.

---

## 8. 패턴 A: 20일선 눌림 후 재상승형

### 8.1 의도

상승 추세가 완전히 무너지지 않은 종목이 20일선 근처까지 쉬었다가 다시 힘을 받는 구조를 찾는다.

### 8.2 필수 조건

```python
pattern_pullback_pass = (
    close_t > ma20 * 0.99
    and close_t <= ma20 * 1.06
    and ma20_slope_5 > 0
    and ma60_slope_20 >= -0.005
    and ret5 <= 0.10
    and ret20 <= 0.25
    and close_t > ma5
    and 45 <= rsi14 <= 65
)
```

### 8.3 눌림 조건

최근 고점 대비 적당히 쉬었는지 확인한다.

```python
high_10 = max(high over last 10 days)
drawdown_from_high_10 = close_t / high_10 - 1
```

좋은 범위:

```text
-12% <= drawdown_from_high_10 <= -3%
```

너무 안 쉬었으면 과열이고, 너무 많이 빠졌으면 추세 훼손이다.

### 8.4 눌림 중 거래량 감소

```python
avg_volume_last_5 = mean(volume over last 5 days)
volume_contraction = avg_volume_last_5 <= median_volume_20 * 0.90
```

또는 하락일 거래량을 비교한다.

```python
down_days_last_5 = days where close < open in last 5 days
avg_down_volume_last_5 = mean(volume on down_days_last_5)

down_volume_contraction = avg_down_volume_last_5 <= median_volume_20 * 0.90
```

좋은 구조:

```text
가격은 쉬었지만 하락 중 거래량은 크지 않음.
```

### 8.5 재상승 트리거

```python
pullback_volume_trigger = (
    1.2 <= vol_rel20 <= 2.5
    and turnover_rel20 >= 1.2
    and vol_z20 >= 0.5
)
```

캔들 조건:

```python
pullback_candle_trigger = (
    close_loc >= 0.65
    and body_ratio >= 0.30
    and upper_wick_ratio <= 0.30
)
```

최종:

```python
pattern_pullback = (
    pattern_pullback_pass
    and volume_contraction
    and pullback_volume_trigger
    and pullback_candle_trigger
)
```

### 8.6 제외 조건

```python
pullback_exclude = (
    close_t < ma20 * 0.97
    or ma20_slope_5 < -0.005
    or ret5 > 0.15
    or dist_ma20 > 0.10
    or vol_rel20 > 5.0
    or upper_wick_ratio >= 0.45
)
```

---

## 9. 패턴 B: 박스권 압축 후 첫 돌파형

### 9.1 의도

가격과 이평선이 수렴하며 조용히 압축되다가, 첫 거래량 증가와 함께 박스 상단을 돌파하는 구조를 찾는다. 기존 학습형 모델이 놓치기 쉬운 선행형 후보에 가깝다.

### 9.2 박스폭 계산

오늘을 제외한 이전 20거래일 기준으로 계산한다.

```python
high_20_prev = max(high over t-20 to t-1)
low_20_prev = min(low over t-20 to t-1)
box_width_20 = high_20_prev / low_20_prev - 1
```

권장 범위:

```text
5% <= box_width_20 <= 18%
```

너무 좁으면 힘이 부족할 수 있고, 너무 넓으면 이미 변동성이 과열된 상태일 수 있다.

### 9.3 이평선 압축

```python
ma_compression_5_20_60 = (max(ma5, ma20, ma60) - min(ma5, ma20, ma60)) / close_t
```

좋은 조건:

```text
ma_compression_5_20_60 <= 8%
```

더 엄격한 조건:

```text
ma_compression_5_20_60 <= 5%
```

### 9.4 볼린저밴드 수축

```python
bb_mid = SMA(close, 20)
bb_std = STD(close, 20)
bb_upper = bb_mid + 2 * bb_std
bb_lower = bb_mid - 2 * bb_std
bb_width = (bb_upper - bb_lower) / bb_mid
bb_width_rank_120 = percentile_rank(bb_width over last 120 days)
```

좋은 조건:

```text
bb_width_rank_120 <= 0.40
```

### 9.5 돌파 조건

```python
breakout_price_pass = close_t > high_20_prev * 1.005
```

거래량 조건:

```python
breakout_volume_pass = (
    1.7 <= vol_rel20 <= 4.0
    and turnover_rel20 >= 1.5
    and vol_z20 >= 1.0
)
```

캔들 조건:

```python
breakout_candle_pass = (
    close_loc >= 0.70
    and upper_wick_ratio <= 0.30
    and body_ratio >= 0.35
)
```

과열 방지:

```python
breakout_not_overheated = (
    ret5 <= 0.12
    and ret20 <= 0.25
    and dist_ma20 <= 0.10
    and rsi14 <= 70
)
```

최종:

```python
pattern_box_breakout = (
    0.05 <= box_width_20 <= 0.18
    and ma_compression_5_20_60 <= 0.08
    and bb_width_rank_120 <= 0.40
    and breakout_price_pass
    and breakout_volume_pass
    and breakout_candle_pass
    and breakout_not_overheated
)
```

### 9.6 제외 조건

```python
box_breakout_exclude = (
    vol_rel20 > 5.0
    or ret1 > 0.10
    or upper_wick_ratio >= 0.45
    or close_loc <= 0.55
    or dist_ma20 > 0.12
)
```

---

## 10. 패턴 C: 역배열 개선형

### 10.1 의도

완전 약세였던 종목이 20일선을 회복하고, 단기 이평선 구조가 개선되는 초입을 찾는다.

이 패턴은 실패 확률이 높기 때문에 기본 추천 비중을 낮춘다.

권장 비중:

```yaml
max_reversal_candidates_ratio: 0.20
```

즉 최종 후보 10개 중 최대 2개까지만 허용한다.

### 10.2 조건

```python
ma5_cross_ma20_up = ma5_t > ma20_t and ma5_t_minus_1 <= ma20_t_minus_1

pattern_reversal_recovery = (
    close_t > ma20
    and ma5_cross_ma20_up
    and ma20_slope_5 > -0.003
    and 1.5 <= vol_rel20 <= 3.5
    and 40 <= rsi14 <= 60
    and close_loc >= 0.65
    and upper_wick_ratio <= 0.30
)
```

### 10.3 강한 제외 조건

```python
full_bear_alignment = ma5 < ma20 < ma60 < ma120
all_slopes_negative = ma20_slope_5 < 0 and ma60_slope_20 < 0 and ma120_slope_20 < 0

reversal_exclude = (
    close_t < ma20
    or (full_bear_alignment and all_slopes_negative)
    or vol_rel20 < 1.5
    or upper_wick_ratio >= 0.45
    or recent_major_bad_disclosure
    or not financial_hard_pass
)
```

---

## 11. 거래량 필터 상세

### 11.1 좋은 거래량 구조

좋은 구조:

```text
최근 며칠간 거래량 감소
→ 가격은 20일선 또는 박스권에서 버팀
→ 기준일에 거래량이 1.3~3.5배 증가
→ 종가가 고가권에서 마감
```

기본 조건:

```python
good_volume_trigger = (
    1.3 <= vol_rel20 <= 3.5
    and turnover_rel20 >= 1.3
    and 0.8 <= vol_z20 <= 2.8
)
```

박스 돌파형은 더 강하게 본다.

```python
good_breakout_volume_trigger = (
    1.7 <= vol_rel20 <= 4.0
    and turnover_rel20 >= 1.5
    and vol_z20 >= 1.0
)
```

### 11.2 거래량 감소 후 증가

```python
volume_dry_up_then_expand = (
    mean(volume over last 5 days) <= median_volume_20 * 0.80
    and volume_t >= median_volume_20 * 1.50
)
```

이 조건은 선행형 후보 탐색에 중요하다.

### 11.3 하락 거래량 위험

다음 조건은 제외한다.

```python
heavy_down_volume_exclude = (
    close_t < open_t
    and ret1 <= -0.03
    and vol_rel20 >= 1.5
    and close_t < ma20
)
```

해석:

```text
거래량 실린 음봉 + 20일선 이탈 = 매도 압력 우세.
```

### 11.4 윗꼬리 거래량 폭발 위험

다음 조건은 제외 또는 강한 감점한다.

```python
upper_wick_distribution = (
    vol_rel20 >= 2.0
    and upper_wick_ratio >= 0.45
    and close_loc <= 0.55
)
```

해석:

```text
장중 강했지만 종가가 밀림.
수급이 들어왔더라도 단기 매물 출회 가능성이 큼.
```

### 11.5 거래량 과열 위험

```python
volume_overheat_exclude = (
    vol_rel20 > 5.0
    and ret1 > 0.08
)
```

더 강한 제외:

```python
extreme_volume_chase_exclude = (
    vol_rel20 > 5.0
    and ret1 > 0.10
    and upper_wick_ratio > 0.35
)
```

---

## 12. 보조지표 필터

보조지표는 메인 판단이 아니라 확인용으로 사용한다.

### 12.1 RSI

```python
rsi14 = RSI(close, 14)
rsi5 = RSI(close, 5)
```

권장 구간:

```text
45 <= RSI14 <= 65 → 가장 선호
40 <= RSI14 < 45 → 회복 초입 가능
65 < RSI14 <= 70 → 강하지만 과열 주의
RSI14 > 72 → 제외 또는 강한 감점
RSI5 > 85 → 단기 과열 감점
```

### 12.2 MACD

MACD는 후행성이 있으므로 단독 신호로 사용하지 않는다. 개선 여부만 본다.

```python
macd_line, macd_signal, macd_hist = MACD(close)
macd_hist_improving_3d = macd_hist_t > macd_hist_t_minus_1 > macd_hist_t_minus_2
macd_hist_cross_up = macd_hist_t > 0 and macd_hist_t_minus_1 <= 0
```

점수화:

```python
if macd_hist_cross_up:
    score += 2
elif macd_hist_improving_3d:
    score += 1
```

감점:

```python
if macd_hist_t < macd_hist_t_minus_1 < macd_hist_t_minus_2 and dist_ma20 > 0.06:
    score -= 2
```

### 12.3 Bollinger Band

볼린저밴드는 압축/확장 확인용으로 사용한다.

좋은 조건:

```python
bb_squeeze_pass = bb_width_rank_120 <= 0.40
```

주의 조건:

```python
bb_upper_chase_risk = (
    close_t > bb_upper
    and vol_rel20 > 4.0
    and ret1 > 0.08
)
```

### 12.4 ATR

```python
atr14 = ATR(high, low, close, 14)
atr_pct = atr14 / close_t
```

권장 범위:

```text
0.02 <= atr_pct <= 0.08 → 적정
0.08 < atr_pct <= 0.12 → 변동성 주의
atr_pct > 0.12 → 제외 또는 강한 감점
```

변동성이 너무 낮으면 수익 기회가 부족할 수 있고, 너무 높으면 리스크 관리가 어려워진다.

---

## 13. 과열 방지 필터

학습형 추천이 이미 오른 종목에 몰리는 문제를 막기 위해 과열 방지 필터를 강하게 둔다.

### 13.1 수익률 과열

```python
return_overheat_exclude = (
    ret5 > 0.15
    or ret10 > 0.25
    or ret20 > 0.40
)
```

보수적 설정:

```python
return_overheat_soft_pass = (
    ret5 <= 0.10
    and ret20 <= 0.25
)
```

### 13.2 이평선 이격 과열

```python
ma_distance_overheat_exclude = (
    dist_ma20 > 0.10
    or dist_ma60 > 0.20
)
```

선호 구간:

```text
-1% <= dist_ma20 <= +6%
0% <= dist_ma60 <= +12%
```

### 13.3 급등 후 추격 위험

```python
chase_risk_exclude = (
    ret1 > 0.08
    and vol_rel20 > 4.0
    and close_t > ma20 * 1.08
)
```

강한 제외:

```python
extreme_chase_risk_exclude = (
    ret1 > 0.10
    and vol_rel20 > 5.0
)
```

### 13.4 연속 양봉 과열

```python
consecutive_up_days = count consecutive days where close > previous close
```

감점:

```python
if consecutive_up_days >= 4 and dist_ma20 > 0.06:
    score -= 5
```

제외:

```python
if consecutive_up_days >= 5 and ret5 > 0.12:
    exclude
```

---

## 14. 지지/저항과 손익 구조

StockMaster는 후보마다 위쪽 공간과 아래쪽 리스크를 같이 계산한다.

### 14.1 저항까지 거리

눌림목형:

```python
resistance_20 = max(high over last 20 days)
upside_to_resistance = resistance_20 / close_t - 1
```

권장 조건:

```text
upside_to_resistance >= 0.04
```

박스 돌파형은 이미 20일 고점을 돌파했기 때문에 다음 저항을 60일 또는 120일 고점으로 본다.

```python
resistance_60 = max(high over last 60 days)
upside_to_resistance_60 = resistance_60 / close_t - 1
```

### 14.2 기준 리스크 거리

눌림목형:

```python
risk_line_pullback = min(low_t, ma20 * 0.985)
risk_distance = close_t / risk_line_pullback - 1
```

박스 돌파형:

```python
risk_line_breakout = high_20_prev * 0.98
risk_distance = close_t / risk_line_breakout - 1
```

역배열 개선형:

```python
risk_line_reversal = ma20 * 0.98
risk_distance = close_t / risk_line_reversal - 1
```

권장 조건:

```text
risk_distance <= 0.05
```

선호 조건:

```text
risk_distance <= 0.04
```

### 14.3 손익비

```python
reward_risk_ratio = upside_to_resistance / risk_distance
```

권장 조건:

```text
reward_risk_ratio >= 1.5
```

강한 후보:

```text
reward_risk_ratio >= 2.0
```

---

## 15. 최종 후보 조건

### 15.1 공통 필수 조건

```python
common_pass = (
    universe_type_pass
    and market_risk_pass
    and ipo_age_pass
    and liquidity_pass
    and financial_hard_pass
    and not return_overheat_exclude
    and not ma_distance_overheat_exclude
    and not heavy_down_volume_exclude
    and not upper_wick_distribution
    and not volume_overheat_exclude
)
```

### 15.2 패턴 통과 조건

```python
pattern_pass = (
    pattern_pullback
    or pattern_box_breakout
    or pattern_reversal_recovery
)
```

### 15.3 최종 후보

```python
candidate = common_pass and pattern_pass
```

---

## 16. 점수화 모델

하드 필터를 통과한 종목만 점수화한다.

### 16.1 점수 배분

| 영역 | 배점 |
|---|---:|
| 차트 구조 | 25 |
| 거래량 구조 | 20 |
| 과열 방지 | 15 |
| 시장/섹터 상대강도 | 10 |
| 지지/저항 구조 | 10 |
| 캔들 품질 | 10 |
| 재무 안정성 | 10 |
| 총점 | 100 |

### 16.2 차트 구조 점수

```python
chart_score = 0

if close_t > ma20: chart_score += 5
if ma20_slope_5 > 0: chart_score += 5
if ma60_slope_20 >= 0: chart_score += 5
elif ma60_slope_20 >= -0.005: chart_score += 3

if ma5 > ma20 or ma5_turning_up: chart_score += 5
if -0.01 <= dist_ma20 <= 0.06: chart_score += 5
```

패턴 보정:

```python
if pattern_pullback: chart_score += 3
if pattern_box_breakout: chart_score += 2
if pattern_reversal_recovery: chart_score -= 2
```

최대 25점으로 제한한다.

```python
chart_score = min(chart_score, 25)
```

### 16.3 거래량 점수

```python
volume_score = 0

if 1.3 <= vol_rel20 <= 3.5: volume_score += 6
elif 1.1 <= vol_rel20 < 1.3: volume_score += 3

if turnover_rel20 >= 1.3: volume_score += 4
if volume_dry_up_then_expand: volume_score += 5
if vol_z20 >= 0.8 and vol_z20 <= 2.8: volume_score += 3
if up_day_volume_dominance: volume_score += 2
```

위험 감점:

```python
if vol_rel20 > 4.0 and ret1 > 0.08: volume_score -= 5
if upper_wick_distribution: volume_score -= 8
```

최대 20점, 최소 0점.

### 16.4 과열 방지 점수

```python
overheat_score = 0

if ret5 <= 0.10: overheat_score += 5
if ret20 <= 0.25: overheat_score += 5
if dist_ma20 <= 0.08: overheat_score += 3
if rsi14 <= 70: overheat_score += 2
```

감점:

```python
if ret5 > 0.12: overheat_score -= 4
if dist_ma20 > 0.10: overheat_score -= 5
if rsi14 > 72: overheat_score -= 5
```

최대 15점, 최소 0점.

### 16.5 시장/섹터 점수

```python
regime_score = 0

if market_strong: regime_score += 4
elif market_neutral: regime_score += 2

if sector_rs_5 > 0: regime_score += 2
if sector_rs_20 > 0: regime_score += 2
if sector_rank_20 <= 0.30: regime_score += 2
```

시장 약세 감점:

```python
if market_weak:
    regime_score -= 3
```

최대 10점, 최소 0점.

### 16.6 지지/저항 점수

```python
rr_score = 0

if risk_distance <= 0.04: rr_score += 4
elif risk_distance <= 0.05: rr_score += 2

if upside_to_resistance >= 0.05: rr_score += 3
elif upside_to_resistance >= 0.04: rr_score += 2

if reward_risk_ratio >= 2.0: rr_score += 3
elif reward_risk_ratio >= 1.5: rr_score += 2
```

최대 10점.

### 16.7 캔들 점수

```python
candle_score = 0

if close_loc >= 0.65: candle_score += 4
if body_ratio >= 0.35: candle_score += 3
if upper_wick_ratio <= 0.30: candle_score += 3
```

감점:

```python
if upper_wick_ratio >= 0.45: candle_score -= 5
if close_loc <= 0.50: candle_score -= 3
```

최대 10점, 최소 0점.

### 16.8 재무 안정성 점수

```python
financial_score = 0

if equity > 0: financial_score += 2
if audit_opinion_is_normal: financial_score += 2
if debt_ratio <= 100: financial_score += 2
elif debt_ratio <= 200: financial_score += 1

if current_ratio >= 150: financial_score += 2
elif current_ratio >= 100: financial_score += 1

if operating_profit_ttm > 0: financial_score += 2
elif growth_profile and cash_runway_months >= 12: financial_score += 2
```

감점:

```python
if recent_paid_in_capital_increase_180d: financial_score -= 5
if recent_cb_bw_180d: financial_score -= 3
if recent_major_shareholder_change_180d: financial_score -= 5
```

최대 10점, 최소 0점.

### 16.9 최종 점수

```python
rule_score = (
    chart_score
    + volume_score
    + overheat_score
    + regime_score
    + rr_score
    + candle_score
    + financial_score
)
```

후보 등급:

| 점수 | 등급 | 의미 |
|---:|---|---|
| 85 이상 | A | 매우 강한 후보 |
| 80~84 | B+ | 강한 후보 |
| 75~79 | B | 일반 후보 |
| 70~74 | C | 관심 후보 |
| 70 미만 | 제외 | 기본 추천 제외 |

권장 기본값:

```yaml
recommendation_threshold: 75
strong_recommendation_threshold: 80
```

시장 약세 시:

```yaml
recommendation_threshold: 80
strong_recommendation_threshold: 85
```

---

## 17. ML 랭킹 사용 방식

### 17.1 기본 원칙

ML은 전체 종목에서 직접 후보를 고르지 않는다.

```text
룰 필터 통과 후보 → ML 랭킹 → 최종 점수 보정
```

ML은 후보군 안에서 우선순위를 정하는 역할만 한다.

### 17.2 권장 라벨: Triple Barrier

단순 D+5 수익률은 후행 모멘텀을 과도하게 학습할 수 있다. 대신 이벤트 기반 라벨을 사용한다.

기준일 `t`의 후보에 대해 평가 시작 가격을 `t+1` 시가로 둔다.

```python
reference_price = open_t_plus_1
upper_barrier = reference_price * 1.05
lower_barrier = reference_price * 0.97
horizon = 5 trading days
```

라벨:

```text
5거래일 안에 +5%에 먼저 도달하고 -3%에 먼저 닿지 않음 → label = 1
-3%에 먼저 도달 → label = 0
둘 다 도달하지 않음 → label = neutral 또는 0.5
```

기본 추천:

```python
label = 1 if upper_barrier_hit_first else 0
```

중립을 별도 처리할 수 있으면 3-class도 가능하다.

```text
positive / neutral / negative
```

### 17.3 갭 리스크 처리

기준일 종가 대비 다음 거래일 시가가 너무 높으면 평가에서 제외하거나 감점한다.

```python
gap_next = open_t_plus_1 / close_t - 1
```

권장 처리:

```text
gap_next <= +3% → 정상 평가
+3% < gap_next <= +5% → 감점 또는 별도 기록
gap_next > +5% → 평가 제외
```

이 규칙을 넣어야 이미 급등한 종목을 뒤늦게 좋게 평가하는 문제가 줄어든다.

### 17.4 ML 피처 목록

ML 피처는 룰 필터에서 사용한 값을 그대로 포함한다.

```text
price features:
- ret1, ret3, ret5, ret10, ret20
- dist_ma5, dist_ma20, dist_ma60, dist_ma120
- drawdown_from_high_10
- upside_to_resistance
- risk_distance
- reward_risk_ratio

trend features:
- ma20_slope_5
- ma60_slope_20
- ma_compression_5_20_60
- full_bear_alignment flag
- ma5_cross_ma20_up flag

volume features:
- vol_rel20
- vol_rel60
- turnover_rel20
- vol_z20
- volume_dry_up_then_expand flag
- heavy_down_volume flag

candle features:
- close_loc
- body_ratio
- upper_wick_ratio
- lower_wick_ratio

indicator features:
- rsi5
- rsi14
- macd_hist
- macd_hist_delta
- bb_width
- bb_width_rank_120
- atr_pct

market/sector features:
- market_regime
- sector_rs_5
- sector_rs_20
- sector_rank_20

financial/risk features:
- debt_ratio
- current_ratio
- equity_positive flag
- operating_profit_positive flag
- cash_runway_months
- recent_cb_bw flag
- recent_paid_in_capital_increase flag
- recent_major_shareholder_change flag
```

### 17.5 ML 점수 결합

ML 성능 검증 전에는 룰 점수를 우선한다.

```python
final_score = rule_score
```

ML 검증이 통과된 후:

```python
ml_score_scaled = calibrated_probability * 100

final_score = (
    0.70 * rule_score
    + 0.20 * ml_score_scaled
    + 0.10 * sector_score_scaled
)
```

ML 성능이 충분히 안정되면:

```python
final_score = (
    0.60 * rule_score
    + 0.30 * ml_score_scaled
    + 0.10 * sector_score_scaled
)
```

룰 점수가 70 미만인 종목은 ML 점수가 높아도 최종 추천하지 않는다.

```python
if rule_score < 70:
    exclude_from_final_ranking
```

---

## 18. 데이터 누수 방지

### 18.1 시세 데이터 누수

기준일 `t`의 추천은 `t` 장 마감 후 확정된 데이터만 사용한다.

금지:

```text
기준일 t의 후보 생성에 t+1 이후 가격/거래량 사용 금지
오늘을 포함한 20일 고점으로 돌파 여부를 잘못 계산하는 것 금지
미래 섹터 수익률 사용 금지
```

박스 돌파 기준은 반드시 오늘을 제외한 과거 20일 고점으로 계산한다.

```python
high_20_prev = max(high from t-20 to t-1)
```

### 18.2 재무/공시 데이터 누수

재무제표는 보고서 기준일이 아니라 공시일 기준으로 반영한다.

잘못된 방식:

```text
2024년 12월 재무제표를 2024년 12월부터 사용
```

올바른 방식:

```text
해당 보고서가 실제 공시된 시각 이후부터 사용
```

필수 필드:

```text
report_period_end
filing_datetime
usable_from_datetime
```

### 18.3 생존자 편향 방지

과거 검증 시 현재 상장된 종목만 사용하면 안 된다. 가능하면 과거 상장폐지/거래정지 종목까지 포함한다.

최소한 다음 리스크를 별도 기록한다.

```text
delisted_missing_risk: true/false
survivorship_bias_note
```

---

## 19. 검증 방법론

### 19.1 랜덤 분할 금지

주식 데이터는 시간 순서가 중요하므로 랜덤 train/test split을 사용하지 않는다.

권장:

```text
walk-forward validation
purged time-series split
expanding window validation
```

예시:

```text
Train: 2018~2021 → Test: 2022
Train: 2018~2022 → Test: 2023
Train: 2018~2023 → Test: 2024
Train: 2018~2024 → Test: 2025
```

### 19.2 평가 단위

매일 후보를 생성하고 상위 N개를 평가한다.

권장 N:

```text
Top 3
Top 5
Top 10
```

### 19.3 주요 평가 지표

```text
precision@N
5일 평균 수익률
5일 중앙값 수익률
positive hit rate
triple-barrier hit rate
average downside before upside
max adverse excursion
max favorable excursion
recommendation turnover
market-regime별 성과
sector별 성과
```

### 19.4 최소 통과 기준

초기 목표:

```text
Top5 hit rate가 무작위 후보 대비 우위
Top5 median return이 0보다 큼
시장 약세 구간에서 손실폭이 제한됨
과열 제외 필터를 켰을 때 성과 안정성이 개선됨
```

고신뢰 기준:

```text
3개 이상 연도에서 일관성 확인
KOSPI/KOSDAQ 각각 성능 확인
시장 강세/중립/약세별 성능 확인
특정 섹터에만 의존하지 않는지 확인
Top5와 Top10 성능이 모두 무작위 대비 우위
```

### 19.5 비용 가정

평가 시 비용은 설정값으로 둔다.

```yaml
evaluation_cost_bps_round_trip: 30
```

보수적으로 검증하려면:

```yaml
evaluation_cost_bps_round_trip: 50
```

성과는 비용 차감 전/후를 모두 기록한다.

---

## 20. 출력 형식

### 20.1 후보 출력 JSON

```json
{
  "date": "YYYY-MM-DD",
  "market_regime": "strong|neutral|weak",
  "symbol": "000000",
  "name": "종목명",
  "market": "KOSPI|KOSDAQ",
  "sector": "섹터명",
  "pattern": "pullback|box_breakout|reversal_recovery",
  "final_score": 82.4,
  "rule_score": 80.0,
  "ml_score": 74.1,
  "grade": "B+",
  "liquidity": {
    "market_cap": 250000000000,
    "avg_turnover_20": 5200000000,
    "turnover_rel20": 1.6,
    "vol_rel20": 2.1
  },
  "technical": {
    "close": 12340,
    "ma5": 12100,
    "ma20": 11850,
    "ma60": 11200,
    "dist_ma20": 0.0295,
    "ma20_slope_5": 0.012,
    "ma60_slope_20": 0.004,
    "rsi14": 58.2,
    "atr_pct": 0.045
  },
  "candle": {
    "close_loc": 0.72,
    "body_ratio": 0.41,
    "upper_wick_ratio": 0.18
  },
  "risk_reward": {
    "upside_to_resistance": 0.063,
    "risk_distance": 0.034,
    "reward_risk_ratio": 1.85
  },
  "financial": {
    "financial_profile": "general",
    "debt_ratio": 145.2,
    "current_ratio": 121.0,
    "audit_opinion_normal": true,
    "red_flags": []
  },
  "reasons": [
    "20일선 위에서 눌림 후 재상승 구조",
    "거래량이 20일 중앙값 대비 2.1배 증가",
    "20일선 이격도가 2.95%로 과열 구간 아님",
    "캔들 종가 위치가 고가권에 가까움",
    "손익비가 1.5 이상"
  ],
  "warnings": []
}
```

### 20.2 설명 생성 규칙

설명은 항상 다음 순서로 작성한다.

```text
1. 패턴명
2. 수급 근거
3. 과열 여부
4. 리스크 라인
5. 재무/공시 리스크 여부
6. 시장/섹터 환경
```

나쁜 설명:

```text
오를 가능성이 높습니다.
```

좋은 설명:

```text
20일선 눌림 후 재상승형입니다. 기준일 거래량은 20일 중앙값 대비 2.1배이며, 종가가 당일 고저 범위의 72% 위치에서 마감했습니다. 20일선 이격도는 2.9%로 과열 기준에는 걸리지 않습니다. 다만 최근 20일 고점까지 남은 상승 여력이 4.2%로 크지 않아 점수는 일부 제한됩니다.
```

---

## 21. 구현 모듈 구조

권장 디렉터리 구조:

```text
stockmaster/
  data/
    loaders.py
    validators.py
    corporate_actions.py
  features/
    price_features.py
    volume_features.py
    candle_features.py
    technical_indicators.py
    financial_features.py
    market_regime.py
    sector_strength.py
  filters/
    hard_exclusions.py
    liquidity.py
    financial_guard.py
    overheat.py
    pattern_filters.py
  scoring/
    rule_score.py
    ml_ranker.py
    final_ranker.py
  validation/
    leakage_checks.py
    walk_forward.py
    metrics.py
  explain/
    reason_builder.py
    warning_builder.py
  config/
    default.yaml
    strict.yaml
  tests/
    test_features.py
    test_filters.py
    test_scoring.py
    test_leakage.py
```

### 21.1 핵심 함수 시그니처

```python
def compute_price_features(df_daily):
    """Return price, MA, slope, return, distance features."""


def compute_volume_features(df_daily):
    """Return volume relative metrics and volume risk flags."""


def compute_candle_features(df_daily):
    """Return close location, body ratio, wick ratios."""


def apply_hard_exclusions(symbol_row, risk_flags, config):
    """Return pass/fail and exclusion reasons."""


def apply_financial_guard(financial_row, disclosure_flags, config):
    """Return pass/fail, financial score, red flags."""


def detect_patterns(feature_row, config):
    """Return detected pattern list and pattern-specific scores."""


def compute_rule_score(feature_row, financial_row, market_row, config):
    """Return 0~100 rule score and component scores."""


def rank_candidates(candidates, config):
    """Return sorted candidates with final_score, grade, reasons."""
```

---

## 22. 기본 설정 파일 예시

```yaml
profile: stockmaster_3_5d_v1

universe:
  include_markets: ["KOSPI", "KOSDAQ"]
  common_stock_only: true
  exclude_preferred: true
  exclude_etf: true
  exclude_etn: true
  exclude_elw: true
  exclude_spac: true
  exclude_reit: true
  min_listed_trading_days: 60

risk_flags:
  strict_mode: true
  exclude_management_issue: true
  exclude_trading_halt: true
  exclude_delisting_review: true
  exclude_unfaithful_disclosure: true
  exclude_investment_caution: true
  exclude_investment_warning: true
  exclude_investment_risk: true
  exclude_short_term_overheated: true

liquidity:
  price_min_krw: 1000
  market_cap_min_krw: 100000000000
  avg_turnover_20_min_krw: 2000000000
  median_turnover_20_min_krw: 1500000000
  avg_volume_20_min: 50000
  reference_amount_krw: 3000000
  max_liquidity_capacity_ratio: 0.005

financial:
  require_positive_equity: true
  require_normal_audit_opinion: true
  exclude_capital_impairment: true
  general:
    debt_ratio_max: 300
    current_ratio_min: 80
    revenue_ttm_min_krw: 30000000000
  strict_general:
    debt_ratio_max: 200
    current_ratio_min: 100
    revenue_ttm_min_krw: 50000000000
    require_positive_operating_profit_ttm: true
  growth:
    cash_runway_months_min: 12
    cash_runway_months_warning: 6
  disclosure_penalties:
    paid_in_capital_increase_180d: -5
    cb_bw_180d: -3
    major_shareholder_change_180d: -5
    major_lawsuit_or_sanction_180d: -3

market_regime:
  use_market_filter: true
  max_candidates_strong: 15
  max_candidates_neutral: 8
  max_candidates_weak: 3
  weak_market_score_threshold_add: 5

technical:
  ma_periods: [5, 20, 60, 120]
  ma20_slope_5_min: 0.0
  ma60_slope_20_min: -0.005
  dist_ma20_min: -0.01
  dist_ma20_max: 0.06
  dist_ma20_exclude: 0.10
  dist_ma60_exclude: 0.20

returns_overheat:
  ret3_exclude: 0.12
  ret5_soft_max: 0.10
  ret5_exclude: 0.15
  ret10_exclude: 0.25
  ret20_soft_max: 0.25
  ret20_exclude: 0.40

volume:
  vol_rel20_min: 1.3
  vol_rel20_max: 3.5
  turnover_rel20_min: 1.3
  vol_z20_min: 0.8
  vol_z20_max: 2.8
  breakout_vol_rel20_min: 1.7
  breakout_vol_rel20_max: 4.0
  extreme_vol_rel20: 5.0
  dry_up_avg5_to_median20_max: 0.8

candle:
  close_loc_min: 0.65
  breakout_close_loc_min: 0.70
  body_ratio_min: 0.35
  upper_wick_ratio_max: 0.30
  upper_wick_distribution_min: 0.45

indicators:
  rsi14_min: 45
  rsi14_max: 70
  rsi14_preferred_max: 65
  rsi14_exclude: 72
  rsi5_overheat: 85
  atr_pct_min: 0.02
  atr_pct_max: 0.08
  atr_pct_exclude: 0.12
  bb_width_rank_120_max: 0.40

patterns:
  pullback:
    enabled: true
    priority: 1
    drawdown_high10_min: -0.12
    drawdown_high10_max: -0.03
    vol_rel20_min: 1.2
    vol_rel20_max: 2.5
  box_breakout:
    enabled: true
    priority: 2
    box_width20_min: 0.05
    box_width20_max: 0.18
    ma_compression_5_20_60_max: 0.08
    breakout_buffer: 0.005
  reversal_recovery:
    enabled: true
    priority: 3
    max_final_ratio: 0.20
    ma20_slope_5_min: -0.003
    rsi14_min: 40
    rsi14_max: 60

risk_reward:
  risk_distance_max: 0.05
  risk_distance_preferred: 0.04
  upside_to_resistance_min: 0.04
  reward_risk_ratio_min: 1.5
  reward_risk_ratio_strong: 2.0

scoring:
  threshold_default: 75
  threshold_strong: 80
  threshold_weak_market_add: 5
  weights:
    chart: 25
    volume: 20
    overheat: 15
    regime: 10
    risk_reward: 10
    candle: 10
    financial: 10

ml:
  use_ml_ranker: false
  min_rule_score_for_ml: 70
  rule_weight_default: 0.70
  ml_weight_default: 0.20
  sector_weight_default: 0.10
  triple_barrier:
    upper: 0.05
    lower: -0.03
    horizon_days: 5
    max_gap_normal: 0.03
    max_gap_allowed: 0.05

validation:
  evaluation_cost_bps_round_trip: 30
  conservative_cost_bps_round_trip: 50
  top_n_list: [3, 5, 10]
```

---

## 23. 테스트 항목

### 23.1 단위 테스트

필수 테스트:

```text
- ma5/ma20/ma60/ma120 계산 정확성
- ma20_slope_5 계산 정확성
- vol_rel20이 중앙값 기준으로 계산되는지
- close_loc, body_ratio, upper_wick_ratio 계산 정확성
- high_20_prev가 오늘을 제외하는지
- ret5, ret20 과열 제외가 정확히 작동하는지
- 투자경고/위험/단기과열 플래그가 있으면 즉시 제외되는지
- 감사의견 비정상, 자본잠식이면 즉시 제외되는지
- 금융업 예외 로직이 일반기업 기준과 분리되는지
```

### 23.2 경계값 테스트

```text
- vol_rel20 = 1.299 → 통과하지 않아야 함
- vol_rel20 = 1.300 → 통과해야 함
- vol_rel20 = 3.500 → 통과해야 함
- vol_rel20 = 3.501 → 일반 거래량 조건에서는 통과하지 않아야 함
- dist_ma20 = 0.100 → 허용 경계
- dist_ma20 = 0.101 → 제외
- ret5 = 0.150 → 허용 경계 또는 설정에 따른 처리
- ret5 = 0.151 → 제외
```

### 23.3 누수 테스트

```text
- 박스 돌파 기준에 오늘 고가가 포함되지 않는지
- 재무제표가 공시일 이전에 사용되지 않는지
- t+1 시가가 t일 피처에 들어가지 않는지
- 미래 섹터 수익률이 현재 피처에 들어가지 않는지
```

### 23.4 결과 안정성 테스트

```text
- 동일 데이터와 동일 config에서 동일 결과가 나오는지
- 데이터 일부 누락 시 보수적으로 제외되는지
- 시장 약세 config에서 후보 수가 줄어드는지
- strict_mode true/false 결과 차이가 설명 가능한지
```

---

## 24. 후보 설명 템플릿

### 24.1 긍정 설명 템플릿

```text
{종목명}은 {pattern_name} 후보입니다. 기준일 종가는 20일선 대비 {dist_ma20_pct}% 위치이며, 20일선 기울기는 {ma20_slope_5_pct}%입니다. 거래량은 20일 중앙값 대비 {vol_rel20}배로 증가했고, 종가는 당일 고저 범위의 {close_loc_pct}% 위치에서 마감했습니다. 최근 5일 상승률은 {ret5_pct}%로 과열 제외 기준에는 걸리지 않습니다.
```

### 24.2 주의 설명 템플릿

```text
주의점은 {warning_reason}입니다. 특히 {risk_metric_name} 값이 {risk_metric_value}로 기준에 근접해 있어, 후보 등급은 {grade}로 제한됩니다.
```

### 24.3 제외 설명 템플릿

```text
{종목명}은 후보에서 제외됩니다. 제외 사유는 {exclusion_reason}입니다. StockMaster는 하드 제외 조건에 걸린 종목을 점수화하지 않습니다.
```

---

## 25. 최종 의사코드

```python
def build_stockmaster_candidates(date, config):
    universe = load_universe(date)
    price_data = load_daily_ohlcv(universe, date)
    market_flags = load_market_risk_flags(universe, date)
    financials = load_financials_asof(universe, date)
    disclosures = load_disclosure_flags_asof(universe, date)
    market_data = load_market_indices(date)
    sector_data = load_sector_indices(date)

    rows = []

    for symbol in universe:
        row = assemble_symbol_row(
            symbol=symbol,
            price_data=price_data,
            market_flags=market_flags,
            financials=financials,
            disclosures=disclosures,
            market_data=market_data,
            sector_data=sector_data,
            date=date,
        )

        hard_result = apply_hard_exclusions(row, config)
        if not hard_result.passed:
            continue

        liquidity_result = apply_liquidity_filter(row, config)
        if not liquidity_result.passed:
            continue

        financial_result = apply_financial_guard(row, config)
        if not financial_result.passed:
            continue

        features = compute_all_features(row, config)

        risk_result = apply_overheat_and_distribution_filters(features, config)
        if not risk_result.passed:
            continue

        patterns = detect_patterns(features, config)
        if not patterns:
            continue

        score_result = compute_rule_score(
            features=features,
            financial=financial_result,
            patterns=patterns,
            config=config,
        )

        if score_result.rule_score < config.scoring.threshold_default:
            continue

        explanation = build_explanation(row, features, financial_result, score_result, patterns)

        rows.append({
            "symbol": symbol,
            "name": row.name,
            "date": date,
            "patterns": patterns,
            "rule_score": score_result.rule_score,
            "component_scores": score_result.components,
            "features": features.to_dict(),
            "financial": financial_result.to_dict(),
            "reasons": explanation.reasons,
            "warnings": explanation.warnings,
        })

    ranked = rank_candidates(rows, config)
    ranked = enforce_pattern_mix_limits(ranked, config)
    ranked = apply_market_regime_candidate_limit(ranked, config)

    return ranked
```

---

## 26. 운영 체크리스트

매일 후보 생성 전 확인:

```text
1. 일봉 데이터 최신성 확인
2. 거래대금/거래량 누락 확인
3. KRX/KIND 시장조치 플래그 최신성 확인
4. DART/OpenDART 공시 반영 시각 확인
5. 지수/섹터 데이터 최신성 확인
6. config 버전 기록
7. 후보 생성 결과와 제외 사유 로그 저장
```

후보 생성 후 확인:

```text
1. Top 후보가 특정 섹터에 과도하게 몰리는지
2. 과열 제외 기준을 우회한 종목이 있는지
3. 거래량 폭발 + 윗꼬리 종목이 남아 있는지
4. 재무/공시 리스크 플래그가 빈 값으로 통과된 종목이 있는지
5. 시장 약세일인데 후보가 너무 많이 나온 것은 아닌지
```

---

## 27. 기본 결론

StockMaster 3~5일 보유형 추천의 기본 방향은 다음이다.

```text
1. 공식 시장조치/공시/재무 리스크를 먼저 제거한다.
2. 유동성 부족 종목을 제거한다.
3. 이미 오른 종목을 과열 필터로 제거한다.
4. 20일선 눌림, 박스권 돌파, 역배열 개선형만 후보로 본다.
5. 거래량은 20일 중앙값 대비 1.3~3.5배를 기본 선호 구간으로 둔다.
6. 5배 이상 거래량 폭발 + 급등 + 윗꼬리는 추격 위험으로 본다.
7. 재무제표는 수익률 예측보다 위험 종목 제거에 사용한다.
8. 시장 약세일에는 후보 수와 기준을 보수적으로 조정한다.
9. ML은 전체 종목 선별기가 아니라, 필터 통과 후보의 랭킹 보조로 사용한다.
10. 검증은 시간 순서 기반 walk-forward 방식으로 한다.
```

한 줄 요약:

```text
StockMaster는 “이미 오른 종목”이 아니라 “과열 전, 수급 초입, 손익 구조가 괜찮은 종목”을 고신뢰 후보로 분류해야 한다.
```
