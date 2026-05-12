# StockMaster v4 — EOD 기준 손익비 / 진입가 산정 명세

> 적용 범위: `StockMaster_Cycle_ML_Hybrid_Methodology_v3.md` 이후 추가 반영 문서  
> 목적: **오늘 장마감 종가 기준으로 다음 거래일 추천 종목을 생성할 때**, 손익비·목표가·손절가·최대 허용 진입가를 일관되게 계산하도록 Codex에 전달하는 구현 지시서

---

## 0. 핵심 결론

StockMaster의 기본 추천 생성 기준은 다음으로 고정한다.

```text
오늘 장마감 데이터 기준
→ 오늘 종가를 기준가로 사용
→ 다음 거래일 매수 후보 산출
```

따라서 장마감 추천 생성 시점의 기본 가격은 다음과 같다.

```text
entry_reference_price = signal_close = 오늘 종가
```

장마감 이후 추천 생성 단계에서는 `current_price`라는 표현을 사용하지 않는다.  
혼동 방지를 위해 반드시 아래 이름을 사용한다.

```text
signal_close          : 신호 발생일 종가
entry_reference_price : 장마감 추천 생성 시 사용하는 기준 진입가
next_open_price       : 다음 거래일 시가. 다음날이 되어야 알 수 있음
live_price            : 다음 거래일 장중 현재가. 장중 데이터가 있을 때만 사용
```

EOD 추천 생성 시점에서는:

```text
entry_reference_price = signal_close
```

이다.

---

## 1. 이 문서가 해결해야 하는 문제

기존에는 다음과 같은 혼동이 발생할 수 있었다.

```text
오늘 종가 기준으로 다음날 추천을 만드는 상황인데,
손익비 계산에서 현재가/current_price라는 표현이 사용됨.
```

하지만 StockMaster의 기본 운영 구조는 장마감 이후 배치 시스템이다.

```text
장마감
→ 당일 데이터 수집
→ feature 생성
→ 룰 필터
→ ML 추론
→ 하이브리드 계산
→ 다음날 추천 종목 생성
```

따라서 EOD 단계에서는 다음날 시가나 장중 현재가를 알 수 없다.  
그러므로 EOD 추천의 손익비는 반드시 **오늘 종가 기준**으로 계산한다.

---

## 2. 용어 정의

| 용어 | 의미 |
|---|---|
| `signal_date` | 신호가 발생한 거래일 |
| `execution_date` | 추천을 사용하는 다음 거래일 |
| `signal_close` | `signal_date`의 종가 |
| `entry_reference_price` | EOD 추천 생성 시 기준 진입가. 기본값은 `signal_close` |
| `target_1` | 3~5일 보유 기준 1차 목표가 |
| `target_2` | 3~5일 보유 기준 2차 목표가 |
| `stop_price` | 구조 훼손 또는 손절 기준가 |
| `rr_min` | 최소 요구 손익비. 기본값 1.5 |
| `rr_at_reference` | `entry_reference_price` 기준 손익비 |
| `max_buy_price` | 손익비 1.5를 만족하는 최대 허용 진입가 |
| `signal_score` | 장마감 기준 신호 품질 점수 |
| `entry_score` | 가격 기준 진입 가능성 점수 |
| `final_score` | 실행 가능 추천 선별용 종합 점수 |

---

## 3. 가격 기준 원칙

### 3.1 EOD 추천 생성 시

장마감 추천 생성 단계에서는:

```python
entry_reference_price = signal_close
price_basis = "EOD_SIGNAL_CLOSE"
```

으로 고정한다.

이때 손익비, 최대 허용 진입가, 목표권 도달 여부는 모두 `entry_reference_price`를 기준으로 계산한다.

```python
rr_at_reference = (target_1 - entry_reference_price) / (entry_reference_price - stop_price)
```

---

### 3.2 다음 거래일 장중 재평가 시

다음날 장중 데이터가 있다면 별도 재평가를 할 수 있다.

```python
entry_check_price = live_price
```

단, 이것은 EOD 추천 생성이 아니라 **추천 유효성 재평가**다.

장중 재평가가 없는 현재 구조에서는 추천 메시지에 아래 문구를 포함한다.

```text
내일 시가 또는 장중 가격이 max_buy_price 이하일 때만 신규 진입 가능.
max_buy_price 초과 시 추격주의 또는 진입불가.
```

---

## 4. 손익비 계산 기준

### 4.1 기본 공식

```text
RR = (target_1 - entry_reference_price) / (entry_reference_price - stop_price)
```

Python 기준:

```python
def calculate_rr(entry_reference_price: float, target_1: float, stop_price: float) -> float | None:
    reward = target_1 - entry_reference_price
    risk = entry_reference_price - stop_price

    if risk <= 0:
        return None
    if reward <= 0:
        return 0.0

    return reward / risk
```

---

### 4.2 필수 검증

손익비 계산 전에 반드시 아래를 검증한다.

```python
assert stop_price < entry_reference_price
assert target_1 > entry_reference_price
```

검증 실패 시:

| 상황 | 처리 |
|---|---|
| `stop_price >= entry_reference_price` | `INVALID_STOP` |
| `target_1 <= entry_reference_price` | `TARGET_ALREADY_REACHED` 또는 `NO_UPSIDE` |
| `risk <= 0` | RR 계산 불가 |
| `reward <= 0` | 신규 추천 불가 |

---

## 5. 최소 손익비 기준

기본값은 다음으로 고정한다.

```yaml
rr_min: 1.5
```

즉:

```text
1차 목표 보상 >= 손절 위험의 1.5배
```

이어야 한다.

예:

```text
진입 기준가: 91,200원
1차 목표가: 93,132원
손절 기준가: 88,004원
```

계산:

```text
reward = 93,132 - 91,200 = 1,932
risk   = 91,200 - 88,004 = 3,196
RR     = 1,932 / 3,196 = 0.6045
```

판정:

```text
RR 0.6045 < 1.5
→ 오늘 종가 기준으로 다음날 신규 매수 추천 불가
```

이 경우 해당 종목은 다음처럼 해석한다.

```text
신호 자체는 존재할 수 있으나,
오늘 종가 기준으로는 손절 위험 대비 1차 목표 보상이 부족하다.
따라서 EXECUTABLE_PICKS에는 포함하지 않는다.
```

---

## 6. 최대 허용 진입가 계산

### 6.1 목적

추천 메시지에는 반드시:

```text
얼마 이하일 때만 진입 가능한가
```

를 표시해야 한다.

이를 위해 `max_buy_price`를 계산한다.

---

### 6.2 공식

손익비 조건:

```text
(target_1 - entry_price) / (entry_price - stop_price) >= rr_min
```

이를 `entry_price`에 대해 풀면:

```text
max_buy_price = (target_1 + rr_min * stop_price) / (1 + rr_min)
```

Python:

```python
def calculate_max_buy_price(target_1: float, stop_price: float, rr_min: float = 1.5) -> float:
    return (target_1 + rr_min * stop_price) / (1.0 + rr_min)
```

---

### 6.3 예시

```text
target_1 = 93,132
stop_price = 88,004
rr_min = 1.5
```

```text
max_buy_price
= (93,132 + 1.5 * 88,004) / 2.5
= 90,055.2
```

따라서:

```text
최대 허용 진입가: 약 90,055원
```

오늘 종가가 91,200원이면:

```text
91,200 > 90,055
```

이므로:

```text
오늘 종가 기준으로도 이미 최대 허용 진입가 초과
→ 다음날 추천 종목에서 제외
→ VALID_SIGNAL 또는 WATCHLIST로 분류
```

---

## 7. 호가 단위 반영

KRX 호가 단위에 맞춰 가격을 정규화한다.  
호가 단위 함수가 이미 있다면 반드시 공통 유틸리티를 사용한다.

권장 처리:

```python
max_buy_price = floor_to_tick(max_buy_price)
target_1_for_rr = floor_to_tick(target_1)
stop_price_for_rr = floor_to_tick(stop_price)
```

보수적으로 평가하기 위해:

| 항목 | 반올림 방향 | 이유 |
|---|---|---|
| `max_buy_price` | 아래로 내림 | 실제 허용 진입가를 과대평가하지 않기 위해 |
| `target_1_for_rr` | 아래로 내림 | 기대 보상을 과대평가하지 않기 위해 |
| `stop_price_for_rr` | 아래로 내림 | 손실 위험을 과소평가하지 않기 위해 |

---

## 8. 목표가 산정 기준

### 8.1 target_1의 역할

`target_1`은 단순 희망가가 아니다.  
3~5일 보유 기준으로 실제 도달 가능성이 있는 1차 목표 가격이어야 한다.

`target_1`은 손익비 계산의 기준이므로, 억지로 높게 잡아서는 안 된다.

금지:

```text
RR을 통과시키기 위해 목표가를 임의로 상향 조정
```

허용:

```text
패턴별 구조상 합리적인 목표가 산정
```

---

### 8.2 공통 target_1 후보

다음 중 패턴에 맞는 값을 사용한다.

```text
최근 20일 또는 60일 의미 있는 스윙 고점
직전 박스권 상단 이후 측정 목표
ATR 기반 단기 목표
주요 매물대 또는 저항선
```

---

### 8.3 패턴별 target_1

#### Pullback

```text
20일선 눌림 후 재상승형
```

권장:

```text
target_1 = 최근 10~20일 스윙 고점 또는 직전 고점
```

단, 목표가가 너무 가까우면 신규 추천하지 않는다.

```python
if target_1 / entry_reference_price - 1 < 0.03:
    target_quality = "TOO_CLOSE"
```

---

#### Breakout

```text
박스권 돌파형
```

권장 후보:

```text
1. 박스 상단 돌파 후 측정 목표
2. entry_reference_price + ATR14 * 1.0~1.5
3. 다음 주요 저항선
```

단, 목표가가 너무 공격적으로 산정되지 않도록 한다.

```text
3~5일 전략의 1차 목표는 보통 +4%~+8% 범위가 적당하다.
```

---

#### Recovery Breakout

```text
하락 후 회복형 돌파
```

권장:

```text
1. 최근 하락 중 생긴 직전 매물대
2. 20~60일 스윙 고점
3. ATR 기반 단기 반등 목표
```

이 패턴은 위쪽 매물이 가까운 경우가 많으므로, `target_1`이 가까우면 RR이 낮아지는 것이 정상이다.

---

#### Reversal Recovery

```text
역배열 개선 초기
```

권장:

```text
1. ma60 또는 직전 저항
2. 최근 20일 고점
3. ATR 기반 1차 반등 목표
```

이 패턴은 실패율이 높으므로 target을 과대 산정하지 않는다.

---

## 9. 손절가 산정 기준

### 9.1 stop_price의 역할

`stop_price`는 단순 손절률이 아니라:

```text
해당 신호 구조가 틀렸다고 판단할 수 있는 가격
```

이어야 한다.

---

### 9.2 공통 stop 후보

```text
신호일 저가
최근 3~10일 pivot low
20일선 하단 보정값
돌파 기준선 하단
박스 상단 이탈선
```

---

### 9.3 패턴별 stop_price

#### Pullback

```text
stop_price = min 또는 max 후보 중 구조적으로 가까운 유효 지지선
```

후보:

```text
signal_low
ma20 * 0.985
recent_pivot_low
```

---

#### Breakout

후보:

```text
breakout_level * 0.98
signal_low
box_upper * 0.98
```

---

#### Recovery Breakout

후보:

```text
recovery_level * 0.98
ma20 * 0.985
signal_low
recent_pivot_low
```

---

### 9.4 stop distance 검증

3~5일 보유형 기준으로 손절폭은 너무 넓어도 안 되고, 너무 좁아도 안 된다.

권장 기본값:

```yaml
min_stop_pct: 0.012   # 1.2%
max_stop_pct: 0.050   # 5.0%
preferred_stop_pct_range: [0.020, 0.040]
```

계산:

```python
stop_distance_pct = (entry_reference_price - stop_price) / entry_reference_price
```

판정:

| 조건 | 처리 |
|---|---|
| `stop_distance_pct <= 0` | INVALID_STOP |
| `stop_distance_pct < 1.2%` | TOO_TIGHT_STOP, WATCH_CAUTION |
| `1.2% <= stop_distance_pct <= 5.0%` | 정상 |
| `stop_distance_pct > 5.0%` | EXECUTABLE_PICKS 제외 |

---

## 10. EOD 추천 생성 판정 순서

EOD 추천 생성 시 반드시 아래 순서로 판정한다.

```text
1. 공통 제외 필터
2. 유효 패턴 판정
3. signal_score 계산
4. target_1 계산
5. stop_price 계산
6. entry_reference_price = signal_close 설정
7. rr_at_reference 계산
8. max_buy_price 계산
9. entry_status_eod 판정
10. final_score 계산
11. 그룹 분류
```

---

## 11. entry_status_eod 판정

EOD 단계에서는 `entry_reference_price = signal_close` 기준으로 판정한다.

```python
def classify_entry_status_eod(
    entry_reference_price: float,
    signal_close: float,
    target_1: float,
    stop_price: float,
    max_buy_price: float,
    rr_at_reference: float | None,
    rr_min: float = 1.5,
) -> str:
    if stop_price >= entry_reference_price:
        return "INVALID_STOP"

    if entry_reference_price <= stop_price:
        return "INVALIDATED"

    if target_1 <= entry_reference_price:
        return "TARGET_ALREADY_REACHED"

    if rr_at_reference is None:
        return "RR_INVALID"

    if entry_reference_price > max_buy_price:
        return "RR_COLLAPSED"

    if rr_at_reference < rr_min:
        return "RR_COLLAPSED"

    if entry_reference_price / signal_close - 1 >= 0.05:
        # EOD에서는 일반적으로 0이므로 장중 재평가용에 가까움
        return "EXTENDED"

    return "BUYABLE"
```

주의:

```text
EOD 단계에서는 signal_close와 entry_reference_price가 같으므로
EXTENDED는 일반적으로 발생하지 않는다.
EXTENDED는 다음날 시가/장중 재평가에서 주로 사용한다.
```

---

## 12. 다음날 가격 재평가 판정

다음날 시가 또는 장중 가격이 들어오면:

```python
entry_check_price = next_open_price or live_price
```

를 사용한다.

```python
def classify_entry_status_next_day(
    entry_check_price: float,
    signal_close: float,
    target_1: float,
    stop_price: float,
    max_buy_price: float,
    rr_min: float = 1.5,
) -> str:
    rr_now = calculate_rr(entry_check_price, target_1, stop_price)
    gap_pct = entry_check_price / signal_close - 1

    if entry_check_price <= stop_price:
        return "INVALIDATED"

    if entry_check_price >= target_1:
        return "TARGET_ZONE_REACHED"

    if entry_check_price > max_buy_price:
        return "RR_COLLAPSED"

    if gap_pct >= 0.05:
        return "EXTENDED"

    if rr_now is not None and rr_now >= rr_min:
        return "BUYABLE"

    return "WATCH_CAUTION"
```

---

## 13. 그룹 분류 기준

### 13.1 signal tier

`signal_score`, `rule_score`, `ml_probability`를 기준으로 신호 등급을 나눈다.

권장 기본값:

```yaml
strong_signal_min_score: 70
borderline_signal_min_score: 60
ml_probability_min_for_executable: 0.50
ml_probability_min_for_watch: 0.45
```

분류:

```python
if signal_score >= 70 and ml_probability >= 0.50:
    signal_tier = "STRONG_SIGNAL"
elif signal_score >= 60 and ml_probability >= 0.45:
    signal_tier = "BORDERLINE_SIGNAL"
else:
    signal_tier = "WEAK_OR_REJECTED"
```

---

### 13.2 EOD 그룹 분류

```python
if (
    signal_tier == "STRONG_SIGNAL"
    and entry_status_eod == "BUYABLE"
    and final_score >= 70
):
    group = "EXECUTABLE_PICKS"

elif signal_tier in ["STRONG_SIGNAL", "BORDERLINE_SIGNAL"] and entry_status_eod in [
    "RR_COLLAPSED",
    "TARGET_ALREADY_REACHED",
    "WATCH_CAUTION",
]:
    group = "VALID_SIGNALS"

elif signal_tier == "BORDERLINE_SIGNAL":
    group = "WATCHLIST"

else:
    group = "REJECTED"
```

---

## 14. 중요한 분리 원칙

### 14.1 RR 미달은 신호 무효가 아니다

RR 미달은:

```text
지금 가격 기준으로 신규 진입이 불리하다
```

는 뜻이다.

그러나 반드시:

```text
차트 신호가 완전히 틀렸다
```

는 뜻은 아니다.

따라서 다음을 분리한다.

```text
signal_quality      : 신호 자체 품질
entry_executability : 현재/기준 가격에서 실행 가능한가
```

---

### 14.2 최종 추천 0개와 유효 신호 0개를 구분

추천 메시지와 로그는 반드시 아래를 구분한다.

```text
유효 신호 수
실행 가능 추천 수
RR 붕괴 수
목표권 도달 수
관심 후보 수
완전 제외 수
```

예:

```text
유효 신호: 1개
실행 가능 추천: 0개
RR 붕괴: 1개
관심 후보: 0개
```

이 경우 메시지는:

```text
오늘 실행 가능한 신규 추천은 없지만,
유효 신호 1개가 손익비 부족으로 제외되었습니다.
```

처럼 표시한다.

---

## 15. 코스메카코리아 예시 판정

입력:

```yaml
symbol: "241710"
name: "코스메카코리아"
pattern: "recovery_breakout"
signal_close: 91200
entry_reference_price: 91200
target_1: 93132
stop_price: 88004
rr_min: 1.5
rule_score: 62
ml_probability: 0.494
```

계산:

```text
reward = 93,132 - 91,200 = 1,932
risk   = 91,200 - 88,004 = 3,196
RR     = 0.6045
```

`max_buy_price`:

```text
max_buy_price = (93,132 + 1.5 * 88,004) / 2.5
              = 90,055.2
```

비교:

```text
entry_reference_price 91,200 > max_buy_price 90,055
RR 0.6045 < 1.5
```

판정:

```yaml
entry_status_eod: RR_COLLAPSED
executable_pick: false
reason:
  - 오늘 종가 기준 손익비 1.5 미만
  - 오늘 종가가 최대 허용 진입가 초과
  - target_1까지 상방 여유가 너무 작음
  - stop_price까지 하방 위험이 상대적으로 큼
```

신호 분류:

```text
rule_score 62
ml_probability 49.4%
```

권장 분류:

```yaml
signal_tier: BORDERLINE_SIGNAL
recommended_group: VALID_SIGNALS 또는 WATCHLIST
```

만약 StockMaster가 `signal_score >= 70`, `ml_probability >= 50%`를 강제한다면:

```yaml
recommended_group: WATCHLIST 또는 REJECTED
```

으로 보내도 된다.

중요한 점:

```text
손익비 미달 때문에 EXECUTABLE_PICKS가 아닌 것은 정상이다.
다만 이 종목을 단순히 “신호 없음”으로 처리해서는 안 된다.
```

---

## 16. 추천 메시지 포맷

### 16.1 EXECUTABLE_PICKS 메시지

```text
[실행 가능 추천]
종목: {name}({symbol})
패턴: {pattern}

기준일 종가: {signal_close}원
다음날 매수 가능가: {max_buy_price}원 이하
1차 목표가: {target_1}원
2차 목표가: {target_2}원
손절/구조 훼손 기준: {stop_price}원

종가 기준 손익비: {rr_at_reference:.2f}
Signal Score: {signal_score}
Entry Score: {entry_score}
Final Score: {final_score}

판정: BUYABLE
설명: 오늘 종가 기준 손익비와 신호 품질이 기준을 충족했습니다.
```

---

### 16.2 VALID_SIGNALS 메시지

```text
[유효 신호 / 신규 진입 불가]
종목: {name}({symbol})
패턴: {pattern}

기준일 종가: {signal_close}원
최대 허용 진입가: {max_buy_price}원
1차 목표가: {target_1}원
손절 기준가: {stop_price}원
종가 기준 손익비: {rr_at_reference:.2f}

판정: {entry_status_eod}
설명: 신호는 포착되었으나, 오늘 종가 기준으로 손익비가 부족해 다음날 신규 추천에서는 제외합니다.
```

---

### 16.3 추천 0개 메시지

```text
오늘 기준 실행 가능한 신규 추천 종목은 없습니다.

진단 요약:
- 유효 신호: {valid_signal_count}개
- 실행 가능 추천: {executable_count}개
- 손익비 부족: {rr_collapsed_count}개
- 목표권 도달/상방 부족: {target_reached_count}개
- 관심 후보: {watchlist_count}개

해석:
신호가 없었던 것이 아니라,
기준 종가에서 손익비 또는 실행 가능 가격 조건을 충족하지 못했습니다.
```

---

## 17. 진단 로그 필수 항목

추천 결과 로그에는 반드시 아래를 남긴다.

```yaml
symbol: "241710"
name: "코스메카코리아"
signal_date: "YYYY-MM-DD"
execution_date: "YYYY-MM-DD"
price_basis: "EOD_SIGNAL_CLOSE"
signal_close: 91200
entry_reference_price: 91200
entry_reference_source: "signal_close"
target_1: 93132
target_2: null
stop_price: 88004
rr_min: 1.5
rr_at_reference: 0.6045
max_buy_price: 90055
entry_status_eod: "RR_COLLAPSED"
signal_score: 62
ml_probability: 0.494
final_score: null
group: "VALID_SIGNALS"
executable_pick: false
rejection_reasons:
  - "RR_BELOW_MIN"
  - "ENTRY_REFERENCE_ABOVE_MAX_BUY_PRICE"
```

---

## 18. 누적 게이트 진단 구조

기존 누적 하드게이트는 유지하되, Signal Gate와 Entry Gate를 분리한다.

```text
Universe:
전체 종목 2,359

Common Gate:
공통 제외 필터 통과 334

Signal Gate:
유효 스윙 패턴 10
signal_score 통과 3
strong signal 1
borderline signal 2

Entry Gate:
EOD 기준 BUYABLE 0
RR_COLLAPSED 1
TARGET_ALREADY_REACHED 0
INVALID_STOP 0

Final:
EXECUTABLE_PICKS 0
VALID_SIGNALS 1
WATCHLIST 2
REJECTED 2,356
```

이렇게 하면:

```text
종목 추천 로직이 정상적으로 후보를 찾았는지
찾았지만 가격 조건 때문에 제외했는지
```

명확히 구분할 수 있다.

---

## 19. 점수 계산 구조

### 19.1 signal_score

장마감 기준 신호 품질.

```text
signal_score =
rule_score * 0.55
+ ml_probability_score * 0.30
+ market_regime_score * 0.10
+ sector_score * 0.05
```

---

### 19.2 entry_score

EOD 기준 진입 가능성.

```text
entry_score =
rr_score * 0.45
+ price_location_score * 0.25
+ stop_distance_score * 0.15
+ target_room_score * 0.15
```

---

### 19.3 final_score

실행 가능 추천 전용 점수.

```text
final_score =
signal_score * 0.60
+ entry_score * 0.40
```

단:

```text
RR < 1.5이면 final_score가 높아도 EXECUTABLE_PICKS 불가
```

---

## 20. RR 관련 점수화 예시

```python
def rr_score(rr: float | None, rr_min: float = 1.5) -> float:
    if rr is None:
        return 0.0
    if rr <= 0:
        return 0.0
    if rr >= 2.5:
        return 100.0
    if rr >= rr_min:
        return 70.0 + (rr - rr_min) / (2.5 - rr_min) * 30.0
    return max(0.0, rr / rr_min * 70.0)
```

예:

```text
RR 0.60 → rr_score 약 28점
RR 1.50 → rr_score 70점
RR 2.00 → rr_score 85점
RR 2.50 이상 → rr_score 100점
```

---

## 21. 다음날 메시지에 표시할 기준

EOD 추천 메시지에는 다음날 사용자가 혼동하지 않도록 반드시 아래를 포함한다.

```text
이 추천은 오늘 종가 기준으로 계산되었습니다.
다음 거래일 가격이 max_buy_price 이하일 때만 유효합니다.
시가 또는 장중 가격이 max_buy_price를 초과하면 추격주의 또는 신규 진입 제외로 봅니다.
```

예:

```text
다음날 매수 가능 조건:
90,055원 이하에서만 유효

91,200원 이상에서는 손익비가 1.5 미만이므로 신규 진입 추천 제외
```

---

## 22. 구현 체크리스트

Codex는 다음 항목을 구현 또는 점검한다.

### 가격 변수 정리

- [ ] EOD 단계에서 `current_price` 사용 제거
- [ ] `entry_reference_price` 도입
- [ ] EOD에서는 `entry_reference_price = signal_close`
- [ ] 다음날 재평가에서는 `entry_check_price = next_open_price or live_price`

### RR 계산

- [ ] `calculate_rr()` 구현
- [ ] `calculate_max_buy_price()` 구현
- [ ] `rr_min = 1.5` config화
- [ ] `max_buy_price` 호가 단위 내림 처리
- [ ] `target_1 <= entry_reference_price` 예외 처리
- [ ] `stop_price >= entry_reference_price` 예외 처리

### 상태 분류

- [ ] `entry_status_eod` 구현
- [ ] `entry_status_next_day` 구현
- [ ] `RR_COLLAPSED` 추가
- [ ] `TARGET_ALREADY_REACHED` 추가
- [ ] `INVALID_STOP` 추가

### 그룹 분류

- [ ] EXECUTABLE_PICKS
- [ ] VALID_SIGNALS
- [ ] WATCHLIST
- [ ] REJECTED
- [ ] 추천 0개와 신호 0개 분리

### 메시지

- [ ] 기준일 종가 표시
- [ ] 최대 허용 진입가 표시
- [ ] 1차 목표가 표시
- [ ] 손절 기준가 표시
- [ ] 종가 기준 RR 표시
- [ ] 다음날 유효 조건 표시

### 진단 로그

- [ ] price_basis 저장
- [ ] entry_reference_source 저장
- [ ] rr_at_reference 저장
- [ ] max_buy_price 저장
- [ ] rejection_reasons 저장
- [ ] Signal Gate / Entry Gate 분리 출력

---

## 23. 테스트 케이스

### Case 1 — 정상 추천

```yaml
signal_close: 10000
target_1: 10600
stop_price: 9700
rr_min: 1.5
```

계산:

```text
reward = 600
risk = 300
RR = 2.0
max_buy_price = (10600 + 1.5 * 9700) / 2.5 = 10090
```

판정:

```text
signal_close 10000 <= max_buy_price 10090
RR 2.0 >= 1.5
→ BUYABLE 가능
```

---

### Case 2 — 손익비 붕괴

```yaml
signal_close: 91200
target_1: 93132
stop_price: 88004
rr_min: 1.5
```

판정:

```text
RR 0.6045
max_buy_price 90,055
signal_close 91,200 > max_buy_price
→ RR_COLLAPSED
```

---

### Case 3 — 목표 이미 도달

```yaml
signal_close: 10000
target_1: 9950
stop_price: 9700
```

판정:

```text
target_1 <= signal_close
→ TARGET_ALREADY_REACHED 또는 NO_UPSIDE
```

---

### Case 4 — 손절가 오류

```yaml
signal_close: 10000
target_1: 10600
stop_price: 10100
```

판정:

```text
stop_price >= signal_close
→ INVALID_STOP
```

---

## 24. 최종 지시 요약

Codex는 StockMaster의 EOD 추천 로직을 다음 기준으로 고정한다.

```text
1. 오늘 장마감 추천 생성의 기준가는 오늘 종가다.
2. EOD 단계에서는 entry_reference_price = signal_close로 계산한다.
3. 손익비는 target_1, stop_price, signal_close 기준으로 계산한다.
4. RR 최소 기준은 1.5다.
5. max_buy_price를 계산해 “얼마 이하일 때만 유효한 추천인지” 표시한다.
6. signal_score와 entry_score를 분리한다.
7. RR 미달은 신호 무효가 아니라 실행 불가 사유로 처리한다.
8. EXECUTABLE_PICKS와 VALID_SIGNALS를 분리한다.
9. 추천 0개와 유효 신호 0개를 절대 혼동하지 않는다.
10. 다음날 가격이 max_buy_price를 초과하면 추격주의 또는 신규 진입 제외로 표시한다.
```

---

## 25. 가장 중요한 한 줄

```text
StockMaster의 다음날 추천은 오늘 종가 기준으로 만들되,
오늘 종가 기준 손익비가 1.5 미만이면 실행 가능 추천이 아니라
유효 신호 또는 관심 후보로만 분류한다.
```
