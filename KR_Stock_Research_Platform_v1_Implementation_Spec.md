# KR Stock Research Platform v1 — 구현 확정 방향서

- 문서 목적: Codex가 프로젝트의 범위, 고정 의사결정, 데이터 정책, 아키텍처, 저장 전략, 모델 철학, UI 요구사항, 운영 스케줄을 오해 없이 이해하도록 하기 위한 구현 기준 문서
- 문서 버전: v1.0
- 기준 시점: 2026-03-07 (Asia/Seoul)
- 대상 독자: Codex, 개발자 본인, 후속 유지보수자
- 프로젝트 성격: 개인용/사설 리서치 플랫폼 (국내주식 우선)

---

## 1. 프로젝트 한 줄 정의

국내 주식 시장에 대해 **장 마감 후 자동으로 데이터 수집 → 시장 분위기 분석 → 종목 랭킹 산출 → 종목 카드 리포트 생성 → Discord 전송 → 다음 영업일 장 마감 후 실제 결과와 비교 평가**까지 수행하는, **단일 사용자용 로컬/클라우드 겸용 리서치 플랫폼**을 구축한다.

핵심은 “자동매매”가 아니라 **리서치, 설명 가능성, 재현성, 사후 평가**다.

---

## 2. 이번 문서 기준으로 확정된 의사결정

아래 항목은 **이미 확정된 방향**이며, Codex는 별도 질문 없이 이 방향을 기본 전제로 구현을 시작해야 한다.

### 2.1 제품 방향
- 대상 시장은 **국내 주식 시장(KOSPI/KOSDAQ)** 이다.
- 미국주식/나스닥 확장은 **v2 이후**로 미룬다.
- 초점은 **장후 리서치 플랫폼**이며, 실시간 장중 전략은 보조 기능으로 취급한다.
- 주요 산출물은 **예쁜 UI + 자동 보고서 + Discord 요약 전송 + 사후 평가 리포트**다.
- 사용 목적은 **개인용 리서치/의사결정 보조**다.

### 2.2 구현/운영 방향
- 개발은 **로컬 환경**에서 시작한다.
- 운영은 **Oracle Cloud Ubuntu/Oracle Linux + Docker**를 기준으로 설계한다.
- 저장공간은 **실사용 80GB 전제**로 설계한다.
- 단일 사용자용 private dashboard 기준으로 구현한다.
- 알림 채널은 우선 **Discord webhook**으로 고정한다.
- 이메일 전송은 v1 필수는 아니며, 구조만 확장 가능하게 만든다.

### 2.3 기술 정책
- **무료 또는 매우 저비용** 전제로 설계한다.
- **합법적인 데이터 접근 방식**만 사용한다.
- **공식 API 우선**, 스크래핑은 보조/실험 수준으로만 취급한다.
- 뉴스 본문 전문 저장/재배포는 하지 않는다.
- 원시 데이터 대량 재배포 시스템을 만들지 않는다.
- 자동 주문/실주문/자동매매 기능은 v1 범위에서 제외한다.

### 2.4 모델/리서치 방향
- “점수 합산식 = 실제 투자 의사결정식”으로 두지 않는다.
- **실제 엔진**은 다음을 기준으로 동작한다.
  - D+1 / D+5 **시장 대비 초과수익률 예측(alpha)**
  - **예측 불확실성(uncertainty)**
  - **모델 간 예측 불일치(disagreement)**
  - **거래비용/체결 가능성(implementation cost)**
  - **시장 장세(regime) 적합성**
- 사용자에게 보여주는 점수는 **설명 가능한 UI 계층(explanatory score)** 으로 분리한다.
- 즉, 내부 의사결정식과 외부 표시 점수는 분리한다.

---

## 3. 최종적으로 만들고자 하는 사용자 경험

### 3.1 매일 장 마감 후 기대 UX
사용자는 저녁에 대시보드나 Discord 메시지를 열었을 때 아래를 즉시 파악할 수 있어야 한다.

1. **오늘 국내 장 분위기**가 어땠는가
   - 공포/경계/중립/낙관/환희
   - 지수 흐름, 시장 폭, 거래대금, 외국인/기관 수급, 섹터 쏠림

2. **왜 이런 장세였는가**
   - 뉴스 이슈 클러스터 3~7개
   - 반도체/바이오/2차전지/방산 등 테마 단위 설명
   - 환율, 유가, 해외 리스크 요약

3. **내일 또는 5영업일 기준으로 볼 때 어떤 종목이 유리해 보이는가**
   - A / A- / B / C 등급
   - D+1 기대 시나리오
   - D+5 기대 시나리오
   - 상승 기대값은 높지만 변동성도 큰 종목 vs 상대적으로 안정적인 종목 구분

4. **개별 종목을 왜 좋게/나쁘게 봤는가**
   - 추세, 거래대금, 수급, 재무, 뉴스, 리스크를 카드 형태로 설명
   - 기대값과 불확실성을 같이 보여줌

5. **어제의 예측이 실제로 맞았는가**
   - 상위 추천 종목 수익률
   - 히트율
   - 오탐/미탐 원인
   - 개선 포인트

---

## 4. v1 범위와 비범위

## 4.1 v1 범위 (반드시 포함)
- 국내 주식 대상 종목 유니버스 관리
- 일봉/재무/공시/뉴스 메타데이터 수집
- 전 종목 장후 피처 생성
- 장세(regime) 추정
- D+1 / D+5 예측용 피처셋 생성
- 종목 랭킹 및 등급 산출
- 종목 카드 리포트 생성
- 시장 요약 리포트 생성
- Discord 요약 메시지 전송
- 다음 영업일 및 5영업일 후 평가 리포트 생성
- Streamlit 기반 대시보드
- DuckDB/Parquet 기반 데이터 레이크
- 실행 이력(run manifest) 저장
- 디스크 워터마크 및 로그 회전

## 4.2 v1 비범위 (의도적으로 제외)
- 자동 주문/자동매매
- HTS/MTS 연동 주문 체결
- 멀티유저/권한관리
- 결제/과금
- 미국주식/나스닥 실운영
- 전 종목 틱 원시데이터 장기보관
- 전 종목 호가 원본 장기보관
- 뉴스 본문 전문 저장
- 초저지연 장중 전략 엔진

---

## 5. 데이터 소스 정책 (매우 중요)

Codex는 데이터 레이어를 만들 때 반드시 아래 원칙을 따른다.

### 5.1 소스 우선순위
1. **공식 API / 공식 데이터 서비스**
2. 공식이 부족한 경우의 보조 소스
3. 실험/개발 편의용 스크래핑

### 5.2 v1에서 기본 채택하는 소스
- **한국투자 Open API (KIS)**
  - 시세/분봉/체결/호가/투자자/프로그램/공매도 관련 소스의 1순위
- **OpenDART**
  - 재무제표, 공시, 기업 고유번호, 주요 재무정보의 기준 소스
- **KRX Data Marketplace / OPEN API**
  - 시장 통계, 보조 참조 데이터, 단일 이슈별 통계 보강
- **Naver News Search API**
  - 뉴스 제목/요약조각/링크/발행시각 수집
- **pykrx**
  - 프로토타입/검증용 보조만 허용
  - 운영 기준값(source of truth)으로 사용하지 않는다

### 5.3 뉴스 저장 원칙
뉴스는 다음만 저장한다.
- 제목
- 원문 링크
- 제공처(언론사)
- 발행 시각
- 검색 질의/키워드
- 태그
- 내부 생성 요약
- 감성/촉매 스코어

저장하지 않는 것:
- 뉴스 본문 전문 대량 저장
- 기사 HTML 덤프 장기보관

### 5.4 법적/운영적 주의사항
- 데이터 제공처의 이용약관/호출 제한을 전제로 설계한다.
- Discord로는 **원시 대량 데이터**가 아니라 **파생 지표/요약/차트/링크**를 보낸다.
- 원시 API 응답은 디버깅/감사 목적으로만 짧게 보관한다.
- 운영 전 실제 약관 확인 절차를 반드시 별도로 수행한다.

---

## 6. 80GB 저장공간 기준 저장 전략

이 시스템의 저장 철학은 다음과 같다.

> “모든 원시 데이터를 영구 저장하는 시스템”이 아니라,
> “판단 당시의 상태를 나중에 재현할 수 있게 만드는 시스템”을 만든다.

### 6.1 저장공간 총량 목표
- 목표 사용량: **80GB 이내**
- 경고 기준: 70%
- 정리 예약 기준: 80%
- 고빈도 수집 제한 기준: 90%

### 6.2 권장 저장 배분
- 7GB: OS / Docker 이미지 / 앱 코드 / 가상환경
- 8GB: 종목마스터 / 일봉 / 재무 / 공시 정규화 결과
- 7GB: 뉴스 메타데이터 / 태그 / 요약 / 이슈 클러스터 캐시
- 18GB: 피처 스토어 / 라벨 / 예측 스냅샷 / 평가결과 / 백테스트 산출물
- 12GB: 전 종목 5분봉
- 10GB: 후보군 1분봉 + 체결/호가 요약
- 4GB: 리포트 HTML / 차트 캐시 / Discord 산출물
- 4GB: 로그 / 임시파일
- 10GB: 안전 버퍼

### 6.3 영구 보관 데이터
- 종목 유니버스 마스터
- 거래일 캘린더
- 일봉 OHLCV
- 재무/공시 정규화 테이블
- 뉴스 메타데이터
- 피처 스냅샷
- 예측값 / 점수 / 등급 / 시나리오 밴드
- 리포트 결과물(텍스트/HTML/메타데이터)
- D+1 / D+5 평가결과
- run manifest

### 6.4 기간 제한 보관 데이터
- 전 종목 5분봉: 90일 기본
- 후보군 1분봉: 60일 기본
- 후보군 체결/호가 요약: 30일 기본
- 원시 API 응답 로그: 7일 기본
- 차트 캐시: 7일 기본
- 운영 로그: 30일 기본

### 6.5 장기 보관하지 않을 데이터
- 전 종목 틱 원본 장기 dump
- 전 종목 호가 원본 장기 dump
- 뉴스 본문 전문 저장
- 웹소켓 패킷 전체 장기저장

---

## 7. 시스템 아키텍처

```text
[External Data Sources]
  ├─ KIS Open API
  ├─ OpenDART
  ├─ KRX Data Marketplace / OPEN API
  └─ Naver News Search API

          ↓

[Ingestion Layer]
  ├─ provider_kis
  ├─ provider_dart
  ├─ provider_krx
  └─ provider_naver_news

          ↓

[Raw Storage Layer]
  ├─ /data/raw/... (json/parquet)
  └─ request logs / response snapshots (short TTL)

          ↓

[Curated Layer]
  ├─ /data/curated/market
  ├─ /data/curated/fundamentals
  ├─ /data/curated/news
  ├─ /data/curated/features
  └─ /data/curated/predictions

          ↓

[DuckDB Semantic Layer]
  ├─ dimensions
  ├─ feature views
  ├─ ranking views
  └─ evaluation views

          ↓

[Research / Decision Engine]
  ├─ feature builder
  ├─ regime detector
  ├─ alpha predictor
  ├─ uncertainty estimator
  ├─ implementation-cost penalty
  └─ ranking + grade assignment

          ↓

[Presentation Layer]
  ├─ Streamlit dashboard
  ├─ HTML report renderer
  └─ Discord webhook sender

          ↓

[Evaluation Layer]
  ├─ D+1 evaluator
  ├─ D+5 evaluator
  └─ miss-analysis / lesson generator
```

### 7.1 핵심 설계 원칙
- UI와 수집/계산 워커를 분리한다.
- ETL과 예측은 백그라운드 작업으로 수행한다.
- Streamlit은 조회/탐색/보고서 확인용이다.
- 모든 주요 산출물은 `run_id` 단위로 재현 가능해야 한다.

### 7.2 단일 사용자 private 운영 전제
- 인증/권한은 v1에서 최소화한다.
- 외부 공개 서비스가 아니라 개인용 운영을 기본으로 한다.
- UI는 localhost 또는 private cloud endpoint 기준으로 설계한다.

---

## 8. 리서치/모델 철학 (Codex가 반드시 지켜야 하는 방향)

이 프로젝트에서 **보이는 점수**와 **실제 선별 엔진**은 동일하지 않다.

### 8.1 실제 내부 선별 개념
내부적으로는 아래 개념을 구현한다.

```text
SelectionValue(h) = ExpectedExcessReturn(h)
                    - λ * PredictiveUncertainty(h)
                    - η * ModelDisagreement(h)
                    - κ * ImplementationCost(h)
                    + ρ * RegimeFit(h)
```

여기서:
- `h`는 예측 호라이즌 (D+1 또는 D+5)
- `ExpectedExcessReturn` = 시장 대비 초과수익률 기대값
- `PredictiveUncertainty` = 예측 구간폭 또는 불확실성
- `ModelDisagreement` = 모델 간 예측 차이
- `ImplementationCost` = 유동성/스프레드/회전율/체결 난이도 패널티
- `RegimeFit` = 현재 장세에서 해당 스타일이 유리한 정도

### 8.2 설명용 점수 계층
사용자에게는 아래처럼 이해 가능한 카테고리 점수로 보여준다.

#### D+5 스윙형 설명 점수 예시
- Core Alpha: 35
- Trend / Momentum: 15
- Flow / Liquidity: 10
- Quality / Value: 15
- News Catalyst: 10
- Regime Fit: 15
- Uncertainty Penalty: -15
- Implementation Penalty: -10

#### D+1 전술형 설명 점수 예시
- Core Alpha: 20
- Intraday / Microstructure: 20
- Flow: 15
- News / Event: 15
- Regime Fit: 15
- Quality / Value: 5
- Uncertainty Penalty: -15
- Implementation Penalty: -10

> 주의: 위 점수표는 **실제 학습식이 아니라 UI 설명용**이다.

### 8.3 목표값(타깃) 설계 원칙
- 예측 타깃은 단일 목표가가 아니라 **D+1 / D+5 초과수익률** 중심으로 둔다.
- 표시 레이어에서는 이를 가격 밴드 또는 시나리오로 번역한다.
- 예측치는 항상 **확률/밴드/불확실성**과 함께 보여준다.

### 8.4 등급 체계
- **A-안정형**: 기대값이 높고 불확실성/비용이 상대적으로 낮음
- **A-촉매형**: 기대값이 높지만 변동성과 이벤트 민감도가 큼
- **B**: 관찰 가치 있음, 진입은 선택적
- **C**: 기대값 대비 불확실성/비용/리스크가 커서 제외

### 8.5 목표가 표기 정책
단일 목표가 숫자를 남발하지 않는다.
대신 다음을 기본 노출값으로 삼는다.
- D+1 기대 초과수익률
- D+5 기대 초과수익률
- 상승 확률 / 하락 확률
- 25% / 50% / 75% 시나리오 밴드
- 리스크 주석

---

## 9. 피처 엔지니어링 요구사항

Codex는 피처 시스템을 “나중에 붙일 수 있게”가 아니라 **처음부터 확장 가능한 형태**로 설계해야 한다.

### 9.1 피처 그룹

#### A. Trend / Momentum
예시:
- 5일 / 20일 / 60일 수익률
- 20일 신고가 대비 거리
- 이동평균 정배열 여부
- 상대강도 (vs KOSPI/KOSDAQ / 섹터)
- 최근 5일/20일 모멘텀 가속도
- 갭 상승 후 지속 여부

#### B. Liquidity / Turnover
예시:
- 거래대금
- 시가총액 대비 거래대금 비율
- 회전율
- 평균 거래대금 대비 급증 비율
- 5일 / 20일 거래대금 z-score

#### C. Flow
예시:
- 외국인 순매수/순매도
- 기관 순매수/순매도
- 프로그램 순매수
- 주체별 연속 순매수 일수
- 수급 강도 변화율

#### D. Quality / Fundamentals
예시:
- 매출 성장률
- 영업이익 성장률
- 영업이익률
- ROE
- 부채비율
- 이자보상배율
- FCF proxy
- 실적 서프라이즈 관련 지표

#### E. Value / Safety
예시:
- PER / PBR / PSR / EV 계열 (가능 범위)
- 동종업종 대비 밸류 괴리
- 변동성 대비 기대수익
- 낙폭 과대 반등 조건 vs 구조적 약세 구분 지표

#### F. News Catalyst
예시:
- 최근 1일 / 3일 / 7일 뉴스 건수
- 긍정/부정 이벤트 점수
- 실적/수주/규제/임상/정책 키워드
- 뉴스 신선도(decay)
- 단일 이슈 과열 여부

#### G. Risk / Penalty
예시:
- 변동성 급증
- 관리종목 / 거래정지 / 투자경고/위험/주의 플래그
- 유동성 부족
- 갭 과열
- 급등 후 이격도 과다
- 재무 리스크 플래그
- 뉴스 과열 대비 실적 부재

#### H. Regime Features
예시:
- 지수 breadth
- 상승/하락 종목 비율
- 지수 실현변동성
- 원/달러, 유가 등 매크로 프록시
- 외국인 수급 추세
- 섹터 집중도
- 단기 panic / unwind / trend / range 상태 분류

#### I. Candidate Intraday Summary (후보군 전용)
예시:
- 1분봉 수익률 구조
- 시가/고가/저가 위치
- VWAP 괴리
- 장초반 거래 집중도
- 체결강도 요약
- 호가 불균형 요약

### 9.2 피처 생성 원칙
- 미래 데이터 누수 금지
- 공시/재무는 **실제 이용 가능한 시점 기준**으로만 사용
- 정정공시로 값이 바뀌는 경우를 염두에 두고 버전 기록
- 모든 피처는 `as_of_date`, `as_of_ts`, `run_id`, `source_version`를 남긴다

---

## 10. 예측/평가 설계 원칙

### 10.1 호라이즌
- 기본 예측 호라이즌: **D+1, D+5**
- D+1 = 전술형/단기 촉매 중심
- D+5 = 스윙형/추세 지속 중심

### 10.2 우선 구현 모델 스택
v1에서는 아래 구조를 기준으로 한다.
- Elastic Net
- LightGBM
- Gradient Boosting Regressor / XGBoost 대체 가능 구조
- 소형 MLP (선택)
- 앙상블 평균 또는 가중 앙상블

### 10.3 v1 목표
- 최첨단 딥러닝 SOTA 구현이 아니라
- **재현 가능하고 검증 가능한 베이스라인 엔진**을 만드는 것

### 10.4 예측 산출물
각 종목, 각 호라이즌별로 최소한 다음을 저장한다.
- expected_excess_return
- expected_raw_return (선택)
- uncertainty_lower
- uncertainty_median
- uncertainty_upper
- disagreement_score
- implementation_penalty
- regime_fit_score
- final_selection_value
- grade
- explanation_json

### 10.5 평가 항목
- 상위 N종목 평균 성과
- 시장 대비 초과성과
- hit ratio
- turnover
- max drawdown proxy
- grade별 실제 성과 분포
- uncertainty calibration
- 실패 원인 태깅

### 10.6 평가 문서의 목적
단순히 “맞았다/틀렸다”가 아니라 아래를 자동으로 남긴다.
- 무엇이 잘 맞았는가
- 어떤 유형에서 자주 실패하는가
- 불확실성 경고가 실제로 유효했는가
- 뉴스 촉매가 과대평가되었는가
- 거래대금/체결성 패널티가 충분했는가

---

## 11. UI / 대시보드 요구사항

UI는 보기 좋아야 한다. 다만 “화려함”보다 **읽기 쉬움 / 정보 계층 / 빠른 스캔**이 더 중요하다.

### 11.1 UI 프레임워크
- Streamlit 사용
- 멀티페이지 구조
- 다크/라이트는 v1에서 필수 아님
- 반응형까지 강제하진 않지만, 1440px 기준에서 보기 좋아야 함

### 11.2 필수 페이지

#### 1) Market Pulse
내용:
- KOSPI/KOSDAQ 요약
- 시장 폭(breadth)
- 외국인/기관 수급
- 거래대금
- 섹터/테마 히트맵
- 오늘의 장세 라벨(공포/경계/중립/낙관/환희)
- 핵심 뉴스 클러스터
- 오늘 장에 대한 한 줄 해석

#### 2) Leaderboard
내용:
- 상위 랭킹 종목 리스트
- A/A-/B/C 등급
- D+1 / D+5 기대 밴드
- 선택값(final_selection_value)
- 핵심 이유 태그 3~5개
- 리스크 플래그
- 정렬/필터 기능

#### 3) Stock Workbench
내용:
- 개별 종목 상세 카드
- 가격 차트
- 거래대금/수급 차트
- 재무 요약
- 뉴스 타임라인
- 기술지표 설명
- 모델 설명 점수
- 리스크 경고

#### 4) Evaluation
내용:
- 전일 추천 종목 결과
- D+1 성과표
- 과거 누적 성과
- 등급별 실현 성과
- 실패 사례 분석
- 개선 코멘트

#### 5) Ops
내용:
- 최근 실행 이력
- 데이터 수집 성공/실패 현황
- 디스크 사용량
- 캐시 정리 상태
- API 에러 현황
- 최근 Discord 전송 상태

### 11.3 UI 스타일 방향
- 카드형 컴포넌트 적극 활용
- 숫자만 나열하지 말고 상태 뱃지/태그/색상 계층 사용
- 경고/리스크는 시각적으로 분명히 표시
- 차트는 Plotly 중심
- 표는 너무 빽빽하지 않게 구성
- 종목명 클릭 시 상세 페이지 이동 가능하게 설계

---

## 12. Discord 리포트 요구사항

Discord는 전체 대시보드의 요약본 역할을 한다.

### 12.1 기본 메시지 구조
1. 헤더
   - 날짜
   - 시장 라벨
   - 주요 지수 요약

2. 시장 요약
   - 상승/하락 종목 비율
   - 거래대금
   - 외국인/기관 수급
   - 핵심 장세 코멘트 2~4줄

3. 탑 랭킹 종목
   - 상위 5~10개
   - 등급
   - 핵심 사유 태그
   - D+1 / D+5 기대밴드 요약

4. 주요 뉴스/이슈
   - 3~5개
   - 제목 + 간단 요약 + 링크

5. 리스크 메모
   - 과열/변동성/이벤트 리스크

### 12.2 전송 원칙
- 너무 긴 전문 리포트를 Discord에 다 보내지 않는다.
- 대시보드 링크 또는 HTML 리포트 경로를 함께 제공할 수 있게 설계한다.
- 원시 대량 데이터가 아니라 요약/파생지표만 보낸다.

---

## 13. 배치 스케줄 요구사항

기본 timezone은 **Asia/Seoul**로 고정한다.

### 13.1 일일 스케줄 초안
- 06:30 — 거래일 캘린더/종목 유니버스 동기화
- 07:00 — 기초 참조 데이터 갱신
- 08:00 — 뉴스 백필/전일 누락분 정리
- 16:05 — 장 마감 후 일봉/시장 데이터 수집 시작
- 16:20 — 뉴스/수급/프로그램/공매도 보강 수집
- 17:00 — 피처 생성
- 17:30 — 장세(regime) 추정
- 17:40 — 예측 및 랭킹 산출
- 18:00 — HTML/대시보드 산출물 생성
- 18:30 — Discord 전송
- 다음 영업일 16:20 — D+1 평가 수행
- 5영업일 후 16:20 — D+5 평가 수행

### 13.2 스케줄링 기술
- APScheduler 또는 cron-compatible 방식
- 작업 단위는 idempotent 하게 설계
- 동일 날짜 재실행을 허용하되 `run_id`는 별도 발급

---

## 14. 저장 구조 / 파일 구조 요구사항

## 14.1 리포지토리 최상위 구조

```text
kr-stock-research/
├─ README.md
├─ pyproject.toml
├─ poetry.lock or uv.lock
├─ .env.example
├─ docker-compose.yml
├─ Dockerfile
├─ Makefile
├─ config/
│  ├─ settings.yaml
│  ├─ logging.yaml
│  └─ retention.yaml
├─ app/
│  ├─ __init__.py
│  ├─ main.py
│  ├─ settings.py
│  ├─ logging.py
│  ├─ common/
│  ├─ providers/
│  │  ├─ base.py
│  │  ├─ kis/
│  │  ├─ dart/
│  │  ├─ krx/
│  │  └─ naver_news/
│  ├─ ingestion/
│  ├─ storage/
│  ├─ features/
│  ├─ models/
│  ├─ ranking/
│  ├─ reports/
│  ├─ notifications/
│  ├─ evaluation/
│  ├─ scheduler/
│  ├─ ops/
│  └─ ui/
│     ├─ Home.py
│     └─ pages/
├─ scripts/
│  ├─ bootstrap.py
│  ├─ run_daily_pipeline.py
│  ├─ run_evaluation.py
│  ├─ prune_storage.py
│  └─ backfill_*.py
├─ data/
│  ├─ raw/
│  ├─ curated/
│  ├─ marts/
│  ├─ cache/
│  ├─ logs/
│  └─ artifacts/
├─ tests/
│  ├─ unit/
│  ├─ integration/
│  └─ smoke/
└─ docs/
   ├─ architecture/
   ├─ tickets/
   └─ decisions/
```

### 14.2 데이터 디렉터리 파티셔닝 권장안
예시:

```text
data/raw/kis/daily_ohlcv/date=2026-03-06/*.parquet
data/raw/kis/intraday_5m/date=2026-03-06/*.parquet
data/raw/naver/news/date=2026-03-06/*.parquet
data/curated/features/as_of_date=2026-03-06/*.parquet
data/curated/predictions/run_date=2026-03-06/horizon=D1/*.parquet
data/artifacts/reports/date=2026-03-06/*.html
```

---

## 15. DuckDB / 데이터 모델 요구사항

Codex는 초기 버전에서 아래 테이블/뷰를 최소 구현해야 한다.

## 15.1 Dimension 테이블

### dim_symbol
필드 예시:
- symbol
- company_name
- market
- sector
- industry
- listing_date
- is_common_stock
- is_etf
- is_etn
- is_spac
- is_delisted
- status_flags
- updated_at

### dim_trading_calendar
필드 예시:
- trading_date
- is_trading_day
- market_session_type
- prev_trading_date
- next_trading_date

## 15.2 Raw / Curated 테이블

### fact_daily_ohlcv
- trading_date
- symbol
- open
- high
- low
- close
- volume
- turnover_value
- market_cap
- source
- ingested_at

### fact_intraday_5m
- bar_ts
- trading_date
- symbol
- open
- high
- low
- close
- volume
- turnover_value
- source
- ingested_at

### fact_candidate_intraday_1m
- bar_ts
- trading_date
- symbol
- open
- high
- low
- close
- volume
- turnover_value
- vwap_proxy
- source
- ingested_at

### fact_investor_flow
- trading_date
- symbol
- foreign_net_buy
- institution_net_buy
- program_net_buy
- source
- ingested_at

### fact_short_interest_or_short_activity
- trading_date
- symbol
- short_value
- short_ratio
- source
- ingested_at

### fact_fundamentals_snapshot
- as_of_date
- symbol
- fiscal_year
- report_code
- revenue
- operating_income
- net_income
- roe
- debt_ratio
- operating_margin
- source_doc_id
- source
- ingested_at

### fact_news_item
- news_id
- published_at
- symbol_candidates
- query_keyword
- title
- publisher
- link
- snippet
- tags_json
- catalyst_score
- sentiment_score
- freshness_score
- source
- ingested_at

### fact_feature_snapshot
- run_id
- as_of_date
- symbol
- feature_name
- feature_value
- feature_group
- source_version
- created_at

### fact_prediction
- run_id
- as_of_date
- symbol
- horizon
- model_name
- expected_excess_return
- lower_band
- median_band
- upper_band
- disagreement_score
- uncertainty_score
- implementation_penalty
- regime_fit_score
- created_at

### fact_ranking
- run_id
- as_of_date
- symbol
- horizon
- final_selection_value
- grade
- explanatory_score_json
- top_reason_tags_json
- risk_flags_json
- created_at

### fact_evaluation
- eval_run_id
- origin_run_id
- as_of_date
- eval_date
- symbol
- horizon
- predicted_grade
- predicted_selection_value
- realized_return
- realized_excess_return
- hit_flag
- evaluation_note_json
- created_at

### ops_run_manifest
- run_id
- run_type
- as_of_date
- started_at
- finished_at
- status
- input_sources_json
- output_artifacts_json
- model_version
- feature_version
- git_commit
- notes

### ops_disk_usage_log
- measured_at
- mount_point
- used_gb
- available_gb
- usage_ratio
- action_taken

---

## 16. 환경변수(.env) 요구사항

`.env.example`에는 최소 아래 키가 포함되어야 한다.

```env
APP_ENV=local
APP_TIMEZONE=Asia/Seoul
APP_LOG_LEVEL=INFO
APP_DATA_DIR=./data
APP_DUCKDB_PATH=./data/marts/main.duckdb

KIS_APP_KEY=
KIS_APP_SECRET=
KIS_ACCOUNT_NO=
KIS_PRODUCT_CODE=
KIS_USE_MOCK=false

DART_API_KEY=

NAVER_CLIENT_ID=
NAVER_CLIENT_SECRET=

KRX_API_KEY=

DISCORD_WEBHOOK_URL=
DISCORD_USERNAME=KR Stock Research Bot

STORAGE_WARNING_RATIO=0.70
STORAGE_PRUNE_RATIO=0.80
STORAGE_LIMIT_RATIO=0.90

RETENTION_RAW_API_DAYS=7
RETENTION_INTRADAY_5M_DAYS=90
RETENTION_INTRADAY_1M_DAYS=60
RETENTION_ORDERBOOK_SUMMARY_DAYS=30
RETENTION_REPORT_CACHE_DAYS=7
RETENTION_LOG_DAYS=30

MODEL_DEFAULT_HORIZONS=D1,D5
MODEL_UNCERTAINTY_LAMBDA=1.0
MODEL_DISAGREEMENT_ETA=1.0
MODEL_IMPLEMENTATION_KAPPA=1.0
MODEL_REGIME_RHO=1.0
```

---

## 17. 운영/장애 대응 요구사항

### 17.1 반드시 필요한 운영 기능
- structured logging
- run_id 기반 추적
- 재시도(retry) 정책
- API rate-limit 대응
- 디스크 사용량 모니터링
- 오래된 캐시/로그 자동 정리
- 실패한 작업의 상태 기록
- 동일 날짜 재실행 가능

### 17.2 실패 시 기본 동작 원칙
- 일부 데이터 소스 실패 시 전체 파이프라인이 무조건 중단되지 않도록 한다.
- 단, 핵심 소스(KIS, DART)가 실패하면 리포트 품질 경고를 표시한다.
- Discord 메시지에는 “데이터 일부 누락” 배지를 붙일 수 있도록 한다.

### 17.3 디스크 워터마크 정책
- 70% 초과: Ops 경고
- 80% 초과: 차트 캐시/임시파일/원시응답 정리
- 90% 초과: 후보군 고빈도 수집 중지 또는 축소

---

## 18. 품질 기준 / Definition of Done

v1의 첫 실사용 가능 상태는 아래를 만족해야 한다.

1. `docker compose up`으로 로컬 구동 가능
2. Streamlit 대시보드 접속 가능
3. 일봉/재무/뉴스/기본 시장 데이터 수집 가능
4. 장후 파이프라인 1회 실행 가능
5. 종목 랭킹 산출 가능
6. 리포트 HTML 생성 가능
7. Discord 요약 전송 가능
8. D+1 평가 수행 가능
9. 모든 주요 산출물에 `run_id` 존재
10. 디스크 사용량/로그/실패 작업이 Ops 화면에서 확인 가능

---

## 19. 구현 우선순위

### Phase 0 — Foundation
- 저장소/도커/설정/로깅/디렉터리/DB 부트스트랩
- Provider 인터페이스
- Run manifest
- Streamlit 기본 골격

### Phase 1 — Core Data
- 종목마스터
- 거래일 캘린더
- 일봉
- 재무/공시
- 뉴스 메타데이터

### Phase 2 — Research Engine
- 피처 스토어
- 기본 장세(regime)
- 랭킹 엔진 v1
- 설명 점수 생성

### Phase 3 — Reporting
- Market Pulse
- Leaderboard
- Stock Workbench
- HTML 리포트
- Discord 전송

### Phase 4 — Evaluation
- D+1 / D+5 평가
- 실패 원인 자동 태깅
- 누적 성과 화면

### Phase 5 — Candidate Intraday
- 후보군 1분봉
- 체결/호가 요약
- 전술형 D+1 보정

---

## 20. Codex가 절대 오해하면 안 되는 핵심 메모

1. 이 프로젝트는 **자동매매 프로젝트가 아니다**.
2. v1의 핵심은 **장후 리포트 품질, 설명 가능성, 재현성, 사후 평가**다.
3. “예쁜 UI”는 중요하지만, 데이터 품질/실행 이력/평가 체계보다 우선하지 않는다.
4. 점수판을 먼저 만들되, 내부적으로는 **초과수익 + 불확실성 + 비용 + 장세 적합성** 구조를 염두에 둔다.
5. 전 종목 고빈도 데이터를 영구 저장하지 않는다.
6. 80GB 내에서 오래 운영 가능해야 한다.
7. UI와 워커는 분리한다.
8. 모든 핵심 결과는 `run_id` 기준으로 재현 가능해야 한다.
9. 사용자가 다음날 “왜 이 종목이 A였지?”라고 물었을 때 설명할 수 있어야 한다.
10. 초기 구현은 완벽한 예측력보다 **안정적인 플랫폼 기반**이 더 중요하다.

---

## 21. 외부 레퍼런스 링크 (구현 시 참고)

> 아래 링크는 구현 참고용이며, 실제 사용 전 약관/제한 확인은 별도로 다시 수행한다.

### 공식 데이터/API
- Oracle Cloud Always Free: https://docs.oracle.com/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm
- 한국투자 Open API 포털: https://apiportal.koreainvestment.com/
- 한국투자 Open API 서비스 소개: https://apiportal.koreainvestment.com/apiservice
- OpenDART 소개: https://opendart.fss.or.kr/intro/main.do
- OpenDART 개발가이드: https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
- OpenDART 오류코드/제한 예시: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DE003&apiId=AE00036
- 네이버 뉴스 검색 API: https://developers.naver.com/docs/serviceapi/search/news/news.md
- 네이버 오픈 API 목록: https://developers.naver.com/docs/common/openapiguide/apilist.md
- KRX Data Marketplace: https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd

### 프레임워크 / 저장 / 스케줄링
- Streamlit multipage apps: https://docs.streamlit.io/develop/concepts/multipage-apps/overview
- Streamlit config: https://docs.streamlit.io/develop/api-reference/configuration/config.toml
- DuckDB Parquet docs: https://duckdb.org/docs/stable/data/parquet/overview.html
- APScheduler CronTrigger: https://apscheduler.readthedocs.io/en/stable/modules/triggers/cron.html

---

## 22. 이 문서의 사용법

- Codex에게 이 문서를 먼저 읽히고, 그 다음 **티켓 문서**를 준다.
- Codex가 질문을 줄이기 위해서는 이 문서를 기준 spec으로 사용한다.
- 구현 중 새로운 결정이 생기면 `docs/decisions/ADR-*.md` 형식으로 남긴다.

