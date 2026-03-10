# StockMaster 사용자 설명서 (한글)

## 1. 문서 목적

이 문서는 StockMaster를 **실제로 사용하는 사람** 기준으로 작성한 한글 사용자 설명서입니다.

대상 독자:
- 로컬 또는 클라우드에서 StockMaster를 직접 운영하는 사용자
- 장후 리포트, 장중 후보군 콘솔, 포트폴리오, 백테스트, 운영 대시보드를 이용하려는 사용자
- 개발자가 아닌 일반 사용자 또는 비전공 투자자

이 문서는 “기능이 무엇인지” 뿐 아니라 **어떻게 써야 하는지**, **어떤 화면을 먼저 봐야 하는지**, **무엇을 믿고 무엇을 경계해야 하는지**까지 안내합니다.

---

## 2. StockMaster가 하는 일

StockMaster는 국내주식 리서치와 검증을 위한 개인용 플랫폼입니다.

주요 기능:
- 국내주식 장후 데이터 수집
- 뉴스/재무/기술/수급/장세 요약
- 종목 랭킹과 후보군 제안
- D+1 / D+5 기대값 기반 판단 보조
- 장중 후보군 진입 타이밍 보조
- 사후 평가와 calibration 리포트
- 포트폴리오 제안과 리스크 버짓 관리
- 백테스트와 비교 실험
- Discord 장후/사후 리포트 발송

중요:
- 이 시스템은 **자동 주문 프로그램이 아닙니다.**
- 이 시스템은 **투자 자문 서비스가 아닙니다.**
- 이 시스템은 **판단 보조와 연구/검증 플랫폼** 입니다.

---

## 3. 시작 전 알아둘 점

### 3.1 가장 먼저 이해해야 할 것
StockMaster의 추천은 다음 순서로 만들어집니다.

1. 데이터 수집
2. 피처 계산
3. 점수/예측/정책 생성
4. 후보군 및 포트폴리오 제안
5. 다음날/5일 후 결과 비교
6. 실패/성공 원인 축적

즉, **예측만 하는 프로그램이 아니라, 예측과 검증을 같이 하는 프로그램** 입니다.

### 3.2 추천을 그대로 매수하라는 뜻이 아님
Leaderboard의 A등급 종목은 “좋아 보이는 종목”이지, 무조건 사야 하는 종목이 아닙니다.
반드시 아래를 함께 보세요.

- uncertainty / disagreement
- implementation penalty
- intraday assist 결과
- portfolio cap 여부
- 최근 장세(regime)

---

## 4. 시스템 접속 방법

### 로컬 실행
보통 로컬에서는 아래 순서로 실행합니다.

```powershell
cd D:\MyApps\StockMaster
docker compose up -d --build
```

실행 후 브라우저에서 기본 주소로 접속합니다.

```text
http://localhost:8501
```

### 클라우드 실행
클라우드(예: Oracle Cloud)에 배포한 경우에는 다음 중 하나로 접속합니다.

- 공인 IP + 포트
- 도메인 + reverse proxy
- HTTPS 설정이 된 외부 주소

---

## 5. 첫 화면에서 무엇을 봐야 하나요?

StockMaster를 열면 보통 아래 순서로 보는 것을 권장합니다.

1. **Ops / Health**
   - 어젯밤 배치가 정상 완료되었는지 확인
2. **Market Pulse**
   - 오늘/어제 장 분위기 확인
3. **Leaderboard**
   - 오늘의 상위 후보군 확인
4. **Stock Workbench**
   - 관심 종목 상세 확인
5. **Portfolio**
   - 실제 담을 종목 수와 비중 확인
6. **Evaluation / Postmortem**
   - 전일 판단이 맞았는지 확인
7. **Research Lab / Backtest**
   - 전략 비교나 검증 실험 수행

---

## 6. 주요 화면 설명

## 6.1 Market Pulse
이 화면은 시장 전체 분위기를 보는 곳입니다.

주로 보게 되는 항목:
- 시장 등락과 breadth
- 거래대금 변화
- 외국인/기관 수급
- 장세 레짐(regime)
- 공포/낙관 계열 요약
- 주요 뉴스 클러스터

### 어떻게 해석하나요?
- 장이 강한데 breadth 가 약하면 소수 대형주 장일 수 있습니다.
- 장이 약한데 후보군 일부만 강하면 테마성/수급성 장일 수 있습니다.
- 외국인 수급이 급격히 악화되면 상위 랭킹 종목도 보수적으로 봐야 합니다.

---

## 6.2 Leaderboard
이 화면은 오늘의 상위 후보군을 보는 핵심 화면입니다.

주요 컬럼 예시:
- symbol / name
- explanatory grade
- selection score
- alpha expectation
- uncertainty
- disagreement
- implementation penalty
- regime fit
- expected band (D+1 / D+5)
- current action suggestion

### 등급 읽는 법
- **A / A-**: 상대적으로 우선 검토 가치가 높음
- **B**: 관찰 우선
- **C**: 기대값보다 불확실성/비용/리스크가 큼

### 이 화면에서 바로 판단하지 말아야 하는 이유
Leaderboard는 1차 필터입니다.
실제 판단은 아래 화면과 함께 봐야 합니다.

- Stock Workbench
- Intraday Console
- Portfolio

---

## 6.3 Stock Workbench
개별 종목을 깊게 보는 화면입니다.

주요 항목:
- 최근 차트
- 기술 지표
- 재무 요약
- 최근 뉴스
- selection 근거
- uncertainty / disagreement
- 과거 유사 상황 성과
- postmortem 기록

### 언제 이 화면을 쓰나요?
- 랭킹 상위 종목의 근거를 보고 싶을 때
- 특정 종목이 왜 제외되었는지 보고 싶을 때
- 모델이 과거에 이 종목을 어떻게 평가했는지 보고 싶을 때

---

## 6.4 Intraday Console
전일 선정된 후보군의 **장중 진입 타이밍 보조** 화면입니다.

가능한 액션 예시:
- `ENTER_NOW`
- `WAIT_RECHECK`
- `AVOID_TODAY`
- `DATA_INSUFFICIENT`

### 중요한 점
이 화면은 **장중 타이밍 보조** 이지, 독립적인 매수 추천 화면이 아닙니다.
즉, 전일 selection 또는 portfolio 후보군을 보조하는 역할입니다.

---

## 6.5 Portfolio
실제 담을 종목 수와 비중을 보는 화면입니다.

주요 항목:
- target book
- target weights
- cash buffer
- sector cap
- KOSDAQ cap
- turnover cap
- execution mode

### 어떤 순서로 보나요?
1. 오늘 신규진입/유지 후보 확인
2. 한 종목 비중이 너무 큰지 확인
3. 특정 섹터 과집중인지 확인
4. 현금 비중이 이상한지 확인
5. turnover 과도 여부 확인

---

## 6.6 Evaluation / Postmortem
이 화면은 전일/과거 판단이 맞았는지 확인하는 곳입니다.

주요 항목:
- selection 성과
- timing 성과
- portfolio 성과
- calibration 상태
- missed winner / avoided loser
- bias / coverage / monotonicity

### 왜 중요한가요?
이 화면을 꾸준히 봐야 시스템이 실제로 좋아지는지 알 수 있습니다.
정답률만 보지 말고 다음을 같이 보세요.

- 비용 반영 후에도 이겼는가
- 특정 장세에서만 잘 맞는가
- 과도하게 보수적/공격적인가
- 예측밴드가 실제로 커버되는가

---

## 6.7 Research Lab / Backtest
전략을 검증하고 비교 실험하는 화면입니다.

대표 사용 예:
- selection v2 vs portfolio policy 비교
- timing raw vs timing adjusted 비교
- D+1 vs D+5 비교
- 비용 반영 전/후 비교
- 특정 기간/레짐만 잘라서 비교

---

## 6.8 Ops / Health Dashboard
운영 상태를 확인하는 화면입니다.

확인해야 할 항목:
- 최근 배치 성공/실패
- provider 준비 상태
- 디스크 사용량
- retention/cleanup 상태
- recovery queue
- stale lock 여부

### 이 화면을 먼저 보는 이유
데이터가 어제 실패했으면 오늘 랭킹도 신뢰하면 안 됩니다.
항상 먼저 배치 상태를 보세요.

---

## 7. 하루 운영 루틴 예시

### 장 전
- 전일 postmortem 확인
- 오늘 관심 후보군 확인
- intraday console 준비 상태 확인

### 장중
- 후보군 장중 액션 확인
- WAIT/AVOID가 과도한지 확인
- 장세 급변 시 market pulse 변화 확인

### 장후
- 새 리포트 생성 여부 확인
- leaderboard/portfolio 확인
- discord 보고서 확인
- evaluation 결과 확인

### 주말
- backtest / research lab 에서 최근 정책 성과 검토
- 과거 한 달 regime별 성과 검토
- 필요 시 학습 또는 파라미터 freeze/rollback 검토

---

## 8. Discord 리포트 읽는 법

StockMaster는 장후 리포트를 Discord로 보낼 수 있습니다.

보통 들어가는 항목:
- 시장 요약
- 상위 후보군
- 주요 섹터/뉴스 촉매
- 주의 종목
- D+1 / D+5 기대 밴드
- 전일 성과 요약

### 주의
Discord 메시지는 **요약본** 입니다.
세부 근거는 웹 UI에서 다시 확인하세요.

---

## 9. 백테스트는 언제 쓰나요?

백테스트는 다음 상황에서 사용합니다.

- 새 점수 구조를 넣기 전에
- selection policy 를 바꾸기 전에
- intraday timing 정책을 바꾸기 전에
- portfolio cap 규칙을 바꾸기 전에
- 최근 장세에서 전략이 깨졌는지 확인하고 싶을 때

### 백테스트에서 꼭 볼 것
- net-of-cost 성과
- drawdown
- turnover
- regime별 성과
- open vs timing 비교
- calibration 상태

---

## 10. 자주 발생하는 오해

### 오해 1. A등급이면 무조건 사야 한다
아닙니다. uncertainty, disagreement, liquidity cap, intraday action 을 같이 봐야 합니다.

### 오해 2. 장중 ENTER_NOW 가 나오면 무조건 강하다
아닙니다. 이는 후보군 내 진입 타이밍 제안일 뿐입니다.

### 오해 3. 백테스트가 좋으면 앞으로도 그대로 된다
아닙니다. 장세 변화와 구조 변화 때문에 주기적 검증이 필요합니다.

### 오해 4. accuracy 가 높으면 좋은 전략이다
아닙니다. 실제로는 비용 반영 후 수익, drawdown, turnover, calibration 이 더 중요합니다.

---

## 11. 문제 해결

### 앱이 안 열릴 때
- Docker Desktop 이 켜져 있는지 확인
- `docker compose ps` 로 컨테이너 상태 확인
- `docker compose logs -f` 로 에러 확인

### 데이터가 비어 있을 때
- Ops/Health 에서 최근 job 실패 여부 확인
- provider API key 확인
- 디스크 full 여부 확인

### 랭킹이 이상할 때
- 오늘 배치가 partial/degraded 인지 확인
- universe coverage 확인
- feature snapshot 최신 여부 확인

### 장중 콘솔이 비어 있을 때
- 전일 selection 결과가 있는지 확인
- candidate-only 추적이 실행되었는지 확인
- market open 상태/시간 확인

---

## 12. 안전한 사용 습관

- 하루에 한 번은 반드시 Evaluation 을 볼 것
- 이상 수익률보다 비용 반영 후 성과를 볼 것
- 장세가 급변하면 최근 1~3개월 백테스트를 다시 돌려볼 것
- 시스템이 추천한 이유를 이해하지 못하면 비중을 줄일 것
- 한 종목/한 섹터 집중을 항상 경계할 것

---

## 13. 용어 간단 설명

- **as_of_date**: 그 판단이 만들어진 기준 날짜
- **matured outcome**: 실제로 결과가 확정된 후 평가 가능한 상태
- **excess return**: 시장 대비 초과 수익률
- **uncertainty**: 예측 불확실성
- **disagreement**: 모델들 간 견해 차이
- **implementation penalty**: 실제 집행 시 불리할 수 있는 요소에 대한 패널티
- **regime**: 시장 상태 구분(강세/약세/충격/회복 등)
- **turnover**: 매매 회전율
- **drawdown**: 고점 대비 하락폭

---

## 14. 마지막 조언

StockMaster는 “정답을 알려주는 기계”가 아니라, **실수 가능성을 줄이고 학습 속도를 높여주는 시스템**으로 쓰는 것이 가장 좋습니다.

항상 아래 순서를 지키세요.

1. 배치 상태 확인
2. 시장 상태 확인
3. 랭킹 확인
4. 종목 근거 확인
5. 포트폴리오 제약 확인
6. 다음날 결과로 다시 검증

그렇게 써야 시스템이 시간이 갈수록 더 좋아집니다.

