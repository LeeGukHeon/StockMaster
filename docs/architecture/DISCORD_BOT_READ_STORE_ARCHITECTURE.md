# Discord Bot Read Store Architecture

## 목표

기존 Streamlit 대시보드와 `main.duckdb` 직접 조회를 사용자 조회 경로에서 제거하고,
Discord bot을 기본 사용자 인터페이스로 전환한다.

핵심 요구사항:

- 학습/배치 중에도 사용자 조회가 가능해야 한다.
- `main.duckdb` write lock과 사용자 조회가 충돌하면 안 된다.
- `read-only DuckDB snapshot`이나 `/tmp` 대용량 임시 복사에 의존하지 않는다.
- 용량은 `latest-only` 기준으로 작게 유지한다.
- Discord webhook 알림과 slash command 응답을 분리한다.

## 현재 문제

현재 구조는 배치와 사용자 조회가 결국 같은 DuckDB 파일을 공유한다.

- Streamlit/UI는 `main.duckdb` 직접 조회 또는 snapshot fallback에 의존한다.
- 배치가 writer lock을 오래 잡으면 UI/알림 경로가 막힌다.
- snapshot fallback은 `/tmp/stockmaster-duckdb-*.duckdb` 같은 대용량 임시 파일을 만들 수 있다.
- read-model parquet 물질화도 배치 후처리를 무겁게 만든다.

## 새 구조

세 층으로 분리한다.

1. `Writer Store`
   - 주체: `main.duckdb`
   - 역할: 수집, 학습, 평가, 리서치, 포트폴리오 산출
   - 사용자 직접 조회 금지

2. `Bot Read Store`
   - 주체: metadata Postgres
   - 역할: Discord bot 응답용 latest snapshot 저장
   - 특징: latest-only overwrite, 작고 빠른 조회, 락 충돌 없음

3. `Discord Interfaces`
   - Webhook: 자동 알림 발송
   - Bot: slash command 응답
   - 둘 다 `Bot Read Store`만 읽는다

## 데이터 흐름

### 1. 정기 배치

- `daily-close`
  - D1/D5 추천 후보, 종목 요약, 상태 snapshot 생성
- `evaluation`
  - 평가 요약, weekly report용 요약 갱신
- `weekly-calibration`
  - 주간 정책/캘리브 관련 요약 갱신

### 2. Bot Read Store 물질화

배치 종료 후 필요한 latest snapshot만 Postgres에 overwrite 한다.

- `next_picks`
  - `horizon=1`, `horizon=5`
- `weekly_report`
  - alpha promotion, evaluation summary, policy evaluation 핵심 줄
- `stock_summary`
  - 종목별 최신 요약 1행
- `status`
  - 마지막 반영 시각, 기준일, 최근 생성 일자

### 3. Discord bot 응답

bot은 Postgres에서 latest snapshot을 읽어 응답한다.

- `/내일종목추천`
- `/주간보고`
- `/종목분석`
- `/상태`

## 저장 전략

`fact_discord_bot_snapshot`

- `snapshot_type`
  - `status`
  - `next_picks`
  - `weekly_report`
  - `stock_summary`
- `snapshot_key`
  - `latest`
  - `h1:005930`
  - `alpha_h1`
  - `005930`
- latest-only overwrite
- 용량 증가 방지

저장 원칙:

- 원본 대형 DataFrame 저장 금지
- 최신 응답에 필요한 작은 요약만 저장
- 과거 히스토리는 1차 구현 범위에서 저장하지 않음
- 실시간 종목분석 결과는 저장하지 않거나 짧은 TTL 캐시만 허용

## 명령어 설계

### `/내일종목추천`

입력:

- `보유기준`: `1`, `5`
- `개수`: 기본 `5`

출력:

- 종목코드/종목명
- 보유 기준
- 등급
- 예상 초과수익률
- 진입 예정일
- 핵심 근거
- 유의할 리스크

### `/주간보고`

출력:

- 하루 보유 기준 모델 점검
- 5거래일 보유 기준 모델 점검
- 주간 평가 요약
- 정책/캘리브레이션 핵심 줄

### `/종목분석`

입력:

- 종목명 또는 6자리 종목코드

1차 출력:

- D1/D5 최신 등급
- D5 예상 초과수익률
- 최근 5일/20일 수익률
- 최근 3일 뉴스 개수
- 요약 코멘트

주의:

- 1차는 latest snapshot 기반
- 실시간 재계산은 2차 범위

### `/상태`

출력:

- 기준일
- 마지막 반영 시각
- 추천 snapshot 기준일
- 주간 보고 snapshot 기준일
- bot read store 생성 시각

## 실시간 종목분석 2차 방향

실시간 분석은 bot read store와 분리한다.

- bot 명령 수신
- 별도 분석 워커 실행
- 최신 시세/뉴스/API 조회
- 메모리 또는 모델 artifact 기반 계산
- Discord 후속 응답

원칙:

- 실시간 분석 경로도 `main.duckdb` 직접 write 금지
- 결과는 기본 저장하지 않음

## 배치 연동 원칙

`bot read store`는 모든 작업 후에 돌리지 않는다.

1차 연동 대상:

- `daily-close`
- `evaluation`
- `weekly-calibration`

제외:

- `news-after-close`
- `intraday-assist`
- `ops-maintenance`

이유:

- 봇용 snapshot은 사용자 응답에 필요한 최소 최신 데이터만 반영하면 된다.
- 배치 후처리 시간을 불필요하게 늘리지 않기 위해서다.

## 운영 원칙

- Discord webhook 알림은 유지
- Discord bot은 별도 서비스로 띄움
- 봇 장애가 배치 장애로 번지면 안 됨
- metadata Postgres가 비활성화된 환경에선 bot read store materialization을 skip 한다

## 1차 구현 범위

- `fact_discord_bot_snapshot` 테이블 추가
- latest snapshot materializer 추가
- `/내일종목추천`, `/주간보고`, `/종목분석`, `/상태` slash command 골격 추가
- 수동 실행 스크립트 추가

## 추후 작업

- Discord bot systemd service / deployment
- 실시간 종목분석 워커
- guild scoped sync / 운영 권한 관리
- 답변 길이 자동 분할
- 사용자별 관심종목/알림 설정
