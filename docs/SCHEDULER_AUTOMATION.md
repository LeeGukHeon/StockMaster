# 자동 스케줄러 개요

## 목적

TICKET-017의 목적은 서버 운영 기준으로 StockMaster의 핵심 번들을 자동 실행하되,
`main.duckdb` single-writer 규율을 깨지 않는 보수적 자동화를 만드는 것입니다.

핵심 원칙:

- host `systemd timer`가 스케줄의 기준입니다.
- 실제 실행은 `docker compose exec -T app python ...` 형태로 호출합니다.
- Streamlit UI 안에는 background scheduler를 넣지 않습니다.
- 모든 write job은 serial lock 규율을 거칩니다.
- 비거래일은 번들 내부에서 self-skip 합니다.
- 자동 학습/보정은 후보 생성까지만 수행하고, active model/policy 자동 반영은 하지 않습니다.

## 자동 실행 번들

### 1. 아침 뉴스 수집

- 시간: 평일 `08:30`
- 진입점: `python scripts/run_news_sync_bundle.py --profile morning`
- 목적:
  - 야간 / 해외발 뉴스 메타데이터 수집
  - 주말/휴일을 끼고 월요일 아침이 되면 금요일 다음 날부터 월요일까지의 뉴스 날짜를 순서대로 수집

### 2. 장중 후보군 보조

- 시간: 평일 `08:55` 시작, `15:15`까지 5분 간격
- 진입점: `python scripts/run_intraday_assist_bundle.py`
- 목적:
  - 전일 selection v2 기반 후보군 세션 생성
  - 1분봉 / 체결 / 호가 요약 갱신
  - timing signal, adjusted action, meta overlay, final action 갱신

### 3. 장 마감 직후 뉴스 수집

- 시간: 평일 `16:10`
- 진입점: `python scripts/run_news_sync_bundle.py --profile after_close`
- 목적:
  - 당일 장중/장후 뉴스 메타데이터를 한 번 더 반영

### 4. 장후 평가

- 시간: 평일 `16:20`
- 진입점: `python scripts/run_evaluation_bundle.py`
- 목적:
  - matured outcome 평가
  - portfolio / postmortem 관련 집계 갱신

### 5. 장후 추천 생성

- 시간: 평일 `18:40`
- 진입점: `python scripts/run_daily_close_bundle.py`
- 목적:
  - 최종 뉴스 재수집이 포함된 일일 파이프라인 실행
  - selection / portfolio / report / latest snapshot 갱신

### 6. 일일 경량 감사

- 시간: 평일 `19:05`
- 진입점: `python scripts/run_daily_audit_lite_bundle.py`
- 목적:
  - latest layer consistency
  - artifact reference integrity
  - release sanity 재점검

### 7. 주간 학습 후보 생성

- 시간: 토요일 `03:30`
- 진입점: `python scripts/run_weekly_training_bundle.py`
- 목적:
  - retrain candidate 생성
  - meta walk-forward / 평가 결과 갱신
- 중요:
  - active meta-model 자동 반영 금지

### 8. 주간 보정 / 정책 후보 생성

- 시간: 토요일 `06:30`
- 진입점: `python scripts/run_weekly_calibration_bundle.py`
- 목적:
  - policy calibration
  - meta threshold calibration
  - recommendation 후보 생성
- 중요:
  - active policy / active meta-model 자동 반영 금지

### 9. 운영 maintenance

- 시간: 매일 `02:30`
- 진입점: `python scripts/run_ops_maintenance_bundle.py`
- 목적:
  - stale lock 정리
  - 로그 회전 / 디스크 usage 요약 / retention dry-run 또는 실행
  - recovery queue / health snapshot 갱신

## 예외 처리 규칙

- non-trading day: `SKIPPED_NON_TRADING_DAY`
- same identity already completed: `SKIPPED_ALREADY_DONE`
- serial lock occupied: `SKIPPED_LOCKED`
- upstream readiness 부족: `BLOCKED` 또는 `DEGRADED_SUCCESS`
- dry-run: 실제 write 없이 상태와 경로만 검증

## 날짜 기준 분리

T017부터 스케줄러 번들은 `달력일 기준`, `거래일 기준`, `혼합 기준`으로 나눠 처리합니다.

- `달력일 기준`
  - `news_morning`
  - `news_after_close`
  - `daily_audit_lite`
  - `ops_maintenance`
- `거래일 기준`
  - `intraday_assist`
  - `evaluation`
  - `daily_close`
- `혼합 기준`
  - `weekly_training_candidate`
  - `weekly_calibration`

의미:

- 달력일 기준 번들은 주말/휴장일에도 독립된 실행 identity를 가질 수 있습니다.
- 거래일 기준 번들은 비영업일에 self-skip 됩니다.
- 혼합 기준 번들은 달력 스케줄로 실행되지만, 내부 입력 데이터와 결과 기준일은 최근 거래일로 정렬합니다.

뉴스는 이 분리의 대표 사례입니다.

- `08:30` 아침 뉴스 수집: 달력일 기준 실행
- `16:10` 장 마감 직후 뉴스 수집: 달력일 기준 실행
- `18:40` 장후 추천/리포트: 거래일 기준 실행이지만 내부에 최종 뉴스 재수집이 포함

## 자동 실행되지만 자동 반영하지 않는 것

아래 두 영역은 **자동 계산만 수행**하고, 활성 교체는 사람이 최종 확인해야 합니다.

- retrain candidate 결과
- calibration / recommendation 결과

즉:

- 자동 실행됨: `run_weekly_training_bundle`, `run_weekly_calibration_bundle`
- 자동 반영 안 됨: active meta-model, active intraday policy

## 수동 반영 UI

리서치 랩 화면에서는 아래를 제공합니다.

- 현재 활성 정책 vs 새 추천 정책 비교
- 현재 활성 메타 모델 vs 새 학습 후보 비교
- before / after 수치 비교
- 확인 체크박스 + 반영 버튼

반영 버튼은 아래 CLI와 같은 역할을 합니다.

```powershell
python scripts/freeze_intraday_active_policy.py --as-of-date 2026-03-20 --promotion-type MANUAL_FREEZE --source scheduler_latest_recommendation --note "Manual review approved"
python scripts/freeze_intraday_active_meta_model.py --as-of-date 2026-03-20 --source scheduler_latest_training_candidate --note "Manual review approved" --horizons 1 5
```

## 절대 자동 반영하지 않는 것

- active model 자동 교체
- active policy 자동 교체
- auto-order / auto-trade

## 수동 smoke 명령

```powershell
python scripts/validate_scheduler_framework.py
python scripts/smoke_scheduler_bundles.py
python scripts/run_scheduled_bundle.py --service-slug news-morning --dry-run
python scripts/run_scheduled_bundle.py --service-slug daily-close --dry-run --skip-discord
```
