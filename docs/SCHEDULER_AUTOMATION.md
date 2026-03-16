# 자동 스케줄러 개요

이 문서는 scheduler 구조만 빠르게 보는 요약본입니다.
운영 절차 전체는 [RUNBOOK_SERVER_OPERATIONS.md](d:/MyApps/StockMaster/docs/RUNBOOK_SERVER_OPERATIONS.md)를 기준으로 합니다.

## 현재 실행 경로

현재 서버 기준 스케줄러 경로:

1. host `systemd timer`
2. `stockmaster-scheduler@.service`
3. `scripts/server/run_scheduler_job_host.sh`
4. `/opt/stockmaster/worker-venv/bin/python`
5. `scripts/run_scheduled_bundle.py`

즉 더 이상 `docker compose exec -T app python ...` 경로를 scheduler의 기준 경로로 사용하지 않습니다.

## 핵심 원칙

- host `systemd timer`가 스케줄 기준이다.
- app 컨테이너는 UI 중심이고, scheduler 실행 프로세스는 host worker다.
- 모든 write job은 serial lock + DB active lock 규율을 거친다.
- 비거래일은 번들 내부에서 self-skip 한다.
- 자동 학습/보정은 후보 생성까지만 수행하고 활성 반영은 수동 승인 기준을 유지한다.

## 자동 실행 번들

| job | service slug | schedule | 목적 |
|---|---|---|---|
| ops maintenance | `ops-maintenance` | daily 02:30 | stale lock 정리, 로그/보관/health 갱신 |
| morning news | `news-morning` | Mon-Fri 08:30 | 야간 및 아침 뉴스 메타데이터 수집 |
| intraday assist | `intraday-assist` | Mon-Fri 08:55-15:15 every 5 min | 장중 후보군/요약/보조 시그널 갱신 |
| after-close news | `news-after-close` | Mon-Fri 16:10 | 장중/장후 뉴스 메타데이터 재수집 + 장마감 직후 브리핑 |
| evaluation | `evaluation` | Mon-Fri 16:20 | matured outcome 평가, 사후 평가 집계 |
| daily close | `daily-close` | Mon-Fri 18:40 | 추천/리포트/latest snapshot 갱신 |
| daily audit lite | `daily-audit-lite` | Tue-Sat 05:30 | latest consistency / artifact integrity 점검 |
| daily overlay refresh | `daily-overlay-refresh` | Mon-Fri 21:30 | light overlay refresh + guarded auto-promotion |
| docker build cache cleanup | `docker-build-cache-cleanup` | daily 04:30 | Docker builder cache 정리 |
| weekly training candidate | `weekly-training` | Sat 03:30 | 학습 후보 생성 |
| weekly calibration | `weekly-calibration` | Sat 10:00 | light policy/threshold refresh |
| weekly policy research | `weekly-policy-research` | Sat 14:00 | heavy walk-forward / ablation research |

Follow-up chaining:

- `daily_close` succeeds => immediately chain `daily_overlay_refresh`
- `daily_overlay_refresh` succeeds => immediately chain `daily_audit_lite` using next-day scheduler identity
- `weekly_training_candidate` succeeds => immediately chain `weekly_calibration`
- `weekly_calibration` succeeds => immediately chain `weekly_policy_research`
- timers remain enabled as backstops if an upstream chain never starts

## 상태 해석

- `SUCCESS`, `PARTIAL_SUCCESS`, `DEGRADED_SUCCESS`: 정상 완료 범주
- `SKIPPED_NON_TRADING_DAY`: 비거래일 정상 스킵
- `SKIPPED_ALREADY_DONE`: 같은 identity 재실행 스킵
- `SKIPPED_LOCKED`: 다른 writer가 이미 진행 중인 정상 스킵
- `BLOCKED`: readiness, 디스크 watermark, upstream 조건 부족 등 구조적 차단
- `FAILED`: 예외 또는 회귀

현재 운영 기준에서는 active lock 충돌도 scheduler 레이어에서 `SKIPPED_LOCKED` 정상 스킵으로 취급합니다.

## 장마감 메시지 구분

- `16:10` 장마감 직후: 추천 없는 `장마감 직후 브리핑`
- `18:40` 일일 추천 배치 완료 후: 최종 추천이 포함된 `장마감 요약`

즉 장마감 직후 브리핑은 뉴스와 준비 상태를 알려주는 메시지이고,
최종 추천 종목은 `daily-close` 완료 이후 메시지에서만 다룹니다.

## 날짜 기준

- 달력일 기준:
  - `news_morning`
  - `news_after_close`
  - `daily_audit_lite`
  - `ops_maintenance`
- 거래일 기준:
  - `intraday_assist`
  - `evaluation`
  - `daily_close`
- 혼합 기준:
  - `weekly_training_candidate`
  - `weekly_calibration`

## 수동 검증 예시

```bash
cd /opt/stockmaster/app
python scripts/validate_scheduler_framework.py
python scripts/smoke_scheduler_bundles.py
sudo -E bash scripts/server/run_scheduler_job_host.sh news-morning --dry-run
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-close --dry-run --force --skip-discord
```
