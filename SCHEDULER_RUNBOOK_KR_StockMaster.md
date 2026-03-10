# StockMaster 서버 자동 실행 가이드

이 문서는 StockMaster를 서버에 올린 뒤, 사람이 매일 직접 명령을 치지 않아도 자동으로 추천/평가/학습 후보 생성이 돌아가게 만드는 운영 가이드다.

## 1. 기본 운영 원칙

- 장후 추천은 자동 생성한다.
- 장후 평가는 자동 생성한다.
- 주간 재학습 후보는 자동 생성한다.
- 하지만 active model/policy는 자동으로 바꾸지 않는다.
- 모든 write job은 동시에 돌지 않게 한다.

즉, 운영 철학은 아래와 같다.

- **자동 실행**: daily recommendation, evaluation, maintenance
- **자동 학습 후보 생성**: weekly retrain, calibration
- **수동 승인 필요**: active model/policy promotion

## 2. 추천 스케줄

- 02:30 매일: maintenance / cleanup / disk check
- 16:20 평일: evaluation
- 18:40 평일: next-day recommendation daily close bundle
- 19:05 평일: audit-lite
- 08:55~15:15 평일 5분 주기: intraday candidate assist
- 토 03:30: weekly training candidate
- 토 06:30: weekly calibration

## 3. 왜 이렇게 나누는가

### daily close
장 마감 후 데이터를 모으고 다음날 추천 종목을 만든다.

### evaluation
당일 장 마감 후 과거 추천의 실제 결과를 계산한다.

### intraday assist
전일 뽑힌 후보군을 장중에 ENTER / WAIT / AVOID 형태로 재보조한다.

### weekly training
모델을 즉시 바꾸는 것이 아니라, 새 후보 모델을 만들고 비교 보고서를 만든다.

## 4. 실제 자동 실행 방식

서버에서는 보통 아래 형태로 실행한다.

- systemd timer가 시간에 맞춰 동작
- timer가 service를 깨움
- service가 `docker compose exec -T app python ...` 실행

즉, 앱이 스스로 백그라운드에서 시계를 보며 도는 방식보다, 서버 운영체제가 바깥에서 정해진 시각에 명령을 쏘는 방식이다.

## 5. 주의할 점

### 1) 평일이라고 무조건 거래일은 아니다
한국 휴장일이 있으므로 각 bundle은 내부에서 거래일 여부를 다시 확인해야 한다.

### 2) 동시에 돌리면 안 된다
DuckDB는 single-writer 전제이므로 동시에 여러 write job이 들어가면 안 된다.

### 3) active model 자동 반영 금지
주간 학습이 돌더라도 production model이 자동으로 바뀌면 안 된다.

## 6. 설치 후 가장 먼저 할 것

1. app 컨테이너가 정상 기동 중인지 확인
2. 수동으로 각 bundle을 한 번씩 dry-run 해보기
3. systemd service/timer 설치
4. timer enable
5. status 확인
6. Journald 로그 확인
7. UI Ops/Health 에 scheduler 정보가 뜨는지 확인

## 7. 수동 점검 기본 명령 예시

```bash
cd /opt/stockmaster

docker compose ps
docker compose exec -T app python scripts/run_daily_close_bundle.py --dry-run
docker compose exec -T app python scripts/run_evaluation_bundle.py --dry-run
docker compose exec -T app python scripts/run_ops_maintenance_bundle.py --dry-run
```

## 8. systemd 설치 후 자주 쓰는 명령

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now stockmaster-daily-close.timer
sudo systemctl enable --now stockmaster-evaluation.timer
sudo systemctl enable --now stockmaster-ops-maintenance.timer
sudo systemctl list-timers | grep stockmaster
sudo systemctl status stockmaster-daily-close.timer
sudo systemctl status stockmaster-daily-close.service
journalctl -u stockmaster-daily-close.service -n 200 --no-pager
```

## 9. 장애가 났을 때 먼저 볼 것

### daily recommendation이 안 나왔다면
- app 컨테이너가 떠 있는지
- daily close timer/service가 enabled 인지
- 최근 journald 로그에 에러가 있는지
- lock이 stale 상태인지
- trading calendar가 비거래일로 판단한 것은 아닌지

### evaluation이 비어 있다면
- matured label/outcome 데이터가 만들어졌는지
- evaluation bundle이 blocked/degraded 상태인지
- as_of_date 계산이 꼬이지 않았는지

### intraday가 멈췄다면
- timer 주기가 너무 짧아 backlog가 쌓였는지
- global write lock에 계속 막히는지
- candidate universe가 비어 있는지

## 10. 추천 운영 규칙

- 먼저 **daily close 자동화**를 확실히 붙인다.
- 그 다음 evaluation 자동화 붙인다.
- 그 다음 intraday assist를 붙인다.
- 마지막으로 weekly training/calibration을 붙인다.

즉, 중요도는 다음 순서다.

1. daily close
2. evaluation
3. maintenance
4. intraday assist
5. weekly train/calibration

## 11. 초반 안전 모드

처음 1~2주는 아래처럼 돌리는 걸 권장한다.

- daily/evaluation/maintenance는 publish 모드
- intraday assist는 dry-run 또는 internal-only
- weekly training은 candidate-only
- Discord publish는 daily만 활성

## 12. 이 문서 기준 최종 목적

매일 장이 끝나면 자동으로:
- 다음날 추천 종목이 생성되고
- 리포트가 갱신되고
- UI latest snapshot이 바뀌고
- Discord로 요약이 발송되며
- 다음날 평가가 또 자동으로 쌓이는 상태

여기까지 되면 StockMaster는 사실상 "서버에서 혼자 도는 연구/리포트 플랫폼" 상태가 된다.
