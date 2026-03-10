# TICKET-014 — OCI 배포 / 외부접속 / Docker Compose 운영 / Reverse Proxy / Runbook

- 문서 목적: TICKET-013 이후, Codex가 바로 이어서 구현할 **Oracle Cloud Infrastructure(OCI) 기반 배포 준비, 외부접속 가능 구성, Docker Compose 운영 프로필, reverse proxy, 환경 분리, 배포/복구 runbook** 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
  - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
  - `TICKET_000_Foundation_and_First_Work_Package.md`
  - `TICKET_001_Universe_Calendar_Provider_Activation.md`
  - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
  - `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
  - `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
  - `TICKET_005_Postmortem_Evaluation_Calibration_Report.md`
  - `TICKET_006_ML_Alpha_Uncertainty_Disagreement_Selection_v2.md`
  - `TICKET_007_Intraday_Candidate_Assist_Engine.md`
  - `TICKET_008_Intraday_Postmortem_Regime_Aware_Strategy_Comparison.md`
  - `TICKET_009_Policy_Calibration_Regime_Tuning_Experiment_Ablation.md`
  - `TICKET_010_Policy_Meta_Model_ML_Timing_Classifier_v1.md`
  - `TICKET_011_Integrated_Portfolio_Capital_Allocation_Risk_Budget.md`
  - `TICKET_012_Operational_Stability_Batch_Recovery_Disk_Guard_Monitoring_Health_Dashboard.md`
  - `TICKET_013_Final_User_Workflow_Dashboard_Report_Polish.md`
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
  - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
  - `CODEX_EIGHTH_INSTRUCTION_StockMaster.md`
  - `CODEX_NINTH_INSTRUCTION_StockMaster.md`
  - `CODEX_TENTH_INSTRUCTION_StockMaster.md`
  - `CODEX_ELEVENTH_INSTRUCTION_StockMaster.md`
  - `CODEX_TWELFTH_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRTEENTH_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTEENTH_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 ~ TICKET-013 완료
- 우선순위: 최상
- 기대 결과: **로컬에서 완성된 StockMaster 앱을 OCI에서 Docker Compose로 안정적으로 띄우고, 외부에서 브라우저로 접속 가능하며, 서버 재부팅/로그/볼륨/복구/보안 규칙까지 포함한 운영형 배포 패키지가 준비된 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **“로컬 개발 완료된 StockMaster를 Oracle Cloud 서버에서 실제로 돌릴 수 있게 만드는 운영 배포 티켓”** 이다.

핵심은 아래 다섯 가지다.

1. 배포용 환경 분리(`local` vs `server` vs `prod-like`)를 명확히 한다.
2. Docker Compose 기반의 서버 실행 프로필을 만든다.
3. Streamlit 앱을 외부접속 가능하게 하되, reverse proxy / health / restart / volume / backup 구조를 갖춘다.
4. OCI 네트워크/보안 규칙/포트/공인 IP 기준의 runbook 을 문서화한다.
5. 사용자가 실제로 서버를 만들고 올릴 때 따라 할 수 있는 **복붙형 설치 절차**를 남긴다.

---

## 2. 이번 티켓의 핵심 원칙

### 2.1 이번 티켓은 “코드 기능 추가”보다 “배포 가능 상태 만들기”가 핵심이다
이번 티켓은 selection / model / portfolio 기능을 추가하는 티켓이 아니다.

해야 하는 것:
- production-like compose 구성
- runtime env 분리
- reverse proxy 준비
- bind address / port 설정 정리
- server restart / container restart 대응
- health endpoint / smoke test
- backup / restore / migration runbook
- OCI 네트워크 오픈 절차 문서화

하지 말아야 할 것:
- 이 티켓에서 모델 구조 갈아엎기
- 이 티켓에서 자동매매 기능 넣기
- 이 티켓에서 멀티유저 auth 를 무겁게 추가하기
- cloud resource를 코드가 강제로 직접 생성하도록 전제하기

### 2.2 v1 은 단일 서버 / 단일 사용자 / long-running service 를 전제로 한다
현재 프로젝트 범위상 가장 현실적인 운영 형태는 아래다.

- 단일 OCI compute instance
- Docker Compose 기반
- app 컨테이너 1개 + reverse proxy 1개(권장)
- 데이터/아티팩트/로그는 서버 로컬 볼륨
- 스케줄링은 app 내부 또는 host cron/systemd 조합
- 장기적으로 domain/TLS 는 선택적

즉 Kubernetes, 다중 노드, 오토스케일링은 이번 티켓 범위가 아니다.

### 2.3 external access 는 “공인 IP + 보안 규칙 + 앱 bind + reverse proxy” 전체가 맞아야 한다
외부에서 접속되려면 단순히 앱을 띄우는 것으로 끝나지 않는다.

아래 모두가 맞아야 한다.
- 인스턴스에 public IP가 있음
- OCI 네트워크 보안 규칙이 열려 있음
- OS 방화벽이 맞음
- 컨테이너 포트가 열려 있음
- reverse proxy 또는 app bind address 가 외부 접속을 허용함
- 접속 URL / 포트가 사용자에게 문서화되어 있음

### 2.4 server 환경은 local 환경과 설정을 분리해야 한다
반드시 환경을 나눈다.

예시:
- `APP_ENV=local`
- `APP_ENV=server`
- `APP_ENV=prod_like`

분리 대상:
- host / port
- log level
- disk paths
- retention settings
- Discord publish default
- scheduler enable flag
- debug flag
- file watcher / autoreload 여부

### 2.5 운영형 배포는 “컨테이너만 띄움”이 아니라 “재시작/복구/백업/런북”까지 포함해야 한다
최소한 아래가 있어야 한다.

- compose profile 또는 server compose file
- `.env.server.example`
- persistent volumes
- restart policy
- healthcheck
- startup order
- smoke test script
- backup script
- restore notes
- upgrade/deploy runbook
- rollback runbook

### 2.6 v1 의 보안 원칙은 “최소 노출 / 명시적 공개 / 비밀값 분리”이다
필수 원칙:
- `.env` 는 Git 제외
- API key / webhook / secret 는 환경변수 또는 별도 secret file
- 디버그 모드 비활성
- 필요 포트만 오픈
- 내부용 포트는 외부에 직접 노출 최소화
- reverse proxy 뒤로 app 직접 포트를 숨기는 구성이 권장

### 2.7 Codex는 “클라우드 콘솔 클릭”을 대신할 수 없으므로, 실행 가능한 코드 + runbook + 템플릿을 만드는 데 집중해야 한다
이번 티켓에서 Codex가 할 수 있는 일:
- 배포용 compose 파일
- reverse proxy config
- environment example files
- health / smoke scripts
- backup / restore scripts
- README / runbook / checklist
- optional systemd unit / host scripts

이번 티켓에서 Codex가 직접 못 하는 일:
- OCI 콘솔에서 실제 인스턴스 생성
- 실제 보안 규칙 클릭 적용
- 실제 DNS 구매/연결
- 실제 인증서 발급 완료

그러므로 **“사람이 따라 하면 되게 만드는 수준”** 으로 문서와 배포 자산을 완성해야 한다.

---

## 3. 이번 티켓에서 반드시 끝내야 하는 것

### 3.1 deployment profile 분리
최소 아래 파일 또는 동등 구조가 있어야 한다.

- `docker-compose.yml` (local 기본)
- `docker-compose.server.yml` 또는 `compose.server.yml`
- `Dockerfile`
- `.env.example`
- `.env.server.example`
- `config/app/environment.server.yaml`
- `config/app/environment.local.yaml`

### 3.2 reverse proxy 구성
권장: `nginx` 또는 `caddy`

최소 요구:
- 외부 포트 80 또는 사용자 지정 포트 수신
- app upstream 전달
- health path pass-through
- websocket/streaming 호환에 문제 없는 기본 설정
- request size / timeout 을 과도하지 않게 조정
- 향후 TLS/domain 추가가 쉬운 구조

이번 티켓에서는:
- HTTP 우선도 허용
- domain/TLS는 선택적이지만, 붙이기 쉬운 구조여야 함

### 3.3 app bind / server startup 정리
반드시 서버용 실행에서는:
- `0.0.0.0` bind
- 고정 포트
- environment badge 표시
- debug off
- file watcher 최소화 또는 off

### 3.4 health / smoke / readiness
최소 아래가 있어야 한다.
- app health check
- compose healthcheck
- reverse proxy 연결 확인
- smoke test script
- “외부 브라우저에서 접속 가능한지”를 확인하는 절차

### 3.5 persistent data layout
서버 볼륨 구조를 명확히 한다.

권장:
- `/opt/stockmaster/app`
- `/opt/stockmaster/data`
- `/opt/stockmaster/artifacts`
- `/opt/stockmaster/logs`
- `/opt/stockmaster/backups`

또는 동등 구조.

반드시 문서화:
- 무엇이 영구 데이터인지
- 무엇이 캐시인지
- 무엇을 백업해야 하는지
- 무엇은 지워도 되는지

### 3.6 scheduler / service persistence
서버 재부팅 시 다시 살아나야 한다.

최소 요구:
- `docker compose up -d` 기준의 운영 흐름
- restart policy
- optional host-level systemd unit 또는 runbook
- 장후/평가/ops maintenance 스케줄을 서버에서 지속 가능하게 실행하는 구조

### 3.7 backup / restore
최소 요구:
- data/artifacts/config backup script
- restore 개요 문서
- 백업 제외 대상 명시
- 백업 파일 위치 규칙
- restore 후 health check 절차

### 3.8 deployment runbook
사람이 그대로 따라 할 수 있어야 한다.

필수 문서:
- `docs/DEPLOY_OCI.md`
- `docs/RUNBOOK_SERVER_OPERATIONS.md`
- `docs/BACKUP_AND_RESTORE.md`
- `docs/EXTERNAL_ACCESS_CHECKLIST.md`

### 3.9 OCI 네트워크 / 접근 절차 문서화
runbook에 최소 아래 절차를 설명해야 한다.

- 인스턴스 생성 시 public IP 필요 여부
- VCN / subnet / 인터넷 연결 전제
- ingress rule 열기
- SSH 접속
- 서버 디렉터리 준비
- Docker / Compose 준비
- repo 또는 배포 패키지 업로드
- `.env.server` 작성
- `docker compose -f docker-compose.server.yml up -d --build`
- 브라우저 접속 확인
- 실패 시 로그 확인

### 3.10 external URL / base config
향후 도메인을 붙이지 않더라도 아래를 준비한다.
- `APP_BASE_URL`
- `STREAMLIT_SERVER_ADDRESS`
- `STREAMLIT_SERVER_PORT`
- `PUBLIC_PORT`
- `REVERSE_PROXY_ENABLED`
- optional `BASE_URL_PATH`

---

## 4. 구현 상세 요구

### 4.1 권장 디렉터리/파일
예시:
- `deploy/docker-compose.server.yml`
- `deploy/nginx/default.conf`
- `deploy/env/.env.server.example`
- `deploy/systemd/stockmaster-compose.service`
- `scripts/server/start_server.sh`
- `scripts/server/stop_server.sh`
- `scripts/server/restart_server.sh`
- `scripts/server/tail_server_logs.sh`
- `scripts/server/smoke_test_server.sh`
- `scripts/server/backup_server_data.sh`
- `scripts/server/check_public_access.sh`
- `docs/DEPLOY_OCI.md`
- `docs/RUNBOOK_SERVER_OPERATIONS.md`
- `docs/BACKUP_AND_RESTORE.md`
- `docs/EXTERNAL_ACCESS_CHECKLIST.md`

### 4.2 compose server profile
최소 요구:
- app service
- reverse proxy service (권장)
- 볼륨 마운트
- env file
- restart unless-stopped 또는 equivalent
- healthcheck
- log size 관리 옵션(가능하면)
- production-like command

### 4.3 app command
예시 방향:
- Streamlit app
- bind address `0.0.0.0`
- 고정 포트
- server mode
- dev watcher 비활성 또는 제한

### 4.4 reverse proxy
기본 요구:
- `/` → app
- `/health` → app health
- 향후 `/ops` 등 path 분리가 가능하도록 단순 구조
- timeout / buffering 최소 설정
- 원격 접속 시 필요한 header 전달

### 4.5 server scripts
최소 아래 script 또는 동등 기능:
- `scripts/server/install_server_prereqs.md` 또는 shell script
- `scripts/server/start_server.sh`
- `scripts/server/stop_server.sh`
- `scripts/server/restart_server.sh`
- `scripts/server/smoke_test_server.sh`
- `scripts/server/check_public_access.sh`
- `scripts/server/backup_server_data.sh`
- `scripts/server/restore_server_data.md`
- `scripts/server/print_runtime_info.sh`

### 4.6 environment / secrets
필수:
- `.env.server.example`
- 명확한 변수 설명
- 민감값 Git 제외
- server용 default 보수적
- local/server 값 차이 문서화

### 4.7 release / upgrade / rollback
최소 runbook 필요:
- 새 코드 pull 또는 배포 패키지 교체
- image rebuild
- container restart
- smoke test
- 실패 시 rollback
- artifacts/data 보호 원칙

---

## 5. 테스트 요구

최소 테스트 범위:
- compose config validation
- environment loading test
- server mode config resolver test
- health endpoint / smoke test
- artifact path permissions/basic IO test
- backup script dry-run test
- deployment docs consistency check
- public access checklist validator (문서/설정 기반)

---

## 6. 하지 말아야 할 것

- 이 티켓에서 cloud console 자동조작 전제 금지
- 주문 API / 자동매매 외부 공개 금지
- 모든 포트를 외부에 그냥 다 열지 말 것
- `.env.server` 실값을 저장소에 넣지 말 것
- reverse proxy 없이 app debug 포트를 무방비로 노출하는 구조를 기본값으로 삼지 말 것
- hardcoded 개인 경로 남발 금지
- backup 없이 destructive deploy 금지
- “작동만 하면 됨” 수준으로 문서 없이 끝내지 말 것

---

## 7. 완료 기준

아래를 만족하면 이번 티켓은 완료로 본다.

1. 서버용 compose/profile 이 존재한다.
2. server용 env example 과 config 가 존재한다.
3. reverse proxy 구성이 존재한다.
4. health / smoke / public access check 스크립트가 있다.
5. data/artifacts/logs/backups 구조가 문서화되어 있다.
6. backup / restore runbook 이 있다.
7. OCI 배포 절차 문서가 초보자도 따라갈 수 있을 정도로 구체적이다.
8. 외부접속을 위한 포트/URL/체크 절차가 정리되어 있다.
9. 배포 후 확인 체크리스트가 있다.
10. local 환경과 server 환경이 분리되어 있다.

---

## 8. Codex가 작업 후 반드시 남겨야 할 보고

- 추가/수정 파일 목록
- local vs server 환경 차이 요약
- compose server profile 요약
- reverse proxy 구성 요약
- persistent volume 구조 요약
- backup / restore 흐름 요약
- OCI runbook 핵심 절차 요약
- 외부접속 체크 절차 요약
- known limitation
- 실제 서버 올릴 때 사람이 해야 하는 수동 단계 목록

---

## 9. 다음 연결 메모

이 티켓이 끝나면 사실상 국내주식 v1 의 **운영 가능한 release candidate** 상태에 가까워진다.

이후 추가 티켓 후보:
- domain/TLS/auth 보강
- 미국주식 확장
- multi-user / access control
- 장중 정책 고도화
- 성능 최적화