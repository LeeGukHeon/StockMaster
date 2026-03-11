# TICKET-023

Scheduler host worker 분리.

목표:

- scheduler가 더 이상 app 컨테이너 내부 `exec`에 의존하지 않게 한다.
- host Python 3.11 worker가 배치를 직접 실행하게 한다.

완료 기준:

- systemd scheduler unit이 host worker 스크립트를 사용한다.
- host worker venv가 Python 3.11 기준으로 생성/갱신된다.
- metadata_db가 필요한 경우 loopback 포트로 접근한다.
- dry-run 기준 scheduler host worker 경로 검증이 끝난다.

세부 작업:

- host worker venv script
- host scheduler runner script
- systemd unit 변경
- metadata db loopback port 추가
- dry-run 및 stale lock 정리 정책 점검

현재 상태:

- 진행 중

진행 메모:

- systemd unit / host runner / venv 재생성 로직 반영
- 서버 Python 3.11 설치 완료
- 실제 dry-run은 기존 active lock 영향으로 추가 점검 필요

