# 서버 운영 런북

## 기본 경로

- 앱: `/opt/stockmaster/app`
- 런타임: `/opt/stockmaster/runtime`
- 백업: `/opt/stockmaster/backups`

## 기본 명령

```bash
cd /opt/stockmaster/app
bash scripts/server/start_server.sh
bash scripts/server/stop_server.sh
bash scripts/server/restart_server.sh
bash scripts/server/tail_server_logs.sh
bash scripts/server/print_runtime_info.sh
```

## 운영 체크 순서

1. `docker compose --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml ps`
2. `curl http://127.0.0.1/healthz`
3. `curl http://127.0.0.1/readyz`
4. `bash scripts/server/check_public_access.sh http://YOUR_PUBLIC_IP`
5. `bash scripts/server/tail_server_logs.sh`

## 재부팅 후 확인

systemd를 쓰는 경우:

```bash
sudo systemctl status stockmaster-compose.service
```

systemd를 쓰지 않는 경우:

```bash
cd /opt/stockmaster/app
bash scripts/server/start_server.sh
```

## 업그레이드 절차

1. 백업 수행
2. 코드 업데이트
3. `restart_server.sh`
4. smoke test
5. 외부 접속 확인

이미지 재빌드까지 필요하면:

```bash
FORCE_BUILD=true bash scripts/server/start_server.sh
```

## 롤백 절차

1. 이전 커밋 또는 이전 배포 패키지로 코드 복원
2. `bash scripts/server/restart_server.sh`
3. 필요 시 최신 백업에서 데이터만 복구

## 로그 확인

- app 로그: `/opt/stockmaster/runtime/logs/app`
- nginx 로그: `/opt/stockmaster/runtime/logs/nginx`
- compose 실시간 로그:

```bash
bash scripts/server/tail_server_logs.sh
```

## 장애 분류

- `healthz` 실패: nginx 또는 호스트 포트 문제
- `readyz` 실패: upstream app 부팅 실패 또는 Streamlit 문제
- 루트 경로 실패: 프록시 연결 또는 Streamlit 렌더 문제
- 외부 접속 실패: OCI ingress / 공인 IP / OS 방화벽 / 포트 매핑 문제

Oracle Ubuntu 기본 iptables가 `80/tcp`를 막는 경우:

```bash
sudo iptables -C INPUT -p tcp --dport 80 -j ACCEPT 2>/dev/null || \
  sudo iptables -I INPUT 4 -p tcp --dport 80 -j ACCEPT
sudo netfilter-persistent save
```
