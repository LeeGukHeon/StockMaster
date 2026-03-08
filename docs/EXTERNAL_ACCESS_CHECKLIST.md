# 외부 접속 체크리스트

## 1. 브라우저 전에 확인할 것

- 인스턴스에 public IP가 있는가
- OCI ingress rule이 `80/tcp`를 허용하는가
- SSH `22/tcp`는 관리자 IP만 허용했는가
- 서버 OS 방화벽이 `80/tcp`를 막고 있지 않은가
- Oracle Ubuntu 기본 iptables가 `80/tcp`를 거부하고 있지 않은가
- compose에서 `PUBLIC_PORT=80`으로 올라왔는가

## 2. 서버 내부 점검 순서

```bash
docker compose --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml ps
curl -fsSL http://127.0.0.1/healthz
curl -fsSL http://127.0.0.1/readyz
curl -I http://127.0.0.1/
```

## 3. 외부 점검 순서

```bash
bash scripts/server/check_public_access.sh http://YOUR_PUBLIC_IP
```

브라우저에서 확인:

- `http://YOUR_PUBLIC_IP/`

## 4. 실패 시 확인 순서

1. `docker compose ps`
2. `bash scripts/server/tail_server_logs.sh`
3. `curl http://127.0.0.1/healthz`
4. `curl http://127.0.0.1/readyz`
5. OCI ingress rule 재확인
6. OS 방화벽 재확인
7. public IP가 바뀌지 않았는지 확인

iptables 예시:

```bash
sudo iptables -C INPUT -p tcp --dport 80 -j ACCEPT 2>/dev/null || \
  sudo iptables -I INPUT 4 -p tcp --dport 80 -j ACCEPT
sudo netfilter-persistent save
```

## 5. 보안 원칙

- 외부 공개는 `80`만 기본값으로 둡니다.
- `8501`은 외부에 직접 열지 않습니다.
- `.env.server`와 SSH 키는 서버 밖으로 재배포하지 않습니다.
