# OCI 배포 가이드

이 문서는 StockMaster를 Oracle Cloud Infrastructure 단일 Ubuntu 서버에 처음 구축하는 절차를 정리합니다.

현재 운영 중 서버의 일상 점검, scheduler, metadata split, 장애 대응은
[RUNBOOK_SERVER_OPERATIONS.md](d:/MyApps/StockMaster/docs/RUNBOOK_SERVER_OPERATIONS.md)를 우선합니다.

## 1. 전제

- 대상 OS: Ubuntu 22.04/24.04 ARM64 또는 x86_64
- 권장 경로
  - 앱 코드: `/opt/stockmaster/app`
  - 런타임 데이터: `/opt/stockmaster/runtime`
  - 백업: `/opt/stockmaster/backups`
- 외부 공개 포트: `80`
- 내부 앱 포트: `8501`

## 2. OCI 콘솔에서 사람이 해야 하는 일

1. Compute instance 생성
2. Public IP 연결 확인
3. SSH 키 등록
4. Security List 또는 NSG에 아래 ingress 허용
   - TCP `22` from your admin IP
   - TCP `80` from `0.0.0.0/0`
5. 필요하면 OS 방화벽에서도 `80/tcp` 허용

Oracle 제공 Ubuntu 이미지에서는 iptables 기본 규칙 때문에 `80/tcp`가 막혀 있을 수 있습니다.

```bash
sudo iptables -C INPUT -p tcp --dport 80 -j ACCEPT 2>/dev/null || \
  sudo iptables -I INPUT 4 -p tcp --dport 80 -j ACCEPT
sudo netfilter-persistent save
```

## 3. 서버 접속 후 초기 준비

```bash
sudo mkdir -p /opt/stockmaster
sudo chown -R "$USER":"$USER" /opt/stockmaster
cd /opt/stockmaster
```

### Docker / Compose 설치

Ubuntu 공식 Docker 저장소 기준:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
newgrp docker
docker --version
docker compose version
```

## 4. 코드 업로드

방법은 둘 중 하나면 충분합니다.

### A. Git pull

```bash
cd /opt/stockmaster
git clone <YOUR_REPO_URL> app
cd app
```

### B. 로컬에서 서버로 복사

Windows PowerShell 예시:

```powershell
scp -i C:\path\to\oci.key -r `
  D:\MyApps\StockMaster\.dockerignore `
  D:\MyApps\StockMaster\.streamlit `
  D:\MyApps\StockMaster\app `
  D:\MyApps\StockMaster\config `
  D:\MyApps\StockMaster\deploy `
  D:\MyApps\StockMaster\docs `
  D:\MyApps\StockMaster\scripts `
  D:\MyApps\StockMaster\Dockerfile `
  D:\MyApps\StockMaster\pyproject.toml `
  D:\MyApps\StockMaster\README.md `
  ubuntu@YOUR_PUBLIC_IP:/opt/stockmaster/app/
```

## 5. 서버 환경 파일 작성

```bash
cd /opt/stockmaster/app
cp deploy/env/.env.server.example deploy/env/.env.server
```

필수 확인 항목:

- `APP_BASE_URL`
- `PUBLIC_PORT`
- `KIS_APP_KEY`, `KIS_APP_SECRET`
- `DART_API_KEY`
- `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
- `DISCORD_WEBHOOK_URL` 필요 시

## 6. 시작 절차

```bash
cd /opt/stockmaster/app
bash scripts/server/start_server.sh
```

이 스크립트는 아래를 수행합니다.

1. 런타임 디렉터리 생성
2. 이미지가 없거나 `FORCE_BUILD=true`일 때만 Docker 이미지 빌드
3. `python scripts/bootstrap.py` 실행
4. app + nginx 기동
5. 로컬 smoke test 수행

이미지 재빌드까지 강제하려면:

```bash
FORCE_BUILD=true bash scripts/server/start_server.sh
```

## 7. 브라우저 확인

- 서버 내부: `curl http://127.0.0.1/healthz`
- 서버 내부: `curl http://127.0.0.1/readyz`
- 외부 브라우저: `http://YOUR_PUBLIC_IP/`

## 8. 재배포 / 업데이트

```bash
cd /opt/stockmaster/app
git pull
bash scripts/server/restart_server.sh
```

코드 복사형 배포라면 새 파일 업로드 후 같은 restart 흐름을 사용합니다.

## 9. 실패 시 1차 확인

```bash
bash scripts/server/tail_server_logs.sh
docker compose --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml ps
curl -fsSL http://127.0.0.1/healthz
curl -fsSL http://127.0.0.1/readyz
```

## 10. systemd 등록

```bash
sudo cp deploy/systemd/stockmaster-compose.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable stockmaster-compose.service
sudo systemctl start stockmaster-compose.service
sudo systemctl status stockmaster-compose.service
```

## 11. 로컬과 서버의 차이

- 로컬은 `.env`를 쓰고, 서버는 `deploy/env/.env.server`를 씁니다.
- 로컬은 `8501` 직노출이 가능하지만, 서버는 nginx를 통해 `80 -> 8501`만 외부 공개합니다.
- 로컬은 소스 bind mount 중심이고, 서버는 체크아웃된 코드 + persistent runtime root를 사용합니다.
- 서버는 `restart: unless-stopped`, healthcheck, 로그 회전, 백업 경로를 함께 둡니다.
