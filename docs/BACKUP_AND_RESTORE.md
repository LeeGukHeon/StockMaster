# 백업 및 복구

## 백업 대상

반드시 보존:

- `/opt/stockmaster/runtime/data`
- `/opt/stockmaster/runtime/artifacts`
- `/opt/stockmaster/runtime/logs`
- `deploy/env/.env.server`
- `deploy/`

자동 삭제 금지:

- curated core data
- predictions
- evaluations
- portfolio snapshots

## 백업 실행

```bash
cd /opt/stockmaster/app
bash scripts/server/backup_server_data.sh
```

dry-run:

```bash
bash scripts/server/backup_server_data.sh --dry-run
```

기본 저장 위치:

- `/opt/stockmaster/backups`

## 복구 절차

1. 서비스 중지

```bash
bash scripts/server/stop_server.sh
```

2. 백업 압축 해제

```bash
mkdir -p /tmp/stockmaster-restore
tar -xzf /opt/stockmaster/backups/stockmaster-backup-YYYYmmddTHHMMSSZ.tgz -C /tmp/stockmaster-restore
```

3. 필요한 경로만 복원

```bash
rsync -av /tmp/stockmaster-restore/runtime/ /opt/stockmaster/runtime/
rsync -av /tmp/stockmaster-restore/deploy/ /opt/stockmaster/app/deploy/
cp /tmp/stockmaster-restore/.env.server /opt/stockmaster/app/deploy/env/.env.server
```

4. 서비스 재기동

```bash
bash scripts/server/start_server.sh
```

5. smoke test

```bash
bash scripts/server/smoke_test_server.sh
```

## 주의사항

- restore 전에 현재 상태를 별도 백업해 두는 편이 안전합니다.
- `.env.server` 복원 시 secret 값이 최신 운영값과 맞는지 다시 확인해야 합니다.
- destructive migration은 백업 없이 수행하지 않습니다.

