#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/common.sh"

load_server_env
mkdir_runtime_dirs

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
dest_root="$(backup_root)"
archive_path="${dest_root}/stockmaster-backup-${timestamp}.tgz"

if [[ "${1:-}" == "--dry-run" ]]; then
  log "dry-run backup target: ${archive_path}"
  printf '%s\n' \
    "$(runtime_root)/data" \
    "$(runtime_root)/artifacts" \
    "$(runtime_root)/logs" \
    "${ENV_FILE}" \
    "${PROJECT_ROOT}/deploy"
  exit 0
fi

log "creating backup archive: ${archive_path}"
tar -czf "${archive_path}" \
  -C "$(dirname "$(runtime_root)")" "$(basename "$(runtime_root)")" \
  -C "${PROJECT_ROOT}" deploy \
  -C "$(dirname "${ENV_FILE}")" "$(basename "${ENV_FILE}")"

log "backup complete"
printf '%s\n' "${archive_path}"

