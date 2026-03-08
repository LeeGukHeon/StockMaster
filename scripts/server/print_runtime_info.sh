#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/common.sh"

require_cmd docker
load_server_env

log "project root: ${PROJECT_ROOT}"
log "compose file: ${COMPOSE_FILE}"
log "env file: ${ENV_FILE}"
log "runtime root: $(runtime_root)"
log "backup root: $(backup_root)"
df -h "$(runtime_root)" "$(backup_root)" 2>/dev/null || true
compose ps

