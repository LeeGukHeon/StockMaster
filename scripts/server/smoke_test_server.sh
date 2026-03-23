#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/common.sh"

require_cmd docker
load_server_env

log "checking docker compose state"
compose ps

if [[ "${METADATA_DB_ENABLED:-false}" == "true" ]] && [[ "${METADATA_DB_BACKEND:-duckdb}" == "postgres" ]]; then
  log "checking metadata db readiness"
  compose exec -T metadata_db pg_isready \
    -U "${METADATA_DB_POSTGRES_USER:-stockmaster}" \
    -d "${METADATA_DB_POSTGRES_DB:-stockmaster_meta}" >/dev/null
fi

log "checking discord bot service"
systemctl is-active --quiet stockmaster-discord-bot.service

log "smoke test passed"
