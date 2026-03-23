#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd python3
load_server_env
mkdir_runtime_dirs

bash "${SERVER_SCRIPT_DIR}/ensure_scheduler_worker_venv.sh"

WORKER_VENV="${STOCKMASTER_SCHEDULER_VENV:-/opt/stockmaster/worker-venv}"
[[ -x "${WORKER_VENV}/bin/python" ]] || fail "missing scheduler worker venv: ${WORKER_VENV}"

export APP_DATA_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/data"
export APP_DUCKDB_PATH="${APP_DATA_DIR}/marts/main.duckdb"

if [[ "${METADATA_DB_ENABLED:-false}" == "true" ]] && [[ "${METADATA_DB_BACKEND:-duckdb}" == "postgres" ]]; then
  compose up -d metadata_db >/dev/null
  export METADATA_DB_URL="postgresql://${METADATA_DB_POSTGRES_USER:-stockmaster}:${METADATA_DB_POSTGRES_PASSWORD:-change_me}@127.0.0.1:${METADATA_DB_HOST_PORT:-5433}/${METADATA_DB_POSTGRES_DB:-stockmaster_meta}"
fi

log "starting discord bot host process"
exec "${WORKER_VENV}/bin/python" "${PROJECT_ROOT}/scripts/run_discord_bot.py"
