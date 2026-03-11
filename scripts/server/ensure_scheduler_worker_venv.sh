#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd python3
load_server_env

WORKER_VENV="${STOCKMASTER_SCHEDULER_VENV:-/opt/stockmaster/worker-venv}"

if [[ ! -x "${WORKER_VENV}/bin/python" ]]; then
  log "creating scheduler worker venv at ${WORKER_VENV}"
  python3 -m venv "${WORKER_VENV}"
fi

log "installing scheduler worker dependencies"
"${WORKER_VENV}/bin/python" -m pip install --upgrade pip >/dev/null
"${WORKER_VENV}/bin/python" -m pip install -e "${PROJECT_ROOT}" >/dev/null

log "scheduler worker venv ready"
