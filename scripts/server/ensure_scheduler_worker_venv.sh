#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd python3
load_server_env

WORKER_VENV="${STOCKMASTER_SCHEDULER_VENV:-/opt/stockmaster/worker-venv}"
PYTHON_BIN="${STOCKMASTER_SCHEDULER_PYTHON:-$(command -v python3.11 || command -v python3)}"

if ! "${PYTHON_BIN}" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
then
  fail "scheduler worker requires Python 3.11+ (resolved: ${PYTHON_BIN})"
fi

if [[ ! -x "${WORKER_VENV}/bin/python" ]]; then
  log "creating scheduler worker venv at ${WORKER_VENV}"
  "${PYTHON_BIN}" -m venv "${WORKER_VENV}"
elif ! "${WORKER_VENV}/bin/python" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
then
  log "recreating scheduler worker venv with Python 3.11+ at ${WORKER_VENV}"
  rm -rf "${WORKER_VENV}"
  "${PYTHON_BIN}" -m venv "${WORKER_VENV}"
fi

log "installing scheduler worker dependencies"
"${WORKER_VENV}/bin/python" -m pip install --upgrade pip >/dev/null
"${WORKER_VENV}/bin/python" -m pip install -e "${PROJECT_ROOT}" >/dev/null

log "scheduler worker venv ready"
