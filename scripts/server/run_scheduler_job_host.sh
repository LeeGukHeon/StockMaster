#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd python3
load_server_env
mkdir_runtime_dirs

WORKER_VENV="${STOCKMASTER_SCHEDULER_VENV:-/opt/stockmaster/worker-venv}"
[[ -x "${WORKER_VENV}/bin/python" ]] || fail "missing scheduler worker venv: ${WORKER_VENV}"

SERVICE_SLUG="${1:-}"
shift || true
[[ -n "${SERVICE_SLUG}" ]] || fail "usage: run_scheduler_job_host.sh <service-slug> [extra args...]"

export APP_DATA_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/data"
export APP_DUCKDB_PATH="${APP_DATA_DIR}/marts/main.duckdb"
export APP_ARTIFACTS_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/artifacts"

if [[ "${METADATA_DB_ENABLED:-false}" == "true" ]] && [[ "${METADATA_DB_BACKEND:-duckdb}" == "postgres" ]]; then
  compose up -d metadata_db >/dev/null
  export METADATA_DB_URL="postgresql://${METADATA_DB_POSTGRES_USER:-stockmaster}:${METADATA_DB_POSTGRES_PASSWORD:-change_me}@127.0.0.1:${METADATA_DB_HOST_PORT:-5433}/${METADATA_DB_POSTGRES_DB:-stockmaster_meta}"
fi

log "running host scheduler job ${SERVICE_SLUG}"
COMMAND=("${WORKER_VENV}/bin/python" "${PROJECT_ROOT}/scripts/run_scheduled_bundle.py" --service-slug "${SERVICE_SLUG}" --scheduler-run "$@")

if [[ "${SERVICE_SLUG}" == "weekly-training" || "${SERVICE_SLUG}" == "weekly-calibration" || "${SERVICE_SLUG}" == "weekly-policy-research" ]]; then
  if command -v ionice >/dev/null 2>&1; then
    COMMAND=(ionice -c3 "${COMMAND[@]}")
  fi
  if command -v nice >/dev/null 2>&1; then
    COMMAND=(nice -n 10 "${COMMAND[@]}")
  fi
fi

"${COMMAND[@]}"
