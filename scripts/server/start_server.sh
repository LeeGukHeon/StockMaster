#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/common.sh"

require_cmd docker
require_cmd python3
load_server_env
mkdir_runtime_dirs

WORKER_VENV="${STOCKMASTER_SCHEDULER_VENV:-/opt/stockmaster/worker-venv}"
[[ -x "${WORKER_VENV}/bin/python" ]] || fail "missing scheduler worker venv: ${WORKER_VENV}"

export APP_DATA_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/data"
export APP_DUCKDB_PATH="${APP_DATA_DIR}/marts/main.duckdb"
export APP_ARTIFACTS_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/artifacts"

if [[ "${METADATA_DB_ENABLED:-false}" == "true" ]] && [[ "${METADATA_DB_BACKEND:-duckdb}" == "postgres" ]]; then
  log "starting metadata db"
  compose up -d metadata_db
  log "waiting for metadata db readiness"
  for attempt in $(seq 1 30); do
    if compose exec -T metadata_db pg_isready \
      -U "${METADATA_DB_POSTGRES_USER:-stockmaster}" \
      -d "${METADATA_DB_POSTGRES_DB:-stockmaster_meta}" >/dev/null 2>&1; then
      break
    fi
    if [[ "${attempt}" == "30" ]]; then
      fail "metadata db did not become ready in time"
    fi
    sleep 2
  done

  export METADATA_DB_URL="postgresql://${METADATA_DB_POSTGRES_USER:-stockmaster}:${METADATA_DB_POSTGRES_PASSWORD:-change_me}@127.0.0.1:${METADATA_DB_HOST_PORT:-5433}/${METADATA_DB_POSTGRES_DB:-stockmaster_meta}"

  log "bootstrapping metadata store"
  "${WORKER_VENV}/bin/python" "${PROJECT_ROOT}/scripts/bootstrap_metadata_store.py"

  if [[ "${METADATA_DB_BOOTSTRAP_FROM_DUCKDB:-false}" == "true" ]]; then
    log "running initial metadata migration if postgres target is empty"
    "${WORKER_VENV}/bin/python" "${PROJECT_ROOT}/scripts/migrate_duckdb_metadata_to_postgres.py" --truncate-first --if-target-empty
  else
    log "skipping DuckDB metadata migration; set METADATA_DB_BOOTSTRAP_FROM_DUCKDB=true to run it"
  fi
fi

if [[ "${STOCKMASTER_BOOTSTRAP_ON_START:-false}" == "true" ]]; then
  log "running bootstrap"
  "${WORKER_VENV}/bin/python" "${PROJECT_ROOT}/scripts/bootstrap.py"
else
  log "skipping DuckDB bootstrap; set STOCKMASTER_BOOTSTRAP_ON_START=true to run it"
fi

log "starting metadata-only server stack"
compose up -d metadata_db
compose ps

log "running local smoke test"
bash "${SCRIPT_DIR}/smoke_test_server.sh"
