#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/common.sh"

require_cmd docker
load_server_env
mkdir_runtime_dirs

if [[ "${FORCE_BUILD:-false}" == "true" ]] || ! docker image inspect "${STOCKMASTER_SERVER_IMAGE:-stockmaster-server:latest}" >/dev/null 2>&1; then
  log "building server image"
  compose build
else
  log "reusing existing server image: ${STOCKMASTER_SERVER_IMAGE:-stockmaster-server:latest}"
fi

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

  log "bootstrapping metadata store"
  compose run --rm app python scripts/bootstrap_metadata_store.py
fi

log "running bootstrap"
compose run --rm app python scripts/bootstrap.py

log "starting server stack"
compose up -d
compose ps

log "running local smoke test"
bash "${SCRIPT_DIR}/smoke_test_server.sh"
