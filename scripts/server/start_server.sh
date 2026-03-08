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

log "running bootstrap"
compose run --rm app python scripts/bootstrap.py

log "starting server stack"
compose up -d
compose ps

log "running local smoke test"
bash "${SCRIPT_DIR}/smoke_test_server.sh"
