#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/common.sh"

require_cmd docker
load_server_env

local_base="http://127.0.0.1:${PUBLIC_PORT:-80}"

log "checking docker compose state"
compose ps

log "checking /healthz"
http_get "${local_base}/healthz" >/dev/null

log "checking /readyz"
http_get "${local_base}/readyz" >/dev/null

log "checking root page"
http_get "${local_base}/" >/dev/null

log "smoke test passed"

