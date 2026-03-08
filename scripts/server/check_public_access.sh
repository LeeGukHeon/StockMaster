#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/common.sh"

load_server_env

target="${1:-${APP_BASE_URL:-}}"
[[ -n "${target}" ]] || fail "public URL missing. Pass a URL or set APP_BASE_URL."
target="${target%/}"

log "checking public access: ${target}"
http_get "${target}/healthz" >/dev/null
http_get "${target}/readyz" >/dev/null
http_get "${target}/" >/dev/null
log "public access check passed"

