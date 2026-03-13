#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/common.sh"

load_server_env

bind_host="${PUBLIC_BIND_HOST:-127.0.0.1}"
if [[ "${bind_host}" == "127.0.0.1" ]] || [[ "${bind_host}" == "::1" ]] || [[ "${bind_host}" == "localhost" ]]; then
  fail "public access is disabled because PUBLIC_BIND_HOST=${bind_host}. Use ssh port forwarding or set PUBLIC_BIND_HOST=0.0.0.0 explicitly."
fi

target="${1:-${APP_BASE_URL:-}}"
[[ -n "${target}" ]] || fail "public URL missing. Pass a URL or set APP_BASE_URL."
target="${target%/}"

log "checking public access: ${target}"
http_get "${target}/healthz" >/dev/null
http_get "${target}/readyz" >/dev/null
http_get "${target}/" >/dev/null
log "public access check passed"
