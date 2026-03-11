#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "${SERVER_SCRIPT_DIR}/../.." && pwd)}"
COMPOSE_FILE="${COMPOSE_FILE:-${PROJECT_ROOT}/deploy/docker-compose.server.yml}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/deploy/env/.env.server}"

log() {
  printf '[stockmaster-server] %s\n' "$*"
}

fail() {
  printf '[stockmaster-server] ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing command: $1"
}

load_server_env() {
  [[ -f "${ENV_FILE}" ]] || fail "missing env file: ${ENV_FILE}"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -z "${line}" ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue
    export "${line}"
  done < "${ENV_FILE}"
  export ENV_FILE_PATH="${ENV_FILE}"
}

compose() {
  docker compose $(compose_profile_args) --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
}

compose_profile_args() {
  if [[ "${METADATA_DB_ENABLED:-false}" == "true" ]] && [[ "${METADATA_DB_BACKEND:-duckdb}" == "postgres" ]]; then
    printf '%s ' --profile metadata
  fi
}

runtime_root() {
  printf '%s' "${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}"
}

backup_root() {
  printf '%s' "${STOCKMASTER_BACKUP_ROOT:-/opt/stockmaster/backups}"
}

mkdir_runtime_dirs() {
  local root
  root="$(runtime_root)"
  mkdir -p \
    "${root}/data/raw" \
    "${root}/data/curated" \
    "${root}/data/marts" \
    "${root}/data/cache" \
    "${root}/artifacts" \
    "${root}/logs/app" \
    "${root}/logs/nginx" \
    "${root}/metadata-postgres" \
    "${root}/backups"
  mkdir -p "$(backup_root)"
}

http_get() {
  local url="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "${url}"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -qO- "${url}"
    return
  fi
  fail "curl or wget is required"
}
