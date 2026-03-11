#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd docker
load_server_env
mkdir_runtime_dirs

SERVICE_SLUG="${1:-}"
shift || true
[[ -n "${SERVICE_SLUG}" ]] || fail "usage: run_scheduler_job.sh <service-slug> [extra args...]"

log "run_scheduler_job.sh is deprecated; delegating to host worker runner"
exec "${SERVER_SCRIPT_DIR}/run_scheduler_job_host.sh" "${SERVICE_SLUG}" "$@"
