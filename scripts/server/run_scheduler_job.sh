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

log "ensuring stockmaster stack is up for scheduler job ${SERVICE_SLUG}"
compose up -d app nginx >/dev/null

log "running scheduler job ${SERVICE_SLUG}"
compose exec -T app python scripts/run_scheduled_bundle.py --service-slug "${SERVICE_SLUG}" --scheduler-run "$@"
