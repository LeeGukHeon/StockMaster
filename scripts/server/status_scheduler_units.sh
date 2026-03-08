#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd systemctl

systemctl list-timers 'stockmaster-*.timer' --all
echo
systemctl --no-pager --full status stockmaster-scheduler@ops-maintenance.service || true
