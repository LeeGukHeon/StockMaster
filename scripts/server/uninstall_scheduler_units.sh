#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd systemctl

SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
TIMER_UNITS=(
  stockmaster-ops-maintenance.timer
  stockmaster-news-morning.timer
  stockmaster-news-after-close.timer
  stockmaster-evaluation.timer
  stockmaster-daily-close.timer
)

for unit in "${TIMER_UNITS[@]}"; do
  systemctl disable --now "${unit}" >/dev/null 2>&1 || true
  rm -f "${SYSTEMD_DIR}/${unit}"
done
rm -f "${SYSTEMD_DIR}/stockmaster-scheduler@.service"
systemctl daemon-reload

log "removed StockMaster scheduler units"
