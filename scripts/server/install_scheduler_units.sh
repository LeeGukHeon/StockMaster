#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd systemctl

SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
UNIT_SOURCE_DIR="${PROJECT_ROOT}/deploy/systemd"
TIMER_UNITS=(
  stockmaster-ops-maintenance.timer
  stockmaster-news-morning.timer
  stockmaster-intraday-assist.timer
  stockmaster-news-after-close.timer
  stockmaster-evaluation.timer
  stockmaster-daily-close.timer
  stockmaster-daily-audit-lite.timer
  stockmaster-weekly-training.timer
  stockmaster-weekly-calibration.timer
)

install -m 0644 "${UNIT_SOURCE_DIR}/stockmaster-scheduler@.service" "${SYSTEMD_DIR}/stockmaster-scheduler@.service"
for unit in "${TIMER_UNITS[@]}"; do
  install -m 0644 "${UNIT_SOURCE_DIR}/${unit}" "${SYSTEMD_DIR}/${unit}"
done

systemctl daemon-reload
for unit in "${TIMER_UNITS[@]}"; do
  systemctl enable --now "${unit}"
done

log "installed and enabled StockMaster scheduler timers"
