#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd systemctl

bash "${SERVER_SCRIPT_DIR}/ensure_scheduler_worker_venv.sh"

SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
UNIT_SOURCE_DIR="${PROJECT_ROOT}/deploy/systemd"

install -m 0644 "${UNIT_SOURCE_DIR}/stockmaster-discord-bot.service" "${SYSTEMD_DIR}/stockmaster-discord-bot.service"
chmod +x "${PROJECT_ROOT}/scripts/server/run_discord_bot_host.sh"

systemctl daemon-reload
systemctl enable --now stockmaster-discord-bot.service

log "installed and started StockMaster Discord bot service"
