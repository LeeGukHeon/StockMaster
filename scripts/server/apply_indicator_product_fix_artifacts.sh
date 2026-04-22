#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd git
require_cmd tar
require_cmd sha256sum

BUNDLE_PATH="${1:-}"
ARCHIVE_PATH="${2:-}"
EXPECTED_BUNDLE_SHA="${3:-}"
EXPECTED_ARCHIVE_SHA="${4:-}"

[[ -n "${BUNDLE_PATH}" ]] || fail "usage: apply_indicator_product_fix_artifacts.sh <bundle-path> <archive-path> [expected-bundle-sha] [expected-archive-sha]"
[[ -n "${ARCHIVE_PATH}" ]] || fail "usage: apply_indicator_product_fix_artifacts.sh <bundle-path> <archive-path> [expected-bundle-sha] [expected-archive-sha]"
[[ -f "${BUNDLE_PATH}" ]] || fail "missing bundle: ${BUNDLE_PATH}"
[[ -f "${ARCHIVE_PATH}" ]] || fail "missing archive: ${ARCHIVE_PATH}"

BUNDLE_SHA="$(sha256sum "${BUNDLE_PATH}" | awk '{print $1}')"
ARCHIVE_SHA="$(sha256sum "${ARCHIVE_PATH}" | awk '{print $1}')"

if [[ -n "${EXPECTED_BUNDLE_SHA}" && "${BUNDLE_SHA}" != "${EXPECTED_BUNDLE_SHA}" ]]; then
  fail "bundle sha mismatch: got ${BUNDLE_SHA}, expected ${EXPECTED_BUNDLE_SHA}"
fi
if [[ -n "${EXPECTED_ARCHIVE_SHA}" && "${ARCHIVE_SHA}" != "${EXPECTED_ARCHIVE_SHA}" ]]; then
  fail "archive sha mismatch: got ${ARCHIVE_SHA}, expected ${EXPECTED_ARCHIVE_SHA}"
fi

log "bundle sha256=${BUNDLE_SHA}"
log "archive sha256=${ARCHIVE_SHA}"
log "repo root=${PROJECT_ROOT}"

git -C "${PROJECT_ROOT}" fetch "${BUNDLE_PATH}" HEAD:indicator-product-fix-import
git -C "${PROJECT_ROOT}" checkout indicator-product-fix-import
tar -xzf "${ARCHIVE_PATH}" -C "${PROJECT_ROOT}"

log "artifacts applied to ${PROJECT_ROOT}"
log "next step: run scripts/server/run_indicator_product_bundle_host.sh <train-end-date> <as-of-date> [shadow-start] [shadow-end]"
