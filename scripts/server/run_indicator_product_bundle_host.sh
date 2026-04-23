#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd python3
load_server_env
mkdir_runtime_dirs

WORKER_VENV="${STOCKMASTER_SCHEDULER_VENV:-/opt/stockmaster/worker-venv}"
[[ -x "${WORKER_VENV}/bin/python" ]] || fail "missing scheduler worker venv: ${WORKER_VENV}"

TRAIN_END_DATE="${1:-}"
AS_OF_DATE="${2:-}"
SHADOW_START_SELECTION_DATE="${3:-${AS_OF_DATE}}"
SHADOW_END_SELECTION_DATE="${4:-${AS_OF_DATE}}"
if [[ $# -gt 4 ]]; then
  FREEZE_HORIZONS=("${@:5}")
else
  FREEZE_HORIZONS=(1)
fi
ALLOW_D5_ACTIVE_FREEZE_ARGS=()
for freeze_horizon in "${FREEZE_HORIZONS[@]}"; do
  if [[ "${freeze_horizon}" == "5" ]]; then
    ALLOW_D5_ACTIVE_FREEZE_ARGS=(--allow-d5-active-freeze)
    break
  fi
done

[[ -n "${TRAIN_END_DATE}" ]] || fail "usage: run_indicator_product_bundle_host.sh <train-end-date> <as-of-date> [shadow-start-selection-date] [shadow-end-selection-date] [freeze-horizons...]"
[[ -n "${AS_OF_DATE}" ]] || fail "usage: run_indicator_product_bundle_host.sh <train-end-date> <as-of-date> [shadow-start-selection-date] [shadow-end-selection-date] [freeze-horizons...]"

export APP_DATA_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/data"
export APP_DUCKDB_PATH="${APP_DATA_DIR}/marts/main.duckdb"
export APP_ARTIFACTS_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/artifacts"

if [[ "${METADATA_DB_ENABLED:-false}" == "true" ]] && [[ "${METADATA_DB_BACKEND:-duckdb}" == "postgres" ]]; then
  compose up -d metadata_db >/dev/null
  export METADATA_DB_URL="postgresql://${METADATA_DB_POSTGRES_USER:-stockmaster}:${METADATA_DB_POSTGRES_PASSWORD:-change_me}@127.0.0.1:${METADATA_DB_HOST_PORT:-5433}/${METADATA_DB_POSTGRES_DB:-stockmaster_meta}"
fi

PYTHON="${WORKER_VENV}/bin/python"
PRESERVED_HORIZON_ARGS=()
for horizon in 1 5; do
  freeze_requested=false
  for freeze_horizon in "${FREEZE_HORIZONS[@]}"; do
    if [[ "${freeze_horizon}" == "${horizon}" ]]; then
      freeze_requested=true
      break
    fi
  done
  if [[ "${freeze_requested}" == true ]]; then
    continue
  fi
  active_spec_id="$("${PYTHON}" - <<'PY' "${APP_DUCKDB_PATH}" "${AS_OF_DATE}" "${horizon}"
from datetime import date
import duckdb
import sys

db_path = sys.argv[1]
as_of_date = date.fromisoformat(sys.argv[2])
horizon = int(sys.argv[3])
con = duckdb.connect(db_path, read_only=True)
row = con.execute(
    """
    SELECT model_spec_id
    FROM fact_alpha_active_model
    WHERE horizon = ?
      AND effective_from_date <= ?
      AND (effective_to_date IS NULL OR effective_to_date >= ?)
      AND active_flag = TRUE
    ORDER BY effective_from_date DESC, created_at DESC, active_alpha_model_id DESC
    LIMIT 1
    """,
    [horizon, as_of_date, as_of_date],
).fetchone()
con.close()
print("" if row is None or row[0] is None else str(row[0]))
PY
)"
  if [[ -n "${active_spec_id}" ]]; then
    PRESERVED_HORIZON_ARGS+=(--expected-preserved-horizon "${horizon}:${active_spec_id}")
  fi
done

log "checking indicator-product readiness train_end_date=${TRAIN_END_DATE}"
"${PYTHON}" "${PROJECT_ROOT}/scripts/check_alpha_indicator_product_readiness.py" \
  --train-end-date "${TRAIN_END_DATE}" \
  --horizons 1 5 \
  --model-spec-ids alpha_swing_d5_v2 alpha_swing_d5_v1

log "rematerializing alpha model specs"
"${PYTHON}" "${PROJECT_ROOT}/scripts/materialize_alpha_model_specs.py"

log "running indicator-product bundle"
"${PYTHON}" "${PROJECT_ROOT}/scripts/run_alpha_indicator_product_bundle.py" \
  --train-end-date "${TRAIN_END_DATE}" \
  --as-of-date "${AS_OF_DATE}" \
  --shadow-start-selection-date "${SHADOW_START_SELECTION_DATE}" \
  --shadow-end-selection-date "${SHADOW_END_SELECTION_DATE}" \
  --horizons 1 5 \
  --model-spec-ids alpha_swing_d5_v2 alpha_swing_d5_v1 \
  --rolling-windows 20 60 \
  --backfill-shadow-history \
  --freeze-horizons "${FREEZE_HORIZONS[@]}" \
  "${ALLOW_D5_ACTIVE_FREEZE_ARGS[@]}"

log "refreshing discord bot read store"
"${PYTHON}" "${PROJECT_ROOT}/scripts/materialize_discord_bot_read_store.py" \
  --as-of-date "${AS_OF_DATE}"

log "rendering release checklist"
"${PYTHON}" "${PROJECT_ROOT}/scripts/render_release_candidate_checklist.py" \
  --as-of-date "${AS_OF_DATE}" \
  --dry-run

log "rendering discord eod report"
"${PYTHON}" "${PROJECT_ROOT}/scripts/render_discord_eod_report.py" \
  --as-of-date "${AS_OF_DATE}" \
  --dry-run

log "verifying indicator-product bundle outputs"
"${SERVER_SCRIPT_DIR}/verify_indicator_product_bundle_host.sh" \
  "${AS_OF_DATE}" \
  "${PRESERVED_HORIZON_ARGS[@]}" \
  --require-comparator 5:alpha_swing_d5_v1 \
  --require-comparator 5:alpha_recursive_expanding_v1 \
  --require-comparator 1:alpha_recursive_expanding_v1 \
  --require-comparator 1:alpha_topbucket_h1_rolling_120_v1

log "indicator-product host bundle completed"
