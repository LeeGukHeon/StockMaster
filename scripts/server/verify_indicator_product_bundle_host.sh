#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SERVER_SCRIPT_DIR}/common.sh"

require_cmd python3
load_server_env
mkdir_runtime_dirs

WORKER_VENV="${STOCKMASTER_SCHEDULER_VENV:-/opt/stockmaster/worker-venv}"
[[ -x "${WORKER_VENV}/bin/python" ]] || fail "missing scheduler worker venv: ${WORKER_VENV}"

AS_OF_DATE="${1:-}"
[[ -n "${AS_OF_DATE}" ]] || fail "usage: verify_indicator_product_bundle_host.sh <as-of-date> [--expected-preserved-horizon H:MODEL_SPEC_ID] [--require-comparator-model-spec-id MODEL_SPEC_ID]"
shift

EXPECTED_PRESERVED_HORIZONS=()
REQUIRED_COMPARATOR_MODEL_SPEC_IDS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --expected-preserved-horizon)
      [[ $# -ge 2 ]] || fail "missing value for --expected-preserved-horizon"
      EXPECTED_PRESERVED_HORIZONS+=("$2")
      shift 2
      ;;
    --require-comparator-model-spec-id)
      [[ $# -ge 2 ]] || fail "missing value for --require-comparator-model-spec-id"
      REQUIRED_COMPARATOR_MODEL_SPEC_IDS+=("$2")
      shift 2
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

export APP_DATA_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/data"
export APP_DUCKDB_PATH="${APP_DATA_DIR}/marts/main.duckdb"
export APP_ARTIFACTS_DIR="${STOCKMASTER_RUNTIME_ROOT:-/opt/stockmaster/runtime}/artifacts"

PYTHON="${WORKER_VENV}/bin/python"

"${PYTHON}" - <<'PY' "${PROJECT_ROOT}" "${AS_OF_DATE}" "$(printf '%s\n' "${EXPECTED_PRESERVED_HORIZONS[@]}")" "$(printf '%s\n' "${REQUIRED_COMPARATOR_MODEL_SPEC_IDS[@]}")"
from pathlib import Path
from datetime import date
import json
import sys

project_root = Path(sys.argv[1]).resolve()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from app.settings import load_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection

as_of_date = date.fromisoformat(sys.argv[2])
expected_preserved_horizons = [
    line for line in sys.argv[3].splitlines() if line.strip()
]
required_comparator_model_spec_ids = [
    line for line in sys.argv[4].splitlines() if line.strip()
]
settings = load_settings(project_root=project_root)
with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
    bootstrap_core_tables(connection)
    active = connection.execute(
        """
        SELECT horizon, model_spec_id, active_alpha_model_id, effective_from_date
        FROM fact_alpha_active_model
        WHERE active_flag = TRUE
        ORDER BY horizon, effective_from_date DESC
        """
    ).fetchdf()
    gap = connection.execute(
        """
        SELECT
            summary_date,
            window_name,
            horizon,
            model_spec_id,
            insufficient_history_flag,
            matured_selection_date_count,
            required_selection_date_count,
            selected_top5_mean_realized_excess_return,
            drag_vs_raw_top5
        FROM fact_alpha_shadow_selection_gap_scorecard
        WHERE summary_date = (
            SELECT MAX(summary_date)
            FROM fact_alpha_shadow_selection_gap_scorecard
            WHERE summary_date <= ?
        )
        ORDER BY window_name, horizon, model_spec_id
        """,
        [as_of_date],
    ).fetchdf()
    validation = connection.execute(
        """
        SELECT run_id, status, notes, started_at
        FROM ops_run_manifest
        WHERE run_type = 'validate_alpha_model_v1'
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchdf()
    latest_summary_date_row = connection.execute(
        """
        SELECT MAX(summary_date)
        FROM fact_alpha_shadow_evaluation_summary
        WHERE summary_date <= ?
        """,
        [as_of_date],
    ).fetchone()
    latest_summary_date = (
        None
        if latest_summary_date_row is None or latest_summary_date_row[0] is None
        else latest_summary_date_row[0]
    )
    comparator_rows = []
    if latest_summary_date is not None and required_comparator_model_spec_ids:
        comparator_rows = connection.execute(
            f"""
            SELECT summary_date, horizon, model_spec_id, segment_value, mean_realized_excess_return
            FROM fact_alpha_shadow_evaluation_summary
            WHERE summary_date = ?
              AND model_spec_id IN ({",".join("?" for _ in required_comparator_model_spec_ids)})
              AND segment_value = 'top5'
            ORDER BY horizon, model_spec_id
            """,
            [latest_summary_date, *required_comparator_model_spec_ids],
        ).fetchdf().to_dict(orient="records")

artifact_root = settings.paths.artifacts_dir
discord_root = artifact_root / f"discord/as_of_date={as_of_date.isoformat()}"
release_root = artifact_root / f"release_candidate_checklist/as_of_date={as_of_date.isoformat()}"

active_model_map = {
    int(row["horizon"]): str(row["model_spec_id"])
    for row in active.to_dict(orient="records")
}
preserved_horizon_mismatches: list[dict[str, object]] = []
for item in expected_preserved_horizons:
    horizon_text, expected_model_spec_id = item.split(":", 1)
    horizon = int(horizon_text)
    actual_model_spec_id = active_model_map.get(horizon)
    if actual_model_spec_id != expected_model_spec_id:
        preserved_horizon_mismatches.append(
            {
                "horizon": horizon,
                "expected_model_spec_id": expected_model_spec_id,
                "actual_model_spec_id": actual_model_spec_id,
            }
        )

found_comparator_pairs = {
    (int(row["horizon"]), str(row["model_spec_id"]))
    for row in comparator_rows
}
missing_comparator_model_spec_ids = []
for model_spec_id in required_comparator_model_spec_ids:
    expected_horizon = 1 if model_spec_id == "alpha_topbucket_h1_rolling_120_v1" else 1
    if (expected_horizon, model_spec_id) not in found_comparator_pairs:
        missing_comparator_model_spec_ids.append(model_spec_id)

payload = {
    "as_of_date": as_of_date.isoformat(),
    "duckdb_path": str(settings.paths.duckdb_path),
    "active_models": active.to_dict(orient="records"),
    "selection_gap_rows": gap.to_dict(orient="records"),
    "latest_validation_run": validation.to_dict(orient="records"),
    "expected_preserved_horizons": expected_preserved_horizons,
    "preserved_horizon_mismatches": preserved_horizon_mismatches,
    "required_comparator_model_spec_ids": required_comparator_model_spec_ids,
    "latest_summary_date": None if latest_summary_date is None else str(latest_summary_date),
    "comparator_rows": comparator_rows,
    "missing_comparator_model_spec_ids": missing_comparator_model_spec_ids,
    "discord_artifact_dirs": sorted(path.name for path in discord_root.glob('*')) if discord_root.exists() else [],
    "release_artifact_dirs": sorted(path.name for path in release_root.glob('*')) if release_root.exists() else [],
}
print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
if preserved_horizon_mismatches or missing_comparator_model_spec_ids:
    raise SystemExit(1)
PY
