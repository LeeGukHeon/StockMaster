from __future__ import annotations

from pathlib import Path


def test_server_deploy_bundle_files_exist():
    required_paths = [
        Path("deploy/docker-compose.server.yml"),
        Path("deploy/env/.env.server.example"),
        Path("deploy/systemd/stockmaster-compose.service"),
        Path("scripts/server/start_server.sh"),
        Path("scripts/server/stop_server.sh"),
        Path("scripts/server/restart_server.sh"),
        Path("scripts/server/tail_server_logs.sh"),
        Path("scripts/server/smoke_test_server.sh"),
        Path("scripts/server/check_public_access.sh"),
        Path("scripts/server/backup_server_data.sh"),
        Path("scripts/server/print_runtime_info.sh"),
        Path("docs/DEPLOY_OCI.md"),
        Path("docs/RUNBOOK_SERVER_OPERATIONS.md"),
        Path("docs/BACKUP_AND_RESTORE.md"),
        Path("docs/EXTERNAL_ACCESS_CHECKLIST.md"),
    ]

    missing = [str(path) for path in required_paths if not path.exists()]
    assert not missing, f"missing deploy bundle files: {missing}"


def test_readme_links_server_deployment_bundle():
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "OCI / server deployment" in readme
    assert "deploy/docker-compose.server.yml" in readme
    assert "docs/DEPLOY_OCI.md" in readme


def test_run_indicator_product_bundle_host_targets_d5_v2_lane():
    script = Path("scripts/server/run_indicator_product_bundle_host.sh").read_text(
        encoding="utf-8"
    )

    assert "--model-spec-ids alpha_swing_d5_v2" in script
    assert "--require-comparator 5:alpha_swing_d5_v1" in script
    assert "--require-comparator 5:alpha_recursive_expanding_v1" in script
    assert "--require-comparator 1:alpha_recursive_expanding_v1" in script
    assert "--require-comparator 1:alpha_topbucket_h1_rolling_120_v1" in script
    assert "--allow-d5-active-freeze" in script
    assert "STOCKMASTER_FORCE_SHADOW_REPLAY" in script
    assert "SHADOW_REPLAY_ARGS" in script
    assert "--no-skip-completed-shadow-dates" in script


def test_verify_indicator_product_bundle_host_maps_recursive_comparator_to_h5():
    script = Path("scripts/server/verify_indicator_product_bundle_host.sh").read_text(
        encoding="utf-8"
    )

    assert "[--require-comparator H:MODEL_SPEC_ID]" in script
    assert "REQUIRED_COMPARATOR_PAIRS=()" in script
    assert 'horizon_text, model_spec_id = item.split(":", 1)' in script
    assert "parsed_required_comparator_pairs.append((int(horizon_text), model_spec_id))" in script
