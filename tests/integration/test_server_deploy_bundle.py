from __future__ import annotations

from pathlib import Path

from app.ops.scheduler import SCHEDULED_JOB_MAP


def test_server_deploy_bundle_files_exist():
    required_paths = [
        Path("deploy/docker-compose.server.yml"),
        Path("deploy/env/.env.server.example"),
        Path("deploy/systemd/stockmaster-compose.service"),
        Path("deploy/systemd/stockmaster-ops-maintenance.timer"),
        Path("deploy/systemd/stockmaster-news-morning.timer"),
        Path("deploy/systemd/stockmaster-news-after-close.timer"),
        Path("deploy/systemd/stockmaster-evaluation.timer"),
        Path("deploy/systemd/stockmaster-daily-close.timer"),
        Path("scripts/server/start_server.sh"),
        Path("scripts/server/stop_server.sh"),
        Path("scripts/server/restart_server.sh"),
        Path("scripts/server/tail_server_logs.sh"),
        Path("scripts/server/smoke_test_server.sh"),
        Path("scripts/server/check_public_access.sh"),
        Path("scripts/server/backup_server_data.sh"),
        Path("scripts/server/print_runtime_info.sh"),
        Path("docs/operations/STOCKMASTER_UNIFIED_MANUAL_KO.md"),
    ]

    missing = [str(path) for path in required_paths if not path.exists()]
    assert not missing, f"missing deploy bundle files: {missing}"


def test_readme_links_server_deployment_bundle():
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "OCI / server deployment" in readme
    assert "deploy/docker-compose.server.yml" in readme
    assert "docs/operations/STOCKMASTER_UNIFIED_MANUAL_KO.md" in readme


def test_daily_close_timer_targets_scheduler_daily_close_slug():
    timer = Path("deploy/systemd/stockmaster-daily-close.timer").read_text(
        encoding="utf-8"
    )
    service = Path("deploy/systemd/stockmaster-scheduler@.service").read_text(
        encoding="utf-8"
    )
    job = SCHEDULED_JOB_MAP["daily_close"]

    assert f"Unit=stockmaster-scheduler@{job.service_slug}.service" in timer
    assert f"OnCalendar={job.on_calendar[0]}" in timer
    assert "run_scheduler_job_host.sh %i" in service


def test_run_indicator_product_bundle_host_targets_current_v4_h5_lane():
    script = Path("scripts/server/run_indicator_product_bundle_host.sh").read_text(
        encoding="utf-8"
    )

    assert "--model-spec-ids alpha_practical_d5_v3" in script
    assert "--require-comparator 5:alpha_practical_d5_v3" in script
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
