from __future__ import annotations

from pathlib import Path

import yaml


def test_server_compose_declares_app_and_proxy():
    compose_path = Path("deploy/docker-compose.server.yml")
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    assert compose["name"] == "${DOCKER_COMPOSE_PROJECT_NAME:-stockmaster}"
    assert set(compose["services"]) == {"app", "nginx"}
    assert "healthcheck" in compose["services"]["app"]
    assert "healthcheck" in compose["services"]["nginx"]
    assert "ports" not in compose["services"]["app"]
    assert compose["services"]["nginx"]["ports"] == ["${PUBLIC_PORT:-80}:80"]


def test_server_env_example_is_tracked_and_documents_runtime():
    env_example = Path("deploy/env/.env.server.example").read_text(encoding="utf-8")

    assert "APP_ENV=server" in env_example
    assert "STOCKMASTER_RUNTIME_ROOT=/opt/stockmaster/runtime" in env_example
    assert "PUBLIC_PORT=80" in env_example
    assert "APP_BASE_URL=http://YOUR_PUBLIC_IP" in env_example
