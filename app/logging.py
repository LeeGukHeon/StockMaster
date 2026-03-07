from __future__ import annotations

import json
import logging
import logging.config
from pathlib import Path
from typing import Any

import yaml

from app.common.paths import ensure_directory
from app.common.run_context import current_run_id, current_run_type
from app.common.time import utc_now
from app.settings import Settings

_RESERVED_LOG_KEYS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        extra: dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_KEYS:
                extra[key] = value
        if record.exc_info:
            extra["exception"] = self.formatException(record.exc_info)
        payload = {
            "timestamp": utc_now().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "run_id": current_run_id(),
            "run_type": current_run_type(),
            "message": record.getMessage(),
            "extra": extra,
        }
        return json.dumps(payload, ensure_ascii=False, default=str)


def _load_logging_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def configure_logging(settings: Settings) -> logging.Logger:
    ensure_directory(settings.paths.logs_dir)
    config_path = settings.paths.project_root / "config" / "logging.yaml"
    config = _load_logging_config(config_path)

    file_handler = config["handlers"]["file"]
    file_handler["filename"] = str(settings.paths.logs_dir / settings.logging.filename)
    file_handler["level"] = settings.logging.level
    config["handlers"]["console"]["level"] = settings.logging.level
    config["root"]["level"] = settings.logging.level

    logging.config.dictConfig(config)
    return logging.getLogger("app")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
