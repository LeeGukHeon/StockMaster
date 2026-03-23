# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.discord_bot.service import run_discord_bot
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def main() -> int:
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    logger.info("Starting Discord bot service.")
    run_discord_bot(settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
