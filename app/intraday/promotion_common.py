from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from app.ml.registry import load_active_alpha_model
from app.settings import Settings

ALPHA_STABILIZATION_TRADING_DAYS = 1


@dataclass(slots=True)
class AlphaLineageStatus:
    lineage_by_horizon: dict[int, str]
    blocked_horizons: list[int]
    detail_by_horizon: dict[int, dict[str, object]]


def resolve_alpha_lineage_status(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date: date,
    horizons: list[int],
    stabilization_trading_days: int = ALPHA_STABILIZATION_TRADING_DAYS,
) -> AlphaLineageStatus:
    lineage_by_horizon: dict[int, str] = {}
    blocked_horizons: list[int] = []
    detail_by_horizon: dict[int, dict[str, object]] = {}
    effective_horizons = sorted({int(value) for value in horizons})
    for horizon in effective_horizons:
        active = load_active_alpha_model(connection, as_of_date=as_of_date, horizon=int(horizon))
        if active is None:
            blocked_horizons.append(int(horizon))
            detail_by_horizon[int(horizon)] = {
                "reason": "missing_active_alpha_model",
                "blocked": True,
            }
            continue
        active_alpha_model_id = str(active["active_alpha_model_id"])
        effective_from_date = active["effective_from_date"]
        trading_day_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM dim_trading_calendar
            WHERE trading_date BETWEEN ? AND ?
              AND is_trading_day = TRUE
            """,
            [effective_from_date, as_of_date],
        ).fetchone()
        trading_days_since = int(trading_day_count[0]) if trading_day_count else 0
        blocked = trading_days_since <= int(max(1, stabilization_trading_days))
        lineage_by_horizon[int(horizon)] = active_alpha_model_id
        if blocked:
            blocked_horizons.append(int(horizon))
        detail_by_horizon[int(horizon)] = {
            "active_alpha_model_id": active_alpha_model_id,
            "model_spec_id": active.get("model_spec_id"),
            "effective_from_date": (
                effective_from_date.isoformat() if effective_from_date is not None else None
            ),
            "trading_days_since_activation": trading_days_since,
            "blocked": blocked,
            "reason": (
                "alpha_stabilization_window"
                if blocked
                else "eligible"
            ),
        }
    return AlphaLineageStatus(
        lineage_by_horizon=lineage_by_horizon,
        blocked_horizons=sorted(set(blocked_horizons)),
        detail_by_horizon=detail_by_horizon,
    )


def write_promotion_decision_artifact(
    settings: Settings,
    *,
    dataset: str,
    run_id: str,
    filename: str,
    payload: dict[str, object],
) -> str:
    artifact_dir = settings.paths.artifacts_dir / dataset / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / filename
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(artifact_path)
