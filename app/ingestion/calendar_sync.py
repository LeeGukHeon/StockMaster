from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import holidays
import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class TradingCalendarSyncResult:
    run_id: str
    row_count: int
    trading_day_count: int
    min_date: date
    max_date: date
    artifact_paths: list[str]


def load_trading_calendar_overrides(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame(columns=["date", "is_trading_day", "holiday_name", "note"])
    frame = pd.read_csv(path)
    if frame.empty:
        return frame
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame["is_trading_day"] = (
        frame["is_trading_day"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})
    )
    frame = frame.drop_duplicates(subset=["date"], keep="last")
    return frame


def build_trading_calendar_frame(
    *,
    start_date: date,
    end_date: date,
    overrides: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    days = pd.date_range(start_date, end_date, freq="D")
    holiday_map = holidays.country_holidays("KR", years=sorted({day.year for day in days}))

    rows: list[dict[str, object]] = []
    for day in days:
        current_date = day.date()
        holiday_name = holiday_map.get(current_date)
        is_weekend = day.weekday() >= 5
        is_public_holiday = holiday_name is not None
        is_trading_day = not is_weekend and not is_public_holiday
        rows.append(
            {
                "trading_date": current_date,
                "is_trading_day": is_trading_day,
                "market_session_type": "regular" if is_trading_day else "closed",
                "weekday": day.weekday(),
                "is_weekend": is_weekend,
                "is_public_holiday": is_public_holiday,
                "holiday_name": holiday_name,
                "source": "weekend+kr_holidays",
                "source_confidence": "high",
                "is_override": False,
            }
        )

    frame = pd.DataFrame(rows)
    if overrides is not None and not overrides.empty:
        indexed = overrides.set_index("date")
        for row_idx, trading_date in frame["trading_date"].items():
            if trading_date not in indexed.index:
                continue
            override = indexed.loc[trading_date]
            override_holiday_name = override.get("holiday_name")
            if pd.isna(override_holiday_name):
                override_holiday_name = None
            frame.at[row_idx, "is_trading_day"] = bool(override["is_trading_day"])
            frame.at[row_idx, "holiday_name"] = override_holiday_name
            frame.at[row_idx, "is_public_holiday"] = override_holiday_name is not None
            frame.at[row_idx, "market_session_type"] = (
                "regular" if bool(override["is_trading_day"]) else "closed"
            )
            frame.at[row_idx, "source"] = "manual_override"
            frame.at[row_idx, "source_confidence"] = "override"
            frame.at[row_idx, "is_override"] = True

    trading_days = frame.loc[frame["is_trading_day"], "trading_date"].tolist()
    prev_map: dict[date, date | None] = {}
    next_map: dict[date, date | None] = {}
    for index, trading_day in enumerate(trading_days):
        prev_map[trading_day] = trading_days[index - 1] if index > 0 else None
        next_map[trading_day] = trading_days[index + 1] if index + 1 < len(trading_days) else None

    prev_values: list[date | None] = []
    next_values: list[date | None] = []
    last_seen: date | None = None
    next_cursor = 0
    for trading_date, is_trading_day in zip(
        frame["trading_date"], frame["is_trading_day"], strict=True
    ):
        if is_trading_day:
            last_seen = trading_date
            while next_cursor < len(trading_days) and trading_days[next_cursor] <= trading_date:
                next_cursor += 1
            prev_values.append(prev_map.get(trading_date))
            next_values.append(next_map.get(trading_date))
        else:
            prev_values.append(last_seen)
            next_values.append(
                trading_days[next_cursor] if next_cursor < len(trading_days) else None
            )

    frame["prev_trading_date"] = prev_values
    frame["next_trading_date"] = next_values
    frame["updated_at"] = pd.Timestamp(now_local("Asia/Seoul"))
    return frame


def _replace_calendar_table(connection, frame: pd.DataFrame) -> None:
    ordered = frame[
        [
            "trading_date",
            "is_trading_day",
            "market_session_type",
            "weekday",
            "is_weekend",
            "is_public_holiday",
            "holiday_name",
            "source",
            "source_confidence",
            "is_override",
            "prev_trading_date",
            "next_trading_date",
            "updated_at",
        ]
    ].copy()
    connection.execute("DELETE FROM dim_trading_calendar")
    connection.register("calendar_stage", ordered)
    connection.execute(
        """
        INSERT INTO dim_trading_calendar (
            trading_date,
            is_trading_day,
            market_session_type,
            weekday,
            is_weekend,
            is_public_holiday,
            holiday_name,
            source,
            source_confidence,
            is_override,
            prev_trading_date,
            next_trading_date,
            updated_at
        )
        SELECT
            trading_date,
            is_trading_day,
            market_session_type,
            weekday,
            is_weekend,
            is_public_holiday,
            holiday_name,
            source,
            source_confidence,
            is_override,
            prev_trading_date,
            next_trading_date,
            updated_at
        FROM calendar_stage
        """
    )
    connection.unregister("calendar_stage")


def sync_trading_calendar(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    overrides_path: Path | None = None,
) -> TradingCalendarSyncResult:
    ensure_storage_layout(settings)
    actual_overrides_path = (
        overrides_path or settings.paths.project_root / "config" / "trading_calendar_overrides.csv"
    )
    overrides = load_trading_calendar_overrides(actual_overrides_path)

    with activate_run_context("sync_trading_calendar", as_of_date=end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[str(actual_overrides_path)]
                if actual_overrides_path.exists()
                else [],
                notes=(
                    f"Sync trading calendar from {start_date.isoformat()} to {end_date.isoformat()}"
                ),
            )
            try:
                frame = build_trading_calendar_frame(
                    start_date=start_date,
                    end_date=end_date,
                    overrides=overrides,
                )
                snapshot_path = (
                    settings.paths.artifacts_dir
                    / "trading_calendar"
                    / f"calendar_{start_date.isoformat()}_{end_date.isoformat()}.parquet"
                )
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                frame.to_parquet(snapshot_path, index=False)

                existing = connection.execute(
                    "SELECT * FROM dim_trading_calendar ORDER BY trading_date"
                ).fetchdf()
                if not existing.empty:
                    existing["trading_date"] = pd.to_datetime(existing["trading_date"]).dt.date
                    existing = existing.loc[~existing["trading_date"].isin(frame["trading_date"])]
                    combined = pd.concat([existing, frame], ignore_index=True)
                    combined = combined.sort_values("trading_date").reset_index(drop=True)
                    combined = build_trading_calendar_frame(
                        start_date=combined["trading_date"].min(),
                        end_date=combined["trading_date"].max(),
                        overrides=load_trading_calendar_overrides(actual_overrides_path),
                    )
                else:
                    combined = frame

                _replace_calendar_table(connection, combined)

                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[str(snapshot_path)],
                    notes=(
                        f"Trading calendar sync completed. rows={len(combined)}, "
                        f"trading_days={int(combined['is_trading_day'].sum())}"
                    ),
                )
                return TradingCalendarSyncResult(
                    run_id=run_context.run_id,
                    row_count=len(combined),
                    trading_day_count=int(combined["is_trading_day"].sum()),
                    min_date=combined["trading_date"].min(),
                    max_date=combined["trading_date"].max(),
                    artifact_paths=[str(snapshot_path)],
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Trading calendar sync failed.",
                    error_message=str(exc),
                )
                raise
