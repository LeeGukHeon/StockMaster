from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.common.time import now_local
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ml.promotion import load_alpha_promotion_summary
from app.ops.common import JobStatus, OpsJobResult
from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.calibration import PREDICTION_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.selection.sector_outlook import sector_outlook_frame
from app.settings import Settings

UI_READ_MODEL_VERSION = "ui_read_model_v1"


@dataclass(frozen=True, slots=True)
class UIReadModelDataset:
    name: str
    frame: pd.DataFrame


def _preferred_ranking_versions() -> list[str]:
    return [
        SELECTION_ENGINE_V2_VERSION,
        SELECTION_ENGINE_VERSION,
        EXPLANATORY_RANKING_VERSION,
    ]


def _prediction_version_for_ranking(ranking_version: str | None) -> str | None:
    if ranking_version == SELECTION_ENGINE_V2_VERSION:
        return ALPHA_PREDICTION_VERSION
    if ranking_version == SELECTION_ENGINE_VERSION:
        return PREDICTION_VERSION
    return None


def _resolve_latest_ranking_version(connection, ranking_version: str | None = None) -> str | None:
    if ranking_version:
        return ranking_version
    preferred_versions = _preferred_ranking_versions()
    order_clause = " ".join(
        f"WHEN ranking_version = '{value}' THEN {index}"
        for index, value in enumerate(preferred_versions)
    )
    row = connection.execute(
        f"""
        SELECT ranking_version
        FROM fact_ranking
        ORDER BY
            CASE {order_clause} ELSE {len(preferred_versions)} END,
            as_of_date DESC,
            created_at DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else str(row[0])


def _resolve_latest_ranking_date(connection, ranking_version: str | None = None):
    effective_version = _resolve_latest_ranking_version(connection, ranking_version)
    if effective_version is None:
        return None
    row = connection.execute(
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = ?
        """,
        [effective_version],
    ).fetchone()
    return None if row is None else row[0]


def _latest_portfolio_as_of_date(connection):
    row = connection.execute(
        """
        SELECT MAX(as_of_date)
        FROM fact_portfolio_target_book
        """
    ).fetchone()
    return None if row is None else row[0]


def _latest_portfolio_session_date(connection, *, as_of_date):
    if as_of_date is None:
        return None
    row = connection.execute(
        """
        SELECT MAX(session_date)
        FROM fact_portfolio_rebalance_plan
        WHERE as_of_date = ?
        """,
        [as_of_date],
    ).fetchone()
    return None if row is None else row[0]


def ui_read_model_root(settings: Settings) -> Path:
    return settings.paths.artifacts_dir / "ui_read_model"


def ui_read_model_latest_root(settings: Settings) -> Path:
    return ui_read_model_root(settings) / "latest"


def ui_read_model_dataset_path(settings: Settings, dataset_name: str) -> Path:
    return ui_read_model_latest_root(settings) / f"{dataset_name}.parquet"


def ui_read_model_manifest_path(settings: Settings) -> Path:
    return ui_read_model_latest_root(settings) / "manifest.json"


def load_ui_read_model_manifest(settings: Settings) -> dict[str, Any]:
    path = ui_read_model_manifest_path(settings)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_ui_read_model_frame(settings: Settings, dataset_name: str) -> pd.DataFrame:
    path = ui_read_model_dataset_path(settings, dataset_name)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = frame.copy()
    for column in normalized.columns:
        if normalized[column].dtype != "object":
            continue
        normalized[column] = normalized[column].map(
            lambda value: json.dumps(value, ensure_ascii=False)
            if isinstance(value, (dict, list))
            else value
        )
    normalized.to_parquet(path, index=False)


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _copy_snapshot_to_latest(snapshot_root: Path, latest_root: Path) -> None:
    latest_root.mkdir(parents=True, exist_ok=True)
    for existing in latest_root.glob("*"):
        if existing.is_file():
            existing.unlink()
    for item in snapshot_root.iterdir():
        if item.is_file():
            shutil.copy2(item, latest_root / item.name)


def _market_pulse_frame(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            regime.as_of_date,
            regime.regime_state,
            regime.regime_score,
            regime.breadth_up_ratio,
            regime.market_realized_vol_20d,
            flow.row_count AS investor_flow_rows,
            flow.foreign_positive_ratio,
            flow.institution_positive_ratio,
            selection.selection_rows,
            prediction.prediction_rows
        FROM (
            SELECT *
            FROM vw_market_regime_latest
            WHERE market_scope = 'KR_ALL'
        ) AS regime
        LEFT JOIN (
            SELECT
                trading_date,
                COUNT(*) AS row_count,
                AVG(CASE WHEN foreign_net_value > 0 THEN 1.0 ELSE 0.0 END) AS foreign_positive_ratio,
                AVG(CASE WHEN institution_net_value > 0 THEN 1.0 ELSE 0.0 END) AS institution_positive_ratio
            FROM fact_investor_flow
            WHERE trading_date = (SELECT MAX(trading_date) FROM fact_investor_flow)
            GROUP BY trading_date
        ) AS flow
          ON regime.as_of_date = flow.trading_date
        LEFT JOIN (
            SELECT as_of_date, COUNT(*) AS selection_rows
            FROM fact_ranking
            WHERE ranking_version = ?
            GROUP BY as_of_date
            QUALIFY ROW_NUMBER() OVER (ORDER BY as_of_date DESC) = 1
        ) AS selection
          ON regime.as_of_date = selection.as_of_date
        LEFT JOIN (
            SELECT as_of_date, COUNT(*) AS prediction_rows
            FROM fact_prediction
            WHERE prediction_version = ?
            GROUP BY as_of_date
            QUALIFY ROW_NUMBER() OVER (ORDER BY as_of_date DESC) = 1
        ) AS prediction
          ON regime.as_of_date = prediction.as_of_date
        """,
        [SELECTION_ENGINE_VERSION, PREDICTION_VERSION],
    ).fetchdf()


def _leaderboard_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date,
    ranking_version: str,
) -> pd.DataFrame:
    prediction_version = _prediction_version_for_ranking(ranking_version)
    if prediction_version is None:
        return pd.DataFrame()
    frame = connection.execute(
        """
        SELECT
            ranking.as_of_date,
            ranking.as_of_date AS selection_date,
            (
                SELECT MIN(calendar.trading_date)
                FROM dim_trading_calendar AS calendar
                WHERE calendar.trading_date > ranking.as_of_date
                  AND calendar.is_trading_day
            ) AS next_entry_trade_date,
            ranking.symbol,
            symbol.company_name,
            symbol.market,
            symbol.sector,
            symbol.industry,
            ranking.horizon,
            ranking.final_selection_value,
            ranking.final_selection_rank_pct,
            ranking.grade,
            ranking.regime_state,
            ranking.ranking_version,
            ranking.top_reason_tags_json,
            ranking.risk_flags_json,
            ranking.explanatory_score_json,
            prediction.expected_excess_return,
            prediction.lower_band,
            prediction.median_band,
            prediction.upper_band,
            prediction.model_spec_id,
            prediction.active_alpha_model_id,
            prediction.uncertainty_score,
            prediction.disagreement_score,
            prediction.fallback_flag,
            prediction.fallback_reason,
            daily.close AS selection_close_price,
            outcome.outcome_status,
            outcome.realized_excess_return,
            outcome.band_status
        FROM fact_ranking AS ranking
        JOIN dim_symbol AS symbol
          ON ranking.symbol = symbol.symbol
        LEFT JOIN fact_prediction AS prediction
          ON ranking.as_of_date = prediction.as_of_date
         AND ranking.symbol = prediction.symbol
         AND ranking.horizon = prediction.horizon
          AND prediction.prediction_version = ?
          AND prediction.ranking_version = ranking.ranking_version
        LEFT JOIN fact_daily_ohlcv AS daily
          ON ranking.symbol = daily.symbol
         AND ranking.as_of_date = daily.trading_date
        LEFT JOIN fact_selection_outcome AS outcome
          ON ranking.as_of_date = outcome.selection_date
         AND ranking.symbol = outcome.symbol
         AND ranking.horizon = outcome.horizon
         AND ranking.ranking_version = outcome.ranking_version
        WHERE ranking.as_of_date = ?
          AND ranking.ranking_version = ?
        ORDER BY ranking.horizon, ranking.final_selection_value DESC, ranking.symbol
        """,
        [prediction_version, as_of_date, ranking_version],
    ).fetchdf()
    if frame.empty:
        return frame
    base_price = pd.to_numeric(frame.get("selection_close_price"), errors="coerce")
    expected = pd.to_numeric(frame.get("expected_excess_return"), errors="coerce")
    upper = pd.to_numeric(frame.get("upper_band"), errors="coerce")
    lower = pd.to_numeric(frame.get("lower_band"), errors="coerce")
    frame["flat_target_price"] = base_price * (1.0 + expected)
    frame["flat_upper_target_price"] = base_price * (1.0 + upper)
    frame["flat_stop_price"] = base_price * (1.0 + lower)
    frame["reasons"] = frame["top_reason_tags_json"].fillna("[]")
    frame["risks"] = frame["risk_flags_json"].fillna("[]")
    return frame


def _leaderboard_grade_counts_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date,
    ranking_version: str,
) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT horizon, grade, COUNT(*) AS row_count
        FROM fact_ranking
        WHERE as_of_date = ?
          AND ranking_version = ?
        GROUP BY horizon, grade
        ORDER BY horizon, grade
        """,
        [as_of_date, ranking_version],
    ).fetchdf()


def _latest_portfolio_policy_registry_frame(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            active_portfolio_policy_id,
            portfolio_policy_id,
            portfolio_policy_version,
            display_name,
            source_type,
            promotion_type,
            effective_from_date,
            effective_to_date,
            active_flag,
            rollback_of_active_portfolio_policy_id,
            note,
            created_at
        FROM fact_portfolio_policy_registry
        ORDER BY effective_from_date DESC, created_at DESC
        """
    ).fetchdf()


def _latest_portfolio_candidate_frame(connection: duckdb.DuckDBPyConnection, *, as_of_date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            as_of_date,
            session_date,
            execution_mode,
            symbol,
            company_name,
            market,
            sector,
            candidate_rank,
            candidate_state,
            final_selection_value,
            effective_alpha_long,
            risk_scaled_conviction,
            timing_action,
            timing_gate_status,
            current_holding_flag
        FROM fact_portfolio_candidate
        WHERE as_of_date = ?
        ORDER BY execution_mode, candidate_rank, symbol
        """,
        [as_of_date],
    ).fetchdf()


def _latest_portfolio_target_book_frame(connection: duckdb.DuckDBPyConnection, *, as_of_date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            as_of_date,
            session_date,
            execution_mode,
            symbol,
            company_name,
            market,
            sector,
            candidate_state,
            target_rank,
            target_weight,
            target_notional,
            target_shares,
            target_price,
            plan_horizon,
            entry_trade_date,
            exit_trade_date,
            entry_basis,
            exit_basis,
            model_spec_id,
            active_alpha_model_id,
            action_plan_label,
            target_return,
            stretch_target_return,
            stop_return,
            action_target_price,
            action_stretch_price,
            action_stop_price,
            current_shares,
            current_weight,
            score_value,
            gate_status,
            included_flag,
            waitlist_flag,
            waitlist_rank,
            blocked_flag,
            CASE
                WHEN blocked_flag THEN constraint_flags_json
                ELSE NULL
            END AS blocked_reason
        FROM fact_portfolio_target_book
        WHERE as_of_date = ?
        ORDER BY execution_mode, target_rank, symbol
        """,
        [as_of_date],
    ).fetchdf()


def _latest_portfolio_rebalance_plan_frame(connection: duckdb.DuckDBPyConnection, *, as_of_date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            as_of_date,
            session_date,
            execution_mode,
            symbol,
            company_name,
            rebalance_action,
            action_sequence,
            gate_status,
            current_shares,
            target_shares,
            delta_shares,
            reference_price,
            notional_delta,
            cash_delta,
            blocked_reason
        FROM fact_portfolio_rebalance_plan
        WHERE as_of_date = ?
        ORDER BY execution_mode, action_sequence, symbol
        """,
        [as_of_date],
    ).fetchdf()


def _latest_portfolio_nav_frame(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            snapshot_date,
            execution_mode,
            portfolio_policy_id,
            portfolio_policy_version,
            nav_value,
            cumulative_return,
            drawdown,
            turnover_ratio,
            cash_weight,
            holding_count,
            max_single_weight,
            top3_weight
        FROM fact_portfolio_nav_snapshot
        ORDER BY snapshot_date DESC, execution_mode
        LIMIT 40
        """
    ).fetchdf()


def _latest_portfolio_constraint_frame(connection: duckdb.DuckDBPyConnection, *, as_of_date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            as_of_date,
            execution_mode,
            symbol,
            constraint_type,
            event_code,
            requested_value,
            applied_value,
            limit_value,
            message
        FROM fact_portfolio_constraint_event
        WHERE as_of_date = ?
        ORDER BY execution_mode, symbol, constraint_type
        """,
        [as_of_date],
    ).fetchdf()


def _latest_market_news_frame(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT signal_date, published_at, title, publisher, link
        FROM fact_news_item
        WHERE signal_date = (SELECT MAX(signal_date) FROM fact_news_item)
          AND COALESCE(is_market_wide, FALSE)
        ORDER BY published_at DESC
        LIMIT 50
        """
    ).fetchdf()


def _latest_regime_frame(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            as_of_date,
            market_scope,
            regime_state,
            regime_score,
            breadth_up_ratio,
            median_symbol_return_1d,
            market_realized_vol_20d,
            turnover_burst_z
        FROM vw_market_regime_latest
        ORDER BY market_scope
        """
    ).fetchdf()


def _latest_flow_summary_frame(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(
        """
        WITH latest_date AS (
            SELECT MAX(trading_date) AS trading_date
            FROM fact_investor_flow
        )
        SELECT
            flow.trading_date,
            COUNT(*) AS row_count,
            AVG(CASE WHEN foreign_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END) AS foreign_value_coverage,
            AVG(CASE WHEN institution_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END) AS institution_value_coverage,
            AVG(CASE WHEN individual_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END) AS individual_value_coverage
        FROM fact_investor_flow AS flow
        JOIN latest_date
          ON flow.trading_date = latest_date.trading_date
        GROUP BY flow.trading_date
        """
    ).fetchdf()


def _latest_market_mood_summary(regime_frame: pd.DataFrame) -> dict[str, str]:
    if regime_frame.empty:
        return {
            "mode": "missing",
            "headline": "시장 분위기 데이터 없음",
            "label": "-",
            "detail": "일봉 기준 시장 구간 데이터가 아직 없습니다.",
        }
    if "market_scope" in regime_frame.columns and regime_frame["market_scope"].astype(str).eq("KR_ALL").any():
        row = regime_frame.loc[regime_frame["market_scope"].astype(str) == "KR_ALL"].iloc[0]
    else:
        row = regime_frame.iloc[0]
    as_of_date = row.get("as_of_date")
    headline = str(row.get("regime_state") or "-")
    return {
        "mode": "daily",
        "headline": headline,
        "label": str(as_of_date or "-"),
        "detail": "장중 컨텍스트 대신 마지막 일봉 시장 구간 스냅샷을 보여줍니다.",
    }


def _latest_recommendation_timeline_payload(
    *,
    ranking_as_of_date,
    portfolio_as_of_date,
    portfolio_session_date,
) -> dict[str, Any]:
    return {
        "selection_as_of_date": None if ranking_as_of_date is None else str(ranking_as_of_date),
        "portfolio_as_of_date": None if portfolio_as_of_date is None else str(portfolio_as_of_date),
        "portfolio_session_date": None if portfolio_session_date is None else str(portfolio_session_date),
        "intraday_session_date": None,
    }


def materialize_ui_read_model_snapshot(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date | None,
    job_run_id: str,
) -> OpsJobResult:
    ranking_version = _resolve_latest_ranking_version(connection)
    ranking_as_of_date = _resolve_latest_ranking_date(connection, ranking_version)
    portfolio_as_of_date = _latest_portfolio_as_of_date(connection)
    portfolio_session_date = _latest_portfolio_session_date(connection, as_of_date=portfolio_as_of_date)
    regime_frame = _latest_regime_frame(connection)
    market_news_frame = _latest_market_news_frame(connection)
    datasets: list[UIReadModelDataset] = [
        UIReadModelDataset("market_pulse", _market_pulse_frame(connection)),
        UIReadModelDataset("market_regime", regime_frame),
        UIReadModelDataset("market_news", market_news_frame),
        UIReadModelDataset("flow_summary", _latest_flow_summary_frame(connection)),
        UIReadModelDataset(
            "alpha_promotion_summary",
            load_alpha_promotion_summary(connection),
        ),
        UIReadModelDataset(
            "portfolio_policy_registry",
            _latest_portfolio_policy_registry_frame(connection),
        ),
        UIReadModelDataset(
            "portfolio_nav",
            _latest_portfolio_nav_frame(connection),
        ),
    ]

    if ranking_version is not None and ranking_as_of_date is not None:
        leaderboard = _leaderboard_frame(
            connection,
            as_of_date=ranking_as_of_date,
            ranking_version=ranking_version,
        )
        grade_counts = _leaderboard_grade_counts_frame(
            connection,
            as_of_date=ranking_as_of_date,
            ranking_version=ranking_version,
        )
        datasets.append(UIReadModelDataset("leaderboard", leaderboard))
        datasets.append(UIReadModelDataset("leaderboard_grade_counts", grade_counts))
        sector_frames: list[pd.DataFrame] = []
        prediction_version = _prediction_version_for_ranking(ranking_version)
        if prediction_version is not None:
            for horizon in (1, 5):
                sector_frame = sector_outlook_frame(
                    connection,
                    as_of_date=ranking_as_of_date,
                    ranking_version=ranking_version,
                    prediction_version=prediction_version,
                    horizon=horizon,
                    candidate_limit=40,
                    limit=10,
                )
                if not sector_frame.empty:
                    sector_frames.append(sector_frame)
            datasets.append(
                UIReadModelDataset(
                    "sector_outlook",
                    pd.concat(sector_frames, ignore_index=True) if sector_frames else pd.DataFrame(),
                )
            )

    if portfolio_as_of_date is not None:
        datasets.extend(
            [
                UIReadModelDataset(
                    "portfolio_candidate",
                    _latest_portfolio_candidate_frame(connection, as_of_date=portfolio_as_of_date),
                ),
                UIReadModelDataset(
                    "portfolio_target_book",
                    _latest_portfolio_target_book_frame(connection, as_of_date=portfolio_as_of_date),
                ),
                UIReadModelDataset(
                    "portfolio_rebalance",
                    _latest_portfolio_rebalance_plan_frame(connection, as_of_date=portfolio_as_of_date),
                ),
                UIReadModelDataset(
                    "portfolio_constraints",
                    _latest_portfolio_constraint_frame(connection, as_of_date=portfolio_as_of_date),
                ),
            ]
        )
        target_book = next(
            (dataset.frame for dataset in datasets if dataset.name == "portfolio_target_book"),
            pd.DataFrame(),
        )
        waitlist_frame = (
            target_book.loc[
                (target_book.get("waitlist_flag", pd.Series(dtype=bool)).fillna(False))
                | (target_book.get("blocked_flag", pd.Series(dtype=bool)).fillna(False))
            ].copy()
            if not target_book.empty
            else pd.DataFrame()
        )
        datasets.append(UIReadModelDataset("portfolio_waitlist", waitlist_frame))

    target_as_of_date = as_of_date or ranking_as_of_date or portfolio_as_of_date or now_local(settings.app.timezone).date()
    snapshot_root = ui_read_model_root(settings) / f"as_of_date={target_as_of_date.isoformat()}" / job_run_id
    latest_root = ui_read_model_latest_root(settings)
    snapshot_root.mkdir(parents=True, exist_ok=True)

    artifact_paths: list[str] = []
    for dataset in datasets:
        path = snapshot_root / f"{dataset.name}.parquet"
        _write_frame(path, dataset.frame)
        artifact_paths.append(str(path))

    manifest = {
        "read_model_version": UI_READ_MODEL_VERSION,
        "built_at": now_local(settings.app.timezone).isoformat(),
        "job_run_id": job_run_id,
        "as_of_date": target_as_of_date.isoformat(),
        "ranking_version": ranking_version,
        "ranking_as_of_date": None if ranking_as_of_date is None else str(ranking_as_of_date),
        "portfolio_as_of_date": None if portfolio_as_of_date is None else str(portfolio_as_of_date),
        "portfolio_session_date": None if portfolio_session_date is None else str(portfolio_session_date),
        "market_mood": _latest_market_mood_summary(regime_frame),
        "recommendation_timeline": _latest_recommendation_timeline_payload(
            ranking_as_of_date=ranking_as_of_date,
            portfolio_as_of_date=portfolio_as_of_date,
            portfolio_session_date=portfolio_session_date,
        ),
        "datasets": {dataset.name: int(len(dataset.frame)) for dataset in datasets},
    }
    manifest_path = snapshot_root / "manifest.json"
    _write_manifest(manifest_path, manifest)
    artifact_paths.append(str(manifest_path))
    _copy_snapshot_to_latest(snapshot_root, latest_root)
    _write_manifest(ui_read_model_manifest_path(settings), manifest)

    return OpsJobResult(
        run_id=job_run_id,
        job_name="materialize_ui_read_model_snapshot",
        status=JobStatus.SUCCESS,
        notes=(
            f"UI read model snapshot refreshed for as_of_date={target_as_of_date.isoformat()} "
            f"datasets={len(datasets)}"
        ),
        artifact_paths=artifact_paths,
        as_of_date=target_as_of_date,
        row_count=sum(len(dataset.frame) for dataset in datasets),
    )
