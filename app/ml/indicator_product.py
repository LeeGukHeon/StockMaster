from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from app.features.feature_store import _load_feature_symbol_frame
from app.evaluation.alpha_shadow import (
    materialize_alpha_shadow_evaluation_summary,
    materialize_alpha_shadow_selection_gap_scorecard,
)
from app.ml.active import freeze_alpha_active_model
from app.ml.constants import (
    D5_PRIMARY_COMPARATOR_PAIRS,
    D5_PRIMARY_FOCUS_MODEL_SPEC_ID,
    MODEL_DOMAIN,
    MODEL_SPEC_ID,
    MODEL_VERSION,
    AlphaModelSpec,
    get_alpha_model_spec,
)
from app.ml.dataset import _resolve_candidate_dates
from app.ml.inference import materialize_alpha_predictions_v1
from app.ml.registry import (
    load_active_alpha_model,
    load_latest_training_run,
    upsert_alpha_model_specs,
)
from app.ml.shadow import materialize_alpha_shadow_candidates
from app.ml.training import (
    build_alpha_model_spec_registry_frame,
    prune_training_result_artifacts,
    train_alpha_model_v1,
    train_alpha_candidate_models,
)
from app.ml.validation import validate_alpha_model_v1
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection


@dataclass(slots=True)
class AlphaIndicatorProductBundleResult:
    train_end_date: date
    as_of_date: date
    model_spec_ids: list[str]
    freeze_horizons: list[int]
    shadow_backfill_enabled: bool
    shadow_backfill_selection_date_count: int
    shadow_backfill_processed_selection_date_count: int
    shadow_backfill_skipped_selection_date_count: int
    frozen_model_spec_ids: list[str]
    missing_training_model_spec_ids: list[str]
    blocked_freeze_model_spec_ids: list[str]
    freeze_block_reasons: dict[str, list[str]]
    active_model_spec_ids_by_horizon: dict[int, str]
    registry_row_count: int
    training_run_count: int
    freeze_row_count: int
    prediction_row_count: int
    ranking_row_count: int
    shadow_prediction_row_count: int
    shadow_ranking_row_count: int
    shadow_evaluation_summary_row_count: int
    gap_scorecard_row_count: int
    validation_check_count: int
    notes: str


@dataclass(slots=True)
class AlphaIndicatorProductSpecReadiness:
    model_spec_id: str
    supported_horizons: list[int]
    runnable_horizons: list[int]
    blocked_horizons: list[int]
    blockers: list[str]


@dataclass(slots=True)
class AlphaIndicatorProductReadinessResult:
    train_end_date: date
    latest_market_date: date | None
    missing_snapshot_dates: list[date]
    label_max_as_of_by_horizon: dict[int, date | None]
    available_label_rows_by_horizon: dict[int, int]
    specs: list[AlphaIndicatorProductSpecReadiness]
    notes: str


@dataclass(slots=True)
class AlphaIndicatorShadowBackfillResult:
    selection_date_count: int
    processed_selection_date_count: int
    skipped_selection_date_count: int
    training_run_count: int
    prediction_row_count: int
    ranking_row_count: int
    notes: str


D1_FREEZE_MIN_MATURED_SELECTION_DATES = 20
D1_FREEZE_MIN_TOP5_BEAT_BASELINE = 0.002
LEGACY_H1_COMPARATOR_MODEL_SPEC_ID = "alpha_topbucket_h1_rolling_120_v1"


def _resolve_model_specs(model_spec_ids: list[str]) -> tuple[AlphaModelSpec, ...]:
    resolved: list[AlphaModelSpec] = []
    for model_spec_id in model_spec_ids:
        resolved.append(get_alpha_model_spec(str(model_spec_id)))
    return tuple(resolved)


def _detect_missing_snapshot_dates(
    settings: Settings,
    *,
    train_end_date: date,
    horizons: list[int],
    limit_symbols: int | None,
    market: str,
) -> list[date]:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        candidate_dates = _resolve_candidate_dates(
            connection,
            train_end_date=train_end_date,
            horizons=horizons,
            symbols=None,
            limit_symbols=limit_symbols,
            market=market,
        )
        missing_dates: list[date] = []
        for candidate_date in candidate_dates:
            try:
                frame = _load_feature_symbol_frame(
                    connection,
                    as_of_date=candidate_date,
                    symbols=None,
                    limit_symbols=limit_symbols,
                    market=market,
                )
            except RuntimeError:
                missing_dates.append(candidate_date)
                continue
            if frame.empty:
                missing_dates.append(candidate_date)
        return missing_dates


def _resolve_bundle_focus_model_spec_id(model_spec_ids: list[str]) -> str | None:
    if D5_PRIMARY_FOCUS_MODEL_SPEC_ID in {str(value) for value in model_spec_ids}:
        return D5_PRIMARY_FOCUS_MODEL_SPEC_ID
    return None


def _ensure_training_run_for_spec(
    settings: Settings,
    *,
    train_end_date: date,
    horizon: int,
    model_spec_id: str,
    min_train_days: int,
    validation_days: int,
    limit_symbols: int | None,
    market: str,
) -> int:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        training_run = load_latest_training_run(
            connection,
            horizon=int(horizon),
            model_version=MODEL_VERSION,
            train_end_date=train_end_date,
            model_domain=MODEL_DOMAIN,
            model_spec_id=model_spec_id,
        )
    if training_run is not None and training_run.get("artifact_uri"):
        return 0
    if model_spec_id == MODEL_SPEC_ID:
        result = train_alpha_model_v1(
            settings,
            train_end_date=train_end_date,
            horizons=[int(horizon)],
            min_train_days=min_train_days,
            validation_days=validation_days,
            limit_symbols=limit_symbols,
            market=market,
        )
        return int(result.training_run_count)
    result = train_alpha_candidate_models(
        settings,
        train_end_date=train_end_date,
        horizons=[int(horizon)],
        min_train_days=min_train_days,
        validation_days=validation_days,
        limit_symbols=limit_symbols,
        market=market,
        model_specs=(get_alpha_model_spec(model_spec_id),),
    )
    return int(result.training_run_count)


def _ensure_d5_primary_reference_training_runs(
    settings: Settings,
    *,
    train_end_date: date,
    min_train_days: int,
    validation_days: int,
    limit_symbols: int | None,
    market: str,
) -> int:
    additional_training_runs = 0
    for horizon, model_spec_id in D5_PRIMARY_COMPARATOR_PAIRS:
        additional_training_runs += _ensure_training_run_for_spec(
            settings,
            train_end_date=train_end_date,
            horizon=int(horizon),
            model_spec_id=str(model_spec_id),
            min_train_days=min_train_days,
            validation_days=validation_days,
            limit_symbols=limit_symbols,
            market=market,
        )
    return additional_training_runs


def _load_trading_dates(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
) -> list[date]:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        rows = connection.execute(
            """
            SELECT trading_date
            FROM dim_trading_calendar
            WHERE is_trading_day = TRUE
              AND trading_date BETWEEN ? AND ?
            ORDER BY trading_date
            """,
            [start_selection_date, end_selection_date],
        ).fetchall()
    return [row[0] for row in rows]


def _shadow_date_complete_for_specs(
    settings: Settings,
    *,
    selection_date: date,
    horizons: list[int],
    model_specs: tuple[AlphaModelSpec, ...],
) -> bool:
    expected_pairs = {
        (str(spec.model_spec_id), int(horizon))
        for spec in model_specs
        for horizon in horizons
        if int(horizon) in set(spec.allowed_horizons or horizons)
    }
    if not expected_pairs:
        return True
    spec_ids = sorted({spec_id for spec_id, _ in expected_pairs})
    horizon_values = sorted({int(horizon) for _, horizon in expected_pairs})
    spec_placeholders = ",".join("?" for _ in spec_ids)
    horizon_placeholders = ",".join("?" for _ in horizon_values)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        rows = connection.execute(
            f"""
            SELECT model_spec_id, horizon
            FROM fact_alpha_shadow_ranking
            WHERE selection_date = ?
              AND model_spec_id IN ({spec_placeholders})
              AND horizon IN ({horizon_placeholders})
            GROUP BY model_spec_id, horizon
            """,
            [selection_date, *spec_ids, *horizon_values],
        ).fetchall()
    found_pairs = {(str(model_spec_id), int(horizon)) for model_spec_id, horizon in rows}
    return expected_pairs.issubset(found_pairs)


def _backfill_indicator_shadow_history(
    settings: Settings,
    *,
    start_selection_date: date,
    end_selection_date: date,
    horizons: list[int],
    model_specs: tuple[AlphaModelSpec, ...],
    min_train_days: int,
    validation_days: int,
    limit_symbols: int | None,
    market: str,
    skip_completed_dates: bool,
    keep_training_artifacts: bool,
) -> AlphaIndicatorShadowBackfillResult:
    trading_dates = _load_trading_dates(
        settings,
        start_selection_date=start_selection_date,
        end_selection_date=end_selection_date,
    )
    processed_dates = 0
    skipped_dates = 0
    training_run_count = 0
    prediction_row_count = 0
    ranking_row_count = 0
    for selection_date in trading_dates:
        if skip_completed_dates and _shadow_date_complete_for_specs(
            settings,
            selection_date=selection_date,
            horizons=horizons,
            model_specs=model_specs,
        ):
            skipped_dates += 1
            continue
        training_result = train_alpha_candidate_models(
            settings,
            train_end_date=selection_date,
            horizons=horizons,
            min_train_days=min_train_days,
            validation_days=validation_days,
            limit_symbols=limit_symbols,
            market=market,
            model_specs=model_specs,
        )
        shadow_result = materialize_alpha_shadow_candidates(
            settings,
            as_of_date=selection_date,
            horizons=horizons,
            limit_symbols=limit_symbols,
            market=market,
        )
        if not keep_training_artifacts:
            prune_training_result_artifacts(settings, training_result=training_result)
        processed_dates += 1
        training_run_count += int(training_result.training_run_count)
        prediction_row_count += int(shadow_result.prediction_row_count)
        ranking_row_count += int(shadow_result.ranking_row_count)
    notes = (
        "Alpha indicator shadow history backfill completed. "
        f"selection_dates={len(trading_dates)} "
        f"processed_dates={processed_dates} "
        f"skipped_dates={skipped_dates} "
        f"training_runs={training_run_count}"
    )
    return AlphaIndicatorShadowBackfillResult(
        selection_date_count=len(trading_dates),
        processed_selection_date_count=processed_dates,
        skipped_selection_date_count=skipped_dates,
        training_run_count=training_run_count,
        prediction_row_count=prediction_row_count,
        ranking_row_count=ranking_row_count,
        notes=notes,
    )


def _analysis_model_spec_ids_for_bundle(
    *,
    model_spec_ids: list[str],
    horizons: list[int],
    focus_model_spec_id: str | None = None,
) -> list[str]:
    analysis_model_spec_ids = list(model_spec_ids)
    if focus_model_spec_id == D5_PRIMARY_FOCUS_MODEL_SPEC_ID:
        analysis_model_spec_ids.extend(
            model_spec_id for _, model_spec_id in D5_PRIMARY_COMPARATOR_PAIRS
        )
    elif 1 in {int(horizon) for horizon in horizons}:
        analysis_model_spec_ids.append(MODEL_SPEC_ID)
        analysis_model_spec_ids.append(LEGACY_H1_COMPARATOR_MODEL_SPEC_ID)
    return sorted(dict.fromkeys(str(value) for value in analysis_model_spec_ids))


def _required_analysis_pairs_for_bundle(
    *,
    model_spec_ids: list[str],
    horizons: list[int],
    focus_model_spec_id: str | None = None,
) -> list[tuple[int, str]]:
    if focus_model_spec_id == D5_PRIMARY_FOCUS_MODEL_SPEC_ID:
        return [
            (5, D5_PRIMARY_FOCUS_MODEL_SPEC_ID),
            *D5_PRIMARY_COMPARATOR_PAIRS,
        ]
    required_pairs: list[tuple[int, str]] = []
    for model_spec_id in model_spec_ids:
        try:
            spec = get_alpha_model_spec(str(model_spec_id))
            allowed_horizons = set(spec.allowed_horizons or horizons)
        except KeyError:
            allowed_horizons = set(int(horizon) for horizon in horizons)
        for horizon in horizons:
            if int(horizon) in allowed_horizons:
                required_pairs.append((int(horizon), str(model_spec_id)))
    if 1 in {int(horizon) for horizon in horizons}:
        required_pairs.extend(
            [
                (1, MODEL_SPEC_ID),
                (1, LEGACY_H1_COMPARATOR_MODEL_SPEC_ID),
            ]
        )
    return sorted(dict.fromkeys(required_pairs))


def _require_analysis_evidence_rows(
    connection,
    *,
    summary_date: date,
    required_pairs: list[tuple[int, str]],
) -> None:
    if not required_pairs:
        return
    required_pair_set = {(str(model_spec_id), int(horizon)) for horizon, model_spec_id in required_pairs}
    model_spec_values = sorted({model_spec_id for model_spec_id, _ in required_pair_set})
    model_spec_placeholders = ",".join("?" for _ in model_spec_values)
    horizon_values = sorted({horizon for _, horizon in required_pair_set})
    horizon_placeholders = ",".join("?" for _ in horizon_values)
    rows = connection.execute(
        f"""
        SELECT DISTINCT model_spec_id, horizon
        FROM fact_alpha_shadow_evaluation_summary
        WHERE summary_date = ?
          AND model_spec_id IN ({model_spec_placeholders})
          AND horizon IN ({horizon_placeholders})
          AND segment_value = 'top5'
        """,
        [summary_date, *model_spec_values, *horizon_values],
    ).fetchall()
    found_pairs = {(str(model_spec_id), int(horizon)) for model_spec_id, horizon in rows}
    missing_pairs = sorted(required_pair_set - found_pairs)
    if missing_pairs:
        missing_text = ", ".join(f"{model_spec_id}:h{horizon}" for model_spec_id, horizon in missing_pairs)
        raise RuntimeError(
            "Missing same-window analysis evidence for required comparator set: "
            f"{missing_text}"
        )


def inspect_alpha_indicator_product_readiness(
    settings: Settings,
    *,
    train_end_date: date,
    horizons: list[int],
    model_spec_ids: list[str],
    limit_symbols: int | None = None,
    market: str = "ALL",
) -> AlphaIndicatorProductReadinessResult:
    model_specs = _resolve_model_specs(model_spec_ids)
    missing_snapshot_dates = _detect_missing_snapshot_dates(
        settings,
        train_end_date=train_end_date,
        horizons=horizons,
        limit_symbols=limit_symbols,
        market=market,
    )
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        latest_market_row = connection.execute(
            "SELECT MAX(trading_date) FROM fact_daily_ohlcv"
        ).fetchone()
        latest_market_date = (
            None
            if latest_market_row is None or latest_market_row[0] is None
            else latest_market_row[0]
        )
        label_max_as_of_by_horizon: dict[int, date | None] = {}
        available_label_rows_by_horizon: dict[int, int] = {}
        for horizon in sorted({int(value) for value in horizons}):
            label_row = connection.execute(
                """
                SELECT MAX(as_of_date), COUNT(*)
                FROM fact_forward_return_label
                WHERE horizon = ?
                  AND label_available_flag
                  AND as_of_date <= ?
                """,
                [int(horizon), train_end_date],
            ).fetchone()
            label_max_as_of_by_horizon[int(horizon)] = (
                None
                if label_row is None or label_row[0] is None
                else label_row[0]
            )
            available_label_rows_by_horizon[int(horizon)] = int(
                0 if label_row is None or label_row[1] is None else label_row[1]
            )
    spec_rows: list[AlphaIndicatorProductSpecReadiness] = []
    for model_spec in model_specs:
        supported_horizons = [
            int(horizon)
            for horizon in horizons
            if int(horizon) in set(model_spec.allowed_horizons or horizons)
        ]
        runnable_horizons: list[int] = []
        blocked_horizons: list[int] = []
        blockers: list[str] = []
        for horizon in supported_horizons:
            if missing_snapshot_dates:
                blocked_horizons.append(int(horizon))
                blockers.append(
                    "missing_snapshot_dates=" + ",".join(
                        sorted(value.isoformat() for value in missing_snapshot_dates)
                    )
                )
                continue
            available_rows = int(available_label_rows_by_horizon.get(int(horizon), 0))
            if available_rows <= 0:
                blocked_horizons.append(int(horizon))
                blockers.append(f"h{int(horizon)}:no_matured_labels")
                continue
            runnable_horizons.append(int(horizon))
        spec_rows.append(
            AlphaIndicatorProductSpecReadiness(
                model_spec_id=model_spec.model_spec_id,
                supported_horizons=supported_horizons,
                runnable_horizons=runnable_horizons,
                blocked_horizons=blocked_horizons,
                blockers=sorted(dict.fromkeys(blockers)),
            )
        )

    notes = (
        "Alpha indicator readiness inspected. "
        f"train_end_date={train_end_date.isoformat()} "
        f"latest_market_date={latest_market_date.isoformat() if latest_market_date else '-'} "
        f"missing_snapshot_dates={','.join(value.isoformat() for value in missing_snapshot_dates) or '-'}"
    )
    return AlphaIndicatorProductReadinessResult(
        train_end_date=train_end_date,
        latest_market_date=latest_market_date,
        missing_snapshot_dates=missing_snapshot_dates,
        label_max_as_of_by_horizon=label_max_as_of_by_horizon,
        available_label_rows_by_horizon=available_label_rows_by_horizon,
        specs=spec_rows,
        notes=notes,
    )


def _load_latest_gap_row(
    connection,
    *,
    as_of_date: date,
    window_name: str,
    horizon: int,
    model_spec_id: str,
) -> dict[str, object] | None:
    row = connection.execute(
        """
        SELECT
            summary_date,
            matured_selection_date_count,
            required_selection_date_count,
            insufficient_history_flag,
            selected_top5_mean_realized_excess_return,
            report_candidates_mean_realized_excess_return,
            drag_vs_raw_top5
        FROM fact_alpha_shadow_selection_gap_scorecard
        WHERE summary_date <= ?
          AND window_name = ?
          AND horizon = ?
          AND model_spec_id = ?
          AND segment_name = 'top5'
        ORDER BY summary_date DESC
        LIMIT 1
        """,
        [as_of_date, window_name, int(horizon), model_spec_id],
    ).fetchone()
    if row is None:
        return None
    keys = (
        "summary_date",
        "matured_selection_date_count",
        "required_selection_date_count",
        "insufficient_history_flag",
        "selected_top5_mean_realized_excess_return",
        "report_candidates_mean_realized_excess_return",
        "drag_vs_raw_top5",
    )
    return dict(zip(keys, row, strict=True))


def _load_latest_evaluation_summary_row(
    connection,
    *,
    as_of_date: date,
    window_type: str,
    horizon: int,
    model_spec_id: str,
    segment_value: str,
) -> dict[str, object] | None:
    row = connection.execute(
        """
        SELECT
            summary_date,
            count_evaluated,
            mean_realized_excess_return,
            mean_point_loss,
            rank_ic
        FROM fact_alpha_shadow_evaluation_summary
        WHERE summary_date <= ?
          AND window_type = ?
          AND horizon = ?
          AND model_spec_id = ?
          AND segment_value = ?
        ORDER BY summary_date DESC
        LIMIT 1
        """,
        [as_of_date, window_type, int(horizon), model_spec_id, segment_value],
    ).fetchone()
    if row is None:
        return None
    keys = (
        "summary_date",
        "count_evaluated",
        "mean_realized_excess_return",
        "mean_point_loss",
        "rank_ic",
    )
    return dict(zip(keys, row, strict=True))


def _evaluate_d1_freeze_gate(
    connection,
    *,
    as_of_date: date,
    candidate_model_spec_id: str,
    baseline_model_spec_id: str,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    gap_row = _load_latest_gap_row(
        connection,
        as_of_date=as_of_date,
        window_name="rolling_20",
        horizon=1,
        model_spec_id=candidate_model_spec_id,
    )
    if gap_row is None:
        reasons.append("missing_gap_scorecard")
        return False, reasons
    matured_count = int(gap_row.get("matured_selection_date_count") or 0)
    if bool(gap_row.get("insufficient_history_flag")) or matured_count < D1_FREEZE_MIN_MATURED_SELECTION_DATES:
        required_count = int(gap_row.get("required_selection_date_count") or D1_FREEZE_MIN_MATURED_SELECTION_DATES)
        reasons.append(
            f"insufficient_matured_shadow_dates={matured_count}/{required_count}"
        )
        return False, reasons

    candidate_top5 = _load_latest_evaluation_summary_row(
        connection,
        as_of_date=as_of_date,
        window_type="rolling_20",
        horizon=1,
        model_spec_id=candidate_model_spec_id,
        segment_value="top5",
    )
    baseline_top5 = _load_latest_evaluation_summary_row(
        connection,
        as_of_date=as_of_date,
        window_type="rolling_20",
        horizon=1,
        model_spec_id=baseline_model_spec_id,
        segment_value="top5",
    )
    candidate_report = _load_latest_evaluation_summary_row(
        connection,
        as_of_date=as_of_date,
        window_type="rolling_20",
        horizon=1,
        model_spec_id=candidate_model_spec_id,
        segment_value="report_candidates",
    )
    baseline_report = _load_latest_evaluation_summary_row(
        connection,
        as_of_date=as_of_date,
        window_type="rolling_20",
        horizon=1,
        model_spec_id=baseline_model_spec_id,
        segment_value="report_candidates",
    )

    if candidate_top5 is None or baseline_top5 is None:
        reasons.append("missing_top5_comparator_rows")
        return False, reasons
    candidate_top5_return = candidate_top5.get("mean_realized_excess_return")
    baseline_top5_return = baseline_top5.get("mean_realized_excess_return")
    if candidate_top5_return is None:
        reasons.append("missing_candidate_top5_return")
    elif float(candidate_top5_return) <= 0.0:
        reasons.append("candidate_top5_not_positive")
    if (
        candidate_top5_return is not None
        and baseline_top5_return is not None
        and float(candidate_top5_return) - float(baseline_top5_return) < D1_FREEZE_MIN_TOP5_BEAT_BASELINE
    ):
        reasons.append("candidate_top5_does_not_beat_baseline")

    if candidate_report is None or baseline_report is None:
        reasons.append("missing_report_candidates_comparator_rows")
    else:
        candidate_report_return = candidate_report.get("mean_realized_excess_return")
        baseline_report_return = baseline_report.get("mean_realized_excess_return")
        if (
            candidate_report_return is not None
            and baseline_report_return is not None
            and float(candidate_report_return) < float(baseline_report_return)
        ):
            reasons.append("candidate_report_candidates_below_baseline")

    return len(reasons) == 0, reasons


def run_alpha_indicator_product_bundle(
    settings: Settings,
    *,
    train_end_date: date,
    as_of_date: date,
    shadow_start_selection_date: date,
    shadow_end_selection_date: date,
    horizons: list[int],
    model_spec_ids: list[str],
    min_train_days: int,
    validation_days: int,
    limit_symbols: int | None = None,
    market: str = "ALL",
    rolling_windows: list[int] | None = None,
    freeze_horizons: list[int] | None = None,
    backfill_shadow_history: bool = False,
    skip_completed_shadow_dates: bool = True,
    keep_shadow_training_artifacts: bool = False,
) -> AlphaIndicatorProductBundleResult:
    model_specs = _resolve_model_specs(model_spec_ids)
    focus_model_spec_id = _resolve_bundle_focus_model_spec_id(model_spec_ids)
    analysis_model_spec_ids = _analysis_model_spec_ids_for_bundle(
        model_spec_ids=model_spec_ids,
        horizons=horizons,
        focus_model_spec_id=focus_model_spec_id,
    )
    required_analysis_pairs = _required_analysis_pairs_for_bundle(
        model_spec_ids=model_spec_ids,
        horizons=horizons,
        focus_model_spec_id=focus_model_spec_id,
    )
    target_freeze_horizons = list(
        dict.fromkeys(
            int(value)
            for value in (
                freeze_horizons
                or ([1] if 1 in {int(horizon) for horizon in horizons} else horizons)
            )
        )
    )
    registry_frame = build_alpha_model_spec_registry_frame()
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        upsert_alpha_model_specs(connection, registry_frame)

    try:
        training_result = train_alpha_candidate_models(
            settings,
            train_end_date=train_end_date,
            horizons=horizons,
            min_train_days=min_train_days,
            validation_days=validation_days,
            limit_symbols=limit_symbols,
            market=market,
            model_specs=model_specs,
        )
    except RuntimeError as exc:
        message = str(exc)
        if "same-day OHLCV is missing" in message:
            missing_dates = _detect_missing_snapshot_dates(
                settings,
                train_end_date=train_end_date,
                horizons=horizons,
                limit_symbols=limit_symbols,
                market=market,
            )
            missing_text = ", ".join(sorted(value.isoformat() for value in missing_dates))
            raise RuntimeError(
                f"{message} Missing feature-snapshot source dates for bundle: {missing_text or 'unknown'}."
            ) from exc
        raise

    additional_reference_training_runs = 0
    if focus_model_spec_id == D5_PRIMARY_FOCUS_MODEL_SPEC_ID:
        additional_reference_training_runs += _ensure_d5_primary_reference_training_runs(
            settings,
            train_end_date=train_end_date,
            min_train_days=min_train_days,
            validation_days=validation_days,
            limit_symbols=limit_symbols,
            market=market,
        )

    if backfill_shadow_history:
        shadow_backfill_result = _backfill_indicator_shadow_history(
            settings,
            start_selection_date=shadow_start_selection_date,
            end_selection_date=shadow_end_selection_date,
            horizons=horizons,
            model_specs=model_specs,
            min_train_days=min_train_days,
            validation_days=validation_days,
            limit_symbols=limit_symbols,
            market=market,
            skip_completed_dates=skip_completed_shadow_dates,
            keep_training_artifacts=keep_shadow_training_artifacts,
        )
        shadow_prediction_row_count = int(shadow_backfill_result.prediction_row_count)
        shadow_ranking_row_count = int(shadow_backfill_result.ranking_row_count)
    else:
        shadow_result = materialize_alpha_shadow_candidates(
            settings,
            as_of_date=as_of_date,
            horizons=horizons,
            limit_symbols=limit_symbols,
            market=market,
        )
        shadow_backfill_result = AlphaIndicatorShadowBackfillResult(
            selection_date_count=1,
            processed_selection_date_count=1,
            skipped_selection_date_count=0,
            training_run_count=0,
            prediction_row_count=int(shadow_result.prediction_row_count),
            ranking_row_count=int(shadow_result.ranking_row_count),
            notes="Alpha indicator shadow history backfill disabled; materialized only the current as_of_date shadow candidates.",
        )
        shadow_prediction_row_count = int(shadow_result.prediction_row_count)
        shadow_ranking_row_count = int(shadow_result.ranking_row_count)
    summary_result = materialize_alpha_shadow_evaluation_summary(
        settings,
        start_selection_date=shadow_start_selection_date,
        end_selection_date=shadow_end_selection_date,
        horizons=horizons,
        model_spec_ids=analysis_model_spec_ids,
        rolling_windows=rolling_windows or [20, 60],
    )
    gap_result = materialize_alpha_shadow_selection_gap_scorecard(
        settings,
        start_selection_date=shadow_start_selection_date,
        end_selection_date=shadow_end_selection_date,
        horizons=horizons,
        model_spec_ids=analysis_model_spec_ids,
        rolling_windows=rolling_windows or [20, 60],
    )
    if backfill_shadow_history:
        with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
            bootstrap_core_tables(connection)
            _require_analysis_evidence_rows(
                connection,
                summary_date=shadow_end_selection_date,
                required_pairs=required_analysis_pairs,
            )

    if any(model_spec.model_spec_id == "alpha_lead_d1_v1" for model_spec in model_specs):
        additional_reference_training_runs += _ensure_training_run_for_spec(
            settings,
            train_end_date=train_end_date,
            horizon=1,
            model_spec_id=MODEL_SPEC_ID,
            min_train_days=min_train_days,
            validation_days=validation_days,
            limit_symbols=limit_symbols,
            market=market,
        )

    frozen_model_spec_ids: list[str] = []
    missing_training_model_spec_ids: list[str] = []
    blocked_freeze_model_spec_ids: list[str] = []
    freeze_block_reasons: dict[str, list[str]] = {}
    freeze_row_count = 0
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        pre_run_active_model_spec_ids_by_horizon = {
            int(horizon): str(active_model["model_spec_id"])
            for horizon in horizons
            if (
                (active_model := load_active_alpha_model(
                    connection,
                    as_of_date=as_of_date,
                    horizon=int(horizon),
                ))
                is not None
            )
        }
        for model_spec in model_specs:
            supported_horizons = [
                int(horizon)
                for horizon in horizons
                if int(horizon) in set(model_spec.allowed_horizons or horizons)
            ]
            latest_training_runs = {
                int(horizon): load_latest_training_run(
                    connection,
                    horizon=int(horizon),
                    model_version=MODEL_VERSION,
                    train_end_date=train_end_date,
                    model_domain=MODEL_DOMAIN,
                    model_spec_id=model_spec.model_spec_id,
                )
                for horizon in supported_horizons
            }
            runnable_horizons = [
                horizon
                for horizon, training_run in latest_training_runs.items()
                if training_run is not None and training_run.get("artifact_uri")
            ]
            freezeable_horizons = [
                horizon for horizon in runnable_horizons if int(horizon) in target_freeze_horizons
            ]
            if not runnable_horizons:
                missing_training_model_spec_ids.append(model_spec.model_spec_id)
                continue

            if model_spec.model_spec_id == D5_PRIMARY_FOCUS_MODEL_SPEC_ID and freezeable_horizons:
                blocked_freeze_model_spec_ids.append(model_spec.model_spec_id)
                freeze_block_reasons[model_spec.model_spec_id] = [
                    "challenger_only_no_auto_freeze"
                ]
                continue

            if model_spec.model_spec_id == "alpha_lead_d1_v1" and 1 in freezeable_horizons:
                d1_gate_ok, d1_gate_reasons = _evaluate_d1_freeze_gate(
                    connection,
                    as_of_date=as_of_date,
                    candidate_model_spec_id=model_spec.model_spec_id,
                    baseline_model_spec_id=MODEL_SPEC_ID,
                )
                if not d1_gate_ok:
                    blocked_freeze_model_spec_ids.append(model_spec.model_spec_id)
                    freeze_block_reasons[model_spec.model_spec_id] = d1_gate_reasons
                    active_h1 = load_active_alpha_model(
                        connection,
                        as_of_date=as_of_date,
                        horizon=1,
                    )
                    if active_h1 is None or str(active_h1.get("model_spec_id")) != MODEL_SPEC_ID:
                        freeze_result = freeze_alpha_active_model(
                            settings,
                            as_of_date=as_of_date,
                            source="indicator_product_bundle_guardrail",
                            note=(
                                "indicator product bundle retained baseline for D+1 because "
                                + ", ".join(d1_gate_reasons)
                            ),
                            horizons=[1],
                            model_spec_id=MODEL_SPEC_ID,
                            train_end_date=train_end_date,
                            promotion_type="MANUAL_FREEZE",
                        )
                        freeze_row_count += int(freeze_result.row_count)
                    continue

            if not freezeable_horizons:
                continue

            freeze_result = freeze_alpha_active_model(
                settings,
                as_of_date=as_of_date,
                source="indicator_product_bundle",
                note=f"indicator product bundle freeze for {model_spec.model_spec_id}",
                horizons=freezeable_horizons,
                model_spec_id=model_spec.model_spec_id,
                train_end_date=train_end_date,
                promotion_type="MANUAL_FREEZE",
            )
            if int(freeze_result.row_count) > 0:
                frozen_model_spec_ids.append(model_spec.model_spec_id)
            freeze_row_count += int(freeze_result.row_count)

        active_model_spec_ids_by_horizon = {
            int(horizon): str(active_model["model_spec_id"])
            for horizon in horizons
            if (
                (active_model := load_active_alpha_model(
                    connection,
                    as_of_date=as_of_date,
                    horizon=int(horizon),
                ))
                is not None
            )
        }
        preserved_horizons = [
            int(horizon) for horizon in horizons if int(horizon) not in target_freeze_horizons
        ]
        for horizon in preserved_horizons:
            before = pre_run_active_model_spec_ids_by_horizon.get(int(horizon))
            after = active_model_spec_ids_by_horizon.get(int(horizon))
            if before != after:
                raise RuntimeError(
                    "Active model drift detected on preserved horizon "
                    f"{int(horizon)}: before={before or '-'} after={after or '-'}"
                )

    prediction_result = materialize_alpha_predictions_v1(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
        limit_symbols=limit_symbols,
        market=market,
    )
    ranking_result = materialize_selection_engine_v2(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
        limit_symbols=limit_symbols,
    )
    validation_result = validate_alpha_model_v1(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
        focus_model_spec_id=focus_model_spec_id,
    )

    notes = (
        "Alpha indicator product bundle completed. "
        f"specs={','.join(model_spec_ids)} "
        f"freeze_horizons={','.join(str(value) for value in target_freeze_horizons) or '-'} "
        f"shadow_backfill={'on' if backfill_shadow_history else 'off'} "
        f"shadow_backfill_dates={shadow_backfill_result.selection_date_count} "
        f"shadow_backfill_processed={shadow_backfill_result.processed_selection_date_count} "
        f"shadow_backfill_skipped={shadow_backfill_result.skipped_selection_date_count} "
        f"frozen={','.join(frozen_model_spec_ids) or '-'} "
        f"missing_training={','.join(missing_training_model_spec_ids) or '-'} "
        f"blocked_freeze={','.join(blocked_freeze_model_spec_ids) or '-'} "
        f"train_end_date={train_end_date.isoformat()} "
        f"as_of_date={as_of_date.isoformat()} "
        f"training_runs={int(training_result.training_run_count) + int(additional_reference_training_runs)} "
        f"freeze_rows={freeze_row_count} "
        f"prediction_rows={prediction_result.row_count} "
        f"ranking_rows={ranking_result.row_count} "
        f"shadow_predictions={shadow_prediction_row_count} "
        f"summary_rows={summary_result.row_count} "
        f"gap_rows={gap_result.row_count} "
        f"validation_checks={validation_result.row_count}"
    )
    return AlphaIndicatorProductBundleResult(
        train_end_date=train_end_date,
        as_of_date=as_of_date,
        model_spec_ids=list(model_spec_ids),
        freeze_horizons=target_freeze_horizons,
        shadow_backfill_enabled=bool(backfill_shadow_history),
        shadow_backfill_selection_date_count=int(shadow_backfill_result.selection_date_count),
        shadow_backfill_processed_selection_date_count=int(
            shadow_backfill_result.processed_selection_date_count
        ),
        shadow_backfill_skipped_selection_date_count=int(
            shadow_backfill_result.skipped_selection_date_count
        ),
        frozen_model_spec_ids=frozen_model_spec_ids,
        missing_training_model_spec_ids=missing_training_model_spec_ids,
        blocked_freeze_model_spec_ids=blocked_freeze_model_spec_ids,
        freeze_block_reasons=freeze_block_reasons,
        active_model_spec_ids_by_horizon=active_model_spec_ids_by_horizon,
        registry_row_count=len(registry_frame),
        training_run_count=int(training_result.training_run_count) + int(additional_reference_training_runs),
        freeze_row_count=freeze_row_count,
        prediction_row_count=int(prediction_result.row_count),
        ranking_row_count=int(ranking_result.row_count),
        shadow_prediction_row_count=shadow_prediction_row_count,
        shadow_ranking_row_count=shadow_ranking_row_count,
        shadow_evaluation_summary_row_count=int(summary_result.row_count),
        gap_scorecard_row_count=int(gap_result.row_count),
        validation_check_count=int(validation_result.row_count),
        notes=notes,
    )
