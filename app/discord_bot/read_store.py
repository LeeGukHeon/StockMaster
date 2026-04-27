from __future__ import annotations

import json
from datetime import date

import duckdb
import pandas as pd

from app.common.time import now_local
from app.discord_bot.data_views import (
    latest_evaluation_summary_frame,
    latest_intraday_policy_evaluation_frame,
    leaderboard_frame,
    resolve_latest_ranking_date,
    resolve_latest_ranking_version,
    stock_workbench_live_snapshot_frame,
    stock_workbench_summary_frame,
)
from app.ml.promotion import load_alpha_promotion_summary
from app.ops.common import JobStatus, OpsJobResult
from app.recommendation.buyability import (
    BUYABILITY_MIN_FINAL_SELECTION_VALUE,
    buyability_priority_score,
    d5_buyability_policy_bucket,
    has_buyability_blocker,
)
from app.recommendation.judgement import (
    ScoreBandEvidence,
    classify_recommendation,
    load_score_band_evidence,
)
from app.reports.discord_eod import (
    ALPHA_DECISION_LABELS,
    ALPHA_DECISION_REASON_LABELS,
    MODEL_SPEC_LABELS,
    REASON_LABELS,
    RISK_LABELS,
)
from app.settings import Settings
from app.storage.metadata_postgres import (
    ensure_postgres_metadata_store,
    execute_postgres_sql,
    executemany_postgres_sql,
    fetchdf_postgres_sql,
    metadata_postgres_enabled,
)

BOT_SNAPSHOT_TABLE = "fact_discord_bot_snapshot"
BOT_PICK_LIMIT = 20
BOT_D5_CORE_PICK_LIMIT = 5
BOT_D5_MAX_PICKS_PER_SECTOR = 2
BOT_WEEKLY_LIMIT = 4
BOT_SNAPSHOT_TYPES = ("status", "next_picks", "weekly_report", "stock_summary")


def _safe_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def _format_percent(value: object, *, decimals: int = 1, signed: bool = False) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    number = float(value)
    format_spec = f"+.{decimals}%" if signed else f".{decimals}%"
    return format(number, format_spec)


def _format_number(value: object, *, decimals: int = 1) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    return f"{float(value):,.{decimals}f}"


def _parse_json_list(value: object, mapping: dict[str, str]) -> list[str]:
    if value in (None, "", "[]"):
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [mapping.get(str(item), str(item)) for item in parsed if str(item).strip()]


def _parse_raw_json_list(value: object) -> list[str]:
    if value in (None, "", "[]"):
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip()]


def _model_label(value: object) -> str:
    text = _safe_text(value)
    return MODEL_SPEC_LABELS.get(text, text)


def _decision_label(value: object) -> str:
    text = _safe_text(value)
    return ALPHA_DECISION_LABELS.get(text, text)


def _decision_reason(value: object) -> str:
    text = _safe_text(value)
    return ALPHA_DECISION_REASON_LABELS.get(text, text)


def _hold_basis_label(horizon: int) -> str:
    if int(horizon) == 1:
        return "하루 보유 기준"
    if int(horizon) == 5:
        return "5거래일 보유 기준"
    return f"{int(horizon)}거래일 보유 기준"


def _snapshot_row(
    *,
    snapshot_type: str,
    snapshot_key: str,
    built_at: str,
    as_of_date: str | None,
    title: str,
    summary: str,
    sort_order: int | None = None,
    horizon: int | None = None,
    symbol: str | None = None,
    company_name: str | None = None,
    market: str | None = None,
    subtitle: str | None = None,
    payload: dict[str, object] | None = None,
    source_run_id: str | None = None,
) -> dict[str, object]:
    return {
        "snapshot_type": snapshot_type,
        "snapshot_key": snapshot_key,
        "as_of_date": as_of_date,
        "horizon": horizon,
        "sort_order": sort_order,
        "symbol": symbol,
        "company_name": company_name,
        "market": market,
        "title": title,
        "subtitle": subtitle,
        "summary": summary,
        "payload_json": json.dumps(payload or {}, ensure_ascii=False),
        "snapshot_ts": built_at,
        "source_run_id": source_run_id,
        "created_at": built_at,
    }


def _build_status_rows(
    *,
    built_at: str,
    as_of_date: str | None,
    ranking_as_of_date: str | None,
    ranking_version: str | None,
    source_run_id: str,
) -> list[dict[str, object]]:
    payload = {
        "as_of_date": as_of_date,
        "ranking_as_of_date": ranking_as_of_date,
        "ranking_version": ranking_version,
        "built_at": built_at,
    }
    summary = (
        f"기준일 {_safe_text(as_of_date)} · 추천 기준일 {_safe_text(ranking_as_of_date)} · "
        f"마지막 반영 {_safe_text(built_at)}"
    )
    return [
        _snapshot_row(
            snapshot_type="status",
            snapshot_key="latest",
            built_at=built_at,
            as_of_date=as_of_date,
            title="봇 응답 기준 상태",
            summary=summary,
            payload=payload,
            source_run_id=source_run_id,
        )
    ]


def _build_pick_rows(
    frame: pd.DataFrame,
    *,
    horizon: int,
    built_at: str,
    as_of_date: str | None,
    source_run_id: str,
    score_evidence: dict[str, ScoreBandEvidence] | None = None,
) -> list[dict[str, object]]:
    if frame.empty:
        return []
    working = frame.loc[pd.to_numeric(frame["horizon"], errors="coerce") == int(horizon)].copy()
    if working.empty:
        return []
    is_d5_candidate_surface = int(horizon) == 5
    if is_d5_candidate_surface:
        working["raw_risks_list"] = working["risks"].apply(_parse_raw_json_list)
        expected = pd.to_numeric(working["expected_excess_return"], errors="coerce")
        eligible = (
            working["eligible_flag"].astype(bool)
            if "eligible_flag" in working.columns
            else pd.Series(True, index=working.index)
        )
        final_score = pd.to_numeric(working["final_selection_value"], errors="coerce")
        working["d5_selection_rank"] = final_score.rank(
            ascending=False,
            method="first",
        )
        working = working.loc[
            eligible
            & (expected > 0.0)
            & (final_score >= BUYABILITY_MIN_FINAL_SELECTION_VALUE)
            & ~working["raw_risks_list"].apply(has_buyability_blocker)
        ].copy()
        if working.empty:
            return []
        working["buyability_priority_score"] = working.apply(
            lambda row: buyability_priority_score(
                expected_excess_return=row.get("expected_excess_return"),
                uncertainty_score=row.get("uncertainty_score"),
                disagreement_score=row.get("disagreement_score"),
            ),
            axis=1,
        )
        working["d5_policy_bucket"] = working.apply(
            lambda row: d5_buyability_policy_bucket(
                selection_rank=row.get("d5_selection_rank"),
                expected_excess_return=row.get("expected_excess_return"),
                final_selection_value=row.get("final_selection_value"),
                risk_flags=row.get("raw_risks_list"),
                fallback_flag=row.get("fallback_flag"),
                uncertainty_score=row.get("uncertainty_score"),
                disagreement_score=row.get("disagreement_score"),
            ),
            axis=1,
        )
        working = working.loc[working["d5_policy_bucket"].notna()].sort_values(
            ["d5_policy_bucket", "d5_selection_rank", "symbol"],
            ascending=[True, True, True],
        )
        working = _limit_d5_sector_concentration(working, limit=BOT_D5_CORE_PICK_LIMIT)
    else:
        working = working.head(BOT_PICK_LIMIT)
    rows: list[dict[str, object]] = []
    for rank, row in enumerate(working.itertuples(index=False), start=1):
        raw_risks = getattr(row, "raw_risks_list", None)
        if raw_risks is None:
            raw_risks = _parse_raw_json_list(getattr(row, "risks", "[]"))
        reasons = _parse_json_list(getattr(row, "reasons", "[]"), REASON_LABELS)[:2]
        risks = _parse_json_list(getattr(row, "risks", "[]"), RISK_LABELS)[:2]
        judgement = classify_recommendation(
            final_selection_value=getattr(row, "final_selection_value", None),
            expected_excess_return=getattr(row, "expected_excess_return", None),
            risk_flags=raw_risks,
            evidence_by_band=score_evidence,
            candidate_selected=is_d5_candidate_surface,
        )
        display_label = judgement.label
        display_summary = judgement.summary
        summary_parts = [
            display_label,
            f"점수 {_format_number(getattr(row, 'final_selection_value', None))}",
            f"등급 {_safe_text(getattr(row, 'grade', None))}",
            f"기대 {_format_percent(getattr(row, 'expected_excess_return', None), signed=True)}",
            f"진입 {_safe_text(getattr(row, 'next_entry_trade_date', None))}",
        ]
        if is_d5_candidate_surface:
            priority_text = _format_number(
                getattr(row, "buyability_priority_score", None),
                decimals=2,
            )
            summary_parts.append(
                f"우선순위 {priority_text}"
            )
        if reasons:
            summary_parts.append(f"핵심 근거 {', '.join(reasons)}")
        if risks:
            summary_parts.append(f"유의할 리스크 {', '.join(risks)}")
        else:
            summary_parts.append("차단 리스크 없음")
        payload = {
            "selection_date": _safe_text(getattr(row, "selection_date", None)),
            "next_entry_trade_date": _safe_text(getattr(row, "next_entry_trade_date", None)),
            "grade": _safe_text(getattr(row, "grade", None)),
            "expected_excess_return": getattr(row, "expected_excess_return", None),
            "final_selection_value": getattr(row, "final_selection_value", None),
            "buyability_priority_score": getattr(row, "buyability_priority_score", None),
            "industry": _safe_text(getattr(row, "industry", None)),
            "sector": _safe_text(getattr(row, "sector", None)),
            "model_spec_id": _model_label(getattr(row, "model_spec_id", None)),
            "judgement_label": display_label,
            "judgement_summary": display_summary,
            "score_band": judgement.score_band,
            "reasons": reasons,
            "risks": risks,
        }
        rows.append(
            _snapshot_row(
                snapshot_type="next_picks",
                snapshot_key=f"h{int(horizon)}:{_safe_text(getattr(row, 'symbol', None))}",
                built_at=built_at,
                as_of_date=as_of_date,
                horizon=int(horizon),
                sort_order=rank,
                symbol=_safe_text(getattr(row, "symbol", None)),
                company_name=_safe_text(getattr(row, "company_name", None)),
                market=_safe_text(getattr(row, "market", None)),
                title=(
                    f"{_safe_text(getattr(row, 'symbol', None))} "
                    f"{_safe_text(getattr(row, 'company_name', None))}"
                ),
                subtitle=_hold_basis_label(int(horizon)),
                summary=" · ".join(summary_parts),
                payload=payload,
                source_run_id=source_run_id,
            )
        )
    return rows


def _limit_d5_sector_concentration(frame: pd.DataFrame, *, limit: int) -> pd.DataFrame:
    if frame.empty:
        return frame
    selected_indices: list[object] = []
    sector_counts: dict[str, int] = {}
    for index, row in frame.iterrows():
        sector = str(row.get("sector") or row.get("industry") or "-")
        if sector_counts.get(sector, 0) >= BOT_D5_MAX_PICKS_PER_SECTOR:
            continue
        selected_indices.append(index)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(selected_indices) >= int(limit):
            break
    if len(selected_indices) < int(limit):
        for index in frame.index:
            if index in selected_indices:
                continue
            selected_indices.append(index)
            if len(selected_indices) >= int(limit):
                break
    return frame.loc[selected_indices].head(int(limit)).copy()


def _build_weekly_rows(
    *,
    alpha_promotion: pd.DataFrame,
    evaluation_summary: pd.DataFrame,
    policy_eval: pd.DataFrame,
    built_at: str,
    as_of_date: str | None,
    source_run_id: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    order = 0
    for item in alpha_promotion.head(BOT_WEEKLY_LIMIT).itertuples(index=False):
        order += 1
        horizon = int(getattr(item, "horizon", 0) or 0)
        summary = " · ".join(
            [
                _decision_label(getattr(item, "decision_label", None)),
                f"현재 모델 {_model_label(getattr(item, 'active_model_label', None))}",
                f"판단 이유 {_decision_reason(getattr(item, 'decision_reason_label', None))}",
            ]
        )
        rows.append(
            _snapshot_row(
                snapshot_type="weekly_report",
                snapshot_key=f"alpha_h{horizon}",
                built_at=built_at,
                as_of_date=as_of_date,
                horizon=horizon,
                sort_order=order,
                title=f"{_hold_basis_label(horizon)} 모델 점검",
                subtitle="알파 비교",
                summary=summary,
                payload={
                    "sample_count": int(getattr(item, "sample_count", 0) or 0),
                    "active_top10_mean_excess_return": getattr(
                        item, "active_top10_mean_excess_return", None
                    ),
                    "comparison_top10_mean_excess_return": getattr(
                        item, "comparison_top10_mean_excess_return", None
                    ),
                },
                source_run_id=source_run_id,
            )
        )
    for item in evaluation_summary.head(BOT_WEEKLY_LIMIT).itertuples(index=False):
        order += 1
        horizon = int(getattr(item, "horizon", 0) or 0)
        mean_excess = _format_percent(
            getattr(item, "mean_realized_excess_return", None),
            signed=True,
        )
        summary = " · ".join(
            [
                _safe_text(getattr(item, "window_type", None)),
                f"평균 초과수익 {mean_excess}",
                f"적중률 {_format_percent(getattr(item, 'hit_rate', None))}",
            ]
        )
        rows.append(
            _snapshot_row(
                snapshot_type="weekly_report",
                snapshot_key=f"evaluation_h{horizon}_{order}",
                built_at=built_at,
                as_of_date=as_of_date,
                horizon=horizon,
                sort_order=order,
                title=f"{_hold_basis_label(horizon)} 성과 요약",
                subtitle="평가 요약",
                summary=summary,
                payload={
                    "count_evaluated": getattr(item, "count_evaluated", None),
                    "ranking_version": _safe_text(getattr(item, "ranking_version", None)),
                },
                source_run_id=source_run_id,
            )
        )
    for item in policy_eval.head(BOT_WEEKLY_LIMIT).itertuples(index=False):
        order += 1
        horizon = int(getattr(item, "horizon", 0) or 0)
        summary = " · ".join(
            [
                _safe_text(getattr(item, "template_id", None)),
                _safe_text(getattr(item, "scope_type", None)),
                f"적중률 {_format_percent(getattr(item, 'hit_rate', None))}",
            ]
        )
        rows.append(
            _snapshot_row(
                snapshot_type="weekly_report",
                snapshot_key=f"policy_h{horizon}_{order}",
                built_at=built_at,
                as_of_date=as_of_date,
                horizon=horizon,
                sort_order=order,
                title=f"{_hold_basis_label(horizon)} 정책 점검",
                subtitle="정책 평가",
                summary=summary,
                payload={
                    "objective_score": getattr(item, "objective_score", None),
                    "test_session_count": getattr(item, "test_session_count", None),
                },
                source_run_id=source_run_id,
            )
        )
    return rows


def _build_stock_summary_rows(
    *,
    summary_frame: pd.DataFrame,
    live_frame: pd.DataFrame,
    built_at: str,
    as_of_date: str | None,
    source_run_id: str,
    score_evidence: dict[str, ScoreBandEvidence] | None = None,
) -> list[dict[str, object]]:
    if summary_frame.empty:
        return []
    live_by_symbol = {
        str(row.symbol): row for row in live_frame.itertuples(index=False)
    } if not live_frame.empty else {}
    rows: list[dict[str, object]] = []
    for item in summary_frame.itertuples(index=False):
        symbol = _safe_text(getattr(item, "symbol", None))
        live = live_by_symbol.get(symbol)
        d1_grade = _safe_text(
            getattr(live, "live_d1_selection_v2_grade", None)
            if live
            else getattr(item, "d1_selection_v2_grade", None)
        )
        d5_grade = _safe_text(
            getattr(live, "live_d5_selection_v2_grade", None)
            if live
            else getattr(item, "d5_selection_v2_grade", None)
        )
        d5_expected = (
            getattr(live, "live_d5_expected_excess_return", None)
            if live is not None
            else getattr(item, "d5_alpha_expected_excess_return", None)
        )
        d5_score = (
            getattr(live, "live_d5_selection_v2_value", None)
            if live is not None
            else getattr(item, "d5_selection_v2_value", None)
        )
        judgement = classify_recommendation(
            final_selection_value=d5_score,
            expected_excess_return=d5_expected,
            evidence_by_band=score_evidence,
        )
        summary = " · ".join(
            [
                judgement.label,
                f"D5 점수 {_format_number(d5_score)}",
                f"D5 {d5_grade}",
                f"기대 {_format_percent(d5_expected, signed=True)}",
                f"5일수익 {_format_percent(getattr(item, 'ret_5d', None), signed=True)}",
            ]
        )
        payload = {
            "d1_grade": d1_grade,
            "d5_grade": d5_grade,
            "d1_model_spec_id": _safe_text(
                getattr(live, "live_d1_model_spec_id", None) if live else None
            ),
            "d1_active_alpha_model_id": _safe_text(
                getattr(live, "live_d1_active_alpha_model_id", None) if live else None
            ),
            "d5_model_spec_id": _safe_text(
                getattr(live, "live_d5_model_spec_id", None) if live else None
            ),
            "d5_active_alpha_model_id": _safe_text(
                getattr(live, "live_d5_active_alpha_model_id", None) if live else None
            ),
            "d5_expected_excess_return": d5_expected,
            "d5_final_selection_value": d5_score,
            "d5_judgement_label": judgement.label,
            "d5_judgement_summary": judgement.summary,
            "ret_5d": getattr(item, "ret_5d", None),
            "ret_20d": getattr(item, "ret_20d", None),
            "news_count_3d": getattr(item, "news_count_3d", None),
            "d5_alpha_uncertainty_score": getattr(item, "d5_alpha_uncertainty_score", None),
        }
        rows.append(
            _snapshot_row(
                snapshot_type="stock_summary",
                snapshot_key=symbol,
                built_at=built_at,
                as_of_date=as_of_date,
                symbol=symbol,
                company_name=_safe_text(getattr(item, "company_name", None)),
                market=_safe_text(getattr(item, "market", None)),
                title=f"{symbol} {_safe_text(getattr(item, 'company_name', None))}",
                subtitle="종목 요약",
                summary=summary,
                payload=payload,
                source_run_id=source_run_id,
            )
        )
    return rows


def _delete_snapshot_types(settings: Settings, snapshot_types: list[str]) -> None:
    for snapshot_type in snapshot_types:
        execute_postgres_sql(
            settings,
            f"DELETE FROM {BOT_SNAPSHOT_TABLE} WHERE snapshot_type = ?",
            [snapshot_type],
        )


def materialize_discord_bot_read_store(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date | None,
    job_run_id: str,
) -> OpsJobResult:
    if not metadata_postgres_enabled(settings):
        return OpsJobResult(
            run_id=job_run_id,
            job_name="materialize_discord_bot_read_store",
            status=JobStatus.SKIPPED,
            notes="Discord bot read store skipped because metadata Postgres is disabled.",
            as_of_date=as_of_date,
        )

    ensure_postgres_metadata_store(settings)
    built_at = now_local(settings.app.timezone)
    built_at_text = built_at.isoformat()
    ranking_version = resolve_latest_ranking_version(connection)
    ranking_as_of_date = resolve_latest_ranking_date(connection, ranking_version)
    target_as_of_date = as_of_date or ranking_as_of_date or built_at.date()
    target_as_of_date_text = None if target_as_of_date is None else target_as_of_date.isoformat()

    leaderboard = pd.DataFrame()
    if ranking_version is not None and ranking_as_of_date is not None:
        leaderboard = leaderboard_frame(
            connection,
            as_of_date=ranking_as_of_date,
            ranking_version=ranking_version,
        )

    score_evidence_by_horizon: dict[int, dict[str, ScoreBandEvidence]] = {}
    if ranking_version is not None:
        for horizon in (1, 5):
            try:
                score_evidence_by_horizon[horizon] = load_score_band_evidence(
                    connection,
                    horizon=horizon,
                    ranking_version=ranking_version,
                )
            except duckdb.Error:
                score_evidence_by_horizon[horizon] = {}

    summary_frame = stock_workbench_summary_frame(connection)
    live_frame = stock_workbench_live_snapshot_frame(
        connection,
        ranking_as_of_date=ranking_as_of_date,
    )
    alpha_promotion = load_alpha_promotion_summary(connection, as_of_date=target_as_of_date)
    evaluation_summary = latest_evaluation_summary_frame(connection)
    policy_eval = latest_intraday_policy_evaluation_frame(connection)

    rows: list[dict[str, object]] = []
    rows.extend(
        _build_status_rows(
            built_at=built_at_text,
            as_of_date=target_as_of_date_text,
            ranking_as_of_date=None if ranking_as_of_date is None else str(ranking_as_of_date),
            ranking_version=ranking_version,
            source_run_id=job_run_id,
        )
    )
    for horizon in (1, 5):
        rows.extend(
            _build_pick_rows(
                leaderboard,
                horizon=horizon,
                built_at=built_at_text,
                as_of_date=target_as_of_date_text,
                source_run_id=job_run_id,
                score_evidence=score_evidence_by_horizon.get(int(horizon)),
            )
        )
    rows.extend(
        _build_weekly_rows(
            alpha_promotion=alpha_promotion,
            evaluation_summary=evaluation_summary,
            policy_eval=policy_eval,
            built_at=built_at_text,
            as_of_date=target_as_of_date_text,
            source_run_id=job_run_id,
        )
    )
    rows.extend(
        _build_stock_summary_rows(
            summary_frame=summary_frame,
            live_frame=live_frame,
            built_at=built_at_text,
            as_of_date=target_as_of_date_text,
            source_run_id=job_run_id,
            score_evidence=score_evidence_by_horizon.get(5),
        )
    )

    snapshot_types = list(BOT_SNAPSHOT_TYPES)
    _delete_snapshot_types(settings, snapshot_types)
    insert_query = f"""
        INSERT INTO {BOT_SNAPSHOT_TABLE} (
            snapshot_type,
            snapshot_key,
            as_of_date,
            horizon,
            sort_order,
            symbol,
            company_name,
            market,
            title,
            subtitle,
            summary,
            payload_json,
            snapshot_ts,
            source_run_id,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    executemany_postgres_sql(
        settings,
        insert_query,
        [
            [
                row["snapshot_type"],
                row["snapshot_key"],
                row["as_of_date"],
                row["horizon"],
                row["sort_order"],
                row["symbol"],
                row["company_name"],
                row["market"],
                row["title"],
                row["subtitle"],
                row["summary"],
                row["payload_json"],
                row["snapshot_ts"],
                row["source_run_id"],
                row["created_at"],
            ]
            for row in rows
        ],
    )
    return OpsJobResult(
        run_id=job_run_id,
        job_name="materialize_discord_bot_read_store",
        status=JobStatus.SUCCESS,
        notes=(
            f"Discord bot read store refreshed. snapshot_rows={len(rows)} "
            f"as_of_date={target_as_of_date_text or '-'}"
        ),
        as_of_date=target_as_of_date,
        row_count=len(rows),
    )


def fetch_discord_bot_snapshot_rows(
    settings: Settings,
    *,
    snapshot_type: str,
    horizon: int | None = None,
    symbol: str | None = None,
    query: str | None = None,
    limit: int = 10,
) -> pd.DataFrame:
    if not metadata_postgres_enabled(settings):
        return pd.DataFrame()
    where = ["snapshot_type = ?"]
    params: list[object] = [snapshot_type]
    if horizon is not None:
        where.append("horizon = ?")
        params.append(int(horizon))
    if symbol is not None:
        where.append("symbol = ?")
        params.append(str(symbol))
    if query:
        like = f"%{str(query).strip()}%"
        where.append("(symbol = ? OR company_name ILIKE ?)")
        params.extend([str(query).strip(), like])
    params.append(int(limit))
    sql = f"""
        SELECT
            snapshot_type,
            snapshot_key,
            as_of_date,
            horizon,
            sort_order,
            symbol,
            company_name,
            market,
            title,
            subtitle,
            summary,
            payload_json,
            snapshot_ts
        FROM {BOT_SNAPSHOT_TABLE}
        WHERE {" AND ".join(where)}
        ORDER BY snapshot_ts DESC, sort_order NULLS LAST, snapshot_key
        LIMIT ?
    """
    return fetchdf_postgres_sql(settings, sql, params)


def fetch_active_job_runs(
    settings: Settings,
    *,
    limit: int = 5,
) -> pd.DataFrame:
    if not metadata_postgres_enabled(settings):
        return pd.DataFrame()
    sql = """
        SELECT
            job.run_id,
            job.job_name,
            job.as_of_date,
            job.started_at,
            ROUND(EXTRACT(EPOCH FROM (NOW() - job.started_at)))::BIGINT AS running_seconds,
            step.step_name,
            step.step_order,
            step.started_at AS step_started_at,
            ROUND(EXTRACT(EPOCH FROM (NOW() - step.started_at)))::BIGINT AS step_running_seconds
        FROM fact_job_run AS job
        JOIN fact_active_lock AS active_lock
          ON active_lock.owner_run_id = job.run_id
         AND active_lock.released_at IS NULL
        LEFT JOIN LATERAL (
            SELECT
                step_name,
                step_order,
                started_at
            FROM fact_job_step_run
            WHERE job_run_id = job.run_id
              AND status = 'RUNNING'
            ORDER BY step_order DESC, started_at DESC
            LIMIT 1
        ) AS step
          ON TRUE
        WHERE job.status = 'RUNNING'
          AND job.finished_at IS NULL
          AND job.job_name IN (
              'run_daily_close_bundle',
              'run_evaluation_bundle',
              'run_news_sync_bundle',
              'run_daily_overlay_refresh_bundle',
              'run_weekly_training_bundle',
              'run_weekly_calibration_bundle',
              'run_weekly_policy_research_bundle'
          )
          AND job.started_at >= (NOW() - INTERVAL '24 hours')
        ORDER BY job.started_at DESC
        LIMIT ?
    """
    return fetchdf_postgres_sql(settings, sql, [int(limit)])
