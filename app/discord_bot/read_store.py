from __future__ import annotations

import json
from datetime import date
from typing import Any

import duckdb
import pandas as pd

from app.common.time import now_local
from app.ml.promotion import load_alpha_promotion_summary
from app.ops.common import JobStatus, OpsJobResult
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
from app.ui.read_model import (
    _leaderboard_frame,
    _latest_evaluation_summary_frame,
    _latest_intraday_policy_evaluation_frame,
    _resolve_latest_ranking_date,
    _resolve_latest_ranking_version,
    _stock_workbench_live_recommendation_frame,
    _stock_workbench_summary_frame,
)

BOT_SNAPSHOT_TABLE = "fact_discord_bot_snapshot"
BOT_PICK_LIMIT = 20
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
) -> list[dict[str, object]]:
    if frame.empty:
        return []
    working = frame.loc[pd.to_numeric(frame["horizon"], errors="coerce") == int(horizon)].copy()
    if working.empty:
        return []
    rows: list[dict[str, object]] = []
    for rank, row in enumerate(working.head(BOT_PICK_LIMIT).itertuples(index=False), start=1):
        reasons = _parse_json_list(getattr(row, "reasons", "[]"), REASON_LABELS)[:2]
        risks = _parse_json_list(getattr(row, "risks", "[]"), RISK_LABELS)[:2]
        summary_parts = [
            f"등급 {_safe_text(getattr(row, 'grade', None))}",
            f"예상 초과수익률 {_format_percent(getattr(row, 'expected_excess_return', None), signed=True)}",
            f"진입 예정일 {_safe_text(getattr(row, 'next_entry_trade_date', None))}",
        ]
        if reasons:
            summary_parts.append(f"핵심 근거 {', '.join(reasons)}")
        if risks:
            summary_parts.append(f"유의할 리스크 {', '.join(risks)}")
        payload = {
            "selection_date": _safe_text(getattr(row, "selection_date", None)),
            "next_entry_trade_date": _safe_text(getattr(row, "next_entry_trade_date", None)),
            "grade": _safe_text(getattr(row, "grade", None)),
            "expected_excess_return": getattr(row, "expected_excess_return", None),
            "final_selection_value": getattr(row, "final_selection_value", None),
            "industry": _safe_text(getattr(row, "industry", None)),
            "sector": _safe_text(getattr(row, "sector", None)),
            "model_spec_id": _model_label(getattr(row, "model_spec_id", None)),
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
                title=f"{_safe_text(getattr(row, 'symbol', None))} {_safe_text(getattr(row, 'company_name', None))}",
                subtitle=_hold_basis_label(int(horizon)),
                summary=" · ".join(summary_parts),
                payload=payload,
                source_run_id=source_run_id,
            )
        )
    return rows


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
                    "active_top10_mean_excess_return": getattr(item, "active_top10_mean_excess_return", None),
                    "comparison_top10_mean_excess_return": getattr(item, "comparison_top10_mean_excess_return", None),
                },
                source_run_id=source_run_id,
            )
        )
    for item in evaluation_summary.head(BOT_WEEKLY_LIMIT).itertuples(index=False):
        order += 1
        horizon = int(getattr(item, "horizon", 0) or 0)
        summary = " · ".join(
            [
                _safe_text(getattr(item, "window_type", None)),
                f"평균 초과수익 {_format_percent(getattr(item, 'mean_realized_excess_return', None), signed=True)}",
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
        d1_grade = _safe_text(getattr(live, "live_d1_selection_v2_grade", None) if live else getattr(item, "d1_selection_v2_grade", None))
        d5_grade = _safe_text(getattr(live, "live_d5_selection_v2_grade", None) if live else getattr(item, "d5_selection_v2_grade", None))
        d5_expected = (
            getattr(live, "live_d5_expected_excess_return", None)
            if live is not None
            else getattr(item, "d5_alpha_expected_excess_return", None)
        )
        summary = " · ".join(
            [
                f"D1 {d1_grade}",
                f"D5 {d5_grade}",
                f"D5 예상 초과수익률 {_format_percent(d5_expected, signed=True)}",
                f"5일 수익률 {_format_percent(getattr(item, 'ret_5d', None), signed=True)}",
                f"뉴스 {_format_number(getattr(item, 'news_count_3d', None), decimals=0)}건",
            ]
        )
        payload = {
            "d1_grade": d1_grade,
            "d5_grade": d5_grade,
            "d5_expected_excess_return": d5_expected,
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
    ranking_version = _resolve_latest_ranking_version(connection)
    ranking_as_of_date = _resolve_latest_ranking_date(connection, ranking_version)
    target_as_of_date = as_of_date or ranking_as_of_date or built_at.date()
    target_as_of_date_text = None if target_as_of_date is None else target_as_of_date.isoformat()

    leaderboard = pd.DataFrame()
    if ranking_version is not None and ranking_as_of_date is not None:
        leaderboard = _leaderboard_frame(
            connection,
            as_of_date=ranking_as_of_date,
            ranking_version=ranking_version,
        )

    summary_frame = _stock_workbench_summary_frame(connection)
    live_frame = _stock_workbench_live_recommendation_frame(
        connection,
        ranking_as_of_date=ranking_as_of_date,
    )
    alpha_promotion = load_alpha_promotion_summary(connection, as_of_date=target_as_of_date)
    evaluation_summary = _latest_evaluation_summary_frame(connection)
    policy_eval = _latest_intraday_policy_evaluation_frame(connection)

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
        ORDER BY sort_order NULLS LAST, snapshot_key
        LIMIT ?
    """
    return fetchdf_postgres_sql(settings, sql, params)
