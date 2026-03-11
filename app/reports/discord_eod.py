from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.discord import publish_discord_messages
from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.promotion import load_alpha_promotion_summary
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

DISCORD_MESSAGE_LIMIT = 1800

REASON_LABELS = {
    "ml_alpha_supportive": "최근 흐름과 모델 판단이 함께 받쳐줌",
    "prediction_fallback_used": "예측 보조값을 함께 참고함",
}

RISK_LABELS = {
    "high_realized_volatility": "최근 흔들림이 큼",
    "large_recent_drawdown": "최근 낙폭이 큼",
    "weak_fundamental_coverage": "재무 근거가 약함",
    "thin_liquidity": "거래량이 얇음",
    "news_link_low_confidence": "뉴스 연결 신뢰가 낮음",
    "data_missingness_high": "데이터 비어 있는 부분이 많음",
    "uncertainty_proxy_high": "예측 흔들림이 큼",
    "implementation_friction_high": "실행 부담이 큼",
    "flow_coverage_missing": "수급 정보가 부족함",
    "model_uncertainty_high": "모델 확신이 낮음",
    "model_disagreement_high": "모델끼리 의견이 갈림",
    "prediction_fallback": "예측 보조값을 함께 참고함",
}

REGIME_LABELS = {
    "panic": "매우 불안한 장",
    "risk_off": "조심해야 하는 장",
    "neutral": "뚜렷한 방향이 약한 장",
    "risk_on": "상대적으로 강한 장",
    "euphoria": "과열 느낌이 강한 장",
}

MODEL_SPEC_LABELS = {
    "alpha_recursive_expanding_v1": "확장형 누적 학습",
    "alpha_rolling_120_v1": "최근 120거래일 중심 학습",
    "alpha_rolling_250_v1": "최근 250거래일 중심 학습",
    "alpha_recursive_rolling_combo": "누적+최근 구간 혼합",
}


@dataclass(slots=True)
class DiscordRenderResult:
    run_id: str
    as_of_date: date
    payload: dict[str, object]
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class DiscordPublishResult:
    run_id: str
    as_of_date: date
    dry_run: bool
    published: bool
    artifact_paths: list[str]
    notes: str


def _publish_readiness(connection, *, as_of_date: date) -> tuple[bool, dict[str, int]]:
    readiness = {
        "ranking_rows": int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM fact_ranking
                WHERE as_of_date = ?
                  AND ranking_version = ?
                """,
                [as_of_date, SELECTION_ENGINE_V2_VERSION],
            ).fetchone()[0]
            or 0
        ),
        "prediction_rows": int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM fact_prediction
                WHERE as_of_date = ?
                  AND ranking_version = ?
                  AND prediction_version = ?
                """,
                [as_of_date, SELECTION_ENGINE_V2_VERSION, ALPHA_PREDICTION_VERSION],
            ).fetchone()[0]
            or 0
        ),
        "regime_rows": int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM fact_market_regime_snapshot
                WHERE as_of_date = ?
                  AND market_scope = 'KR_ALL'
                """,
                [as_of_date],
            ).fetchone()[0]
            or 0
        ),
        "ohlcv_rows": int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM fact_daily_ohlcv
                WHERE trading_date = ?
                """,
                [as_of_date],
            ).fetchone()[0]
            or 0
        ),
        "portfolio_rows": int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM fact_portfolio_target_book
                WHERE as_of_date = ?
                  AND execution_mode = 'OPEN_ALL'
                  AND symbol <> '__CASH__'
                """,
                [as_of_date],
            ).fetchone()[0]
            or 0
        ),
    }
    ready = all(
        readiness[key] > 0
        for key in ("ranking_rows", "prediction_rows", "regime_rows", "ohlcv_rows", "portfolio_rows")
    )
    return ready, readiness


def _load_market_pulse(connection, *, as_of_date: date) -> dict[str, object]:
    regime_row = connection.execute(
        """
        SELECT regime_state, regime_score, breadth_up_ratio, market_realized_vol_20d
        FROM fact_market_regime_snapshot
        WHERE as_of_date = ?
          AND market_scope = 'KR_ALL'
        """,
        [as_of_date],
    ).fetchone()
    flow_row = connection.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            AVG(CASE WHEN foreign_net_value > 0 THEN 1.0 ELSE 0.0 END) AS foreign_positive_ratio,
            AVG(
                CASE WHEN institution_net_value > 0 THEN 1.0 ELSE 0.0 END
            ) AS institution_positive_ratio
        FROM fact_investor_flow
        WHERE trading_date = ?
        """,
        [as_of_date],
    ).fetchone()
    return {
        "regime_state": regime_row[0] if regime_row else None,
        "regime_score": regime_row[1] if regime_row else None,
        "breadth_up_ratio": regime_row[2] if regime_row else None,
        "market_realized_vol_20d": regime_row[3] if regime_row else None,
        "flow_row_count": flow_row[0] if flow_row else 0,
        "foreign_positive_ratio": flow_row[1] if flow_row else None,
        "institution_positive_ratio": flow_row[2] if flow_row else None,
    }


def _load_top_selection_rows(
    connection,
    *,
    as_of_date: date,
    horizon: int,
    limit: int,
) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
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
            ranking.final_selection_value,
            ranking.grade,
            ranking.top_reason_tags_json,
            ranking.risk_flags_json,
            prediction.expected_excess_return,
            prediction.lower_band,
            prediction.upper_band,
            prediction.model_spec_id,
            prediction.active_alpha_model_id,
            daily.close AS selection_close_price
        FROM fact_ranking AS ranking
        JOIN dim_symbol AS symbol
          ON ranking.symbol = symbol.symbol
        LEFT JOIN fact_prediction AS prediction
          ON ranking.as_of_date = prediction.as_of_date
         AND ranking.symbol = prediction.symbol
         AND ranking.horizon = prediction.horizon
         AND prediction.prediction_version = ?
         AND prediction.ranking_version = ?
        LEFT JOIN fact_daily_ohlcv AS daily
          ON ranking.symbol = daily.symbol
         AND ranking.as_of_date = daily.trading_date
        WHERE ranking.as_of_date = ?
          AND ranking.horizon = ?
          AND ranking.ranking_version = ?
        ORDER BY ranking.final_selection_value DESC, ranking.symbol
        LIMIT ?
        """,
        [
            ALPHA_PREDICTION_VERSION,
            SELECTION_ENGINE_V2_VERSION,
            as_of_date,
            horizon,
            SELECTION_ENGINE_V2_VERSION,
            limit,
        ],
    ).fetchdf()


def _load_official_target_rows(
    connection,
    *,
    as_of_date: date,
    limit: int,
) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            as_of_date,
            execution_mode,
            symbol,
            company_name,
            market,
            target_rank,
            target_weight,
            target_price,
            plan_horizon,
            entry_trade_date,
            exit_trade_date,
            action_plan_label,
            action_target_price,
            action_stretch_price,
            action_stop_price,
            model_spec_id,
            active_alpha_model_id,
            score_value,
            gate_status
        FROM fact_portfolio_target_book
        WHERE as_of_date = ?
          AND execution_mode = 'OPEN_ALL'
          AND included_flag = TRUE
          AND symbol <> '__CASH__'
        ORDER BY target_rank, symbol
        LIMIT ?
        """,
        [as_of_date, limit],
    ).fetchdf()


def _load_market_news(connection, *, as_of_date: date, limit: int = 3) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT title, publisher
        FROM fact_news_item
        WHERE signal_date = ?
          AND COALESCE(is_market_wide, FALSE)
        ORDER BY published_at DESC
        LIMIT ?
        """,
        [as_of_date, limit],
    ).fetchdf()


def _translate_tags(raw_value: object, mapping: dict[str, str]) -> str:
    try:
        parsed = json.loads(raw_value or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = []
    if not isinstance(parsed, list):
        return "-"
    labels = [mapping.get(str(item), str(item)) for item in parsed[:2]]
    return ", ".join(labels) if labels else "-"


def _pct_text(value: object) -> str:
    if value is None or pd.isna(value):
        return "미확인"
    return f"{float(value):.1%}"


def _format_pick_block(row: pd.Series, *, rank: int) -> list[str]:
    reasons = _translate_tags(row["top_reason_tags_json"], REASON_LABELS)
    risks = _translate_tags(row["risk_flags_json"], RISK_LABELS)
    lines = [
        f"{rank}. `{row['symbol']}` {row['company_name']} ({row['market']})",
        f"   - 왜 봐야 하나: 등급 {row['grade']} / 종합점수 {float(row['final_selection_value']):.1f}",
    ]
    if pd.notna(row.get("selection_date")) or pd.notna(row.get("next_entry_trade_date")):
        lines.append(
            f"   - 언제 보나: 선정일 {row.get('selection_date') or '-'} / 진입 예정일 {row.get('next_entry_trade_date') or '-'}"
        )
    if pd.notna(row.get("selection_close_price")):
        lines.append(f"   - 참고 기준가: {float(row['selection_close_price']):,.0f}원")
    if all(pd.notna(row.get(key)) for key in ("expected_excess_return", "lower_band", "upper_band")):
        lines.append(
            "   - 참고 흐름: 기대수익 {expected:+.2%}, 참고 범위 {lower:+.2%} ~ {upper:+.2%}".format(
                expected=float(row["expected_excess_return"]),
                lower=float(row["lower_band"]),
                upper=float(row["upper_band"]),
            )
        )
    if all(pd.notna(row.get(key)) for key in ("selection_close_price", "expected_excess_return", "lower_band", "upper_band")):
        base_price = float(row["selection_close_price"])
        lines.append(
            "   - 참고 가격선: 목표 {target:,.0f}원 / 강한 흐름 {upper:,.0f}원 / 손절선 {stop:,.0f}원".format(
                target=base_price * (1.0 + float(row["expected_excess_return"])),
                upper=base_price * (1.0 + float(row["upper_band"])),
                stop=base_price * (1.0 + float(row["lower_band"])),
            )
        )
    model_spec = MODEL_SPEC_LABELS.get(str(row.get("model_spec_id")), str(row.get("model_spec_id") or "-"))
    if row.get("active_alpha_model_id") or row.get("model_spec_id"):
        lines.append(
            f"   - 사용 모델: {model_spec} / 활성 모델 ID {row.get('active_alpha_model_id') or '-'}"
        )
    lines.append(f"   - 주요 근거: {reasons}")
    lines.append(f"   - 주의할 점: {risks}")
    return lines


def _format_official_pick_block(row: pd.Series, *, rank: int) -> list[str]:
    lines = [
        f"{rank}. `{row['symbol']}` {row['company_name']} ({row['market']})",
    ]

    summary_parts: list[str] = []
    if pd.notna(row.get("action_plan_label")):
        summary_parts.append(str(row["action_plan_label"]))
    if pd.notna(row.get("target_weight")):
        summary_parts.append(f"목표 비중 {float(row['target_weight']):.1%}")
    if pd.notna(row.get("score_value")):
        summary_parts.append(f"추천 점수 {float(row['score_value']):+.2f}")
    if pd.notna(row.get("gate_status")):
        summary_parts.append(f"진입 판단 {row['gate_status']}")
    lines.append(
        f"   - 공식 추천안: {' | '.join(summary_parts) if summary_parts else '다음 거래일 공식 추천안에 포함'}"
    )

    schedule_parts: list[str] = []
    if pd.notna(row.get("entry_trade_date")):
        schedule_parts.append(f"진입 예정일 {row['entry_trade_date']}")
    if pd.notna(row.get("exit_trade_date")):
        schedule_parts.append(f"관찰 종료일 {row['exit_trade_date']}")
    if pd.notna(row.get("plan_horizon")):
        schedule_parts.append(f"관찰 기간 {int(row['plan_horizon'])}거래일")
    if schedule_parts:
        lines.append(f"   - 언제 보나: {' / '.join(schedule_parts)}")

    if pd.notna(row.get("target_price")):
        price_parts = [f"기준가 {float(row['target_price']):,.0f}원"]
        if pd.notna(row.get("action_target_price")):
            price_parts.append(f"목표가 {float(row['action_target_price']):,.0f}원")
        if pd.notna(row.get("action_stretch_price")):
            price_parts.append(f"강한 흐름 목표가 {float(row['action_stretch_price']):,.0f}원")
        if pd.notna(row.get("action_stop_price")):
            price_parts.append(f"손절 참고선 {float(row['action_stop_price']):,.0f}원")
        lines.append(f"   - 참고 가격선: {' / '.join(price_parts)}")

    model_spec = MODEL_SPEC_LABELS.get(str(row.get("model_spec_id")), str(row.get("model_spec_id") or "-"))
    if row.get("active_alpha_model_id") or row.get("model_spec_id"):
        lines.append(
            f"   - 사용 모델: {model_spec} / 활성 모델 ID {row.get('active_alpha_model_id') or '-'}"
        )
    return lines


def _format_alpha_promotion_line(row: pd.Series) -> str:
    p_value = ""
    if pd.notna(row.get("p_value")):
        p_value = f" | p={float(row['p_value']):.3f}"
    active_top10 = ""
    if pd.notna(row.get("active_top10_mean_excess_return")):
        active_top10 = f"{float(row['active_top10_mean_excess_return']):+.2%}"
    compare_top10 = ""
    if pd.notna(row.get("comparison_top10_mean_excess_return")):
        compare_top10 = f"{float(row['comparison_top10_mean_excess_return']):+.2%}"
    compare_text = str(row.get("comparison_model_label") or "-")
    if compare_top10:
        compare_text = f"{compare_text} {compare_top10}"
    active_text = str(row.get("active_model_label") or "-")
    if active_top10:
        active_text = f"{active_text} {active_top10}"
    return (
        f"- {int(row['horizon'])}거래일 모델 점검: {row['decision_label']} "
        f"| 현재 사용 {active_text} | 비교 후보 {compare_text} "
        f"| 비교 표본 {int(row['sample_count'])}{p_value} | 판단 이유 {row['decision_reason_label']}"
    )


def _build_payload_content(
    *,
    as_of_date: date,
    market_pulse: dict[str, object],
    alpha_promotion: pd.DataFrame,
    official_targets: pd.DataFrame,
    market_news: pd.DataFrame,
) -> str:
    lines = [
        f"**StockMaster 오늘 장마감 요약 | {as_of_date.isoformat()}**",
        "",
        "**한눈에 보기**",
        (
            f"- 오늘 시장 흐름: {REGIME_LABELS.get(str(market_pulse.get('regime_state')), market_pulse.get('regime_state') or '미확인')}"
            f" | 시장 점수 {market_pulse.get('regime_score') or '미확인'}"
            f" | 상승 종목 비율 {_pct_text(market_pulse.get('breadth_up_ratio'))}"
        ),
        (
            f"- 수급 체감: 집계 종목 {market_pulse.get('flow_row_count') or 0}개"
            f" | 외국인 플러스 비율 {_pct_text(market_pulse.get('foreign_positive_ratio'))}"
            f" | 기관 플러스 비율 {_pct_text(market_pulse.get('institution_positive_ratio'))}"
        ),
        "- 아래 종목은 웹의 '오늘의 주목 종목'과 같은 공식 추천안 기준입니다.",
        "- 기대수익과 참고 범위는 과거 통계 기반 참고치일 뿐, 실제 수익을 보장하는 값은 아닙니다.",
        "",
        "**모델 점검**",
    ]
    if alpha_promotion.empty:
        lines.append("- 오늘 확인할 모델 점검 결과는 아직 없습니다.")
    else:
        lines.extend(_format_alpha_promotion_line(row) for _, row in alpha_promotion.iterrows())
    lines.extend(
        [
            "",
            "**다음 거래일 공식 추천안**",
        ]
    )
    if official_targets.empty:
        lines.append("- 오늘 생성된 공식 추천안에는 바로 담을 종목이 없습니다.")
    else:
        for index, (_, row) in enumerate(official_targets.iterrows(), start=1):
            lines.extend(_format_official_pick_block(row, rank=index))
    lines.append("")
    lines.append("**시장 전체 주요 뉴스**")
    if market_news.empty:
        lines.append("- 해당 날짜의 시장 전체 뉴스가 없습니다.")
    else:
        for _, row in market_news.iterrows():
            lines.append(f"- {row['title']} ({row['publisher']})")
    return "\n".join(lines)


def _split_long_line(line: str, *, limit: int) -> list[str]:
    if len(line) <= limit:
        return [line]

    segments: list[str] = []
    remainder = line
    while len(remainder) > limit:
        split_at = remainder.rfind(" ", 0, limit)
        if split_at <= 0:
            split_at = limit
        segments.append(remainder[:split_at].rstrip())
        remainder = remainder[split_at:].lstrip()
    if remainder:
        segments.append(remainder)
    return segments


def _chunk_content(content: str, *, limit: int = DISCORD_MESSAGE_LIMIT) -> list[str]:
    chunks: list[str] = []
    current_lines: list[str] = []
    current_length = 0

    def flush() -> None:
        nonlocal current_lines, current_length
        if current_lines:
            chunks.append("\n".join(current_lines).strip())
            current_lines = []
            current_length = 0

    for raw_line in content.splitlines():
        line_variants = _split_long_line(raw_line, limit=limit)
        if not line_variants:
            line_variants = [""]
        for line in line_variants:
            line_length = len(line)
            separator_length = 1 if current_lines else 0
            if current_lines and current_length + separator_length + line_length > limit:
                flush()
            current_lines.append(line)
            current_length += line_length + (1 if len(current_lines) > 1 else 0)
    flush()
    return [chunk for chunk in chunks if chunk]


def _build_payload_messages(
    *,
    username: str,
    as_of_date: date,
    content: str,
) -> list[dict[str, str]]:
    raw_chunks = _chunk_content(content)
    if len(raw_chunks) <= 1:
        return [{"username": username, "content": raw_chunks[0] if raw_chunks else ""}]

    total = len(raw_chunks)
    messages: list[dict[str, str]] = []
    for index, chunk in enumerate(raw_chunks, start=1):
        if index == 1:
            content_text = chunk
        else:
            header = (
                f"**StockMaster 장마감 요약 | {as_of_date.isoformat()} "
                f"(계속 {index}/{total})**"
            )
            content_text = f"{header}\n\n{chunk}"
        messages.append({"username": username, "content": content_text})
    return messages


def render_discord_eod_report(
    settings: Settings,
    *,
    as_of_date: date,
    dry_run: bool,
    top_limit: int = 5,
) -> DiscordRenderResult:
    ensure_storage_layout(settings)

    with activate_run_context("render_discord_eod_report", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_ranking",
                    "fact_prediction",
                    "fact_portfolio_target_book",
                    "fact_market_regime_snapshot",
                    "fact_news_item",
                    "fact_alpha_promotion_test",
                    "fact_alpha_active_model",
                ],
                notes=f"Render Discord EOD report for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_V2_VERSION,
            )
            try:
                market_pulse = _load_market_pulse(connection, as_of_date=as_of_date)
                alpha_promotion = load_alpha_promotion_summary(
                    connection,
                    as_of_date=as_of_date,
                )
                official_targets = _load_official_target_rows(
                    connection,
                    as_of_date=as_of_date,
                    limit=top_limit,
                )
                market_news = _load_market_news(connection, as_of_date=as_of_date)
                content = _build_payload_content(
                    as_of_date=as_of_date,
                    market_pulse=market_pulse,
                    alpha_promotion=alpha_promotion,
                    official_targets=official_targets,
                    market_news=market_news,
                )
                messages = _build_payload_messages(
                    username=settings.discord.username,
                    as_of_date=as_of_date,
                    content=content,
                )
                payload = {
                    "username": settings.discord.username,
                    "content": messages[0]["content"] if messages else "",
                    "message_count": len(messages),
                    "messages": messages,
                }

                artifact_dir = (
                    settings.paths.artifacts_dir
                    / "discord"
                    / f"as_of_date={as_of_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                payload_path = artifact_dir / "discord_payload.json"
                payload_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                preview_path = artifact_dir / "discord_preview.md"
                preview_lines: list[str] = []
                for index, message in enumerate(messages, start=1):
                    preview_lines.append(f"## Message {index}")
                    preview_lines.append("")
                    preview_lines.append(str(message["content"]))
                    preview_lines.append("")
                preview_path.write_text("\n".join(preview_lines).strip(), encoding="utf-8")
                artifact_paths = [str(payload_path), str(preview_path)]
                notes = (
                    f"Discord EOD report rendered. as_of_date={as_of_date.isoformat()} "
                    f"dry_run={dry_run} message_count={len(messages)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                )
                return DiscordRenderResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    payload=payload,
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes=f"Discord render failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                )
                raise


def publish_discord_eod_report(
    settings: Settings,
    *,
    as_of_date: date,
    dry_run: bool,
) -> DiscordPublishResult:
    ensure_storage_layout(settings)

    with activate_run_context("publish_discord_eod_report", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["render_discord_eod_report"],
                notes=f"Publish Discord EOD report for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_V2_VERSION,
            )
            ready, readiness = _publish_readiness(connection, as_of_date=as_of_date)
            if not ready:
                notes = (
                    f"Discord publish skipped for {as_of_date.isoformat()}. "
                    "Required same-day inputs are not ready: "
                    f"ranking_rows={readiness['ranking_rows']}, "
                    f"prediction_rows={readiness['prediction_rows']}, "
                    f"regime_rows={readiness['regime_rows']}, "
                    f"ohlcv_rows={readiness['ohlcv_rows']}, "
                    f"portfolio_rows={readiness['portfolio_rows']}."
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                )
                return DiscordPublishResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    dry_run=dry_run,
                    published=False,
                    artifact_paths=[],
                    notes=notes,
                )

        render_result = render_discord_eod_report(settings, as_of_date=as_of_date, dry_run=dry_run)
        artifact_paths = list(render_result.artifact_paths)
        notes = f"Discord publish dry-run completed for {as_of_date.isoformat()}."
        published = False

        try:
            webhook_url = settings.discord.webhook_url
            messages = render_result.payload.get("messages") or []
            if not settings.discord.enabled:
                notes = (
                    f"Discord publish skipped for {as_of_date.isoformat()}. "
                    "DISCORD_REPORT_ENABLED=false."
                )
            elif dry_run or not webhook_url:
                if not webhook_url:
                    notes = (
                        f"Discord publish skipped for {as_of_date.isoformat()}. "
                        "Webhook URL is not configured."
                    )
            else:
                response_payloads = publish_discord_messages(
                    webhook_url,
                    list(messages),
                    timeout=10.0,
                )
                published = True
                publish_path = (
                    settings.paths.artifacts_dir
                    / "discord"
                    / f"as_of_date={as_of_date.isoformat()}"
                    / run_context.run_id
                    / "publish_response.json"
                )
                publish_path.parent.mkdir(parents=True, exist_ok=True)
                publish_path.write_text(
                    json.dumps(response_payloads, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                artifact_paths.append(str(publish_path))
                notes = (
                    f"Discord publish completed for {as_of_date.isoformat()}. "
                    f"message_count={len(messages)}"
                )
        except Exception as exc:
            notes = (
                f"Discord publish warning for {as_of_date.isoformat()}: {exc}. "
                "The report was rendered but publish did not complete."
            )
        finally:
            with duckdb_connection(settings.paths.duckdb_path) as connection:
                bootstrap_core_tables(connection)
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                )

        return DiscordPublishResult(
            run_id=run_context.run_id,
            as_of_date=as_of_date,
            dry_run=dry_run,
            published=published,
            artifact_paths=artifact_paths,
            notes=notes,
        )
