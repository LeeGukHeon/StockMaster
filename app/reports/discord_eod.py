from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.discord import (
    DiscordPublishDecision,
    publish_discord_messages,
    resolve_discord_publish_decision,
)
from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import MODEL_SPEC_ID
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ml.promotion import load_alpha_promotion_summary
from app.selection.sector_outlook import sector_outlook_frame
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

DISCORD_MESSAGE_LIMIT = 1800
DISCORD_EOD_CANDIDATE_HORIZON = 5
DISCORD_EOD_SECTOR_HORIZON = 5
DISCORD_EOD_REFERENCE_HORIZON = 1

REASON_LABELS = {
    "ml_alpha_supportive": "최근 흐름과 모델 판단이 함께 받쳐줌",
    "prediction_fallback_used": "예측 보조값을 함께 참고함",
    "short_term_momentum_strong": "단기 탄력 강함",
    "hort_term_momentum_strong": "단기 탄력 강함",
    "breakout_near_20d_high": "20일 고점 돌파 직전",
    "turnover_surge": "거래대금 급증",
    "fresh_news_catalyst": "새 뉴스 모멘텀",
    "quality_metrics_supportive": "기초 지표 우호적",
    "low_drawdown_relative": "낙폭이 상대적으로 작음",
    "foreign_institution_flow_supportive": "외국인·기관 수급 우호적",
    "implementation_friction_contained": "실행 부담 낮음",
    "residual_strength_improving": "상대 강도가 살아나는 흐름",
    "flow_persistence_supportive": "수급 지속성이 받쳐줌",
    "news_drift_underreacted": "뉴스 재평가가 덜 반영됨",
    "crowding_risk_low": "과열 혼잡 부담이 낮음",
    "raw_alpha_leader_preserved": "원점수 상위 신호를 최대한 보존함",
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
    "model_disagreement_high": "모델 판단이 엇갈림",
    "prediction_fallback": "예측 보조값을 함께 참고함",
}

ALPHA_DECISION_LABELS = {
    "Active kept": "기존 모델 유지",
    "Challenger promoted": "도전자 모델 승격",
    "No auto-promotion": "자동 승격 없음",
    "KEEP_ACTIVE": "기존 모델 유지",
    "PROMOTE_CHALLENGER": "도전자 모델 승격",
    "NO_AUTO_PROMOTION": "자동 승격 없음",
}

ALPHA_DECISION_REASON_LABELS = {
    "incumbent remained in the superior set": "현재 모델이 우수 후보군에 남음",
    "one challenger survived the superior set": "도전자 1개만 우수 후보군에 남음",
    "combo candidate survived the superior set": "혼합 후보가 우수 후보군에 남음",
    "multiple challengers survived without a clear winner": "도전자 여러 개가 남았지만 뚜렷한 승자가 없음",
    "matured shadow self-backtest history is not available": "성숙한 shadow 검증 이력이 아직 부족함",
    "shadow self-backtest matrix is incomplete": "shadow 검증 손실 행렬이 아직 불완전함",
    "no active H5 champion was registered, so the latest candidate initialized serving": "활성 H5 챔피언이 없어 최신 학습 후보로 초기화함",
    "the latest H5 candidate already matches the active champion": "최신 H5 후보가 이미 현재 챔피언과 같음",
    "no trained H5 candidate run was available for checkpoint challenge": "H5 체크포인트 비교에 쓸 최신 학습 후보 run이 없음",
    "the active H5 champion run could not be resolved for checkpoint challenge": "현재 H5 챔피언 run을 불러오지 못해 체크포인트 비교를 중단함",
    "no matured H5 checkpoint challenge history was available": "성숙한 H5 체크포인트 비교 이력이 부족함",
    "the latest H5 candidate did not improve selected top5 performance enough": "최신 H5 후보가 selected top5 성과를 충분히 개선하지 못함",
    "the latest H5 candidate regressed on selection drag": "최신 H5 후보가 selection drag에서 악화됨",
    "the latest H5 candidate regressed on selected top5 hit rate": "최신 H5 후보가 selected top5 적중률에서 악화됨",
    "the latest H5 candidate regressed on worst selected top5 loss": "최신 H5 후보가 selected top5 최악 손실에서 악화됨",
    "the latest H5 candidate cleared checkpoint guardrails and replaced the champion": "최신 H5 후보가 체크포인트 승급 조건을 통과해 챔피언을 교체함",
}

EXECUTION_STYLE_LABELS = {
    "OPEN_ALL": "시가 일괄 진입",
    "TIMING_ASSISTED": "장중 보정 진입",
}

REGIME_LABELS = {
    "panic": "공포가 강한 장",
    "risk_off": "방어가 우선인 장",
    "neutral": "방향성이 뚜렷하지 않은 장",
    "risk_on": "상승이 우세한 장",
    "euphoria": "과열이 강한 장",
}

MODEL_SPEC_LABELS = {
    "alpha_recursive_expanding_v1": "확장형 누적 학습",
    "alpha_rolling_120_v1": "최근 120거래일 중심 학습",
    "alpha_rolling_250_v1": "최근 250거래일 중심 학습",
    "alpha_rank_rolling_120_v1": "5일 지속성 비교 기준",
    "alpha_topbucket_h1_rolling_120_v1": "하루 선행 비교 기준",
    "alpha_lead_d1_v1": "하루 선행 포착 v1",
    "alpha_swing_d5_v2": "2~5일 스윙 포착 v2",
    "alpha_recursive_rolling_combo": "누적+최근 구간 혼합",
    "recursive": "확장형 누적 학습",
    "rolling 120d": "최근 120거래일 중심 학습",
    "rolling 250d": "최근 250거래일 중심 학습",
    "recursive+rolling combo": "누적+최근 구간 혼합",
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
    }
    ready = all(
        readiness[key] > 0
        for key in ("ranking_rows", "prediction_rows", "regime_rows", "ohlcv_rows")
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
        WITH active_models AS (
            SELECT
                horizon,
                active_alpha_model_id,
                model_spec_id
            FROM fact_alpha_active_model
            WHERE effective_from_date <= ?
              AND (effective_to_date IS NULL OR effective_to_date >= ?)
              AND active_flag = TRUE
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY horizon
                ORDER BY effective_from_date DESC, created_at DESC, active_alpha_model_id DESC
            ) = 1
        )
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
            symbol.sector,
            symbol.industry,
            ranking.final_selection_value,
            ranking.grade,
            ranking.top_reason_tags_json,
            ranking.risk_flags_json,
            prediction.expected_excess_return,
            prediction.lower_band,
            prediction.upper_band,
            COALESCE(active_models.model_spec_id, prediction.model_spec_id) AS model_spec_id,
            COALESCE(active_models.active_alpha_model_id, prediction.active_alpha_model_id) AS active_alpha_model_id,
            daily.close AS selection_close_price
        FROM fact_ranking AS ranking
        JOIN dim_symbol AS symbol
          ON ranking.symbol = symbol.symbol
        LEFT JOIN active_models
          ON ranking.horizon = active_models.horizon
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
            as_of_date,
            as_of_date,
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
            target.as_of_date,
            target.execution_mode,
            target.symbol,
            target.company_name,
            target.market,
            symbol_meta.sector,
            symbol_meta.industry,
            target.target_rank,
            target.target_weight,
            target.target_price,
            target.plan_horizon,
            target.entry_trade_date,
            target.exit_trade_date,
            target.action_plan_label,
            target.action_target_price,
            target.action_stretch_price,
            target.action_stop_price,
            target.model_spec_id,
            target.active_alpha_model_id,
            target.score_value,
            target.gate_status
        FROM fact_portfolio_target_book AS target
        LEFT JOIN dim_symbol AS symbol_meta
          ON target.symbol = symbol_meta.symbol
        WHERE target.as_of_date = ?
          AND target.execution_mode = 'OPEN_ALL'
          AND target.included_flag = TRUE
          AND target.symbol <> '__CASH__'
          AND COALESCE(target.target_weight, 0.0) > 0.0
        ORDER BY target.target_rank, target.symbol
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


def _load_selection_gap_rows(connection, *, as_of_date: date) -> pd.DataFrame:
    summary_row = connection.execute(
        """
        SELECT MAX(summary_date)
        FROM fact_alpha_shadow_selection_gap_scorecard
        WHERE summary_date <= ?
        """,
        [as_of_date],
    ).fetchone()
    summary_date = None if summary_row is None or summary_row[0] is None else summary_row[0]
    if summary_date is None:
        return pd.DataFrame()
    return connection.execute(
        """
        SELECT
            summary_date,
            window_name,
            horizon,
            model_spec_id,
            insufficient_history_flag,
            matured_selection_date_count,
            required_selection_date_count,
            raw_top5_mean_realized_excess_return,
            selected_top5_mean_realized_excess_return,
            report_candidates_mean_realized_excess_return,
            selected_top5_hit_rate,
            drag_vs_raw_top5
        FROM fact_alpha_shadow_selection_gap_scorecard
        WHERE summary_date = ?
          AND window_name = 'rolling_20'
          AND segment_name = 'top5'
        ORDER BY horizon, model_spec_id
        """,
        [summary_date],
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


def _translate_alpha_decision_label(value: object) -> str:
    text = str(value or "-")
    return ALPHA_DECISION_LABELS.get(text, text)


def _translate_alpha_decision_reason(value: object) -> str:
    text = str(value or "-")
    return ALPHA_DECISION_REASON_LABELS.get(text, text)


def _translate_execution_style(value: object) -> str:
    text = str(value or "-")
    return EXECUTION_STYLE_LABELS.get(text, text)


def _translate_model_label(value: object) -> str:
    text = str(value or "-")
    return MODEL_SPEC_LABELS.get(text, text)


def _horizon_hold_basis_label(horizon: int) -> str:
    horizon_value = int(horizon)
    if horizon_value == 1:
        return "하루 보유 기준"
    if horizon_value == 5:
        return "5거래일 보유 기준"
    return f"{horizon_value}거래일 보유 기준"


def _date_text(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    text = str(value)
    return text.split(" ", 1)[0]


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
    if pd.notna(row.get("sector")) or pd.notna(row.get("industry")):
        lines.append(
            f"   - 업종: {row.get('industry') or '-'} / 상위 섹터 {row.get('sector') or '-'}"
        )
    if pd.notna(row.get("selection_date")) or pd.notna(row.get("next_entry_trade_date")):
        lines.append(
            "   - 언제 보나: 선정일 {selection_date} / 진입 예정일 {entry_date}".format(
                selection_date=_date_text(row.get("selection_date")),
                entry_date=_date_text(row.get("next_entry_trade_date")),
            )
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
    model_spec = _translate_model_label(row.get("model_spec_id"))
    if row.get("active_alpha_model_id") or row.get("model_spec_id"):
        lines.append(f"   - active serving spec: {model_spec}")
    lines.append(f"   - 주요 근거: {reasons}")
    lines.append(f"   - 주의할 점: {risks}")
    return lines


def _format_sector_outlook_line(row: pd.Series, *, rank: int) -> str:
    broad_sector = row.get("broad_sector") or "-"
    examples = row.get("sample_symbols") or "-"
    return (
        f"{rank}. {row['outlook_label']} ({broad_sector})"
        f" | 상위 10위 내 {int(row['top10_count'] or 0)}종목"
        f" | 평균 기대 초과수익 {_pct_text(row.get('avg_expected_excess_return'))}"
        f" | 대표 종목 {examples}"
    )


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
        summary_parts.append(f"진입 방식 {_translate_execution_style(row['gate_status'])}")
    lines.append(
        f"   - 공식 추천안: {' | '.join(summary_parts) if summary_parts else '다음 거래일 공식 추천안에 포함'}"
    )
    if pd.notna(row.get("sector")) or pd.notna(row.get("industry")):
        lines.append(
            f"   - 업종: {row.get('industry') or '-'} / 상위 섹터 {row.get('sector') or '-'}"
        )

    schedule_parts: list[str] = []
    if pd.notna(row.get("entry_trade_date")):
        schedule_parts.append(f"진입 예정일 {_date_text(row['entry_trade_date'])}")
    if pd.notna(row.get("exit_trade_date")):
        schedule_parts.append(f"관찰 종료일 {_date_text(row['exit_trade_date'])}")
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
    active_top10 = ""
    if pd.notna(row.get("active_top10_mean_excess_return")):
        active_top10 = f"{float(row['active_top10_mean_excess_return']):+.2%}"
    compare_top10 = ""
    if pd.notna(row.get("comparison_top10_mean_excess_return")):
        compare_top10 = f"{float(row['comparison_top10_mean_excess_return']):+.2%}"
    compare_text = _translate_model_label(row.get("comparison_model_label"))
    if compare_top10:
        compare_text = f"{compare_text} {compare_top10}"
    active_text = _translate_model_label(row.get("active_model_label"))
    if active_top10:
        active_text = f"{active_text} {active_top10}"
    horizon_basis = _horizon_hold_basis_label(int(row["horizon"]))
    decision_label = _translate_alpha_decision_label(row.get("decision_label"))
    decision_reason = _translate_alpha_decision_reason(row.get("decision_reason_label"))
    summary_parts = [
        f"- {horizon_basis} 모델 점검 (D+{int(row['horizon'])}): {decision_label}",
        f"{row.get('active_role_label') or 'active serving spec'} {active_text}",
    ]
    if compare_text not in {"-", ""}:
        summary_parts.append(
            f"{row.get('comparison_role_label') or 'legacy comparison baseline'} "
            f"{compare_text}"
        )
    fallback_text = _translate_model_label(
        row.get("fallback_model_label") or MODEL_SPEC_LABELS.get(MODEL_SPEC_ID, MODEL_SPEC_ID)
    )
    if fallback_text not in {"-", ""}:
        summary_parts.append(
            f"{row.get('fallback_role_label') or '기본 비교 모델'} {fallback_text}"
        )
    if pd.notna(row.get("sample_count")):
        summary_parts.append(f"비교 표본 {int(row['sample_count'])}개")
    summary_parts.append(f"판단 이유 {decision_reason}")
    return " | ".join(summary_parts)


def _format_selection_gap_line(row: pd.Series) -> str:
    horizon_basis = _horizon_hold_basis_label(int(row["horizon"]))
    model_text = _translate_model_label(row.get("model_spec_id"))
    if bool(row.get("insufficient_history_flag")):
        return (
            f"- {horizon_basis} drag 점검 | {model_text} | "
            f"표본 부족 {int(row.get('matured_selection_date_count') or 0)}"
            f"/{int(row.get('required_selection_date_count') or 0)}"
        )
    drag_text = _pct_text(row.get("drag_vs_raw_top5"))
    selected_text = _pct_text(row.get("selected_top5_mean_realized_excess_return"))
    report_text = _pct_text(row.get("report_candidates_mean_realized_excess_return"))
    hit_text = _pct_text(row.get("selected_top5_hit_rate"))
    return (
        f"- {horizon_basis} drag 점검 | {model_text} | "
        f"selected top5 {selected_text} | report_candidates {report_text} | "
        f"drag vs raw {drag_text} | hit rate {hit_text}"
    )


def _build_payload_content(
    *,
    as_of_date: date,
    sector_horizon: int,
    candidate_horizon: int,
    market_pulse: dict[str, object],
    alpha_promotion: pd.DataFrame,
    selection_gap: pd.DataFrame | None = None,
    sector_outlook: pd.DataFrame,
    single_buy_candidates: pd.DataFrame,
    market_news: pd.DataFrame,
    reference_horizon: int | None = None,
    reference_candidates: pd.DataFrame | None = None,
) -> str:
    sector_basis = _horizon_hold_basis_label(sector_horizon)
    candidate_basis = _horizon_hold_basis_label(candidate_horizon)
    reference_basis = (
        _horizon_hold_basis_label(reference_horizon)
        if reference_horizon is not None
        else None
    )
    primary_candidate_title = (
        f"**2~5거래일 스윙 상위 후보 5종목 | {candidate_basis} (D+{int(candidate_horizon)})**"
        if int(candidate_horizon) == 5
        else f"**다음 거래일 상위 후보 5종목 | {candidate_basis} (D+{int(candidate_horizon)})**"
    )
    sector_title = (
        f"**2~5거래일 스윙 강세 예상 업종 | {sector_basis} (D+{int(sector_horizon)})**"
        if int(sector_horizon) == 5
        else f"**다음 거래일 강세 예상 업종 | {sector_basis} (D+{int(sector_horizon)})**"
    )
    primary_summary_line = (
        "- 아래는 상위 업종 흐름과 2~5거래일 스윙 후보를 순서대로 정리한 장마감 요약입니다."
        if int(candidate_horizon) == 5
        else "- 아래는 상위 업종 흐름과 다음 거래일 상위 후보를 순서대로 정리한 장마감 요약입니다."
    )
    primary_horizon_line = (
        f"- 메인 후보와 업종 흐름은 {candidate_basis}(D+{int(candidate_horizon)})으로 읽어주세요."
        if int(candidate_horizon) == 5
        else (
            f"- 상위 후보와 업종 흐름은 "
            f"{candidate_basis}(D+{int(candidate_horizon)})으로 읽어주세요."
        )
    )
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
        primary_summary_line,
        primary_horizon_line,
    ]
    if int(candidate_horizon) == 5:
        lines.append("- D1 후보는 단기 참고용이며, 메인 매수/관찰 리스트는 D5 스윙 후보입니다.")
    lines.extend([
        "- 모델 점검은 하루 보유 기준(D+1)과 5거래일 보유 기준(D+5)을 함께 보여줍니다.",
        "- 기대수익과 참고 범위는 과거 통계 기반 참고치일 뿐, 실제 수익을 보장하는 값은 아닙니다.",
        "",
        "**모델 점검**",
    ])
    if alpha_promotion.empty:
        lines.append("- 오늘 확인할 모델 점검 결과는 아직 없습니다.")
    else:
        lines.extend(_format_alpha_promotion_line(row) for _, row in alpha_promotion.iterrows())
    lines.extend(["", "**선택 드래그 점검**"])
    if selection_gap is None or selection_gap.empty:
        lines.append("- 최신 선택 드래그 점검 값이 아직 없습니다.")
    else:
        lines.extend(_format_selection_gap_line(row) for _, row in selection_gap.iterrows())
    lines.extend(
        [
            "",
            sector_title,
        ]
    )
    if sector_outlook.empty:
        lines.append("- 상위 랭킹 기준으로 눈에 띄는 업종 집중이 아직 없습니다.")
    else:
        for index, (_, row) in enumerate(sector_outlook.iterrows(), start=1):
            lines.append(_format_sector_outlook_line(row, rank=index))
    lines.extend(
        [
            "",
            primary_candidate_title,
        ]
    )
    if single_buy_candidates.empty:
        lines.append("- 단일매수 상위 후보가 아직 없습니다.")
    else:
        for index, (_, row) in enumerate(single_buy_candidates.iterrows(), start=1):
            lines.extend(_format_pick_block(row, rank=index))
    if reference_horizon is not None and reference_candidates is not None:
        lines.extend(
            [
                "",
                f"**참고용 D1 단기 후보 | {reference_basis} (D+{int(reference_horizon)})**",
            ]
        )
        if reference_candidates.empty:
            lines.append("- 참고용 D1 후보가 아직 없습니다.")
        else:
            for index, (_, row) in enumerate(reference_candidates.iterrows(), start=1):
                lines.extend(_format_pick_block(row, rank=index))
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
    continuation_title: str = "StockMaster 장마감 요약",
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
                f"**{continuation_title} | {as_of_date.isoformat()} "
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
                    "fact_market_regime_snapshot",
                    "fact_news_item",
                    "fact_alpha_promotion_test",
                    "fact_alpha_active_model",
                    "dim_symbol",
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
                selection_gap = _load_selection_gap_rows(connection, as_of_date=as_of_date)
                sector_outlook = sector_outlook_frame(
                    connection,
                    as_of_date=as_of_date,
                    ranking_version=SELECTION_ENGINE_V2_VERSION,
                    prediction_version=ALPHA_PREDICTION_VERSION,
                    horizon=DISCORD_EOD_SECTOR_HORIZON,
                    candidate_limit=max(top_limit * 8, 30),
                    limit=3,
                )
                single_buy_candidates = _load_top_selection_rows(
                    connection,
                    as_of_date=as_of_date,
                    horizon=DISCORD_EOD_CANDIDATE_HORIZON,
                    limit=top_limit,
                )
                reference_candidates = (
                    _load_top_selection_rows(
                        connection,
                        as_of_date=as_of_date,
                        horizon=DISCORD_EOD_REFERENCE_HORIZON,
                        limit=top_limit,
                    )
                    if DISCORD_EOD_REFERENCE_HORIZON != DISCORD_EOD_CANDIDATE_HORIZON
                    else None
                )
                market_news = _load_market_news(connection, as_of_date=as_of_date)
                content = _build_payload_content(
                    as_of_date=as_of_date,
                    sector_horizon=DISCORD_EOD_SECTOR_HORIZON,
                    candidate_horizon=DISCORD_EOD_CANDIDATE_HORIZON,
                    reference_horizon=(
                        DISCORD_EOD_REFERENCE_HORIZON
                        if reference_candidates is not None
                        else None
                    ),
                    market_pulse=market_pulse,
                    alpha_promotion=alpha_promotion,
                    selection_gap=selection_gap,
                    sector_outlook=sector_outlook,
                    single_buy_candidates=single_buy_candidates,
                    reference_candidates=reference_candidates,
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
                    f"ohlcv_rows={readiness['ohlcv_rows']}."
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="skipped",
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

        artifact_paths: list[str] = []
        notes = f"Discord publish skipped for {as_of_date.isoformat()}."
        published = False
        manifest_status = "failed"
        error_message: str | None = None
        try:
            render_result = render_discord_eod_report(
                settings,
                as_of_date=as_of_date,
                dry_run=dry_run,
            )
            artifact_paths = list(render_result.artifact_paths)
            webhook_url = settings.discord.webhook_url
            messages = render_result.payload.get("messages") or []
            decision = resolve_discord_publish_decision(
                enabled=settings.discord.enabled,
                webhook_url=webhook_url,
                dry_run=dry_run,
            )
            if decision == DiscordPublishDecision.SKIP_DISABLED:
                notes = (
                    f"Discord publish skipped for {as_of_date.isoformat()}. "
                    "DISCORD_REPORT_ENABLED=false."
                )
                manifest_status = "skipped"
            elif decision == DiscordPublishDecision.SKIP_DRY_RUN:
                notes = f"Discord publish dry-run completed for {as_of_date.isoformat()}."
                manifest_status = "skipped"
            elif decision == DiscordPublishDecision.SKIP_MISSING_WEBHOOK:
                notes = (
                    f"Discord publish skipped for {as_of_date.isoformat()}. "
                    "Webhook URL is not configured."
                )
                manifest_status = "skipped"
            else:
                response_payloads = publish_discord_messages(
                    webhook_url,
                    list(messages),
                    timeout=10.0,
                )
                published = True
                manifest_status = "success"
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
            notes = f"Discord publish failed for {as_of_date.isoformat()}."
            error_message = str(exc)
            manifest_status = "failed"
            raise
        finally:
            with duckdb_connection(settings.paths.duckdb_path) as connection:
                bootstrap_core_tables(connection)
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status=manifest_status,
                    output_artifacts=artifact_paths,
                    notes=notes,
                    error_message=error_message,
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
