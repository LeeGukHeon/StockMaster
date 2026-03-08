from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from app.common.disk import DiskUsageReport, measure_disk_usage
from app.ml.constants import MODEL_VERSION as ALPHA_MODEL_VERSION
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.providers.base import ProviderHealth
from app.providers.dart.client import DartProvider
from app.providers.kis.client import KISProvider
from app.providers.krx.client import KrxProvider
from app.providers.naver_news.client import NaverNewsProvider
from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.calibration import PREDICTION_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.settings import Settings, load_settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import fetch_recent_runs


def load_ui_settings(project_root: Path) -> Settings:
    settings = load_settings(project_root=project_root)
    ensure_storage_layout(settings)
    read_only = settings.paths.duckdb_path.exists()
    with duckdb_connection(settings.paths.duckdb_path, read_only=read_only) as connection:
        bootstrap_core_tables(connection)
    return settings


UI_COLUMN_LABELS: dict[str, str] = {
    "threshold": "임계치",
    "ratio": "비율",
    "provider": "제공처",
    "configured": "설정됨",
    "status": "상태",
    "detail": "상세",
    "total_symbols": "전체 종목",
    "kospi_symbols": "코스피 종목",
    "kosdaq_symbols": "코스닥 종목",
    "dart_mapped_symbols": "DART 매핑 종목",
    "active_common_stock_count": "활성 보통주 수",
    "min_trading_date": "최소 거래일",
    "max_trading_date": "최대 거래일",
    "total_days": "전체 일수",
    "trading_days": "거래일 수",
    "override_days": "오버라이드 일수",
    "run_type": "실행 종류",
    "started_at": "시작 시각",
    "finished_at": "종료 시각",
    "notes": "메모",
    "error_message": "오류 메시지",
    "latest_ohlcv_date": "최신 OHLCV 날짜",
    "latest_ohlcv_rows": "최신 OHLCV 행수",
    "latest_fundamentals_date": "최신 재무 날짜",
    "latest_fundamentals_rows": "최신 재무 행수",
    "latest_news_date": "최신 뉴스 날짜",
    "latest_news_rows": "최신 뉴스 행수",
    "latest_news_unmatched": "최신 뉴스 미매칭",
    "latest_flow_date": "최신 수급 날짜",
    "latest_flow_rows": "최신 수급 행수",
    "latest_feature_date": "최신 피처 날짜",
    "latest_feature_rows": "최신 피처 행수",
    "latest_label_date": "최신 라벨 날짜",
    "latest_available_label_rows": "최신 사용 가능 라벨 행수",
    "latest_regime_date": "최신 시장 상태 날짜",
    "latest_explanatory_ranking_date": "최신 설명형 순위 날짜",
    "latest_explanatory_ranking_rows": "최신 설명형 순위 행수",
    "latest_selection_date": "최신 선정 엔진 날짜",
    "latest_selection_rows": "최신 선정 엔진 행수",
    "latest_prediction_date": "최신 예측 밴드 날짜",
    "latest_prediction_rows": "최신 예측 밴드 행수",
    "latest_outcome_date": "최신 성과 날짜",
    "latest_outcome_rows": "최신 성과 행수",
    "latest_evaluation_summary_date": "최신 평가 요약 날짜",
    "latest_evaluation_summary_rows": "최신 평가 요약 행수",
    "latest_calibration_date": "최신 보정 진단 날짜",
    "latest_calibration_rows": "최신 보정 진단 행수",
    "feature_name": "피처명",
    "symbol_rows": "종목 수",
    "null_ratio": "결측 비율",
    "as_of_date": "기준일",
    "market_scope": "시장 범위",
    "regime_state": "시장 상태",
    "regime_score": "상태 점수",
    "breadth_up_ratio": "상승 종목 비율",
    "median_symbol_return_1d": "중앙값 1일 수익률",
    "market_realized_vol_20d": "20일 시장 실현 변동성",
    "turnover_burst_z": "거래대금 급증 Z",
    "latest_feature_version": "최신 피처 버전",
    "latest_ranking_version": "최신 설명형 순위 버전",
    "latest_selection_version": "최신 Selection 버전",
    "latest_prediction_version": "최신 예측 밴드 버전",
    "trading_date": "거래일",
    "signal_date": "신호일",
    "published_at": "발행 시각",
    "title": "제목",
    "publisher": "언론사",
    "query_bucket": "쿼리 묶음",
    "link": "링크",
    "symbol": "종목코드",
    "company_name": "종목명",
    "market": "시장",
    "horizon": "기간",
    "final_selection_value": "최종 선택 점수",
    "final_selection_rank_pct": "선택 상위 비율",
    "grade": "등급",
    "ranking_version": "순위 버전",
    "reasons": "주요 사유",
    "risks": "위험 신호",
    "expected_excess_return": "예상 초과수익률",
    "expected_excess_return_at_selection": "선정 시 예상 초과수익률",
    "lower_band": "하단 밴드",
    "median_band": "중앙 밴드",
    "upper_band": "상단 밴드",
    "outcome_status": "성과 상태",
    "realized_excess_return": "실현 초과수익률",
    "band_status": "밴드 판정",
    "row_count": "행수",
    "foreign_value_coverage": "외국인 금액 커버리지",
    "institution_value_coverage": "기관 금액 커버리지",
    "individual_value_coverage": "개인 금액 커버리지",
    "avg_expected_excess_return": "평균 예상 초과수익률",
    "avg_band_width": "평균 밴드 폭",
    "start_date": "시작일",
    "end_date": "종료일",
    "bucket_type": "구간 유형",
    "bucket_name": "구간명",
    "symbol_count": "종목 수",
    "avg_gross_forward_return": "평균 총 수익률",
    "avg_excess_forward_return": "평균 초과수익률",
    "median_excess_forward_return": "중앙값 초과수익률",
    "hit_rate": "적중률",
    "avg_prediction_error": "평균 예측 오차",
    "top_decile_gap": "상하위 10% 격차",
    "evaluation_date": "평가일",
    "matured_rows": "평가 완료 행수",
    "summary_date": "요약일",
    "window_type": "집계 창",
    "segment_value": "세그먼트",
    "count_evaluated": "평가 완료 수",
    "selection_avg_excess": "Selection 평균 초과수익률",
    "explanatory_avg_excess": "설명형 평균 초과수익률",
    "avg_excess_gap": "평균 초과수익률 차이",
    "hit_rate_gap": "적중률 차이",
    "diagnostic_date": "진단일",
    "bin_type": "구간 유형",
    "bin_value": "구간값",
    "sample_count": "표본 수",
    "expected_median": "예상 중앙값",
    "observed_mean": "관측 평균",
    "coverage_rate": "커버리지",
    "median_bias": "중앙값 편향",
    "quality_flag": "품질 플래그",
    "selection_date": "선정일",
    "investor_flow_rows": "수급 행수",
    "foreign_positive_ratio": "외국인 순매수 비율",
    "institution_positive_ratio": "기관 순매수 비율",
    "selection_rows": "선정 엔진 행수",
    "prediction_rows": "예측 밴드 행수",
    "open": "시가",
    "high": "고가",
    "low": "저가",
    "close": "종가",
    "volume": "거래량",
    "turnover_value": "거래대금",
    "revenue": "매출액",
    "operating_income": "영업이익",
    "net_income": "순이익",
    "roe": "ROE",
    "debt_ratio": "부채비율",
    "ret_5d": "5일 수익률",
    "ret_20d": "20일 수익률",
    "adv_20": "20일 평균 거래대금",
    "news_count_3d": "3일 뉴스 수",
    "foreign_net_value_ratio_5d": "5일 외국인 순매수 비율",
    "smart_money_flow_ratio_20d": "20일 스마트머니 수급 비율",
    "flow_coverage_flag": "수급 커버리지",
    "d1_selection_value": "D+1 선택 점수",
    "d1_grade": "D+1 등급",
    "d5_selection_value": "D+5 선택 점수",
    "d5_grade": "D+5 등급",
    "d5_expected_excess_return": "D+5 예상 초과수익률",
    "d5_lower_band": "D+5 하단 밴드",
    "d5_upper_band": "D+5 상단 밴드",
    "d1_realized_excess_return": "D+1 실현 초과수익률",
    "d1_band_status": "D+1 밴드 판정",
    "d5_realized_excess_return": "D+5 실현 초과수익률",
    "d5_band_status": "D+5 밴드 판정",
    "foreign_net_value": "외국인 순매수금액",
    "institution_net_value": "기관 순매수금액",
    "individual_net_value": "개인 순매수금액",
    "foreign_net_volume": "외국인 순매수수량",
    "institution_net_volume": "기관 순매수수량",
    "individual_net_volume": "개인 순매수수량",
}

UI_VALUE_LABELS: dict[str, dict[str, str]] = {
    "threshold": {
        "warning": "경고",
        "prune": "정리",
        "limit": "한계",
    },
    "provider": {
        "kis": "한국투자",
        "dart": "DART",
        "krx": "KRX",
        "naver_news": "네이버 뉴스",
        "KIS": "한국투자",
        "DART": "DART",
        "KRX": "KRX",
        "NAVER_NEWS": "네이버 뉴스",
    },
    "status": {
        "normal": "정상",
        "warning": "주의",
        "prune": "정리 필요",
        "limit": "한계",
        "success": "성공",
        "failed": "실패",
        "error": "오류",
        "ok": "정상",
        "running": "실행 중",
        "pending": "대기",
        "healthy": "정상",
        "unhealthy": "비정상",
    },
    "market": {
        "ALL": "전체",
        "KOSPI": "코스피",
        "KOSDAQ": "코스닥",
    },
    "market_scope": {
        "KR_ALL": "국내 전체",
        "KOSPI": "코스피",
        "KOSDAQ": "코스닥",
    },
    "ranking_version": {
        EXPLANATORY_RANKING_VERSION: "설명형 순위 v0",
        SELECTION_ENGINE_VERSION: "선정 엔진 v1",
    },
    "prediction_version": {
        PREDICTION_VERSION: "프록시 예측 밴드 v1",
    },
    "run_type": {
        "bootstrap": "초기화",
        "sync_universe": "종목 유니버스 동기화",
        "sync_trading_calendar": "거래일 캘린더 동기화",
        "provider_smoke_check": "프로바이더 스모크 체크",
        "sync_daily_ohlcv": "일봉 동기화",
        "sync_fundamentals_snapshot": "재무 스냅샷 동기화",
        "sync_news_metadata": "뉴스 메타데이터 동기화",
        "sync_investor_flow": "수급 데이터 동기화",
        "build_feature_store": "피처 스토어 생성",
        "build_forward_labels": "미래 수익률 라벨 생성",
        "build_market_regime_snapshot": "시장 상태 스냅샷 생성",
        "materialize_explanatory_ranking": "설명형 순위 생성",
        "validate_explanatory_ranking": "설명형 순위 검증",
        "materialize_selection_engine_v1": "Selection 엔진 생성",
        "calibrate_proxy_prediction_bands": "Proxy 밴드 보정",
        "validate_selection_engine_v1": "Selection 엔진 검증",
        "render_discord_eod_report": "Discord 장마감 리포트 렌더",
        "publish_discord_eod_report": "Discord 장마감 리포트 발행",
        "materialize_selection_outcomes": "Selection Outcome 생성",
        "materialize_prediction_evaluation": "예측 평가 요약 생성",
        "materialize_calibration_diagnostics": "Calibration 진단 생성",
        "render_postmortem_report": "Postmortem 리포트 렌더",
        "publish_discord_postmortem_report": "Postmortem Discord 발행",
        "validate_evaluation_pipeline": "평가 파이프라인 검증",
        "run_daily_pipeline": "일일 파이프라인 실행",
        "run_evaluation": "평가 실행",
        "prune_storage": "저장소 정리",
    },
    "regime_state": {
        "panic": "패닉",
        "risk_off": "리스크 오프",
        "neutral": "중립",
        "risk_on": "리스크 온",
        "euphoria": "과열",
    },
    "outcome_status": {
        "matured": "평가 완료",
        "pending": "대기",
        "unavailable": "평가 불가",
    },
    "band_status": {
        "in_band": "밴드 내",
        "above_upper": "상단 초과",
        "below_lower": "하단 하회",
        "band_missing": "밴드 없음",
        "label_pending": "라벨 대기",
    },
    "window_type": {
        "cohort": "코호트",
        "rolling_20d": "20거래일 롤링",
        "rolling_60d": "60거래일 롤링",
    },
    "segment_value": {
        "all": "전체",
        "top_decile": "상위 10%",
        "report_candidates": "리포트 후보",
    },
    "bucket_type": {
        "grade": "등급",
        "decile": "10분위",
        "overall": "전체",
        "expected_return_bin": "예상수익 구간",
    },
    "quality_flag": {
        "ok": "양호",
        "coverage_drift": "커버리지 이탈",
        "low_sample": "표본 부족",
        "band_missing": "밴드 없음",
    },
}

UI_VALUE_LABELS.setdefault("ranking_version", {}).update(
    {
        SELECTION_ENGINE_V2_VERSION: "선정 엔진 v2",
    }
)
UI_VALUE_LABELS.setdefault("prediction_version", {}).update(
    {
        ALPHA_PREDICTION_VERSION: "ML 알파 예측 v1",
    }
)
UI_VALUE_LABELS.setdefault("run_type", {}).update(
    {
        "build_model_training_dataset": "모델 학습 데이터셋 생성",
        "train_alpha_model_v1": "ML 알파 모델 학습",
        "backfill_alpha_oof_predictions": "알파 OOF 백필",
        "materialize_alpha_predictions_v1": "ML 알파 추론 생성",
        "materialize_selection_engine_v2": "선정 엔진 v2 생성",
        "validate_alpha_model_v1": "알파 모델 검증",
        "compare_selection_engines": "선정 엔진 비교",
        "render_model_diagnostic_report": "모델 진단 리포트 렌더",
    }
)
UI_VALUE_LABELS.setdefault("split_name", {}).update(
    {
        "train": "학습",
        "validation": "검증",
        "inference": "추론",
    }
)
UI_VALUE_LABELS.setdefault("member_name", {}).update(
    {
        "elasticnet": "ElasticNet",
        "hist_gbm": "HistGBM",
        "extra_trees": "ExtraTrees",
        "ensemble": "Ensemble",
    }
)
UI_COLUMN_LABELS.update(
    {
        "latest_selection_v2_ranking_version": "최신 Selection v2 버전",
        "latest_alpha_model_version": "최신 알파 모델 버전",
        "latest_alpha_prediction_version": "최신 알파 예측 버전",
        "uncertainty_score": "불확실성 점수",
        "disagreement_score": "불일치 점수",
        "fallback_flag": "Fallback 여부",
        "fallback_reason": "Fallback 사유",
        "latest_model_train_date": "최신 모델 학습일",
        "latest_model_train_rows": "최신 모델 학습 행수",
        "latest_model_prediction_date": "최신 알파 예측일",
        "latest_model_prediction_rows": "최신 알파 예측 행수",
        "latest_selection_v2_date": "최신 Selection v2 일자",
        "latest_selection_v2_rows": "최신 Selection v2 행수",
        "d1_selection_v2_value": "D+1 Selection v2 점수",
        "d1_selection_v2_grade": "D+1 Selection v2 등급",
        "d5_selection_v2_value": "D+5 Selection v2 점수",
        "d5_selection_v2_grade": "D+5 Selection v2 등급",
        "d5_alpha_expected_excess_return": "D+5 알파 예상 초과수익률",
        "d5_alpha_lower_band": "D+5 알파 하단 밴드",
        "d5_alpha_upper_band": "D+5 알파 상단 밴드",
        "d5_alpha_uncertainty_score": "D+5 알파 불확실성",
        "d5_alpha_disagreement_score": "D+5 알파 불일치",
        "d5_alpha_fallback_flag": "D+5 알파 fallback 여부",
        "d5_selection_v2_realized_excess_return": "D+5 Selection v2 실현 초과수익률",
        "d5_selection_v2_band_status": "D+5 Selection v2 밴드 판정",
        "train_row_count": "학습 행수",
        "validation_row_count": "검증 행수",
        "member_name": "모델 구성원",
        "split_name": "분할",
        "metric_name": "지표명",
        "metric_value": "지표값",
        "sample_count": "표본 수",
        "selection_v2_avg_excess": "Selection v2 평균 초과수익률",
        "selection_v1_avg_excess": "Selection v1 평균 초과수익률",
        "explanatory_v0_avg_excess": "설명형 v0 평균 초과수익률",
        "v2_vs_v1_gap": "v2-v1 차이",
        "v2_vs_explanatory_gap": "v2-설명형 차이",
    }
)

UI_REASON_TAG_LABELS: dict[str, str] = {
    "short_term_momentum_strong": "단기 모멘텀 강함",
    "breakout_near_20d_high": "20일 고점 근접",
    "turnover_surge": "거래대금 급증",
    "fresh_news_catalyst": "신규 뉴스 촉매",
    "quality_metrics_supportive": "질적 지표 우호",
    "low_drawdown_relative": "낙폭 안정적",
    "foreign_institution_flow_supportive": "외국인·기관 수급 우호",
    "implementation_friction_contained": "실행 마찰 낮음",
}

UI_RISK_TAG_LABELS: dict[str, str] = {
    "high_realized_volatility": "실현 변동성 높음",
    "large_recent_drawdown": "최근 낙폭 큼",
    "weak_fundamental_coverage": "재무 커버리지 약함",
    "thin_liquidity": "유동성 부족",
    "news_link_low_confidence": "뉴스 연결 신뢰 낮음",
    "data_missingness_high": "데이터 결손 높음",
    "uncertainty_proxy_high": "불확실성 프록시 높음",
    "implementation_friction_high": "실행 마찰 높음",
    "flow_coverage_missing": "수급 커버리지 부족",
}

UI_NOTE_TAG_LABELS: dict[str, str] = {
    "missing_price": "가격 데이터 없음",
    "stale_price": "가격 데이터 지연",
    "adv20_below_threshold": "20일 평균 거래대금 기준 미달",
    "feature_missingness_high": "피처 결손 높음",
    **UI_RISK_TAG_LABELS,
}

UI_REASON_TAG_LABELS.update(
    {
        "ml_alpha_supportive": "ML 알파 지지",
        "prediction_fallback_used": "예측 fallback 사용",
    }
)
UI_RISK_TAG_LABELS.update(
    {
        "model_uncertainty_high": "모델 불확실성 높음",
        "model_disagreement_high": "모델 불일치 높음",
        "prediction_fallback": "예측 fallback 사용",
    }
)
UI_NOTE_TAG_LABELS.update(UI_RISK_TAG_LABELS)


def _translate_scalar(column: str, value: object) -> object:
    if pd.isna(value):
        return value
    if isinstance(value, bool):
        return "예" if value else "아니오"
    if column in {"latest_ranking_version", "latest_selection_version"}:
        return _translate_scalar("ranking_version", value)
    if column == "latest_prediction_version":
        return _translate_scalar("prediction_version", value)
    mapping = UI_VALUE_LABELS.get(column)
    if mapping is None:
        return value
    text = str(value)
    return mapping.get(text, value)


def _translate_json_list(value: object, mapping: dict[str, str]) -> object:
    if pd.isna(value):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return value
    if not isinstance(parsed, list):
        return value
    translated = [mapping.get(str(item), str(item)) for item in parsed]
    return ", ".join(translated) if translated else "-"


def localize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    localized = frame.copy()
    for column in localized.columns:
        if column in {"reasons", "top_reason_tags_json"}:
            localized[column] = localized[column].map(
                lambda value: _translate_json_list(value, UI_REASON_TAG_LABELS)
            )
            continue
        if column in {"risks", "risk_flags_json"}:
            localized[column] = localized[column].map(
                lambda value: _translate_json_list(value, UI_RISK_TAG_LABELS)
            )
            continue
        if column == "eligibility_notes_json":
            localized[column] = localized[column].map(
                lambda value: _translate_json_list(value, UI_NOTE_TAG_LABELS)
            )
            continue
        localized[column] = localized[column].map(
            lambda value, current_column=column: _translate_scalar(current_column, value)
        )
    return localized.rename(columns=UI_COLUMN_LABELS)


def format_ranking_version_label(value: str) -> str:
    return str(_translate_scalar("ranking_version", value))


def format_market_label(value: str) -> str:
    translated = _translate_scalar("market", value)
    if translated == value:
        translated = _translate_scalar("market_scope", value)
    return str(translated)


def format_disk_status_label(value: object) -> str:
    return str(_translate_scalar("status", value))


def recent_runs_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        frame = fetch_recent_runs(connection, limit=limit)
    return frame


def disk_report(settings: Settings) -> DiskUsageReport:
    return measure_disk_usage(
        settings.paths.data_dir,
        warning_ratio=settings.storage.warning_ratio,
        prune_ratio=settings.storage.prune_ratio,
        limit_ratio=settings.storage.limit_ratio,
    )


def provider_health_frame(settings: Settings) -> pd.DataFrame:
    providers = [
        KISProvider(settings),
        DartProvider(settings),
        KrxProvider(settings),
        NaverNewsProvider(settings),
    ]
    rows: list[ProviderHealth] = []
    try:
        for provider in providers:
            try:
                rows.append(provider.health_check())
            except Exception as exc:
                rows.append(
                    ProviderHealth(
                        provider=provider.provider_name,
                        configured=provider.is_configured(),
                        status="error",
                        detail=str(exc),
                    )
                )
    finally:
        for provider in providers:
            provider.close()
    return pd.DataFrame([asdict(row) for row in rows])


def watermark_frame(settings: Settings) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"threshold": "warning", "ratio": settings.storage.warning_ratio},
            {"threshold": "prune", "ratio": settings.storage.prune_ratio},
            {"threshold": "limit", "ratio": settings.storage.limit_ratio},
        ]
    )


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


def _resolve_latest_ranking_version(connection, ranking_version: str | None) -> str | None:
    if ranking_version:
        return ranking_version
    preferred_versions = _preferred_ranking_versions()
    order_clause = " ".join(
        [
            f"WHEN ranking_version = '{value}' THEN {index}"
            for index, value in enumerate(preferred_versions)
        ]
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


def _resolve_latest_ranking_date(connection, ranking_version: str | None) -> object:
    effective_version = _resolve_latest_ranking_version(connection, ranking_version)
    if effective_version is None:
        return None
    return connection.execute(
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = ?
        """,
        [effective_version],
    ).fetchone()[0]


def universe_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                COUNT(*) AS total_symbols,
                COUNT(*) FILTER (WHERE market = 'KOSPI') AS kospi_symbols,
                COUNT(*) FILTER (WHERE market = 'KOSDAQ') AS kosdaq_symbols,
                COUNT(*) FILTER (WHERE dart_corp_code IS NOT NULL) AS dart_mapped_symbols,
                COUNT(*) FILTER (
                    WHERE market IN ('KOSPI', 'KOSDAQ')
                      AND COALESCE(is_common_stock, FALSE)
                      AND NOT COALESCE(is_etf, FALSE)
                      AND NOT COALESCE(is_etn, FALSE)
                      AND NOT COALESCE(is_spac, FALSE)
                      AND NOT COALESCE(is_reit, FALSE)
                      AND NOT COALESCE(is_delisted, FALSE)
                ) AS active_common_stock_count
            FROM dim_symbol
            """
        ).fetchdf()


def calendar_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                MIN(trading_date) AS min_trading_date,
                MAX(trading_date) AS max_trading_date,
                COUNT(*) AS total_days,
                COUNT(*) FILTER (WHERE is_trading_day) AS trading_days,
                COUNT(*) FILTER (WHERE is_override) AS override_days
            FROM dim_trading_calendar
            """
        ).fetchdf()


def latest_sync_runs_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                run_type,
                started_at,
                finished_at,
                status,
                notes
            FROM ops_run_manifest
            WHERE run_type IN (
                'sync_universe',
                'sync_trading_calendar',
                'sync_daily_ohlcv',
                'sync_fundamentals_snapshot',
                'sync_news_metadata',
                'sync_investor_flow',
                'build_feature_store',
                'build_forward_labels',
                'build_market_regime_snapshot',
                'materialize_explanatory_ranking',
                'materialize_selection_engine_v1',
                'calibrate_proxy_prediction_bands',
                'materialize_selection_outcomes',
                'materialize_prediction_evaluation',
                'materialize_calibration_diagnostics',
                'validate_explanatory_ranking'
                ,
                'validate_selection_engine_v1',
                'validate_evaluation_pipeline',
                'render_discord_eod_report',
                'publish_discord_eod_report',
                'render_postmortem_report',
                'publish_discord_postmortem_report'
            )
            QUALIFY ROW_NUMBER() OVER (PARTITION BY run_type ORDER BY started_at DESC) = 1
            ORDER BY run_type
            """
        ).fetchdf()


def research_data_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                (SELECT MAX(trading_date) FROM fact_daily_ohlcv) AS latest_ohlcv_date,
                (SELECT COUNT(*) FROM fact_daily_ohlcv WHERE trading_date = (
                    SELECT MAX(trading_date) FROM fact_daily_ohlcv
                )) AS latest_ohlcv_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_fundamentals_snapshot
                ) AS latest_fundamentals_date,
                (SELECT COUNT(*) FROM fact_fundamentals_snapshot WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_fundamentals_snapshot
                )) AS latest_fundamentals_rows,
                (SELECT MAX(signal_date) FROM fact_news_item) AS latest_news_date,
                (SELECT COUNT(*) FROM fact_news_item WHERE signal_date = (
                    SELECT MAX(signal_date) FROM fact_news_item
                )) AS latest_news_rows,
                (SELECT COUNT(*) FROM fact_news_item WHERE signal_date = (
                    SELECT MAX(signal_date) FROM fact_news_item
                ) AND COALESCE(symbol_candidates, '[]') = '[]') AS latest_news_unmatched,
                (SELECT MAX(trading_date) FROM fact_investor_flow) AS latest_flow_date,
                (SELECT COUNT(*) FROM fact_investor_flow WHERE trading_date = (
                    SELECT MAX(trading_date) FROM fact_investor_flow
                )) AS latest_flow_rows,
                (SELECT MAX(as_of_date) FROM fact_feature_snapshot) AS latest_feature_date,
                (SELECT COUNT(*) FROM fact_feature_snapshot WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_feature_snapshot
                )) AS latest_feature_rows,
                (SELECT MAX(as_of_date) FROM fact_forward_return_label) AS latest_label_date,
                (SELECT COUNT(*) FROM fact_forward_return_label WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_forward_return_label
                ) AND label_available_flag) AS latest_available_label_rows,
                (SELECT MAX(as_of_date) FROM fact_market_regime_snapshot) AS latest_regime_date,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = 'explanatory_ranking_v0'
                ) AS latest_explanatory_ranking_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = 'explanatory_ranking_v0'
                ) AND ranking_version = 'explanatory_ranking_v0')
                    AS latest_explanatory_ranking_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = ?
                ) AS latest_selection_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_ranking WHERE ranking_version = ?
                ) AND ranking_version = ?) AS latest_selection_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_prediction
                    WHERE prediction_version = ?
                ) AS latest_prediction_date,
                (SELECT COUNT(*) FROM fact_prediction WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_prediction WHERE prediction_version = ?
                ) AND prediction_version = ?) AS latest_prediction_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_prediction
                    WHERE prediction_version = ?
                ) AS latest_model_prediction_date,
                (SELECT COUNT(*) FROM fact_prediction WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_prediction WHERE prediction_version = ?
                ) AND prediction_version = ?) AS latest_model_prediction_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = ?
                ) AS latest_selection_v2_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_ranking WHERE ranking_version = ?
                ) AND ranking_version = ?) AS latest_selection_v2_rows,
                (
                    SELECT MAX(train_end_date)
                    FROM fact_model_training_run
                    WHERE model_version = ?
                ) AS latest_model_train_date,
                (
                    SELECT COALESCE(SUM(train_row_count), 0)
                    FROM fact_model_training_run
                    WHERE train_end_date = (
                        SELECT MAX(train_end_date)
                        FROM fact_model_training_run
                        WHERE model_version = ?
                    )
                      AND model_version = ?
                ) AS latest_model_train_rows,
                (SELECT MAX(evaluation_date) FROM fact_selection_outcome) AS latest_outcome_date,
                (SELECT COUNT(*) FROM fact_selection_outcome WHERE evaluation_date = (
                    SELECT MAX(evaluation_date) FROM fact_selection_outcome
                )) AS latest_outcome_rows,
                (
                    SELECT MAX(summary_date)
                    FROM fact_evaluation_summary
                ) AS latest_evaluation_summary_date,
                (SELECT COUNT(*) FROM fact_evaluation_summary WHERE summary_date = (
                    SELECT MAX(summary_date) FROM fact_evaluation_summary
                )) AS latest_evaluation_summary_rows,
                (
                    SELECT MAX(diagnostic_date)
                    FROM fact_calibration_diagnostic
                ) AS latest_calibration_date,
                (SELECT COUNT(*) FROM fact_calibration_diagnostic WHERE diagnostic_date = (
                    SELECT MAX(diagnostic_date) FROM fact_calibration_diagnostic
                )) AS latest_calibration_rows
            """,
            [
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_VERSION,
                PREDICTION_VERSION,
                PREDICTION_VERSION,
                PREDICTION_VERSION,
                ALPHA_PREDICTION_VERSION,
                ALPHA_PREDICTION_VERSION,
                ALPHA_PREDICTION_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                ALPHA_MODEL_VERSION,
                ALPHA_MODEL_VERSION,
                ALPHA_MODEL_VERSION,
            ],
        ).fetchdf()


def recent_failure_runs_frame(settings: Settings, *, limit: int = 5) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                run_type,
                as_of_date,
                started_at,
                finished_at,
                error_message
            FROM ops_run_manifest
            WHERE status = 'failed'
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_ohlcv_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                trading_date,
                symbol,
                open,
                high,
                low,
                close,
                volume
            FROM fact_daily_ohlcv
            ORDER BY trading_date DESC, symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_fundamentals_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                as_of_date,
                symbol,
                revenue,
                operating_income,
                net_income,
                roe,
                debt_ratio
            FROM fact_fundamentals_snapshot
            ORDER BY as_of_date DESC, symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_news_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                signal_date,
                published_at,
                title,
                publisher,
                symbol_candidates,
                query_bucket
            FROM fact_news_item
            ORDER BY signal_date DESC, published_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_feature_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT *
            FROM vw_feature_matrix_latest
            ORDER BY symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_label_coverage_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(as_of_date) AS as_of_date
                FROM fact_forward_return_label
            )
            SELECT
                label.horizon,
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE label_available_flag) AS available_rows,
                AVG(CASE WHEN label_available_flag THEN 1.0 ELSE 0.0 END) AS coverage_ratio
            FROM fact_forward_return_label AS label
            JOIN latest_date
              ON label.as_of_date = latest_date.as_of_date
            GROUP BY label.horizon
            ORDER BY label.horizon
            """
        ).fetchdf()


def latest_feature_coverage_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(as_of_date) AS as_of_date
                FROM fact_feature_snapshot
            )
            SELECT
                feature_name,
                COUNT(*) AS symbol_rows,
                AVG(CASE WHEN feature_value IS NULL THEN 1.0 ELSE 0.0 END) AS null_ratio
            FROM fact_feature_snapshot
            WHERE as_of_date = (SELECT as_of_date FROM latest_date)
              AND feature_name IN (
                'ret_5d',
                'ret_20d',
                'adv_20',
                'roe_latest',
                'debt_ratio_latest',
                'news_count_3d',
                'foreign_net_value_ratio_5d',
                'smart_money_flow_ratio_20d',
                'flow_coverage_flag',
                'data_confidence_score'
              )
            GROUP BY feature_name
            ORDER BY feature_name
            """
        ).fetchdf()


def latest_regime_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
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


def latest_version_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                (
                    SELECT feature_version
                    FROM ops_run_manifest
                    WHERE run_type = 'build_feature_store'
                      AND status = 'success'
                      AND feature_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_feature_version,
                (
                    SELECT ranking_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_explanatory_ranking'
                      AND status = 'success'
                      AND ranking_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_explanatory_ranking_version,
                (
                    SELECT ranking_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_selection_engine_v1'
                      AND status = 'success'
                      AND ranking_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_selection_ranking_version,
                (
                    SELECT ranking_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_selection_engine_v2'
                      AND status = 'success'
                      AND ranking_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_selection_v2_ranking_version,
                (
                    SELECT model_version
                    FROM ops_run_manifest
                    WHERE run_type = 'calibrate_proxy_prediction_bands'
                      AND status = 'success'
                      AND model_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_prediction_version
                ,
                (
                    SELECT model_version
                    FROM ops_run_manifest
                    WHERE run_type = 'train_alpha_model_v1'
                      AND status = 'success'
                      AND model_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_alpha_model_version
                ,
                (
                    SELECT model_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_alpha_predictions_v1'
                      AND status = 'success'
                      AND model_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_alpha_prediction_version
            """
        ).fetchdf()


def latest_validation_summary_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                start_date,
                end_date,
                horizon,
                bucket_type,
                bucket_name,
                symbol_count,
                avg_gross_forward_return,
                avg_excess_forward_return,
                median_excess_forward_return,
                top_decile_gap
            FROM vw_latest_ranking_validation_summary
            ORDER BY bucket_type, horizon, bucket_name
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def available_ranking_versions(settings: Settings) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        preferred_versions = _preferred_ranking_versions()
        order_clause = " ".join(
            [
                f"WHEN ranking_version = '{value}' THEN {index}"
                for index, value in enumerate(preferred_versions)
            ]
        )
        rows = connection.execute(
            f"""
            SELECT DISTINCT ranking_version
            FROM fact_ranking
            ORDER BY
                CASE {order_clause} ELSE {len(preferred_versions)} END,
                ranking_version
            """
        ).fetchall()
    return [str(row[0]) for row in rows]


def available_ranking_dates(settings: Settings, *, ranking_version: str | None = None) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return []
        rows = connection.execute(
            """
            SELECT DISTINCT as_of_date
            FROM fact_ranking
            WHERE ranking_version = ?
            ORDER BY as_of_date DESC
            """,
            [effective_version],
        ).fetchall()
    return [str(row[0]) for row in rows]


def available_evaluation_dates(settings: Settings) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT evaluation_date
            FROM fact_selection_outcome
            WHERE evaluation_date IS NOT NULL
            ORDER BY evaluation_date DESC
            """
        ).fetchall()
    return [str(row[0]) for row in rows]


def leaderboard_frame(
    settings: Settings,
    *,
    as_of_date: str | None = None,
    horizon: int = 5,
    market: str = "ALL",
    limit: int = 20,
    ranking_version: str | None = None,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return pd.DataFrame()
        prediction_version = _prediction_version_for_ranking(effective_version)
        selected_date = as_of_date or _resolve_latest_ranking_date(connection, effective_version)
        if selected_date is None:
            return pd.DataFrame()
        frame = connection.execute(
            """
            SELECT
                ranking.as_of_date,
                ranking.symbol,
                symbol.company_name,
                symbol.market,
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
                prediction.uncertainty_score,
                prediction.disagreement_score,
                prediction.fallback_flag,
                prediction.fallback_reason,
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
            LEFT JOIN fact_selection_outcome AS outcome
              ON ranking.as_of_date = outcome.selection_date
             AND ranking.symbol = outcome.symbol
             AND ranking.horizon = outcome.horizon
             AND ranking.ranking_version = outcome.ranking_version
            WHERE ranking.as_of_date = ?
              AND ranking.horizon = ?
              AND ranking.ranking_version = ?
            ORDER BY ranking.final_selection_value DESC, ranking.symbol
            """,
            [prediction_version, selected_date, horizon, effective_version],
        ).fetchdf()
    if frame.empty:
        return frame
    if market.upper() != "ALL":
        frame = frame.loc[frame["market"].str.upper() == market.upper()].copy()
    frame["reasons"] = frame["top_reason_tags_json"].fillna("[]")
    frame["risks"] = frame["risk_flags_json"].fillna("[]")
    return frame.head(limit).reset_index(drop=True)


def leaderboard_grade_count_frame(
    settings: Settings,
    *,
    as_of_date: str | None = None,
    horizon: int = 5,
    ranking_version: str | None = None,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return pd.DataFrame()
        selected_date = as_of_date or _resolve_latest_ranking_date(connection, effective_version)
        if selected_date is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT grade, COUNT(*) AS row_count
            FROM fact_ranking
            WHERE as_of_date = ?
              AND horizon = ?
              AND ranking_version = ?
            GROUP BY grade
            ORDER BY grade
            """,
            [selected_date, horizon, effective_version],
        ).fetchdf()


def latest_flow_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(trading_date) AS trading_date
                FROM fact_investor_flow
            )
            SELECT
                flow.trading_date,
                COUNT(*) AS row_count,
                AVG(
                    CASE WHEN foreign_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS foreign_value_coverage,
                AVG(
                    CASE WHEN institution_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS institution_value_coverage,
                AVG(
                    CASE WHEN individual_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS individual_value_coverage
            FROM fact_investor_flow AS flow
            JOIN latest_date
              ON flow.trading_date = latest_date.trading_date
            GROUP BY flow.trading_date
            """
        ).fetchdf()


def latest_prediction_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_dates AS (
                SELECT
                    prediction_version,
                    MAX(as_of_date) AS as_of_date
                FROM fact_prediction
                WHERE prediction_version IN (?, ?)
                GROUP BY prediction_version
            )
            SELECT
                prediction.prediction_version,
                horizon,
                COUNT(*) AS row_count,
                AVG(expected_excess_return) AS avg_expected_excess_return,
                AVG(upper_band - lower_band) AS avg_band_width,
                AVG(uncertainty_score) AS uncertainty_score,
                AVG(disagreement_score) AS disagreement_score
            FROM fact_prediction AS prediction
            JOIN latest_dates
              ON prediction.prediction_version = latest_dates.prediction_version
             AND prediction.as_of_date = latest_dates.as_of_date
            GROUP BY prediction.prediction_version, horizon
            ORDER BY prediction.prediction_version, horizon
            """,
            [PREDICTION_VERSION, ALPHA_PREDICTION_VERSION],
        ).fetchdf()


def latest_model_training_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                horizon,
                train_end_date,
                train_row_count,
                validation_row_count,
                fallback_flag,
                fallback_reason
            FROM vw_latest_model_training_run
            WHERE model_version = ?
            ORDER BY horizon
            """,
            [ALPHA_MODEL_VERSION],
        ).fetchdf()


def latest_model_metric_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                horizon,
                member_name,
                split_name,
                metric_name,
                metric_value,
                sample_count
            FROM vw_latest_model_metric_summary
            WHERE model_version = ?
              AND split_name = 'validation'
            ORDER BY horizon, member_name, metric_name
            """,
            [ALPHA_MODEL_VERSION],
        ).fetchdf()


def latest_selection_engine_comparison_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_summary AS (
                SELECT *
                FROM vw_latest_evaluation_summary
                WHERE segment_type = 'coverage'
                  AND segment_value = 'all'
                  AND ranking_version IN (?, ?, ?)
            )
            SELECT
                v2.summary_date,
                v2.window_type,
                v2.horizon,
                v2.mean_realized_excess_return AS selection_v2_avg_excess,
                v1.mean_realized_excess_return AS selection_v1_avg_excess,
                expl.mean_realized_excess_return AS explanatory_v0_avg_excess,
                v2.mean_realized_excess_return - v1.mean_realized_excess_return
                    AS v2_vs_v1_gap,
                v2.mean_realized_excess_return - expl.mean_realized_excess_return
                    AS v2_vs_explanatory_gap
            FROM latest_summary AS v2
            LEFT JOIN latest_summary AS v1
              ON v2.summary_date = v1.summary_date
             AND v2.window_type = v1.window_type
             AND v2.horizon = v1.horizon
             AND v1.ranking_version = ?
            LEFT JOIN latest_summary AS expl
              ON v2.summary_date = expl.summary_date
             AND v2.window_type = expl.window_type
             AND v2.horizon = expl.horizon
             AND expl.ranking_version = ?
            WHERE v2.ranking_version = ?
            ORDER BY v2.window_type, v2.horizon
            """,
            [
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_VERSION,
                EXPLANATORY_RANKING_VERSION,
                SELECTION_ENGINE_VERSION,
                EXPLANATORY_RANKING_VERSION,
                SELECTION_ENGINE_V2_VERSION,
            ],
        ).fetchdf()


def latest_selection_validation_summary_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                start_date,
                end_date,
                horizon,
                bucket_type,
                bucket_name,
                symbol_count,
                avg_excess_forward_return,
                median_excess_forward_return,
                hit_rate,
                avg_expected_excess_return,
                avg_prediction_error,
                top_decile_gap
            FROM vw_latest_selection_validation_summary
            WHERE ranking_version = ?
            ORDER BY bucket_type, horizon, bucket_name
            LIMIT ?
            """,
            [SELECTION_ENGINE_VERSION, limit],
        ).fetchdf()


def latest_outcome_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(evaluation_date) AS evaluation_date
                FROM fact_selection_outcome
            )
            SELECT
                evaluation_date,
                horizon,
                ranking_version,
                COUNT(*) AS row_count,
                COUNT(*) FILTER (WHERE outcome_status = 'matured') AS matured_rows,
                AVG(realized_excess_return) AS avg_realized_excess_return,
                AVG(CASE WHEN realized_excess_return > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate
            FROM fact_selection_outcome
            WHERE evaluation_date = (SELECT evaluation_date FROM latest_date)
            GROUP BY evaluation_date, horizon, ranking_version
            ORDER BY horizon, ranking_version
            """
        ).fetchdf()


def latest_evaluation_summary_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                summary_date,
                window_type,
                horizon,
                ranking_version,
                segment_value,
                count_evaluated,
                mean_realized_excess_return,
                hit_rate,
                avg_expected_excess_return
            FROM vw_latest_evaluation_summary
            WHERE segment_type = 'coverage'
              AND segment_value = 'all'
            ORDER BY window_type, horizon, ranking_version
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_evaluation_comparison_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_summary AS (
                SELECT *
                FROM vw_latest_evaluation_summary
                WHERE segment_type = 'coverage'
                  AND segment_value = 'all'
            )
            SELECT
                selection.summary_date,
                selection.window_type,
                selection.horizon,
                selection.mean_realized_excess_return AS selection_avg_excess,
                explanatory.mean_realized_excess_return AS explanatory_avg_excess,
                selection.mean_realized_excess_return
                    - explanatory.mean_realized_excess_return AS avg_excess_gap,
                selection.hit_rate - explanatory.hit_rate AS hit_rate_gap
            FROM latest_summary AS selection
            JOIN latest_summary AS explanatory
              ON selection.summary_date = explanatory.summary_date
             AND selection.window_type = explanatory.window_type
             AND selection.horizon = explanatory.horizon
             AND selection.ranking_version = ?
             AND explanatory.ranking_version = ?
            ORDER BY selection.window_type, selection.horizon
            """,
            [SELECTION_ENGINE_VERSION, EXPLANATORY_RANKING_VERSION],
        ).fetchdf()


def latest_calibration_diagnostic_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                diagnostic_date,
                horizon,
                bin_type,
                bin_value,
                sample_count,
                expected_median,
                observed_mean,
                coverage_rate,
                median_bias,
                quality_flag
            FROM vw_latest_calibration_diagnostic
            ORDER BY horizon, bin_type, bin_value
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def evaluation_outcomes_frame(
    settings: Settings,
    *,
    evaluation_date: str | None = None,
    horizon: int = 5,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    limit: int = 50,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if evaluation_date is None:
            row = connection.execute(
                """
                SELECT MAX(evaluation_date)
                FROM fact_selection_outcome
                """
            ).fetchone()
            if row is None or row[0] is None:
                return pd.DataFrame()
            evaluation_date = str(row[0])
        return connection.execute(
            """
            SELECT
                outcome.evaluation_date,
                outcome.selection_date,
                outcome.symbol,
                meta.company_name,
                meta.market,
                outcome.horizon,
                outcome.ranking_version,
                outcome.final_selection_value,
                outcome.expected_excess_return_at_selection,
                outcome.realized_excess_return,
                outcome.band_status,
                outcome.outcome_status
            FROM fact_selection_outcome AS outcome
            JOIN dim_symbol AS meta
              ON outcome.symbol = meta.symbol
            WHERE outcome.evaluation_date = ?
              AND outcome.horizon = ?
              AND outcome.ranking_version = ?
            ORDER BY outcome.realized_excess_return DESC, outcome.symbol
            LIMIT ?
            """,
            [evaluation_date, horizon, ranking_version, limit],
        ).fetchdf()


def market_pulse_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
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
                    AVG(
                        CASE WHEN foreign_net_value > 0 THEN 1.0 ELSE 0.0 END
                    ) AS foreign_positive_ratio,
                    AVG(
                        CASE WHEN institution_net_value > 0 THEN 1.0 ELSE 0.0 END
                    ) AS institution_positive_ratio
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


def latest_market_news_frame(settings: Settings, *, limit: int = 5) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT signal_date, title, publisher, link
            FROM fact_news_item
            WHERE signal_date = (SELECT MAX(signal_date) FROM fact_news_item)
              AND COALESCE(is_market_wide, FALSE)
            ORDER BY published_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def available_symbols(settings: Settings, *, limit: int = 200) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT symbol
            FROM dim_symbol
            WHERE market IN ('KOSPI', 'KOSDAQ')
            ORDER BY symbol
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    return [str(row[0]).zfill(6) for row in rows]


def stock_workbench_summary_frame(settings: Settings, *, symbol: str) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                feature.symbol,
                symbol_meta.company_name,
                symbol_meta.market,
                feature.as_of_date,
                feature.ret_5d,
                feature.ret_20d,
                feature.adv_20,
                feature.news_count_3d,
                feature.foreign_net_value_ratio_5d,
                feature.smart_money_flow_ratio_20d,
                feature.flow_coverage_flag,
                selection_v2_1.final_selection_value AS d1_selection_v2_value,
                selection_v2_1.grade AS d1_selection_v2_grade,
                selection_1.final_selection_value AS d1_selection_value,
                selection_1.grade AS d1_grade,
                selection_v2_5.final_selection_value AS d5_selection_v2_value,
                selection_v2_5.grade AS d5_selection_v2_grade,
                selection_5.final_selection_value AS d5_selection_value,
                selection_5.grade AS d5_grade,
                prediction_alpha_5.expected_excess_return AS d5_alpha_expected_excess_return,
                prediction_alpha_5.lower_band AS d5_alpha_lower_band,
                prediction_alpha_5.upper_band AS d5_alpha_upper_band,
                prediction_alpha_5.uncertainty_score AS d5_alpha_uncertainty_score,
                prediction_alpha_5.disagreement_score AS d5_alpha_disagreement_score,
                prediction_alpha_5.fallback_flag AS d5_alpha_fallback_flag,
                prediction_5.expected_excess_return AS d5_expected_excess_return,
                prediction_5.lower_band AS d5_lower_band,
                prediction_5.upper_band AS d5_upper_band,
                outcome_1.realized_excess_return AS d1_realized_excess_return,
                outcome_1.band_status AS d1_band_status,
                outcome_v2_5.realized_excess_return AS d5_selection_v2_realized_excess_return,
                outcome_v2_5.band_status AS d5_selection_v2_band_status,
                outcome_5.realized_excess_return AS d5_realized_excess_return,
                outcome_5.band_status AS d5_band_status
            FROM vw_feature_matrix_latest AS feature
            JOIN dim_symbol AS symbol_meta
              ON feature.symbol = symbol_meta.symbol
            LEFT JOIN vw_ranking_latest AS selection_v2_1
              ON feature.symbol = selection_v2_1.symbol
             AND selection_v2_1.horizon = 1
             AND selection_v2_1.ranking_version = ?
            LEFT JOIN vw_ranking_latest AS selection_1
              ON feature.symbol = selection_1.symbol
             AND selection_1.horizon = 1
             AND selection_1.ranking_version = ?
            LEFT JOIN vw_ranking_latest AS selection_v2_5
              ON feature.symbol = selection_v2_5.symbol
             AND selection_v2_5.horizon = 5
             AND selection_v2_5.ranking_version = ?
            LEFT JOIN vw_ranking_latest AS selection_5
              ON feature.symbol = selection_5.symbol
             AND selection_5.horizon = 5
             AND selection_5.ranking_version = ?
            LEFT JOIN vw_prediction_latest AS prediction_alpha_5
              ON feature.symbol = prediction_alpha_5.symbol
             AND prediction_alpha_5.horizon = 5
             AND prediction_alpha_5.prediction_version = ?
            LEFT JOIN vw_prediction_latest AS prediction_5
              ON feature.symbol = prediction_5.symbol
             AND prediction_5.horizon = 5
             AND prediction_5.prediction_version = ?
            LEFT JOIN vw_selection_outcome_latest AS outcome_1
             ON feature.symbol = outcome_1.symbol
             AND outcome_1.horizon = 1
             AND outcome_1.ranking_version = ?
            LEFT JOIN vw_selection_outcome_latest AS outcome_v2_5
              ON feature.symbol = outcome_v2_5.symbol
             AND outcome_v2_5.horizon = 5
             AND outcome_v2_5.ranking_version = ?
            LEFT JOIN vw_selection_outcome_latest AS outcome_5
              ON feature.symbol = outcome_5.symbol
             AND outcome_5.horizon = 5
             AND outcome_5.ranking_version = ?
            WHERE feature.symbol = ?
            """,
            [
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_VERSION,
                ALPHA_PREDICTION_VERSION,
                PREDICTION_VERSION,
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_V2_VERSION,
                SELECTION_ENGINE_VERSION,
                symbol,
            ],
        ).fetchdf()


def stock_workbench_price_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT trading_date, open, high, low, close, volume, turnover_value
            FROM fact_daily_ohlcv
            WHERE symbol = ?
            ORDER BY trading_date DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def stock_workbench_flow_frame(settings: Settings, *, symbol: str, limit: int = 30) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                trading_date,
                foreign_net_value,
                institution_net_value,
                individual_net_value,
                foreign_net_volume,
                institution_net_volume,
                individual_net_volume
            FROM fact_investor_flow
            WHERE symbol = ?
            ORDER BY trading_date DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def stock_workbench_news_frame(settings: Settings, *, symbol: str, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT signal_date, published_at, title, publisher, query_bucket, link
            FROM fact_news_item
            WHERE symbol_candidates LIKE ?
            ORDER BY signal_date DESC, published_at DESC
            LIMIT ?
            """,
            [f"%{symbol}%", limit],
        ).fetchdf()


def stock_workbench_outcome_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                selection_date,
                evaluation_date,
                horizon,
                ranking_version,
                final_selection_value,
                expected_excess_return_at_selection,
                realized_excess_return,
                band_status,
                outcome_status
            FROM fact_selection_outcome
            WHERE symbol = ?
            ORDER BY selection_date DESC, ranking_version, horizon
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def latest_discord_preview(settings: Settings) -> str | None:
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            """
            SELECT output_artifacts_json
            FROM ops_run_manifest
            WHERE run_type = 'render_discord_eod_report'
              AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None or not row[0]:
        return None
    artifacts = json.loads(row[0])
    preview_candidates = [Path(item) for item in artifacts if str(item).endswith(".md")]
    if not preview_candidates:
        return None
    preview_path = preview_candidates[-1]
    if not preview_path.exists():
        return None
    return preview_path.read_text(encoding="utf-8")


def latest_postmortem_preview(settings: Settings) -> str | None:
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            """
            SELECT output_artifacts_json
            FROM ops_run_manifest
            WHERE run_type = 'render_postmortem_report'
              AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None or not row[0]:
        return None
    artifacts = json.loads(row[0])
    preview_candidates = [Path(item) for item in artifacts if str(item).endswith(".md")]
    if not preview_candidates:
        return None
    preview_path = preview_candidates[-1]
    if not preview_path.exists():
        return None
    return preview_path.read_text(encoding="utf-8")


UI_COLUMN_LABELS.update(
    {
        "session_date": "세션 날짜",
        "selection_date": "선정 날짜",
        "candidate_count": "후보 수",
        "candidate_symbols": "후보 종목 수",
        "bar_symbols": "1분봉 종목 수",
        "trade_symbols": "체결 요약 종목 수",
        "quote_symbols": "호가 요약 종목 수",
        "signal_symbols": "신호 종목 수",
        "decision_symbols": "판단 종목 수",
        "avg_bar_latency_ms": "평균 1분봉 지연(ms)",
        "avg_quote_latency_ms": "평균 호가 지연(ms)",
        "checkpoint_time": "체크포인트",
        "avg_signal_quality": "평균 신호 품질",
        "enter_now_count": "즉시 진입 수",
        "wait_recheck_count": "재확인 수",
        "avoid_today_count": "오늘 회피 수",
        "data_insufficient_count": "데이터 부족 수",
        "quote_unavailable_count": "호가 미가용 수",
        "trade_unavailable_count": "체결 미가용 수",
        "candidate_rank": "후보 순위",
        "session_status": "세션 상태",
        "timing_adjustment_score": "타이밍 조정 점수",
        "signal_quality_score": "신호 품질 점수",
        "gap_opening_quality_score": "갭/시가 품질",
        "micro_trend_score": "미세 추세",
        "relative_activity_score": "상대 활동성",
        "orderbook_score": "호가 점수",
        "execution_strength_score": "체결 강도 점수",
        "risk_friction_score": "마찰/충격 리스크",
        "action": "액션",
        "action_score": "액션 점수",
        "entry_reference_price": "판단 기준 가격",
        "selected_checkpoint_time": "선택 체크포인트",
        "selected_action": "선택 액션",
        "execution_flag": "진입 실행 여부",
        "naive_open_price": "시가 기준 가격",
        "decision_entry_price": "판단 진입 가격",
        "future_exit_price": "미래 청산 가격",
        "realized_return_from_open": "시가 기준 수익률",
        "realized_return_from_decision": "판단 기준 수익률",
        "timing_edge_return": "타이밍 엣지 수익률",
        "timing_edge_bps": "타이밍 엣지(bps)",
        "quote_status": "호가 상태",
        "trade_summary_status": "체결 상태",
    }
)


def _latest_intraday_session_date(settings: Settings):
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            "SELECT MAX(session_date) FROM fact_intraday_candidate_session"
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return pd.Timestamp(row[0]).date()


def latest_intraday_status_frame(settings: Settings) -> pd.DataFrame:
    session_date = _latest_intraday_session_date(settings)
    if session_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                candidate.session_date,
                COUNT(DISTINCT candidate.symbol) AS candidate_symbols,
                COUNT(DISTINCT bar.symbol) AS bar_symbols,
                COUNT(DISTINCT trade.symbol) AS trade_symbols,
                COUNT(DISTINCT quote.symbol) AS quote_symbols,
                COUNT(DISTINCT signal.symbol) AS signal_symbols,
                COUNT(DISTINCT decision.symbol) AS decision_symbols,
                AVG(bar.fetch_latency_ms) AS avg_bar_latency_ms,
                AVG(quote.fetch_latency_ms) AS avg_quote_latency_ms
            FROM fact_intraday_candidate_session AS candidate
            LEFT JOIN fact_intraday_bar_1m AS bar
              ON candidate.session_date = bar.session_date
             AND candidate.symbol = bar.symbol
            LEFT JOIN fact_intraday_trade_summary AS trade
              ON candidate.session_date = trade.session_date
             AND candidate.symbol = trade.symbol
            LEFT JOIN fact_intraday_quote_summary AS quote
              ON candidate.session_date = quote.session_date
             AND candidate.symbol = quote.symbol
            LEFT JOIN fact_intraday_signal_snapshot AS signal
              ON candidate.session_date = signal.session_date
             AND candidate.symbol = signal.symbol
            LEFT JOIN fact_intraday_entry_decision AS decision
              ON candidate.session_date = decision.session_date
             AND candidate.symbol = decision.symbol
            WHERE candidate.session_date = ?
            GROUP BY candidate.session_date
            """,
            [session_date],
        ).fetchdf()


def latest_intraday_checkpoint_health_frame(settings: Settings) -> pd.DataFrame:
    session_date = _latest_intraday_session_date(settings)
    if session_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                signal.checkpoint_time,
                AVG(signal.signal_quality_score) AS avg_signal_quality,
                SUM(CASE WHEN decision.action = 'ENTER_NOW' THEN 1 ELSE 0 END) AS enter_now_count,
                SUM(
                    CASE WHEN decision.action = 'WAIT_RECHECK' THEN 1 ELSE 0 END
                ) AS wait_recheck_count,
                SUM(
                    CASE WHEN decision.action = 'AVOID_TODAY' THEN 1 ELSE 0 END
                ) AS avoid_today_count,
                SUM(
                    CASE WHEN decision.action = 'DATA_INSUFFICIENT' THEN 1 ELSE 0 END
                ) AS data_insufficient_count,
                SUM(
                    CASE WHEN quote.quote_status = 'unavailable' THEN 1 ELSE 0 END
                ) AS quote_unavailable_count,
                SUM(
                    CASE WHEN trade.trade_summary_status = 'unavailable' THEN 1 ELSE 0 END
                ) AS trade_unavailable_count
            FROM fact_intraday_signal_snapshot AS signal
            LEFT JOIN fact_intraday_entry_decision AS decision
              ON signal.session_date = decision.session_date
             AND signal.symbol = decision.symbol
             AND signal.horizon = decision.horizon
             AND signal.checkpoint_time = decision.checkpoint_time
             AND signal.ranking_version = decision.ranking_version
            LEFT JOIN fact_intraday_quote_summary AS quote
              ON signal.session_date = quote.session_date
             AND signal.symbol = quote.symbol
             AND signal.checkpoint_time = quote.checkpoint_time
            LEFT JOIN fact_intraday_trade_summary AS trade
              ON signal.session_date = trade.session_date
             AND signal.symbol = trade.symbol
             AND signal.checkpoint_time = trade.checkpoint_time
            WHERE signal.session_date = ?
            GROUP BY signal.checkpoint_time
            ORDER BY signal.checkpoint_time
            """,
            [session_date],
        ).fetchdf()


def intraday_console_candidate_frame(
    settings: Settings,
    *,
    session_date=None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                selection_date,
                symbol,
                company_name,
                market,
                horizon,
                candidate_rank,
                final_selection_value,
                grade,
                expected_excess_return,
                session_status
            FROM fact_intraday_candidate_session
            WHERE session_date = ?
            ORDER BY horizon, candidate_rank, symbol
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def intraday_console_signal_frame(
    settings: Settings,
    *,
    session_date=None,
    checkpoint: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if checkpoint is None:
            row = connection.execute(
                """
                SELECT MAX(checkpoint_time)
                FROM fact_intraday_signal_snapshot
                WHERE session_date = ?
                """,
                [target_date],
            ).fetchone()
            checkpoint = row[0] if row and row[0] else None
        if checkpoint is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT
                session_date,
                checkpoint_time,
                symbol,
                horizon,
                gap_opening_quality_score,
                micro_trend_score,
                relative_activity_score,
                orderbook_score,
                execution_strength_score,
                risk_friction_score,
                signal_quality_score,
                timing_adjustment_score
            FROM fact_intraday_signal_snapshot
            WHERE session_date = ?
              AND checkpoint_time = ?
            ORDER BY horizon, timing_adjustment_score DESC, symbol
            LIMIT ?
            """,
            [target_date, checkpoint, limit],
        ).fetchdf()


def intraday_console_decision_frame(
    settings: Settings,
    *,
    session_date=None,
    checkpoint: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if checkpoint is None:
            row = connection.execute(
                """
                SELECT MAX(checkpoint_time)
                FROM fact_intraday_entry_decision
                WHERE session_date = ?
                """,
                [target_date],
            ).fetchone()
            checkpoint = row[0] if row and row[0] else None
        if checkpoint is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT
                decision.session_date,
                decision.checkpoint_time,
                decision.symbol,
                candidate.company_name,
                decision.horizon,
                decision.action,
                decision.action_score,
                decision.signal_quality_score,
                decision.entry_reference_price
            FROM fact_intraday_entry_decision AS decision
            LEFT JOIN fact_intraday_candidate_session AS candidate
              ON decision.session_date = candidate.session_date
             AND decision.symbol = candidate.symbol
             AND decision.horizon = candidate.horizon
             AND decision.ranking_version = candidate.ranking_version
            WHERE decision.session_date = ?
              AND decision.checkpoint_time = ?
            ORDER BY decision.horizon, decision.action_score DESC, decision.symbol
            LIMIT ?
            """,
            [target_date, checkpoint, limit],
        ).fetchdf()


def intraday_console_timing_frame(settings: Settings, *, limit: int = 30) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                symbol,
                horizon,
                selected_checkpoint_time,
                selected_action,
                timing_edge_bps,
                realized_return_from_open,
                realized_return_from_decision,
                outcome_status
            FROM fact_intraday_timing_outcome
            ORDER BY session_date DESC, horizon, symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def stock_workbench_intraday_decision_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                checkpoint_time,
                horizon,
                action,
                action_score,
                signal_quality_score,
                entry_reference_price
            FROM fact_intraday_entry_decision
            WHERE symbol = ?
            ORDER BY session_date DESC, checkpoint_time DESC, horizon
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def stock_workbench_intraday_timing_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                session_date,
                horizon,
                selected_checkpoint_time,
                selected_action,
                timing_edge_bps,
                realized_return_from_open,
                realized_return_from_decision,
                outcome_status
            FROM fact_intraday_timing_outcome
            WHERE symbol = ?
            ORDER BY session_date DESC, horizon
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()
