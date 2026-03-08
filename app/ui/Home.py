# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ui.helpers import (
    calendar_summary_frame,
    disk_report,
    format_disk_status_label,
    latest_calibration_diagnostic_frame,
    latest_discord_preview,
    latest_evaluation_comparison_frame,
    latest_evaluation_summary_frame,
    latest_feature_coverage_frame,
    latest_feature_sample_frame,
    latest_flow_summary_frame,
    latest_market_news_frame,
    latest_outcome_summary_frame,
    latest_postmortem_preview,
    latest_prediction_summary_frame,
    latest_regime_frame,
    latest_selection_validation_summary_frame,
    latest_sync_runs_frame,
    latest_validation_summary_frame,
    latest_version_frame,
    leaderboard_frame,
    leaderboard_grade_count_frame,
    load_ui_settings,
    localize_frame,
    market_pulse_frame,
    provider_health_frame,
    recent_runs_frame,
    research_data_summary_frame,
    universe_summary_frame,
)


def _disk_message(report) -> str:
    status = str(report.status)
    usage = f"{report.usage_ratio:.1%}"
    if status == "limit":
        return f"디스크 사용률이 {usage}입니다. 고빈도 수집을 줄여야 합니다."
    if status == "prune":
        return f"디스크 사용률이 {usage}입니다. 지금 정리 작업이 필요합니다."
    if status == "warning":
        return f"디스크 사용률이 {usage}입니다. 저장 공간을 주의 깊게 봐야 합니다."
    return f"디스크 사용률이 {usage}입니다. 현재는 정상 범위입니다."


def render_home() -> None:
    settings = load_ui_settings(PROJECT_ROOT)
    runs = recent_runs_frame(settings, limit=10)
    storage_report = disk_report(settings)
    provider_health = provider_health_frame(settings)
    universe_summary = universe_summary_frame(settings)
    calendar_summary = calendar_summary_frame(settings)
    latest_sync_runs = latest_sync_runs_frame(settings)
    research_summary = research_data_summary_frame(settings)
    latest_flow_summary = latest_flow_summary_frame(settings)
    latest_prediction_summary = latest_prediction_summary_frame(settings)
    latest_outcomes = latest_outcome_summary_frame(settings)
    latest_evaluation_summary = latest_evaluation_summary_frame(settings, limit=12)
    latest_evaluation_comparison = latest_evaluation_comparison_frame(settings)
    latest_calibration = latest_calibration_diagnostic_frame(settings, limit=12)
    market_pulse = market_pulse_frame(settings)
    latest_market_news = latest_market_news_frame(settings, limit=5)
    latest_feature_sample = latest_feature_sample_frame(settings, limit=5)
    latest_feature_coverage = latest_feature_coverage_frame(settings)
    latest_regime = latest_regime_frame(settings)
    latest_versions = latest_version_frame(settings)
    selection_preview = leaderboard_frame(
        settings,
        horizon=5,
        limit=10,
        ranking_version=SELECTION_ENGINE_V2_VERSION,
    )
    selection_grades = leaderboard_grade_count_frame(
        settings,
        horizon=5,
        ranking_version=SELECTION_ENGINE_V2_VERSION,
    )
    explanatory_validation = latest_validation_summary_frame(settings, limit=8)
    selection_validation = latest_selection_validation_summary_frame(settings, limit=8)
    discord_preview = latest_discord_preview(settings)
    postmortem_preview = latest_postmortem_preview(settings)

    st.title(f"{settings.app.display_name} 홈")
    st.caption(
        "국내주식 리서치 스택의 운영 현황을 보는 기본 화면입니다. "
        "설명형 순위와 선정 엔진 v1 상태를 함께 확인할 수 있습니다."
    )

    col_env, col_disk, col_status = st.columns(3)
    col_env.metric("환경", settings.app.env.upper(), settings.app.timezone)
    col_disk.metric(
        "디스크 사용률",
        f"{storage_report.usage_ratio:.1%}",
        f"{storage_report.used_gb:.2f} GB 사용 중",
    )
    col_status.metric(
        "워터마크",
        format_disk_status_label(storage_report.status).upper(),
        _disk_message(storage_report),
    )

    path_col, db_col = st.columns(2)
    with path_col:
        st.subheader("데이터 루트")
        st.code(str(settings.paths.data_dir))
    with db_col:
        st.subheader("DuckDB 경로")
        st.code(str(settings.paths.duckdb_path))

    st.subheader("기준 데이터 요약")
    summary_left, summary_right = st.columns(2)
    with summary_left:
        if universe_summary.empty:
            st.info(
                "종목 유니버스가 아직 없습니다. "
                "`python scripts/sync_universe.py`를 실행하세요."
            )
        else:
            row = universe_summary.iloc[0]
            metric_cols = st.columns(3)
            metric_cols[0].metric("전체 종목", int(row["total_symbols"]))
            metric_cols[1].metric("코스피", int(row["kospi_symbols"]))
            metric_cols[2].metric("코스닥", int(row["kosdaq_symbols"]))
            metric_cols = st.columns(2)
            metric_cols[0].metric("활성 보통주", int(row["active_common_stock_count"]))
            metric_cols[1].metric("DART 매핑", int(row["dart_mapped_symbols"]))
    with summary_right:
        if calendar_summary.empty or pd.isna(calendar_summary.iloc[0]["min_trading_date"]):
            st.info(
                "거래일 캘린더가 아직 없습니다. "
                "`python scripts/sync_trading_calendar.py`를 실행하세요."
            )
        else:
            row = calendar_summary.iloc[0]
            metric_cols = st.columns(2)
            metric_cols[0].metric("캘린더 시작", str(row["min_trading_date"]))
            metric_cols[1].metric("캘린더 종료", str(row["max_trading_date"]))
            metric_cols = st.columns(2)
            metric_cols[0].metric("거래일 수", int(row["trading_days"]))
            metric_cols[1].metric("오버라이드 일수", int(row["override_days"]))

    st.subheader("최근 동기화")
    if latest_sync_runs.empty:
        st.info("아직 동기화 이력이 없습니다.")
    else:
        st.dataframe(localize_frame(latest_sync_runs), width="stretch", hide_index=True)

    st.subheader("연구 데이터 신선도")
    if research_summary.empty or research_summary.iloc[0].isna().all():
        st.info("핵심 연구 데이터가 아직 적재되지 않았습니다.")
    else:
        row = research_summary.iloc[0]
        top, mid, bottom = st.columns(3)
        top.metric("최신 OHLCV", str(row["latest_ohlcv_date"]), int(row["latest_ohlcv_rows"] or 0))
        mid.metric(
            "최신 재무",
            str(row["latest_fundamentals_date"]),
            int(row["latest_fundamentals_rows"] or 0),
        )
        bottom.metric(
            "최신 뉴스",
            str(row["latest_news_date"]),
            (
                f"rows={int(row['latest_news_rows'] or 0)} "
                f"미매칭={int(row['latest_news_unmatched'] or 0)}"
            ),
        )
        top, mid, bottom = st.columns(3)
        top.metric("최신 수급", str(row["latest_flow_date"]), int(row["latest_flow_rows"] or 0))
        mid.metric(
            "최신 피처 스냅샷",
            str(row["latest_feature_date"]),
            int(row["latest_feature_rows"] or 0),
        )
        bottom.metric(
            "최신 라벨",
            str(row["latest_label_date"]),
            int(row["latest_available_label_rows"] or 0),
        )
        top, mid, bottom = st.columns(3)
        top.metric(
            "최신 선정 엔진 v1",
            str(row["latest_selection_date"]),
            int(row["latest_selection_rows"] or 0),
        )
        mid.metric(
            "최신 예측 밴드",
            str(row["latest_prediction_date"]),
            int(row["latest_prediction_rows"] or 0),
        )
        bottom.metric(
            "최신 설명형 순위 v0",
            str(row["latest_explanatory_ranking_date"]),
            int(row["latest_explanatory_ranking_rows"] or 0),
        )
        top, mid, bottom = st.columns(3)
        top.metric(
            "최신 모델 학습",
            str(row["latest_model_train_date"]),
            int(row["latest_model_train_rows"] or 0),
        )
        mid.metric(
            "최신 알파 예측",
            str(row["latest_model_prediction_date"]),
            int(row["latest_model_prediction_rows"] or 0),
        )
        bottom.metric(
            "최신 Selection v2",
            str(row["latest_selection_v2_date"]),
            int(row["latest_selection_v2_rows"] or 0),
        )
        top, mid, bottom = st.columns(3)
        top.metric(
            "최신 Outcome",
            str(row["latest_outcome_date"]),
            int(row["latest_outcome_rows"] or 0),
        )
        mid.metric(
            "최신 평가 요약",
            str(row["latest_evaluation_summary_date"]),
            int(row["latest_evaluation_summary_rows"] or 0),
        )
        bottom.metric(
            "최신 Calibration",
            str(row["latest_calibration_date"]),
            int(row["latest_calibration_rows"] or 0),
        )

    st.subheader("시장 현황과 선정 엔진")
    pulse_left, pulse_right = st.columns((1, 2))
    with pulse_left:
        if market_pulse.empty:
            st.info("시장 상태, 수급, 선정 엔진 스크립트를 실행하면 시장 현황이 표시됩니다.")
        else:
            st.dataframe(localize_frame(market_pulse), width="stretch", hide_index=True)
        st.markdown("**최신 수급 커버리지**")
        if latest_flow_summary.empty:
            st.info("수급 요약이 아직 없습니다.")
        else:
            st.dataframe(localize_frame(latest_flow_summary), width="stretch", hide_index=True)
        st.markdown("**최신 프록시 예측 요약**")
        if latest_prediction_summary.empty:
            st.info("프록시 예측 밴드가 아직 없습니다.")
        else:
            st.dataframe(
                localize_frame(latest_prediction_summary),
                width="stretch",
                hide_index=True,
            )
    with pulse_right:
        st.markdown("**Selection 엔진 v2 미리보기 (D+5)**")
        if selection_preview.empty:
            st.info("Selection 엔진 v2 스냅샷이 아직 없습니다.")
        else:
            preview = selection_preview[
                [
                    "symbol",
                    "company_name",
                    "market",
                    "final_selection_value",
                    "final_selection_rank_pct",
                    "grade",
                    "expected_excess_return",
                    "lower_band",
                    "upper_band",
                    "reasons",
                    "risks",
                ]
            ].copy()
            preview["final_selection_rank_pct"] = (
                pd.to_numeric(preview["final_selection_rank_pct"], errors="coerce") * 100.0
            ).round(1)
            st.dataframe(localize_frame(preview), width="stretch", hide_index=True)
        st.markdown("**Selection 엔진 v2 등급 분포 (D+5)**")
        if selection_grades.empty:
            st.info("Selection 엔진 v2 등급 분포가 아직 없습니다.")
        else:
            st.dataframe(localize_frame(selection_grades), width="stretch", hide_index=True)

    coverage_left, coverage_right = st.columns(2)
    with coverage_left:
        st.subheader("피처 커버리지")
        if latest_feature_coverage.empty:
            st.info("피처 스토어를 만들면 피처 커버리지가 표시됩니다.")
        else:
            st.dataframe(localize_frame(latest_feature_coverage), width="stretch", hide_index=True)
    with coverage_right:
        st.subheader("최신 피처 샘플")
        if latest_feature_sample.empty:
            st.info("피처 매트릭스 샘플이 아직 없습니다.")
        else:
            st.dataframe(localize_frame(latest_feature_sample), width="stretch", hide_index=True)

    validation_left, validation_right = st.columns(2)
    with validation_left:
        st.subheader("선정 엔진 검증")
        if selection_validation.empty:
            st.info("선정 엔진 검증 요약이 아직 없습니다.")
        else:
            st.dataframe(localize_frame(selection_validation), width="stretch", hide_index=True)
    with validation_right:
        st.subheader("설명형 순위 검증")
        if explanatory_validation.empty:
            st.info("설명형 순위 검증 요약이 아직 없습니다.")
        else:
            st.dataframe(
                localize_frame(explanatory_validation),
                width="stretch",
                hide_index=True,
            )

    evaluation_left, evaluation_right = st.columns(2)
    with evaluation_left:
        st.subheader("최신 성과")
        if latest_outcomes.empty:
            st.info("평가 완료된 성과 요약이 아직 없습니다.")
        else:
            st.dataframe(localize_frame(latest_outcomes), width="stretch", hide_index=True)
        st.subheader("평가 비교")
        if latest_evaluation_comparison.empty:
            st.info("선정 엔진과 설명형 순위 비교가 아직 없습니다.")
        else:
            st.dataframe(
                localize_frame(latest_evaluation_comparison),
                width="stretch",
                hide_index=True,
            )
    with evaluation_right:
        st.subheader("Rolling 평가 요약")
        if latest_evaluation_summary.empty:
            st.info("평가 요약 행이 아직 없습니다.")
        else:
            st.dataframe(
                localize_frame(latest_evaluation_summary),
                width="stretch",
                hide_index=True,
            )
        st.subheader("보정 진단")
        if latest_calibration.empty:
            st.info("보정 진단이 아직 없습니다.")
        else:
            st.dataframe(localize_frame(latest_calibration), width="stretch", hide_index=True)

    news_left, news_right = st.columns(2)
    with news_left:
        st.subheader("최신 시장 뉴스")
        st.dataframe(localize_frame(latest_market_news), width="stretch", hide_index=True)
    with news_right:
        st.subheader("버전 추적")
        st.dataframe(localize_frame(latest_versions), width="stretch", hide_index=True)

    st.subheader("최신 시장 상태 스냅샷")
    st.dataframe(localize_frame(latest_regime), width="stretch", hide_index=True)

    if discord_preview:
        with st.expander("최신 디스코드 미리보기", expanded=False):
            st.code(discord_preview)

    if postmortem_preview:
        with st.expander("최신 사후 분석 미리보기", expanded=False):
            st.code(postmortem_preview)

    st.subheader("최근 실행 이력")
    if runs.empty:
        st.info("아직 실행 이력이 없습니다. `python scripts/bootstrap.py`를 먼저 실행하세요.")
    else:
        st.dataframe(localize_frame(runs), width="stretch", hide_index=True)

    st.subheader("프로바이더 상태")
    st.dataframe(localize_frame(provider_health), width="stretch", hide_index=True)


st.set_page_config(page_title="StockMaster", page_icon="SM", layout="wide")

navigation = st.navigation(
    [
        st.Page(render_home, title="홈", icon=":material/home:", url_path="home"),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/01_Ops.py",
            title="운영",
            icon=":material/settings:",
            url_path="ops",
        ),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/02_Placeholder_Research.py",
            title="연구",
            icon=":material/science:",
            url_path="research",
        ),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/03_Leaderboard.py",
            title="순위표",
            icon=":material/leaderboard:",
            url_path="leaderboard",
        ),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/04_Market_Pulse.py",
            title="시장 현황",
            icon=":material/monitoring:",
            url_path="market-pulse",
        ),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/05_Stock_Workbench.py",
            title="종목 분석",
            icon=":material/query_stats:",
            url_path="stock-workbench",
        ),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/06_Evaluation.py",
            title="사후 평가",
            icon=":material/fact_check:",
            url_path="evaluation",
        ),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/07_Intraday_Console.py",
            title="장중 콘솔",
            icon=":material/timeline:",
            url_path="intraday-console",
        ),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/08_Portfolio_Studio.py",
            title="포트폴리오 스튜디오",
            icon=":material/account_balance:",
            url_path="portfolio-studio",
        ),
        st.Page(
            PROJECT_ROOT / "app/ui/pages/09_Portfolio_Evaluation.py",
            title="포트폴리오 평가",
            icon=":material/analytics:",
            url_path="portfolio-evaluation",
        ),
    ],
    position="sidebar",
)
navigation.run()
