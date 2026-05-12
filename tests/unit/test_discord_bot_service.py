from __future__ import annotations

import json

import pandas as pd

from app.discord_bot.service import (
    JOB_LABELS,
    STEP_LABELS,
    _next_picks_empty_message,
    _render_next_picks,
    _render_status,
)


def test_render_status_includes_active_jobs() -> None:
    summary = (
        "기준일 2026-03-24 · 추천 기준일 2026-03-23 · "
        "마지막 반영 2026-03-24T01:10:00+09:00"
    )
    rows = pd.DataFrame(
        [
            {
                "summary": summary,
                "payload_json": json.dumps(
                    {"ranking_version": "selection_engine_v2"},
                    ensure_ascii=False,
                ),
            }
        ]
    )
    active_jobs = pd.DataFrame(
        [
            {
                "job_name": "run_daily_close_bundle",
                "as_of_date": "2026-03-23",
                "running_seconds": 7260,
                "step_name": "train_alpha_candidate_models",
                "step_running_seconds": 5400,
            }
        ]
    )

    rendered = _render_status(rows, active_jobs=active_jobs)

    assert "StockMaster 상태" in rendered
    assert "추천 모델 버전 selection_engine_v2" in rendered
    assert "지금 진행 중인 핵심 작업" in rendered
    assert "장마감 v4 종가RR 추천 업데이트" in rendered
    assert "후보 모델 비교 학습" in rendered


def test_v4_status_labels_include_outcome_and_entry_policy_steps() -> None:
    assert JOB_LABELS["run_daily_close_bundle"] == "장마감 v4 종가RR 추천 업데이트"
    assert STEP_LABELS["materialize_selection_outcomes"] == "기존 추천 성과·성숙 라벨 갱신"
    assert (
        STEP_LABELS["materialize_selection_engine_v2"]
        == "v4 종가RR 하이브리드 추천·entry_policy 계산"
    )


def test_render_status_mentions_no_active_jobs() -> None:
    summary = (
        "기준일 2026-03-24 · 추천 기준일 2026-03-23 · "
        "마지막 반영 2026-03-24T01:10:00+09:00"
    )
    rows = pd.DataFrame(
        [
            {
                "summary": summary,
                "payload_json": "{}",
            }
        ]
    )

    rendered = _render_status(rows, active_jobs=pd.DataFrame())

    assert "지금 진행 중인 핵심 작업은 없습니다." in rendered


def test_next_picks_empty_message_distinguishes_v4_no_candidate() -> None:
    assert _next_picks_empty_message(5) == (
        "오늘은 v4 종가RR·entry_policy 기준으로 표시할 5거래일 스윙 후보가 없습니다."
    )
    assert _next_picks_empty_message(1) == "참고용 H1 단기 후보가 아직 없습니다."


def test_render_next_picks_groups_h5_rows_even_without_executable() -> None:
    rows = pd.DataFrame(
        [
            {
                "title": "111111 유효신호",
                "subtitle": "5거래일 보유 기준 · 유효 신호(추격주의/손익비 부족)",
                "summary": "유효 신호(추격주의/손익비 부족) · 현재RR 0.80",
                "payload_json": json.dumps({"message_group": "VALID_SIGNAL"}, ensure_ascii=False),
            },
            {
                "title": "222222 관찰후보",
                "subtitle": "5거래일 보유 기준 · 관찰 후보",
                "summary": "관찰 후보 · 트리거 대기",
                "payload_json": json.dumps({"message_group": "WATCHLIST"}, ensure_ascii=False),
            },
        ]
    )

    rendered = _render_next_picks(
        "내일 종목 추천 · 5거래일 보유 기준 (D+5)",
        rows,
        horizon=5,
        empty_message="없음",
        per_group_limit=5,
    )

    assert "좋은 신호는 있었지만 현재 가격 기준 실제 진입 가능한 종목은 없었습니다." in rendered
    assert "**실제 매수 가능 종목**" in rendered
    assert "**유효 신호(추격주의/손익비 부족)**" in rendered
    assert "**관찰 후보**" in rendered
    assert "111111 유효신호" in rendered
    assert "222222 관찰후보" in rendered
