from __future__ import annotations

import json

import pandas as pd

from app.discord_bot.service import _render_status


def test_render_status_includes_active_jobs() -> None:
    rows = pd.DataFrame(
        [
            {
                "summary": "기준일 2026-03-24 · 추천 기준일 2026-03-23 · 마지막 반영 2026-03-24T01:10:00+09:00",
                "payload_json": json.dumps({"ranking_version": "selection_engine_v2"}, ensure_ascii=False),
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
    assert "내일 종목 추천 업데이트" in rendered
    assert "후보 모델 비교 학습" in rendered


def test_render_status_mentions_no_active_jobs() -> None:
    rows = pd.DataFrame(
        [
            {
                "summary": "기준일 2026-03-24 · 추천 기준일 2026-03-23 · 마지막 반영 2026-03-24T01:10:00+09:00",
                "payload_json": "{}",
            }
        ]
    )

    rendered = _render_status(rows, active_jobs=pd.DataFrame())

    assert "지금 진행 중인 핵심 작업은 없습니다." in rendered
