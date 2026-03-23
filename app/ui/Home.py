# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ui.components import inject_app_styles


def render_today_page() -> None:
    inject_app_styles()
    st.set_page_config(page_title="StockMaster", page_icon="SM", layout="wide")
    st.markdown(
        """
        <section class="sm-hero">
          <div class="sm-hero-copy">
            <div class="sm-hero-kicker">Dashboard Retired</div>
            <h1 class="sm-hero-title">대시보드는 종료됐고, 이제 Discord bot이 기본 화면입니다.</h1>
            <p class="sm-hero-body">
              학습/배치 중 충돌을 줄이기 위해 사용자 조회 경로를 Discord bot으로 옮겼습니다.
              아래 명령어로 추천, 주간 보고, 종목 요약을 조회하세요.
            </p>
          </div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("### Discord 명령어")
    st.markdown(
        "\n".join(
            [
                "- `/상태` : 마지막 반영 시각과 현재 기준 상태 확인",
                "- `/내일종목추천` : 하루 보유(D+1) / 5거래일 보유(D+5) 후보 조회",
                "- `/주간보고` : 주간 모델 점검과 정책 요약 조회",
                "- `/종목분석 종목명` : 최신 안정 스냅샷 기준 종목 요약",
                "- `/실시간종목분석 종목명` : 최신 시세와 뉴스까지 포함한 실시간 분석",
            ]
        )
    )
    st.info(
        "Discord bot이 기본 사용자 인터페이스입니다. Streamlit은 문서/안내용으로만 유지됩니다."
    )
    st.caption("대시보드 데이터 화면 종료 · Discord bot 전환 진행 중")


render_today_page()
