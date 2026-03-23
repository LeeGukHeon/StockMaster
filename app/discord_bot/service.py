from __future__ import annotations

import json
from typing import Any

from app.discord_bot.live_analysis import render_live_stock_analysis
from app.discord_bot.read_store import (
    fetch_active_job_runs,
    fetch_discord_bot_snapshot_rows,
)
from app.logging import get_logger
from app.settings import Settings

logger = get_logger(__name__)

JOB_LABELS: dict[str, str] = {
    "run_daily_close_bundle": "내일 종목 추천 업데이트",
    "run_evaluation_bundle": "사후 평가 정리",
    "run_news_sync_bundle": "뉴스 반영",
    "run_daily_overlay_refresh_bundle": "장중 정책 갱신",
    "run_weekly_training_bundle": "주간 학습",
    "run_weekly_calibration_bundle": "주간 캘리브레이션",
    "run_weekly_policy_research_bundle": "주간 정책 리서치",
}

STEP_LABELS: dict[str, str] = {
    "daily_pipeline": "추천 데이터 수집과 계산",
    "sync_daily_ohlcv": "일봉 시세 수집",
    "sync_fundamentals_snapshot": "재무 데이터 수집",
    "sync_news_metadata": "뉴스 수집",
    "sync_investor_flow": "수급 데이터 수집",
    "build_feature_store": "특징값 생성",
    "build_market_regime_snapshot": "시장 국면 계산",
    "materialize_explanatory_ranking": "기본 점수 계산",
    "materialize_selection_engine_v1": "기본 추천 계산",
    "train_alpha_model_v1": "기본 추천 모델 학습",
    "train_alpha_candidate_models": "후보 모델 비교 학습",
    "materialize_alpha_shadow_candidates": "후보 모델 비교 점검",
    "run_alpha_auto_promotion": "추천 모델 교체 점검",
    "materialize_alpha_predictions_v1": "예상 수익 계산",
    "materialize_selection_engine_v2": "최종 추천 점수 계산",
    "calibrate_proxy_prediction_bands": "예측 구간 보정",
    "evaluation_pipeline": "사후 평가 계산",
    "render_evaluation_report": "사후 평가 리포트 생성",
    "build_report_index": "리포트 인덱스 갱신",
    "materialize_health_snapshots": "운영 상태 기록",
    "check_pipeline_dependencies": "의존성 점검",
    "materialize_discord_bot_read_store": "봇 조회 스냅샷 갱신",
}


class DiscordBotConfigError(RuntimeError):
    pass


def _render_snapshot_list(
    title: str,
    rows,
    *,
    empty_message: str,
) -> str:
    if rows.empty:
        return empty_message
    lines = [f"**{title}**"]
    for row in rows.itertuples(index=False):
        subtitle = f" ({row.subtitle})" if getattr(row, "subtitle", None) else ""
        lines.append(f"- {row.title}{subtitle}")
        if getattr(row, "summary", None):
            lines.append(f"  {row.summary}")
    return "\n".join(lines)


def _format_running_duration(value: Any) -> str:
    try:
        total_seconds = max(0, int(float(value)))
    except (TypeError, ValueError):
        return "-"
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}시간 {minutes}분"
    if minutes:
        return f"{minutes}분"
    return f"{seconds}초"


def _job_label(value: object) -> str:
    text = str(value or "").strip()
    return JOB_LABELS.get(text, text or "알 수 없는 작업")


def _step_label(value: object) -> str:
    text = str(value or "").strip()
    return STEP_LABELS.get(text, "")


def _render_status(rows, active_jobs=None) -> str:
    if rows.empty:
        return "상태 스냅샷이 아직 준비되지 않았습니다."

    row = rows.iloc[0]
    lines = ["**StockMaster 상태**", row["summary"]]

    payload = row.get("payload_json")
    if isinstance(payload, str) and payload.strip():
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = {}
        ranking_version = parsed.get("ranking_version")
        if ranking_version:
            lines.append(f"추천 모델 버전 {ranking_version}")

    lines.append("")
    if active_jobs is not None and not active_jobs.empty:
        lines.append("**지금 진행 중인 핵심 작업**")
        for item in active_jobs.itertuples(index=False):
            job_name = _job_label(getattr(item, "job_name", ""))
            job_elapsed = _format_running_duration(getattr(item, "running_seconds", None))
            as_of_date = str(getattr(item, "as_of_date", "") or "")
            step_name = _step_label(getattr(item, "step_name", ""))
            step_elapsed = _format_running_duration(getattr(item, "step_running_seconds", None))
            header = f"- {job_name} ({job_elapsed})"
            if as_of_date:
                header += f" · 기준일 {as_of_date}"
            lines.append(header)
            if step_name:
                lines.append(f"  현재 단계: {step_name} ({step_elapsed})")
    else:
        lines.append("지금 진행 중인 핵심 작업은 없습니다.")

    return "\n".join(lines)


def _render_stock_summary(query: str, rows) -> str:
    if rows.empty:
        return f"`{query}` 기준으로 찾은 종목 요약이 없습니다."
    if len(rows) > 1:
        return _render_snapshot_list(
            "종목 후보",
            rows.head(5),
            empty_message=f"`{query}` 기준으로 찾은 종목이 없습니다.",
        )
    row = rows.iloc[0]
    return "\n".join([f"**{row['title']}**", row["summary"]])


def build_discord_bot(settings: Settings):
    try:
        import discord
        from discord import app_commands
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise DiscordBotConfigError(
            "discord.py is not installed. Install dependencies before running the bot."
        ) from exc

    if not settings.discord.bot_enabled:
        raise DiscordBotConfigError("DISCORD_BOT_ENABLED=false")
    if not settings.discord.bot_token:
        raise DiscordBotConfigError("DISCORD_BOT_TOKEN is not configured.")

    globals()["discord"] = discord
    globals()["app_commands"] = app_commands

    intents = discord.Intents.none()

    class StockMasterDiscordBot(discord.Client):
        def __init__(self) -> None:
            super().__init__(
                intents=intents,
                application_id=settings.discord.bot_application_id,
            )
            self.tree = app_commands.CommandTree(self)

        async def setup_hook(self) -> None:
            guild_id = settings.discord.bot_guild_id
            if guild_id:
                guild = discord.Object(id=int(guild_id))
                try:
                    self.tree.copy_global_to(guild=guild)
                    await self.tree.sync(guild=guild)
                    logger.info(
                        "Discord bot guild command sync completed.",
                        extra={"guild_id": int(guild_id)},
                    )
                    return
                except discord.HTTPException as exc:
                    logger.warning(
                        "Discord bot guild sync failed. Falling back to global sync.",
                        extra={"guild_id": int(guild_id), "error": str(exc)},
                    )
            await self.tree.sync()
            logger.info("Discord bot global command sync completed.")

        async def on_ready(self) -> None:
            logger.info(
                "Discord bot connected.",
                extra={
                    "user": str(self.user),
                    "application_id": settings.discord.bot_application_id,
                    "guild_id": settings.discord.bot_guild_id,
                },
            )

    client = StockMasterDiscordBot()

    @client.tree.command(name="상태", description="마지막 반영 시각과 현재 진행 상태를 보여줍니다.")
    async def bot_status(interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True)
        rows = fetch_discord_bot_snapshot_rows(settings, snapshot_type="status", limit=1)
        active_jobs = fetch_active_job_runs(settings, limit=5)
        await interaction.followup.send(_render_status(rows, active_jobs=active_jobs))

    @client.tree.command(name="내일종목추천", description="다음 거래일 기준 상위 후보를 보여줍니다.")
    @app_commands.rename(basis="보유기준", count="개수")
    @app_commands.describe(
        basis="1은 하루 보유 기준, 5는 5거래일 보유 기준입니다.",
        count="보여줄 종목 수입니다.",
    )
    @app_commands.choices(
        basis=[
            app_commands.Choice(name="하루 보유 기준 (D+1)", value=1),
            app_commands.Choice(name="5거래일 보유 기준 (D+5)", value=5),
        ]
    )
    async def next_picks(
        interaction: discord.Interaction,
        basis: app_commands.Choice[int],
        count: app_commands.Range[int, 1, 10] = 5,
    ) -> None:
        await interaction.response.defer(thinking=True)
        rows = fetch_discord_bot_snapshot_rows(
            settings,
            snapshot_type="next_picks",
            horizon=int(basis.value),
            limit=int(count),
        )
        message = _render_snapshot_list(
            f"내일 종목 추천 · {basis.name}",
            rows,
            empty_message="추천 후보 스냅샷이 아직 준비되지 않았습니다.",
        )
        await interaction.followup.send(message)

    @client.tree.command(name="주간보고", description="주간 모델 평가와 정책 요약을 보여줍니다.")
    async def weekly_report(interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True)
        rows = fetch_discord_bot_snapshot_rows(
            settings,
            snapshot_type="weekly_report",
            limit=10,
        )
        message = _render_snapshot_list(
            "주간 보고",
            rows,
            empty_message="주간 보고 스냅샷이 아직 준비되지 않았습니다.",
        )
        await interaction.followup.send(message)

    @client.tree.command(name="종목요약", description="종목명 또는 6자리 코드로 최신 요약을 보여줍니다.")
    @app_commands.rename(query="종목")
    @app_commands.describe(query="종목명 또는 6자리 종목코드를 입력하세요.")
    async def stock_summary(interaction: discord.Interaction, query: str) -> None:
        await interaction.response.defer(thinking=True)
        rows = fetch_discord_bot_snapshot_rows(
            settings,
            snapshot_type="stock_summary",
            query=query,
            limit=5,
        )
        await interaction.followup.send(_render_stock_summary(query, rows))

    @client.tree.command(
        name="즉석종목분석",
        description="최신 시세와 최근 뉴스를 반영한 즉석 분석을 보여줍니다.",
    )
    @app_commands.rename(query="종목")
    @app_commands.describe(query="종목명 또는 6자리 종목코드를 입력하세요.")
    async def live_stock_summary(interaction: discord.Interaction, query: str) -> None:
        await interaction.response.defer(thinking=True)
        message = render_live_stock_analysis(settings, query=query)
        await interaction.followup.send(message)

    return client


def run_discord_bot(settings: Settings) -> None:
    client = build_discord_bot(settings)
    client.run(settings.discord.bot_token, log_handler=None)
