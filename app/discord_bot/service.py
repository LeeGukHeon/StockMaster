from __future__ import annotations

import json

from app.discord_bot.read_store import fetch_discord_bot_snapshot_rows
from app.logging import get_logger
from app.settings import Settings

logger = get_logger(__name__)


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


def _render_status(rows) -> str:
    if rows.empty:
        return "봇 상태 스냅샷이 아직 준비되지 않았습니다."
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

    @client.tree.command(name="상태", description="마지막 반영 시각과 봇 응답 기준을 보여줍니다.")
    async def bot_status(interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True)
        rows = fetch_discord_bot_snapshot_rows(settings, snapshot_type="status", limit=1)
        await interaction.followup.send(_render_status(rows))

    @client.tree.command(name="내일종목추천", description="다음 거래일 상위 후보를 보여줍니다.")
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

    @client.tree.command(name="주간보고", description="주간 모델 점검과 정책 요약을 보여줍니다.")
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

    @client.tree.command(name="종목분석", description="종목명 또는 6자리 코드로 최신 요약을 보여줍니다.")
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

    return client


def run_discord_bot(settings: Settings) -> None:
    client = build_discord_bot(settings)
    client.run(settings.discord.bot_token, log_handler=None)
