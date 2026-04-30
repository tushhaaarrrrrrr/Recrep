import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from services.s3_service import upload_image
from utils.views import ApprovalView
from utils.logger import get_logger

logger = get_logger(__name__)

class RecruitmentCog(commands.Cog):
    """Commands for submitting recruitment logs."""

    _TABLE_PREFIX = {
        'recruitment': 'rec'
    }

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="recruitment",
        description="Log a new player recruitment into the town"
    )
    @app_commands.describe(
        ingame_username="Minecraft username of the new player",
        nickname="Discord nickname of the new player",
        discord_username="Discord username (optional, e.g., `@player`)",
        age="Player's age (optional)",
        plots="Number of plots given (default: 2)",
        screenshot1="Screenshot evidence (optional)",
        screenshot2="Additional screenshot (optional)",
        screenshot3="Additional screenshot (optional)",
        screenshot4="Additional screenshot (optional)",
        screenshot5="Additional screenshot (optional)"
    )
    async def recruitment_add(
        self,
        interaction: discord.Interaction,
        ingame_username: str,
        nickname: str,
        discord_username: str = None,
        age: str = None,
        plots: int = 2,
        screenshot1: discord.Attachment = None,
        screenshot2: discord.Attachment = None,
        screenshot3: discord.Attachment = None,
        screenshot4: discord.Attachment = None,
        screenshot5: discord.Attachment = None
    ):
        try:
            await interaction.response.defer()
        except (discord.NotFound, discord.HTTPException):
            return

        try:
            screenshots = [s for s in (screenshot1, screenshot2, screenshot3, screenshot4, screenshot5) if s]
            screenshot_urls = []
            for img in screenshots:
                url = await upload_image(await img.read(), img.filename)
                screenshot_urls.append(url)

            data = {
                'submitted_by': interaction.user.id,
                'submitter_display': interaction.user.display_name,
                'ingame_username': ingame_username,
                'discord_username': discord_username,
                'age': age,
                'nickname': nickname,
                'recruiter_display': interaction.user.display_name,
                'plots': plots,
                'screenshot_urls': ','.join(screenshot_urls) if screenshot_urls else None
            }

            form_id = await DBService.insert_recruitment(data)
            logger.info(f"Recruitment form #{form_id} submitted by {interaction.user.id}")

            form_data = {
                'ingame_username': ingame_username,
                'nickname': nickname,
                'plots': plots,
                'discord_username': discord_username,
                'age': age,
                'screenshot_urls': data['screenshot_urls']
            }

            prefix = self._TABLE_PREFIX['recruitment']
            display_id = f"{prefix}_{form_id}"

            confirm_msg = await interaction.followup.send(f"✅ Recruitment `{display_id}` logged - pending approval.")

            config = await DBService.get_guild_config(interaction.guild_id)
            if config and config.get('approval_channel_id'):
                approval_channel = self.bot.get_channel(config['approval_channel_id'])
                if approval_channel:
                    embed = discord.Embed(
                        title="Recruitment Log",
                        color=discord.Color.blue(),
                        timestamp=discord.utils.utcnow()
                    )
                    embed.add_field(name="Recruiter", value=interaction.user.display_name, inline=True)
                    embed.add_field(name="New Player", value=f"{nickname} ({ingame_username})", inline=True)
                    embed.add_field(name="Plots", value=str(plots), inline=True)
                    if discord_username:
                        embed.add_field(name="Discord", value=discord_username, inline=True)
                    if age:
                        embed.add_field(name="Age", value=age, inline=True)
                    if screenshot_urls:
                        embed.set_image(url=screenshot_urls[0])
                        if len(screenshot_urls) > 1:
                            embed.add_field(name="Additional Screenshots", value=f"{len(screenshot_urls)-1} more", inline=False)
                    embed.set_footer(text=f"Form ID: {display_id}")

                    view = ApprovalView(
                        table='recruitment',
                        form_id=form_id,
                        form_type='recruitment',
                        submitter_id=interaction.user.id,
                        guild_id=interaction.guild_id,
                        channel_config_key='recruitment_channel_id',
                        thread_prefix="Recruitments",
                        confirmation_msg_id=confirm_msg.id,
                        confirmation_channel_id=interaction.channel_id,
                        form_data=form_data
                    )
                    msg = await approval_channel.send(embed=embed, view=view)
                    await DBService.set_approval_message_id('recruitment', form_id, msg.id)

                    # Persist confirmation message IDs
                    await DBService.execute(
                        "UPDATE recruitment SET confirmation_msg_id = $1, confirmation_channel_id = $2 WHERE id = $3",
                        confirm_msg.id, interaction.channel_id, form_id
                    )

        except Exception as e:
            logger.exception(f"Error in recruitment_add: {e}")
            await interaction.followup.send("❌ An error occurred. Please try again later.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(RecruitmentCog(bot))