import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from services.s3_service import upload_image
from utils.views import ApprovalView
from utils.logger import get_logger

logger = get_logger(__name__)

class EvictionCog(commands.Cog):
    """Commands for submitting eviction reports."""

    _TABLE_PREFIX = {
        'eviction_report': 'evc'
    }

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="report_eviction",
        description="Submit an eviction report when a player is evicted due to inactivity or rule violation"
    )
    @app_commands.describe(
        ingame_owner="Minecraft username of the plot owner being evicted",
        items_stored="Were items moved to town storage? `yes` or `no`",
        inactivity_period="How long the player has been inactive (e.g., `3 months`, `45 days`)",
        screenshot1="Screenshot evidence (at least one required)",
        screenshot2="Additional screenshot (optional)",
        screenshot3="Additional screenshot (optional)",
        screenshot4="Additional screenshot (optional)",
        screenshot5="Additional screenshot (optional)"
    )
    async def eviction_submit(
        self,
        interaction: discord.Interaction,
        ingame_owner: str,
        items_stored: str,
        inactivity_period: str,
        screenshot1: discord.Attachment = None,
        screenshot2: discord.Attachment = None,
        screenshot3: discord.Attachment = None,
        screenshot4: discord.Attachment = None,
        screenshot5: discord.Attachment = None
    ):
        await interaction.response.defer()
        try:
            screenshots = [s for s in (screenshot1, screenshot2, screenshot3, screenshot4, screenshot5) if s]
            if not screenshots:
                await interaction.followup.send(
                    "❌ At least one screenshot is required. Please attach an image.",
                    ephemeral=True
                )
                return

            screenshot_urls = []
            for img in screenshots:
                img_bytes = await img.read()
                url = await upload_image(img_bytes, img.filename)
                screenshot_urls.append(url)

            data = {
                'submitted_by': interaction.user.id,
                'submitter_display': interaction.user.display_name,
                'ingame_owner': ingame_owner,
                'items_stored': items_stored.lower() == 'yes',
                'inactivity_period': inactivity_period,
                'screenshot_urls': ','.join(screenshot_urls)
            }

            form_id = await DBService.insert_eviction(data)
            logger.info(f"Eviction report #{form_id} submitted by {interaction.user.id}")

            form_data = {
                'ingame_owner': ingame_owner,
                'items_stored': items_stored.lower() == 'yes',
                'inactivity_period': inactivity_period,
                'screenshot_urls': data['screenshot_urls']
            }

            prefix = self._TABLE_PREFIX['eviction_report']
            display_id = f"{prefix}_{form_id}"

            confirm_msg = await interaction.followup.send(f"✅ Eviction report `{display_id}` submitted – pending approval.")

            config = await DBService.get_guild_config(interaction.guild_id)
            if config and config.get('approval_channel_id'):
                approval_channel = self.bot.get_channel(config['approval_channel_id'])
                if approval_channel:
                    embed = discord.Embed(
                        title="Eviction Report",
                        color=discord.Color.dark_red(),
                        timestamp=discord.utils.utcnow()
                    )
                    embed.add_field(name="Submitter", value=interaction.user.display_name, inline=True)
                    embed.add_field(name="Owner", value=ingame_owner, inline=True)
                    embed.add_field(name="Items Stored", value=items_stored, inline=True)
                    embed.add_field(name="Inactivity Period", value=inactivity_period, inline=True)

                    if screenshot_urls:
                        embed.set_image(url=screenshot_urls[0])
                        if len(screenshot_urls) > 1:
                            embed.add_field(name="Additional Screenshots", value=f"{len(screenshot_urls)-1} more", inline=False)

                    embed.set_footer(text=f"Form ID: {display_id}")

                    view = ApprovalView(
                        table='eviction_report',
                        form_id=form_id,
                        form_type='eviction_report',
                        submitter_id=interaction.user.id,
                        guild_id=interaction.guild_id,
                        channel_config_key='eviction_channel_id',
                        thread_prefix="Evictions",
                        confirmation_msg_id=confirm_msg.id,
                        confirmation_channel_id=interaction.channel_id,
                        form_data=form_data
                    )
                    msg = await approval_channel.send(embed=embed, view=view)
                    await DBService.set_approval_message_id('eviction_report', form_id, msg.id)

        except Exception as e:
            logger.exception(f"Error in eviction_submit: {e}")
            await interaction.followup.send(
                "❌ An error occurred while submitting the eviction report. Please try again later.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(EvictionCog(bot))