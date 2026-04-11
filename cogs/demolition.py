import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from services.s3_service import upload_image
from utils.views import ApprovalView
from utils.logger import get_logger

logger = get_logger(__name__)

class DemolitionCog(commands.Cog):
    """Commands for submitting demolition reports and admin demolition requests."""

    _TABLE_PREFIX = {
        'demolition_report': 'dem',
        'demolition_request': 'dmr'
    }

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="report_demolition",
        description="Submit a demolition report after removing a player's plots"
    )
    @app_commands.describe(
        ingame_username="Minecraft username of the player whose plots were demolished",
        removed="Status: `yes` (removed) or `tbd`",
        stashed_items="Were items moved to town storage? `yes` or `no`",
        screenshot1="Screenshot evidence (at least one required)",
        screenshot2="Additional screenshot (optional)",
        screenshot3="Additional screenshot (optional)",
        screenshot4="Additional screenshot (optional)",
        screenshot5="Additional screenshot (optional)"
    )
    async def demolition_submit(
        self,
        interaction: discord.Interaction,
        ingame_username: str,
        removed: str,
        stashed_items: str,
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
                'ingame_username': ingame_username,
                'removed': removed,
                'stashed_items': stashed_items.lower() == 'yes',
                'screenshot_urls': ','.join(screenshot_urls)
            }

            form_id = await DBService.insert_demolition(data)
            logger.info(f"Demolition report #{form_id} submitted by {interaction.user.id}")

            form_data = {
                'ingame_username': ingame_username,
                'removed': removed,
                'stashed_items': stashed_items.lower() == 'yes',
                'screenshot_urls': data['screenshot_urls']
            }

            prefix = self._TABLE_PREFIX['demolition_report']
            display_id = f"{prefix}_{form_id}"

            confirm_msg = await interaction.followup.send(f"✅ Demolition report `{display_id}` submitted – pending approval.")

            config = await DBService.get_guild_config(interaction.guild_id)
            if config and config.get('approval_channel_id'):
                approval_channel = self.bot.get_channel(config['approval_channel_id'])
                if approval_channel:
                    embed = discord.Embed(
                        title="Demolition Report",
                        color=discord.Color.red(),
                        timestamp=discord.utils.utcnow()
                    )
                    embed.add_field(name="Submitter", value=interaction.user.display_name, inline=True)
                    embed.add_field(name="Player", value=ingame_username, inline=True)
                    embed.add_field(name="Removed", value=removed, inline=True)
                    embed.add_field(name="Items Stashed", value=stashed_items, inline=True)

                    if screenshot_urls:
                        embed.set_image(url=screenshot_urls[0])
                        if len(screenshot_urls) > 1:
                            embed.add_field(name="Additional Screenshots", value=f"{len(screenshot_urls)-1} more", inline=False)

                    embed.set_footer(text=f"Form ID: {display_id}")

                    view = ApprovalView(
                        table='demolition_report',
                        form_id=form_id,
                        form_type='demolition_report',
                        submitter_id=interaction.user.id,
                        guild_id=interaction.guild_id,
                        channel_config_key='demolition_channel_id',
                        thread_prefix="Demolitions",
                        confirmation_msg_id=confirm_msg.id,
                        confirmation_channel_id=interaction.channel_id,
                        form_data=form_data
                    )
                    msg = await approval_channel.send(embed=embed, view=view)
                    await DBService.set_approval_message_id('demolition_report', form_id, msg.id)

        except Exception as e:
            logger.exception(f"Error in demolition_submit: {e}")
            await interaction.followup.send(
                "❌ An error occurred while submitting the report. Please try again later.",
                ephemeral=True
            )

    @app_commands.command(
        name="request_demolition",
        description="[Admin] Request demolition of a player's plots"
    )
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(
        ingame_username="Minecraft username of the player whose plots should be demolished",
        reason="Reason for the demolition request (e.g., inactivity, violation)",
        screenshot1="Screenshot evidence (at least one required)",
        screenshot2="Additional screenshot (optional)",
        screenshot3="Additional screenshot (optional)",
        screenshot4="Additional screenshot (optional)",
        screenshot5="Additional screenshot (optional)"
    )
    async def demolition_request(
        self,
        interaction: discord.Interaction,
        ingame_username: str,
        reason: str,
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
                'ingame_username': ingame_username,
                'reason': reason,
                'screenshot_urls': ','.join(screenshot_urls),
                'status': 'pending'
            }

            form_id = await DBService.insert_demolition_request(data)
            logger.info(f"Demolition request #{form_id} submitted by admin {interaction.user.id}")

            form_data = {
                'ingame_username': ingame_username,
                'reason': reason,
                'screenshot_urls': data['screenshot_urls']
            }

            prefix = self._TABLE_PREFIX['demolition_request']
            display_id = f"{prefix}_{form_id}"

            confirm_msg = await interaction.followup.send(f"📢 Demolition request `{display_id}` submitted – pending admin review.")

            config = await DBService.get_guild_config(interaction.guild_id)
            if config and config.get('approval_channel_id'):
                approval_channel = self.bot.get_channel(config['approval_channel_id'])
                if approval_channel:
                    embed = discord.Embed(
                        title="Demolition Request (Admin)",
                        color=discord.Color.orange(),
                        timestamp=discord.utils.utcnow()
                    )
                    embed.add_field(name="Requested by", value=interaction.user.display_name, inline=True)
                    embed.add_field(name="Target Player", value=ingame_username, inline=True)
                    embed.add_field(name="Reason", value=reason, inline=False)

                    if screenshot_urls:
                        embed.set_image(url=screenshot_urls[0])
                        if len(screenshot_urls) > 1:
                            embed.add_field(name="Additional Screenshots", value=f"{len(screenshot_urls)-1} more", inline=False)

                    embed.set_footer(text=f"Request ID: {display_id}")

                    view = ApprovalView(
                        table='demolition_request',
                        form_id=form_id,
                        form_type='demolition_request',
                        submitter_id=interaction.user.id,
                        guild_id=interaction.guild_id,
                        channel_config_key='demolition_channel_id',
                        thread_prefix="Demolition Requests",
                        confirmation_msg_id=confirm_msg.id,
                        confirmation_channel_id=interaction.channel_id,
                        form_data=form_data
                    )
                    msg = await approval_channel.send(embed=embed, view=view)
                    await DBService.set_approval_message_id('demolition_request', form_id, msg.id)

        except Exception as e:
            logger.exception(f"Error in demolition_request: {e}")
            await interaction.followup.send(
                "❌ An error occurred while submitting the request. Please try again later.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(DemolitionCog(bot))