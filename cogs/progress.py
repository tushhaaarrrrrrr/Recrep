import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from services.s3_service import upload_image
from utils.views import ApprovalView
from utils.logger import get_logger

logger = get_logger(__name__)

class ProgressCog(commands.Cog):
    """Commands for submitting progress reports on building projects."""

    _TABLE_PREFIX = {
        'progress_report': 'rep'
    }

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="report_progress",
        description="Submit a progress report for a building project"
    )
    @app_commands.describe(
        project_name="Name of the project (e.g., 'Town Hall', 'Mob Farm')",
        time_spent="Time spent on the project (e.g., '2 hours', '30 minutes')",
        helper="Optional: mention a user who helped (they will also receive reputation)",
        screenshot1="Screenshot evidence (at least one required)",
        screenshot2="Additional screenshot (optional)",
        screenshot3="Additional screenshot (optional)",
        screenshot4="Additional screenshot (optional)",
        screenshot5="Additional screenshot (optional)"
    )
    async def progress_submit(
        self,
        interaction: discord.Interaction,
        project_name: str,
        time_spent: str,
        helper: str = None,
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
                'helper_mentions': helper,
                'project_name': project_name,
                'time_spent': time_spent,
                'screenshot_urls': ','.join(screenshot_urls)
            }

            form_id = await DBService.insert_progress(data)
            logger.info(f"Progress report #{form_id} submitted by {interaction.user.id}")

            form_data = {
                'project_name': project_name,
                'time_spent': time_spent,
                'helper_mentions': helper,
                'screenshot_urls': data['screenshot_urls']
            }

            prefix = self._TABLE_PREFIX['progress_report']
            display_id = f"{prefix}_{form_id}"

            confirm_msg = await interaction.followup.send(f"✅ Progress report `{display_id}` submitted – pending approval.")

            config = await DBService.get_guild_config(interaction.guild_id)
            if config and config.get('approval_channel_id'):
                approval_channel = self.bot.get_channel(config['approval_channel_id'])
                if approval_channel:
                    embed = discord.Embed(
                        title="Progress Report",
                        color=discord.Color.green(),
                        timestamp=discord.utils.utcnow()
                    )
                    embed.add_field(name="Builder", value=interaction.user.display_name, inline=True)
                    embed.add_field(name="Project", value=project_name, inline=True)
                    embed.add_field(name="Time Spent", value=time_spent, inline=True)
                    if helper:
                        embed.add_field(name="Helper", value=helper, inline=True)

                    if screenshot_urls:
                        embed.set_image(url=screenshot_urls[0])
                        if len(screenshot_urls) > 1:
                            embed.add_field(name="Additional Screenshots", value=f"{len(screenshot_urls)-1} more", inline=False)

                    embed.set_footer(text=f"Form ID: {display_id}")

                    view = ApprovalView(
                        table='progress_report',
                        form_id=form_id,
                        form_type='progress_report',
                        submitter_id=interaction.user.id,
                        guild_id=interaction.guild_id,
                        channel_config_key='progress_channel_id',
                        thread_prefix="Progress Reports",
                        confirmation_msg_id=confirm_msg.id,
                        confirmation_channel_id=interaction.channel_id,
                        form_data=form_data
                    )
                    msg = await approval_channel.send(embed=embed, view=view)
                    await DBService.set_approval_message_id('progress_report', form_id, msg.id)

        except Exception as e:
            logger.exception(f"Error in progress_submit: {e}")
            await interaction.followup.send(
                "❌ An error occurred while submitting the progress report. Please try again later.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(ProgressCog(bot))