import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from services.s3_service import upload_image
from utils.views import ApprovalView
from utils.logger import get_logger

logger = get_logger(__name__)

class ScrollCog(commands.Cog):
    """Commands for submitting scroll completion reports."""

    _VALID_SCROLL_TYPES = ['common', 'special', 'epic', 'mythic', 'legendary', 'mystery', 'spawn_egg']
    _TABLE_PREFIX = {
        'scroll_completion': 'scr'
    }

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="report_scroll",
        description="Submit a scroll completion report"
    )
    @app_commands.describe(
        scroll_type="Type of scroll completed: common, special, epic, mythic, legendary, mystery, spawn_egg",
        items_stored="Were the rewards stored in town storage? (`yes` or `no`)",
        screenshot1="Screenshot evidence (at least one required)",
        screenshot2="Additional screenshot (optional)",
        screenshot3="Additional screenshot (optional)",
        screenshot4="Additional screenshot (optional)",
        screenshot5="Additional screenshot (optional)"
    )
    async def scroll_submit(
        self,
        interaction: discord.Interaction,
        scroll_type: str,
        items_stored: str,
        screenshot1: discord.Attachment,
        screenshot2: discord.Attachment = None,
        screenshot3: discord.Attachment = None,
        screenshot4: discord.Attachment = None,
        screenshot5: discord.Attachment = None
    ):
        await interaction.response.defer()
        try:
            scroll_type_lower = scroll_type.lower()
            if scroll_type_lower not in self._VALID_SCROLL_TYPES:
                valid_list = ", ".join(self._VALID_SCROLL_TYPES)
                await interaction.followup.send(
                    f"❌ Invalid scroll type.\nValid options: `{valid_list}`",
                    ephemeral=True
                )
                return

            screenshots = [screenshot1] + [s for s in (screenshot2, screenshot3, screenshot4, screenshot5) if s]
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
                'scroll_type': scroll_type_lower,
                'items_stored': items_stored.lower() == 'yes',
                'screenshot_urls': ','.join(screenshot_urls)
            }

            form_id = await DBService.insert_scroll(data)
            logger.info(f"Scroll completion report #{form_id} submitted by {interaction.user.id}")

            form_data = {
                'scroll_type': scroll_type_lower,
                'items_stored': items_stored.lower() == 'yes',
                'screenshot_urls': data['screenshot_urls']
            }

            prefix = self._TABLE_PREFIX['scroll_completion']
            display_id = f"{prefix}_{form_id}"

            confirm_msg = await interaction.followup.send(f"✅ Scroll report `{display_id}` submitted – pending approval.")

            config = await DBService.get_guild_config(interaction.guild_id)
            if config and config.get('approval_channel_id'):
                approval_channel = self.bot.get_channel(config['approval_channel_id'])
                if approval_channel:
                    embed = discord.Embed(
                        title="Scroll Completion Report",
                        color=discord.Color.purple(),
                        timestamp=discord.utils.utcnow()
                    )
                    embed.add_field(name="Submitter", value=interaction.user.display_name, inline=True)
                    embed.add_field(name="Scroll Type", value=scroll_type.capitalize(), inline=True)
                    embed.add_field(name="Items Stored", value="Yes" if items_stored.lower() == 'yes' else "No", inline=True)

                    if screenshot_urls:
                        embed.set_image(url=screenshot_urls[0])
                        if len(screenshot_urls) > 1:
                            embed.add_field(name="Additional Screenshots", value=f"{len(screenshot_urls)-1} more", inline=False)

                    embed.set_footer(text=f"Form ID: {display_id}")

                    view = ApprovalView(
                        table='scroll_completion',
                        form_id=form_id,
                        form_type='scroll_completion',
                        submitter_id=interaction.user.id,
                        guild_id=interaction.guild_id,
                        channel_config_key='scroll_channel_id',
                        thread_prefix="Scrolls",
                        confirmation_msg_id=confirm_msg.id,
                        confirmation_channel_id=interaction.channel_id,
                        form_data=form_data
                    )
                    msg = await approval_channel.send(embed=embed, view=view)
                    await DBService.set_approval_message_id('scroll_completion', form_id, msg.id)

        except Exception as e:
            logger.exception(f"Error in scroll_submit: {e}")
            await interaction.followup.send(
                "❌ An error occurred while submitting the scroll report. Please try again later.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(ScrollCog(bot))