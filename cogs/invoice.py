import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from services.s3_service import upload_image
from utils.views import ApprovalView
from utils.logger import get_logger

logger = get_logger(__name__)

class InvoiceCog(commands.Cog):
    """Commands for submitting purchase invoices (plots or mall shops)."""

    _TABLE_PREFIX = {
        'purchase_invoice': 'inv'
    }

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="invoice",
        description="Submit a purchase invoice for plot sales or mall shop purchases"
    )
    @app_commands.describe(
        purchasee_nickname="Discord nickname of the buyer",
        purchasee_ingame="Minecraft username of the buyer",
        purchase_type="Type of purchase: `premium`, `normal`, `staff`, or `mall_shop`",
        amount_deposited="Amount of coins deposited to the town bank",
        num_plots="Number of plots sold (for plot purchases)",
        total_plots="Buyer's total plots after this purchase",
        banner_color="Banner color of the mall shop (for mall shop purchases)",
        shop_number="Shop number in the mall (for mall shop purchases)",
        screenshot1="Screenshot evidence (at least one required)",
        screenshot2="Additional screenshot (optional)",
        screenshot3="Additional screenshot (optional)",
        screenshot4="Additional screenshot (optional)",
        screenshot5="Additional screenshot (optional)"
    )
    async def invoice_submit(
        self,
        interaction: discord.Interaction,
        purchasee_nickname: str,
        purchasee_ingame: str,
        purchase_type: str,
        amount_deposited: float,
        screenshot1: discord.Attachment = None,
        screenshot2: discord.Attachment = None,
        screenshot3: discord.Attachment = None,
        screenshot4: discord.Attachment = None,
        screenshot5: discord.Attachment = None,
        num_plots: int = None,
        total_plots: int = None,
        banner_color: str = None,
        shop_number: int = None
    ):
        await interaction.response.defer()
        try:
            screenshots = [s for s in (screenshot1, screenshot2, screenshot3, screenshot4, screenshot5) if s]
            if not screenshots:
                await interaction.followup.send(
                    "❌ **At least one screenshot is required.** Please attach an image.",
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
                'seller_display': interaction.user.display_name,
                'purchasee_nickname': purchasee_nickname,
                'purchasee_ingame': purchasee_ingame,
                'purchase_type': purchase_type,
                'num_plots': num_plots,
                'total_plots': total_plots,
                'banner_color': banner_color,
                'shop_number': shop_number,
                'amount_deposited': amount_deposited,
                'screenshot_urls': ','.join(screenshot_urls)
            }

            form_id = await DBService.insert_invoice(data)
            logger.info(f"Invoice #{form_id} submitted by {interaction.user.id}")

            form_data = {
                'purchasee_nickname': purchasee_nickname,
                'purchasee_ingame': purchasee_ingame,
                'amount_deposited': amount_deposited,
                'purchase_type': purchase_type,
                'num_plots': num_plots,
                'total_plots': total_plots,
                'banner_color': banner_color,
                'shop_number': shop_number,
                'screenshot_urls': data['screenshot_urls']
            }

            prefix = self._TABLE_PREFIX['purchase_invoice']
            display_id = f"{prefix}_{form_id}"

            confirm_msg = await interaction.followup.send(f"✅ Invoice `{display_id}` submitted - pending approval.")

            config = await DBService.get_guild_config(interaction.guild_id)
            if config and config.get('approval_channel_id'):
                approval_channel = self.bot.get_channel(config['approval_channel_id'])
                if approval_channel:
                    embed = discord.Embed(
                        title="Purchase Invoice",
                        color=discord.Color.gold(),
                        timestamp=discord.utils.utcnow()
                    )
                    embed.add_field(name="Seller", value=interaction.user.display_name, inline=True)
                    embed.add_field(name="Buyer", value=f"{purchasee_nickname} ({purchasee_ingame})", inline=True)
                    embed.add_field(name="Type", value=purchase_type, inline=True)
                    embed.add_field(name="Amount", value=f"{amount_deposited} coins", inline=True)

                    if num_plots:
                        embed.add_field(
                            name="Plots",
                            value=f"{num_plots} (total after: {total_plots})",
                            inline=True
                        )
                    if banner_color:
                        embed.add_field(
                            name="Mall Shop",
                            value=f"Color: {banner_color} · #{shop_number}",
                            inline=True
                        )
                    if screenshot_urls:
                        embed.set_image(url=screenshot_urls[0])
                        if len(screenshot_urls) > 1:
                            embed.add_field(name="Additional Screenshots", value=f"{len(screenshot_urls)-1} more", inline=False)

                    embed.set_footer(text=f"Form ID: {display_id}")

                    view = ApprovalView(
                        table='purchase_invoice',
                        form_id=form_id,
                        form_type='purchase_invoice',
                        submitter_id=interaction.user.id,
                        guild_id=interaction.guild_id,
                        channel_config_key='invoice_channel_id',
                        thread_prefix="Invoices",
                        confirmation_msg_id=confirm_msg.id,
                        confirmation_channel_id=interaction.channel_id,
                        form_data=form_data
                    )
                    msg = await approval_channel.send(embed=embed, view=view)
                    await DBService.set_approval_message_id('purchase_invoice', form_id, msg.id)

        except Exception as e:
            logger.exception(f"Error in invoice_submit: {e}")
            await interaction.followup.send(
                "❌ An error occurred while submitting the invoice. Please try again later.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(InvoiceCog(bot))