import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from utils.logger import get_logger

logger = get_logger(__name__)


class FormEditCog(commands.Cog):
    """Allows submitters to edit their pending or held forms."""

    def __init__(self, bot):
        self.bot = bot

    VALID_TABLES = [
        'recruitment', 'progress_report', 'purchase_invoice',
        'demolition_report', 'demolition_request', 'eviction_report',
        'scroll_completion'
    ]

    TABLE_PREFIX = {
        'recruitment': 'rec',
        'progress_report': 'rep',
        'purchase_invoice': 'inv',
        'demolition_report': 'dem',
        'demolition_request': 'dmr',
        'eviction_report': 'evc',
        'scroll_completion': 'scr'
    }

    ALLOWED_FIELDS = {
        'recruitment': {
            'ingame_username': 'In-game Username',
            'discord_username': 'Discord Username',
            'age': 'Age',
            'nickname': 'Nickname',
            'plots': 'Plots'
        },
        'progress_report': {
            'project_name': 'Project Name',
            'time_spent': 'Time Spent',
            'helper_mentions': 'Helper'
        },
        'purchase_invoice': {
            'purchasee_nickname': 'Buyer Nickname',
            'purchasee_ingame': 'Buyer In-game',
            'purchase_type': 'Purchase Type',
            'amount_deposited': 'Amount Deposited',
            'num_plots': 'Number of Plots',
            'total_plots': 'Total Plots',
            'banner_color': 'Banner Color',
            'shop_number': 'Shop Number'
        },
        'demolition_report': {
            'ingame_username': 'Player',
            'removed': 'Removed',
            'stashed_items': 'Items Stashed'
        },
        'demolition_request': {
            'ingame_username': 'Player',
            'reason': 'Reason'
        },
        'eviction_report': {
            'ingame_owner': 'Owner',
            'items_stored': 'Items Stored',
            'inactivity_period': 'Inactivity Period'
        },
        'scroll_completion': {
            'scroll_type': 'Scroll Type',
            'items_stored': 'Items Stored'
        }
    }

    @app_commands.command(name="form", description="Edit a pending or held form")
    @app_commands.describe(
        form_id="Form ID with prefix (e.g., rec_1, rep_2, inv_3)",
        field="Field to edit (see list of valid fields)",
        value="New value for the field"
    )
    async def form_edit(
        self,
        interaction: discord.Interaction,
        form_id: str,
        field: str,
        value: str
    ):
        """Edit a specific field of a pending or held form."""
        await interaction.response.defer(ephemeral=True)

        if '_' not in form_id:
            await interaction.followup.send(
                "❌ Invalid form ID format. Use format like `rec_1`, `rep_2`, `inv_3`, etc.",
                ephemeral=True
            )
            return
        prefix, num_part = form_id.split('_', 1)
        try:
            numeric_id = int(num_part)
        except ValueError:
            await interaction.followup.send(
                "❌ Invalid numeric ID in form ID.",
                ephemeral=True
            )
            return

        table = None
        for t, p in self.TABLE_PREFIX.items():
            if p == prefix:
                table = t
                break
        if not table:
            await interaction.followup.send(
                f"❌ Unknown prefix `{prefix}`. Valid prefixes: {', '.join(self.TABLE_PREFIX.values())}",
                ephemeral=True
            )
            return

        form_info = await DBService.get_form_by_id(table, numeric_id)
        if not form_info:
            await interaction.followup.send("❌ Form not found.", ephemeral=True)
            return
        status, submitter_id = form_info
        if interaction.user.id != submitter_id:
            await interaction.followup.send("❌ You can only edit your own forms.", ephemeral=True)
            return
        if status not in ('pending', 'hold'):
            await interaction.followup.send("❌ This form cannot be edited (already approved/denied).", ephemeral=True)
            return

        allowed = self.ALLOWED_FIELDS.get(table, {})
        if field not in allowed:
            valid_fields = ", ".join(allowed.keys())
            await interaction.followup.send(
                f"❌ Invalid field for this form type.\nValid fields: `{valid_fields}`",
                ephemeral=True
            )
            return

        original_value = value
        if field in ('stashed_items', 'items_stored'):
            if value.lower() not in ('yes', 'no'):
                await interaction.followup.send("❌ Value must be `yes` or `no`.", ephemeral=True)
                return
            value = value.lower() == 'yes'
        elif field in ('plots', 'num_plots', 'total_plots', 'shop_number'):
            try:
                value = int(value)
            except ValueError:
                await interaction.followup.send("❌ Value must be a number.", ephemeral=True)
                return
        elif field == 'amount_deposited':
            try:
                value = float(value)
            except ValueError:
                await interaction.followup.send("❌ Value must be a number.", ephemeral=True)
                return

        await DBService.update_form_field(table, numeric_id, field, value)
        logger.info(f"User {interaction.user.id} edited {table}#{numeric_id}: {field}={value}")

        await self._refresh_approval_embed(interaction.guild, table, numeric_id)

        await interaction.followup.send(
            f"✅ **Form `{form_id}` updated.** Field `{field}` changed from `{original_value}` to `{value}`.",
            ephemeral=True
        )

    async def _refresh_approval_embed(self, guild: discord.Guild, table: str, form_id: int):
        config = await DBService.get_guild_config(guild.id)
        if not config or not config.get('approval_channel_id'):
            logger.warning(f"No approval channel configured for guild {guild.id}")
            return

        approval_channel = guild.get_channel(config['approval_channel_id'])
        if not approval_channel:
            logger.warning(f"Approval channel not found for guild {guild.id}")
            return

        message_id = await DBService.get_approval_message_id(table, form_id)
        if not message_id:
            logger.warning(f"No approval message ID stored for {table}#{form_id}")
            return

        try:
            message = await approval_channel.fetch_message(message_id)
        except discord.NotFound:
            logger.warning(f"Approval message {message_id} not found (may have been deleted)")
            return

        form_data = await DBService.get_full_form_data(table, form_id)
        if not form_data:
            logger.error(f"Form data missing for {table}#{form_id}")
            return

        embed = self._build_embed(table, form_data, form_id)
        await message.edit(embed=embed)

    def _build_embed(self, table: str, form_data: dict, form_id: int) -> discord.Embed:
        title_map = {
            'recruitment': 'Recruitment Log',
            'progress_report': 'Progress Report',
            'purchase_invoice': 'Purchase Invoice',
            'demolition_report': 'Demolition Report',
            'demolition_request': 'Demolition Request (Admin)',
            'eviction_report': 'Eviction Report',
            'scroll_completion': 'Scroll Completion Report'
        }
        color_map = {
            'recruitment': discord.Color.blue(),
            'progress_report': discord.Color.green(),
            'purchase_invoice': discord.Color.gold(),
            'demolition_report': discord.Color.red(),
            'demolition_request': discord.Color.orange(),
            'eviction_report': discord.Color.dark_red(),
            'scroll_completion': discord.Color.purple()
        }
        title = title_map.get(table, 'Form')
        color = color_map.get(table, discord.Color.blue())
        embed = discord.Embed(title=title, color=color, timestamp=discord.utils.utcnow())

        prefix = self.TABLE_PREFIX.get(table, 'unk')
        display_id = f"{prefix}_{form_id}"

        embed.add_field(name="Submitter", value=f"<@{form_data['submitted_by']}>", inline=True)
        embed.add_field(name="Form ID", value=display_id, inline=True)

        if table == 'recruitment':
            embed.add_field(name="New Player", value=f"{form_data['nickname']} ({form_data['ingame_username']})", inline=False)
            embed.add_field(name="Plots", value=form_data['plots'], inline=True)
            if form_data.get('discord_username'):
                embed.add_field(name="Discord", value=form_data['discord_username'], inline=True)
            if form_data.get('age'):
                embed.add_field(name="Age", value=form_data['age'], inline=True)
        elif table == 'progress_report':
            embed.add_field(name="Builder", value=f"<@{form_data['submitted_by']}>", inline=True)
            embed.add_field(name="Project", value=form_data['project_name'], inline=True)
            embed.add_field(name="Time Spent", value=form_data['time_spent'], inline=True)
            if form_data.get('helper_mentions'):
                embed.add_field(name="Helper", value=form_data['helper_mentions'], inline=True)
        elif table == 'purchase_invoice':
            embed.add_field(name="Seller", value=form_data['seller_display'], inline=True)
            embed.add_field(name="Buyer", value=f"{form_data['purchasee_nickname']} ({form_data['purchasee_ingame']})", inline=True)
            embed.add_field(name="Type", value=form_data['purchase_type'], inline=True)
            embed.add_field(name="Amount", value=f"{form_data['amount_deposited']} coins", inline=True)
            if form_data.get('num_plots'):
                embed.add_field(name="Plots", value=f"{form_data['num_plots']} (total: {form_data['total_plots']})", inline=True)
            if form_data.get('banner_color'):
                embed.add_field(name="Mall Shop", value=f"Color {form_data['banner_color']}, #{form_data['shop_number']}", inline=True)
        elif table == 'demolition_report':
            embed.add_field(name="Submitter", value=f"<@{form_data['submitted_by']}>", inline=True)
            embed.add_field(name="Player", value=form_data['ingame_username'], inline=True)
            embed.add_field(name="Removed", value=form_data['removed'], inline=True)
            embed.add_field(name="Items Stashed", value="Yes" if form_data['stashed_items'] else "No", inline=True)
        elif table == 'demolition_request':
            embed.add_field(name="Requested by", value=f"<@{form_data['submitted_by']}>", inline=True)
            embed.add_field(name="Target Player", value=form_data['ingame_username'], inline=True)
            embed.add_field(name="Reason", value=form_data['reason'], inline=False)
        elif table == 'eviction_report':
            embed.add_field(name="Submitter", value=f"<@{form_data['submitted_by']}>", inline=True)
            embed.add_field(name="Owner", value=form_data['ingame_owner'], inline=True)
            embed.add_field(name="Items Stored", value="Yes" if form_data['items_stored'] else "No", inline=True)
            embed.add_field(name="Inactivity Period", value=form_data['inactivity_period'], inline=True)
        elif table == 'scroll_completion':
            embed.add_field(name="Submitter", value=f"<@{form_data['submitted_by']}>", inline=True)
            embed.add_field(name="Scroll Type", value=form_data['scroll_type'].capitalize(), inline=True)
            embed.add_field(name="Items Stored", value="Yes" if form_data['items_stored'] else "No", inline=True)

        screenshot_urls_str = form_data.get('screenshot_urls', '')
        if screenshot_urls_str:
            url_list = screenshot_urls_str.split(',')
            embed.set_image(url=url_list[0])
            if len(url_list) > 1:
                embed.add_field(name="Additional Screenshots", value=f"{len(url_list)-1} more", inline=False)

        embed.set_footer(text=f"Status: {form_data['status']}")
        return embed


async def setup(bot):
    await bot.add_cog(FormEditCog(bot))