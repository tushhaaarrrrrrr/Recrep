import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from utils.views import ApprovalView
from utils.logger import get_logger

logger = get_logger(__name__)

class ApprovalCog(commands.Cog):
    """Administrative commands for managing pending forms."""

    _FORM_TABLES = [
        'recruitment',
        'progress_report',
        'purchase_invoice',
        'demolition_report',
        'demolition_request',
        'eviction_report',
        'scroll_completion'
    ]

    _TABLE_PREFIX = {
        'recruitment': 'rec',
        'progress_report': 'rep',
        'purchase_invoice': 'inv',
        'demolition_report': 'dem',
        'demolition_request': 'dmr',
        'eviction_report': 'evc',
        'scroll_completion': 'scr'
    }

    # Thread prefix used by each cog (must match exactly)
    _THREAD_PREFIX = {
        'recruitment': 'Recruitments',
        'progress_report': 'Progress Reports',
        'purchase_invoice': 'Invoices',
        'demolition_report': 'Demolitions',
        'demolition_request': 'Demolition Requests',
        'eviction_report': 'Evictions',
        'scroll_completion': 'Scrolls'
    }

    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        logger.info("ApprovalCog loaded - views are attached per form.")

    @app_commands.command(
        name="list_pending",
        description="List all forms waiting for approval"
    )
    @app_commands.default_permissions(manage_guild=True)
    async def list_pending(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        config = await DBService.get_guild_config(interaction.guild_id)
        if not config or not config.get('approval_channel_id'):
            await interaction.followup.send(
                "❌ **Approval channel not configured.**\nUse `/set_approval_channel` first.",
                ephemeral=True
            )
            return

        pending = []
        for table in self._FORM_TABLES:
            rows = await DBService.fetch(
                f"SELECT id, submitted_by, submitted_at FROM {table} WHERE status = 'pending'"
            )
            prefix = self._TABLE_PREFIX.get(table, 'unk')
            for row in rows:
                pending.append((table, row['id'], prefix, row['submitted_by'], row['submitted_at']))

        if not pending:
            await interaction.followup.send(
                "✅ **No pending forms** - the approval queue is empty.",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title="📋 Pending Approval Forms",
            description=f"**Total:** {len(pending)}",
            color=discord.Color.orange(),
            timestamp=discord.utils.utcnow()
        )

        for table, fid, prefix, submitter_id, submitted_at in pending[:25]:
            submitter = interaction.guild.get_member(submitter_id)
            submitter_name = submitter.display_name if submitter else f"User {submitter_id}"
            display_id = f"{prefix}_{fid}"
            embed.add_field(
                name=f"🔹 {table.replace('_', ' ').title()} · ID `{display_id}`",
                value=f"**Submitted by:** {submitter_name}\n**At:** {submitted_at.strftime('%Y-%m-%d %H:%M')} UTC",
                inline=False
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="list_held",
        description="List all forms currently on hold"
    )
    @app_commands.default_permissions(manage_guild=True)
    async def list_held(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        config = await DBService.get_guild_config(interaction.guild_id)
        if not config or not config.get('approval_channel_id'):
            await interaction.followup.send(
                "❌ **Approval channel not configured.**\nUse `/set_approval_channel` first.",
                ephemeral=True
            )
            return

        held = []
        for table in self._FORM_TABLES:
            rows = await DBService.fetch(
                f"SELECT id, submitted_by, submitted_at FROM {table} WHERE status = 'hold'"
            )
            prefix = self._TABLE_PREFIX.get(table, 'unk')
            for row in rows:
                held.append((table, row['id'], prefix, row['submitted_by'], row['submitted_at']))

        if not held:
            await interaction.followup.send(
                "✅ **No forms on hold.**",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title="⏸️ Forms on Hold",
            description=f"**Total:** {len(held)}",
            color=discord.Color.greyple(),
            timestamp=discord.utils.utcnow()
        )

        for table, fid, prefix, submitter_id, submitted_at in held[:25]:
            submitter = interaction.guild.get_member(submitter_id)
            submitter_name = submitter.display_name if submitter else f"User {submitter_id}"
            display_id = f"{prefix}_{fid}"
            embed.add_field(
                name=f"🔹 {table.replace('_', ' ').title()} · ID `{display_id}`",
                value=f"**Submitted by:** {submitter_name}\n**At:** {submitted_at.strftime('%Y-%m-%d %H:%M')} UTC",
                inline=False
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="resend_pending",
        description="Resend a pending or held form to the approval channel (use prefixed ID, e.g., rec_103)"
    )
    @app_commands.default_permissions(manage_guild=True)
    async def resend_pending(
        self,
        interaction: discord.Interaction,
        form_id: str
    ):
        """Resend a pending or held form using its prefixed ID (e.g., rec_103)."""
        await interaction.response.defer(ephemeral=True)

        if '_' not in form_id:
            await interaction.followup.send(
                "❌ Invalid form ID format. Use like `rec_103`, `rep_5`, etc.",
                ephemeral=True
            )
            return
        prefix, num_str = form_id.split('_', 1)
        try:
            numeric_id = int(num_str)
        except ValueError:
            await interaction.followup.send("❌ Invalid numeric ID.", ephemeral=True)
            return

        table = None
        for t, p in self._TABLE_PREFIX.items():
            if p == prefix:
                table = t
                break
        if not table:
            await interaction.followup.send(
                f"❌ Unknown prefix `{prefix}`. Valid prefixes: {', '.join(self._TABLE_PREFIX.values())}",
                ephemeral=True
            )
            return

        # Fetch the form – now allows 'pending' OR 'hold'
        row = await DBService.fetchrow(
            f"SELECT * FROM {table} WHERE id = $1 AND status IN ('pending', 'hold')",
            numeric_id
        )
        if not row:
            await interaction.followup.send(
                f"❌ No pending or held form with ID `{form_id}`.",
                ephemeral=True
            )
            return

        config = await DBService.get_guild_config(interaction.guild_id)
        if not config or not config.get('approval_channel_id'):
            await interaction.followup.send(
                "❌ Approval channel not configured. Use `/set_approval_channel` first.",
                ephemeral=True
            )
            return

        approval_channel = self.bot.get_channel(config['approval_channel_id'])
        if not approval_channel:
            await interaction.followup.send(
                "❌ Approval channel not found - the channel may have been deleted.",
                ephemeral=True
            )
            return

        # Delete the existing approval message if it exists
        old_msg_id = row.get('approval_message_id')
        if old_msg_id:
            try:
                old_msg = await approval_channel.fetch_message(old_msg_id)
                await old_msg.delete()
                logger.debug(f"Deleted old approval message {old_msg_id} for {table} #{numeric_id}")
            except discord.NotFound:
                logger.debug(f"Old approval message {old_msg_id} already deleted")
            except Exception as e:
                logger.warning(f"Could not delete old approval message {old_msg_id}: {e}")

        # Build the embed – reflect current status in the title
        status_label = "On Hold" if row['status'] == 'hold' else "Resubmitted"
        embed = discord.Embed(
            title=f"📄 {status_label}: {table.replace('_', ' ').title()}",
            description=f"**Form ID:** `{form_id}`",
            color=discord.Color.greyple() if row['status'] == 'hold' else discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )
        embed.add_field(name="👤 Submitted by", value=f"<@{row['submitted_by']}>", inline=True)
        embed.add_field(name="⏰ Submitted at", value=row['submitted_at'].strftime("%Y-%m-%d %H:%M UTC"), inline=True)
        if row.get('screenshot_urls'):
            embed.set_image(url=row['screenshot_urls'].split(',')[0])

        thread_prefix = self._THREAD_PREFIX.get(table, table.replace('_', ' ').title())

        view = ApprovalView(
            table=table,
            form_id=numeric_id,
            form_type=table,
            submitter_id=row['submitted_by'],
            guild_id=interaction.guild_id,
            channel_config_key=f"{table}_channel_id",
            thread_prefix=thread_prefix,
            confirmation_msg_id=None,
            confirmation_channel_id=None,
            form_data=None,
            resend_confirmation_msg_id=None,
            resend_confirmation_channel_id=interaction.channel_id
        )

        msg = await approval_channel.send(embed=embed, view=view)
        await DBService.set_approval_message_id(table, numeric_id, msg.id)

        confirm_msg = await interaction.followup.send(
            f"✅ **Form `{form_id}` resent to {approval_channel.mention}.**",
            ephemeral=True,
            wait=True
        )
        view.resend_confirmation_msg_id = confirm_msg.id

async def setup(bot):
    await bot.add_cog(ApprovalCog(bot))