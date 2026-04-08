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
        # Defer to prevent interaction timeout
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
        name="resend_pending",
        description="Resend a pending form to the approval channel (use prefixed ID, e.g., rec_103)"
    )
    @app_commands.default_permissions(manage_guild=True)
    async def resend_pending(
        self,
        interaction: discord.Interaction,
        form_id: str
    ):
        """Resend a pending form using its prefixed ID (e.g., rec_103)."""
        # Defer to prevent interaction timeout
        await interaction.response.defer(ephemeral=True)

        # Parse prefixed ID
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

        # Find table from prefix
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

        # Fetch the pending form
        row = await DBService.fetchrow(
            f"SELECT * FROM {table} WHERE id = $1 AND status = 'pending'",
            numeric_id
        )
        if not row:
            await interaction.followup.send(
                f"❌ No pending form with ID `{form_id}`.",
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

        # Build the embed
        embed = discord.Embed(
            title=f"📄 Resubmitted: {table.replace('_', ' ').title()}",
            description=f"**Form ID:** `{form_id}`",
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )
        embed.add_field(name="👤 Submitted by", value=f"<@{row['submitted_by']}>", inline=True)
        embed.add_field(name="⏰ Submitted at", value=row['submitted_at'].strftime("%Y-%m-%d %H:%M UTC"), inline=True)
        if row.get('screenshot_urls'):
            embed.set_image(url=row['screenshot_urls'].split(',')[0])

        # Get the correct thread prefix
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
            # New: track the resend command's confirmation message for cleanup
            resend_confirmation_msg_id=None,
            resend_confirmation_channel_id=interaction.channel_id
        )

        msg = await approval_channel.send(embed=embed, view=view)
        await DBService.set_approval_message_id(table, numeric_id, msg.id)

        # Send confirmation and store its ID for cleanup
        confirm_msg = await interaction.followup.send(
            f"✅ **Form `{form_id}` resent to {approval_channel.mention}.**",
            ephemeral=True,
            wait=True
        )
        # Update the view with the confirmation message ID
        view.resend_confirmation_msg_id = confirm_msg.id

async def setup(bot):
    await bot.add_cog(ApprovalCog(bot))