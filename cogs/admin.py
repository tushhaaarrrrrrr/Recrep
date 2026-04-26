import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from services.reputation_service import (
    award_submitter_points,
    award_helper_points,
    award_approval_points,
    SCROLL_POINTS,
    REP_POINTS,
)
from database.connection import get_db_pool
from utils.logger import get_logger
from config.settings import OWNER_ID
import time
import sys

logger = get_logger(__name__)

class AdminCog(commands.Cog):
    """Administrator commands for configuration and role management."""

    _VALID_ROLES = ['admin', 'comayor', 'builder', 'recruiter']
    _CHANNEL_KEYS = {
        'Recruitment': 'recruitment_channel_id',
        'Progress': 'progress_channel_id',
        'Invoice': 'invoice_channel_id',
        'Demolition': 'demolition_channel_id',
        'Eviction': 'eviction_channel_id',
        'Scroll': 'scroll_channel_id'
    }
    # Additional configurable IDs (not channels)
    _CONFIG_KEYS = {
        'Community Guild': 'community_guild_id',
        'Player Role': 'player_role_id'
    }

    def __init__(self, bot):
        self.bot = bot
        self.start_time = time.time()

    async def _safe_defer(self, interaction: discord.Interaction, ephemeral: bool = True) -> bool:
        """Safely defer an interaction, returning False if the interaction is already invalid."""
        try:
            await interaction.response.defer(ephemeral=ephemeral)
            return True
        except (discord.NotFound, discord.HTTPException):
            return False

    # ── Existing configuration commands (set_approval_channel, set_log_channel, …) remain unchanged ──
    # (I'll include them all for completeness but they are exactly as you provided.)

    @app_commands.command(
        name="set_approval_channel",
        description="Set the channel where pending forms are sent for approval"
    )
    @app_commands.default_permissions(administrator=True)
    async def set_approval(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await DBService.set_guild_config(interaction.guild_id, approval_channel_id=channel.id)
        logger.info(f"Guild {interaction.guild_id}: Approval channel set to {channel.id}")
        await interaction.response.send_message(
            f"✅ Approval channel set – new forms will appear in {channel.mention}",
            ephemeral=True
        )

    @app_commands.command(
        name="set_log_channel",
        description="Set the channel where monthly threads for a specific log type will be created"
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.choices(log_type=[
        app_commands.Choice(name=name, value=value)
        for name, value in _CHANNEL_KEYS.items()
    ])
    async def set_log_channel(
        self,
        interaction: discord.Interaction,
        log_type: app_commands.Choice[str],
        channel: discord.TextChannel
    ):
        await DBService.set_guild_config(interaction.guild_id, **{log_type.value: channel.id})
        logger.info(f"Guild {interaction.guild_id}: {log_type.name} log channel set to {channel.id}")
        await interaction.response.send_message(
            f"✅ {log_type.name} log channel set – monthly threads will be created in {channel.mention}",
            ephemeral=True
        )

    @app_commands.command(
        name="set_community_guild",
        description="Set the ID of the community server where the player role will be assigned"
    )
    @app_commands.default_permissions(administrator=True)
    async def set_community_guild(self, interaction: discord.Interaction, guild_id: str):
        """Store the community guild ID (must be a server the bot is in)."""
        try:
            guild_id_int = int(guild_id)
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid guild ID. Please provide a numeric ID.",
                ephemeral=True
            )
            return

        # Verify the bot is in that guild
        guild = self.bot.get_guild(guild_id_int)
        if not guild:
            await interaction.response.send_message(
                f"❌ Bot is not a member of guild with ID `{guild_id_int}`. Invite the bot to that server first.",
                ephemeral=True
            )
            return

        await DBService.set_guild_config(interaction.guild_id, community_guild_id=guild_id_int)
        logger.info(f"Guild {interaction.guild_id}: Community guild set to {guild_id_int} ({guild.name})")
        await interaction.response.send_message(
            f"✅ Community guild set to **{guild.name}** (`{guild_id_int}`).",
            ephemeral=True
        )

    @app_commands.command(
        name="set_player_role",
        description="Set the role to assign to new players in the community server"
    )
    @app_commands.default_permissions(administrator=True)
    async def set_player_role(self, interaction: discord.Interaction, role_id: str):
        """Store the player role ID (must exist in the configured community guild)."""
        try:
            role_id_int = int(role_id)
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid role ID. Please provide a numeric ID.",
                ephemeral=True
            )
            return

        # Verify the role exists in the configured community guild
        config = await DBService.get_guild_config(interaction.guild_id)
        community_guild_id = config.get('community_guild_id') if config else None
        if not community_guild_id:
            await interaction.response.send_message(
                "❌ Community guild not configured. Use `/set_community_guild` first.",
                ephemeral=True
            )
            return

        guild = self.bot.get_guild(community_guild_id)
        if not guild:
            await interaction.response.send_message(
                f"❌ Community guild (ID `{community_guild_id}`) not found. The bot may have been removed.",
                ephemeral=True
            )
            return

        role = guild.get_role(role_id_int)
        if not role:
            await interaction.response.send_message(
                f"❌ Role with ID `{role_id_int}` not found in **{guild.name}**.",
                ephemeral=True
            )
            return

        await DBService.set_guild_config(interaction.guild_id, player_role_id=role_id_int)
        logger.info(f"Guild {interaction.guild_id}: Player role set to {role_id_int} ({role.name})")
        await interaction.response.send_message(
            f"✅ Player role set to **{role.name}** (`{role_id_int}`) in **{guild.name}**.",
            ephemeral=True
        )

    @app_commands.command(
        name="role",
        description="Grant, revoke, or list internal staff roles"
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.choices(action=[
        app_commands.Choice(name="grant", value="grant"),
        app_commands.Choice(name="revoke", value="revoke"),
        app_commands.Choice(name="list", value="list")
    ])
    async def role_command(
        self,
        interaction: discord.Interaction,
        action: app_commands.Choice[str],
        member: discord.Member = None,
        role: str = None
    ):
        if action.value == "grant":
            if not member or not role or role not in self._VALID_ROLES:
                await interaction.response.send_message(
                    f"❌ Usage: `/role grant @user` `role`\nValid roles: {', '.join(self._VALID_ROLES)}",
                    ephemeral=True
                )
                return
            await DBService.add_user_role(member.id, role, interaction.user.id)
            await interaction.response.send_message(
                f"✅ Granted `{role}` role to {member.mention}",
                ephemeral=True
            )
            logger.info(f"{interaction.user.id} granted {role} to {member.id}")

        elif action.value == "revoke":
            if not member or not role or role not in self._VALID_ROLES:
                await interaction.response.send_message(
                    f"❌ Usage: `/role revoke @user` `role`\nValid roles: {', '.join(self._VALID_ROLES)}",
                    ephemeral=True
                )
                return
            await DBService.remove_user_role(member.id, role)
            await interaction.response.send_message(
                f"✅ Revoked `{role}` role from {member.mention}",
                ephemeral=True
            )
            logger.info(f"{interaction.user.id} revoked {role} from {member.id}")

        elif action.value == "list":
            if member:
                roles = await DBService.get_user_roles(member.id)
                if roles:
                    await interaction.response.send_message(
                        f"📋 **{member.display_name}** – internal roles: **{', '.join(roles)}**",
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        f"ℹ️ {member.display_name} has no internal roles.",
                        ephemeral=True
                    )
            else:
                # Full role list may take time; defer to avoid timeout
                if not await self._safe_defer(interaction):
                    return

                embed = discord.Embed(
                    title="Internal Staff Roles",
                    color=discord.Color.green(),
                    timestamp=discord.utils.utcnow()
                )
                for r in self._VALID_ROLES:
                    users = await DBService.list_users_with_role(r)
                    if users:
                        mentions = []
                        for u in users:
                            user = interaction.guild.get_member(u['user_id'])
                            mentions.append(user.mention if user else f"Unknown ({u['user_id']})")
                        mentions_str = ", ".join(mentions)
                        if len(mentions_str) > 1024:
                            truncated = mentions[:20]
                            remaining = len(mentions) - 20
                            mentions_str = ", ".join(truncated)
                            if remaining > 0:
                                mentions_str += f", and {remaining} more"
                        embed.add_field(
                            name=f"**{r.capitalize()}**",
                            value=mentions_str,
                            inline=False
                        )
                    else:
                        embed.add_field(
                            name=f"**{r.capitalize()}**",
                            value="*None*",
                            inline=False
                        )
                await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="view_config",
        description="Show current channel configuration, community settings, and internal roles"
    )
    @app_commands.default_permissions(administrator=True)
    async def view_config(self, interaction: discord.Interaction):
        # Defer to prevent timeout while fetching config and roles
        if not await self._safe_defer(interaction):
            return

        config = await DBService.get_guild_config(interaction.guild_id)
        embed = discord.Embed(
            title="Guild Configuration",
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )

        # Channel configuration
        channel_config = {
            "Approval Channel": config.get("approval_channel_id") if config else None,
            "Recruitment Logs": config.get("recruitment_channel_id") if config else None,
            "Progress Logs": config.get("progress_channel_id") if config else None,
            "Invoice Logs": config.get("invoice_channel_id") if config else None,
            "Demolition Logs": config.get("demolition_channel_id") if config else None,
            "Eviction Logs": config.get("eviction_channel_id") if config else None,
            "Scroll Logs": config.get("scroll_channel_id") if config else None
        }

        channel_text = []
        for name, chan_id in channel_config.items():
            if chan_id:
                channel = interaction.guild.get_channel(chan_id)
                channel_text.append(f"• {name}: {channel.mention if channel else f'`{chan_id}` (deleted)'}")
            else:
                channel_text.append(f"• {name}: ❌ Not set")
        embed.add_field(
            name="Log Channels",
            value="\n".join(channel_text),
            inline=False
        )

        # Community configuration
        community_guild_id = config.get('community_guild_id') if config else None
        player_role_id = config.get('player_role_id') if config else None

        comm_text = []
        if community_guild_id:
            guild = self.bot.get_guild(community_guild_id)
            comm_text.append(f"• Community Guild: {guild.name if guild else f'`{community_guild_id}` (bot not in server)'}")
        else:
            comm_text.append("• Community Guild: ❌ Not set")
        if player_role_id:
            if community_guild_id:
                guild = self.bot.get_guild(community_guild_id)
                if guild:
                    role = guild.get_role(player_role_id)
                    comm_text.append(f"• Player Role: {role.mention if role else f'`{player_role_id}` (deleted)'}")
                else:
                    comm_text.append(f"• Player Role: `{player_role_id}` (guild unknown)")
            else:
                comm_text.append(f"• Player Role: `{player_role_id}` (guild not set)")
        else:
            comm_text.append("• Player Role: ❌ Not set")
        embed.add_field(
            name="Community Server Integration",
            value="\n".join(comm_text),
            inline=False
        )

        # Internal roles
        for role_name in self._VALID_ROLES:
            users = await DBService.list_users_with_role(role_name)
            if users:
                mentions = []
                for u in users:
                    user = interaction.guild.get_member(u['user_id'])
                    mentions.append(user.mention if user else f"Unknown ({u['user_id']})")
                mentions_str = ", ".join(mentions)
                if len(mentions_str) > 1024:
                    truncated = mentions[:20]
                    remaining = len(mentions) - 20
                    mentions_str = ", ".join(truncated)
                    if remaining > 0:
                        mentions_str += f", and {remaining} more"
                embed.add_field(
                    name=f"**{role_name.capitalize()}**",
                    value=mentions_str,
                    inline=False
                )
            else:
                embed.add_field(
                    name=f"**{role_name.capitalize()}**",
                    value="*None*",
                    inline=False
                )

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="status",
        description="View bot status, uptime, and database health"
    )
    @app_commands.default_permissions(administrator=True)
    async def status_command(self, interaction: discord.Interaction):
        """Display bot status, uptime, and database connection health."""
        if not await self._safe_defer(interaction):
            return

        uptime_seconds = int(time.time() - self.start_time)
        days = uptime_seconds // 86400
        hours = (uptime_seconds % 86400) // 3600
        minutes = (uptime_seconds % 3600) // 60
        seconds = uptime_seconds % 60
        uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"

        # Check database connectivity using the existing pool
        db_ok = False
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            db_ok = True
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")

        embed = discord.Embed(
            title="Bot Status",
            color=discord.Color.green() if db_ok else discord.Color.red(),
            timestamp=discord.utils.utcnow()
        )
        embed.add_field(name="Bot Name", value=self.bot.user.name, inline=True)
        embed.add_field(name="Bot ID", value=str(self.bot.user.id), inline=True)
        embed.add_field(name="Uptime", value=uptime_str, inline=True)
        embed.add_field(name="Database", value="✅ Connected" if db_ok else "❌ Disconnected", inline=True)
        embed.add_field(name="Latency", value=f"{round(self.bot.latency * 1000)}ms", inline=True)
        embed.add_field(name="Guilds", value=str(len(self.bot.guilds)), inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="recalculate_reputation",
        description="Recalculate all staff reputation from reputation_log (fix manual DB changes)"
    )
    @app_commands.default_permissions(administrator=True)
    async def recalculate_reputation(self, interaction: discord.Interaction):
        """Recalculate reputation for all staff members."""
        if not await self._safe_defer(interaction):
            return
        try:
            # Sum points per staff_id from reputation_log
            rows = await DBService.fetch(
                "SELECT staff_id, SUM(points) as total FROM reputation_log GROUP BY staff_id"
            )
            if not rows:
                await interaction.followup.send("No reputation entries found.", ephemeral=True)
                return
            count = 0
            for row in rows:
                staff_id = row['staff_id']
                total = row['total']
                await DBService.execute(
                    "UPDATE staff_member SET reputation = $1 WHERE discord_id = $2",
                    total, staff_id
                )
                count += 1
            await interaction.followup.send(
                f"✅ Reputation recalculated for {count} staff members.",
                ephemeral=True
            )
        except Exception as e:
            logger.exception(f"Failed to recalculate reputation: {e}")
            await interaction.followup.send("❌ An error occurred while recalculating reputation.", ephemeral=True)

    @app_commands.command(
        name="refresh_stats",
        description="Completely rebuild reputation and stats from all approved forms"
    )
    @app_commands.default_permissions(administrator=True)
    async def refresh_stats(self, interaction: discord.Interaction):
        """Rebuild reputation_log and staff_member.reputation from scratch using approved forms."""
        if not await self._safe_defer(interaction):
            return
        try:
            await DBService.refresh_all_reputation()
            await interaction.followup.send(
                "✅ Reputation and statistics have been fully refreshed from approved forms.",
                ephemeral=True
            )
        except Exception as e:
            logger.exception(f"Failed to refresh stats: {e}")
            await interaction.followup.send(
                "❌ An error occurred while refreshing statistics.",
                ephemeral=True
            )

    @app_commands.command(
        name="shutdown",
        description="[Owner Only] Shut down the bot completely"
    )
    @app_commands.default_permissions(administrator=True)
    async def shutdown(self, interaction: discord.Interaction):
        """Gracefully shut down the bot. Only the bot owner can use this."""
        if interaction.user.id != int(OWNER_ID):
            await interaction.response.send_message(
                "❌ Only the bot owner can use this command.",
                ephemeral=True
            )
            return

        await interaction.response.send_message("🛑 Shutting down... Goodbye!", ephemeral=False)
        logger.critical(f"Shutdown command issued by {interaction.user} ({interaction.user.id})")
        await self.bot.close()
        sys.exit(0)

    # ── Approve all pending forms ────────────────────────────────
    @app_commands.command(
        name="approve_all",
        description="Approve all pending forms and clean up approval messages"
    )
    @app_commands.default_permissions(administrator=True)
    async def approve_all(self, interaction: discord.Interaction):
        """Bulk-approve every pending form, award reputation, and delete approval messages."""
        if not await self._safe_defer(interaction):
            return

        # Fetch all pending forms across all tables
        pending_forms = await DBService.get_all_pending_forms()
        if not pending_forms:
            await interaction.followup.send("✅ No pending forms to approve.", ephemeral=True)
            return

        approved_count = 0
        failed_count = 0
        messages_to_delete = []  # list of (channel_id, message_id)

        # Fetch approval channel for this guild
        config = await DBService.get_guild_config(interaction.guild_id)
        approval_channel_id = config.get('approval_channel_id') if config else None

        for form in pending_forms:
            table = form['table']
            form_id = form['id']
            try:
                # Approve the form
                await DBService.approve_form(table, form_id, interaction.user.id)

                # Award submitter points (with scroll override)
                points_override = None
                if table == 'scroll_completion':
                    scroll_type = (form.get('scroll_type') or '').lower()
                    points_override = SCROLL_POINTS.get(scroll_type, REP_POINTS.get('scroll_completion', 2))
                await award_submitter_points(
                    form['submitted_by'], table, form_id, points_override
                )

                # Helper points for progress reports
                if table == 'progress_report' and form.get('helper_mentions'):
                    await award_helper_points(form['helper_mentions'], form_id)

                # Approver points
                await award_approval_points(interaction.user.id, table, form_id)

                # Queue approval message for deletion (if exists)
                if form.get('approval_message_id') and approval_channel_id:
                    messages_to_delete.append((approval_channel_id, form['approval_message_id']))

                approved_count += 1
            except Exception as e:
                logger.exception(f"Failed to approve {table}#{form_id}: {e}")
                failed_count += 1

        # Delete all queued approval messages
        deleted_msgs = 0
        for channel_id, msg_id in messages_to_delete:
            try:
                channel = self.bot.get_channel(channel_id)
                if channel:
                    msg = await channel.fetch_message(msg_id)
                    await msg.delete()
                    deleted_msgs += 1
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
            except Exception as e:
                logger.warning(f"Could not delete approval message {msg_id} in channel {channel_id}: {e}")

        result_msg = (
            f"✅ **Bulk approval complete.**\n"
            f"Approved: {approved_count}\n"
            f"Failed: {failed_count}\n"
            f"Approval messages deleted: {deleted_msgs}"
        )
        await interaction.followup.send(result_msg, ephemeral=True)
        logger.info(f"Bulk approval by {interaction.user.id}: {approved_count} approved, {failed_count} failed.")

async def setup(bot):
    await bot.add_cog(AdminCog(bot))