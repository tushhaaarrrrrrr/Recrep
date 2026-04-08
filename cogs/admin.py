import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from database.connection import get_db_pool
from utils.logger import get_logger
import time

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

    def __init__(self, bot):
        self.bot = bot
        self.start_time = time.time()

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
                await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(
        name="view_config",
        description="Show current channel configuration and internal roles"
    )
    @app_commands.default_permissions(administrator=True)
    async def view_config(self, interaction: discord.Interaction):
        config = await DBService.get_guild_config(interaction.guild_id)
        embed = discord.Embed(
            title="Guild Configuration",
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )

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

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(
        name="status",
        description="View bot status, uptime, and database health"
    )
    @app_commands.default_permissions(administrator=True)
    async def status_command(self, interaction: discord.Interaction):
        """Display bot status, uptime, and database connection health."""
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

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(
        name="recalculate_reputation",
        description="Recalculate all staff reputation from reputation_log (fix manual DB changes)"
    )
    @app_commands.default_permissions(administrator=True)
    async def recalculate_reputation(self, interaction: discord.Interaction):
        """Recalculate reputation for all staff members."""
        await interaction.response.defer(ephemeral=True)
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

async def setup(bot):
    await bot.add_cog(AdminCog(bot))