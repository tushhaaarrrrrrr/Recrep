import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from utils.logger import get_logger

logger = get_logger(__name__)

class LeaderboardStatsCog(commands.Cog):
    """Commands for viewing leaderboards and staff statistics."""

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="leaderboard",
        description="Show the leaderboard for a specific category and time period"
    )
    @app_commands.choices(
        category=[
            app_commands.Choice(name="🏆 Reputation", value="reputation"),
            app_commands.Choice(name="📋 Recruitments", value="recruitment"),
            app_commands.Choice(name="📈 Progress Reports", value="progress_report"),
            app_commands.Choice(name="🤝 Progress Help", value="progress_help"),
            app_commands.Choice(name="💰 Invoices", value="purchase_invoice"),
            app_commands.Choice(name="🏚️ Demolitions", value="demolition_report"),
            app_commands.Choice(name="🏠 Evictions", value="eviction_report"),
            app_commands.Choice(name="📜 Scrolls", value="scroll_completion")
        ],
        period=[
            app_commands.Choice(name="Weekly", value="weekly"),
            app_commands.Choice(name="Bi-weekly", value="biweekly"),
            app_commands.Choice(name="Monthly", value="monthly"),
            app_commands.Choice(name="All Time", value="all")
        ]
    )
    async def leaderboard(
        self,
        interaction: discord.Interaction,
        category: app_commands.Choice[str],
        period: app_commands.Choice[str]
    ):
        # Defer to prevent timeout while fetching data
        await interaction.response.defer()

        try:
            if category.value == "reputation":
                rows = await DBService.get_leaderboard(period.value)
                title = f"🏆 {period.name} Reputation Leaderboard"
                value_key = "points"
                unit = "pts"
            elif category.value == "progress_help":
                rows = await DBService.get_category_leaderboard(category.value, period.value)
                title = f"🤝 {period.name} Progress Help Leaderboard"
                value_key = "count"
                unit = "help(s)"
            else:
                rows = await DBService.get_category_leaderboard(category.value, period.value)
                title = f"📊 {period.name} {category.name} Leaderboard"
                value_key = "count"
                unit = "form(s)"

            embed = discord.Embed(
                title=title,
                color=discord.Color.gold(),
                timestamp=discord.utils.utcnow()
            )

            if not rows:
                embed.description = "📭 **No data yet.** Submit and approve forms to appear here!"
            else:
                lines = []
                for idx, row in enumerate(rows[:10], 1):
                    member = interaction.guild.get_member(row['discord_id'])
                    name = member.display_name if member else f"User {row['discord_id']}"
                    value = row[value_key]
                    lines.append(f"{idx}. **{name}** - {value} {unit}")
                embed.description = "\n".join(lines)
                embed.set_footer(text=f"Showing top {min(len(rows), 10)} out of {len(rows)}")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.exception(f"Leaderboard error: {e}")
            await interaction.followup.send(
                "❌ **Failed to load leaderboard.** Please try again later.",
                ephemeral=True
            )

    @app_commands.command(
        name="stats",
        description="[Admin] View detailed statistics of a staff member"
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        member="The staff member whose stats you want to see",
        period="Time period for the statistics (default: All Time)"
    )
    @app_commands.choices(period=[
        app_commands.Choice(name="Weekly", value="weekly"),
        app_commands.Choice(name="Bi-weekly", value="biweekly"),
        app_commands.Choice(name="Monthly", value="monthly"),
        app_commands.Choice(name="All Time", value="all")
    ])
    async def stats(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        period: app_commands.Choice[str] = None
    ):
        # Defer to prevent timeout while fetching detailed stats
        await interaction.response.defer()

        try:
            if period is None:
                period = app_commands.Choice(name="All Time", value="all")

            stats = await DBService.get_user_detailed_stats(member.id, period.value)

            embed = discord.Embed(
                title=f"📊 Statistics for {member.display_name}",
                description=f"**Period:** {period.name}",
                color=discord.Color.blue(),
                timestamp=discord.utils.utcnow()
            )

            # Form counts
            counts = [
                ("📋 Recruitments", stats.get('recruitment', 0)),
                ("📈 Progress Reports", stats.get('progress_report', 0)),
                ("🤝 Progress Helps", stats.get('progress_help', 0)),
                ("💰 Invoices", stats.get('purchase_invoice', 0)),
                ("🏚️ Demolition Reports", stats.get('demolition_report', 0)),
                ("📝 Demolition Requests", stats.get('demolition_request', 0)),
                ("🏠 Evictions", stats.get('eviction_report', 0)),
                ("📜 Scrolls", stats.get('scroll_completion', 0)),
                ("✅ Form Approvals", stats.get('approval_count', 0))
            ]
            # Display in two columns (first 5, then next 4)
            for label, value in counts[:5]:
                embed.add_field(name=label, value=value, inline=True)
            embed.add_field(name="\u200b", value="\u200b", inline=True)
            for label, value in counts[5:]:
                embed.add_field(name=label, value=value, inline=True)

            # Points breakdown
            breakdown = stats.get('points_breakdown', {})
            if breakdown:
                breakdown_text = []
                for form_type, points in breakdown.items():
                    display = form_type.replace('_', ' ').title()
                    if display.endswith('Approval'):
                        display = "Form Approvals"
                    breakdown_text.append(f"• **{display}:** {points} pts")
                embed.add_field(
                    name="📊 Points Breakdown",
                    value="\n".join(breakdown_text) or "*None*",
                    inline=False
                )

            embed.add_field(
                name="⭐ Total Reputation",
                value=f"**{stats.get('reputation', 0)}** pts",
                inline=False
            )
            embed.set_footer(text="Only approved forms count towards reputation")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.exception(f"Stats command error: {e}")
            await interaction.followup.send(
                "❌ **Failed to load statistics.** Please try again later.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(LeaderboardStatsCog(bot))