import discord
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from utils.logger import get_logger

logger = get_logger(__name__)


class LeaderboardView(discord.ui.View):
    """Pagination view for leaderboard command."""

    def __init__(self, rows, title, value_key, unit, per_page=10):
        super().__init__(timeout=180)
        self.rows = rows
        self.title = title
        self.value_key = value_key
        self.unit = unit
        self.per_page = per_page
        self.current_page = 0
        self.max_page = (len(rows) - 1) // per_page if rows else 0
        self.update_buttons()

    def update_buttons(self):
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.max_page

    def get_page_embed(self, guild: discord.Guild) -> discord.Embed:
        start = self.current_page * self.per_page
        end = start + self.per_page
        page_rows = self.rows[start:end]

        embed = discord.Embed(
            title=self.title,
            color=discord.Color.gold(),
            timestamp=discord.utils.utcnow()
        )

        if not page_rows:
            embed.description = "📭 **No data yet.** Submit and approve forms to appear here!"
        else:
            lines = []
            for idx, row in enumerate(page_rows, start=start + 1):
                member = guild.get_member(row['discord_id'])
                name = member.display_name if member else f"User {row['discord_id']}"
                value = row[self.value_key]
                lines.append(f"{idx}. **{name}** - {value} {self.unit}")
            embed.description = "\n".join(lines)
            embed.set_footer(
                text=f"Page {self.current_page + 1}/{self.max_page + 1} • Total: {len(self.rows)}"
            )

        return embed

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, disabled=True)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        embed = self.get_page_embed(interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        embed = self.get_page_embed(interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self)


class LeaderboardStatsCog(commands.Cog):
    """Commands for viewing leaderboards and staff statistics."""

    def __init__(self, bot):
        self.bot = bot

    async def _safe_defer(self, interaction: discord.Interaction, ephemeral: bool = False) -> bool:
        try:
            await interaction.response.defer(ephemeral=ephemeral)
            return True
        except (discord.NotFound, discord.HTTPException):
            return False

    @app_commands.command(
        name="leaderboard",
        description="Show the full leaderboard for a specific category and time period"
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
        if not await self._safe_defer(interaction):
            return

        try:
            if category.value == "reputation":
                rows = await DBService.get_leaderboard(period.value, limit=1000)
                title = f"🏆 {period.name} Reputation Leaderboard"
                value_key = "points"
                unit = "pts"
            elif category.value == "progress_help":
                rows = await DBService.get_category_leaderboard(category.value, period.value, limit=1000)
                title = f"🤝 {period.name} Progress Help Leaderboard"
                value_key = "count"
                unit = "help(s)"
            else:
                rows = await DBService.get_category_leaderboard(category.value, period.value, limit=1000)
                title = f"📊 {period.name} {category.name} Leaderboard"
                value_key = "count"
                unit = "form(s)"

            if not rows:
                embed = discord.Embed(
                    title=title,
                    description="📭 **No data yet.** Submit and approve forms to appear here!",
                    color=discord.Color.gold(),
                    timestamp=discord.utils.utcnow()
                )
                await interaction.followup.send(embed=embed)
                return

            view = LeaderboardView(rows, title, value_key, unit)
            embed = view.get_page_embed(interaction.guild)
            await interaction.followup.send(embed=embed, view=view)

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
        if not await self._safe_defer(interaction):
            return

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
            for label, value in counts[:5]:
                embed.add_field(name=label, value=value, inline=True)
            embed.add_field(name="\u200b", value="\u200b", inline=True)
            for label, value in counts[5:]:
                embed.add_field(name=label, value=value, inline=True)

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