import discord
import asyncpg
from discord import app_commands
from discord.ext import commands
from services.db_service import DBService
from utils.logger import get_logger
import re

logger = get_logger(__name__)


class LookupView(discord.ui.View):
    """Pagination view for lookup results."""

    def __init__(self, results: list, title: str, per_page: int = 10):
        super().__init__(timeout=180)
        self.results = results
        self.title = title
        self.per_page = per_page
        self.current_page = 0
        self.max_page = (len(results) - 1) // per_page if results else 0
        self.update_buttons()

    def update_buttons(self):
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.max_page

    def build_embed(self) -> discord.Embed:
        start = self.current_page * self.per_page
        end = start + self.per_page
        page_items = self.results[start:end]

        embed = discord.Embed(
            title=self.title,
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )

        if not page_items:
            embed.description = "No results found."
        else:
            for item in page_items:
                embed.add_field(
                    name=f"{item['form_type']} `{item['display_id']}` ({item['status']})",
                    value=item['description'],
                    inline=False
                )

        embed.set_footer(text=f"Page {self.current_page + 1}/{self.max_page + 1} • Total: {len(self.results)}")
        return embed

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, disabled=True)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


class LookupCog(commands.Cog):
    """Commands to look up recruitments and invoices with full details."""

    _TABLE_PREFIX = {
        'recruitment': 'rec',
        'purchase_invoice': 'inv'
    }

    def __init__(self, bot):
        self.bot = bot

    async def _is_authorized(self, interaction: discord.Interaction) -> bool:
        has_admin = await DBService.user_has_role(interaction.user.id, 'admin')
        has_comayor = await DBService.user_has_role(interaction.user.id, 'comayor')
        if has_admin or has_comayor:
            return True
        return interaction.user.guild_permissions.manage_guild

    async def _safe_defer(self, interaction: discord.Interaction, ephemeral: bool = True) -> bool:
        try:
            await interaction.response.defer(ephemeral=ephemeral)
            return True
        except (discord.NotFound, discord.HTTPException):
            return False

    def _format_record(self, table: str, row: asyncpg.Record, prefix: str) -> str:
        """Build a detailed, human-readable string from all columns of a form row."""
        display_id = f"{prefix}_{row['id']}"
        lines = []
        # Fields to skip (internal IDs, timestamps handled separately)
        skip_fields = {
            'id', 'submitted_by', 'submitted_at', 'status', 'approved_by', 'approved_at',
            'confirmation_msg_id', 'confirmation_channel_id', 'approval_message_id',
            'thread_message_id'
        }

        # Add submission date at the top
        submitted = row.get('submitted_at')
        if submitted and hasattr(submitted, 'strftime'):
            lines.append(f"**Submitted:** {submitted.strftime('%Y-%m-%d %H:%M UTC')}")

        # Loop through every column and add if not empty/skipped
        for key in row.keys():
            if key in skip_fields:
                continue
            value = row[key]
            if value is None or (isinstance(value, str) and value.strip() == ''):
                continue

            # Format booleans
            if isinstance(value, bool):
                value = 'Yes' if value else 'No'

            # Handle screenshot URLs specially (show count, not full URLs)
            if key == 'screenshot_urls' and isinstance(value, str):
                urls = [u for u in value.split(',') if u.strip()]
                value = f"{len(urls)} screenshot(s)"
            elif isinstance(value, str) and len(value) > 100:
                # Truncate very long text fields for readability
                value = value[:100] + "…"

            label = key.replace('_', ' ').title()
            lines.append(f"**{label}:** {value}")

        if not lines:
            lines.append("(no additional details)")
        return "\n".join(lines)

    @app_commands.command(
        name="lookup_recruitment",
        description="Search recruitments by in-game name or Discord username"
    )
    @app_commands.describe(
        query="In-game name or Discord username/mention to search for"
    )
    async def lookup_recruitment(self, interaction: discord.Interaction, query: str):
        if not await self._safe_defer(interaction, ephemeral=True):
            return

        if not await self._is_authorized(interaction):
            await interaction.followup.send(
                "❌ You don't have permission to use this command.",
                ephemeral=True
            )
            return

        # Extract user ID if query is a mention
        mention_match = re.search(r'<@!?(\d+)>', query)
        if mention_match:
            search_term = str(int(mention_match.group(1)))
        else:
            search_term = query.strip()

        pattern = f"%{search_term}%"
        rows = await DBService.fetch(
            "SELECT * FROM recruitment "
            "WHERE (ingame_username ILIKE $1 OR discord_username ILIKE $1) "
            "ORDER BY submitted_at DESC LIMIT 50",
            pattern
        )

        if not rows:
            await interaction.followup.send(
                f"No recruitments found matching '{query}'.",
                ephemeral=True
            )
            return

        results = []
        for row in rows:
            desc = self._format_record('recruitment', row, 'rec')
            results.append({
                'form_type': 'Recruitment',
                'display_id': f"rec_{row['id']}",
                'status': row['status'],
                'description': desc,
            })

        view = LookupView(results, f"Recruitment search results for '{query}'")
        embed = view.build_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @app_commands.command(
        name="lookup_invoice",
        description="Search purchase invoices by buyer's in-game name"
    )
    @app_commands.describe(
        ingame_name="Buyer's Minecraft username (partial match supported)"
    )
    async def lookup_invoice(self, interaction: discord.Interaction, ingame_name: str):
        if not await self._safe_defer(interaction, ephemeral=True):
            return

        if not await self._is_authorized(interaction):
            await interaction.followup.send(
                "❌ You don't have permission to use this command.",
                ephemeral=True
            )
            return

        pattern = f"%{ingame_name.strip()}%"
        rows = await DBService.fetch(
            "SELECT * FROM purchase_invoice "
            "WHERE purchasee_ingame ILIKE $1 "
            "ORDER BY submitted_at DESC LIMIT 50",
            pattern
        )

        if not rows:
            await interaction.followup.send(
                f"No invoices found for buyer '{ingame_name}'.",
                ephemeral=True
            )
            return

        results = []
        for row in rows:
            desc = self._format_record('purchase_invoice', row, 'inv')
            results.append({
                'form_type': 'Invoice',
                'display_id': f"inv_{row['id']}",
                'status': row['status'],
                'description': desc,
            })

        view = LookupView(results, f"Invoice search results for '{ingame_name}'")
        embed = view.build_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


async def setup(bot):
    await bot.add_cog(LookupCog(bot))