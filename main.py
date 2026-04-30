import asyncio
import discord
import logging
import sys
from pathlib import Path
from discord.ext import commands, tasks

from config.settings import DISCORD_TOKEN
from utils.logger import setup_logging
from database.connection import init_db_pool, close_db_pool
from services.s3_service import init_s3_client

from cogs.recruitment import RecruitmentCog
from cogs.progress import ProgressCog
from cogs.invoice import InvoiceCog
from cogs.demolition import DemolitionCog
from cogs.eviction import EvictionCog
from cogs.scroll import ScrollCog
from cogs.admin import AdminCog
from cogs.approval import ApprovalCog
from cogs.leaderboard_stats import LeaderboardStatsCog
from cogs.form_edit import FormEditCog
from cogs.lookup import LookupCog
from utils.views import ApprovalView

# Configure logging before anything else
setup_logging(debug=False)

READY_FILE = Path("bot.ready")


class TownyBot(commands.Bot):
    """Main bot class for the Towny logging system."""

    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        super().__init__(command_prefix="!", intents=intents)

        self.db_pool = None
        self.s3_client = None
        self.logger = logging.getLogger(__name__)

    @tasks.loop(minutes=4)
    async def keep_db_alive(self):
        """Send a trivial query every 4 minutes to keep a connection warm."""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("SELECT 1")
        except Exception:
            pass  # ignore - the pool handles reconnects naturally

    async def setup_hook(self):
        """Initialize database pool, S3 client, and load all cogs."""
        self.logger.info("Starting setup_hook...")
        try:
            self.db_pool = await init_db_pool()
            self.s3_client = init_s3_client()
            self.keep_db_alive.start()          # fix #4: keep DB connection warm
        except Exception as e:
            self.logger.critical(f"Failed to initialize services: {e}")
            await self.close()
            sys.exit(1)

        # Load all cogs
        await self.add_cog(RecruitmentCog(self))
        await self.add_cog(ProgressCog(self))
        await self.add_cog(InvoiceCog(self))
        await self.add_cog(DemolitionCog(self))
        await self.add_cog(EvictionCog(self))
        await self.add_cog(ScrollCog(self))
        await self.add_cog(AdminCog(self))
        await self.add_cog(ApprovalCog(self))
        await self.add_cog(LeaderboardStatsCog(self))
        await self.add_cog(FormEditCog(self))
        await self.add_cog(LookupCog(self))

        # Register persistent ApprovalView for handling button interactions after restart
        # The view reconstructs its state from custom_id on interaction - bug #3 is handled there.
        persistent_view = ApprovalView(
            table='',
            form_id=0,
            form_type='',
            submitter_id=0,
            guild_id=0,
            channel_config_key='',
            thread_prefix=''
        )
        self.add_view(persistent_view)
        self.logger.info("Registered persistent ApprovalView.")

        # Sync slash commands globally
        await self.tree.sync()
        self.logger.info("Setup hook completed.")

    async def on_ready(self):
        """Called when the bot is connected and ready."""
        self.logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        # Mark bot as connected for the dashboard status endpoint
        READY_FILE.touch()

        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="town logs | /help"
            )
        )

    async def on_command_error(self, ctx, error):
        """Ignore CommandNotFound errors (prefix commands)."""
        if isinstance(error, commands.CommandNotFound):
            return
        self.logger.error(f"Command error: {error}")

    async def close(self):
        """Clean up resources before shutdown."""
        self.logger.info("Shutting down...")
        # Remove connected flag
        READY_FILE.unlink(missing_ok=True)
        if self.db_pool:
            await close_db_pool(self.db_pool)
        await super().close()


async def main():
    """Entry point: start the bot."""
    bot = TownyBot()
    try:
        async with bot:
            await bot.start(DISCORD_TOKEN)
    except discord.LoginFailure:
        logging.getLogger(__name__).critical("Invalid Discord token. Check your .env file.")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Bot stopped by user.")
    except Exception as e:
        logging.getLogger(__name__).exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())