import discord
from discord import ButtonStyle
from datetime import datetime, timezone
from services.db_service import DBService
from services.reputation_service import award_submitter_points, award_helper_points, award_approval_points
from services.thread_manager import ThreadManager
from services.s3_service import delete_image
from utils.logger import get_logger

logger = get_logger(__name__)


class ApprovalView(discord.ui.View):
    """View with Approve/Deny/Hold buttons for form approval."""

    def __init__(self, table: str, form_id: int, form_type: str, submitter_id: int,
                 guild_id: int, channel_config_key: str, thread_prefix: str,
                 confirmation_msg_id: int = None, confirmation_channel_id: int = None,
                 form_data: dict = None):
        super().__init__(timeout=604800)
        self.table = table
        self.form_id = form_id
        self.form_type = form_type
        self.submitter_id = submitter_id
        self.guild_id = guild_id
        self.channel_config_key = channel_config_key
        self.thread_prefix = thread_prefix
        self.confirmation_msg_id = confirmation_msg_id
        self.confirmation_channel_id = confirmation_channel_id
        self.form_data = form_data

    async def _is_authorized(self, interaction: discord.Interaction) -> bool:
        has_admin = await DBService.user_has_role(interaction.user.id, 'admin')
        has_comayor = await DBService.user_has_role(interaction.user.id, 'comayor')
        if has_admin or has_comayor:
            return True
        return interaction.user.guild_permissions.manage_guild

    async def _fetch_form_details(self) -> dict:
        columns = {
            'recruitment': ['submitted_by', 'ingame_username', 'nickname', 'plots', 'screenshot_urls', 'status', 'discord_username'],
            'progress_report': ['submitted_by', 'project_name', 'time_spent', 'helper_mentions', 'screenshot_urls', 'status'],
            'purchase_invoice': ['submitted_by', 'purchasee_nickname', 'purchasee_ingame', 'amount_deposited', 'screenshot_urls', 'status'],
            'demolition_report': ['submitted_by', 'ingame_username', 'removed', 'screenshot_urls', 'status'],
            'demolition_request': ['submitted_by', 'ingame_username', 'reason', 'screenshot_urls', 'status'],
            'eviction_report': ['submitted_by', 'ingame_owner', 'inactivity_period', 'screenshot_urls', 'status'],
            'scroll_completion': ['submitted_by', 'scroll_type', 'screenshot_urls', 'status']
        }
        cols = columns.get(self.table, ['submitted_by'])
        select_cols = ", ".join(cols)
        query = f"SELECT {select_cols} FROM {self.table} WHERE id = $1"
        row = await DBService.fetchrow(query, self.form_id)
        return dict(row) if row else {}

    async def _delete_form_images(self):
        if not self.form_data:
            self.form_data = await self._fetch_form_details()
        urls_str = self.form_data.get('screenshot_urls', '')
        if urls_str:
            for url in urls_str.split(','):
                await delete_image(url)
            logger.info(f"Deleted images for form {self.form_id}")

    async def _send_notification(self, guild: discord.Guild, approver: discord.Member):
        config = await DBService.get_guild_config(self.guild_id)
        if not config:
            logger.warning(f"No guild config for {self.guild_id}, cannot send notification.")
            return
        channel_id = config.get(self.channel_config_key)
        if not channel_id:
            logger.warning(f"No channel configured for key {self.channel_config_key}")
            return

        fresh_data = await self._fetch_form_details()
        if fresh_data:
            self.form_data = fresh_data

        submitter = guild.get_member(self.submitter_id)
        submitter_name = submitter.display_name if submitter else f"User {self.submitter_id}"
        summary = self._build_summary()
        screenshot_urls_str = self.form_data.get('screenshot_urls') if self.form_data else ''
        url_list = screenshot_urls_str.split(',') if screenshot_urls_str else []
        first_url = url_list[0] if url_list else None
        extra_count = len(url_list) - 1 if url_list else 0
        now_utc = datetime.now(timezone.utc)
        timestamp_str = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')

        if self.table == 'recruitment':
            notification = (
                f"✅ **{self.form_type.replace('_', ' ').title()} Approved**\n"
                f"• **Submitted by:** {submitter_name}\n"
                f"• **Approved by:** {approver.display_name}\n"
                f"• **Form ID:** {self.form_id}\n"
                f"• **Details:** {summary}\n"
                f"• **Timestamp:** {timestamp_str}"
            )
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(notification)
            except Exception as e:
                logger.error(f"Failed to send notification: {e}")
        else:
            embed = discord.Embed(
                title=f"✅ {self.form_type.replace('_', ' ').title()} Approved",
                color=discord.Color.green(),
                timestamp=now_utc
            )
            embed.add_field(name="Submitted by", value=submitter_name, inline=True)
            embed.add_field(name="Approved by", value=approver.display_name, inline=True)
            embed.add_field(name="Form ID", value=str(self.form_id), inline=True)
            embed.add_field(name="Details", value=summary, inline=False)

            if first_url:
                embed.set_image(url=first_url)
                if extra_count > 0:
                    embed.add_field(name="Additional Screenshots", value=f"{extra_count} more", inline=False)

            embed.set_footer(text=f"Approved on {timestamp_str}")
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send embed notification: {e}")

    def _build_summary(self) -> str:
        if not self.form_data:
            return f"ID {self.form_id}"
        if self.table == 'recruitment':
            return (f"Recruited {self.form_data.get('nickname', '?')} "
                    f"({self.form_data.get('ingame_username', '?')}) - "
                    f"{self.form_data.get('plots', 0)} plots")
        if self.table == 'progress_report':
            return (f"Project '{self.form_data.get('project_name', '?')}' - "
                    f"{self.form_data.get('time_spent', '?')}")
        if self.table == 'purchase_invoice':
            return (f"Sale to {self.form_data.get('purchasee_nickname', '?')} for "
                    f"{self.form_data.get('amount_deposited', 0)} coins")
        if self.table == 'demolition_report':
            return (f"Demolished {self.form_data.get('ingame_username', '?')} - "
                    f"{self.form_data.get('removed', '?')}")
        if self.table == 'demolition_request':
            reason = self.form_data.get('reason', '?')[:50]
            return f"Request to demolish {self.form_data.get('ingame_username', '?')} - Reason: {reason}"
        if self.table == 'eviction_report':
            return (f"Evicted {self.form_data.get('ingame_owner', '?')} - "
                    f"Inactive {self.form_data.get('inactivity_period', '?')}")
        if self.table == 'scroll_completion':
            return f"Scroll type: {self.form_data.get('scroll_type', '?')}"
        return f"Form ID {self.form_id}"

    async def _handle_approval(self, interaction: discord.Interaction, approve: bool, hold: bool = False):
        await interaction.response.defer()

        # Delete the confirmation message from its original channel
        if self.confirmation_msg_id and self.confirmation_channel_id:
            try:
                channel = interaction.client.get_channel(self.confirmation_channel_id)
                if channel:
                    msg = await channel.fetch_message(self.confirmation_msg_id)
                    await msg.delete()
                    logger.debug(f"Deleted confirmation message {self.confirmation_msg_id}")
            except discord.NotFound:
                logger.debug(f"Confirmation message {self.confirmation_msg_id} already deleted")
            except Exception as e:
                logger.warning(f"Failed to delete confirmation message {self.confirmation_msg_id}: {e}")

        if not await self._is_authorized(interaction):
            await interaction.followup.send(
                "❌ You don't have permission to approve/deny forms. (Requires Admin or Comayor role)",
                ephemeral=True
            )
            return

        # Disable buttons
        for child in self.children:
            child.disabled = True
        try:
            await interaction.edit_original_response(view=self)
        except discord.NotFound:
            # Original message may have been deleted already
            pass

        current = await self._fetch_form_details()
        current_status = current.get('status') if current else None
        if current_status in ('approved', 'denied'):
            await interaction.followup.send(
                f"⚠️ This form has already been **{current_status}**. No further action needed.",
                ephemeral=True
            )
            return
        if hold and current_status == 'hold':
            await interaction.followup.send("⚠️ This form is already on hold.", ephemeral=True)
            return

        try:
            if approve:
                await award_submitter_points(self.submitter_id, self.form_type, self.form_id)
                if self.form_type == 'progress_report' and self.form_data:
                    helper_mention = self.form_data.get('helper_mentions')
                    if helper_mention:
                        await award_helper_points(helper_mention, self.form_id)
                await award_approval_points(interaction.user.id, self.form_type, self.form_id)
                await DBService.approve_form(self.table, self.form_id, interaction.user.id)
                await self._send_notification(interaction.guild, interaction.user)

                # Delete the approval embed message (ignore if already gone)
                try:
                    await interaction.message.delete()
                except (discord.NotFound, AttributeError):
                    pass
                await interaction.followup.send(
                    f"✅ **Form #{self.form_id} approved** by {interaction.user.display_name}.",
                    ephemeral=True
                )
            elif hold:
                await DBService.hold_form(self.table, self.form_id)
                for child in self.children:
                    child.disabled = False
                try:
                    await interaction.edit_original_response(view=self)
                except discord.NotFound:
                    pass
                await interaction.followup.edit_message(
                    message_id=interaction.message.id,
                    content=(
                        f"⏸️ **Form #{self.form_id} put on hold** by {interaction.user.display_name}.\n"
                        "*You can approve or deny it later using the buttons below.*"
                    ),
                    view=self
                )
            else:
                await DBService.deny_form(self.table, self.form_id)
                await self._delete_form_images()
                try:
                    await interaction.message.delete()
                except (discord.NotFound, AttributeError):
                    pass
                await interaction.followup.send(
                    f"❌ **Form #{self.form_id} denied** by {interaction.user.display_name}.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error handling approval: {e}", exc_info=True)
            # Re‑enable buttons on error
            for child in self.children:
                child.disabled = False
            try:
                await interaction.edit_original_response(view=self)
            except discord.NotFound:
                pass
            await interaction.followup.send(
                "⚠️ An error occurred. Please try again later.",
                ephemeral=True
            )

    @discord.ui.button(label="Approve", style=ButtonStyle.success, emoji="✅", custom_id="approve_button")
    async def approve_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_approval(interaction, approve=True, hold=False)

    @discord.ui.button(label="Deny", style=ButtonStyle.danger, emoji="❌", custom_id="deny_button")
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_approval(interaction, approve=False, hold=False)

    @discord.ui.button(label="Hold", style=ButtonStyle.secondary, emoji="⏸️", custom_id="hold_button")
    async def hold_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_approval(interaction, approve=False, hold=True)