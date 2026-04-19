import discord
from discord import ButtonStyle
from datetime import datetime, timezone
from services.db_service import DBService
from services.reputation_service import (
    award_submitter_points, award_helper_points, award_approval_points,
    extract_user_id_from_mention, SCROLL_POINTS, REP_POINTS
)
from services.thread_manager import ThreadManager
from services.s3_service import delete_image
from utils.logger import get_logger

logger = get_logger(__name__)


class ApprovalView(discord.ui.View):
    """Persistent view with Approve/Deny/Hold buttons for form approval."""

    _TABLE_PREFIX = {
        'recruitment': 'rec',
        'progress_report': 'rep',
        'purchase_invoice': 'inv',
        'demolition_report': 'dem',
        'demolition_request': 'dmr',
        'eviction_report': 'evc',
        'scroll_completion': 'scr'
    }

    _THREAD_PREFIX = {
        'recruitment': 'Recruitments',
        'progress_report': 'Progress Reports',
        'purchase_invoice': 'Invoices',
        'demolition_report': 'Demolitions',
        'demolition_request': 'Demolition Requests',
        'eviction_report': 'Evictions',
        'scroll_completion': 'Scrolls'
    }

    def __init__(self, table: str, form_id: int, form_type: str, submitter_id: int,
                 guild_id: int, channel_config_key: str, thread_prefix: str,
                 confirmation_msg_id: int = None, confirmation_channel_id: int = None,
                 form_data: dict = None,
                 resend_confirmation_msg_id: int = None, resend_confirmation_channel_id: int = None):
        super().__init__(timeout=None)
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
        self.resend_confirmation_msg_id = resend_confirmation_msg_id
        self.resend_confirmation_channel_id = resend_confirmation_channel_id

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.custom_id = f"{child.custom_id}_{table}_{form_id}"

    def _get_display_id(self) -> str:
        prefix = self._TABLE_PREFIX.get(self.table, 'unk')
        return f"{prefix}_{self.form_id}"

    async def _ensure_loaded_from_custom_id(self, interaction: discord.Interaction) -> bool:
        if not self.table or self.form_id == 0:
            button_id = interaction.data.get('custom_id', '')
            parts = button_id.split('_')
            if len(parts) >= 3:
                table_candidate = parts[-2]
                form_id_candidate = parts[-1]

                if table_candidate in self._TABLE_PREFIX:
                    self.table = table_candidate
                    try:
                        self.form_id = int(form_id_candidate)
                    except ValueError:
                        logger.error(f"Invalid form_id in custom_id: {button_id}")
                        return False

                    row = await DBService.fetchrow(
                        f"SELECT submitted_by FROM {self.table} WHERE id = $1",
                        self.form_id
                    )
                    if row:
                        self.submitter_id = row['submitted_by']
                        self.guild_id = interaction.guild_id
                        self.form_type = self.table
                        self.channel_config_key = f"{self.table}_channel_id"
                        self.thread_prefix = self._THREAD_PREFIX.get(
                            self.table, self.table.replace('_', ' ').title()
                        )
                        self.form_data = None
                        logger.info(f"Reconstructed ApprovalView for {self.table} #{self.form_id} after restart")
                        return True
                    else:
                        logger.warning(f"Form {self.table} #{self.form_id} not found in database")
                        return False
                else:
                    logger.warning(f"Unknown table '{table_candidate}' in custom_id: {button_id}")
                    return False
            else:
                logger.warning(f"Unexpected custom_id format: {button_id}")
                return False
        return True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not await self._ensure_loaded_from_custom_id(interaction):
            await interaction.response.send_message(
                "❌ Could not load form data. The form may have been deleted.",
                ephemeral=True
            )
            return False

        row = await DBService.fetchrow(
            f"SELECT status FROM {self.table} WHERE id = $1", self.form_id
        )
        if not row:
            await interaction.response.send_message(
                "⚠️ This form no longer exists.",
                ephemeral=True
            )
            for child in self.children:
                child.disabled = True
            await interaction.message.edit(view=self)
            return False

        status = row['status']
        # Allow interaction only for pending or held forms
        if status not in ('pending', 'hold'):
            await interaction.response.send_message(
                f"⚠️ This form has been **{status}**. No further actions allowed.",
                ephemeral=True
            )
            for child in self.children:
                child.disabled = True
            await interaction.message.edit(view=self)
            return False

        return True

    async def _is_authorized(self, interaction: discord.Interaction) -> bool:
        has_admin = await DBService.user_has_role(interaction.user.id, 'admin')
        has_comayor = await DBService.user_has_role(interaction.user.id, 'comayor')
        if has_admin or has_comayor:
            return True
        return interaction.user.guild_permissions.manage_guild

    async def _fetch_form_details(self) -> dict:
        columns = {
            'recruitment': ['submitted_by', 'ingame_username', 'nickname', 'plots', 'screenshot_urls', 'status', 'discord_username', 'age'],
            'progress_report': ['submitted_by', 'project_name', 'time_spent', 'helper_mentions', 'screenshot_urls', 'status'],
            'purchase_invoice': ['submitted_by', 'seller_display', 'purchasee_nickname', 'purchasee_ingame',
                                 'purchase_type', 'num_plots', 'total_plots', 'banner_color', 'shop_number',
                                 'amount_deposited', 'screenshot_urls', 'status'],
            'demolition_report': ['submitted_by', 'ingame_username', 'removed', 'stashed_items', 'screenshot_urls', 'status'],
            'demolition_request': ['submitted_by', 'ingame_username', 'reason', 'screenshot_urls', 'status'],
            'eviction_report': ['submitted_by', 'ingame_owner', 'items_stored', 'inactivity_period', 'screenshot_urls', 'status'],
            'scroll_completion': ['submitted_by', 'scroll_type', 'items_stored', 'screenshot_urls', 'status']
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

    async def _assign_player_role(self, interaction: discord.Interaction) -> tuple[bool, str]:
        """
        Assign the configured player role and set nickname in the community server
        for the recruited player.

        Returns:
            (success: bool, message: str) indicating outcome.
        """
        if not self.form_data:
            self.form_data = await self._fetch_form_details()

        discord_username = self.form_data.get('discord_username')
        if not discord_username:
            logger.warning(f"Recruitment #{self.form_id} approved but no Discord username provided.")
            return False, "No Discord username provided in the form."

        user_id = extract_user_id_from_mention(discord_username)
        if not user_id:
            try:
                user_id = int(discord_username.strip())
            except ValueError:
                logger.warning(f"Could not extract user ID from '{discord_username}'.")
                return False, f"Invalid Discord username format: '{discord_username}'."

        try:
            community_guild, player_role_id = await DBService.get_community_guild_and_role(
                interaction.client, self.guild_id
            )
        except ValueError as e:
            logger.error(f"Configuration error for role assignment: {e}")
            return False, f"Configuration error: {e}"

        role = community_guild.get_role(player_role_id)
        if not role:
            logger.error(f"Player role ID {player_role_id} not found in community guild {community_guild.name}.")
            return False, f"Configured player role (ID {player_role_id}) not found in the community server."

        try:
            member = await community_guild.fetch_member(user_id)
        except discord.NotFound:
            logger.warning(f"User {user_id} is not in community guild {community_guild.name}.")
            return False, f"User <@{user_id}> is not a member of the community server."
        except discord.Forbidden:
            logger.error(f"Bot lacks permission to fetch members in community guild {community_guild.name}.")
            return False, "Bot lacks 'Server Members Intent' or 'View Channel' permission in community server."
        except Exception as e:
            logger.exception(f"Unexpected error fetching member {user_id}: {e}")
            return False, f"Unexpected error fetching member: {e}"

        role_success = False
        nickname_success = False
        role_error = ""
        nick_error = ""

        # Assign role
        try:
            # Check role hierarchy
            bot_member = community_guild.me
            if bot_member.top_role <= role:
                role_error = f"Bot's top role ({bot_member.top_role.name}) is not higher than the player role ({role.name})."
                logger.error(role_error)
            else:
                await member.add_roles(role, reason=f"Recruitment approved (form #{self.form_id})")
                logger.info(f"Assigned role {role.name} to {member.display_name} ({member.id}).")
                role_success = True
        except discord.Forbidden:
            role_error = "Bot lacks 'Manage Roles' permission in community server (even with Admin, check role hierarchy)."
            logger.error(role_error)
        except Exception as e:
            role_error = f"Failed to assign role: {e}"
            logger.exception(role_error)

        # Change nickname to format: nickname (ingame_username)
        nickname = self.form_data.get('nickname', '').strip()
        ingame_username = self.form_data.get('ingame_username', '').strip()
        if nickname and ingame_username:
            desired_nick = f"{nickname} ({ingame_username})"
        elif nickname:
            desired_nick = nickname
        else:
            desired_nick = None

        if desired_nick:
            try:
                await member.edit(nick=desired_nick, reason=f"Recruitment approved (form #{self.form_id})")
                logger.info(f"Set nickname of {member.id} to '{desired_nick}'.")
                nickname_success = True
            except discord.Forbidden:
                nick_error = "Bot lacks 'Manage Nicknames' permission."
                logger.error(nick_error)
            except Exception as e:
                nick_error = f"Failed to set nickname: {e}"
                logger.exception(nick_error)

        # Build result message
        messages = []
        if role_success:
            messages.append(f"✅ Role '{role.name}' assigned.")
        elif role_error:
            messages.append(f"❌ Role not assigned: {role_error}")

        if nickname_success:
            messages.append(f"✅ Nickname set to '{desired_nick}'.")
        elif nick_error:
            messages.append(f"❌ Nickname not set: {nick_error}")

        if not messages:
            messages.append("ℹ️ No role or nickname changes attempted.")

        result_msg = " ".join(messages)
        success = role_success  # primary success is role assignment

        return success, result_msg

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
        now_utc = datetime.now(timezone.utc)
        timestamp_str = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
        display_id = self._get_display_id()
        screenshot_urls_str = self.form_data.get('screenshot_urls', '') if self.form_data else ''
        url_list = screenshot_urls_str.split(',') if screenshot_urls_str else []
        first_url = url_list[0] if url_list else None
        extra_count = len(url_list) - 1 if url_list else 0

        # Build rich embed for all non-recruitment forms
        embed = discord.Embed(
            title=f"✅ {self.form_type.replace('_', ' ').title()} Approved",
            color=discord.Color.green(),
            timestamp=now_utc
        )
        embed.add_field(name="Submitted by", value=submitter_name, inline=True)
        embed.add_field(name="Approved by", value=approver.display_name, inline=True)
        embed.add_field(name="Form ID", value=display_id, inline=True)

        # Add form-specific details
        if self.table == 'recruitment':
            nickname = self.form_data.get('nickname', '?')
            ingame = self.form_data.get('ingame_username', '?')
            plots = self.form_data.get('plots', 0)
            discord_user = self.form_data.get('discord_username')
            age = self.form_data.get('age')
            details = f"Recruited **{nickname}** ({ingame}) – {plots} plots"
            if discord_user:
                details += f"\n• Discord: {discord_user}"
            if age:
                details += f"\n• Age: {age}"
            embed.add_field(name="Details", value=details, inline=False)
            if first_url:
                embed.set_image(url=first_url)
                if extra_count > 0:
                    embed.add_field(name="Additional Screenshots", value=f"{extra_count} more", inline=False)
            embed.set_footer(text=f"Approved on {timestamp_str}")
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send recruitment embed: {e}")

        elif self.table == 'progress_report':
            project = self.form_data.get('project_name', '?')
            time_spent = self.form_data.get('time_spent', '?')
            helper = self.form_data.get('helper_mentions')
            embed.add_field(name="Project", value=project, inline=False)
            embed.add_field(name="Time Spent", value=time_spent, inline=True)
            if helper:
                embed.add_field(name="Helper", value=helper, inline=True)
            if first_url:
                embed.set_image(url=first_url)
                if extra_count > 0:
                    embed.add_field(name="Additional Screenshots", value=f"{extra_count} more", inline=False)
            embed.set_footer(text=f"Approved on {timestamp_str}")
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send progress embed: {e}")

        elif self.table == 'purchase_invoice':
            buyer_nick = self.form_data.get('purchasee_nickname', '?')
            buyer_ign = self.form_data.get('purchasee_ingame', '?')
            amount = self.form_data.get('amount_deposited', 0)
            purchase_type = self.form_data.get('purchase_type', '?')
            num_plots = self.form_data.get('num_plots')
            total_plots = self.form_data.get('total_plots')
            banner_color = self.form_data.get('banner_color')
            shop_number = self.form_data.get('shop_number')
            seller_display = self.form_data.get('seller_display', submitter_name)

            embed.add_field(name="Seller", value=seller_display, inline=True)
            embed.add_field(name="Buyer", value=f"{buyer_nick} ({buyer_ign})", inline=True)
            embed.add_field(name="Type", value=purchase_type, inline=True)
            embed.add_field(name="Amount", value=f"{amount} coins", inline=True)
            if num_plots:
                embed.add_field(name="Plots", value=f"{num_plots} (total: {total_plots})", inline=True)
            if banner_color:
                embed.add_field(name="Mall Shop", value=f"Color {banner_color} · #{shop_number}", inline=True)
            if first_url:
                embed.set_image(url=first_url)
                if extra_count > 0:
                    embed.add_field(name="Additional Screenshots", value=f"{extra_count} more", inline=False)
            embed.set_footer(text=f"Approved on {timestamp_str}")
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send invoice embed: {e}")

        elif self.table == 'demolition_report':
            player = self.form_data.get('ingame_username', '?')
            removed = self.form_data.get('removed', '?')
            stashed = "Yes" if self.form_data.get('stashed_items') else "No"
            embed.add_field(name="Player", value=player, inline=True)
            embed.add_field(name="Removed", value=removed, inline=True)
            embed.add_field(name="Items Stashed", value=stashed, inline=True)
            if first_url:
                embed.set_image(url=first_url)
                if extra_count > 0:
                    embed.add_field(name="Additional Screenshots", value=f"{extra_count} more", inline=False)
            embed.set_footer(text=f"Approved on {timestamp_str}")
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send demolition embed: {e}")

        elif self.table == 'demolition_request':
            player = self.form_data.get('ingame_username', '?')
            reason = self.form_data.get('reason', '?')
            embed.add_field(name="Target Player", value=player, inline=True)
            embed.add_field(name="Reason", value=reason, inline=False)
            if first_url:
                embed.set_image(url=first_url)
                if extra_count > 0:
                    embed.add_field(name="Additional Screenshots", value=f"{extra_count} more", inline=False)
            embed.set_footer(text=f"Approved on {timestamp_str}")
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send demolition request embed: {e}")

        elif self.table == 'eviction_report':
            owner = self.form_data.get('ingame_owner', '?')
            items_stored = "Yes" if self.form_data.get('items_stored') else "No"
            inactivity = self.form_data.get('inactivity_period', '?')
            embed.add_field(name="Owner", value=owner, inline=True)
            embed.add_field(name="Items Stored", value=items_stored, inline=True)
            embed.add_field(name="Inactivity Period", value=inactivity, inline=True)
            if first_url:
                embed.set_image(url=first_url)
                if extra_count > 0:
                    embed.add_field(name="Additional Screenshots", value=f"{extra_count} more", inline=False)
            embed.set_footer(text=f"Approved on {timestamp_str}")
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send eviction embed: {e}")

        elif self.table == 'scroll_completion':
            scroll_type = self.form_data.get('scroll_type', '?').capitalize()
            items_stored = "Yes" if self.form_data.get('items_stored') else "No"
            embed.add_field(name="Scroll Type", value=scroll_type, inline=True)
            embed.add_field(name="Items Stored", value=items_stored, inline=True)
            if first_url:
                embed.set_image(url=first_url)
                if extra_count > 0:
                    embed.add_field(name="Additional Screenshots", value=f"{extra_count} more", inline=False)
            embed.set_footer(text=f"Approved on {timestamp_str}")
            try:
                thread = await ThreadManager.get_or_create_monthly_thread(guild, channel_id, self.thread_prefix)
                await thread.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send scroll embed: {e}")

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

    async def _cleanup_messages(self, interaction: discord.Interaction):
        """Delete all associated confirmation messages and the approval message itself."""
        if self.confirmation_msg_id and self.confirmation_channel_id:
            try:
                channel = interaction.client.get_channel(self.confirmation_channel_id)
                if channel:
                    msg = await channel.fetch_message(self.confirmation_msg_id)
                    await msg.delete()
                    logger.debug(f"Deleted original confirmation message {self.confirmation_msg_id}")
            except discord.NotFound:
                logger.debug(f"Original confirmation message {self.confirmation_msg_id} already deleted")
            except Exception as e:
                logger.warning(f"Failed to delete original confirmation message {self.confirmation_msg_id}: {e}")

        if self.resend_confirmation_msg_id and self.resend_confirmation_channel_id:
            try:
                channel = interaction.client.get_channel(self.resend_confirmation_channel_id)
                if channel:
                    msg = await channel.fetch_message(self.resend_confirmation_msg_id)
                    await msg.delete()
                    logger.debug(f"Deleted resend confirmation message {self.resend_confirmation_msg_id}")
            except discord.NotFound:
                logger.debug(f"Resend confirmation message {self.resend_confirmation_msg_id} already deleted")
            except Exception as e:
                logger.warning(f"Failed to delete resend confirmation message {self.resend_confirmation_msg_id}: {e}")

        try:
            await interaction.message.delete()
        except discord.NotFound:
            pass
        except Exception as e:
            logger.warning(f"Failed to delete approval message: {e}")

    async def _handle_approval(self, interaction: discord.Interaction, approve: bool, hold: bool = False):
        await interaction.response.defer()
        display_id = self._get_display_id()

        if not await self._is_authorized(interaction):
            await interaction.followup.send(
                "❌ You don't have permission to approve/deny forms. (Requires Admin or Comayor role)",
                ephemeral=True
            )
            return

        for child in self.children:
            child.disabled = True
        await interaction.edit_original_response(view=self)

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
                # Determine submitter points (with possible override for scrolls)
                points_override = None
                if self.table == 'scroll_completion':
                    scroll_type = (self.form_data or {}).get('scroll_type', '').lower()
                    points_override = SCROLL_POINTS.get(scroll_type, REP_POINTS.get('scroll_completion', 5))

                await award_submitter_points(self.submitter_id, self.form_type, self.form_id, points_override)

                if self.form_type == 'progress_report' and self.form_data:
                    helper_mention = self.form_data.get('helper_mentions')
                    if helper_mention:
                        await award_helper_points(helper_mention, self.form_id)
                await award_approval_points(interaction.user.id, self.form_type, self.form_id)
                await DBService.approve_form(self.table, self.form_id, interaction.user.id)

                # Cross-server role assignment for recruitment approvals
                role_result_msg = ""
                if self.table == 'recruitment':
                    try:
                        success, role_result_msg = await self._assign_player_role(interaction)
                    except Exception as e:
                        logger.exception(f"Unexpected error in role assignment for recruitment #{self.form_id}: {e}")
                        role_result_msg = f"❌ Unexpected error during role assignment: {e}"

                await self._send_notification(interaction.guild, interaction.user)
                await self._cleanup_messages(interaction)

                # Build final confirmation message
                base_msg = f"✅ **Form {display_id} approved** by {interaction.user.display_name}."
                if role_result_msg:
                    base_msg += f"\n{role_result_msg}"

                await interaction.followup.send(base_msg, ephemeral=True)
            elif hold:
                await DBService.hold_form(self.table, self.form_id)
                for child in self.children:
                    child.disabled = False
                await interaction.edit_original_response(view=self)
                await interaction.followup.send(
                    f"⏸️ **Form {display_id} put on hold** by {interaction.user.display_name}.\n"
                    "*You can approve or deny it later using the buttons below.*",
                    ephemeral=True
                )
            else:
                await DBService.deny_form(self.table, self.form_id)
                await self._delete_form_images()
                await self._cleanup_messages(interaction)
                await interaction.followup.send(
                    f"❌ **Form {display_id} denied** by {interaction.user.display_name}.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error handling approval: {e}", exc_info=True)
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