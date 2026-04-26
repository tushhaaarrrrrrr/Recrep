from database.connection import get_db_pool
from typing import Optional, List, Dict, Any
import asyncpg
import re
from datetime import datetime
from utils.logger import get_logger
from config.points import REP_POINTS, SCROLL_POINTS

logger = get_logger(__name__)


def extract_user_id_from_mention(mention: str) -> int:
    match = re.search(r'<@!?(\d+)>', mention)
    if match:
        return int(match.group(1))
    return None


class DBService:
    # Core database helpers
    @staticmethod
    async def execute(query: str, *args) -> Any:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            return await conn.execute(query, *args)

    @staticmethod
    async def fetch(query: str, *args) -> List[asyncpg.Record]:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @staticmethod
    async def fetchrow(query: str, *args) -> Optional[asyncpg.Record]:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    # Guild configuration
    @staticmethod
    async def get_guild_config(guild_id: int) -> Optional[Dict]:
        row = await DBService.fetchrow(
            "SELECT * FROM guild_config WHERE guild_id = $1", guild_id
        )
        return dict(row) if row else None

    @staticmethod
    async def set_guild_config(guild_id: int, **kwargs):
        cols = ", ".join(kwargs.keys())
        values = [guild_id] + list(kwargs.values())
        query = f"""
            INSERT INTO guild_config (guild_id, {cols})
            VALUES ({', '.join(['$1'] + [f'${i+2}' for i in range(len(kwargs))])})
            ON CONFLICT (guild_id) DO UPDATE SET
            {', '.join(f"{k} = EXCLUDED.{k}" for k in kwargs)}
        """
        await DBService.execute(query, *values)

    @staticmethod
    async def get_community_guild_and_role(bot, staff_guild_id: int) -> tuple:
        """
        Retrieve the community guild object and player role ID from config.

        Args:
            bot: The Discord bot instance.
            staff_guild_id: The ID of the staff server (guild where the command/config is).

        Returns:
            (community_guild, player_role_id) where community_guild is a discord.Guild or None.
        Raises:
            ValueError: If config is missing or invalid.
        """
        config = await DBService.get_guild_config(staff_guild_id)
        if not config:
            raise ValueError("No guild configuration found for this server.")
        community_guild_id = config.get('community_guild_id')
        if not community_guild_id:
            raise ValueError("Community guild not configured. Use `/set_community_guild`.")
        player_role_id = config.get('player_role_id')
        if not player_role_id:
            raise ValueError("Player role not configured. Use `/set_player_role`.")

        community_guild = bot.get_guild(community_guild_id)
        if not community_guild:
            raise ValueError(f"Bot is not in the configured community guild (ID {community_guild_id}).")
        return community_guild, player_role_id

    # Staff member management
    @staticmethod
    async def ensure_staff_member(discord_id: int, display_name: str):
        """Insert or update staff member. Only update display_name if a non-empty value is provided."""
        # First try to insert
        await DBService.execute(
            """
            INSERT INTO staff_member (discord_id, display_name)
            VALUES ($1, $2)
            ON CONFLICT (discord_id) DO NOTHING
            """,
            discord_id, display_name
        )
        # If a display name was provided, update it (only if it's non-empty)
        if display_name and display_name.strip():
            await DBService.execute(
                "UPDATE staff_member SET display_name = $1 WHERE discord_id = $2",
                display_name, discord_id
            )

    # Insert forms
    @staticmethod
    async def insert_recruitment(data: Dict) -> int:
        display_name = data.get('submitter_display', data.get('recruiter_display', ''))
        await DBService.ensure_staff_member(data['submitted_by'], display_name)
        row = await DBService.fetchrow(
            """
            INSERT INTO recruitment (submitted_by, ingame_username, discord_username, age,
                                     nickname, recruiter_display, plots, screenshot_urls)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            """,
            data['submitted_by'], data['ingame_username'], data.get('discord_username'),
            data.get('age'), data['nickname'], data['recruiter_display'],
            data['plots'], data['screenshot_urls']
        )
        return row['id']

    @staticmethod
    async def insert_progress(data: Dict) -> int:
        display_name = data.get('submitter_display', '')
        await DBService.ensure_staff_member(data['submitted_by'], display_name)
        row = await DBService.fetchrow(
            """
            INSERT INTO progress_report (submitted_by, helper_mentions, project_name,
                                         time_spent, screenshot_urls)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            data['submitted_by'], data.get('helper_mentions'), data['project_name'],
            data['time_spent'], data['screenshot_urls']
        )
        return row['id']

    @staticmethod
    async def insert_invoice(data: Dict) -> int:
        display_name = data.get('submitter_display', data.get('seller_display', ''))
        await DBService.ensure_staff_member(data['submitted_by'], display_name)
        row = await DBService.fetchrow(
            """
            INSERT INTO purchase_invoice (
                submitted_by, seller_display, purchasee_nickname, purchasee_ingame,
                purchase_type, num_plots, total_plots, banner_color, shop_number,
                amount_deposited, screenshot_urls
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
            """,
            data['submitted_by'], data['seller_display'], data['purchasee_nickname'],
            data['purchasee_ingame'], data['purchase_type'], data.get('num_plots'),
            data.get('total_plots'), data.get('banner_color'), data.get('shop_number'),
            data.get('amount_deposited'), data['screenshot_urls']
        )
        return row['id']

    @staticmethod
    async def insert_demolition(data: Dict) -> int:
        display_name = data.get('submitter_display', '')
        await DBService.ensure_staff_member(data['submitted_by'], display_name)
        row = await DBService.fetchrow(
            """
            INSERT INTO demolition_report (submitted_by, ingame_username, removed,
                                          stashed_items, screenshot_urls)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            data['submitted_by'], data['ingame_username'], data['removed'],
            data['stashed_items'], data['screenshot_urls']
        )
        return row['id']

    @staticmethod
    async def insert_demolition_request(data: Dict) -> int:
        display_name = data.get('submitter_display', '')
        await DBService.ensure_staff_member(data['submitted_by'], display_name)
        row = await DBService.fetchrow(
            """
            INSERT INTO demolition_request (submitted_by, ingame_username, reason, screenshot_urls)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            data['submitted_by'], data['ingame_username'], data['reason'], data['screenshot_urls']
        )
        return row['id']

    @staticmethod
    async def insert_eviction(data: Dict) -> int:
        display_name = data.get('submitter_display', '')
        await DBService.ensure_staff_member(data['submitted_by'], display_name)
        row = await DBService.fetchrow(
            """
            INSERT INTO eviction_report (submitted_by, ingame_owner, items_stored,
                                        inactivity_period, screenshot_urls)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            data['submitted_by'], data['ingame_owner'], data['items_stored'],
            data['inactivity_period'], data['screenshot_urls']
        )
        return row['id']

    @staticmethod
    async def insert_scroll(data: Dict) -> int:
        display_name = data.get('submitter_display', '')
        await DBService.ensure_staff_member(data['submitted_by'], display_name)
        row = await DBService.fetchrow(
            """
            INSERT INTO scroll_completion (submitted_by, scroll_type, items_stored, screenshot_urls)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            data['submitted_by'], data['scroll_type'], data['items_stored'], data['screenshot_urls']
        )
        return row['id']

    # Approval actions
    @staticmethod
    async def approve_form(table: str, form_id: int, approver_id: int):
        await DBService.execute(
            f"UPDATE {table} SET status = 'approved', approved_by = $1, approved_at = NOW() WHERE id = $2",
            approver_id, form_id
        )

    @staticmethod
    async def deny_form(table: str, form_id: int):
        await DBService.execute(f"UPDATE {table} SET status = 'denied' WHERE id = $1", form_id)

    @staticmethod
    async def hold_form(table: str, form_id: int):
        await DBService.execute(f"UPDATE {table} SET status = 'hold' WHERE id = $1", form_id)

    @staticmethod
    async def get_pending_form(table: str, form_id: int) -> Optional[Dict]:
        row = await DBService.fetchrow(
            f"SELECT * FROM {table} WHERE id = $1 AND status = 'pending'", form_id
        )
        return dict(row) if row else None

    @staticmethod
    async def set_thread_message_id(table: str, form_id: int, message_id: int):
        await DBService.execute(
            f"UPDATE {table} SET thread_message_id = $1 WHERE id = $2", message_id, form_id
        )

    @staticmethod
    async def set_approval_message_id(table: str, form_id: int, message_id: int):
        await DBService.execute(
            f"UPDATE {table} SET approval_message_id = $1 WHERE id = $2",
            message_id, form_id
        )

    @staticmethod
    async def get_approval_message_id(table: str, form_id: int) -> Optional[int]:
        row = await DBService.fetchrow(
            f"SELECT approval_message_id FROM {table} WHERE id = $1", form_id
        )
        return row['approval_message_id'] if row else None

    @staticmethod
    async def get_full_form_data(table: str, form_id: int) -> Optional[Dict]:
        row = await DBService.fetchrow(f"SELECT * FROM {table} WHERE id = $1", form_id)
        return dict(row) if row else None

    # NEW: Bulk fetch all pending forms (patched)
    @staticmethod
    async def get_all_pending_forms() -> List[Dict]:
        """Return a list of all pending forms across all tables."""
        TABLE_CONFIG = {
            'recruitment':        ('',),
            'progress_report':    ('helper_mentions',),
            'purchase_invoice':   ('',),
            'demolition_report':  ('',),
            'demolition_request': ('',),
            'eviction_report':    ('',),
            'scroll_completion':  ('scroll_type',),
        }
        results = []
        for table, extras in TABLE_CONFIG.items():
            extra_cols = ', ' + ', '.join(extras) if extras and extras[0] else ''
            query = f"SELECT id, submitted_by, approval_message_id{extra_cols} FROM {table} WHERE status = 'pending'"
            rows = await DBService.fetch(query)
            for row in rows:
                result = {
                    'table': table,
                    'id': row['id'],
                    'submitted_by': row['submitted_by'],
                    'approval_message_id': row.get('approval_message_id'),
                    'helper_mentions': None,
                    'scroll_type': None,
                }
                if 'helper_mentions' in row.keys():
                    result['helper_mentions'] = row['helper_mentions']
                if 'scroll_type' in row.keys():
                    result['scroll_type'] = row['scroll_type']
                results.append(result)
        return results

    # Reputation and leaderboards
    @staticmethod
    async def add_reputation(staff_id: int, points: int, reason: str, form_type: str, form_id: int, created_at: datetime = None):
        await DBService.ensure_staff_member(staff_id, "")
        if created_at is None:
            await DBService.execute(
                "INSERT INTO reputation_log (staff_id, points, reason, form_type, form_id) "
                "VALUES ($1, $2, $3, $4, $5)",
                staff_id, points, reason, form_type, form_id
            )
        else:
            await DBService.execute(
                "INSERT INTO reputation_log (staff_id, points, reason, form_type, form_id, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                staff_id, points, reason, form_type, form_id, created_at
            )
        await DBService.execute(
            "UPDATE staff_member SET reputation = reputation + $1 WHERE discord_id = $2",
            points, staff_id
        )

    @staticmethod
    async def get_leaderboard(period: str, limit: int = 10) -> List[Dict]:
        if period == 'weekly':
            view = 'weekly_reputation'
        elif period == 'biweekly':
            view = 'biweekly_reputation'
        elif period == 'monthly':
            view = 'monthly_reputation'
        else:
            rows = await DBService.fetch(
                "SELECT discord_id, reputation AS points FROM staff_member ORDER BY reputation DESC LIMIT $1",
                limit
            )
            return [{'discord_id': r['discord_id'], 'points': r['points']} for r in rows]

        rows = await DBService.fetch(
            f"SELECT staff_id, points FROM {view} ORDER BY points DESC LIMIT $1", limit
        )
        return [{'discord_id': r['staff_id'], 'points': r['points']} for r in rows]

    @staticmethod
    async def get_user_points_breakdown(discord_id: int) -> Dict[str, int]:
        rows = await DBService.fetch(
            "SELECT form_type, SUM(points) AS total FROM reputation_log WHERE staff_id = $1 GROUP BY form_type",
            discord_id
        )
        return {row['form_type']: row['total'] for row in rows}

    @staticmethod
    async def get_user_stats(discord_id: int) -> Dict:
        stats = {}
        tables = [
            'recruitment', 'progress_report', 'purchase_invoice',
            'demolition_report', 'demolition_request', 'eviction_report',
            'scroll_completion'
        ]
        for table in tables:
            count = await DBService.fetchrow(
                f"SELECT COUNT(*) FROM {table} WHERE submitted_by = $1 AND status = 'approved'",
                discord_id
            )
            stats[table] = count[0] if count else 0

        approval_count = await DBService.fetchrow(
            "SELECT COUNT(*) FROM reputation_log WHERE staff_id = $1 AND form_type LIKE '%_approval'",
            discord_id
        )
        stats['approval_count'] = approval_count[0] if approval_count else 0

        help_count = await DBService.fetchrow(
            "SELECT COUNT(*) FROM reputation_log WHERE staff_id = $1 AND form_type = 'progress_help'",
            discord_id
        )
        stats['progress_help'] = help_count[0] if help_count else 0

        rep = await DBService.fetchrow(
            "SELECT reputation FROM staff_member WHERE discord_id = $1", discord_id
        )
        stats['reputation'] = rep['reputation'] if rep else 0
        return stats

    @staticmethod
    async def get_help_leaderboard(period: str, limit: int = 10) -> List[Dict]:
        if period == 'weekly':
            time_filter = "created_at >= date_trunc('week', CURRENT_DATE)"
        elif period == 'biweekly':
            time_filter = "created_at >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'"
        elif period == 'monthly':
            time_filter = "created_at >= date_trunc('month', CURRENT_DATE)"
        else:
            time_filter = "TRUE"

        rows = await DBService.fetch(
            f"""
            SELECT staff_id, COUNT(*) as count
            FROM reputation_log
            WHERE form_type = 'progress_help' AND {time_filter}
            GROUP BY staff_id
            ORDER BY count DESC
            LIMIT $1
            """,
            limit
        )
        return [{'discord_id': r['staff_id'], 'count': r['count']} for r in rows]

    @staticmethod
    async def get_category_leaderboard(category: str, period: str, limit: int = 10) -> List[Dict]:
        if category == 'progress_help':
            return await DBService.get_help_leaderboard(period, limit)

        table_map = {
            'recruitment': 'recruitment',
            'progress_report': 'progress_report',
            'purchase_invoice': 'purchase_invoice',
            'demolition_report': 'demolition_report',
            'eviction_report': 'eviction_report',
            'scroll_completion': 'scroll_completion'
        }
        table = table_map.get(category)
        if not table:
            return []

        if period == 'weekly':
            time_filter = "submitted_at >= date_trunc('week', CURRENT_DATE)"
        elif period == 'biweekly':
            time_filter = "submitted_at >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'"
        elif period == 'monthly':
            time_filter = "submitted_at >= date_trunc('month', CURRENT_DATE)"
        else:
            time_filter = "TRUE"

        query = f"""
            SELECT submitted_by AS discord_id, COUNT(*) AS count
            FROM {table}
            WHERE status = 'approved' AND {time_filter}
            GROUP BY submitted_by
            ORDER BY count DESC
            LIMIT $1
        """
        rows = await DBService.fetch(query, limit)
        return [dict(row) for row in rows]

    @staticmethod
    async def get_user_detailed_stats(discord_id: int, period: str = 'all') -> Dict:
        # Time filter for form submissions
        if period == 'weekly':
            time_filter = "submitted_at >= date_trunc('week', CURRENT_DATE)"
        elif period == 'biweekly':
            time_filter = "submitted_at >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'"
        elif period == 'monthly':
            time_filter = "submitted_at >= date_trunc('month', CURRENT_DATE)"
        else:
            time_filter = "TRUE"

        stats = {}
        tables = [
            'recruitment', 'progress_report', 'purchase_invoice',
            'demolition_report', 'demolition_request', 'eviction_report',
            'scroll_completion'
        ]
        for table in tables:
            count = await DBService.fetchrow(
                f"SELECT COUNT(*) FROM {table} WHERE submitted_by = $1 AND status = 'approved' AND {time_filter}",
                discord_id
            )
            stats[table] = count[0] if count else 0

        # Time filter for reputation log
        if period == 'weekly':
            rep_time_filter = "created_at >= date_trunc('week', CURRENT_DATE)"
        elif period == 'biweekly':
            rep_time_filter = "created_at >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'"
        elif period == 'monthly':
            rep_time_filter = "created_at >= date_trunc('month', CURRENT_DATE)"
        else:
            rep_time_filter = "TRUE"

        approval_count = await DBService.fetchrow(
            f"SELECT COUNT(*) FROM reputation_log WHERE staff_id = $1 AND form_type LIKE '%_approval' AND {rep_time_filter}",
            discord_id
        )
        stats['approval_count'] = approval_count[0] if approval_count else 0

        help_count = await DBService.fetchrow(
            f"SELECT COUNT(*) FROM reputation_log WHERE staff_id = $1 AND form_type = 'progress_help' AND {rep_time_filter}",
            discord_id
        )
        stats['progress_help'] = help_count[0] if help_count else 0

        rep_points = await DBService.fetchrow(
            f"SELECT COALESCE(SUM(points), 0) AS total FROM reputation_log WHERE staff_id = $1 AND {rep_time_filter}",
            discord_id
        )
        stats['reputation'] = rep_points['total'] if rep_points else 0

        breakdown_rows = await DBService.fetch(
            f"SELECT form_type, SUM(points) AS total FROM reputation_log WHERE staff_id = $1 AND {rep_time_filter} GROUP BY form_type",
            discord_id
        )
        stats['points_breakdown'] = {row['form_type']: row['total'] for row in breakdown_rows}

        return stats

    @staticmethod
    async def refresh_all_reputation():
        """Rebuild reputation_log and staff_member.reputation from all approved forms, preserving original timestamps."""
        # Clear existing data
        await DBService.execute("TRUNCATE reputation_log")
        await DBService.execute("UPDATE staff_member SET reputation = 0")
        logger.info("Cleared reputation_log and reset staff_member.reputation to 0.")

        # Process each form table (with original timestamps)
        form_config = [
            ('recruitment', REP_POINTS['recruitment'], 'recruitment'),
            ('progress_report', REP_POINTS['progress_report'], 'progress_report'),
            ('purchase_invoice', REP_POINTS['purchase_invoice'], 'purchase_invoice'),
            ('demolition_report', REP_POINTS['demolition_report'], 'demolition_report'),
            ('demolition_request', REP_POINTS['demolition_request'], 'demolition_request'),
            ('eviction_report', REP_POINTS['eviction_report'], 'eviction_report'),
        ]
        for table, points, form_type in form_config:
            rows = await DBService.fetch(
                f"SELECT id, submitted_by, approved_by, submitted_at, approved_at FROM {table} WHERE status = 'approved'"
            )
            logger.info(f"Processing {len(rows)} approved rows from {table} ({points} pts each)")
            for row in rows:
                # Submitter points with original submission timestamp
                await DBService.add_reputation(
                    row['submitted_by'], points, f"Submitted {form_type}", form_type, row['id'],
                    created_at=row['submitted_at']
                )
                # Approver points with original approval timestamp
                if row['approved_by'] and row['approved_at']:
                    await DBService.add_reputation(
                        row['approved_by'], REP_POINTS['approval'],
                        f"Approved {form_type}", f"{form_type}_approval", row['id'],
                        created_at=row['approved_at']
                    )

        # Process scroll_completion with variable points and timestamps
        scroll_rows = await DBService.fetch(
            "SELECT id, submitted_by, approved_by, scroll_type, submitted_at, approved_at "
            "FROM scroll_completion WHERE status = 'approved'"
        )
        logger.info(f"Processing {len(scroll_rows)} approved scroll completions")
        for row in scroll_rows:
            scroll_type = (row['scroll_type'] or '').lower()
            points = SCROLL_POINTS.get(scroll_type, REP_POINTS['scroll_completion'])
            logger.debug(f"Scroll #{row['id']} type '{scroll_type}' -> {points} pts")
            await DBService.add_reputation(
                row['submitted_by'], points, f"Submitted scroll_completion",
                'scroll_completion', row['id'],
                created_at=row['submitted_at']
            )
            if row['approved_by'] and row['approved_at']:
                await DBService.add_reputation(
                    row['approved_by'], REP_POINTS['approval'],
                    f"Approved scroll_completion", 'scroll_completion_approval', row['id'],
                    created_at=row['approved_at']
                )

        # Process progress_help from helper mentions (use progress report's submitted_at)
        help_rows = await DBService.fetch(
            "SELECT id, helper_mentions, submitted_at FROM progress_report "
            "WHERE status = 'approved' AND helper_mentions IS NOT NULL"
        )
        logger.info(f"Processing {len(help_rows)} helper mentions ({REP_POINTS['progress_help']} pts each)")
        for row in help_rows:
            helper_id = extract_user_id_from_mention(row['helper_mentions'])
            if helper_id:
                await DBService.add_reputation(
                    helper_id, REP_POINTS['progress_help'],
                    f"Helped in progress report {row['id']}", 'progress_help', row['id'],
                    created_at=row['submitted_at']
                )

        logger.info("Reputation refresh completed (historical timestamps preserved).")

    # Internal role management
    @staticmethod
    async def add_user_role(user_id: int, role: str, granted_by: int):
        await DBService.execute(
            "INSERT INTO user_roles (user_id, role, granted_by) VALUES ($1, $2, $3) "
            "ON CONFLICT (user_id, role) DO NOTHING",
            user_id, role, granted_by
        )

    @staticmethod
    async def remove_user_role(user_id: int, role: str):
        await DBService.execute(
            "DELETE FROM user_roles WHERE user_id = $1 AND role = $2",
            user_id, role
        )

    @staticmethod
    async def get_user_roles(user_id: int) -> List[str]:
        rows = await DBService.fetch(
            "SELECT role FROM user_roles WHERE user_id = $1", user_id
        )
        return [row['role'] for row in rows]

    @staticmethod
    async def user_has_role(user_id: int, role: str) -> bool:
        row = await DBService.fetchrow(
            "SELECT 1 FROM user_roles WHERE user_id = $1 AND role = $2",
            user_id, role
        )
        return row is not None

    @staticmethod
    async def list_users_with_role(role: str) -> List[Dict]:
        rows = await DBService.fetch(
            "SELECT user_id, granted_by, granted_at FROM user_roles WHERE role = $1",
            role
        )
        return [dict(row) for row in rows]

    # Form editing support
    @staticmethod
    async def get_form_by_id(table: str, form_id: int) -> Optional[tuple]:
        row = await DBService.fetchrow(f"SELECT status, submitted_by FROM {table} WHERE id = $1", form_id)
        if row:
            return (row['status'], row['submitted_by'])
        return None

    @staticmethod
    async def update_form_field(table: str, form_id: int, field: str, value):
        query = f"UPDATE {table} SET {field} = $1 WHERE id = $2"
        await DBService.execute(query, value, form_id)