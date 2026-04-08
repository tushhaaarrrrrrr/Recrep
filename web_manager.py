import os
import time
import asyncio
import threading
import subprocess
import psutil

from flask import Flask, jsonify, request, render_template_string
from flask_socketio import SocketIO, emit

from services.db_service import DBService
from database.connection import init_db_pool

BOT_SCRIPT = "main.py"
PID_FILE = "bot.pid"
VENV_PYTHON = os.path.join("venv", "Scripts", "python.exe")
LOG_FILE = "bot.log"

app = Flask(__name__)
app.config["SECRET_KEY"] = "replace-this-in-production"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ----------------------------------------------------------------------
# Async DB loop
# ----------------------------------------------------------------------
_db_pool = None
_event_loop = None
_loop_thread = None


async def init_global_pool():
    global _db_pool
    _db_pool = await init_db_pool()
    DBService._db_pool = _db_pool
    return _db_pool


def start_async_loop():
    global _event_loop
    _event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_event_loop)
    _event_loop.run_until_complete(init_global_pool())
    _event_loop.run_forever()


def run_async(coro):
    if _event_loop is None:
        raise RuntimeError("Async loop not started")
    future = asyncio.run_coroutine_threadsafe(coro, _event_loop)
    return future.result()


_loop_thread = threading.Thread(target=start_async_loop, daemon=True)
_loop_thread.start()
time.sleep(0.5)

# ----------------------------------------------------------------------
# Bot process management
# ----------------------------------------------------------------------
def get_bot_process():
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE, "r") as f:
            pid = int(f.read().strip())
        proc = psutil.Process(pid)
        if BOT_SCRIPT in " ".join(proc.cmdline()):
            return proc
        else:
            os.remove(PID_FILE)
            return None
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError, FileNotFoundError):
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        return None


def get_bot_status():
    proc = get_bot_process()
    if proc is None:
        return {"running": False, "pid": None, "uptime": None}
    uptime = int(time.time() - proc.create_time())
    days = uptime // 86400
    hours = (uptime % 86400) // 3600
    minutes = (uptime % 3600) // 60
    seconds = uptime % 60
    uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
    return {"running": True, "pid": proc.pid, "uptime": uptime_str}


def start_bot():
    if get_bot_process() is not None:
        return False, "Bot is already running."
    proc = subprocess.Popen([VENV_PYTHON, BOT_SCRIPT])
    with open(PID_FILE, "w") as f:
        f.write(str(proc.pid))
    return True, f"Bot started with PID {proc.pid}."


def stop_bot():
    proc = get_bot_process()
    if proc is None:
        return False, "Bot is not running."
    proc.terminate()
    time.sleep(2)
    if proc.is_running():
        proc.kill()
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)
    return True, "Bot stopped."


def restart_bot():
    stop_bot()
    time.sleep(2)
    return start_bot()


def reset_bot():
    stop_bot()
    try:
        subprocess.run([VENV_PYTHON, "reset_db.py"], check=True)
        subprocess.run([VENV_PYTHON, "reset_s3.py"], check=True)
    except subprocess.CalledProcessError as e:
        return False, f"Reset failed: {e}"
    start_bot()
    return True, "Database and S3 reset, bot restarted."


# ----------------------------------------------------------------------
# Async helpers for data
# ----------------------------------------------------------------------
FORM_TABLES = [
    "recruitment",
    "progress_report",
    "purchase_invoice",
    "demolition_report",
    "eviction_report",
    "scroll_completion",
]

FORM_LABELS = {
    "recruitment": "Recruitments",
    "progress_report": "Progress Reports",
    "purchase_invoice": "Invoices",
    "demolition_report": "Demolitions",
    "eviction_report": "Evictions",
    "scroll_completion": "Scrolls",
}

LEADERBOARD_CATEGORIES = [
    "reputation",
    "recruitment",
    "progress_report",
    "progress_help",
    "purchase_invoice",
    "demolition_report",
    "eviction_report",
    "scroll_completion",
]


async def async_get_overview():
    approved_counts = {}
    pending_counts = {}
    total_approved = 0
    total_pending = 0

    for table in FORM_TABLES:
        row_approved = await DBService.fetchrow(
            f"SELECT COUNT(*) FROM {table} WHERE status = 'approved'"
        )
        row_pending = await DBService.fetchrow(
            f"SELECT COUNT(*) FROM {table} WHERE status = 'pending'"
        )
        approved = row_approved[0] if row_approved else 0
        pending = row_pending[0] if row_pending else 0
        approved_counts[table] = approved
        pending_counts[table] = pending
        total_approved += approved
        total_pending += pending

    row_rep = await DBService.fetchrow(
        "SELECT COALESCE(SUM(reputation), 0) FROM staff_member"
    )
    total_rep = row_rep[0] if row_rep else 0
    row_staff_count = await DBService.fetchrow(
        "SELECT COUNT(*) FROM staff_member"
    )
    staff_count = row_staff_count[0] if row_staff_count else 0

    return {
        "totals": {
            "approved_total": total_approved,
            "pending_total": total_pending,
            "reputation_total": total_rep,
            "staff_total": staff_count,
        },
        "approved_breakdown": approved_counts,
        "pending_breakdown": pending_counts,
    }


async def async_get_activity(limit=30):
    activities = []
    for table in FORM_TABLES:
        rows = await DBService.fetch(
            f"""
            SELECT id, submitted_by, submitted_at, status
            FROM {table}
            ORDER BY submitted_at DESC
            LIMIT $1
            """,
            limit,
        )
        for row in rows:
            activities.append(
                {
                    "table": table,
                    "id": row["id"],
                    "submitted_by": row["submitted_by"],
                    "submitted_at": row["submitted_at"].isoformat(),
                    "status": row["status"],
                }
            )

    activities.sort(key=lambda x: x["submitted_at"], reverse=True)
    return activities[:limit]


async def async_get_activity_timeseries(granularity: str):
    granularity = granularity.lower()
    if granularity not in {"daily", "weekly", "monthly"}:
        granularity = "weekly"

    if granularity == "daily":
        span = 7
        label_for = lambda i: "Today" if i == 0 else f"{i}d ago"
        def bounds(i):
            start = f"CURRENT_DATE - INTERVAL '{i} day'"
            end = f"CURRENT_DATE - INTERVAL '{i - 1} day'"
            return start, end

    elif granularity == "monthly":
        span = 6
        label_for = lambda i: "This month" if i == 0 else f"{i}mo ago"
        def bounds(i):
            start = f"date_trunc('month', CURRENT_DATE) - INTERVAL '{i} month'"
            end = f"date_trunc('month', CURRENT_DATE) - INTERVAL '{i - 1} month'"
            return start, end

    else:  # weekly
        span = 8
        label_for = lambda i: "This week" if i == 0 else f"{i}w ago"
        def bounds(i):
            start = f"date_trunc('week', CURRENT_DATE) - INTERVAL '{i} week'"
            end = f"date_trunc('week', CURRENT_DATE) - INTERVAL '{i - 1} week'"
            return start, end

    labels = []
    series = {
        "recruitment": [],
        "progress_report": [],
        "progress_help": [],
        "purchase_invoice": [],
        "demolition_report": [],
        "eviction_report": [],
        "scroll_completion": [],
        "reputation": [],
    }

    for i in range(span - 1, -1, -1):
        start_expr, end_expr = bounds(i)
        labels.append(label_for(i))

        for table_key in [
            "recruitment",
            "progress_report",
            "purchase_invoice",
            "demolition_report",
            "eviction_report",
            "scroll_completion",
        ]:
            sql = (
                f"SELECT COUNT(*) FROM {table_key} "
                f"WHERE status = 'approved' "
                f"AND submitted_at >= {start_expr} "
                f"AND submitted_at < {end_expr}"
            )
            row = await DBService.fetchrow(sql)
            value = row[0] if row else 0
            series[table_key].append(value)

        try:
            sql_help = (
                "SELECT COUNT(*) FROM progress_help "
                f"WHERE created_at >= {start_expr} "
                f"AND created_at < {end_expr}"
            )
            row_help = await DBService.fetchrow(sql_help)
            help_value = row_help[0] if row_help else 0
        except Exception:
            help_value = 0
        series["progress_help"].append(help_value)

        row_rep = await DBService.fetchrow(
            f"""
            SELECT COALESCE(SUM(points), 0)
            FROM reputation_log
            WHERE created_at >= {start_expr}
              AND created_at < {end_expr}
            """
        )
        rep_value = row_rep[0] if row_rep else 0
        series["reputation"].append(rep_value)

    return {"labels": labels, "series": series}


async def async_get_leaderboard(category, period):
    category = category.lower()
    period = period.lower()
    if category == "reputation":
        rows = await DBService.get_leaderboard(period)
    else:
        rows = await DBService.get_category_leaderboard(category, period)
    return rows


async def async_get_staff_directory():
    rows = await DBService.fetch(
        "SELECT discord_id, display_name, reputation "
        "FROM staff_member ORDER BY reputation DESC"
    )

    staff_map = {}
    for row in rows:
        discord_id = row["discord_id"]
        raw_name = row.get("display_name") or ""
        if raw_name:
            label = raw_name
        else:
            s = str(discord_id)
            label = f"User {s[:4]}…{s[-4:]}" if len(s) > 8 else f"User {s}"
        staff_map[discord_id] = {
            "discord_id": discord_id,
            "label": label,
            "reputation": row["reputation"],
            "recruitment": 0,
            "progress_report": 0,
            "progress_help": 0,
            "purchase_invoice": 0,
            "demolition_report": 0,
            "eviction_report": 0,
            "scroll_completion": 0,
            "roles": [],
        }

    for category in LEADERBOARD_CATEGORIES:
        if category == "reputation":
            continue
        try:
            lb_rows = await DBService.get_category_leaderboard(category, "all")
        except Exception:
            continue
        for r in lb_rows:
            did = r["discord_id"]
            if did not in staff_map:
                s = str(did)
                label = f"User {s[:4]}…{s[-4:]}" if len(s) > 8 else f"User {s}"
                staff_map[did] = {
                    "discord_id": did,
                    "label": label,
                    "reputation": 0,
                    "recruitment": 0,
                    "progress_report": 0,
                    "progress_help": 0,
                    "purchase_invoice": 0,
                    "demolition_report": 0,
                    "eviction_report": 0,
                    "scroll_completion": 0,
                    "roles": [],
                }
            val = r.get("count") or r.get("points") or 0
            staff_map[did][category] = val

    for did, data in staff_map.items():
        try:
            roles = await DBService.get_user_roles(did)
        except Exception:
            roles = []
        data["roles"] = roles

    staff_list = sorted(
        staff_map.values(), key=lambda x: x["reputation"], reverse=True
    )
    return staff_list


# ----------------------------------------------------------------------
# Flask routes
# ----------------------------------------------------------------------
@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/status")
def api_status():
    return jsonify(get_bot_status())


@app.route("/api/overview")
def api_overview():
    try:
        data = run_async(async_get_overview())
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/activity")
def api_activity():
    try:
        data = run_async(async_get_activity(30))
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/activity_timeseries")
def api_activity_timeseries():
    granularity = request.args.get("granularity", "weekly")
    try:
        data = run_async(async_get_activity_timeseries(granularity))
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leaderboard/<category>/<period>")
def api_leaderboard(category, period):
    try:
        rows = run_async(async_get_leaderboard(category, period))
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/staff")
def api_staff():
    try:
        staff = run_async(async_get_staff_directory())
        return jsonify({"staff": staff})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/form/<string:table>/<int:form_id>")
def api_form_detail(table, form_id):
    try:
        row = run_async(DBService.fetchrow(f"SELECT * FROM {table} WHERE id = $1", form_id))
        return jsonify(dict(row) if row else None)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/start", methods=["POST"])
def start():
    ok, msg = start_bot()
    return jsonify({"success": ok, "message": msg})


@app.route("/stop", methods=["POST"])
def stop():
    ok, msg = stop_bot()
    return jsonify({"success": ok, "message": msg})


@app.route("/restart", methods=["POST"])
def restart():
    ok, msg = restart_bot()
    return jsonify({"success": ok, "message": msg})


@app.route("/reset", methods=["POST"])
def reset():
    ok, msg = reset_bot()
    return jsonify({"success": ok, "message": msg})


# ----------------------------------------------------------------------
# WebSocket live logs
# ----------------------------------------------------------------------
def log_watcher():
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if line:
                socketio.emit("log", {"line": line.strip()})
            else:
                time.sleep(0.5)


@socketio.on("connect")
def handle_connect():
    emit("connected", {"data": "Connected"})
    if not hasattr(app, "_log_thread"):
        app._log_thread = threading.Thread(target=log_watcher, daemon=True)
        app._log_thread.start()


# ----------------------------------------------------------------------
# HTML Template – Moonlit Ocean UI with Chart.js sizing fixes
# ----------------------------------------------------------------------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Recrep | Control Panel</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <style>
        :root {
            --space-indigo: #2b2d42;
            --lavender-grey: #8d99ae;
            --platinum: #edf2f4;
            --surface-dark: #1e2230;
            --surface-darker: #181b25;
            --accent-soft: #4f8cc9;
            --accent-strong: #f3c969;
            --danger: #f07167;
            --success: #70e000;
            --warning: #ffb703;
            --muted: #6c7281;
            --radius-lg: 14px;
            --radius-md: 10px;
            --radius-pill: 999px;
            --shadow-soft: 0 18px 45px rgba(0, 0, 0, 0.45);
        }

        * {
            box-sizing: border-box;
        }

        html, body {
            margin: 0;
            padding: 0;
            background: radial-gradient(circle at top, #3b3e57 0%, var(--space-indigo) 40%, #151728 100%);
            color: var(--platinum);
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            -webkit-font-smoothing: antialiased;
        }

        body {
            min-height: 100vh;
        }

        .page-frame {
            max-width: 1200px;
            margin: 0 auto;
            padding: 24px 18px 32px;
        }

        .page-header {
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 16px;
            margin-bottom: 20px;
        }

        .page-title-block h1 {
            margin: 0;
            font-size: 24px;
            letter-spacing: 0.03em;
        }

        .page-title-block span {
            display: inline-block;
            margin-top: 4px;
            font-size: 12px;
            color: var(--lavender-grey);
        }

        .status-block {
            display: flex;
            align-items: center;
            gap: 20px;
            font-size: 12px;
        }

        .status-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 5px 10px;
            border-radius: var(--radius-pill);
            background: rgba(0, 0, 0, 0.25);
            border: 1px solid rgba(237, 242, 244, 0.08);
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 999px;
            background: var(--danger);
            box-shadow: 0 0 10px rgba(240, 113, 103, 0.55);
        }

        .status-dot.online {
            background: var(--success);
            box-shadow: 0 0 10px rgba(112, 224, 0, 0.7);
        }

        .status-label {
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 10px;
            color: var(--lavender-grey);
        }

        .status-value {
            font-weight: 600;
            font-size: 12px;
        }

        .status-metadata {
            color: var(--muted);
        }

        .control-strip {
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            background: linear-gradient(135deg, rgba(24, 27, 37, 0.95), rgba(31, 34, 48, 0.95));
            border-radius: var(--radius-lg);
            padding: 10px 14px;
            border: 1px solid rgba(141, 153, 174, 0.25);
            box-shadow: var(--shadow-soft);
            margin-bottom: 20px;
        }

        .control-buttons {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }

        .btn {
            border: none;
            outline: none;
            border-radius: var(--radius-pill);
            padding: 7px 14px;
            font-size: 12px;
            font-weight: 500;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: background 0.15s ease, transform 0.08s ease, box-shadow 0.15s ease;
            background: rgba(141, 153, 174, 0.15);
            color: var(--platinum);
        }

        .btn:hover {
            transform: translateY(-1px);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.4);
        }

        .btn-primary {
            background: linear-gradient(135deg, #4f8cc9, #8bbdf2);
            color: #0f172a;
        }

        .btn-danger {
            background: linear-gradient(135deg, #f07167, #fbb1a1);
            color: #1b0b0b;
        }

        .btn-ghost {
            background: transparent;
            border: 1px solid rgba(141, 153, 174, 0.4);
            color: var(--lavender-grey);
        }

        .control-message {
            font-size: 11px;
            color: var(--lavender-grey);
            min-height: 18px;
        }

        .badge-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            background: rgba(15, 23, 42, 0.7);
            border-radius: var(--radius-pill);
            border: 1px solid rgba(141, 153, 174, 0.25);
            font-size: 11px;
            color: var(--lavender-grey);
        }

        .badge-pill span {
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
            color: var(--platinum);
        }

        /* Tabs */
        .tab-row {
            display: flex;
            gap: 8px;
            border-bottom: 1px solid rgba(141, 153, 174, 0.2);
            margin-bottom: 16px;
        }

        .tab {
            position: relative;
            padding: 7px 14px;
            font-size: 12px;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--lavender-grey);
            cursor: pointer;
        }

        .tab::after {
            content: "";
            position: absolute;
            left: 12px;
            right: 12px;
            bottom: -1px;
            height: 2px;
            border-radius: 999px;
            background: transparent;
            transition: background 0.18s ease, transform 0.18s ease;
            transform-origin: center;
        }

        .tab.active {
            color: var(--platinum);
        }

        .tab.active::after {
            background: linear-gradient(90deg, var(--accent-soft), var(--accent-strong));
            transform: scaleX(1.05);
        }

        .tab-content {
            display: none;
        }

        .tab-content.active {
            display: block;
        }

        /* Layout grid */
        .grid {
            display: grid;
            gap: 14px;
        }

        .grid-4 {
            grid-template-columns: repeat(4, minmax(0, 1fr));
        }

        .grid-2 {
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }

        @media (max-width: 900px) {
            .page-header {
                flex-direction: column;
                align-items: stretch;
            }
            .grid-4 {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
            .grid-2 {
                grid-template-columns: 1fr;
            }
        }

        @media (max-width: 640px) {
            .grid-4 {
                grid-template-columns: 1fr;
            }
        }

        /* Cards */
        .card {
            background: radial-gradient(circle at top left, rgba(141, 153, 174, 0.08), rgba(24, 27, 37, 0.98));
            border-radius: var(--radius-lg);
            border: 1px solid rgba(141, 153, 174, 0.18);
            padding: 14px 16px;
            box-shadow: var(--shadow-soft);
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }

        .card-title {
            font-size: 13px;
            font-weight: 600;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--lavender-grey);
        }

        .card-meta {
            font-size: 11px;
            color: var(--muted);
        }

        .stat-value {
            font-size: 26px;
            font-weight: 700;
            letter-spacing: 0.04em;
            margin: 4px 0 0;
            background: linear-gradient(135deg, var(--platinum), #e0fbfc);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
        }

        .stat-subline {
            margin-top: 4px;
            font-size: 11px;
            color: var(--lavender-grey);
        }

        .stat-subline span {
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
            color: var(--platinum);
        }

        /* Activity list */
        .activity-list {
            margin: 0;
            padding: 0;
            list-style: none;
            max-height: 260px;
            overflow-y: auto;
        }

        .activity-row {
            display: grid;
            grid-template-columns: minmax(0, 1.7fr) minmax(0, 1fr) minmax(0, 0.7fr);
            align-items: center;
            gap: 8px;
            padding: 7px 0;
            border-bottom: 1px solid rgba(141, 153, 174, 0.15);
            font-size: 11px;
        }

        .activity-row:last-child {
            border-bottom: none;
        }

        .activity-main {
            display: flex;
            flex-direction: column;
        }

        .activity-meta {
            color: var(--muted);
        }

        .activity-label {
            font-weight: 500;
        }

        .badge-status {
            display: inline-flex;
            padding: 2px 8px;
            border-radius: var(--radius-pill);
            font-size: 10px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }

        .badge-approved {
            color: #b9fbc0;
            border: 1px solid rgba(112, 224, 0, 0.35);
            background: rgba(112, 224, 0, 0.12);
        }

        .badge-pending {
            color: #fcd5a5;
            border: 1px solid rgba(255, 183, 3, 0.35);
            background: rgba(255, 183, 3, 0.12);
        }

        .badge-denied {
            color: #ffadad;
            border: 1px solid rgba(240, 113, 103, 0.35);
            background: rgba(240, 113, 103, 0.12);
        }

        /* Tables */
        .table-wrapper {
            width: 100%;
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 11px;
        }

        thead {
            background: rgba(24, 27, 37, 0.9);
        }

        th, td {
            padding: 7px 8px;
            text-align: left;
            white-space: nowrap;
        }

        th {
            font-weight: 600;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            font-size: 11px;
            color: var(--lavender-grey);
            border-bottom: 1px solid rgba(141, 153, 174, 0.3);
        }

        tbody tr:nth-child(even) {
            background: rgba(24, 27, 37, 0.9);
        }

        tbody tr:nth-child(odd) {
            background: rgba(20, 22, 33, 0.9);
        }

        tbody tr:hover {
            background: rgba(79, 140, 201, 0.18);
        }

        .text-right {
            text-align: right;
        }

        .name-cell {
            display: flex;
            flex-direction: column;
        }

        .name-primary {
            font-weight: 500;
            font-size: 11px;
        }

        .name-secondary {
            font-size: 10px;
            color: var(--muted);
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        }

        .role-badge {
            display: inline-flex;
            padding: 3px 8px;
            border-radius: var(--radius-pill);
            border: 1px solid rgba(141, 153, 174, 0.4);
            margin: 2px 3px 2px 0;
            font-size: 10px;
            color: var(--lavender-grey);
        }

        /* Inputs */
        .input-row {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            align-items: center;
        }

        .select,
        .input {
            border-radius: var(--radius-pill);
            border: 1px solid rgba(141, 153, 174, 0.45);
            background: rgba(10, 12, 20, 0.9);
            color: var(--platinum);
            padding: 6px 10px;
            font-size: 11px;
            outline: none;
        }

        .select:focus,
        .input:focus {
            border-color: var(--accent-soft);
            box-shadow: 0 0 0 1px rgba(79, 140, 201, 0.6);
        }

        .input::placeholder {
            color: rgba(141, 153, 174, 0.75);
        }

        /* Logs */
        .log-box {
            background: rgba(0, 0, 0, 0.75);
            border-radius: var(--radius-md);
            padding: 10px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
            font-size: 11px;
            height: 350px;
            overflow-y: auto;
            border: 1px solid rgba(141, 153, 174, 0.4);
        }

        .log-line {
            padding: 2px 0;
            border-left: 2px solid transparent;
            padding-left: 6px;
            color: var(--platinum);
            word-break: break-all;
        }

        .log-line:nth-child(odd) {
            color: var(--lavender-grey);
        }

        .log-line:hover {
            border-left-color: var(--accent-strong);
            background: rgba(79, 140, 201, 0.12);
        }

        /* Scrollbar */
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }

        ::-webkit-scrollbar-track {
            background: rgba(15, 16, 24, 0.9);
        }

        ::-webkit-scrollbar-thumb {
            background: rgba(141, 153, 174, 0.65);
            border-radius: 999px;
        }

        /* Utility */
        .mono {
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        }

        .text-muted {
            color: var(--muted);
        }

        .text-small {
            font-size: 10px;
        }

        .mt-8 {
            margin-top: 8px;
        }

        .mt-12 {
            margin-top: 12px;
        }

        .inline-spinner {
            width: 12px;
            height: 12px;
            border-radius: 999px;
            border: 2px solid var(--accent-strong);
            border-top-color: transparent;
            animation: spin 0.6s linear infinite;
        }

        @keyframes spin {
            to { transform: rotate(360deg); }
        }

        /* Chart container fixed heights to prevent reflow loops */
        .chart-container {
            position: relative;
            height: 200px;
            width: 100%;
        }
        .chart-container canvas {
            display: block;
            width: 100% !important;
            height: 100% !important;
        }
    </style>
</head>
<body>
    <div class="page-frame">
        <header class="page-header">
            <div class="page-title-block">
                <h1>Recrep Control</h1>
                <span>Bot orchestration, staff performance, and form analytics.</span>
            </div>
            <div class="status-block">
                <div class="status-pill">
                    <div id="statusDot" class="status-dot"></div>
                    <div>
                        <div class="status-label">Status</div>
                        <div id="statusText" class="status-value">Offline</div>
                    </div>
                </div>
                <div class="badge-pill">
                    Uptime
                    <span id="uptimeText">–</span>
                </div>
                <button id="refreshAllBtn" class="btn btn-ghost">Refresh Data</button>
            </div>
        </header>

        <section class="control-strip">
            <div class="control-buttons">
                <button id="startBtn" class="btn btn-primary">Start</button>
                <button id="restartBtn" class="btn">Restart</button>
                <button id="stopBtn" class="btn btn-danger">Stop</button>
                <button id="resetBtn" class="btn btn-ghost">Reset DB + S3</button>
            </div>
            <div id="controlMsg" class="control-message"></div>
        </section>

        <nav class="tab-row">
            <div class="tab active" data-tab="overview">Overview</div>
            <div class="tab" data-tab="leaderboard">Leaderboard</div>
            <div class="tab" data-tab="staff">Staff Directory</div>
            <div class="tab" data-tab="logs">Live Logs</div>
        </nav>

        <section id="overview" class="tab-content active">
            <div class="grid grid-4">
                <div class="card">
                    <div class="card-header">
                        <div class="card-title">Approved Forms</div>
                    </div>
                    <div id="approvedTotal" class="stat-value">0</div>
                    <div id="approvedSubline" class="stat-subline text-small text-muted">–</div>
                </div>
                <div class="card">
                    <div class="card-header">
                        <div class="card-title">Pending Forms</div>
                    </div>
                    <div id="pendingTotal" class="stat-value">0</div>
                    <div id="pendingSubline" class="stat-subline text-small text-muted">–</div>
                </div>
                <div class="card">
                    <div class="card-header">
                        <div class="card-title">Total Reputation</div>
                    </div>
                    <div id="totalRep" class="stat-value">0</div>
                    <div class="stat-subline text-small text-muted">Sum of all staff reputation.</div>
                </div>
                <div class="card">
                    <div class="card-header">
                        <div class="card-title">Staff Count</div>
                    </div>
                    <div id="staffCount" class="stat-value">0</div>
                    <div class="stat-subline text-small text-muted">Active staff members in the system.</div>
                </div>
            </div>

            <div class="grid grid-2 mt-12">
                <div class="card">
                    <div class="card-header">
                        <div>
                            <div class="card-title">Activity Over Time</div>
                            <div class="card-meta">Recruitments, forms, and reputation across time buckets.</div>
                        </div>
                        <div class="input-row">
                            <button class="btn btn-ghost text-small" data-granularity="daily">Daily</button>
                            <button class="btn btn-ghost text-small" data-granularity="weekly">Weekly</button>
                            <button class="btn btn-ghost text-small" data-granularity="monthly">Monthly</button>
                        </div>
                    </div>
                    <div class="chart-container">
                        <canvas id="activityChart"></canvas>
                    </div>
                </div>

                <div class="card">
                    <div class="card-header">
                        <div>
                            <div class="card-title">Form Distribution</div>
                            <div class="card-meta">Who is contributing which forms.</div>
                        </div>
                        <select id="distCategory" class="select text-small">
                            <option value="recruitment">Recruitments</option>
                            <option value="progress_report">Progress Reports</option>
                            <option value="progress_help">Progress Help</option>
                            <option value="purchase_invoice">Invoices</option>
                            <option value="demolition_report">Demolitions</option>
                            <option value="eviction_report">Evictions</option>
                            <option value="scroll_completion">Scrolls</option>
                        </select>
                    </div>
                    <div class="chart-container">
                        <canvas id="distributionChart"></canvas>
                    </div>
                </div>
            </div>

            <div class="card mt-12">
                <div class="card-header">
                    <div class="card-title">Recent Activity</div>
                    <div class="card-meta">Latest approved and pending forms across all categories.</div>
                </div>
                <ul id="activityList" class="activity-list"></ul>
            </div>
        </section>

        <section id="leaderboard" class="tab-content">
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Leaderboard</div>
                    <div class="card-meta">Rank staff by category and period.</div>
                </div>
                <div class="input-row mt-8">
                    <select id="lbCat" class="select">
                        <option value="reputation">Reputation</option>
                        <option value="recruitment">Recruitments</option>
                        <option value="progress_report">Progress Reports</option>
                        <option value="progress_help">Progress Help</option>
                        <option value="purchase_invoice">Invoices</option>
                        <option value="demolition_report">Demolitions</option>
                        <option value="eviction_report">Evictions</option>
                        <option value="scroll_completion">Scrolls</option>
                    </select>
                    <select id="lbPeriod" class="select">
                        <option value="weekly">Weekly</option>
                        <option value="biweekly">Bi-weekly</option>
                        <option value="monthly">Monthly</option>
                        <option value="all">All Time</option>
                    </select>
                    <input id="lbSearch" class="input" placeholder="Search user label or id..." />
                    <button id="lbFilter" class="btn btn-ghost text-small">Filter</button>
                </div>
                <div class="table-wrapper mt-12">
                    <table>
                        <thead>
                            <tr>
                                <th style="width:48px;">Rank</th>
                                <th>Name</th>
                                <th class="text-right">Score</th>
                            </tr>
                        </thead>
                        <tbody id="leaderboardBody"></tbody>
                    </table>
                </div>
            </div>
        </section>

        <section id="staff" class="tab-content">
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Staff Directory</div>
                    <div class="card-meta">Per-staff breakdown across all categories.</div>
                </div>
                <div class="table-wrapper mt-8">
                    <table>
                        <thead>
                            <tr>
                                <th>Name</th>
                                <th class="text-right">Rep</th>
                                <th class="text-right">Rec</th>
                                <th class="text-right">Prog</th>
                                <th class="text-right">Help</th>
                                <th class="text-right">Inv</th>
                                <th class="text-right">Demo</th>
                                <th class="text-right">Evict</th>
                                <th class="text-right">Scroll</th>
                                <th>Roles</th>
                            </tr>
                        </thead>
                        <tbody id="staffBody"></tbody>
                    </table>
                </div>
            </div>
        </section>

        <section id="logs" class="tab-content">
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Live Console</div>
                    <div class="card-meta">Streaming logs from the running bot process.</div>
                </div>
                <div id="logContainer" class="log-box"></div>
            </div>
        </section>
    </div>

    <script>
        const statusDot = document.getElementById("statusDot");
        const statusText = document.getElementById("statusText");
        const uptimeText = document.getElementById("uptimeText");
        const controlMsg = document.getElementById("controlMsg");

        const approvedTotal = document.getElementById("approvedTotal");
        const approvedSubline = document.getElementById("approvedSubline");
        const pendingTotal = document.getElementById("pendingTotal");
        const pendingSubline = document.getElementById("pendingSubline");
        const totalRep = document.getElementById("totalRep");
        const staffCount = document.getElementById("staffCount");

        const activityList = document.getElementById("activityList");
        const leaderboardBody = document.getElementById("leaderboardBody");
        const staffBody = document.getElementById("staffBody");
        const logContainer = document.getElementById("logContainer");

        const distCategorySelect = document.getElementById("distCategory");
        const lbCat = document.getElementById("lbCat");
        const lbPeriod = document.getElementById("lbPeriod");
        const lbSearch = document.getElementById("lbSearch");
        const lbFilter = document.getElementById("lbFilter");
        const refreshAllBtn = document.getElementById("refreshAllBtn");

        let staffData = [];
        let leaderboardRows = [];
        let nameMap = {};
        let currentGranularity = "weekly";

        const activityCtx = document.getElementById("activityChart").getContext("2d");
        const distCtx = document.getElementById("distributionChart").getContext("2d");
        let activityChart = null;
        let distributionChart = null;
        let resizeScheduled = false;

        const activityColors = {
            recruitment: "#8bbdf2",
            progress_report: "#f3c969",
            progress_help: "#efb0ff",
            purchase_invoice: "#9de7d7",
            demolition_report: "#f28f8f",
            eviction_report: "#ffafcc",
            scroll_completion: "#a0c4ff",
            reputation: "#ffffff"
        };

        function formatNumber(n) {
            if (n == null) return "0";
            return n.toLocaleString("en-US");
        }

        async function fetchStatus() {
            try {
                const res = await fetch("/api/status");
                const data = await res.json();
                if (data.running) {
                    statusDot.classList.add("online");
                    statusText.textContent = `Online (PID ${data.pid})`;
                    uptimeText.textContent = data.uptime || "–";
                } else {
                    statusDot.classList.remove("online");
                    statusText.textContent = "Offline";
                    uptimeText.textContent = "–";
                }
            } catch (e) {
                console.error(e);
            }
        }

        async function fetchOverview() {
            try {
                const res = await fetch("/api/overview");
                const data = await res.json();
                if (data.error) throw new Error(data.error);

                const totals = data.totals || {};
                const approved = data.approved_breakdown || {};
                const pending = data.pending_breakdown || {};

                approvedTotal.textContent = formatNumber(totals.approved_total || 0);
                pendingTotal.textContent = formatNumber(totals.pending_total || 0);
                totalRep.textContent = formatNumber(totals.reputation_total || 0);
                staffCount.textContent = formatNumber(totals.staff_total || 0);

                const partsApproved = [];
                const mapLabels = {
                    recruitment: "Rec",
                    progress_report: "Prog",
                    purchase_invoice: "Inv",
                    demolition_report: "Demo",
                    eviction_report: "Evict",
                    scroll_completion: "Scroll"
                };
                for (const key of Object.keys(mapLabels)) {
                    const v = approved[key] || 0;
                    partsApproved.push(`${mapLabels[key]} <span>${formatNumber(v)}</span>`);
                }
                approvedSubline.innerHTML = partsApproved.join(" · ");

                const partsPending = [];
                for (const key of Object.keys(mapLabels)) {
                    const v = pending[key] || 0;
                    partsPending.push(`${mapLabels[key]} <span>${formatNumber(v)}</span>`);
                }
                pendingSubline.innerHTML = partsPending.join(" · ");
            } catch (e) {
                console.error(e);
            }
        }

        async function fetchActivityList() {
            try {
                const res = await fetch("/api/activity");
                const data = await res.json();
                if (data.error) throw new Error(data.error);
                activityList.innerHTML = "";

                if (!data.length) {
                    activityList.innerHTML = "<li class='text-small text-muted'>No recent activity.</li>";
                    return;
                }

                for (const act of data) {
                    const li = document.createElement("li");
                    li.className = "activity-row";
                    const date = new Date(act.submitted_at);
                    const status = act.status || "";
                    let statusClass = "badge-denied";
                    if (status === "approved") statusClass = "badge-approved";
                    if (status === "pending") statusClass = "badge-pending";

                    li.innerHTML = `
                        <div class="activity-main">
                            <div class="activity-label">${(act.table || "").replace(/_/g, " ").toUpperCase()} #${act.id}</div>
                            <div class="activity-meta">${date.toLocaleString()}</div>
                        </div>
                        <div class="text-muted mono">By ${act.submitted_by}</div>
                        <div class="text-right">
                            <span class="badge-status ${statusClass}">${status || "unknown"}</span>
                        </div>
                    `;
                    activityList.appendChild(li);
                }
            } catch (e) {
                console.error(e);
            }
        }

        async function fetchStaff() {
            try {
                const res = await fetch("/api/staff");
                const data = await res.json();
                if (data.error) throw new Error(data.error);
                staffData = data.staff || [];
                renderStaffTable();
                nameMap = {};
                for (const s of staffData) {
                    nameMap[String(s.discord_id)] = s.label || `User ${s.discord_id}`;
                }
                updateDistributionChart();
            } catch (e) {
                console.error(e);
            }
        }

        function renderStaffTable() {
            staffBody.innerHTML = "";
            if (!staffData.length) {
                staffBody.innerHTML = "<tr><td colspan='10' class='text-small text-muted'>No staff records found.</td></tr>";
                return;
            }
            for (const s of staffData) {
                const tr = document.createElement("tr");
                tr.innerHTML = `
                    <td>
                        <div class="name-cell">
                            <span class="name-primary">${s.label || `User ${s.discord_id}`}</span>
                            <span class="name-secondary">${s.discord_id}</span>
                        </div>
                    </td>
                    <td class="text-right mono">${formatNumber(s.reputation || 0)}</td>
                    <td class="text-right mono">${formatNumber(s.recruitment || 0)}</td>
                    <td class="text-right mono">${formatNumber(s.progress_report || 0)}</td>
                    <td class="text-right mono">${formatNumber(s.progress_help || 0)}</td>
                    <td class="text-right mono">${formatNumber(s.purchase_invoice || 0)}</td>
                    <td class="text-right mono">${formatNumber(s.demolition_report || 0)}</td>
                    <td class="text-right mono">${formatNumber(s.eviction_report || 0)}</td>
                    <td class="text-right mono">${formatNumber(s.scroll_completion || 0)}</td>
                    <td>
                        ${(s.roles || []).map(r => `<span class="role-badge">${r}</span>`).join("")}
                    </td>
                `;
                staffBody.appendChild(tr);
            }
        }

        async function fetchActivitySeries(granularity) {
            try {
                const res = await fetch(`/api/activity_timeseries?granularity=${encodeURIComponent(granularity)}`);
                const data = await res.json();
                if (data.error) throw new Error(data.error);
                updateActivityChart(data);
            } catch (e) {
                console.error(e);
            }
        }

        function scheduleChartResize() {
            if (resizeScheduled) return;
            resizeScheduled = true;
            requestAnimationFrame(() => {
                resizeScheduled = false;
                if (activityChart) activityChart.resize();
                if (distributionChart) distributionChart.resize();
            });
        }

        function updateActivityChart(payload) {
            const labels = payload.labels || [];
            const series = payload.series || {};

            const datasets = [
                { key: "recruitment", label: "Recruitments" },
                { key: "progress_report", label: "Progress Reports" },
                { key: "progress_help", label: "Progress Help" },
                { key: "purchase_invoice", label: "Invoices" },
                { key: "demolition_report", label: "Demolitions" },
                { key: "eviction_report", label: "Evictions" },
                { key: "scroll_completion", label: "Scrolls" },
                { key: "reputation", label: "Reputation" }
            ].map(cfg => {
                return {
                    label: cfg.label,
                    data: (series[cfg.key] || []).map(x => x || 0),
                    borderColor: activityColors[cfg.key],
                    backgroundColor: activityColors[cfg.key] + "40",
                    tension: 0.35,
                    fill: cfg.key !== "reputation",
                    borderWidth: cfg.key === "reputation" ? 2 : 1.5,
                    pointRadius: 2,
                    pointHoverRadius: 3
                };
            });

            if (!activityChart) {
                activityChart = new Chart(activityCtx, {
                    type: "line",
                    data: { labels, datasets },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                labels: {
                                    color: "#edf2f4",
                                    font: { size: 10 }
                                }
                            }
                        },
                        scales: {
                            x: {
                                grid: { color: "rgba(141,153,174,0.25)" },
                                ticks: { color: "#edf2f4", font: { size: 10 } }
                            },
                            y: {
                                beginAtZero: true,
                                grid: { color: "rgba(141,153,174,0.25)" },
                                ticks: { color: "#edf2f4", font: { size: 10 } }
                            }
                        }
                    }
                });
            } else {
                activityChart.data.labels = labels;
                activityChart.data.datasets = datasets;
                activityChart.update();
            }
        }

        function updateDistributionChart() {
            const category = distCategorySelect.value;
            if (!staffData.length) return;
            const sorted = [...staffData].sort((a, b) => (b[category] || 0) - (a[category] || 0)).slice(0, 12);
            const labels = sorted.map(s => s.label || `User ${s.discord_id}`);
            const values = sorted.map(s => s[category] || 0);

            if (!distributionChart) {
                distributionChart = new Chart(distCtx, {
                    type: "bar",
                    data: {
                        labels,
                        datasets: [{
                            label: "Forms",
                            data: values,
                            backgroundColor: "rgba(79,140,201,0.7)"
                        }]
                    },
                    options: {
                        indexAxis: "y",
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false }
                        },
                        scales: {
                            x: {
                                grid: { color: "rgba(141,153,174,0.25)" },
                                ticks: { color: "#edf2f4", font: { size: 10 } }
                            },
                            y: {
                                grid: { display: false },
                                ticks: { color: "#edf2f4", font: { size: 10 } }
                            }
                        }
                    }
                });
            } else {
                distributionChart.data.labels = labels;
                distributionChart.data.datasets[0].data = values;
                distributionChart.update();
            }
        }

        async function fetchLeaderboard() {
            try {
                const category = lbCat.value;
                const period = lbPeriod.value;
                const res = await fetch(`/api/leaderboard/${category}/${period}`);
                const data = await res.json();
                if (data.error) throw new Error(data.error);
                leaderboardRows = data || [];
                renderLeaderboard();
            } catch (e) {
                console.error(e);
            }
        }

        function renderLeaderboard() {
            leaderboardBody.innerHTML = "";
            if (!leaderboardRows.length) {
                leaderboardBody.innerHTML = "<tr><td colspan='3' class='text-small text-muted'>No leaderboard data.</td></tr>";
                return;
            }
            const search = (lbSearch.value || "").toLowerCase();
            let rank = 1;
            for (const row of leaderboardRows) {
                const id = String(row.discord_id || "");
                const label = nameMap[id] || `User ${id}`;
                const value = row.points || row.count || 0;

                if (search) {
                    const text = (label + " " + id).toLowerCase();
                    if (!text.includes(search)) continue;
                }

                const tr = document.createElement("tr");
                tr.innerHTML = `
                    <td class="text-right mono">${rank++}</td>
                    <td>
                        <div class="name-cell">
                            <span class="name-primary">${label}</span>
                            <span class="name-secondary">${id}</span>
                        </div>
                    </td>
                    <td class="text-right mono">${formatNumber(value)}</td>
                `;
                leaderboardBody.appendChild(tr);
            }
            if (!leaderboardBody.children.length) {
                leaderboardBody.innerHTML = "<tr><td colspan='3' class='text-small text-muted'>No results match the current filter.</td></tr>";
            }
        }

        async function sendAction(action) {
            controlMsg.innerHTML = '<span class="inline-spinner"></span> &nbsp;Processing…';
            try {
                const res = await fetch(`/${action}`, { method: "POST" });
                const data = await res.json();
                controlMsg.textContent = data.message || "Done.";
                setTimeout(() => { controlMsg.textContent = ""; }, 3500);

                if (["start", "stop", "restart"].includes(action)) {
                    setTimeout(() => {
                        fetchStatus();
                        fetchOverview();
                        fetchActivityList();
                        fetchStaff();
                    }, 1500);
                } else if (action === "reset") {
                    setTimeout(() => { location.reload(); }, 3000);
                }
            } catch (e) {
                console.error(e);
                controlMsg.textContent = "Error while sending action.";
            }
        }

        document.querySelectorAll(".tab").forEach(tab => {
            tab.addEventListener("click", () => {
                const target = tab.dataset.tab;
                document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
                document.querySelectorAll(".tab-content").forEach(c => c.classList.remove("active"));
                tab.classList.add("active");
                document.getElementById(target).classList.add("active");
                scheduleChartResize();
            });
        });

        document.getElementById("startBtn").onclick = () => sendAction("start");
        document.getElementById("stopBtn").onclick = () => sendAction("stop");
        document.getElementById("restartBtn").onclick = () => sendAction("restart");
        document.getElementById("resetBtn").onclick = () => sendAction("reset");
        refreshAllBtn.onclick = () => {
            fetchStatus();
            fetchOverview();
            fetchActivityList();
            fetchStaff();
            fetchLeaderboard();
            fetchActivitySeries(currentGranularity);
        };

        document.querySelectorAll("[data-granularity]").forEach(btn => {
            btn.addEventListener("click", () => {
                currentGranularity = btn.dataset.granularity;
                fetchActivitySeries(currentGranularity);
            });
        });

        distCategorySelect.addEventListener("change", updateDistributionChart);
        lbCat.addEventListener("change", fetchLeaderboard);
        lbPeriod.addEventListener("change", fetchLeaderboard);
        lbFilter.addEventListener("click", renderLeaderboard);
        lbSearch.addEventListener("input", renderLeaderboard);

        const socket = io();
        socket.on("log", (data) => {
            const line = document.createElement("div");
            line.className = "log-line";
            line.textContent = data.line;
            logContainer.appendChild(line);
            logContainer.scrollTop = logContainer.scrollHeight;
            if (logContainer.children.length > 800) {
                logContainer.removeChild(logContainer.firstChild);
            }
        });

        window.addEventListener("resize", scheduleChartResize);

        (async function init() {
            await fetchStatus();
            await fetchOverview();
            await fetchActivityList();
            await fetchStaff();
            await fetchLeaderboard();
            await fetchActivitySeries(currentGranularity);

            setInterval(() => {
                fetchStatus();
                fetchOverview();
                fetchActivityList();
            }, 15000);
        })();
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.run(app, host="127.0.0.1", port=5000, debug=False)