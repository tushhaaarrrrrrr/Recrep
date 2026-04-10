import os
import sys
import time
import asyncio
import threading
import subprocess
import psutil

from flask import Flask, jsonify, request, render_template_string
from flask_socketio import SocketIO, emit

from services.db_service import DBService
from database.connection import init_db_pool, close_db_pool
from utils.logger import get_logger

BOT_SCRIPT = "main.py"
PID_FILE = "bot.pid"
VENV_PYTHON = sys.executable
LOG_FILE = "bot.log"

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "replace-this-in-production")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Async event loop
# ─────────────────────────────────────────────────────────────────────────────
_db_pool = None
_event_loop = None


async def _init_global_pool():
    global _db_pool
    _db_pool = await init_db_pool()
    DBService._db_pool = _db_pool


def _start_async_loop():
    global _event_loop
    _event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_event_loop)
    _event_loop.run_until_complete(_init_global_pool())
    _event_loop.run_forever()


def run_async(coro):
    if _event_loop is None:
        raise RuntimeError("Async loop not started")
    return asyncio.run_coroutine_threadsafe(coro, _event_loop).result(timeout=30)


_loop_thread = threading.Thread(target=_start_async_loop, daemon=True)
_loop_thread.start()
time.sleep(0.5)

# ─────────────────────────────────────────────────────────────────────────────
# Bot process management
# ─────────────────────────────────────────────────────────────────────────────
def _get_bot_process():
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        proc = psutil.Process(pid)
        if BOT_SCRIPT in " ".join(proc.cmdline()):
            return proc
        os.remove(PID_FILE)
        return None
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError, FileNotFoundError):
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        return None


def get_bot_status():
    proc = _get_bot_process()
    if proc is None:
        return {"running": False, "pid": None, "uptime": None}
    secs = int(time.time() - proc.create_time())
    uptime = f"{secs//86400}d {(secs%86400)//3600}h {(secs%3600)//60}m {secs%60}s"
    return {"running": True, "pid": proc.pid, "uptime": uptime}


VENV_PYTHON = sys.executable

def start_bot():
    if _get_bot_process():
        return False, "Bot is already running."

    log_file = open(LOG_FILE, "a")

    try:
        proc = subprocess.Popen(
            [VENV_PYTHON, BOT_SCRIPT],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    except Exception as e:
        log_file.close()
        return False, f"Failed to start bot: {e}"

    with open(PID_FILE, "w") as f:
        f.write(str(proc.pid))

    return True, f"Bot started (PID {proc.pid})."


def stop_bot():
    proc = _get_bot_process()
    if not proc:
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


# ─────────────────────────────────────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────────────────────────────────────
FORM_TABLES = [
    "recruitment",
    "progress_report",
    "purchase_invoice",
    "demolition_report",
    "eviction_report",
    "scroll_completion",
]


async def async_get_overview():
    approved_counts, pending_counts = {}, {}
    total_approved = total_pending = 0

    for table in FORM_TABLES:
        r_a = await DBService.fetchrow(f"SELECT COUNT(*) FROM {table} WHERE status='approved'")
        r_p = await DBService.fetchrow(f"SELECT COUNT(*) FROM {table} WHERE status='pending'")
        a, p = (r_a[0] if r_a else 0), (r_p[0] if r_p else 0)
        approved_counts[table], pending_counts[table] = a, p
        total_approved += a
        total_pending += p

    r_rep = await DBService.fetchrow("SELECT COALESCE(SUM(reputation),0) FROM staff_member")
    r_staff = await DBService.fetchrow("SELECT COUNT(*) FROM staff_member")
    return {
        "totals": {
            "approved_total": total_approved,
            "pending_total": total_pending,
            "reputation_total": r_rep[0] if r_rep else 0,
            "staff_total": r_staff[0] if r_staff else 0,
        },
        "approved_breakdown": approved_counts,
        "pending_breakdown": pending_counts,
    }


async def async_get_activity(limit=30):
    activities = []
    for table in FORM_TABLES:
        rows = await DBService.fetch(
            f"SELECT id, submitted_by, submitted_at, status FROM {table} "
            f"ORDER BY submitted_at DESC LIMIT $1", limit
        )
        for row in rows:
            activities.append({
                "table": table,
                "id": row["id"],
                "submitted_by": str(row["submitted_by"]),
                "submitted_at": row["submitted_at"].isoformat(),
                "status": row["status"],
            })
    activities.sort(key=lambda x: x["submitted_at"], reverse=True)
    return activities[:limit]


async def async_get_activity_timeseries(granularity: str):
    granularity = granularity.lower()
    if granularity not in {"daily", "weekly", "monthly"}:
        granularity = "weekly"

    if granularity == "daily":
        span, label_fn = 7, lambda i: "Today" if i == 0 else f"{i}d ago"
        bounds = lambda i: (
            f"CURRENT_DATE - INTERVAL '{i} day'",
            f"CURRENT_DATE - INTERVAL '{i-1} day'"
        )
    elif granularity == "monthly":
        span, label_fn = 6, lambda i: "This month" if i == 0 else f"{i}mo ago"
        bounds = lambda i: (
            f"date_trunc('month',CURRENT_DATE) - INTERVAL '{i} month'",
            f"date_trunc('month',CURRENT_DATE) - INTERVAL '{i-1} month'"
        )
    else:
        span, label_fn = 8, lambda i: "This week" if i == 0 else f"{i}w ago"
        bounds = lambda i: (
            f"date_trunc('week',CURRENT_DATE) - INTERVAL '{i} week'",
            f"date_trunc('week',CURRENT_DATE) - INTERVAL '{i-1} week'"
        )

    labels, series = [], {k: [] for k in [
        "recruitment", "progress_report", "progress_help",
        "purchase_invoice", "demolition_report", "eviction_report",
        "scroll_completion", "reputation"
    ]}

    for i in range(span - 1, -1, -1):
        start_expr, end_expr = bounds(i)
        labels.append(label_fn(i))
        for t in FORM_TABLES:
            r = await DBService.fetchrow(
                f"SELECT COUNT(*) FROM {t} WHERE status='approved' "
                f"AND submitted_at >= {start_expr} AND submitted_at < {end_expr}"
            )
            series[t].append(r[0] if r else 0)
        r_h = await DBService.fetchrow(
            f"SELECT COUNT(*) FROM reputation_log WHERE form_type='progress_help' "
            f"AND created_at >= {start_expr} AND created_at < {end_expr}"
        )
        series["progress_help"].append(r_h[0] if r_h else 0)
        r_rep = await DBService.fetchrow(
            f"SELECT COALESCE(SUM(points),0) FROM reputation_log "
            f"WHERE created_at >= {start_expr} AND created_at < {end_expr}"
        )
        series["reputation"].append(r_rep[0] if r_rep else 0)

    return {"labels": labels, "series": series}


async def async_get_leaderboard(category, period):
    category, period = category.lower(), period.lower()
    if category == "reputation":
        return await DBService.get_leaderboard(period)
    return await DBService.get_category_leaderboard(category, period)


async def async_get_staff_directory():
    rows = await DBService.fetch(
        "SELECT discord_id, display_name, reputation FROM staff_member ORDER BY reputation DESC"
    )
    staff_map = {}
    for row in rows:
        did = row["discord_id"]
        sid = str(did)  # Always store as string to avoid JS precision loss
        raw = row.get("display_name") or ""
        label = raw if raw else (f"User {sid[:4]}…{sid[-4:]}" if len(sid) > 8 else f"User {sid}")
        staff_map[sid] = {
            "discord_id": sid, "label": label, "reputation": row["reputation"],
            "recruitment": 0, "progress_report": 0, "progress_help": 0,
            "purchase_invoice": 0, "demolition_report": 0, "eviction_report": 0,
            "scroll_completion": 0, "approvals": 0, "roles": [],
        }

    for table in FORM_TABLES:
        rows = await DBService.fetch(
            f"SELECT submitted_by, COUNT(*) as cnt FROM {table} WHERE status='approved' GROUP BY submitted_by"
        )
        for r in rows:
            sid = str(r["submitted_by"])
            if sid not in staff_map:
                staff_map[sid] = {
                    "discord_id": sid, "label": f"User {sid}", "reputation": 0,
                    "recruitment": 0, "progress_report": 0, "progress_help": 0,
                    "purchase_invoice": 0, "demolition_report": 0, "eviction_report": 0,
                    "scroll_completion": 0, "approvals": 0, "roles": [],
                }
            staff_map[sid][table] = r["cnt"]

    rows = await DBService.fetch(
        "SELECT staff_id, COUNT(*) as cnt FROM reputation_log WHERE form_type='progress_help' GROUP BY staff_id"
    )
    for r in rows:
        sid = str(r["staff_id"])
        if sid in staff_map:
            staff_map[sid]["progress_help"] = r["cnt"]

    for table in FORM_TABLES:
        rows = await DBService.fetch(
            f"SELECT approved_by, COUNT(*) as cnt FROM {table} "
            f"WHERE status='approved' AND approved_by IS NOT NULL GROUP BY approved_by"
        )
        for r in rows:
            sid = str(r["approved_by"])
            if sid in staff_map:
                staff_map[sid]["approvals"] += r["cnt"]

    for sid, data in staff_map.items():
        try:
            data["roles"] = await DBService.get_user_roles(int(sid))
        except Exception:
            data["roles"] = []

    return sorted(staff_map.values(), key=lambda x: x["reputation"], reverse=True)


async def async_get_user_history(discord_id_str: str):
    """
    BUG FIX: discord_id is received and stored as string to prevent JS integer precision loss.
    We cast to Python int only when querying the database.
    """
    try:
        discord_id_int = int(discord_id_str)
    except (TypeError, ValueError):
        return {"history": [], "counts": {}, "error": "Invalid user ID"}

    history, counts = [], {}
    for table in FORM_TABLES:
        try:
            rows = await DBService.fetch(
                f"SELECT id, submitted_at, status FROM {table} "
                f"WHERE submitted_by = $1::bigint ORDER BY submitted_at DESC",
                discord_id_int
            )
        except Exception as e:
            logger.error(f"Error querying {table} for {discord_id_int}: {e}")
            rows = []
        for row in rows:
            history.append({
                "table": table, "id": row["id"],
                "submitted_at": row["submitted_at"].isoformat(),
                "status": row["status"],
            })
        try:
            r = await DBService.fetchrow(
                f"SELECT COUNT(*) FROM {table} WHERE submitted_by = $1::bigint", discord_id_int
            )
            counts[table] = r[0] if r else 0
        except Exception as e:
            logger.error(f"Error counting {table} for {discord_id_int}: {e}")
            counts[table] = 0

    history.sort(key=lambda x: x["submitted_at"], reverse=True)
    return {"history": history, "counts": counts}


# ─────────────────────────────────────────────────────────────────────────────
# Flask routes
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/status")
def api_status():
    return jsonify(get_bot_status())


@app.route("/api/overview")
def api_overview():
    try:
        return jsonify(run_async(async_get_overview()))
    except Exception as e:
        logger.exception("Overview error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/activity")
def api_activity():
    try:
        return jsonify(run_async(async_get_activity(30)))
    except Exception as e:
        logger.exception("Activity error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/activity_timeseries")
def api_activity_timeseries():
    try:
        return jsonify(run_async(async_get_activity_timeseries(request.args.get("granularity", "weekly"))))
    except Exception as e:
        logger.exception("Timeseries error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/leaderboard/<category>/<period>")
def api_leaderboard(category, period):
    try:
        return jsonify(run_async(async_get_leaderboard(category, period)))
    except Exception as e:
        logger.exception("Leaderboard error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/staff")
def api_staff():
    try:
        return jsonify({"staff": run_async(async_get_staff_directory())})
    except Exception as e:
        logger.exception("Staff error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/form/<string:table>/<int:form_id>")
def api_form_detail(table, form_id):
    try:
        row = run_async(DBService.fetchrow(f"SELECT * FROM {table} WHERE id = $1", form_id))
        return jsonify(dict(row) if row else None)
    except Exception as e:
        logger.exception("Form detail error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/user/<discord_id>/history")
def api_user_history(discord_id):
    """
    BUG FIX: Route accepts discord_id as string (not <int:>) to preserve full precision.
    The JS side sends it as a string that was originally serialized from the Python backend,
    so no precision loss occurs in the URL.
    """
    try:
        return jsonify(run_async(async_get_user_history(discord_id)))
    except Exception as e:
        logger.exception(f"User history error for {discord_id}")
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


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket live logs
# ─────────────────────────────────────────────────────────────────────────────
def _log_watcher():
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if line:
                socketio.emit("log", {"line": line.strip()})
            else:
                time.sleep(0.3)


@socketio.on("connect")
def handle_connect():
    emit("connected", {"data": "Connected"})
    if not hasattr(app, "_log_thread"):
        app._log_thread = threading.Thread(target=_log_watcher, daemon=True)
        app._log_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# HTML Template
# ─────────────────────────────────────────────────────────────────────────────
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RECREP · Control</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Oxanium:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@300;400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
<style>
:root {
  --bg:       #05070e;
  --s0:       #080c17;
  --s1:       #0d1220;
  --s2:       #121828;
  --s3:       #182030;
  --border:   #1e2d44;
  --border2:  #253548;
  --cyan:     #00d4ff;
  --cyan-dim: #0099bb;
  --amber:    #f5a623;
  --green:    #00e5a0;
  --red:      #ff4757;
  --purple:   #9d7ef5;
  --text:     #d8e4f0;
  --text-2:   #7a99b8;
  --text-3:   #3d5670;
  --font:     'Oxanium', sans-serif;
  --mono:     'JetBrains Mono', monospace;
  --r:        6px;
  --r2:       10px;
  --transition: 0.18s cubic-bezier(0.4, 0, 0.2, 1);
}

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

html { scroll-behavior: smooth; }

body {
  background: var(--bg);
  color: var(--text);
  font-family: var(--font);
  font-size: 13px;
  min-height: 100vh;
  overflow-x: hidden;
  -webkit-font-smoothing: antialiased;
}

body::before {
  content: '';
  position: fixed;
  inset: 0;
  background-image:
    linear-gradient(rgba(0,212,255,0.015) 1px, transparent 1px),
    linear-gradient(90deg, rgba(0,212,255,0.015) 1px, transparent 1px);
  background-size: 40px 40px;
  pointer-events: none;
  z-index: 0;
}

/* ── Layout ── */
.shell { position: relative; z-index: 1; max-width: 1280px; margin: 0 auto; padding: 0 20px 40px; }

/* ── Header ── */
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 18px 0 16px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 20px;
}

.logo {
  display: flex;
  align-items: center;
  gap: 12px;
}

.logo-mark {
  width: 36px;
  height: 36px;
  background: linear-gradient(135deg, var(--cyan), var(--purple));
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 16px;
  font-weight: 800;
  color: #000;
  letter-spacing: -1px;
  flex-shrink: 0;
}

.logo-text { font-size: 18px; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; }
.logo-sub { font-size: 10px; color: var(--text-3); letter-spacing: 0.2em; text-transform: uppercase; margin-top: 1px; font-family: var(--mono); }

.header-right { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }

.status-chip {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 7px 14px;
  background: var(--s1);
  border: 1px solid var(--border);
  border-radius: 999px;
  font-family: var(--mono);
  font-size: 11px;
}

.status-dot {
  width: 7px; height: 7px;
  border-radius: 50%;
  background: var(--red);
  box-shadow: 0 0 8px var(--red);
  transition: background var(--transition), box-shadow var(--transition);
}
.status-dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }

.status-label { color: var(--text-2); }
.status-val { color: var(--text); font-weight: 500; }

.uptime-chip {
  padding: 7px 14px;
  background: var(--s1);
  border: 1px solid var(--border);
  border-radius: 999px;
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text-2);
}
.uptime-chip span { color: var(--cyan); }

/* ── Controls ── */
.controls {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 12px 16px;
  background: var(--s1);
  border: 1px solid var(--border);
  border-radius: var(--r2);
  margin-bottom: 20px;
}

.btn-group { display: flex; gap: 8px; flex-wrap: wrap; }

.btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 7px 16px;
  border-radius: 999px;
  border: 1px solid transparent;
  font-family: var(--font);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all var(--transition);
  background: var(--s2);
  color: var(--text-2);
  border-color: var(--border);
}

.btn:hover { color: var(--text); border-color: var(--border2); transform: translateY(-1px); box-shadow: 0 6px 20px rgba(0,0,0,0.4); }

.btn-cyan { background: rgba(0,212,255,0.1); border-color: rgba(0,212,255,0.35); color: var(--cyan); }
.btn-cyan:hover { background: rgba(0,212,255,0.18); border-color: var(--cyan); box-shadow: 0 0 20px rgba(0,212,255,0.2); }

.btn-red { background: rgba(255,71,87,0.1); border-color: rgba(255,71,87,0.35); color: var(--red); }
.btn-red:hover { background: rgba(255,71,87,0.18); border-color: var(--red); }

.btn-amber { background: rgba(245,166,35,0.08); border-color: rgba(245,166,35,0.25); color: var(--amber); }
.btn-amber:hover { background: rgba(245,166,35,0.14); border-color: var(--amber); }

.ctrl-msg {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text-2);
  min-height: 16px;
  display: flex;
  align-items: center;
  gap: 8px;
}

/* ── Tabs ── */
.tabs { display: flex; gap: 2px; border-bottom: 1px solid var(--border); margin-bottom: 20px; }

.tab {
  padding: 10px 18px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--text-3);
  cursor: pointer;
  position: relative;
  transition: color var(--transition);
  border-bottom: 2px solid transparent;
  margin-bottom: -1px;
}

.tab:hover { color: var(--text-2); }
.tab.active { color: var(--cyan); border-bottom-color: var(--cyan); }

.panel { display: none; }
.panel.active { display: block; }

/* ── Grid ── */
.g2 { display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px; }
.g4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
.mt { margin-top: 14px; }

@media (max-width: 960px) {
  .g4 { grid-template-columns: repeat(2, 1fr); }
  .header { flex-wrap: wrap; }
}
@media (max-width: 600px) {
  .g4 { grid-template-columns: 1fr; }
  .g2 { grid-template-columns: 1fr; }
}

/* ── Card ── */
.card {
  background: var(--s1);
  border: 1px solid var(--border);
  border-radius: var(--r2);
  padding: 16px;
  position: relative;
  overflow: hidden;
  transition: border-color var(--transition);
}

.card::before {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent, rgba(0,212,255,0.25), transparent);
  opacity: 0;
  transition: opacity var(--transition);
}

.card:hover::before { opacity: 1; }
.card:hover { border-color: var(--border2); }

.card-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
  margin-bottom: 12px;
}

.card-title {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--text-3);
}

.card-sub { font-size: 10px; color: var(--text-3); margin-top: 2px; }

/* ── Stat cards ── */
.stat-num {
  font-size: 32px;
  font-weight: 700;
  letter-spacing: 0.02em;
  color: var(--text);
  line-height: 1;
  margin: 4px 0 8px;
  font-variant-numeric: tabular-nums;
}

.stat-detail {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-3);
  line-height: 1.6;
}

.stat-detail b { color: var(--text-2); font-weight: 500; }

/* ── Select / Input ── */
.sel, .inp {
  background: var(--s2);
  border: 1px solid var(--border);
  border-radius: var(--r);
  color: var(--text);
  font-family: var(--font);
  font-size: 11px;
  padding: 6px 10px;
  outline: none;
  transition: border-color var(--transition);
}
.sel:focus, .inp:focus { border-color: var(--cyan-dim); }
.inp::placeholder { color: var(--text-3); }
.sel option { background: var(--s2); }

/* ── Row of inputs ── */
.row-filters { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }

/* ── Chart containers ── */
.chart-wrap { position: relative; }
.chart-wrap canvas { display: block; width: 100% !important; }

/* ── Activity list ── */
.activity-list { list-style: none; }

.act-item {
  display: grid;
  grid-template-columns: 1fr auto auto;
  align-items: center;
  gap: 12px;
  padding: 9px 0;
  border-bottom: 1px solid rgba(30,45,68,0.7);
}
.act-item:last-child { border-bottom: none; }
.act-name { font-weight: 500; font-size: 12px; }
.act-meta { font-family: var(--mono); font-size: 10px; color: var(--text-3); margin-top: 2px; }
.act-user { font-family: var(--mono); font-size: 10px; color: var(--text-2); }

/* ── Status badges ── */
.badge {
  display: inline-flex;
  padding: 2px 9px;
  border-radius: 999px;
  font-size: 9px;
  font-weight: 600;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}
.badge-ok   { background: rgba(0,229,160,0.1); border: 1px solid rgba(0,229,160,0.3); color: var(--green); }
.badge-wait { background: rgba(245,166,35,0.1); border: 1px solid rgba(245,166,35,0.3); color: var(--amber); }
.badge-no   { background: rgba(255,71,87,0.1); border: 1px solid rgba(255,71,87,0.3); color: var(--red); }

/* ── Tables ── */
.tbl-wrap { overflow-x: auto; }

table { width: 100%; border-collapse: collapse; font-size: 12px; }

thead tr { background: var(--s0); }
th {
  padding: 9px 10px;
  text-align: left;
  font-size: 9px;
  font-weight: 600;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--text-3);
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}
th.r { text-align: right; }
td {
  padding: 8px 10px;
  white-space: nowrap;
  border-bottom: 1px solid rgba(30,45,68,0.5);
}
td.r { text-align: right; font-family: var(--mono); font-size: 11px; }

tbody tr { cursor: pointer; transition: background var(--transition); }
tbody tr:hover { background: rgba(0,212,255,0.04); }
tbody tr:last-child td { border-bottom: none; }

.name-cell .n-main { font-weight: 500; font-size: 12px; }
.name-cell .n-sub { font-family: var(--mono); font-size: 10px; color: var(--text-3); margin-top: 1px; }

.rank-num {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text-3);
  text-align: right;
}
.rank-num.gold   { color: #ffd700; }
.rank-num.silver { color: #c0c0c0; }
.rank-num.bronze { color: #cd7f32; }

.role-tag {
  display: inline-flex;
  padding: 2px 8px;
  border-radius: var(--r);
  border: 1px solid var(--border2);
  font-size: 10px;
  color: var(--text-2);
  margin: 1px 2px 1px 0;
}

/* ── Log box ── */
.log-box {
  background: var(--s0);
  border: 1px solid var(--border);
  border-radius: var(--r2);
  padding: 12px;
  height: 400px;
  overflow-y: auto;
  font-family: var(--mono);
  font-size: 11px;
  line-height: 1.7;
}

.log-line {
  padding: 1px 6px;
  border-left: 2px solid transparent;
  border-radius: 2px;
  color: var(--text-2);
  word-break: break-all;
  transition: background var(--transition);
}
.log-line:hover { background: rgba(0,212,255,0.05); border-left-color: var(--cyan); }
.log-line.err { color: var(--red); }
.log-line.warn { color: var(--amber); }
.log-line.info { color: var(--text); }

/* ── Modal ── */
.overlay {
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.7);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal {
  background: var(--s1);
  border: 1px solid var(--border2);
  border-radius: 14px;
  width: 90%;
  max-width: 680px;
  max-height: 80vh;
  display: flex;
  flex-direction: column;
  box-shadow: 0 30px 80px rgba(0,0,0,0.6);
}

.modal-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 18px 22px 14px;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}

.modal-title { font-size: 13px; font-weight: 600; }
.modal-close-btn {
  background: none; border: none;
  color: var(--text-3); font-size: 20px;
  cursor: pointer; line-height: 1;
  transition: color var(--transition);
  padding: 0 4px;
}
.modal-close-btn:hover { color: var(--text); }

.modal-body { padding: 16px 22px; overflow-y: auto; flex: 1; }

.modal-counts {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-bottom: 14px;
  padding-bottom: 12px;
  border-bottom: 1px solid var(--border);
}

.count-chip {
  padding: 3px 10px;
  background: var(--s2);
  border: 1px solid var(--border);
  border-radius: 999px;
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-2);
}
.count-chip b { color: var(--cyan); }

.hist-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  padding: 10px 0;
  border-bottom: 1px solid rgba(30,45,68,0.6);
}
.hist-item:last-child { border-bottom: none; }
.hist-name { font-weight: 500; font-size: 12px; }
.hist-date { font-family: var(--mono); font-size: 10px; color: var(--text-3); margin-top: 2px; }
.hist-actions { display: flex; align-items: center; gap: 8px; }

.view-link {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--cyan-dim);
  text-decoration: underline;
  cursor: pointer;
  transition: color var(--transition);
}
.view-link:hover { color: var(--cyan); }

/* ── Spinner ── */
.spin {
  width: 14px; height: 14px;
  border: 2px solid var(--border2);
  border-top-color: var(--cyan);
  border-radius: 50%;
  animation: spin 0.6s linear infinite;
  flex-shrink: 0;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--s0); }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 999px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-3); }

/* ── Animations ── */
@keyframes fadeUp {
  from { opacity: 0; transform: translateY(8px); }
  to   { opacity: 1; transform: translateY(0); }
}
.panel.active { animation: fadeUp 0.22s ease; }
</style>
</head>
<body>

<div class="shell">

  <!-- Header -->
  <header class="header">
    <div class="logo">
      <div class="logo-mark">RC</div>
      <div>
        <div class="logo-text">Recrep</div>
        <div class="logo-sub">Control Panel</div>
      </div>
    </div>
    <div class="header-right">
      <div class="status-chip">
        <div id="statusDot" class="status-dot"></div>
        <span class="status-label">Bot&nbsp;</span>
        <span id="statusVal" class="status-val">Offline</span>
      </div>
      <div class="uptime-chip">Uptime <span id="uptimeVal">—</span></div>
      <button class="btn btn-cyan" id="refreshBtn">⟳ Refresh</button>
    </div>
  </header>

  <!-- Controls -->
  <div class="controls">
    <div class="btn-group">
      <button class="btn btn-cyan" id="startBtn">▶ Start</button>
      <button class="btn"          id="restartBtn">↺ Restart</button>
      <button class="btn btn-red"  id="stopBtn">■ Stop</button>
      <button class="btn btn-amber" id="resetBtn">⚠ Reset DB + S3</button>
    </div>
    <div id="ctrlMsg" class="ctrl-msg"></div>
  </div>

  <!-- Tabs -->
  <nav class="tabs">
    <div class="tab active" data-panel="overview">Overview</div>
    <div class="tab" data-panel="leaderboard">Leaderboard</div>
    <div class="tab" data-panel="staff">Staff</div>
    <div class="tab" data-panel="logs">Live Logs</div>
  </nav>

  <!-- ── OVERVIEW ── -->
  <div id="overview" class="panel active">

    <!-- Stat row -->
    <div class="g4">
      <div class="card">
        <div class="card-title">Approved Forms</div>
        <div id="approvedNum" class="stat-num">0</div>
        <div id="approvedDetail" class="stat-detail">—</div>
      </div>
      <div class="card">
        <div class="card-title">Pending Forms</div>
        <div id="pendingNum" class="stat-num">0</div>
        <div id="pendingDetail" class="stat-detail">—</div>
      </div>
      <div class="card">
        <div class="card-title">Total Reputation</div>
        <div id="repNum" class="stat-num">0</div>
        <div class="stat-detail">Cumulative staff reputation</div>
      </div>
      <div class="card">
        <div class="card-title">Staff Members</div>
        <div id="staffNum" class="stat-num">0</div>
        <div class="stat-detail">Active in the system</div>
      </div>
    </div>

    <!-- Charts -->
    <div class="g2 mt">
      <div class="card">
        <div class="card-head">
          <div>
            <div class="card-title">Activity Over Time</div>
            <div class="card-sub">Forms & reputation across time buckets</div>
          </div>
          <div class="btn-group">
            <button class="btn" style="padding:4px 10px;font-size:10px;" data-gran="daily">Daily</button>
            <button class="btn" style="padding:4px 10px;font-size:10px;" data-gran="weekly">Weekly</button>
            <button class="btn" style="padding:4px 10px;font-size:10px;" data-gran="monthly">Monthly</button>
          </div>
        </div>
        <div class="chart-wrap" style="height:220px;">
          <canvas id="actChart"></canvas>
        </div>
      </div>

      <div class="card">
        <div class="card-head">
          <div>
            <div class="card-title">Form Distribution</div>
            <div class="card-sub">Top contributors per category</div>
          </div>
          <select id="distSel" class="sel" style="font-size:11px;">
            <option value="recruitment">Recruitments</option>
            <option value="progress_report">Progress Reports</option>
            <option value="progress_help">Progress Help</option>
            <option value="purchase_invoice">Invoices</option>
            <option value="demolition_report">Demolitions</option>
            <option value="eviction_report">Evictions</option>
            <option value="scroll_completion">Scrolls</option>
          </select>
        </div>
        <div style="height:220px;overflow-y:auto;">
          <div id="distInner" style="min-height:100%;">
            <canvas id="distChart"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- Recent activity -->
    <div class="card mt">
      <div class="card-head">
        <div>
          <div class="card-title">Recent Activity</div>
          <div class="card-sub">Latest form submissions across all categories</div>
        </div>
      </div>
      <ul id="actList" class="activity-list"></ul>
    </div>
  </div>

  <!-- ── LEADERBOARD ── -->
  <div id="leaderboard" class="panel">
    <div class="card">
      <div class="card-head">
        <div>
          <div class="card-title">Leaderboard</div>
          <div class="card-sub">Staff rankings by category and period</div>
        </div>
      </div>
      <div class="row-filters" style="margin-bottom:14px;">
        <select id="lbCat" class="sel">
          <option value="reputation">Reputation</option>
          <option value="recruitment">Recruitments</option>
          <option value="progress_report">Progress Reports</option>
          <option value="progress_help">Progress Help</option>
          <option value="purchase_invoice">Invoices</option>
          <option value="demolition_report">Demolitions</option>
          <option value="eviction_report">Evictions</option>
          <option value="scroll_completion">Scrolls</option>
        </select>
        <select id="lbPeriod" class="sel">
          <option value="weekly">Weekly</option>
          <option value="biweekly">Bi-weekly</option>
          <option value="monthly">Monthly</option>
          <option value="all">All Time</option>
        </select>
        <input id="lbSearch" class="inp" placeholder="Search by name or ID…" style="flex:1;min-width:160px;">
      </div>
      <div class="tbl-wrap">
        <table>
          <thead>
            <tr>
              <th style="width:50px;" class="r">Rank</th>
              <th>Name</th>
              <th class="r">Score</th>
            </tr>
          </thead>
          <tbody id="lbBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ── STAFF ── -->
  <div id="staff" class="panel">
    <div class="card">
      <div class="card-head">
        <div>
          <div class="card-title">Staff Directory</div>
          <div class="card-sub">Click any row to view submitted form history</div>
        </div>
      </div>
      <div class="tbl-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th class="r">Rep</th>
              <th class="r">Rec</th>
              <th class="r">Prog</th>
              <th class="r">Help</th>
              <th class="r">Inv</th>
              <th class="r">Demo</th>
              <th class="r">Evict</th>
              <th class="r">Scroll</th>
              <th class="r">Appr</th>
              <th>Roles</th>
            </tr>
          </thead>
          <tbody id="staffBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ── LOGS ── -->
  <div id="logs" class="panel">
    <div class="card">
      <div class="card-head">
        <div>
          <div class="card-title">Live Console</div>
          <div class="card-sub">Real-time output from the bot process</div>
        </div>
        <button class="btn" id="clearLogsBtn" style="font-size:10px;padding:4px 10px;">Clear</button>
      </div>
      <div id="logBox" class="log-box"></div>
    </div>
  </div>

</div>

<!-- Modal -->
<div id="histOverlay" class="overlay" style="display:none;">
  <div class="modal">
    <div class="modal-head">
      <div id="modalTitle" class="modal-title">User History</div>
      <button id="modalClose" class="modal-close-btn">&times;</button>
    </div>
    <div id="modalBody" class="modal-body">
      <div style="display:flex;align-items:center;gap:10px;color:var(--text-2);">
        <div class="spin"></div> Loading…
      </div>
    </div>
  </div>
</div>

<script>
// ── Utilities ──────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt = n => (n ?? 0).toLocaleString('en-US');
const TABLE_LABELS = {
  recruitment:'Rec', progress_report:'Prog', purchase_invoice:'Inv',
  demolition_report:'Demo', eviction_report:'Evict', scroll_completion:'Scroll'
};
const COLORS = {
  recruitment:'#22d3ee', progress_report:'#f59e0b', progress_help:'#a78bfa',
  purchase_invoice:'#34d399', demolition_report:'#f87171', eviction_report:'#fb7185',
  scroll_completion:'#60a5fa', reputation:'#ffffff'
};

// ── State ──────────────────────────────────────────────────────────────────
let staffData = [], leaderboardRows = [], nameMap = {}, currentGran = 'weekly';
let actChart = null, distChart = null;

// ── Status ─────────────────────────────────────────────────────────────────
async function loadStatus() {
  const d = await apiFetch('/api/status');
  if (!d) return;
  const dot = $('statusDot'), val = $('statusVal'), up = $('uptimeVal');
  if (d.running) {
    dot.classList.add('on');
    val.textContent = `Online · PID ${d.pid}`;
    up.textContent = d.uptime || '—';
  } else {
    dot.classList.remove('on');
    val.textContent = 'Offline';
    up.textContent = '—';
  }
}

// ── Overview ───────────────────────────────────────────────────────────────
async function loadOverview() {
  const d = await apiFetch('/api/overview');
  if (!d || d.error) return;
  const t = d.totals || {};
  $('approvedNum').textContent = fmt(t.approved_total);
  $('pendingNum').textContent = fmt(t.pending_total);
  $('repNum').textContent = fmt(t.reputation_total);
  $('staffNum').textContent = fmt(t.staff_total);

  const ab = d.approved_breakdown || {}, pb = d.pending_breakdown || {};
  const buildDetail = (src) => Object.entries(TABLE_LABELS)
    .map(([k,l]) => `${l} <b>${fmt(src[k]||0)}</b>`).join(' · ');
  $('approvedDetail').innerHTML = buildDetail(ab);
  $('pendingDetail').innerHTML = buildDetail(pb);
}

// ── Activity list ──────────────────────────────────────────────────────────
async function loadActivity() {
  const data = await apiFetch('/api/activity');
  if (!data) return;
  const ul = $('actList');
  ul.innerHTML = '';
  if (!data.length) {
    ul.innerHTML = '<li style="color:var(--text-3);padding:10px 0;font-size:11px;">No recent activity.</li>';
    return;
  }
  for (const a of data) {
    const li = document.createElement('li');
    li.className = 'act-item';
    const d = new Date(a.submitted_at);
    const sc = a.status === 'approved' ? 'badge-ok' : a.status === 'pending' ? 'badge-wait' : 'badge-no';
    li.innerHTML = `
      <div>
        <div class="act-name">${(a.table||'').replace(/_/g,' ').toUpperCase()} #${a.id}</div>
        <div class="act-meta">${d.toLocaleString()}</div>
      </div>
      <div class="act-user">${a.submitted_by}</div>
      <div><span class="badge ${sc}">${a.status}</span></div>`;
    ul.appendChild(li);
  }
}

// ── Time series chart ──────────────────────────────────────────────────────
async function loadTimeseries(gran) {
  const d = await apiFetch(`/api/activity_timeseries?granularity=${gran}`);
  if (!d || d.error) return;
  const labels = d.labels || [], series = d.series || {};
  const cfg = [
    {k:'recruitment',l:'Recruitments'},{k:'progress_report',l:'Progress'},{k:'progress_help',l:'Help'},
    {k:'purchase_invoice',l:'Invoices'},{k:'demolition_report',l:'Demolitions'},
    {k:'eviction_report',l:'Evictions'},{k:'scroll_completion',l:'Scrolls'},{k:'reputation',l:'Reputation'}
  ];
  const datasets = cfg.map(c => ({
    label: c.l,
    data: (series[c.k]||[]).map(x=>x||0),
    borderColor: COLORS[c.k],
    backgroundColor: COLORS[c.k] + '22',
    tension: 0.4,
    fill: c.k !== 'reputation',
    borderWidth: c.k === 'reputation' ? 2 : 1.5,
    pointRadius: 2, pointHoverRadius: 4
  }));
  const ctx = $('actChart').getContext('2d');
  const opts = {
    responsive: true, maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color:'#7a99b8', font:{size:10, family:'JetBrains Mono'}, boxWidth:10 } }
    },
    scales: {
      x: { grid:{color:'rgba(30,45,68,0.7)'}, ticks:{color:'#7a99b8', font:{size:10}} },
      y: { beginAtZero:true, grid:{color:'rgba(30,45,68,0.7)'}, ticks:{color:'#7a99b8', font:{size:10}} }
    }
  };
  if (!actChart) {
    actChart = new Chart(ctx, {type:'line', data:{labels, datasets}, options:opts});
  } else {
    actChart.data.labels = labels;
    actChart.data.datasets = datasets;
    actChart.update();
  }
}

// ── Distribution chart ─────────────────────────────────────────────────────
function updateDistChart() {
  const cat = $('distSel').value;
  if (!staffData.length) return;
  const sorted = [...staffData].sort((a,b)=>(b[cat]||0)-(a[cat]||0)).slice(0,14);
  const labels = sorted.map(s => s.label || `User ${s.discord_id}`);
  const values = sorted.map(s => s[cat] || 0);
  const inner = $('distInner');
  inner.style.height = Math.max(220, labels.length * 28) + 'px';
  const ctx = $('distChart').getContext('2d');
  if (!distChart) {
    distChart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets:[{
        label:'Forms', data:values,
        backgroundColor: sorted.map((_,i) => `hsl(${190 + i*12},70%,55%)`),
        borderRadius: 3, barPercentage:0.75
      }]},
      options: {
        indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins: {
          legend:{display:false},
          tooltip:{callbacks:{label:c=>`${c.raw} forms`}}
        },
        scales: {
          x: { beginAtZero:true, grid:{color:'rgba(30,45,68,0.7)'}, ticks:{color:'#7a99b8',font:{size:10}} },
          y: { grid:{display:false}, ticks:{color:'#d8e4f0',font:{size:11},autoSkip:false,maxRotation:0} }
        }
      }
    });
  } else {
    distChart.data.labels = labels;
    distChart.data.datasets[0].data = values;
    distChart.data.datasets[0].backgroundColor = sorted.map((_,i) => `hsl(${190+i*12},70%,55%)`);
    distChart.update();
  }
}

// ── Staff directory ────────────────────────────────────────────────────────
async function loadStaff() {
  const d = await apiFetch('/api/staff');
  if (!d || d.error) return;
  staffData = d.staff || [];
  nameMap = {};
  for (const s of staffData) nameMap[s.discord_id] = s.label || `User ${s.discord_id}`;
  renderStaff();
  updateDistChart();
}

function renderStaff() {
  const tbody = $('staffBody');
  tbody.innerHTML = '';
  if (!staffData.length) {
    tbody.innerHTML = '<tr><td colspan="11" style="color:var(--text-3);padding:14px 10px;">No staff records.</td></tr>';
    return;
  }
  for (const s of staffData) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>
        <div class="name-cell">
          <div class="n-main">${s.label}</div>
          <div class="n-sub">${s.discord_id}</div>
        </div>
      </td>
      <td class="r">${fmt(s.reputation)}</td>
      <td class="r">${fmt(s.recruitment)}</td>
      <td class="r">${fmt(s.progress_report)}</td>
      <td class="r">${fmt(s.progress_help)}</td>
      <td class="r">${fmt(s.purchase_invoice)}</td>
      <td class="r">${fmt(s.demolition_report)}</td>
      <td class="r">${fmt(s.eviction_report)}</td>
      <td class="r">${fmt(s.scroll_completion)}</td>
      <td class="r">${fmt(s.approvals)}</td>
      <td>${(s.roles||[]).map(r=>`<span class="role-tag">${r}</span>`).join('')}</td>`;
    // discord_id is already a string from the server – no precision loss
    tr.addEventListener('click', () => openUserHistory(s.discord_id, s.label));
    tbody.appendChild(tr);
  }
}

// ── User history modal ─────────────────────────────────────────────────────
// BUG FIX: discord_id is a string throughout JS. No parseInt() anywhere.
// The server returns it as a string; we pass it directly in the URL.
async function openUserHistory(discordId, label) {
  $('modalTitle').textContent = `History · ${label}`;
  $('modalBody').innerHTML = `<div style="display:flex;align-items:center;gap:10px;color:var(--text-2);"><div class="spin"></div> Loading…</div>`;
  $('histOverlay').style.display = 'flex';

  const d = await apiFetch(`/api/user/${discordId}/history`);
  if (!d) { $('modalBody').innerHTML = `<p style="color:var(--red);">Failed to load.</p>`; return; }
  if (d.error) { $('modalBody').innerHTML = `<p style="color:var(--red);">Error: ${d.error}</p>`; return; }

  const counts = d.counts || {}, history = d.history || [];

  // Count chips
  let html = `<div class="modal-counts">` +
    Object.entries(counts).map(([t,c]) =>
      `<span class="count-chip">${t.replace(/_/g,' ')} <b>${c}</b></span>`
    ).join('') + `</div>`;

  if (!history.length) {
    html += `<p style="color:var(--text-3);font-size:12px;">No submitted forms found for this user.</p>`;
  } else {
    for (const item of history) {
      const dt = new Date(item.submitted_at);
      const sc = item.status==='approved'?'badge-ok':item.status==='pending'?'badge-wait':'badge-no';
      html += `<div class="hist-item">
        <div>
          <div class="hist-name">${item.table.replace(/_/g,' ').toUpperCase()} #${item.id}</div>
          <div class="hist-date">${dt.toLocaleString()}</div>
        </div>
        <div class="hist-actions">
          <span class="badge ${sc}">${item.status}</span>
          <span class="view-link" data-table="${item.table}" data-id="${item.id}">View →</span>
        </div>
      </div>`;
    }
  }
  $('modalBody').innerHTML = html;

  document.querySelectorAll('.view-link').forEach(el => {
    el.addEventListener('click', async e => {
      e.stopPropagation();
      const r = await apiFetch(`/api/form/${el.dataset.table}/${el.dataset.id}`);
      if (r) alert(JSON.stringify(r, null, 2));
    });
  });
}

// ── Leaderboard ────────────────────────────────────────────────────────────
async function loadLeaderboard() {
  const cat = $('lbCat').value, period = $('lbPeriod').value;
  const d = await apiFetch(`/api/leaderboard/${cat}/${period}`);
  if (!d) return;
  leaderboardRows = d;
  renderLeaderboard();
}

function renderLeaderboard() {
  const tbody = $('lbBody');
  tbody.innerHTML = '';
  if (!leaderboardRows.length) {
    tbody.innerHTML = '<tr><td colspan="3" style="color:var(--text-3);padding:14px;">No data.</td></tr>';
    return;
  }
  const q = ($('lbSearch').value||'').toLowerCase();
  let rank = 1;
  for (const row of leaderboardRows) {
    const id = String(row.discord_id||'');
    const label = nameMap[id] || `User ${id}`;
    if (q && !(label+' '+id).toLowerCase().includes(q)) continue;
    const val = row.points ?? row.count ?? 0;
    const rankClass = rank===1?'gold':rank===2?'silver':rank===3?'bronze':'';
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="rank-num ${rankClass}">#${rank}</td>
      <td><div class="name-cell"><div class="n-main">${label}</div><div class="n-sub">${id}</div></div></td>
      <td class="r">${fmt(val)}</td>`;
    tbody.appendChild(tr);
    rank++;
  }
  if (!tbody.children.length)
    tbody.innerHTML = '<tr><td colspan="3" style="color:var(--text-3);padding:14px;">No results match filter.</td></tr>';
}

// ── Bot actions ────────────────────────────────────────────────────────────
async function botAction(action) {
  const msg = $('ctrlMsg');
  msg.innerHTML = '<div class="spin"></div> Processing…';
  try {
    const r = await fetch(`/${action}`, {method:'POST'});
    const d = await r.json();
    msg.textContent = d.message || 'Done.';
    setTimeout(()=>msg.textContent='', 3500);
    if (['start','stop','restart'].includes(action)) {
      setTimeout(()=>{ loadStatus(); loadOverview(); loadActivity(); loadStaff(); }, 1500);
    } else if (action==='reset') {
      setTimeout(()=>location.reload(), 3000);
    }
  } catch(e) {
    msg.textContent = 'Error: ' + e.message;
  }
}

// ── Fetch helper ───────────────────────────────────────────────────────────
async function apiFetch(url) {
  try {
    const r = await fetch(url);
    return await r.json();
  } catch(e) {
    console.error(url, e);
    return null;
  }
}

// ── WebSocket logs ─────────────────────────────────────────────────────────
const socket = io();
socket.on('log', d => {
  const box = $('logBox');
  const el = document.createElement('div');
  el.className = 'log-line';
  const txt = d.line || '';
  if (/error|exception|traceback/i.test(txt)) el.classList.add('err');
  else if (/warn/i.test(txt)) el.classList.add('warn');
  else if (/info/i.test(txt)) el.classList.add('info');
  el.textContent = txt;
  box.appendChild(el);
  box.scrollTop = box.scrollHeight;
  if (box.children.length > 1000) box.removeChild(box.firstChild);
});

// ── Event wiring ───────────────────────────────────────────────────────────
$('startBtn').onclick = ()=>botAction('start');
$('stopBtn').onclick  = ()=>botAction('stop');
$('restartBtn').onclick = ()=>botAction('restart');
$('resetBtn').onclick = ()=>botAction('reset');
$('clearLogsBtn').onclick = ()=>$('logBox').innerHTML='';

$('refreshBtn').onclick = ()=>{
  loadStatus(); loadOverview(); loadActivity();
  loadStaff(); loadLeaderboard(); loadTimeseries(currentGran);
};

document.querySelectorAll('[data-gran]').forEach(btn => {
  btn.addEventListener('click', ()=>{
    currentGran = btn.dataset.gran;
    loadTimeseries(currentGran);
  });
});

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', ()=>{
    document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
    tab.classList.add('active');
    $(tab.dataset.panel).classList.add('active');
    if (tab.dataset.panel === 'staff') {
      setTimeout(()=>{ distChart?.resize(); distChart?.update(); }, 60);
    }
    if (tab.dataset.panel === 'leaderboard') loadLeaderboard();
  });
});

$('distSel').addEventListener('change', updateDistChart);
$('lbCat').addEventListener('change', loadLeaderboard);
$('lbPeriod').addEventListener('change', loadLeaderboard);
$('lbSearch').addEventListener('input', renderLeaderboard);

$('modalClose').onclick = ()=>$('histOverlay').style.display='none';
$('histOverlay').addEventListener('click', e=>{ if(e.target===$('histOverlay')) $('histOverlay').style.display='none'; });

// ── Init ───────────────────────────────────────────────────────────────────
(async function init() {
  await loadStatus();
  await Promise.all([loadOverview(), loadActivity(), loadStaff(), loadLeaderboard()]);
  await loadTimeseries(currentGran);
  setInterval(()=>{ loadStatus(); loadOverview(); loadActivity(); }, 15000);
})();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.run(app, host="127.0.0.1", port=5000, debug=False)