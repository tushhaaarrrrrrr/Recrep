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
PID_FILE   = "bot.pid"
LOG_FILE   = "bot.log"
VENV_PYTHON = sys.executable

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "replace-this-in-production")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

logger = get_logger(__name__)

# ── Async event loop ──────────────────────────────────────────────────────────
_db_pool    = None
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

# ── Bot process management ────────────────────────────────────────────────────
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
    secs   = int(time.time() - proc.create_time())
    uptime = f"{secs//86400}d {(secs%86400)//3600}h {(secs%3600)//60}m {secs%60}s"
    return {"running": True, "pid": proc.pid, "uptime": uptime}

def start_bot():
    if _get_bot_process():
        return False, "Bot is already running."
    log_file = open(LOG_FILE, "a")
    try:
        proc = subprocess.Popen(
            [VENV_PYTHON, BOT_SCRIPT],
            stdout=log_file, stderr=subprocess.STDOUT, start_new_session=True
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
        subprocess.run([VENV_PYTHON, "reset_db.py"],  check=True)
        subprocess.run([VENV_PYTHON, "reset_s3.py"],  check=True)
    except subprocess.CalledProcessError as e:
        return False, f"Reset failed: {e}"
    start_bot()
    return True, "Database and S3 reset, bot restarted."

# ── Data helpers ──────────────────────────────────────────────────────────────
FORM_TABLES = [
    "recruitment", "progress_report", "purchase_invoice",
    "demolition_report", "eviction_report", "scroll_completion",
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
        total_pending  += p
    r_rep   = await DBService.fetchrow("SELECT COALESCE(SUM(reputation),0) FROM staff_member")
    r_staff = await DBService.fetchrow("SELECT COUNT(*) FROM staff_member")
    return {
        "totals": {
            "approved_total":  total_approved,
            "pending_total":   total_pending,
            "reputation_total": r_rep[0] if r_rep else 0,
            "staff_total":     r_staff[0] if r_staff else 0,
        },
        "approved_breakdown": approved_counts,
        "pending_breakdown":  pending_counts,
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
                "table":        table,
                "id":           row["id"],
                "submitted_by": str(row["submitted_by"]),
                "submitted_at": row["submitted_at"].isoformat(),
                "status":       row["status"],
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

    labels  = []
    series  = {k: [] for k in [
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
    rows = await (
        DBService.get_leaderboard(period)
        if category == "reputation"
        else DBService.get_category_leaderboard(category, period)
    )
    # Enrich every row with display_name from staff_member in one query
    staff_rows = await DBService.fetch("SELECT discord_id, display_name FROM staff_member")
    names = {str(r["discord_id"]): (r["display_name"] or "") for r in staff_rows}
    return [dict(r) | {"display_name": names.get(str(r["discord_id"]), "")} for r in rows]

async def async_get_staff_directory():
    rows = await DBService.fetch(
        "SELECT discord_id, display_name, reputation FROM staff_member ORDER BY reputation DESC"
    )
    staff_map = {}
    for row in rows:
        sid = str(row["discord_id"])
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
                "table":        table,
                "id":           row["id"],
                "submitted_at": row["submitted_at"].isoformat(),
                "status":       row["status"],
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

# ── Flask routes ──────────────────────────────────────────────────────────────
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
        gran = request.args.get("granularity", "weekly")
        return jsonify(run_async(async_get_activity_timeseries(gran)))
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
    try:
        return jsonify(run_async(async_get_user_history(discord_id)))
    except Exception as e:
        logger.exception(f"User history error for {discord_id}")
        return jsonify({"error": str(e)}), 500

@app.route("/start",   methods=["POST"])
def start():
    ok, msg = start_bot()
    return jsonify({"success": ok, "message": msg})

@app.route("/stop",    methods=["POST"])
def stop():
    ok, msg = stop_bot()
    return jsonify({"success": ok, "message": msg})

@app.route("/restart", methods=["POST"])
def restart():
    ok, msg = restart_bot()
    return jsonify({"success": ok, "message": msg})

@app.route("/reset",   methods=["POST"])
def reset():
    ok, msg = reset_bot()
    return jsonify({"success": ok, "message": msg})

# ── WebSocket live logs ───────────────────────────────────────────────────────
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

# ── HTML Template ─────────────────────────────────────────────────────────────
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RECREP · Control</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=IBM+Plex+Mono:wght@300;400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
<style>
:root {
  --bg:        #060a12;
  --bg1:       #080d17;
  --bg2:       #0c1320;
  --bg3:       #101929;
  --bg4:       #141f32;
  --border:    rgba(255,255,255,0.06);
  --border2:   rgba(255,255,255,0.10);
  --teal:      #00e5c8;
  --teal-dim:  #00b89e;
  --teal-glow: rgba(0,229,200,0.15);
  --amber:     #ffb347;
  --amber-dim: rgba(255,179,71,0.15);
  --green:     #39e879;
  --green-dim: rgba(57,232,121,0.12);
  --red:       #ff4f6a;
  --red-dim:   rgba(255,79,106,0.12);
  --blue:      #5b8df8;
  --purple:    #a78bfa;
  --text:      #e8edf5;
  --text-2:    #7a8fa8;
  --text-3:    #3d5270;
  --font:      'Rajdhani', sans-serif;
  --mono:      'IBM Plex Mono', monospace;
  --r:         6px;
  --r2:        10px;
  --sidebar:   220px;
  --ease:      cubic-bezier(0.4, 0, 0.2, 1);
}

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; }

body {
  background: var(--bg);
  color: var(--text);
  font-family: var(--font);
  font-size: 14px;
  min-height: 100vh;
  display: flex;
  overflow: hidden;
  -webkit-font-smoothing: antialiased;
}

/* ── Noise texture overlay ── */
body::after {
  content: '';
  position: fixed;
  inset: 0;
  background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='1'/%3E%3C/svg%3E");
  opacity: 0.022;
  pointer-events: none;
  z-index: 9999;
}

/* ── Sidebar ── */
.sidebar {
  width: var(--sidebar);
  flex-shrink: 0;
  height: 100vh;
  background: var(--bg1);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  position: fixed;
  top: 0; left: 0;
  z-index: 100;
  padding: 0 0 20px;
}

.sb-logo {
  display: flex;
  align-items: center;
  gap: 11px;
  padding: 22px 18px 20px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 8px;
}

.sb-logo-mark {
  width: 32px; height: 32px;
  background: linear-gradient(135deg, var(--teal), var(--blue));
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 13px;
  font-weight: 700;
  color: #060a12;
  letter-spacing: -0.5px;
  flex-shrink: 0;
  box-shadow: 0 0 20px rgba(0,229,200,0.25);
}

.sb-logo-text {
  font-size: 17px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--text);
  line-height: 1;
}

.sb-logo-sub {
  font-size: 9px;
  color: var(--text-3);
  letter-spacing: 0.2em;
  text-transform: uppercase;
  font-family: var(--mono);
  margin-top: 3px;
}

/* ── Nav ── */
.sb-nav { flex: 1; padding: 4px 10px; }

.nav-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 10px;
  border-radius: var(--r);
  cursor: pointer;
  font-size: 13px;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--text-3);
  transition: color 0.15s var(--ease), background 0.15s var(--ease);
  user-select: none;
  margin-bottom: 2px;
  position: relative;
}

.nav-item:hover { color: var(--text-2); background: rgba(255,255,255,0.03); }

.nav-item.active {
  color: var(--teal);
  background: var(--teal-glow);
}

.nav-item.active::before {
  content: '';
  position: absolute;
  left: -10px; top: 50%;
  transform: translateY(-50%);
  width: 3px; height: 18px;
  background: var(--teal);
  border-radius: 0 2px 2px 0;
  box-shadow: 0 0 10px var(--teal);
}

.nav-icon {
  width: 16px;
  height: 16px;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  opacity: 0.7;
}
.nav-item.active .nav-icon { opacity: 1; }

/* ── Sidebar status ── */
.sb-status {
  padding: 14px 18px 12px;
  border-top: 1px solid var(--border);
  border-bottom: 1px solid var(--border);
  margin: 0 0 10px;
}

.status-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
}

.status-dot {
  width: 7px; height: 7px;
  border-radius: 50%;
  background: var(--red);
  box-shadow: 0 0 6px var(--red);
  flex-shrink: 0;
  transition: background 0.3s, box-shadow 0.3s;
}

.status-dot.on {
  background: var(--green);
  box-shadow: 0 0 10px var(--green);
  animation: pulse 2.5s ease infinite;
}

@keyframes pulse {
  0%, 100% { box-shadow: 0 0 6px var(--green); }
  50%       { box-shadow: 0 0 14px var(--green); }
}

.status-label {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text-2);
}

.status-pid {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-3);
}

.uptime-val {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--teal);
  display: block;
  margin-top: 2px;
}

/* ── Sidebar controls ── */
.sb-ctrl { padding: 0 10px; display: flex; flex-direction: column; gap: 6px; }

.sb-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 9px 12px;
  border-radius: var(--r);
  border: 1px solid var(--border);
  background: var(--bg2);
  color: var(--text-2);
  font-family: var(--font);
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all 0.15s var(--ease);
  width: 100%;
}

.sb-btn:hover { border-color: var(--border2); color: var(--text); background: var(--bg3); }

.sb-btn-teal  { border-color: rgba(0,229,200,0.25); color: var(--teal); background: var(--teal-glow); }
.sb-btn-teal:hover { border-color: var(--teal); box-shadow: 0 0 16px rgba(0,229,200,0.12); }

.sb-btn-red   { border-color: rgba(255,79,106,0.2); color: var(--red); background: var(--red-dim); }
.sb-btn-red:hover { border-color: var(--red); }

.sb-btn-amber { border-color: rgba(255,179,71,0.2); color: var(--amber); background: var(--amber-dim); }
.sb-btn-amber:hover { border-color: var(--amber); }

.ctrl-msg {
  padding: 0 10px;
  margin-top: 8px;
  font-family: var(--mono);
  font-size: 10px;
  color: var(--teal);
  min-height: 14px;
  display: flex;
  align-items: center;
  gap: 6px;
}

/* ── Main area ── */
.main {
  margin-left: var(--sidebar);
  flex: 1;
  height: 100vh;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
}

/* ── Top bar ── */
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 28px;
  border-bottom: 1px solid var(--border);
  background: var(--bg1);
  position: sticky;
  top: 0;
  z-index: 50;
  flex-shrink: 0;
}

.page-title {
  font-size: 20px;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.page-sub {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-3);
  letter-spacing: 0.12em;
  margin-top: 1px;
}

.topbar-right { display: flex; align-items: center; gap: 10px; }

.refresh-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 7px 14px;
  border-radius: 999px;
  border: 1px solid var(--border2);
  background: var(--bg2);
  color: var(--text-2);
  font-family: var(--font);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all 0.15s var(--ease);
}
.refresh-btn:hover { color: var(--teal); border-color: rgba(0,229,200,0.4); background: var(--teal-glow); }

/* ── Content ── */
.content { padding: 24px 28px 40px; flex: 1; }

/* ── Panels ── */
.panel { display: none; animation: fadeUp 0.2s var(--ease); }
.panel.active { display: block; }

@keyframes fadeUp {
  from { opacity: 0; transform: translateY(6px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ── Grid layouts ── */
.g2  { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; }
.g4  { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; }
.gap { margin-top: 16px; }

/* ── Card ── */
.card {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--r2);
  padding: 20px;
  position: relative;
  overflow: hidden;
  transition: border-color 0.15s var(--ease), box-shadow 0.15s var(--ease);
}

.card::after {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent 0%, rgba(0,229,200,0.3) 50%, transparent 100%);
  opacity: 0;
  transition: opacity 0.2s var(--ease);
}

.card:hover { border-color: var(--border2); }
.card:hover::after { opacity: 1; }

.card-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 16px;
}

.card-title {
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--text-3);
}

.card-sub { font-size: 11px; color: var(--text-3); margin-top: 3px; font-family: var(--mono); }

/* ── Stat cards ── */
.stat-num {
  font-size: 36px;
  font-weight: 700;
  color: var(--text);
  line-height: 1;
  letter-spacing: -0.01em;
  margin: 8px 0 10px;
  font-variant-numeric: tabular-nums;
  font-family: var(--mono);
}

.stat-bar {
  height: 2px;
  background: var(--border);
  border-radius: 2px;
  margin-bottom: 10px;
  overflow: hidden;
}

.stat-bar-fill {
  height: 100%;
  border-radius: 2px;
  background: var(--teal);
  transition: width 0.8s var(--ease);
  width: 0;
}

.stat-detail {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-3);
  line-height: 1.7;
}
.stat-detail b { color: var(--text-2); font-weight: 500; }

/* ── Stat accent colors ── */
.stat-teal  .stat-bar-fill { background: var(--teal); }
.stat-amber .stat-bar-fill { background: var(--amber); }
.stat-green .stat-bar-fill { background: var(--green); }
.stat-blue  .stat-bar-fill { background: var(--blue); }
.stat-teal  .stat-num { color: var(--teal); }
.stat-amber .stat-num { color: var(--amber); }
.stat-green .stat-num { color: var(--green); }
.stat-blue  .stat-num { color: var(--blue); }

/* ── Chart containers ── */
.chart-wrap { position: relative; }
.chart-wrap canvas { display: block; width: 100% !important; }

/* ── Gran buttons ── */
.gran-group { display: flex; gap: 4px; }

.gran-btn {
  padding: 4px 10px;
  border-radius: var(--r);
  border: 1px solid var(--border);
  background: transparent;
  color: var(--text-3);
  font-family: var(--mono);
  font-size: 10px;
  cursor: pointer;
  transition: all 0.12s var(--ease);
}
.gran-btn:hover  { color: var(--text-2); border-color: var(--border2); }
.gran-btn.active { color: var(--teal); border-color: rgba(0,229,200,0.4); background: var(--teal-glow); }

/* ── Filters row ── */
.filters { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 16px; }

.sel, .inp {
  background: var(--bg3);
  border: 1px solid var(--border);
  border-radius: var(--r);
  color: var(--text);
  font-family: var(--mono);
  font-size: 11px;
  padding: 7px 10px;
  outline: none;
  transition: border-color 0.12s var(--ease);
  height: 32px;
}
.sel:focus, .inp:focus { border-color: rgba(0,229,200,0.4); }
.inp::placeholder { color: var(--text-3); }
.sel option { background: var(--bg3); }
.inp { flex: 1; min-width: 160px; }

/* ── Activity list ── */
.act-list { list-style: none; }

.act-item {
  display: grid;
  grid-template-columns: 1fr 140px 80px;
  align-items: center;
  gap: 16px;
  padding: 11px 0;
  border-bottom: 1px solid var(--border);
}
.act-item:last-child { border-bottom: none; }
.act-name { font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; }
.act-meta { font-family: var(--mono); font-size: 10px; color: var(--text-3); margin-top: 3px; }
.act-user { font-family: var(--mono); font-size: 11px; color: var(--text-2); text-align: right; }

/* ── Badges ── */
.badge {
  display: inline-flex;
  align-items: center;
  padding: 3px 9px;
  border-radius: 999px;
  font-family: var(--mono);
  font-size: 9px;
  font-weight: 600;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  white-space: nowrap;
}
.badge-ok   { background: var(--green-dim);  border: 1px solid rgba(57,232,121,0.25); color: var(--green); }
.badge-wait { background: var(--amber-dim);  border: 1px solid rgba(255,179,71,0.25); color: var(--amber); }
.badge-no   { background: var(--red-dim);    border: 1px solid rgba(255,79,106,0.25); color: var(--red); }

/* ── Tables ── */
.tbl-wrap { overflow-x: auto; }

table { width: 100%; border-collapse: collapse; font-size: 13px; }

thead th {
  padding: 8px 12px;
  font-family: var(--mono);
  font-size: 9px;
  font-weight: 600;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--text-3);
  border-bottom: 1px solid var(--border);
  text-align: left;
  white-space: nowrap;
  background: var(--bg1);
}
thead th.r { text-align: right; }

tbody td {
  padding: 10px 12px;
  border-bottom: 1px solid rgba(255,255,255,0.04);
  white-space: nowrap;
  vertical-align: middle;
}
tbody td.r { text-align: right; font-family: var(--mono); font-size: 12px; }

tbody tr { cursor: pointer; transition: background 0.1s var(--ease); }
tbody tr:hover { background: rgba(0,229,200,0.03); }
tbody tr:last-child td { border-bottom: none; }

.name-cell .n-main { font-size: 13px; font-weight: 600; }
.name-cell .n-sub  { font-family: var(--mono); font-size: 10px; color: var(--text-3); margin-top: 2px; }

/* ── Rank medal ── */
.rank-cell {
  font-family: var(--mono);
  font-size: 12px;
  color: var(--text-3);
  text-align: right;
  width: 48px;
}
.rank-cell.gold   { color: #ffd700; text-shadow: 0 0 12px rgba(255,215,0,0.4); }
.rank-cell.silver { color: #c0c0c0; text-shadow: 0 0 12px rgba(192,192,192,0.4); }
.rank-cell.bronze { color: #cd7f32; text-shadow: 0 0 12px rgba(205,127,50,0.4); }

/* ── Role tags ── */
.role-tag {
  display: inline-flex;
  padding: 2px 8px;
  border-radius: 4px;
  background: var(--bg4);
  border: 1px solid var(--border2);
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-2);
  margin: 1px 2px 1px 0;
}

/* ── Log box ── */
.log-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

.log-actions { display: flex; gap: 8px; align-items: center; }

.log-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-3);
  cursor: pointer;
  user-select: none;
}

.log-toggle input[type=checkbox] { accent-color: var(--teal); }

.log-box {
  background: var(--bg1);
  border: 1px solid var(--border);
  border-radius: var(--r);
  padding: 12px 14px;
  height: 480px;
  overflow-y: auto;
  font-family: var(--mono);
  font-size: 11px;
  line-height: 1.8;
  position: relative;
}

.log-line {
  display: block;
  padding: 0 6px;
  border-left: 2px solid transparent;
  border-radius: 2px;
  color: var(--text-3);
  word-break: break-all;
  transition: background 0.1s var(--ease);
}
.log-line:hover { background: rgba(0,229,200,0.04); border-left-color: var(--teal); color: var(--text-2); }
.log-line.err  { color: var(--red); }
.log-line.warn { color: var(--amber); }
.log-line.info { color: var(--text-2); }

.log-count {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-3);
  padding: 6px 14px;
  border-top: 1px solid var(--border);
  text-align: right;
}

/* ── Modal ── */
.overlay {
  position: fixed;
  inset: 0;
  background: rgba(4,8,18,0.82);
  backdrop-filter: blur(6px);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal {
  background: var(--bg2);
  border: 1px solid var(--border2);
  border-radius: 14px;
  width: 90%;
  max-width: 700px;
  max-height: 82vh;
  display: flex;
  flex-direction: column;
  box-shadow: 0 40px 100px rgba(0,0,0,0.7), 0 0 0 1px rgba(0,229,200,0.06);
  animation: modalIn 0.2s var(--ease);
}

@keyframes modalIn {
  from { opacity: 0; transform: scale(0.96) translateY(8px); }
  to   { opacity: 1; transform: scale(1) translateY(0); }
}

.modal-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px 24px 16px;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}

.modal-title { font-size: 14px; font-weight: 700; letter-spacing: 0.04em; text-transform: uppercase; }
.modal-close {
  background: none; border: none;
  color: var(--text-3); font-size: 20px;
  cursor: pointer; line-height: 1;
  transition: color 0.12s var(--ease);
  padding: 0 4px;
}
.modal-close:hover { color: var(--text); }

.modal-body { padding: 18px 24px; overflow-y: auto; flex: 1; }

.count-row { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 16px; padding-bottom: 14px; border-bottom: 1px solid var(--border); }

.count-chip {
  padding: 4px 10px;
  background: var(--bg3);
  border: 1px solid var(--border);
  border-radius: 999px;
  font-family: var(--mono);
  font-size: 10px;
  color: var(--text-2);
}
.count-chip b { color: var(--teal); }

.hist-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  padding: 11px 0;
  border-bottom: 1px solid rgba(255,255,255,0.04);
}
.hist-item:last-child { border-bottom: none; }
.hist-name { font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; }
.hist-date { font-family: var(--mono); font-size: 10px; color: var(--text-3); margin-top: 3px; }
.hist-right { display: flex; align-items: center; gap: 10px; flex-shrink: 0; }

.view-btn {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--teal-dim);
  cursor: pointer;
  transition: color 0.12s var(--ease);
  text-decoration: underline;
  text-underline-offset: 2px;
}
.view-btn:hover { color: var(--teal); }

/* ── Spinner ── */
.spin {
  width: 14px; height: 14px;
  border: 2px solid var(--border2);
  border-top-color: var(--teal);
  border-radius: 50%;
  animation: spin 0.6s linear infinite;
  flex-shrink: 0;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--bg1); }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 999px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-3); }

/* ── Responsive ── */
@media (max-width: 960px) {
  .g4 { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 720px) {
  .sidebar { width: 180px; }
  .main { margin-left: 180px; }
  .g4, .g2 { grid-template-columns: 1fr; }
  .content { padding: 16px 16px 40px; }
  .topbar  { padding: 14px 16px; }
}
</style>
</head>
<body>

<!-- ── Sidebar ── -->
<aside class="sidebar">
  <div class="sb-logo">
    <div class="sb-logo-mark">RC</div>
    <div>
      <div class="sb-logo-text">Recrep</div>
      <div class="sb-logo-sub">Control Panel</div>
    </div>
  </div>

  <nav class="sb-nav">
    <div class="nav-item active" data-panel="overview">
      <span class="nav-icon">
        <svg width="15" height="15" viewBox="0 0 16 16" fill="none">
          <rect x="1" y="1" width="6" height="6" rx="1.5" stroke="currentColor" stroke-width="1.3"/>
          <rect x="9" y="1" width="6" height="6" rx="1.5" stroke="currentColor" stroke-width="1.3"/>
          <rect x="1" y="9" width="6" height="6" rx="1.5" stroke="currentColor" stroke-width="1.3"/>
          <rect x="9" y="9" width="6" height="6" rx="1.5" stroke="currentColor" stroke-width="1.3"/>
        </svg>
      </span>
      Overview
    </div>
    <div class="nav-item" data-panel="leaderboard">
      <span class="nav-icon">
        <svg width="15" height="15" viewBox="0 0 16 16" fill="none">
          <path d="M2 12h2.5V7H2v5zm4.75 3H9.25V4H6.75v11zM11.5 15H14V8h-2.5v7z" fill="currentColor"/>
        </svg>
      </span>
      Leaderboard
    </div>
    <div class="nav-item" data-panel="staff">
      <span class="nav-icon">
        <svg width="15" height="15" viewBox="0 0 16 16" fill="none">
          <circle cx="8" cy="5" r="3" stroke="currentColor" stroke-width="1.3"/>
          <path d="M2 14c0-3.314 2.686-6 6-6s6 2.686 6 6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
        </svg>
      </span>
      Staff
    </div>
    <div class="nav-item" data-panel="logs">
      <span class="nav-icon">
        <svg width="15" height="15" viewBox="0 0 16 16" fill="none">
          <rect x="2" y="2" width="12" height="12" rx="2" stroke="currentColor" stroke-width="1.3"/>
          <path d="M5 6h6M5 9h4" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
        </svg>
      </span>
      Live Logs
    </div>
  </nav>

  <div class="sb-status">
    <div class="status-row">
      <div id="statusDot" class="status-dot"></div>
      <span class="status-label" id="statusVal">Offline</span>
    </div>
    <span id="pidVal" class="status-pid"></span>
    <span id="uptimeVal" class="uptime-val"></span>
  </div>

  <div class="sb-ctrl">
    <button class="sb-btn sb-btn-teal" id="startBtn">
      <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor"><path d="M2 2l8 4-8 4z"/></svg>
      Start Bot
    </button>
    <button class="sb-btn" id="restartBtn">
      <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M10 6A4 4 0 1 1 6 2M6 2l2-2M6 2l2 2"/></svg>
      Restart
    </button>
    <button class="sb-btn sb-btn-red" id="stopBtn">
      <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor"><rect width="10" height="10" rx="1"/></svg>
      Stop Bot
    </button>
    <button class="sb-btn sb-btn-amber" id="resetBtn">
      <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><path d="M6 1v2M6 9v2M1 6h2M9 6h2M2.93 2.93l1.41 1.41M7.66 7.66l1.41 1.41M2.93 9.07l1.41-1.41M7.66 4.34l1.41-1.41"/></svg>
      Reset DB + S3
    </button>
  </div>
  <div id="ctrlMsg" class="ctrl-msg"></div>
</aside>

<!-- ── Main ── -->
<main class="main">
  <!-- Top bar -->
  <header class="topbar">
    <div>
      <div id="pageTitle" class="page-title">Overview</div>
      <div class="page-sub" id="pageSub">System dashboard · Auto-refresh every 15s</div>
    </div>
    <div class="topbar-right">
      <button class="refresh-btn" id="refreshBtn">
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M11 2v3H8M1 10V7h3"/><path d="M10.3 7a4.5 4.5 0 1 1-.8-3.5L11 5"/></svg>
        Refresh
      </button>
    </div>
  </header>

  <div class="content">

    <!-- ── OVERVIEW ── -->
    <div id="overview" class="panel active">

      <!-- Stats row -->
      <div class="g4">
        <div class="card stat-teal">
          <div class="card-title">Approved Forms</div>
          <div id="approvedNum" class="stat-num">—</div>
          <div class="stat-bar"><div id="approvedBar" class="stat-bar-fill"></div></div>
          <div id="approvedDetail" class="stat-detail">—</div>
        </div>
        <div class="card stat-amber">
          <div class="card-title">Pending Review</div>
          <div id="pendingNum" class="stat-num">—</div>
          <div class="stat-bar"><div id="pendingBar" class="stat-bar-fill" style="background:var(--amber);"></div></div>
          <div id="pendingDetail" class="stat-detail">—</div>
        </div>
        <div class="card stat-green">
          <div class="card-title">Total Reputation</div>
          <div id="repNum" class="stat-num">—</div>
          <div class="stat-bar"><div class="stat-bar-fill" style="background:var(--green);width:100%;"></div></div>
          <div class="stat-detail">Cumulative staff reputation pts</div>
        </div>
        <div class="card stat-blue">
          <div class="card-title">Staff Members</div>
          <div id="staffNum" class="stat-num">—</div>
          <div class="stat-bar"><div class="stat-bar-fill" style="background:var(--blue);width:100%;"></div></div>
          <div class="stat-detail">Active in the system</div>
        </div>
      </div>

      <!-- Charts row -->
      <div class="g2 gap">
        <div class="card">
          <div class="card-head">
            <div>
              <div class="card-title">Activity Over Time</div>
              <div class="card-sub">Forms approved + reputation per period</div>
            </div>
            <div class="gran-group">
              <button class="gran-btn" data-gran="daily">D</button>
              <button class="gran-btn active" data-gran="weekly">W</button>
              <button class="gran-btn" data-gran="monthly">M</button>
            </div>
          </div>
          <div class="chart-wrap" style="height:220px;"><canvas id="actChart"></canvas></div>
        </div>

        <div class="card">
          <div class="card-head">
            <div>
              <div class="card-title">Form Distribution</div>
              <div class="card-sub">Top contributors by category</div>
            </div>
            <select id="distSel" class="sel">
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
            <div id="distInner" style="min-height:100%;"><canvas id="distChart"></canvas></div>
          </div>
        </div>
      </div>

      <!-- Activity list -->
      <div class="card gap">
        <div class="card-head">
          <div>
            <div class="card-title">Recent Activity</div>
            <div class="card-sub">Latest form submissions across all categories</div>
          </div>
        </div>
        <ul id="actList" class="act-list"></ul>
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
        <div class="filters">
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
          <input id="lbSearch" class="inp" placeholder="Filter by name or ID…">
        </div>
        <div class="tbl-wrap">
          <table>
            <thead>
              <tr>
                <th class="r" style="width:50px;">Rank</th>
                <th>Name / ID</th>
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
            <div class="card-sub">Click any row to view form submission history</div>
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
        <div class="log-header">
          <div>
            <div class="card-title">Live Console</div>
            <div class="card-sub">Real-time output from the bot process</div>
          </div>
          <div class="log-actions">
            <label class="log-toggle">
              <input type="checkbox" id="autoScroll" checked>
              Auto-scroll
            </label>
            <button class="refresh-btn" id="clearLogsBtn">Clear</button>
          </div>
        </div>
        <div id="logBox" class="log-box"></div>
        <div class="log-count" id="logCount">0 lines</div>
      </div>
    </div>

  </div><!-- /content -->
</main>

<!-- ── Modal ── -->
<div id="histOverlay" class="overlay" style="display:none;">
  <div class="modal">
    <div class="modal-head">
      <div id="modalTitle" class="modal-title">User History</div>
      <button id="modalClose" class="modal-close">&times;</button>
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
const $   = id => document.getElementById(id);
const fmt = n  => (n ?? 0).toLocaleString('en-US');

const TABLE_LABELS = {
  recruitment: 'Rec', progress_report: 'Prog', purchase_invoice: 'Inv',
  demolition_report: 'Demo', eviction_report: 'Evict', scroll_completion: 'Scroll',
};

const COLORS = {
  recruitment: '#00e5c8', progress_report: '#ffb347', progress_help: '#a78bfa',
  purchase_invoice: '#39e879', demolition_report: '#ff4f6a', eviction_report: '#fb7185',
  scroll_completion: '#5b8df8', reputation: '#e8edf5',
};

// ── Animated counter ───────────────────────────────────────────────────────
function animateNum(el, target, duration = 600) {
  const start = parseInt(el.textContent.replace(/[^0-9]/g, '')) || 0;
  const diff  = target - start;
  if (diff === 0) return;
  const t0 = performance.now();
  function step(now) {
    const p = Math.min((now - t0) / duration, 1);
    const ease = 1 - Math.pow(1 - p, 3);
    el.textContent = fmt(Math.round(start + diff * ease));
    if (p < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

// ── State ──────────────────────────────────────────────────────────────────
let staffData      = [];
let leaderboardRows = [];
let nameMap        = {};
let currentGran    = 'weekly';
let actChart       = null;
let distChart      = null;
let logLineCount   = 0;

// ── API helper ─────────────────────────────────────────────────────────────
async function apiFetch(url) {
  try {
    const r = await fetch(url);
    return await r.json();
  } catch (e) {
    console.error(url, e);
    return null;
  }
}

// ── Status ─────────────────────────────────────────────────────────────────
async function loadStatus() {
  const d = await apiFetch('/api/status');
  if (!d) return;
  const dot = $('statusDot'), val = $('statusVal'), pid = $('pidVal'), up = $('uptimeVal');
  if (d.running) {
    dot.classList.add('on');
    val.textContent = 'Online';
    pid.textContent = `PID ${d.pid}`;
    up.textContent  = d.uptime || '';
  } else {
    dot.classList.remove('on');
    val.textContent = 'Offline';
    pid.textContent = '';
    up.textContent  = '';
  }
}

// ── Overview ───────────────────────────────────────────────────────────────
async function loadOverview() {
  const d = await apiFetch('/api/overview');
  if (!d || d.error) return;
  const t  = d.totals || {};
  const ab = d.approved_breakdown || {};
  const pb = d.pending_breakdown  || {};

  animateNum($('approvedNum'), t.approved_total || 0);
  animateNum($('pendingNum'),  t.pending_total  || 0);
  animateNum($('repNum'),      t.reputation_total || 0);
  animateNum($('staffNum'),    t.staff_total    || 0);

  // Animate stat bars
  const total = (t.approved_total || 0) + (t.pending_total || 0);
  const pct   = total > 0 ? Math.round((t.approved_total / total) * 100) : 0;
  setTimeout(() => {
    $('approvedBar').style.width = pct + '%';
    $('pendingBar').style.width  = (100 - pct) + '%';
  }, 100);

  const buildDetail = src =>
    Object.entries(TABLE_LABELS)
      .map(([k, l]) => `${l} <b>${fmt(src[k] || 0)}</b>`)
      .join(' &middot; ');

  $('approvedDetail').innerHTML = buildDetail(ab);
  $('pendingDetail').innerHTML  = buildDetail(pb);
}

// ── Activity list ──────────────────────────────────────────────────────────
async function loadActivity() {
  const data = await apiFetch('/api/activity');
  if (!data) return;
  const ul = $('actList');
  ul.innerHTML = '';
  if (!data.length) {
    ul.innerHTML = '<li style="color:var(--text-3);padding:12px 0;font-family:var(--mono);font-size:11px;">No recent activity.</li>';
    return;
  }
  for (const a of data) {
    const li  = document.createElement('li');
    li.className = 'act-item';
    const dt  = new Date(a.submitted_at);
    const sc  = a.status === 'approved' ? 'badge-ok' : a.status === 'pending' ? 'badge-wait' : 'badge-no';
    li.innerHTML = `
      <div>
        <div class="act-name">${(a.table || '').replace(/_/g, ' ')} #${a.id}</div>
        <div class="act-meta">${dt.toLocaleString()}</div>
      </div>
      <div class="act-user">${a.submitted_by}</div>
      <div style="text-align:right;"><span class="badge ${sc}">${a.status}</span></div>`;
    ul.appendChild(li);
  }
}

// ── Timeseries chart ───────────────────────────────────────────────────────
async function loadTimeseries(gran) {
  const d = await apiFetch(`/api/activity_timeseries?granularity=${gran}`);
  if (!d || d.error) return;
  const { labels, series } = d;

  const cfg = [
    { k: 'recruitment',       l: 'Recruitments' },
    { k: 'progress_report',   l: 'Progress'      },
    { k: 'progress_help',     l: 'Help'          },
    { k: 'purchase_invoice',  l: 'Invoices'      },
    { k: 'demolition_report', l: 'Demolitions'   },
    { k: 'eviction_report',   l: 'Evictions'     },
    { k: 'scroll_completion', l: 'Scrolls'       },
    { k: 'reputation',        l: 'Reputation'    },
  ];

  const datasets = cfg.map(c => ({
    label:           c.l,
    data:            (series[c.k] || []).map(x => x || 0),
    borderColor:     COLORS[c.k],
    backgroundColor: COLORS[c.k] + '18',
    tension:         0.4,
    fill:            c.k !== 'reputation',
    borderWidth:     c.k === 'reputation' ? 2 : 1.5,
    pointRadius:     2,
    pointHoverRadius: 5,
    pointBackgroundColor: COLORS[c.k],
  }));

  const sharedOpts = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: {
        labels: { color: '#7a8fa8', font: { size: 10, family: 'IBM Plex Mono' }, boxWidth: 10, padding: 12 },
      },
      tooltip: {
        backgroundColor: '#0c1320',
        borderColor: 'rgba(255,255,255,0.08)',
        borderWidth: 1,
        titleColor: '#e8edf5',
        bodyColor: '#7a8fa8',
        padding: 10,
        titleFont: { family: 'Rajdhani', size: 13, weight: '600' },
        bodyFont:  { family: 'IBM Plex Mono', size: 10 },
      },
    },
    scales: {
      x: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#3d5270', font: { size: 10 } } },
      y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#3d5270', font: { size: 10 } } },
    },
  };

  const ctx = $('actChart').getContext('2d');
  if (!actChart) {
    actChart = new Chart(ctx, { type: 'line', data: { labels, datasets }, options: sharedOpts });
  } else {
    actChart.data.labels   = labels;
    actChart.data.datasets = datasets;
    actChart.update();
  }
}

// ── Distribution chart ─────────────────────────────────────────────────────
function updateDistChart() {
  if (!staffData.length) return;
  const cat    = $('distSel').value;
  const sorted = [...staffData].sort((a, b) => (b[cat] || 0) - (a[cat] || 0)).slice(0, 14);
  const labels = sorted.map(s => s.label || `User ${s.discord_id}`);
  const values = sorted.map(s => s[cat] || 0);
  const colors = sorted.map((_, i) => `hsl(${175 + i * 14}, 65%, 52%)`);

  const inner = $('distInner');
  inner.style.height = Math.max(220, labels.length * 30) + 'px';

  const topts = {
    backgroundColor: '#0c1320',
    borderColor: 'rgba(255,255,255,0.08)',
    borderWidth: 1,
    titleColor: '#e8edf5',
    bodyColor: '#7a8fa8',
    padding: 10,
    bodyFont: { family: 'IBM Plex Mono', size: 10 },
  };

  const ctx = $('distChart').getContext('2d');
  if (!distChart) {
    distChart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [{ label: 'Count', data: values, backgroundColor: colors, borderRadius: 4, barPercentage: 0.72 }] },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: topts },
        scales: {
          x: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#3d5270', font: { size: 10 } } },
          y: { grid: { display: false }, ticks: { color: '#e8edf5', font: { size: 11 }, autoSkip: false, maxRotation: 0 } },
        },
      },
    });
  } else {
    distChart.data.labels                        = labels;
    distChart.data.datasets[0].data             = values;
    distChart.data.datasets[0].backgroundColor  = colors;
    distChart.update();
  }
}

// ── Staff directory ────────────────────────────────────────────────────────
async function loadStaff() {
  const d = await apiFetch('/api/staff');
  if (!d || d.error) return;
  staffData = d.staff || [];
  nameMap   = Object.fromEntries(staffData.map(s => [s.discord_id, s.label || `User ${s.discord_id}`]));
  renderStaff();
  updateDistChart();
}

function renderStaff() {
  const tbody = $('staffBody');
  tbody.innerHTML = '';
  if (!staffData.length) {
    tbody.innerHTML = '<tr><td colspan="11" style="color:var(--text-3);padding:14px 12px;font-family:var(--mono);font-size:11px;">No staff records.</td></tr>';
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
      <td class="r" style="color:var(--teal);">${fmt(s.reputation)}</td>
      <td class="r">${fmt(s.recruitment)}</td>
      <td class="r">${fmt(s.progress_report)}</td>
      <td class="r">${fmt(s.progress_help)}</td>
      <td class="r">${fmt(s.purchase_invoice)}</td>
      <td class="r">${fmt(s.demolition_report)}</td>
      <td class="r">${fmt(s.eviction_report)}</td>
      <td class="r">${fmt(s.scroll_completion)}</td>
      <td class="r">${fmt(s.approvals)}</td>
      <td>${(s.roles || []).map(r => `<span class="role-tag">${r}</span>`).join('')}</td>`;
    tr.addEventListener('click', () => openUserHistory(s.discord_id, s.label));
    tbody.appendChild(tr);
  }
}

// ── User history modal ─────────────────────────────────────────────────────
async function openUserHistory(discordId, label) {
  $('modalTitle').textContent = `History · ${label}`;
  $('modalBody').innerHTML    = `<div style="display:flex;align-items:center;gap:10px;color:var(--text-2);"><div class="spin"></div> Loading…</div>`;
  $('histOverlay').style.display = 'flex';

  const d = await apiFetch(`/api/user/${discordId}/history`);
  if (!d)       { $('modalBody').innerHTML = `<p style="color:var(--red);">Failed to load.</p>`; return; }
  if (d.error)  { $('modalBody').innerHTML = `<p style="color:var(--red);">Error: ${d.error}</p>`; return; }

  const { counts = {}, history = [] } = d;

  let html = `<div class="count-row">` +
    Object.entries(counts).map(([t, c]) =>
      `<span class="count-chip">${t.replace(/_/g, ' ')} <b>${c}</b></span>`
    ).join('') + `</div>`;

  if (!history.length) {
    html += `<p style="color:var(--text-3);font-family:var(--mono);font-size:11px;">No submitted forms found.</p>`;
  } else {
    for (const item of history) {
      const dt = new Date(item.submitted_at);
      const sc = item.status === 'approved' ? 'badge-ok' : item.status === 'pending' ? 'badge-wait' : 'badge-no';
      html += `<div class="hist-item">
        <div>
          <div class="hist-name">${item.table.replace(/_/g, ' ')} #${item.id}</div>
          <div class="hist-date">${dt.toLocaleString()}</div>
        </div>
        <div class="hist-right">
          <span class="badge ${sc}">${item.status}</span>
          <span class="view-btn" data-table="${item.table}" data-id="${item.id}">View →</span>
        </div>
      </div>`;
    }
  }

  $('modalBody').innerHTML = html;

  document.querySelectorAll('.view-btn').forEach(el => {
    el.addEventListener('click', async e => {
      e.stopPropagation();
      const r = await apiFetch(`/api/form/${el.dataset.table}/${el.dataset.id}`);
      if (r) alert(JSON.stringify(r, null, 2));
    });
  });
}

// ── Leaderboard ────────────────────────────────────────────────────────────
async function loadLeaderboard() {
  const cat    = $('lbCat').value;
  const period = $('lbPeriod').value;
  const d      = await apiFetch(`/api/leaderboard/${cat}/${period}`);
  if (!d) return;
  leaderboardRows = d;
  renderLeaderboard();
}

function renderLeaderboard() {
  const tbody = $('lbBody');
  tbody.innerHTML = '';
  if (!leaderboardRows.length) {
    tbody.innerHTML = '<tr><td colspan="3" style="color:var(--text-3);padding:14px 12px;font-family:var(--mono);font-size:11px;">No data available.</td></tr>';
    return;
  }
  const q = ($('lbSearch').value || '').toLowerCase();
  let rank = 1;
  for (const row of leaderboardRows) {
    const id    = String(row.discord_id || '');
    // Use display_name from API response first, then fall back to nameMap
    const label = row.display_name || nameMap[id] || `User ${id.slice(-6)}`;
    if (q && !(label + ' ' + id).toLowerCase().includes(q)) continue;
    const val   = row.points ?? row.count ?? 0;
    const rankCls = rank === 1 ? 'gold' : rank === 2 ? 'silver' : rank === 3 ? 'bronze' : '';
    const tr    = document.createElement('tr');
    tr.innerHTML = `
      <td class="rank-cell ${rankCls}">#${rank}</td>
      <td>
        <div class="name-cell">
          <div class="n-main">${label}</div>
          <div class="n-sub">${id}</div>
        </div>
      </td>
      <td class="r" style="color:var(--teal);font-size:14px;font-weight:600;">${fmt(val)}</td>`;
    tbody.appendChild(tr);
    rank++;
  }
  if (!tbody.children.length)
    tbody.innerHTML = '<tr><td colspan="3" style="color:var(--text-3);padding:14px 12px;font-family:var(--mono);font-size:11px;">No results match filter.</td></tr>';
}

// ── Bot actions ────────────────────────────────────────────────────────────
async function botAction(action) {
  const msg = $('ctrlMsg');
  msg.innerHTML = '<div class="spin"></div>';
  try {
    const r = await fetch(`/${action}`, { method: 'POST' });
    const d = await r.json();
    msg.innerHTML = '';
    msg.textContent = d.message || 'Done.';
    setTimeout(() => { msg.textContent = ''; }, 4000);
    if (['start', 'stop', 'restart'].includes(action)) {
      setTimeout(() => { loadStatus(); loadOverview(); loadActivity(); loadStaff(); }, 1500);
    } else if (action === 'reset') {
      setTimeout(() => location.reload(), 3000);
    }
  } catch (e) {
    msg.textContent = 'Error: ' + e.message;
  }
}

// ── WebSocket logs ─────────────────────────────────────────────────────────
const socket = io();
socket.on('log', d => {
  const box  = $('logBox');
  const el   = document.createElement('span');
  el.className = 'log-line';
  const txt  = d.line || '';
  if (/error|exception|traceback/i.test(txt)) el.classList.add('err');
  else if (/warn/i.test(txt))                  el.classList.add('warn');
  else if (/info/i.test(txt))                  el.classList.add('info');
  el.textContent = txt;
  box.appendChild(el);
  logLineCount++;
  $('logCount').textContent = `${logLineCount.toLocaleString()} lines`;
  if ($('autoScroll').checked) box.scrollTop = box.scrollHeight;
  if (box.children.length > 2000) box.removeChild(box.firstChild);
});

// ── Navigation ─────────────────────────────────────────────────────────────
const PAGE_TITLES = {
  overview:    ['Overview',    'System dashboard · Auto-refresh every 15s'],
  leaderboard: ['Leaderboard', 'Staff rankings by category and period'],
  staff:       ['Staff',       'Directory of all staff members'],
  logs:        ['Live Logs',   'Real-time bot process output via WebSocket'],
};

document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', () => {
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    item.classList.add('active');
    const panel = item.dataset.panel;
    $(panel).classList.add('active');
    const [title, sub] = PAGE_TITLES[panel] || ['', ''];
    $('pageTitle').textContent = title;
    $('pageSub').textContent   = sub;
    if (panel === 'staff')       setTimeout(() => { distChart?.resize(); distChart?.update(); }, 60);
    if (panel === 'leaderboard') loadLeaderboard();
  });
});

// ── Gran buttons ───────────────────────────────────────────────────────────
document.querySelectorAll('.gran-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.gran-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentGran = btn.dataset.gran;
    loadTimeseries(currentGran);
  });
});

// ── Event wiring ───────────────────────────────────────────────────────────
$('startBtn').onclick   = () => botAction('start');
$('restartBtn').onclick = () => botAction('restart');
$('stopBtn').onclick    = () => botAction('stop');
$('resetBtn').onclick   = () => botAction('reset');

$('refreshBtn').onclick = () => {
  loadStatus(); loadOverview(); loadActivity();
  loadStaff(); loadLeaderboard(); loadTimeseries(currentGran);
};

$('distSel').addEventListener('change', updateDistChart);
$('lbCat').addEventListener('change',   loadLeaderboard);
$('lbPeriod').addEventListener('change', loadLeaderboard);
$('lbSearch').addEventListener('input',  renderLeaderboard);

$('clearLogsBtn').onclick = () => { $('logBox').innerHTML = ''; logLineCount = 0; $('logCount').textContent = '0 lines'; };
$('modalClose').onclick   = () => { $('histOverlay').style.display = 'none'; };
$('histOverlay').addEventListener('click', e => { if (e.target === $('histOverlay')) $('histOverlay').style.display = 'none'; });

// ── Init ───────────────────────────────────────────────────────────────────
(async function init() {
  await loadStatus();
  await loadStaff();  // Load staff first so nameMap is populated before leaderboard renders
  await Promise.all([loadOverview(), loadActivity(), loadLeaderboard()]);
  await loadTimeseries(currentGran);
  setInterval(() => { loadStatus(); loadOverview(); loadActivity(); }, 15_000);
})();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.run(
        app,
        host  = "0.0.0.0",
        port  = int(os.environ.get("PORT", 5000)),
        debug = False,
    )