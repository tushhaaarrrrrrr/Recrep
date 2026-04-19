import os
import sys
import time
import asyncio
import threading
import subprocess
import psutil
from datetime import datetime, timezone

from flask import Flask, jsonify, request, render_template_string
from flask_socketio import SocketIO, emit

from services.db_service import DBService
from database.connection import init_db_pool, close_db_pool
from utils.logger import get_logger

BOT_SCRIPT  = "main.py"
PID_FILE    = "bot.pid"
LOG_FILE    = "bot.log"
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
        cmdline = proc.cmdline()
        if not cmdline:
            raise psutil.NoSuchProcess(pid)
        if len(cmdline) >= 2 and BOT_SCRIPT in cmdline[1]:
            return proc
        os.remove(PID_FILE)
        return None
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError, FileNotFoundError, ProcessLookupError):
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
        subprocess.run([VENV_PYTHON, "reset_db.py"], check=True)
        subprocess.run([VENV_PYTHON, "reset_s3.py"], check=True)
    except subprocess.CalledProcessError as e:
        return False, f"Reset failed: {e}"
    start_bot()
    return True, "Database and S3 reset, bot restarted."

# ── Data helpers ──────────────────────────────────────────────────────────────
FORM_TABLES = [
    "recruitment", "progress_report", "purchase_invoice",
    "demolition_report", "eviction_report", "scroll_completion",
]

# Discord IDs are 64-bit integers that exceed JS Number.MAX_SAFE_INTEGER.
# Always serialise them as strings so the browser never silently corrupts them.
_DISCORD_ID_FIELDS = {"submitted_by", "approved_by", "discord_id", "staff_id"}

def _serialize_row(row):
    d = {}
    for k in row.keys():
        v = row[k]
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
        elif v is None:
            d[k] = None
        elif k in _DISCORD_ID_FIELDS and isinstance(v, int):
            # Stringify so JS JSON.parse() doesn't lose precision
            d[k] = str(v)
        else:
            d[k] = v
    return d

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
            "approved_total":   total_approved,
            "pending_total":    total_pending,
            "reputation_total": r_rep[0] if r_rep else 0,
            "staff_total":      r_staff[0] if r_staff else 0,
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
                "submitted_by": str(row["submitted_by"]),   # always string
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
        bounds = lambda i: (f"CURRENT_DATE - INTERVAL '{i} day'", f"CURRENT_DATE - INTERVAL '{i-1} day'")
    elif granularity == "monthly":
        span, label_fn = 6, lambda i: "This month" if i == 0 else f"{i}mo ago"
        bounds = lambda i: (f"date_trunc('month',CURRENT_DATE) - INTERVAL '{i} month'", f"date_trunc('month',CURRENT_DATE) - INTERVAL '{i-1} month'")
    else:
        span, label_fn = 8, lambda i: "This week" if i == 0 else f"{i}w ago"
        bounds = lambda i: (f"date_trunc('week',CURRENT_DATE) - INTERVAL '{i} week'", f"date_trunc('week',CURRENT_DATE) - INTERVAL '{i-1} week'")

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
    rows = await (
        DBService.get_leaderboard(period)
        if category == "reputation"
        else DBService.get_category_leaderboard(category, period)
    )
    staff_rows = await DBService.fetch("SELECT discord_id, display_name FROM staff_member")
    names = {str(r["discord_id"]): (r["display_name"] or "") for r in staff_rows}
    return [dict(r) | {"display_name": names.get(str(r["discord_id"]), ""), "discord_id": str(r["discord_id"])} for r in rows]

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

async def async_get_monthly_records(year_month: str):
    try:
        parts = year_month.split('-')
        year, month = int(parts[0]), int(parts[1])
    except Exception:
        now = datetime.now(timezone.utc)
        year, month = now.year, now.month
    result = {}
    for table in FORM_TABLES:
        try:
            rows = await DBService.fetch(
                f"SELECT * FROM {table} "
                f"WHERE EXTRACT(YEAR FROM submitted_at) = $1 "
                f"AND EXTRACT(MONTH FROM submitted_at) = $2 "
                f"ORDER BY submitted_at DESC",
                year, month
            )
            result[table] = [_serialize_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Monthly records error for {table}: {e}")
            result[table] = []
    return result

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
        if table not in FORM_TABLES:
            return jsonify({"error": "Invalid table"}), 400
        row = run_async(DBService.fetchrow(f"SELECT * FROM {table} WHERE id = $1", form_id))
        return jsonify(_serialize_row(row) if row else None)
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

@app.route("/api/monthly_records")
def api_monthly_records():
    try:
        month = request.args.get("month", datetime.now(timezone.utc).strftime("%Y-%m"))
        return jsonify(run_async(async_get_monthly_records(month)))
    except Exception as e:
        logger.exception("Monthly records error")
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
<title>RECREP · OPS</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@300;400;500;600;700;800&family=Barlow:wght@300;400;500;600&family=JetBrains+Mono:wght@300;400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
<style>
/* ═══════════════════════════════════════
   TOKENS
═══════════════════════════════════════ */
:root {
  --bg:  #070708;
  --s1:  #0d0d0f;
  --s2:  #111113;
  --s3:  #161618;
  --s4:  #1d1d20;

  --b0: rgba(255,255,255,0.04);
  --b1: rgba(255,255,255,0.07);
  --b2: rgba(255,255,255,0.12);
  --b3: rgba(255,255,255,0.20);

  --a:   #CFFF47;
  --a-d: rgba(207,255,71,0.07);
  --a-g: rgba(207,255,71,0.15);

  --ok:   #4ade80;
  --ok-d: rgba(74,222,128,0.09);
  --no:   #f87171;
  --no-d: rgba(248,113,113,0.09);
  --warn: #fbbf24;
  --wd:   rgba(251,191,36,0.09);
  --sky:  #60a5fa;
  --skd:  rgba(96,165,250,0.09);
  --lav:  #a78bfa;
  --lvd:  rgba(167,139,250,0.09);
  --cor:  #fb923c;
  --cod:  rgba(251,146,60,0.09);
  --pnk:  #f472b6;
  --pkd:  rgba(244,114,182,0.09);

  --t1: #ededf0;
  --t2: #6b6b75;
  --t3: #333338;

  --disp: 'Barlow Condensed', sans-serif;
  --body: 'Barlow', sans-serif;
  --mono: 'JetBrains Mono', monospace;

  --sw: 208px;
  --topbar-h: 56px;
  --r: 3px;
  --r2: 6px;
}

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; font-size: 14px; }

body {
  background: var(--bg);
  color: var(--t1);
  font-family: var(--body);
  font-weight: 400;
  min-height: 100vh;
  display: flex;
  overflow: hidden;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

/* ═══════════════════════════════════════
   SIDEBAR
═══════════════════════════════════════ */
.sidebar {
  width: var(--sw);
  flex-shrink: 0;
  height: 100vh;
  display: flex;
  flex-direction: column;
  position: fixed;
  top: 0; left: 0; z-index: 200;
  border-right: 1px solid var(--b0);
  background: var(--s1);
}

.brand {
  padding: 24px 20px 22px;
  border-bottom: 1px solid var(--b0);
  display: flex;
  align-items: center;
  gap: 12px;
  flex-shrink: 0;
}
.brand-mark {
  width: 32px; height: 32px;
  background: var(--a);
  border-radius: 2px;
  display: flex; align-items: center; justify-content: center;
  font-family: var(--disp);
  font-weight: 800;
  font-size: 13px;
  letter-spacing: 0.05em;
  color: #070708;
  flex-shrink: 0;
}
.brand-name {
  font-family: var(--disp);
  font-weight: 800;
  font-size: 15px;
  letter-spacing: 0.14em;
  color: var(--t1);
  line-height: 1;
}
.brand-sub {
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 0.22em;
  color: var(--t3);
  margin-top: 4px;
  text-transform: uppercase;
}

.nav {
  flex: 1;
  padding: 12px 0;
  overflow-y: auto;
}
.nav-section {
  padding: 16px 20px 6px;
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 0.28em;
  color: var(--t3);
  text-transform: uppercase;
}
.nav-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 20px;
  border-left: 2px solid transparent;
  cursor: pointer;
  user-select: none;
  transition: all 140ms ease;
}
.nav-item:hover .nav-label { color: var(--t2); }
.nav-item.active {
  border-left-color: var(--a);
  background: var(--a-d);
}
.nav-item.active .nav-label { color: var(--a); }
.nav-item.active .nav-num   { color: var(--a); opacity: 0.6; }
.nav-num {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--t3);
  min-width: 18px;
  transition: color 140ms;
}
.nav-label {
  font-family: var(--disp);
  font-weight: 600;
  font-size: 13px;
  letter-spacing: 0.12em;
  color: var(--t3);
  text-transform: uppercase;
  transition: color 140ms;
}
.nav-badge {
  margin-left: auto;
  background: var(--no-d);
  border: 1px solid rgba(248,113,113,0.25);
  color: var(--no);
  font-family: var(--mono);
  font-size: 9px;
  padding: 1px 6px;
  border-radius: 999px;
}

.status-block {
  padding: 16px 20px;
  border-top: 1px solid var(--b0);
  border-bottom: 1px solid var(--b0);
  flex-shrink: 0;
}
.status-row { display: flex; align-items: center; gap: 9px; margin-bottom: 5px; }
.dot {
  width: 6px; height: 6px;
  border-radius: 50%;
  background: var(--no);
  flex-shrink: 0;
  transition: background 400ms, box-shadow 400ms;
}
.dot.on {
  background: var(--ok);
  box-shadow: 0 0 8px var(--ok);
  animation: pulse 2.6s ease infinite;
}
@keyframes pulse {
  0%,100% { box-shadow: 0 0 4px var(--ok); }
  50%      { box-shadow: 0 0 14px var(--ok), 0 0 28px rgba(74,222,128,0.3); }
}
.status-text {
  font-family: var(--mono);
  font-size: 11px;
  font-weight: 500;
  color: var(--t2);
}
.status-meta {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--t3);
  line-height: 1.8;
}
.status-uptime { color: var(--a); }

.controls { padding: 12px 12px 16px; flex-shrink: 0; }
.ctrl {
  display: block;
  width: 100%;
  padding: 8px 14px;
  margin-bottom: 5px;
  background: transparent;
  border: 1px solid var(--b1);
  border-radius: var(--r);
  color: var(--t2);
  font-family: var(--mono);
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all 140ms ease;
  text-align: left;
}
.ctrl:hover { background: var(--s3); border-color: var(--b2); color: var(--t1); }
.ctrl:disabled { opacity: 0.4; cursor: not-allowed; }
.ctrl-start  { color: var(--a); border-color: rgba(207,255,71,0.18); }
.ctrl-start:hover  { background: var(--a-d); border-color: rgba(207,255,71,0.35); }
.ctrl-stop   { color: var(--no); border-color: rgba(248,113,113,0.18); }
.ctrl-stop:hover   { background: var(--no-d); border-color: rgba(248,113,113,0.35); }
.ctrl-warn   { color: var(--warn); border-color: rgba(251,191,36,0.18); }
.ctrl-warn:hover   { background: var(--wd); border-color: rgba(251,191,36,0.35); }
.ctrl-msg {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--a);
  padding: 5px 2px 0;
  min-height: 16px;
  display: flex; align-items: center; gap: 6px;
}

/* ═══════════════════════════════════════
   MAIN
═══════════════════════════════════════ */
.main {
  margin-left: var(--sw);
  flex: 1;
  height: 100vh;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
}

.topbar {
  height: var(--topbar-h);
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 28px;
  border-bottom: 1px solid var(--b0);
  background: var(--s1);
  position: sticky; top: 0; z-index: 100;
  flex-shrink: 0;
  gap: 16px;
}
.topbar-left { display: flex; align-items: baseline; gap: 14px; }
.page-title {
  font-family: var(--disp);
  font-weight: 800;
  font-size: 20px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--t1);
  line-height: 1;
}
.page-sub {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--t3);
  letter-spacing: 0.1em;
}
.topbar-right { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
.top-btn {
  display: flex; align-items: center; gap: 6px;
  padding: 6px 14px;
  border-radius: var(--r);
  border: 1px solid var(--b1);
  background: transparent;
  color: var(--t2);
  font-family: var(--mono);
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all 140ms ease;
}
.top-btn:hover { background: var(--s3); border-color: var(--b2); color: var(--t1); }
.top-clock {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--t3);
  letter-spacing: 0.08em;
}

.workspace { padding: 24px 28px 48px; flex: 1; }
.view { display: none; animation: viewIn 0.2s ease; }
.view.active { display: block; }
@keyframes viewIn {
  from { opacity: 0; transform: translateY(6px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ═══════════════════════════════════════
   GRID
═══════════════════════════════════════ */
.g2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.g3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; }
.g4 { display: grid; grid-template-columns: repeat(4,1fr); gap: 14px; }
.mt  { margin-top: 14px; }
.mt2 { margin-top: 8px; }

/* ═══════════════════════════════════════
   SURFACES
═══════════════════════════════════════ */
.panel {
  background: var(--s2);
  border: 1px solid var(--b0);
  border-radius: var(--r);
  padding: 22px;
  position: relative;
}
.panel-hd {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 20px;
  gap: 12px;
}
.panel-kicker {
  font-family: var(--mono);
  font-size: 9px;
  font-weight: 500;
  letter-spacing: 0.24em;
  text-transform: uppercase;
  color: var(--t3);
  margin-bottom: 5px;
}
.panel-title {
  font-family: var(--disp);
  font-weight: 700;
  font-size: 15px;
  letter-spacing: 0.06em;
  color: var(--t1);
}
.panel-desc {
  font-family: var(--mono);
  font-size: 9px;
  color: var(--t3);
  margin-top: 3px;
  line-height: 1.6;
}

/* ═══════════════════════════════════════
   STAT CARDS
═══════════════════════════════════════ */
.stat {
  background: var(--s2);
  border: 1px solid var(--b0);
  border-radius: var(--r);
  padding: 22px 22px 18px;
  transition: border-color 160ms;
}
.stat:hover { border-color: var(--b1); }
.stat-kicker {
  font-family: var(--mono);
  font-size: 9px;
  font-weight: 500;
  letter-spacing: 0.24em;
  text-transform: uppercase;
  color: var(--t3);
  margin-bottom: 12px;
}
.stat-num {
  font-family: var(--disp);
  font-weight: 800;
  font-size: 52px;
  letter-spacing: -0.01em;
  line-height: 1;
  color: var(--t1);
  margin-bottom: 14px;
  font-variant-numeric: tabular-nums;
  transition: color 300ms;
}
.stat-track {
  height: 1px;
  background: var(--b1);
  margin-bottom: 12px;
  border-radius: 1px;
  overflow: hidden;
}
.stat-fill {
  height: 100%;
  border-radius: 1px;
  transition: width 1s cubic-bezier(0,0,0.2,1);
  width: 0;
}
.stat-detail {
  font-family: var(--mono);
  font-size: 9.5px;
  color: var(--t3);
  line-height: 1.7;
}
.stat-detail b { color: var(--t2); font-weight: 500; }

.v-a   .stat-num { color: var(--a);    }  .v-a   .stat-fill { background: var(--a);    }
.v-ok  .stat-num { color: var(--ok);   }  .v-ok  .stat-fill { background: var(--ok);   }
.v-sky .stat-num { color: var(--sky);  }  .v-sky .stat-fill { background: var(--sky);  }
.v-lav .stat-num { color: var(--lav);  }  .v-lav .stat-fill { background: var(--lav);  }
.v-no  .stat-num { color: var(--no);   }  .v-no  .stat-fill { background: var(--no);   }
.v-cor .stat-num { color: var(--cor);  }  .v-cor .stat-fill { background: var(--cor);  }
.v-warn .stat-num { color: var(--warn); } .v-warn .stat-fill { background: var(--warn); }
.v-pnk .stat-num { color: var(--pnk);  } .v-pnk .stat-fill { background: var(--pnk);  }

/* ═══════════════════════════════════════
   BADGES
═══════════════════════════════════════ */
.badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: 999px;
  font-family: var(--mono);
  font-size: 9px;
  font-weight: 600;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  white-space: nowrap;
}
.b-ok   { background: var(--ok-d);  border: 1px solid rgba(74,222,128,0.22);  color: var(--ok);   }
.b-wait { background: var(--wd);    border: 1px solid rgba(251,191,36,0.22);  color: var(--warn); }
.b-no   { background: var(--no-d);  border: 1px solid rgba(248,113,113,0.22); color: var(--no);   }

/* ═══════════════════════════════════════
   CHARTS
═══════════════════════════════════════ */
.chart-wrap { position: relative; }
.chart-wrap canvas { display: block; width: 100% !important; }

.gran-row { display: flex; gap: 3px; }
.gran-btn {
  padding: 4px 10px;
  border-radius: var(--r);
  border: 1px solid var(--b1);
  background: transparent;
  color: var(--t3);
  font-family: var(--mono);
  font-size: 9px;
  cursor: pointer;
  transition: all 120ms;
  letter-spacing: 0.08em;
}
.gran-btn:hover  { color: var(--t2); border-color: var(--b2); }
.gran-btn.on { color: var(--a); border-color: rgba(207,255,71,0.3); background: var(--a-d); }

/* ═══════════════════════════════════════
   ACTIVITY FEED
═══════════════════════════════════════ */
.feed { list-style: none; }
.feed-item {
  display: grid;
  grid-template-columns: 1fr 140px 90px;
  align-items: center;
  gap: 12px;
  padding: 12px 0;
  border-bottom: 1px solid var(--b0);
  transition: background 100ms;
}
.feed-item:last-child { border-bottom: none; }
.feed-item:hover { background: rgba(207,255,71,0.02); border-radius: var(--r); }
.feed-form {
  font-family: var(--disp);
  font-weight: 600;
  font-size: 12px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--t1);
}
.feed-time { font-family: var(--mono); font-size: 9px; color: var(--t3); margin-top: 3px; }
.feed-user { font-family: var(--mono); font-size: 10px; color: var(--t2); text-align: right; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

/* ═══════════════════════════════════════
   TABLES
═══════════════════════════════════════ */
.tbl-scroll { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
thead th {
  padding: 8px 14px;
  font-family: var(--mono);
  font-size: 8px;
  font-weight: 600;
  letter-spacing: 0.22em;
  text-transform: uppercase;
  color: var(--t3);
  border-bottom: 1px solid var(--b1);
  text-align: left;
  white-space: nowrap;
  background: var(--s1);
}
thead th.r { text-align: right; }
tbody td {
  padding: 12px 14px;
  border-bottom: 1px solid var(--b0);
  white-space: nowrap;
  vertical-align: middle;
  font-size: 13px;
}
tbody td.r { text-align: right; font-family: var(--mono); font-size: 11px; }
tbody tr { cursor: pointer; transition: background 120ms; }
tbody tr:hover { background: rgba(255,255,255,0.02); }
tbody tr:last-child td { border-bottom: none; }
.cell-name .n1 { font-weight: 600; color: var(--t1); font-size: 13px; }
.cell-name .n2 { font-family: var(--mono); font-size: 9px; color: var(--t3); margin-top: 2px; }
.rank-col { font-family: var(--mono); font-size: 11px; color: var(--t3); text-align: right; }
.rank-gold   { color: #fcd34d; }
.rank-silver { color: #94a3b8; }
.rank-bronze { color: #b87333; }
.role-tag {
  display: inline-flex;
  padding: 2px 7px;
  border-radius: 2px;
  background: var(--s4);
  border: 1px solid var(--b1);
  font-family: var(--mono);
  font-size: 9px;
  color: var(--t2);
  margin: 1px 2px 1px 0;
}

/* ═══════════════════════════════════════
   FILTERS
═══════════════════════════════════════ */
.filter-row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 16px; }
select, .inp {
  background: var(--s3);
  border: 1px solid var(--b1);
  border-radius: var(--r);
  color: var(--t1);
  font-family: var(--mono);
  font-size: 10px;
  padding: 7px 11px;
  outline: none;
  height: 32px;
  transition: border-color 120ms;
}
select:focus, .inp:focus { border-color: rgba(207,255,71,0.3); }
.inp::placeholder { color: var(--t3); }
.inp { flex: 1; min-width: 180px; }
select option { background: var(--s3); }

/* ═══════════════════════════════════════
   LEADERBOARD
═══════════════════════════════════════ */
.podium { display: flex; gap: 1px; margin-bottom: 1px; }
.podium-card {
  flex: 1;
  padding: 28px 20px 22px;
  background: var(--s3);
  border-radius: 0;
  position: relative;
  cursor: pointer;
  transition: background 160ms;
  overflow: hidden;
}
.podium-card:first-child { border-radius: var(--r) 0 0 0; }
.podium-card:last-child  { border-radius: 0 var(--r) 0 0; }
.podium-card:hover { background: var(--s4); }
.podium-place {
  font-family: var(--disp);
  font-weight: 800;
  font-size: 64px;
  line-height: 1;
  letter-spacing: -0.02em;
  opacity: 0.08;
  position: absolute;
  top: 16px; right: 16px;
  user-select: none;
}
.podium-card.p1 .podium-place { color: #fcd34d; opacity: 0.15; }
.podium-card.p2 .podium-place { color: #94a3b8; opacity: 0.1; }
.podium-card.p3 .podium-place { color: #b87333; opacity: 0.1; }
.podium-avatar {
  width: 38px; height: 38px;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-family: var(--disp);
  font-size: 13px; font-weight: 800;
  margin-bottom: 12px;
}
.podium-card.p1 .podium-avatar { background: rgba(252,211,77,0.12); border: 1px solid rgba(252,211,77,0.3); color: #fcd34d; }
.podium-card.p2 .podium-avatar { background: rgba(148,163,184,0.1); border: 1px solid rgba(148,163,184,0.2); color: #94a3b8; }
.podium-card.p3 .podium-avatar { background: rgba(184,115,51,0.1);  border: 1px solid rgba(184,115,51,0.2);  color: #b87333; }
.podium-name  { font-family: var(--disp); font-weight: 700; font-size: 16px; letter-spacing: 0.06em; color: var(--t1); margin-bottom: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.podium-score { font-family: var(--disp); font-weight: 800; font-size: 32px; line-height: 1; letter-spacing: -0.01em; color: var(--a); }
.podium-label { font-family: var(--mono); font-size: 9px; color: var(--t3); margin-top: 2px; }

/* ═══════════════════════════════════════
   ANALYTICS
═══════════════════════════════════════ */
.rate-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 0;
  border-bottom: 1px solid var(--b0);
}
.rate-row:last-child { border-bottom: none; }
.rate-label {
  font-family: var(--disp);
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--t2);
  width: 140px;
  flex-shrink: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.rate-track { flex: 1; height: 3px; background: var(--b1); border-radius: 2px; overflow: hidden; }
.rate-fill  { height: 100%; border-radius: 2px; transition: width 0.9s cubic-bezier(0,0,0.2,1); }
.rate-pct   { font-family: var(--mono); font-size: 10px; color: var(--t2); width: 34px; text-align: right; flex-shrink: 0; }

.heatmap-wrap { overflow-x: auto; margin-top: 4px; }
.hm-table { border-collapse: separate; border-spacing: 2px; width: 100%; }
.hm-table th {
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--t3);
  padding: 4px 6px;
  white-space: nowrap;
  font-weight: 500;
  background: none;
  border: none;
}
.hm-table td {
  border-radius: 2px;
  width: 52px;
  height: 28px;
  text-align: center;
  vertical-align: middle;
  font-family: var(--mono);
  font-size: 10px;
  font-weight: 600;
  cursor: default;
  transition: transform 100ms;
  border: none;
  white-space: nowrap;
  padding: 0;
}
.hm-table td:hover { transform: scale(1.12); position: relative; z-index: 2; }
.hm0 { background: rgba(255,255,255,0.02); color: var(--t3); }
.hm1 { background: rgba(207,255,71,0.07);  color: rgba(207,255,71,0.4); }
.hm2 { background: rgba(207,255,71,0.15);  color: rgba(207,255,71,0.65); }
.hm3 { background: rgba(207,255,71,0.26);  color: rgba(207,255,71,0.85); }
.hm4 { background: rgba(207,255,71,0.42);  color: #0c1a00; }
.hm5 { background: var(--a);               color: #0c1a00; }
.hm-name-cell {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--t2);
  white-space: nowrap;
  padding: 4px 12px 4px 0 !important;
  width: auto !important;
  text-align: left !important;
  background: none !important;
  border: none !important;
}

/* ═══════════════════════════════════════
   RECORDS
═══════════════════════════════════════ */
.month-bar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}
.month-nav {
  display: flex;
  border: 1px solid var(--b1);
  border-radius: var(--r);
  overflow: hidden;
}
.month-btn {
  background: none;
  border: none;
  padding: 7px 13px;
  color: var(--t3);
  cursor: pointer;
  font-size: 16px;
  line-height: 1;
  transition: color 120ms, background 120ms;
  display: flex; align-items: center;
}
.month-btn:hover { color: var(--a); background: var(--a-d); }
.month-label {
  padding: 7px 16px;
  font-family: var(--disp);
  font-weight: 700;
  font-size: 14px;
  letter-spacing: 0.1em;
  color: var(--t1);
  border-left: 1px solid var(--b1);
  border-right: 1px solid var(--b1);
  background: var(--s2);
  min-width: 136px;
  text-align: center;
  text-transform: uppercase;
}
.month-chips { display: flex; gap: 6px; flex-wrap: wrap; margin-left: auto; }
.chip {
  padding: 4px 11px;
  border: 1px solid var(--b1);
  border-radius: 999px;
  font-family: var(--mono);
  font-size: 9px;
  color: var(--t3);
  background: var(--s2);
}
.chip b { font-weight: 600; color: var(--t2); }
.chip.c-ok   b { color: var(--ok); }
.chip.c-warn b { color: var(--warn); }

.type-tabs { display: flex; gap: 5px; flex-wrap: wrap; margin-bottom: 14px; }
.type-tab {
  padding: 6px 14px;
  border-radius: 999px;
  border: 1px solid var(--b1);
  background: var(--s2);
  color: var(--t3);
  font-family: var(--body);
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
  transition: all 140ms;
  display: flex; align-items: center; gap: 7px;
}
.type-tab:hover { color: var(--t2); border-color: var(--b2); }
.type-tab.on { color: var(--a); background: var(--a-d); border-color: rgba(207,255,71,0.25); }
.tab-ct {
  font-family: var(--mono);
  font-size: 9px;
  padding: 1px 6px;
  border-radius: 999px;
  background: var(--s3);
  border: 1px solid var(--b0);
  color: var(--t3);
}
.type-tab.on .tab-ct { background: rgba(207,255,71,0.1); border-color: rgba(207,255,71,0.2); color: var(--a); }

.rec-entry { border-bottom: 1px solid var(--b0); }
.rec-entry:last-child { border-bottom: none; }
.rec-row {
  display: grid;
  grid-template-columns: 56px 1fr 200px 28px;
  align-items: center;
  gap: 14px;
  padding: 13px 4px;
  cursor: pointer;
  transition: background 100ms;
  border-radius: var(--r);
}
.rec-row:hover { background: rgba(255,255,255,0.02); }
.rec-id   { font-family: var(--mono); font-size: 11px; color: var(--t3); }
.rec-who  { font-size: 13px; font-weight: 600; color: var(--t1); }
.rec-sub  { font-family: var(--mono); font-size: 9px; color: var(--t3); margin-top: 2px; }
.rec-meta { text-align: right; }
.rec-date { font-family: var(--mono); font-size: 9.5px; color: var(--t3); margin-top: 4px; }
.rec-approver { font-family: var(--mono); font-size: 8.5px; color: var(--t3); margin-top: 3px; }
.rec-approver span { color: var(--ok); }
.rec-chevron {
  color: var(--t3);
  transition: transform 0.2s ease, color 0.15s;
  display: flex; align-items: center; justify-content: center;
}
.rec-entry.open .rec-chevron { transform: rotate(180deg); color: var(--a); }
.rec-body {
  max-height: 0;
  overflow: hidden;
  transition: max-height 0.28s ease;
  padding: 0 4px;
}
.rec-entry.open .rec-body { max-height: 900px; padding: 0 4px 18px; }

.fields-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 8px;
  margin-bottom: 12px;
}
.field {
  background: var(--s3);
  border: 1px solid var(--b0);
  border-radius: var(--r);
  padding: 10px 13px;
}
.field-lbl {
  font-family: var(--mono);
  font-size: 8px;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--t3);
  margin-bottom: 5px;
}
.field-val { font-size: 13px; color: var(--t1); font-weight: 500; word-break: break-word; }
.field-val.lg { font-size: 11px; font-family: var(--mono); }

.ss-grid { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
.ss-hd {
  font-family: var(--mono); font-size: 8.5px; letter-spacing: 0.2em;
  text-transform: uppercase; color: var(--t3); margin-bottom: 10px;
  display: flex; align-items: center; gap: 10px;
}
.ss-hd::after { content: ''; flex: 1; height: 1px; background: var(--b0); }
.ss-item {
  position: relative; cursor: pointer;
  border-radius: var(--r); overflow: hidden;
  border: 1px solid var(--b1);
  transition: all 160ms;
  background: var(--s3);
}
.ss-item:hover { border-color: rgba(207,255,71,0.3); transform: translateY(-2px); }
.ss-img { display: block; width: 150px; height: 100px; object-fit: cover; }
.ss-cap { font-family: var(--mono); font-size: 8.5px; color: var(--t3); padding: 5px 8px; }

/* ═══════════════════════════════════════
   STAFF
═══════════════════════════════════════ */
.staff-header { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; }
.view-tog { display: flex; border: 1px solid var(--b1); border-radius: var(--r); overflow: hidden; }
.view-btn {
  background: transparent;
  border: none;
  color: var(--t3);
  padding: 7px 12px;
  cursor: pointer;
  font-family: var(--mono);
  font-size: 10px;
  transition: all 120ms;
  border-right: 1px solid var(--b1);
  letter-spacing: 0.08em;
}
.view-btn:last-child { border-right: none; }
.view-btn:hover  { color: var(--t2); background: var(--s3); }
.view-btn.on { color: var(--a); background: var(--a-d); }
.staff-search {
  background: var(--s3); border: 1px solid var(--b1); border-radius: var(--r);
  color: var(--t1); font-family: var(--mono); font-size: 10px; padding: 7px 12px;
  outline: none; height: 32px; width: 220px; transition: border-color 120ms;
}
.staff-search:focus { border-color: rgba(207,255,71,0.3); }
.staff-search::placeholder { color: var(--t3); }

.staff-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(188px, 1fr)); gap: 10px; }
.scard {
  background: var(--s2);
  border: 1px solid var(--b0);
  border-radius: var(--r);
  padding: 18px 16px;
  cursor: pointer;
  transition: all 200ms ease;
  position: relative; overflow: hidden;
}
.scard::after {
  content: ''; position: absolute; top: 0; left: 0; right: 0; height: 1px;
  background: linear-gradient(90deg, transparent, rgba(207,255,71,0.2), transparent);
  opacity: 0; transition: opacity 200ms;
}
.scard:hover { border-color: var(--b1); transform: translateY(-2px); box-shadow: 0 8px 32px rgba(0,0,0,0.5); }
.scard:hover::after { opacity: 1; }
.scard-avatar {
  width: 40px; height: 40px;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-family: var(--disp);
  font-size: 13px; font-weight: 800;
  margin-bottom: 11px;
}
.scard-name { font-family: var(--body); font-size: 13px; font-weight: 700; color: var(--t1); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-bottom: 1px; }
.scard-id   { font-family: var(--mono); font-size: 9px; color: var(--t3); margin-bottom: 9px; }
.scard-rep  { font-family: var(--disp); font-weight: 800; font-size: 26px; color: var(--a); line-height: 1; margin-bottom: 2px; }
.scard-rep-lbl { font-family: var(--mono); font-size: 8px; letter-spacing: 0.16em; text-transform: uppercase; color: var(--t3); margin-bottom: 9px; }
.scard-stats { display: flex; flex-wrap: wrap; gap: 3px; }
.scard-chip {
  font-family: var(--mono); font-size: 9px;
  padding: 2px 7px; border-radius: 2px;
  background: var(--s3); border: 1px solid var(--b0); color: var(--t3);
}
.scard-chip b { color: var(--t2); font-weight: 500; }

/* ═══════════════════════════════════════
   TERMINAL
═══════════════════════════════════════ */
.term-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 14px; flex-wrap: wrap; gap: 10px; }
.term-controls { display: flex; gap: 7px; align-items: center; flex-wrap: wrap; }
.term-input {
  background: var(--s3); border: 1px solid var(--b1); border-radius: var(--r);
  color: var(--t1); font-family: var(--mono); font-size: 10px;
  padding: 6px 10px; outline: none; height: 30px; transition: border-color 120ms;
}
.term-input:focus { border-color: rgba(207,255,71,0.3); }
.term-input::placeholder { color: var(--t3); }
.term-select {
  background: var(--s3); border: 1px solid var(--b1); border-radius: var(--r);
  color: var(--t1); font-family: var(--mono); font-size: 10px;
  padding: 6px 10px; outline: none; height: 30px;
}
.term-scroll-lbl { display: flex; align-items: center; gap: 5px; font-family: var(--mono); font-size: 9px; color: var(--t3); cursor: pointer; user-select: none; }
.term-scroll-lbl input { accent-color: var(--a); }

.term-box {
  background: #000000;
  border: 1px solid var(--b0);
  border-radius: var(--r);
  padding: 16px;
  height: 520px;
  overflow-y: auto;
  font-family: var(--mono);
  font-size: 11px;
  line-height: 1.9;
}
.log-ln {
  display: block;
  padding: 0 8px;
  border-left: 2px solid transparent;
  border-radius: 1px;
  color: #444450;
  word-break: break-all;
  transition: background 100ms;
}
.log-ln:hover { background: rgba(255,255,255,0.03); color: #787882; }
.log-ln.e { color: var(--no); border-left-color: rgba(248,113,113,0.3); }
.log-ln.w { color: var(--warn); }
.log-ln.i { color: #6b7fa8; }
.log-ln.gone { display: none; }
.term-foot { font-family: var(--mono); font-size: 9px; color: var(--t3); padding: 7px 0 0; text-align: right; }

/* ═══════════════════════════════════════
   MODAL
═══════════════════════════════════════ */
.overlay {
  position: fixed; inset: 0;
  background: rgba(4,4,6,0.88);
  backdrop-filter: blur(12px);
  display: flex; align-items: center; justify-content: center;
  z-index: 500;
}
.modal {
  background: var(--s2);
  border: 1px solid var(--b2);
  border-radius: var(--r2);
  width: 92%;
  max-width: 740px;
  max-height: 88vh;
  display: flex;
  flex-direction: column;
  box-shadow: 0 40px 80px rgba(0,0,0,0.8);
  animation: modalIn 0.18s ease;
}
@keyframes modalIn { from { opacity: 0; transform: scale(0.97) translateY(8px); } to { opacity: 1; transform: none; } }
.modal-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px 24px 0;
  flex-shrink: 0;
}
.modal-title { font-family: var(--disp); font-size: 16px; font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase; }
.modal-x { background: none; border: none; color: var(--t3); font-size: 20px; cursor: pointer; transition: color 120ms; line-height: 1; padding: 2px 4px; }
.modal-x:hover { color: var(--t1); }
.modal-tabs { display: flex; padding: 16px 24px 0; border-bottom: 1px solid var(--b0); flex-shrink: 0; gap: 2px; }
.mtab {
  padding: 8px 14px;
  font-family: var(--mono);
  font-size: 9.5px;
  font-weight: 500;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--t3);
  cursor: pointer;
  border-bottom: 2px solid transparent;
  margin-bottom: -1px;
  transition: all 140ms;
}
.mtab:hover { color: var(--t2); }
.mtab.on { color: var(--a); border-bottom-color: var(--a); }
.modal-body { padding: 20px 24px; overflow-y: auto; flex: 1; }
.mtab-panel { display: none; }
.mtab-panel.on { display: block; animation: viewIn 0.16s ease; }

.modal-user-row {
  display: flex; align-items: center; gap: 16px;
  margin-bottom: 18px; padding-bottom: 16px;
  border-bottom: 1px solid var(--b0);
}
.modal-avatar { width: 50px; height: 50px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-family: var(--disp); font-size: 16px; font-weight: 800; flex-shrink: 0; }
.modal-uname  { font-family: var(--disp); font-weight: 700; font-size: 17px; letter-spacing: 0.05em; }
.modal-uid    { font-family: var(--mono); font-size: 10px; color: var(--t3); margin-top: 3px; }
.modal-roles  { display: flex; gap: 4px; flex-wrap: wrap; margin-top: 7px; }
.modal-counts { display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 14px; padding-bottom: 14px; border-bottom: 1px solid var(--b0); }
.modal-chip   { padding: 4px 10px; background: var(--s3); border: 1px solid var(--b0); border-radius: 999px; font-family: var(--mono); font-size: 9.5px; color: var(--t2); }
.modal-chip b { color: var(--a); }

.hist-row { display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 10px 0; border-bottom: 1px solid var(--b0); }
.hist-row:last-child { border-bottom: none; }
.hist-form { font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--t1); }
.hist-ts   { font-family: var(--mono); font-size: 9px; color: var(--t3); margin-top: 2px; }
.hist-right { display: flex; align-items: center; gap: 9px; flex-shrink: 0; }
.hist-link { font-family: var(--mono); font-size: 9.5px; color: var(--t3); cursor: pointer; text-decoration: underline; text-underline-offset: 2px; transition: color 120ms; }
.hist-link:hover { color: var(--a); }
.hist-detail { margin-top: 8px; padding: 12px; background: var(--s3); border-radius: var(--r); border: 1px solid var(--b0); display: none; }
.hist-detail.on { display: block; animation: viewIn 0.14s ease; }

/* ═══════════════════════════════════════
   LIGHTBOX
═══════════════════════════════════════ */
.lightbox { position: fixed; inset: 0; background: rgba(3,3,5,0.96); backdrop-filter: blur(16px); display: none; align-items: center; justify-content: center; z-index: 600; }
.lightbox.on { display: flex; animation: viewIn 0.16s ease; }
.lightbox-inner { position: relative; max-width: 90vw; max-height: 86vh; }
.lb-img { max-width: 100%; max-height: 86vh; object-fit: contain; border-radius: var(--r2); display: block; box-shadow: 0 40px 80px rgba(0,0,0,0.9); }
.lb-close { position: absolute; top: -14px; right: -14px; width: 30px; height: 30px; border-radius: 50%; background: var(--s4); border: 1px solid var(--b2); color: var(--t2); cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 14px; transition: all 120ms; }
.lb-close:hover { color: var(--no); border-color: var(--no); }

/* ═══════════════════════════════════════
   UTILITIES
═══════════════════════════════════════ */
.spinner { width: 13px; height: 13px; border: 1.5px solid var(--b2); border-top-color: var(--a); border-radius: 50%; animation: spin 0.6s linear infinite; flex-shrink: 0; }
@keyframes spin { to { transform: rotate(360deg); } }
.loading { display: flex; align-items: center; gap: 10px; color: var(--t2); padding: 24px 0; font-family: var(--mono); font-size: 10px; justify-content: center; }
.empty { text-align: center; padding: 32px 0; color: var(--t3); font-family: var(--mono); font-size: 11px; }

::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--b2); border-radius: 999px; }
::-webkit-scrollbar-thumb:hover { background: var(--t3); }

@media (max-width: 900px) {
  .g4 { grid-template-columns: 1fr 1fr; }
  .g3 { grid-template-columns: 1fr 1fr; }
}
@media (max-width: 640px) {
  :root { --sw: 0px; }
  .sidebar { display: none; }
  .main { margin-left: 0; }
  .g4, .g2, .g3 { grid-template-columns: 1fr; }
  .workspace { padding: 16px 16px 40px; }
  .topbar { padding: 0 16px; }
}
</style>
</head>
<body>

<!-- ═══ SIDEBAR ═══ -->
<aside class="sidebar">
  <div class="brand">
    <div class="brand-mark">RC</div>
    <div class="brand-text">
      <div class="brand-name">RECREP</div>
      <div class="brand-sub">Operations Centre</div>
    </div>
  </div>

  <nav class="nav">
    <div class="nav-section">Dashboard</div>
    <div class="nav-item active" data-view="pulse">
      <span class="nav-num">01</span>
      <span class="nav-label">Pulse</span>
    </div>
    <div class="nav-item" data-view="signal">
      <span class="nav-num">02</span>
      <span class="nav-label">Signal</span>
    </div>
    <div class="nav-item" data-view="archive">
      <span class="nav-num">03</span>
      <span class="nav-label">Archive</span>
      <span class="nav-badge" id="pendingBadge" style="display:none">0</span>
    </div>

    <div class="nav-section">People</div>
    <div class="nav-item" data-view="rank">
      <span class="nav-num">04</span>
      <span class="nav-label">Rank</span>
    </div>
    <div class="nav-item" data-view="roster">
      <span class="nav-num">05</span>
      <span class="nav-label">Roster</span>
    </div>

    <div class="nav-section">System</div>
    <div class="nav-item" data-view="terminal">
      <span class="nav-num">06</span>
      <span class="nav-label">Terminal</span>
    </div>
  </nav>

  <div class="status-block">
    <div class="status-row">
      <div id="dot" class="dot"></div>
      <span id="statusTxt" class="status-text">Offline</span>
    </div>
    <div id="statusMeta" class="status-meta"></div>
  </div>

  <div class="controls">
    <button class="ctrl ctrl-start" id="btnStart">&#9654; Start</button>
    <button class="ctrl" id="btnRestart">&#8635; Restart</button>
    <button class="ctrl ctrl-stop" id="btnStop">&#9632; Stop</button>
    <button class="ctrl ctrl-warn" id="btnReset">&#9711; Reset DB + S3</button>
    <div id="ctrlMsg" class="ctrl-msg"></div>
  </div>
</aside>

<!-- ═══ MAIN ═══ -->
<main class="main">
  <header class="topbar">
    <div class="topbar-left">
      <div id="pageTitle" class="page-title">Pulse</div>
      <div id="pageSub" class="page-sub">Real-time overview &middot; refreshes every 15s</div>
    </div>
    <div class="topbar-right">
      <span id="clock" class="top-clock"></span>
      <button class="top-btn" id="refreshBtn">
        <svg width="10" height="10" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><path d="M11 2v3H8M1 10V7h3"/><path d="M10.3 7a4.5 4.5 0 1 1-.8-3.5L11 5"/></svg>
        Refresh
      </button>
    </div>
  </header>

  <div class="workspace">

    <!-- ═══ PULSE ═══ -->
    <div id="pulse" class="view active">
      <div class="g4 mt2">
        <div class="stat v-a">
          <div class="stat-kicker">Approved Forms</div>
          <div id="sApproved" class="stat-num">—</div>
          <div class="stat-track"><div id="sApprovedBar" class="stat-fill"></div></div>
          <div id="sApprovedDetail" class="stat-detail">—</div>
        </div>
        <div class="stat v-warn">
          <div class="stat-kicker">Pending Review</div>
          <div id="sPending" class="stat-num">—</div>
          <div class="stat-track"><div id="sPendingBar" class="stat-fill"></div></div>
          <div id="sPendingDetail" class="stat-detail">—</div>
        </div>
        <div class="stat v-ok">
          <div class="stat-kicker">Total Reputation</div>
          <div id="sRep" class="stat-num">—</div>
          <div class="stat-track"><div class="stat-fill" style="width:100%"></div></div>
          <div class="stat-detail">Cumulative staff score</div>
        </div>
        <div class="stat v-sky">
          <div class="stat-kicker">Active Staff</div>
          <div id="sStaff" class="stat-num">—</div>
          <div class="stat-track"><div class="stat-fill" style="width:100%"></div></div>
          <div class="stat-detail">Registered members</div>
        </div>
      </div>

      <div class="g2 mt">
        <div class="panel">
          <div class="panel-hd">
            <div>
              <div class="panel-kicker">Throughput Over Time</div>
              <div class="panel-title">Submission Trends</div>
              <div class="panel-desc">Approved submissions across all form types</div>
            </div>
            <div class="gran-row">
              <button class="gran-btn" data-gran="daily">D</button>
              <button class="gran-btn on"  data-gran="weekly">W</button>
              <button class="gran-btn" data-gran="monthly">M</button>
            </div>
          </div>
          <div class="chart-wrap" style="height:200px;"><canvas id="trendChart"></canvas></div>
        </div>

        <div class="panel">
          <div class="panel-hd">
            <div>
              <div class="panel-kicker">Volume by Contributor</div>
              <div class="panel-title">Distribution</div>
            </div>
            <select id="distSel" style="width:140px;">
              <option value="recruitment">Recruitment</option>
              <option value="progress_report">Progress</option>
              <option value="progress_help">Help</option>
              <option value="purchase_invoice">Invoice</option>
              <option value="demolition_report">Demolition</option>
              <option value="eviction_report">Eviction</option>
              <option value="scroll_completion">Scroll</option>
            </select>
          </div>
          <div style="height:200px; overflow-y:auto;">
            <div id="distInner" style="min-height:100%;"><canvas id="distChart"></canvas></div>
          </div>
        </div>
      </div>

      <div class="panel mt">
        <div class="panel-hd">
          <div>
            <div class="panel-kicker">Live Activity Feed</div>
            <div class="panel-title">Recent Submissions</div>
          </div>
        </div>
        <ul id="feedList" class="feed"></ul>
      </div>
    </div>

    <!-- ═══ SIGNAL ═══ -->
    <div id="signal" class="view">
      <div class="g3 mt2">
        <div class="panel">
          <div class="panel-kicker">Overall</div>
          <div class="panel-title" style="margin-bottom:10px;">Approval Rate</div>
          <div id="anlRate" style="font-family:var(--disp);font-weight:800;font-size:52px;letter-spacing:-0.01em;color:var(--a);line-height:1;">—%</div>
          <div id="anlRateSub" style="font-family:var(--mono);font-size:9.5px;color:var(--t3);margin-top:8px;">—</div>
        </div>
        <div class="panel">
          <div class="panel-kicker">Leading Category</div>
          <div class="panel-title" style="margin-bottom:10px;">Top Form Type</div>
          <div id="anlTopForm" style="font-family:var(--disp);font-weight:800;font-size:26px;letter-spacing:0.04em;text-transform:uppercase;color:var(--sky);line-height:1;margin-top:4px;">—</div>
          <div id="anlTopSub" style="font-family:var(--mono);font-size:9.5px;color:var(--t3);margin-top:10px;">—</div>
        </div>
        <div class="panel">
          <div class="panel-kicker">Cumulative</div>
          <div class="panel-title" style="margin-bottom:10px;">Total Approved</div>
          <div id="anlTotal" style="font-family:var(--disp);font-weight:800;font-size:52px;letter-spacing:-0.01em;color:var(--ok);line-height:1;">—</div>
          <div style="font-family:var(--mono);font-size:9.5px;color:var(--t3);margin-top:8px;">Forms approved by all staff</div>
        </div>
      </div>

      <div class="g2 mt">
        <div class="panel">
          <div class="panel-hd">
            <div>
              <div class="panel-kicker">Per Category</div>
              <div class="panel-title">Approval Rates</div>
              <div class="panel-desc">Approved ÷ total submitted per type</div>
            </div>
          </div>
          <div id="rateRows"></div>
        </div>
        <div class="panel">
          <div class="panel-hd">
            <div>
              <div class="panel-kicker">Composition</div>
              <div class="panel-title">Form Breakdown</div>
              <div class="panel-desc">Approved volume by category</div>
            </div>
          </div>
          <div class="chart-wrap" style="height:210px;"><canvas id="donutChart"></canvas></div>
        </div>
      </div>

      <div class="panel mt">
        <div class="panel-hd">
          <div>
            <div class="panel-kicker">Performance Matrix</div>
            <div class="panel-title">Staff × Category Heatmap</div>
            <div class="panel-desc">Approved submissions per member by form type — top 15 by reputation</div>
          </div>
        </div>
        <div id="heatmapWrap" class="heatmap-wrap">
          <div class="loading"><div class="spinner"></div> Building matrix…</div>
        </div>
      </div>
    </div>

    <!-- ═══ ARCHIVE ═══ -->
    <div id="archive" class="view">
      <div class="month-bar">
        <div class="month-nav">
          <button class="month-btn" id="monthPrev">&#8249;</button>
          <div class="month-label" id="monthDisplay">—</div>
          <button class="month-btn" id="monthNext">&#8250;</button>
        </div>
        <input type="month" id="monthPicker" style="background:var(--s3);border:1px solid var(--b1);border-radius:var(--r);color:var(--t1);font-family:var(--mono);font-size:10px;padding:7px 11px;outline:none;height:32px;transition:border-color 120ms;">
        <div id="monthChips" class="month-chips"></div>
      </div>

      <div id="formTabs" class="type-tabs"></div>

      <div class="panel">
        <div id="recList" class="records-list">
          <div class="loading"><div class="spinner"></div> Retrieving records…</div>
        </div>
      </div>
    </div>

    <!-- ═══ RANK ═══ -->
    <div id="rank" class="view">
      <div id="podium" class="podium mt2"></div>
      <div class="panel mt">
        <div class="filter-row">
          <select id="lbCat">
            <option value="reputation">Reputation</option>
            <option value="recruitment">Recruitment</option>
            <option value="progress_report">Progress Reports</option>
            <option value="progress_help">Progress Help</option>
            <option value="purchase_invoice">Invoices</option>
            <option value="demolition_report">Demolitions</option>
            <option value="eviction_report">Evictions</option>
            <option value="scroll_completion">Scrolls</option>
          </select>
          <select id="lbPeriod">
            <option value="weekly">This Week</option>
            <option value="biweekly">Bi-Weekly</option>
            <option value="monthly">This Month</option>
            <option value="all">All Time</option>
          </select>
          <input id="lbSearch" class="inp" placeholder="Filter by name or ID…">
        </div>
        <div class="tbl-scroll">
          <table>
            <thead>
              <tr>
                <th class="r" style="width:44px;">#</th>
                <th>Member</th>
                <th class="r">Score</th>
                <th>Roles</th>
              </tr>
            </thead>
            <tbody id="lbBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ═══ ROSTER ═══ -->
    <div id="roster" class="view">
      <div class="staff-header">
        <input id="staffSearch" class="staff-search" placeholder="Search by name or ID…">
        <div class="view-tog">
          <button class="view-btn on" id="vGrid">Grid</button>
          <button class="view-btn" id="vTable">Table</button>
        </div>
      </div>
      <div id="staffGrid" class="staff-grid"></div>
      <div id="staffTable" style="display:none;">
        <div class="panel">
          <div class="tbl-scroll">
            <table>
              <thead>
                <tr>
                  <th>Member</th>
                  <th class="r">Rep</th>
                  <th class="r">Rec</th>
                  <th class="r">Prog</th>
                  <th class="r">Help</th>
                  <th class="r">Inv</th>
                  <th class="r">Demo</th>
                  <th class="r">Evict</th>
                  <th class="r">Scroll</th>
                  <th class="r">Approvals</th>
                  <th>Roles</th>
                </tr>
              </thead>
              <tbody id="staffTBody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <!-- ═══ TERMINAL ═══ -->
    <div id="terminal" class="view">
      <div class="panel">
        <div class="term-header">
          <div>
            <div class="panel-kicker">Live Output</div>
            <div class="panel-title">Process Log</div>
          </div>
          <div class="term-controls">
            <input class="term-input" id="logFilter" placeholder="Filter output…" style="width:180px;">
            <select class="term-select" id="logLevel">
              <option value="all">All levels</option>
              <option value="info">Info</option>
              <option value="warn">Warnings</option>
              <option value="error">Errors</option>
            </select>
            <label class="term-scroll-lbl">
              <input type="checkbox" id="autoScroll" checked> Auto-scroll
            </label>
            <button class="top-btn" id="clearLogs">Clear</button>
          </div>
        </div>
        <div id="termBox" class="term-box"></div>
        <div id="termFoot" class="term-foot">0 lines</div>
      </div>
    </div>

  </div><!-- /workspace -->
</main>

<!-- ═══ USER MODAL ═══ -->
<div id="modalOverlay" class="overlay" style="display:none;">
  <div class="modal">
    <div class="modal-top">
      <div class="modal-title" id="modalTitle">Member Profile</div>
      <button class="modal-x" id="modalClose">&times;</button>
    </div>
    <div class="modal-tabs">
      <div class="mtab on" data-mtab="overview">Overview</div>
      <div class="mtab" data-mtab="history">History</div>
      <div class="mtab" data-mtab="monthly">By Month</div>
    </div>
    <div class="modal-body">
      <div id="mtab-overview" class="mtab-panel on">
        <div class="modal-user-row">
          <div id="modalAvatar" class="modal-avatar"></div>
          <div>
            <div id="modalName"  class="modal-uname"></div>
            <div id="modalId"    class="modal-uid"></div>
            <div id="modalRoles" class="modal-roles"></div>
          </div>
        </div>
        <div id="modalCounts" class="modal-counts"></div>
        <div style="height:180px;"><canvas id="modalChart"></canvas></div>
      </div>
      <div id="mtab-history" class="mtab-panel">
        <div id="modalHistList"></div>
      </div>
      <div id="mtab-monthly" class="mtab-panel">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:14px;">
          <span style="font-family:var(--mono);font-size:9px;color:var(--t3);letter-spacing:0.14em;">MONTH</span>
          <input type="month" id="modalMonthPicker" style="background:var(--s3);border:1px solid var(--b1);border-radius:var(--r);color:var(--t1);font-family:var(--mono);font-size:10px;padding:6px 10px;outline:none;height:30px;">
        </div>
        <div id="modalMonthList"></div>
      </div>
    </div>
  </div>
</div>

<!-- ═══ LIGHTBOX ═══ -->
<div id="lightbox" class="lightbox">
  <div class="lightbox-inner">
    <img id="lbImg" class="lb-img" src="" alt="">
    <button class="lb-close" id="lbClose">&times;</button>
  </div>
</div>

<script>
/* ═══════════════════════════════════════
   CONFIG
═══════════════════════════════════════ */
const TABLES = ['recruitment','progress_report','purchase_invoice','demolition_report','eviction_report','scroll_completion'];
const LABELS = {
  recruitment:'Recruitment', progress_report:'Progress Report',
  purchase_invoice:'Purchase Invoice', demolition_report:'Demolition Report',
  eviction_report:'Eviction Report', scroll_completion:'Scroll Completion',
};
const SHORT = {
  recruitment:'Rec', progress_report:'Prog', progress_help:'Help',
  purchase_invoice:'Inv', demolition_report:'Demo', eviction_report:'Evict',
  scroll_completion:'Scroll',
};
const PAL = {
  recruitment:'#CFFF47', progress_report:'#60a5fa', progress_help:'#a78bfa',
  purchase_invoice:'#4ade80', demolition_report:'#f87171',
  eviction_report:'#fb923c', scroll_completion:'#f472b6', reputation:'#ededf0',
};
const AV_COLS = ['#CFFF47','#60a5fa','#a78bfa','#4ade80','#f87171','#fb923c','#f472b6','#22d3ee','#fbbf24','#e879f9'];

/* ═══════════════════════════════════════
   STATE
═══════════════════════════════════════ */
const S = {
  staffData: [],
  lbRows: [],
  nameMap: {},          // discord_id (string) → display name
  gran: 'weekly',
  month: (() => { const n = new Date(); return `${n.getFullYear()}-${String(n.getMonth()+1).padStart(2,'0')}`; })(),
  recordsData: {},
  activeFormTab: TABLES[0],
  modalUser: null,
  staffView: 'grid',
  logCount: 0,
};
const charts = {};

/* ═══════════════════════════════════════
   UTILITIES
═══════════════════════════════════════ */
const $ = id => document.getElementById(id);
const N = n => (n ?? 0).toLocaleString('en-US');
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

const fmtDate = s => {
  if (!s) return '—';
  try { return new Date(s).toLocaleString('en-US',{month:'short',day:'numeric',year:'numeric',hour:'2-digit',minute:'2-digit'}); }
  catch { return s; }
};
const monthLabel = ym => {
  if (!ym) return '—';
  const [y,m] = ym.split('-');
  return new Date(+y, +m-1, 1).toLocaleDateString('en-US',{month:'long',year:'numeric'}).toUpperCase();
};
const initials = name => {
  if (!name) return '?';
  const p = name.trim().split(/\s+/);
  return p.length >= 2 ? (p[0][0]+p[p.length-1][0]).toUpperCase() : name.slice(0,2).toUpperCase();
};
const avColor = id => {
  let h = 0; for (const c of String(id)) h = (h*31+c.charCodeAt(0))>>>0;
  return AV_COLS[h % AV_COLS.length];
};

// Resolve a discord_id (string) to a display name, with graceful fallback
const resolveName = id => {
  const sid = String(id || '');
  return S.nameMap[sid] || (sid.length > 8 ? `${sid.slice(0,4)}…${sid.slice(-4)}` : `User ${sid}`);
};

const isImg = (k, v) => {
  if (typeof v !== 'string' || !v.startsWith('http')) return false;
  const lo = v.toLowerCase(), kl = (k||'').toLowerCase();
  return /\.(jpg|jpeg|png|gif|webp|bmp|svg)(\?.*)?$/.test(lo)
    || /s3\.amazonaws\.com|cloudfront\.net|cdn\.discordapp\.com|media\.discordapp\.net/.test(lo)
    || kl.includes('screenshot') || kl.includes('image') || kl.includes('photo')
    || kl.includes('proof') || kl.includes('attachment')
    || (kl.includes('url') && lo.startsWith('http'));
};

function animateNum(el, target, dur=700) {
  const start = parseInt(el.textContent.replace(/\D/g,''))||0;
  const diff = target - start;
  if (!diff) return;
  const t0 = performance.now();
  (function step(now) {
    const p = Math.min((now-t0)/dur,1);
    const e = 1-Math.pow(1-p,3);
    el.textContent = N(Math.round(start+diff*e));
    if (p < 1) requestAnimationFrame(step);
  })(performance.now());
}

/* ═══════════════════════════════════════
   API
═══════════════════════════════════════ */
async function api(url) {
  try { return await (await fetch(url)).json(); }
  catch(e) { console.error(url, e); return null; }
}

/* ═══════════════════════════════════════
   CHARTS
═══════════════════════════════════════ */
Chart.defaults.font.family = "'JetBrains Mono', monospace";
Chart.defaults.color = '#333338';

const TOOLTIP = {
  backgroundColor:'#111113', borderColor:'rgba(255,255,255,0.07)', borderWidth:1,
  titleColor:'#ededf0', bodyColor:'#6b6b75', padding:12, cornerRadius:4,
  titleFont:{ family:"'Barlow Condensed', sans-serif", size:14, weight:'700' },
  bodyFont:{ family:"'JetBrains Mono', monospace", size:10 },
};

function upsertChart(id, type, data, extraOpts={}) {
  const canvas = $(id);
  if (!canvas) return null;
  const baseOpts = {
    responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{ display:false }, tooltip:{ ...TOOLTIP } },
    ...(['line','bar'].includes(type) ? { scales:{
      x:{ grid:{color:'rgba(255,255,255,0.025)',drawBorder:false}, ticks:{color:'#333338',font:{size:9}}, border:{display:false} },
      y:{ beginAtZero:true, grid:{color:'rgba(255,255,255,0.025)',drawBorder:false}, ticks:{color:'#333338',font:{size:9}}, border:{display:false} },
    }} : {}),
  };
  const opts = mergeDeep(baseOpts, extraOpts);
  if (charts[id]) {
    charts[id].data = data;
    Object.assign(charts[id].options, opts);
    charts[id].update('none');
    return charts[id];
  }
  charts[id] = new Chart(canvas.getContext('2d'), { type, data, options:opts });
  return charts[id];
}

function mergeDeep(a, b) {
  const out = { ...a };
  for (const k in b) {
    if (b[k] && typeof b[k] === 'object' && !Array.isArray(b[k]))
      out[k] = mergeDeep(a[k]||{}, b[k]);
    else out[k] = b[k];
  }
  return out;
}

/* ═══════════════════════════════════════
   FORM DETAIL RENDERER
═══════════════════════════════════════ */
const SKIP = new Set(['id','submitted_by','submitted_at','status','approved_by','approved_at']);

function renderFormDetail(data) {
  if (!data) return '<p class="empty">No data available.</p>';
  const imgs = [], fields = [];
  for (const [k,v] of Object.entries(data)) {
    if (SKIP.has(k) || v === null || v === undefined || v === '') continue;
    isImg(k, String(v)) ? imgs.push({k,v}) : fields.push({k,v});
  }
  let h = '';
  if (fields.length) {
    h += '<div class="fields-grid">';
    for (const {k,v} of fields) {
      const lbl  = k.replace(/_/g,' ').replace(/\b\w/,c=>c.toUpperCase());
      const isDate = typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(v);
      const disp   = isDate ? fmtDate(v) : v;
      const isLong = String(disp).length > 60;
      h += `<div class="field"><div class="field-lbl">${esc(lbl)}</div><div class="field-val${isLong?' lg':''}">${esc(String(disp))}</div></div>`;
    }
    h += '</div>';
  }
  if (imgs.length) {
    h += '<div class="ss-hd">Screenshots &amp; Evidence</div><div class="ss-grid">';
    for (const {k,v} of imgs) {
      const safe = esc(v);
      const lbl  = k.replace(/_/g,' ').replace(/\b\w/,c=>c.toUpperCase());
      h += `<div class="ss-item" onclick="openLB('${safe}')"><img src="${safe}" class="ss-img" loading="lazy" onerror="this.closest('.ss-item').style.display='none'"><div class="ss-cap">${esc(lbl)}</div></div>`;
    }
    h += '</div>';
  }
  if (!fields.length && !imgs.length) h = '<p class="empty">No additional fields.</p>';
  return h;
}

/* ═══════════════════════════════════════
   STATUS
═══════════════════════════════════════ */
async function loadStatus() {
  const d = await api('/api/status');
  if (!d) return;
  const dot = $('dot'), txt = $('statusTxt'), meta = $('statusMeta');
  if (d.running) {
    dot.classList.add('on');
    txt.textContent = 'Online';
    meta.innerHTML = `pid ${d.pid}<br><span class="status-uptime">${d.uptime||''}</span>`;
  } else {
    dot.classList.remove('on');
    txt.textContent = 'Offline';
    meta.innerHTML = '';
  }
}

/* ═══════════════════════════════════════
   PULSE
═══════════════════════════════════════ */
async function loadPulse() {
  const d = await api('/api/overview');
  if (!d || d.error) return;
  const t = d.totals||{}, ab = d.approved_breakdown||{}, pb = d.pending_breakdown||{};

  animateNum($('sApproved'), t.approved_total||0);
  animateNum($('sPending'),  t.pending_total||0);
  animateNum($('sRep'),      t.reputation_total||0);
  animateNum($('sStaff'),    t.staff_total||0);

  const total = (t.approved_total||0) + (t.pending_total||0);
  const pct   = total > 0 ? Math.round(t.approved_total/total*100) : 0;
  setTimeout(() => {
    $('sApprovedBar').style.width = pct+'%';
    $('sPendingBar').style.width  = (100-pct)+'%';
  }, 120);

  const buildDetail = src => TABLES.map(k => `${SHORT[k]} <b>${N(src[k]||0)}</b>`).join(' · ');
  $('sApprovedDetail').innerHTML = buildDetail(ab);
  $('sPendingDetail').innerHTML  = buildDetail(pb);

  const pend = t.pending_total||0;
  const badge = $('pendingBadge');
  if (pend > 0) { badge.textContent = pend; badge.style.display=''; }
  else badge.style.display = 'none';

  // Analytics stats (shared data)
  $('anlRate').textContent    = pct+'%';
  $('anlRateSub').textContent = `${N(t.approved_total||0)} of ${N(total)} forms processed`;
  $('anlTotal').textContent   = N(t.approved_total||0);

  let topCat='', topVal=0;
  for (const [k,v] of Object.entries(ab)) if (v>topVal) { topVal=v; topCat=k; }
  $('anlTopForm').textContent = (LABELS[topCat]||topCat||'—').toUpperCase();
  $('anlTopSub').textContent  = `${N(topVal)} approved submissions`;

  buildRateBars(ab, pb);
  buildDonut(ab);
}

async function loadActivity() {
  const data = await api('/api/activity');
  if (!data) return;
  const ul = $('feedList');
  ul.innerHTML = '';
  if (!data.length) { ul.innerHTML = '<li class="empty">No recent activity.</li>'; return; }
  for (const a of data) {
    const li  = document.createElement('li');
    li.className = 'feed-item';
    const sc   = a.status==='approved'?'b-ok':a.status==='pending'?'b-wait':'b-no';
    const name = resolveName(a.submitted_by);
    li.innerHTML = `
      <div>
        <div class="feed-form">${(a.table||'').replace(/_/g,' ')} <span style="color:var(--t3);font-weight:400;">#${a.id}</span></div>
        <div class="feed-time">${fmtDate(a.submitted_at)}</div>
      </div>
      <div class="feed-user">${esc(name)}</div>
      <div style="text-align:right;"><span class="badge ${sc}">${a.status}</span></div>`;
    ul.appendChild(li);
  }
}

async function loadTimeseries(gran) {
  const d = await api(`/api/activity_timeseries?granularity=${gran}`);
  if (!d||d.error) return;
  const {labels, series} = d;
  const defs = [
    {k:'recruitment',l:'Recruitment'},{k:'progress_report',l:'Progress'},
    {k:'progress_help',l:'Help'},{k:'purchase_invoice',l:'Invoice'},
    {k:'demolition_report',l:'Demolition'},{k:'eviction_report',l:'Eviction'},
    {k:'scroll_completion',l:'Scroll'},
  ];
  const datasets = defs.map(c => ({
    label:c.l, data:(series[c.k]||[]),
    borderColor:PAL[c.k], backgroundColor:PAL[c.k]+'10',
    tension:0.4, fill:false, borderWidth:1.5,
    pointRadius:1.5, pointHoverRadius:5, pointBackgroundColor:PAL[c.k],
  }));
  upsertChart('trendChart','line',{labels,datasets},{
    plugins:{ legend:{
      display:true,
      labels:{color:'#6b6b75',font:{size:9,family:"'JetBrains Mono',monospace"},boxWidth:8,padding:12},
    }},
  });
}

function buildDistChart() {
  if (!S.staffData.length) return;
  const cat = $('distSel').value;
  const sorted = [...S.staffData].sort((a,b)=>(b[cat]||0)-(a[cat]||0)).slice(0,14);
  const labels = sorted.map(s => s.label || resolveName(s.discord_id));
  const values = sorted.map(s => s[cat]||0);
  const colors = sorted.map((_,i)=>`hsl(${80+i*20},80%,54%)`);

  $('distInner').style.height = Math.max(200, labels.length*28)+'px';
  upsertChart('distChart','bar',{labels,datasets:[{data:values,backgroundColor:colors,borderRadius:2,borderWidth:0}]},{
    indexAxis:'y',
    scales:{
      x:{beginAtZero:true,grid:{color:'rgba(255,255,255,0.025)',drawBorder:false},ticks:{color:'#333338',font:{size:9}},border:{display:false}},
      y:{grid:{display:false},ticks:{color:'#6b6b75',font:{size:10}},border:{display:false}},
    },
  });
}

function buildRateBars(approved, pending) {
  const el = $('rateRows');
  if (!el) return;
  el.innerHTML = TABLES.map(t => {
    const a = approved[t]||0, p = pending[t]||0;
    const tot = a+p, pct = tot>0?Math.round(a/tot*100):0;
    const col = PAL[t]||'var(--a)';
    return `<div class="rate-row">
      <div class="rate-label">${LABELS[t]||t}</div>
      <div class="rate-track"><div class="rate-fill" style="width:${pct}%;background:${col};"></div></div>
      <div class="rate-pct">${pct}%</div>
    </div>`;
  }).join('');
}

function buildDonut(ab) {
  const keys = Object.keys(ab).filter(k=>ab[k]>0);
  upsertChart('donutChart','doughnut',{
    labels:keys.map(k=>LABELS[k]||k),
    datasets:[{data:keys.map(k=>ab[k]),backgroundColor:keys.map(k=>PAL[k]||'#fff'),borderWidth:0,hoverOffset:8}],
  },{
    cutout:'66%',
    plugins:{
      legend:{display:true,position:'right',labels:{color:'#6b6b75',font:{size:9,family:"'JetBrains Mono',monospace"},boxWidth:8,padding:10}},
      tooltip:TOOLTIP,
    },
  });
}

/* ═══════════════════════════════════════
   SIGNAL — Heatmap
═══════════════════════════════════════ */
function buildHeatmap() {
  if (!S.staffData.length) return;
  const top  = S.staffData.slice(0,15);
  const cats = TABLES;
  const catLabels = cats.map(c => SHORT[c] || c);
  let max = 0;
  for (const s of top) for (const c of cats) max = Math.max(max, s[c]||0);

  const hmClass = v => {
    if (!v || !max) return 'hm0';
    const r = v/max;
    if (r < 0.05) return 'hm0';
    if (r < 0.2)  return 'hm1';
    if (r < 0.4)  return 'hm2';
    if (r < 0.6)  return 'hm3';
    if (r < 0.8)  return 'hm4';
    return 'hm5';
  };

  let html = '<table class="hm-table"><thead><tr><th style="text-align:left;">Staff</th>';
  for (const l of catLabels) html += `<th>${l}</th>`;
  html += '</tr></thead><tbody>';
  for (const s of top) {
    const col   = avColor(s.discord_id);
    const label = s.label || resolveName(s.discord_id);
    html += `<tr><td class="hm-name-cell">
      <span style="display:inline-flex;align-items:center;gap:7px;">
        <span style="width:20px;height:20px;border-radius:50%;background:${col}18;border:1px solid ${col}40;display:inline-flex;align-items:center;justify-content:center;font-size:8px;font-weight:800;color:${col};font-family:var(--disp);flex-shrink:0;">${initials(label)}</span>
        <span style="color:var(--t2);font-size:10px;">${esc(label)}</span>
      </span>
    </td>`;
    for (const c of cats) {
      const v = s[c]||0;
      html += `<td class="${hmClass(v)}" title="${LABELS[c]||c}: ${N(v)}">${v>0?N(v):''}</td>`;
    }
    html += '</tr>';
  }
  $('heatmapWrap').innerHTML = html + '</tbody></table>';
}

/* ═══════════════════════════════════════
   ARCHIVE
═══════════════════════════════════════ */
function setMonth(ym) {
  S.month = ym;
  $('monthDisplay').textContent = monthLabel(ym);
  $('monthPicker').value = ym;
  loadRecords(ym);
}
function stepMonth(delta) {
  const [y,m] = S.month.split('-').map(Number);
  const d = new Date(y, m-1+delta, 1);
  setMonth(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`);
}

async function loadRecords(ym) {
  $('recList').innerHTML = '<div class="loading"><div class="spinner"></div> Retrieving records…</div>';
  $('monthChips').innerHTML = '';
  $('formTabs').innerHTML  = '';
  const data = await api(`/api/monthly_records?month=${ym}`);
  if (!data || data.error) {
    $('recList').innerHTML = '<p style="color:var(--no);font-family:var(--mono);font-size:11px;padding:20px 0;">Failed to load records.</p>';
    return;
  }
  S.recordsData = data;

  let total=0, approved=0, pending=0;
  for (const t of TABLES) {
    const rows = data[t]||[];
    total    += rows.length;
    approved += rows.filter(r=>r.status==='approved').length;
    pending  += rows.filter(r=>r.status==='pending').length;
  }
  $('monthChips').innerHTML = `
    <div class="chip"><b>${N(total)}</b> total</div>
    <div class="chip c-ok"><b>${N(approved)}</b> approved</div>
    <div class="chip c-warn"><b>${N(pending)}</b> pending</div>`;

  const tabsEl = $('formTabs');
  let firstActive = null;
  for (const t of TABLES) {
    const count = (data[t]||[]).length;
    const tab   = document.createElement('div');
    tab.className = 'type-tab';
    tab.dataset.table = t;
    tab.innerHTML = `${LABELS[t]} <span class="tab-ct">${count}</span>`;
    if (!firstActive && count > 0) firstActive = t;
    tab.addEventListener('click', () => {
      tabsEl.querySelectorAll('.type-tab').forEach(x=>x.classList.remove('on'));
      tab.classList.add('on');
      S.activeFormTab = t;
      renderRecList(data[t]||[], t);
    });
    tabsEl.appendChild(tab);
  }
  const activeTable = firstActive || TABLES[0];
  S.activeFormTab = activeTable;
  const firstTab = tabsEl.querySelector(`[data-table="${activeTable}"]`);
  if (firstTab) firstTab.classList.add('on');
  renderRecList(data[activeTable]||[], activeTable);
}

function renderRecList(rows, table) {
  const el = $('recList');
  if (!rows.length) { el.innerHTML = `<div class="empty">No ${LABELS[table]||table} records this month.</div>`; return; }
  el.innerHTML = '';
  for (const row of rows) el.appendChild(buildRecEntry(row));
}

function buildRecEntry(row) {
  const entry = document.createElement('div');
  entry.className = 'rec-entry';

  const sc   = row.status==='approved'?'b-ok':row.status==='pending'?'b-wait':'b-no';
  // submitted_by is now always a string from the server — no precision loss
  const submitterId = String(row.submitted_by || '');
  const name  = resolveName(submitterId);
  const col   = avColor(submitterId);

  // Approver info
  let approverHtml = '';
  if (row.approved_by) {
    const approverId = String(row.approved_by);
    const approverName = resolveName(approverId);
    approverHtml = `<div class="rec-approver">via <span>${esc(approverName)}</span></div>`;
  }

  entry.innerHTML = `
    <div class="rec-row">
      <div class="rec-id">#${row.id}</div>
      <div>
        <div class="rec-who" style="display:flex;align-items:center;gap:7px;">
          <span style="width:20px;height:20px;border-radius:50%;background:${col}18;border:1px solid ${col}40;display:inline-flex;align-items:center;justify-content:center;font-size:7px;font-weight:800;color:${col};font-family:var(--disp);flex-shrink:0;">${initials(name)}</span>
          ${esc(name)}
        </div>
        <div class="rec-sub">${submitterId}</div>
      </div>
      <div class="rec-meta">
        <span class="badge ${sc}">${row.status}</span>
        <div class="rec-date">${fmtDate(row.submitted_at)}</div>
        ${approverHtml}
      </div>
      <div class="rec-chevron">
        <svg width="11" height="11" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M2 4l4 4 4-4"/></svg>
      </div>
    </div>
    <div class="rec-body">${renderFormDetail(row)}</div>`;

  entry.querySelector('.rec-row').addEventListener('click', () => entry.classList.toggle('open'));
  return entry;
}

/* ═══════════════════════════════════════
   RANK
═══════════════════════════════════════ */
async function loadRank() {
  const cat = $('lbCat').value, period = $('lbPeriod').value;
  const d = await api(`/api/leaderboard/${cat}/${period}`);
  if (!d) return;
  S.lbRows = d;
  buildPodium(d.slice(0,3));
  renderLb();
}

function buildPodium(top3) {
  const el = $('podium');
  if (!top3.length) { el.innerHTML=''; return; }
  const orders=[1,0,2], cls=['p2','p1','p3'], places=['2','1','3'];
  let html = '';
  for (let i=0; i<3; i++) {
    const r = top3[orders[i]];
    if (!r) continue;
    const id    = String(r.discord_id||'');
    const label = r.display_name || resolveName(id);
    const val   = r.points ?? r.count ?? 0;
    const col   = avColor(id);
    html += `<div class="podium-card ${cls[i]}" onclick="openModal('${esc(id)}')">
      <div class="podium-place">${places[i]}</div>
      <div class="podium-avatar" style="background:${col}18;border:1px solid ${col}40;color:${col};">${initials(label)}</div>
      <div class="podium-name">${esc(label)}</div>
      <div class="podium-score">${N(val)}</div>
      <div class="podium-label">${$('lbCat').value.replace(/_/g,' ')}</div>
    </div>`;
  }
  el.innerHTML = html;
}

function renderLb() {
  const q = ($('lbSearch').value||'').toLowerCase();
  const rows = S.lbRows.filter(r => {
    const name = (r.display_name || resolveName(String(r.discord_id||''))).toLowerCase();
    return !q || name.includes(q) || String(r.discord_id).includes(q);
  });
  if (!rows.length) {
    $('lbBody').innerHTML = `<tr><td colspan="4" class="empty" style="padding:24px;">No results.</td></tr>`;
    return;
  }
  $('lbBody').innerHTML = rows.map((r,i) => {
    const rank  = i+1;
    const id    = String(r.discord_id||'');
    const label = r.display_name || resolveName(id);
    const val   = r.points ?? r.count ?? 0;
    const col   = avColor(id);
    const rcls  = rank===1?'rank-gold':rank===2?'rank-silver':rank===3?'rank-bronze':'';
    const roles = (r.roles||[]).map(x=>`<span class="role-tag">${esc(x)}</span>`).join('');
    return `<tr onclick="openModal('${esc(id)}')">
      <td class="rank-col ${rcls}">${rank}</td>
      <td class="cell-name">
        <div style="display:flex;align-items:center;gap:10px;">
          <span style="width:28px;height:28px;border-radius:50%;background:${col}18;border:1px solid ${col}40;display:inline-flex;align-items:center;justify-content:center;font-size:9px;font-weight:800;color:${col};font-family:var(--disp);flex-shrink:0;">${initials(label)}</span>
          <div><div class="n1">${esc(label)}</div><div class="n2">${id}</div></div>
        </div>
      </td>
      <td class="r" style="color:var(--a);font-family:var(--disp);font-weight:700;font-size:16px;">${N(val)}</td>
      <td>${roles||'<span style="color:var(--t3);font-size:10px;">—</span>'}</td>
    </tr>`;
  }).join('');
}

/* ═══════════════════════════════════════
   ROSTER
═══════════════════════════════════════ */
async function loadRoster() {
  const d = await api('/api/staff');
  if (!d || !d.staff) return;
  S.staffData = d.staff;
  // discord_id is guaranteed to be a string from the server
  S.nameMap = {};
  for (const s of S.staffData) S.nameMap[s.discord_id] = s.label;
  renderRoster();
  buildDistChart();
}

function renderRoster() {
  S.staffView === 'grid' ? renderGrid() : renderTable();
}

function renderGrid() {
  const q   = ($('staffSearch').value||'').toLowerCase();
  const el  = $('staffGrid');
  const filtered = S.staffData.filter(s => !q || (s.label||'').toLowerCase().includes(q) || s.discord_id.includes(q));
  if (!filtered.length) { el.innerHTML='<div class="empty">No members found.</div>'; return; }
  el.innerHTML = filtered.map(s => {
    const col = avColor(s.discord_id);
    const statsHtml = TABLES.filter(t=>s[t]>0).map(t=>`<span class="scard-chip">${SHORT[t]} <b>${N(s[t])}</b></span>`).join('');
    return `<div class="scard" onclick="openModal('${esc(s.discord_id)}')">
      <div class="scard-avatar" style="background:${col}18;border:1px solid ${col}40;color:${col};">${initials(s.label)}</div>
      <div class="scard-name">${esc(s.label)}</div>
      <div class="scard-id">${s.discord_id}</div>
      <div class="scard-rep">${N(s.reputation)}</div>
      <div class="scard-rep-lbl">Reputation</div>
      <div class="scard-stats">${statsHtml}</div>
    </div>`;
  }).join('');
}

function renderTable() {
  const q   = ($('staffSearch').value||'').toLowerCase();
  const filtered = S.staffData.filter(s => !q || (s.label||'').toLowerCase().includes(q) || s.discord_id.includes(q));
  const tbody = $('staffTBody');
  if (!filtered.length) { tbody.innerHTML='<tr><td colspan="11" class="empty" style="padding:24px;">No members found.</td></tr>'; return; }
  tbody.innerHTML = filtered.map(s => {
    const col   = avColor(s.discord_id);
    const roles = (s.roles||[]).map(r=>`<span class="role-tag">${esc(r)}</span>`).join('') || '—';
    return `<tr onclick="openModal('${esc(s.discord_id)}')">
      <td class="cell-name">
        <div style="display:flex;align-items:center;gap:9px;">
          <span style="width:26px;height:26px;border-radius:50%;background:${col}18;border:1px solid ${col}40;display:inline-flex;align-items:center;justify-content:center;font-size:8px;font-weight:800;color:${col};font-family:var(--disp);flex-shrink:0;">${initials(s.label)}</span>
          <div><div class="n1">${esc(s.label)}</div><div class="n2">${s.discord_id}</div></div>
        </div>
      </td>
      <td class="r" style="color:var(--a);font-family:var(--disp);font-weight:700;font-size:15px;">${N(s.reputation)}</td>
      ${TABLES.map(t=>`<td class="r">${N(s[t])}</td>`).join('')}
      <td class="r">${N(s.approvals)}</td>
      <td>${roles}</td>
    </tr>`;
  }).join('');
}

/* ═══════════════════════════════════════
   USER MODAL
═══════════════════════════════════════ */
async function openModal(discordId) {
  S.modalUser = discordId;
  $('modalOverlay').style.display = 'flex';
  switchMTab('overview');

  const s   = S.staffData.find(x=>x.discord_id===discordId) || {discord_id:discordId,label:resolveName(discordId),reputation:0,roles:[]};
  const col = avColor(discordId);

  $('modalAvatar').style.cssText = `background:${col}18;border:2px solid ${col}40;color:${col};`;
  $('modalAvatar').textContent   = initials(s.label);
  $('modalName').textContent     = s.label;
  $('modalId').textContent       = discordId;
  $('modalRoles').innerHTML      = (s.roles||[]).map(r=>`<span class="role-tag">${esc(r)}</span>`).join('');

  const hist = await api(`/api/user/${discordId}/history`);
  if (!hist) return;

  $('modalCounts').innerHTML = `<div class="modal-chip">Rep <b>${N(s.reputation)}</b></div>` +
    Object.entries(hist.counts||{}).map(([t,c])=>`<div class="modal-chip">${SHORT[t]||t} <b>${N(c)}</b></div>`).join('');

  buildModalChart(hist.counts||{});

  const histItems = (hist.history||[]).slice(0,80);
  $('modalHistList').innerHTML = histItems.length ? histItems.map(h => {
    const sc = h.status==='approved'?'b-ok':h.status==='pending'?'b-wait':'b-no';
    return `<div class="hist-row">
      <div>
        <div class="hist-form">${(h.table||'').replace(/_/g,' ')} <span style="color:var(--t3);font-weight:400;">#${h.id}</span></div>
        <div class="hist-ts">${fmtDate(h.submitted_at)}</div>
      </div>
      <div class="hist-right">
        <span class="badge ${sc}">${h.status}</span>
        <span class="hist-link" onclick="toggleHistDetail(this,'${h.table}',${h.id})">details</span>
      </div>
    </div>
    <div class="hist-detail" id="hd-${h.table}-${h.id}"></div>`;
  }).join('') : '<div class="empty">No submission history.</div>';

  $('modalMonthPicker').value = S.month;
}

function buildModalChart(counts) {
  const labels = TABLES.map(t=>SHORT[t]);
  const values = TABLES.map(t=>counts[t]||0);
  const colors = TABLES.map(t=>PAL[t]);
  upsertChart('modalChart','bar',{labels,datasets:[{data:values,backgroundColor:colors,borderRadius:3,borderWidth:0}]});
}

async function toggleHistDetail(btn, table, id) {
  const el = $(`hd-${table}-${id}`);
  if (el.classList.toggle('on')) {
    if (!el.innerHTML.trim()) {
      el.innerHTML = '<div class="loading" style="padding:10px 0;"><div class="spinner"></div> Loading…</div>';
      const d = await api(`/api/form/${table}/${id}`);
      el.innerHTML = renderFormDetail(d);
    }
  }
}

async function loadModalMonthly() {
  const ym = $('modalMonthPicker').value;
  const el = $('modalMonthList');
  if (!S.modalUser || !ym) return;
  el.innerHTML = '<div class="loading" style="padding:10px 0;"><div class="spinner"></div> Loading…</div>';
  const hist = await api(`/api/user/${S.modalUser}/history`);
  if (!hist) return;
  const items = (hist.history||[]).filter(h => h.submitted_at.startsWith(ym));
  if (!items.length) { el.innerHTML = `<div class="empty">No submissions in ${monthLabel(ym)}.</div>`; return; }
  el.innerHTML = items.map(h => {
    const sc = h.status==='approved'?'b-ok':h.status==='pending'?'b-wait':'b-no';
    return `<div class="hist-row">
      <div><div class="hist-form">${(h.table||'').replace(/_/g,' ')} <span style="color:var(--t3);font-weight:400;">#${h.id}</span></div><div class="hist-ts">${fmtDate(h.submitted_at)}</div></div>
      <span class="badge ${sc}">${h.status}</span>
    </div>`;
  }).join('');
}

function switchMTab(id) {
  document.querySelectorAll('.mtab').forEach(t=>t.classList.toggle('on',t.dataset.mtab===id));
  document.querySelectorAll('.mtab-panel').forEach(p=>p.classList.toggle('on',p.id===`mtab-${id}`));
}

function closeModal() { $('modalOverlay').style.display='none'; }

/* ═══════════════════════════════════════
   LIGHTBOX
═══════════════════════════════════════ */
function openLB(url) { $('lbImg').src=url; $('lightbox').classList.add('on'); }
function closeLB()   { $('lightbox').classList.remove('on'); $('lbImg').src=''; }
$('lbClose').addEventListener('click', closeLB);
$('lightbox').addEventListener('click', e => { if (e.target===$('lightbox')) closeLB(); });

/* ═══════════════════════════════════════
   BOT CONTROLS
═══════════════════════════════════════ */
function setCtrlsDisabled(disabled) {
  ['btnStart','btnStop','btnRestart','btnReset'].forEach(id => $(id).disabled = disabled);
}

async function botAction(action) {
  const msg = $('ctrlMsg');
  setCtrlsDisabled(true);
  msg.innerHTML = '<div class="spinner"></div>';
  try {
    const d = await (await fetch(`/${action}`, {method:'POST'})).json();
    msg.innerHTML = '';
    msg.textContent = d.message || 'Done.';
    setTimeout(() => msg.textContent='', 4000);
    if (['start','stop','restart'].includes(action)) {
      setTimeout(async () => { await loadStatus(); await loadPulse(); await loadActivity(); await loadRoster(); }, 1500);
    } else if (action === 'reset') {
      setTimeout(() => location.reload(), 3000);
    }
  } catch(e) {
    msg.textContent = 'Error: '+e.message;
  } finally {
    setCtrlsDisabled(false);
  }
}

/* ═══════════════════════════════════════
   WEBSOCKET LOGS
═══════════════════════════════════════ */
const socket = io();
socket.on('log', d => {
  const txt = d.line||'';
  const box = $('termBox');
  const el  = document.createElement('span');
  el.className = 'log-ln';
  if (/error|exception|traceback/i.test(txt)) el.classList.add('e');
  else if (/warn/i.test(txt)) el.classList.add('w');
  else if (/info/i.test(txt)) el.classList.add('i');
  el.textContent = txt;
  applyLogFilter(el, txt);
  box.appendChild(el);
  S.logCount++;
  $('termFoot').textContent = `${S.logCount.toLocaleString()} lines captured`;
  if ($('autoScroll').checked) box.scrollTop = box.scrollHeight;
  if (box.children.length > 2000) box.removeChild(box.firstChild);
});

function applyLogFilter(el, txt) {
  const f  = ($('logFilter').value||'').toLowerCase();
  const lv = $('logLevel').value;
  const isE = /error|exception|traceback/i.test(txt);
  const isW = /warn/i.test(txt);
  const isI = /info/i.test(txt);
  let hide = false;
  if (f && !txt.toLowerCase().includes(f)) hide = true;
  if (lv==='error' && !isE) hide = true;
  if (lv==='warn'  && !isW) hide = true;
  if (lv==='info'  && !isI) hide = true;
  el.classList.toggle('gone', hide);
}
const refilter = () => $('termBox').querySelectorAll('.log-ln').forEach(el=>applyLogFilter(el,el.textContent));

/* ═══════════════════════════════════════
   NAVIGATION
═══════════════════════════════════════ */
const META = {
  pulse:    ['Pulse',    'Real-time overview · refreshes every 15s'],
  signal:   ['Signal',   'Analytics · approval rates · performance matrix'],
  archive:  ['Archive',  'Monthly form browser · screenshots · submission details'],
  rank:     ['Rank',     'Staff leaderboard · filter by category and period'],
  roster:   ['Roster',   'Complete staff directory · individual profiles'],
  terminal: ['Terminal', 'Live process output via WebSocket stream'],
};

document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', () => {
    document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
    document.querySelectorAll('.view').forEach(v=>v.classList.remove('active'));
    item.classList.add('active');
    const id = item.dataset.view;
    $(id).classList.add('active');
    const [title, sub] = META[id]||['',''];
    $('pageTitle').textContent = title.toUpperCase();
    $('pageSub').textContent   = sub;
    if (id === 'rank')    loadRank();
    if (id === 'archive') setMonth(S.month);
    if (id === 'signal')  buildHeatmap();
  });
});

// Granularity buttons
document.querySelectorAll('.gran-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.gran-btn').forEach(b=>b.classList.remove('on'));
    btn.classList.add('on');
    S.gran = btn.dataset.gran;
    loadTimeseries(S.gran);
  });
});

// Month navigation
$('monthPrev').addEventListener('click', () => stepMonth(-1));
$('monthNext').addEventListener('click', () => stepMonth(1));
$('monthPicker').addEventListener('change', () => setMonth($('monthPicker').value));

// Staff view toggle
$('vGrid').addEventListener('click', () => {
  S.staffView='grid';
  $('vGrid').classList.add('on'); $('vTable').classList.remove('on');
  $('staffGrid').style.display=''; $('staffTable').style.display='none';
  renderGrid();
});
$('vTable').addEventListener('click', () => {
  S.staffView='table';
  $('vTable').classList.add('on'); $('vGrid').classList.remove('on');
  $('staffTable').style.display=''; $('staffGrid').style.display='none';
  renderTable();
});

$('staffSearch').addEventListener('input', renderRoster);
$('lbCat').addEventListener('change', loadRank);
$('lbPeriod').addEventListener('change', loadRank);
$('lbSearch').addEventListener('input', renderLb);
$('distSel').addEventListener('change', buildDistChart);

// Bot controls
$('btnStart').onclick   = () => botAction('start');
$('btnRestart').onclick = () => botAction('restart');
$('btnStop').onclick    = () => botAction('stop');
$('btnReset').onclick   = () => {
  if (!confirm('Reset the database and S3 bucket? This cannot be undone.')) return;
  botAction('reset');
};

$('refreshBtn').onclick = async () => {
  await loadStatus();
  await loadRoster();
  await Promise.all([loadPulse(), loadActivity(), loadRank(), loadTimeseries(S.gran)]);
  if (document.querySelector('.nav-item.active')?.dataset.view === 'signal') buildHeatmap();
};

// Logs
$('logFilter').addEventListener('input', refilter);
$('logLevel').addEventListener('change', refilter);
$('clearLogs').onclick = () => {
  $('termBox').innerHTML = '';
  S.logCount = 0;
  $('termFoot').textContent = '0 lines captured';
};

// Modal tabs
document.querySelectorAll('.mtab').forEach(t => {
  t.addEventListener('click', () => {
    switchMTab(t.dataset.mtab);
    if (t.dataset.mtab === 'monthly') loadModalMonthly();
  });
});
$('modalMonthPicker').addEventListener('change', loadModalMonthly);
$('modalClose').onclick = closeModal;
$('modalOverlay').addEventListener('click', e => { if (e.target===$('modalOverlay')) closeModal(); });

// Keyboard shortcuts
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') { closeLB(); closeModal(); }
});

// Clock
function tickClock() {
  $('clock').textContent = new Date().toLocaleTimeString('en-US',{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
setInterval(tickClock, 1000);
tickClock();

/* ═══════════════════════════════════════
   INIT
═══════════════════════════════════════ */
(async function init() {
  $('monthPicker').value        = S.month;
  $('monthDisplay').textContent = monthLabel(S.month);
  $('pageTitle').textContent    = 'PULSE';

  await loadStatus();
  await loadRoster();   // populate nameMap first — archive/feed depend on it
  await Promise.all([loadPulse(), loadActivity(), loadRank()]);
  await loadTimeseries(S.gran);

  setInterval(async () => {
    await loadStatus();
    await loadPulse();
    await loadActivity();
  }, 15_000);
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