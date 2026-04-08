import subprocess
import sys
import os
import time
import signal
from flask import Flask, jsonify, render_template_string, request

BOT_SCRIPT = "main.py"
PID_FILE = "bot.pid"
START_TIME_FILE = "bot_start_time.txt"
VENV_PYTHON = os.path.join("venv", "Scripts", "python.exe")

app = Flask(__name__)

def get_pid():
    if not os.path.exists(PID_FILE):
        return None
    with open(PID_FILE, "r") as f:
        return int(f.read().strip())

def get_start_time():
    if not os.path.exists(START_TIME_FILE):
        return None
    with open(START_TIME_FILE, "r") as f:
        return float(f.read().strip())

def save_start_time():
    with open(START_TIME_FILE, "w") as f:
        f.write(str(time.time()))

def clear_start_time():
    if os.path.exists(START_TIME_FILE):
        os.remove(START_TIME_FILE)

def start():
    if get_pid() is not None:
        return False, "Bot already running (PID file exists)."
    proc = subprocess.Popen([VENV_PYTHON, BOT_SCRIPT])
    with open(PID_FILE, "w") as f:
        f.write(str(proc.pid))
    save_start_time()
    return True, f"Bot started with PID {proc.pid}"

def stop():
    pid = get_pid()
    if pid is None:
        return False, "No PID file found."
    try:
        subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False, capture_output=True)
        time.sleep(1)
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        clear_start_time()
        return True, f"Bot stopped (PID {pid})"
    except Exception as e:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        clear_start_time()
        return False, f"Error stopping bot: {e}"

def restart():
    stop_result = stop()
    time.sleep(2)
    start_result = start()
    if stop_result[0] and start_result[0]:
        return True, "Bot restarted."
    else:
        return False, f"Stop: {stop_result[1]}, Start: {start_result[1]}"

def reset():
    stop_result = stop()
    # Run reset scripts
    try:
        subprocess.run([VENV_PYTHON, "reset_db.py"], check=True)
        subprocess.run([VENV_PYTHON, "reset_s3.py"], check=True)
    except subprocess.CalledProcessError as e:
        return False, f"Reset failed: {e}"
    start_result = start()
    if stop_result[0] and start_result[0]:
        return True, "Database and S3 reset, bot restarted."
    else:
        return False, f"Stop: {stop_result[1]}, Start: {start_result[1]}"

def get_status():
    pid = get_pid()
    start_time = get_start_time()
    if pid is None:
        return {"running": False, "pid": None, "uptime": None}
    else:
        uptime = None
        if start_time:
            uptime = int(time.time() - start_time)
            days = uptime // 86400
            hours = (uptime % 86400) // 3600
            minutes = (uptime % 3600) // 60
            seconds = uptime % 60
            uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
        else:
            uptime_str = "Unknown"
        return {"running": True, "pid": pid, "uptime": uptime_str}

# HTML template for the web interface
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Bot Manager</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #1e1e2f;
            color: #ffffff;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
        }
        .container {
            background: #2a2a3b;
            padding: 2rem;
            border-radius: 1rem;
            box-shadow: 0 0 20px rgba(0,0,0,0.5);
            text-align: center;
            min-width: 300px;
        }
        h1 {
            margin-top: 0;
            color: #ffcc00;
        }
        .status {
            margin: 20px 0;
            padding: 10px;
            background: #1e1e2f;
            border-radius: 8px;
            font-family: monospace;
        }
        button {
            background: #3a3a4f;
            color: white;
            border: none;
            padding: 10px 20px;
            margin: 5px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 16px;
            transition: 0.2s;
        }
        button:hover {
            background: #4a4a6f;
        }
        button.danger {
            background: #8b0000;
        }
        button.danger:hover {
            background: #a00000;
        }
        button.success {
            background: #006400;
        }
        button.success:hover {
            background: #008000;
        }
        .message {
            margin-top: 15px;
            padding: 8px;
            border-radius: 5px;
            background: #1e1e2f;
            font-size: 14px;
        }
        .loading {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 2px solid #fff;
            border-radius: 50%;
            border-top-color: #ffcc00;
            animation: spin 1s linear infinite;
        }
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Bot Manager</h1>
        <div class="status" id="status">Loading...</div>
        <div>
            <button class="success" onclick="sendAction('start')">▶ Start</button>
            <button class="danger" onclick="sendAction('stop')">⏹ Stop</button>
            <button onclick="sendAction('restart')">🔄 Restart</button>
            <button class="danger" onclick="sendAction('reset')">⚠️ Reset</button>
        </div>
        <div id="message" class="message"></div>
    </div>
    <script>
        async function fetchStatus() {
            try {
                const res = await fetch('/status');
                const data = await res.json();
                const statusDiv = document.getElementById('status');
                if (data.running) {
                    statusDiv.innerHTML = `✅ Bot is running<br>PID: ${data.pid}<br>Uptime: ${data.uptime}`;
                } else {
                    statusDiv.innerHTML = `❌ Bot is not running`;
                }
            } catch (err) {
                document.getElementById('status').innerHTML = '⚠️ Could not fetch status';
            }
        }

        async function sendAction(action) {
            const msgDiv = document.getElementById('message');
            msgDiv.innerHTML = '<div class="loading"></div> Processing...';
            try {
                const res = await fetch(`/${action}`, { method: 'POST' });
                const data = await res.json();
                msgDiv.innerHTML = data.message;
                fetchStatus();
            } catch (err) {
                msgDiv.innerHTML = '❌ Error: ' + err;
            }
        }

        // Poll status every 5 seconds
        fetchStatus();
        setInterval(fetchStatus, 5000);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/status')
def status():
    return jsonify(get_status())

@app.route('/start', methods=['POST'])
def start_bot():
    success, msg = start()
    return jsonify({"success": success, "message": msg})

@app.route('/stop', methods=['POST'])
def stop_bot():
    success, msg = stop()
    return jsonify({"success": success, "message": msg})

@app.route('/restart', methods=['POST'])
def restart_bot():
    success, msg = restart()
    return jsonify({"success": success, "message": msg})

@app.route('/reset', methods=['POST'])
def reset_bot():
    success, msg = reset()
    return jsonify({"success": success, "message": msg})

if __name__ == "__main__":
    # Run on localhost, port 5000
    app.run(host="127.0.0.1", port=5000, debug=False)