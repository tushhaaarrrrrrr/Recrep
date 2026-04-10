import subprocess
import sys
import os
import time
import signal

BOT_SCRIPT = "main.py"
PID_FILE = "bot.pid"
VENV_PYTHON = sys.executable

def get_pid():
    if not os.path.exists(PID_FILE):
        return None
    with open(PID_FILE, "r") as f:
        return int(f.read().strip())

def start():
    if get_pid() is not None:
        print("Bot already running (PID file exists).")
        return

    with open("bot.log", "a") as log_file:
        proc = subprocess.Popen(
            [VENV_PYTHON, BOT_SCRIPT],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )

    with open(PID_FILE, "w") as f:
        f.write(str(proc.pid))

    print(f"Bot started in background with PID {proc.pid}")
    print("Use 'python bot_manager.py stop' to stop it.")
    print("Logs are written to bot.log")

def stop():
    pid = get_pid()
    if pid is None:
        print("No PID file found.")
        return
    try:
        subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False, capture_output=True)
        time.sleep(1)
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        print(f"Bot stopped (PID {pid})")
    except Exception as e:
        print(f"Error stopping bot: {e}")
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)

def run():
    """Run the bot in the foreground (same terminal)."""
    if get_pid() is not None:
        print("Bot is already running in background. Stop it first with 'python bot_manager.py stop'.")
        return
    print("Starting bot in foreground... Press Ctrl+C to stop.")
    try:
        subprocess.run([VENV_PYTHON, BOT_SCRIPT])
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
    except Exception as e:
        print(f"Error: {e}")

def reset():
    print("Stopping bot...")
    stop()
    print("Resetting database...")
    subprocess.run([VENV_PYTHON, "reset_db.py"], check=True)
    print("Resetting S3 bucket...")
    subprocess.run([VENV_PYTHON, "reset_s3.py"], check=True)
    print("Restarting bot in background...")
    start()

def main():
    if len(sys.argv) != 2:
        print("Usage: python bot_manager.py [start|stop|restart|reset|run]")
        print("  start   - Start bot in background (logs to bot.log)")
        print("  stop    - Stop background bot")
        print("  restart - Stop then start background bot")
        print("  reset   - Stop, reset DB & S3, then start background bot")
        print("  run     - Run bot in foreground (live logs, Ctrl+C to stop)")
        sys.exit(1)
    cmd = sys.argv[1].lower()
    if cmd == "start":
        start()
    elif cmd == "stop":
        stop()
    elif cmd == "restart":
        stop()
        time.sleep(2)
        start()
    elif cmd == "reset":
        reset()
    elif cmd == "run":
        run()
    else:
        print("Invalid command. Use start, stop, restart, reset, or run.")

if __name__ == "__main__":
    main()