from __future__ import annotations

import json
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path


STATE_PATH = Path("scheduler_state.json")

FAST_INTERVAL_SECONDS = 12 * 60 * 60
SLOW_INTERVAL_SECONDS = 36 * 60 * 60

# Slow scan starts 6 hours after scheduler starts, so it does not collide with fast scan.
SLOW_INITIAL_DELAY_SECONDS = 6 * 60 * 60


def now_ts() -> float:
    return time.time()


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_state() -> dict:
    current = now_ts()

    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            pass

    return {
        "started_at": current,
        "last_fast_scan": 0,
        # Pretend slow ran recently so first slow scan waits 6 hours.
        "last_slow_scan": current - SLOW_INTERVAL_SECONDS + SLOW_INITIAL_DELAY_SECONDS,
    }


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2))


def run_scan(label: str, source_file: str) -> bool:
    print(f"\n[{now_text()}] Starting {label} scan using {source_file}...", flush=True)

    if not Path(source_file).exists():
        print(f"[ERROR] Missing {source_file}", flush=True)
        return False

    if Path("sources.yaml").exists():
        shutil.copyfile("sources.yaml", "sources_before_scheduler.yaml")

    shutil.copyfile(source_file, "sources.yaml")

    scan = subprocess.run(
        ["python", "-u", "main.py", "scan-once"],
        text=True,
        check=False,
    )

    dash = subprocess.run(
        ["python", "main.py", "dashboard"],
        text=True,
        check=False,
    )

    if scan.returncode != 0:
        print(f"[WARN] {label} scan exited with code {scan.returncode}", flush=True)
        return False

    if dash.returncode != 0:
        print(f"[WARN] dashboard exited with code {dash.returncode}", flush=True)

    print(f"[{now_text()}] Finished {label} scan.", flush=True)
    return True


def main() -> None:
    print("Job Fit Radar scheduler started.", flush=True)
    print("Fast scan: every 12h | sources_fast.yaml", flush=True)
    print("Slow scan: every 36h, offset by 6h | sources_slow.yaml", flush=True)
    print("Press Control+C to stop.", flush=True)

    state = load_state()
    save_state(state)

    while True:
        current = now_ts()

        due_fast = current - float(state.get("last_fast_scan", 0)) >= FAST_INTERVAL_SECONDS
        due_slow = current - float(state.get("last_slow_scan", 0)) >= SLOW_INTERVAL_SECONDS

        if due_fast:
            ok = run_scan("FAST", "sources_fast.yaml")
            if ok:
                state["last_fast_scan"] = now_ts()
                save_state(state)

        if due_slow:
            ok = run_scan("SLOW", "sources_slow.yaml")
            if ok:
                state["last_slow_scan"] = now_ts()
                save_state(state)

        time.sleep(10 * 60)


if __name__ == "__main__":
    main()
