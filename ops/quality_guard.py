#!/usr/bin/env python3
import glob
import json
import os
import shutil
import subprocess
from datetime import datetime

QUALITY_DIR = os.getenv("QUALITY_DIR", "/opt/quantlab/quality")
STATE_FILE = os.getenv("QUALITY_GUARD_STATE", "/var/lib/quantlab/quality_guard_state.json")


def _parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def _load_latest_window():
    pattern = os.path.join(QUALITY_DIR, "date=*", "window=*.json")
    files = glob.glob(pattern)
    latest = None
    latest_ts = None
    for fp in files:
        try:
            with open(fp, "r") as f:
                data = json.load(f)
            ts = _parse_ts(data.get("window_start", ""))
        except Exception:
            continue
        if latest_ts is None or ts > latest_ts:
            latest_ts = ts
            latest = data
    return latest


def _load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def _compute_triggers(window):
    sig = window.get("signals", {})
    drops = sig.get("dropped_events", 0)
    queue = sig.get("queue_pct_peak", 0.0)
    offline = sig.get("offline_seconds_by_exchange", {}) or {}
    max_off = max(offline.values()) if offline else 0.0

    triggers = []
    if drops > 0:
        triggers.append(f"drops>0 ({drops})")
    if max_off > 120:
        triggers.append(f"max_offline>120s ({max_off:.1f}s)")
    if queue >= 80:
        triggers.append(f"queue>=80% ({queue:.1f}%)")
    return triggers, max_off


def _log(message: str):
    logger = shutil.which("logger")
    if logger:
        subprocess.run([logger, "-t", "quantlab-quality-guard", message], check=False)
    else:
        print(message)


def main():
    window = _load_latest_window()
    if not window:
        return 0

    if window.get("quality") != "BAD":
        return 0

    window_start = window.get("window_start")
    window_end = window.get("window_end")

    state = _load_state()
    if state.get("last_alert_window_start") == window_start:
        return 0

    sig = window.get("signals", {})
    triggers, max_off = _compute_triggers(window)

    offline = sig.get("offline_seconds_by_exchange", {}) or {}
    reconnects = sig.get("reconnects", 0)
    drops = sig.get("dropped_events", 0)
    queue = sig.get("queue_pct_peak", 0.0)
    accel = sig.get("drain_mode_accelerated_seconds", 0.0)

    trigger_text = ", ".join(triggers) if triggers else "no-threshold-trigger"
    message = (
        "BAD quality window detected | "
        f"window={window_start}..{window_end} | "
        f"triggers={trigger_text} | "
        f"offline={offline} max_offline={max_off:.1f}s | "
        f"drops={drops} queue_pct_peak={queue:.1f} accel_sec={accel:.1f} reconnects={reconnects}"
    )

    _log(message)
    state["last_alert_window_start"] = window_start
    _save_state(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
