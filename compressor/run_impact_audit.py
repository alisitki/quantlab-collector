#!/usr/bin/env python3
"""
Audit whether the latest overnight compressor run disturbed collector behavior.
"""

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

UTC = timezone.utc
ISTANBUL = ZoneInfo("Europe/Istanbul")
COLLECTOR_BASE_URL = "http://127.0.0.1:9100"
COMPRESSOR_UNIT = "quantlab-compact.service"
LOOKBACK_HOURS = 36
WINDOW_MINUTES = 15
SCHEDULED_UTC_HOUR = 2
SCHEDULED_UTC_MINUTE = 30
SCHEDULE_START_SLACK_MINUTES = 15
MAX_WINDOWS_IN_MESSAGE = 8


@dataclass
class CompressorRun:
    start: datetime
    end: Optional[datetime]
    messages: List[str]

    @property
    def completed(self) -> bool:
        return self.end is not None

    @property
    def duration_seconds(self) -> Optional[int]:
        if not self.end:
            return None
        return int((self.end - self.start).total_seconds())


def http_get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def send_telegram_message(message: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing")

    payload = json.dumps(
        {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    ).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        data = json.loads(response.read().decode("utf-8"))
    return bool(data.get("ok"))


def journal_entries(unit: str, since: str) -> List[Dict]:
    proc = subprocess.run(
        ["journalctl", "-u", unit, "--since", since, "--no-pager", "-o", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    entries = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


def ts_from_entry(entry: Dict) -> datetime:
    micros = int(entry["__REALTIME_TIMESTAMP"])
    return datetime.fromtimestamp(micros / 1_000_000, tz=UTC)


def compressor_runs() -> List[CompressorRun]:
    entries = journal_entries(COMPRESSOR_UNIT, f"{LOOKBACK_HOURS} hours ago")
    runs: List[CompressorRun] = []
    current: Optional[CompressorRun] = None

    for entry in entries:
        timestamp = ts_from_entry(entry)
        message = entry.get("MESSAGE") or ""
        if "Starting QuantLab Catch-Up Parquet Compaction Job" in message:
            if current is not None:
                runs.append(current)
            current = CompressorRun(start=timestamp, end=None, messages=[message])
            continue
        if current is None:
            continue
        current.messages.append(message)
        if "Finished QuantLab Catch-Up Parquet Compaction Job." in message:
            current.end = timestamp
            runs.append(current)
            current = None

    if current is not None and current.messages:
        runs.append(current)

    return runs


def scheduled_start_window(now_utc: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    now_utc = now_utc or datetime.now(tz=UTC)
    scheduled_start = datetime.combine(
        now_utc.date(),
        time(hour=SCHEDULED_UTC_HOUR, minute=SCHEDULED_UTC_MINUTE),
        tzinfo=UTC,
    )
    if now_utc < scheduled_start:
        scheduled_start -= timedelta(days=1)
    return scheduled_start, scheduled_start + timedelta(minutes=SCHEDULE_START_SLACK_MINUTES)


def scheduled_compressor_run(now_utc: Optional[datetime] = None) -> CompressorRun:
    runs = compressor_runs()
    window_start, window_end = scheduled_start_window(now_utc)
    candidates = [run for run in runs if window_start <= run.start <= window_end]

    if not candidates:
        start_text = window_start.astimezone(ISTANBUL).strftime("%Y-%m-%d %H:%M:%S %Z")
        end_text = window_end.astimezone(ISTANBUL).strftime("%Y-%m-%d %H:%M:%S %Z")
        raise RuntimeError(
            "No scheduled quantlab-compact run found in expected start window "
            f"{start_text} -> {end_text}"
        )

    return sorted(candidates, key=lambda run: run.start)[0]


def window_start_for(date_str: str, hhmm: str) -> datetime:
    return datetime.strptime(f"{date_str}{hhmm}", "%Y%m%d%H:%M").replace(tzinfo=UTC)


def overlapping_windows(run: CompressorRun) -> List[Dict]:
    overlap: List[Dict] = []
    end = run.end or datetime.now(tz=UTC)
    current_date = run.start.date()
    final_date = end.date()

    while current_date <= final_date:
        date_str = current_date.strftime("%Y%m%d")
        try:
            windows = http_get_json(f"{COLLECTOR_BASE_URL}/collector/day/{date_str}/windows")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"Collector windows unavailable for {date_str}: HTTP {exc.code}") from exc
        except Exception as exc:
            raise RuntimeError(f"Collector windows unavailable for {date_str}: {exc}") from exc

        for item in windows:
            start = window_start_for(date_str, item["window"])
            finish = start + timedelta(minutes=WINDOW_MINUTES)
            if finish > run.start and start < end:
                overlap.append({"date": date_str, "start": start, "finish": finish, **item})

        current_date += timedelta(days=1)

    return overlap


def classify(run: CompressorRun, windows: List[Dict], uploader_now: Optional[Dict]) -> Dict:
    issues: List[str] = []
    notes: List[str] = []

    if not run.completed:
        issues.append("compressor run is still active or journal completion marker is missing")

    if not windows:
        issues.append("no collector quality windows overlap the compressor run")
        return {"status": "ISSUE", "issues": issues, "notes": notes}

    for item in windows:
        label = f"{item['date']} {item['window']} UTC"
        if item.get("quality") != "GOOD":
            issues.append(f"{label}: quality={item.get('quality')}")
        if item.get("drops", 0) > 0:
            issues.append(f"{label}: drops={item['drops']}")
        offline_total = sum(float(v) for v in item.get("offline_seconds", {}).values())
        if offline_total > 0:
            issues.append(f"{label}: offline_seconds_total={offline_total:.1f}")
        if float(item.get("accelerated_drain_seconds", 0.0)) > 0:
            issues.append(f"{label}: accelerated_drain_seconds={item['accelerated_drain_seconds']}")
        queue_peak = float(item.get("queue_peak_pct", 0.0))
        if queue_peak >= 50:
            issues.append(f"{label}: queue_peak_pct={queue_peak:.1f}")
        elif queue_peak >= 25:
            notes.append(f"{label}: queue peak elevated at {queue_peak:.1f}%")
        reconnects = int(item.get("reconnects", 0))
        if reconnects > 0:
            notes.append(f"{label}: reconnects={reconnects}")

    if uploader_now:
        state = uploader_now.get("state")
        if state != "READY":
            issues.append(
                "uploader now state is "
                f"{state} (pending_files={uploader_now.get('pending_files')}, "
                f"spool_size_gb={uploader_now.get('spool_size_gb')})"
            )

    status = "OK" if not issues else "ISSUE"
    return {"status": status, "issues": issues, "notes": notes}


def compressor_summary(run: CompressorRun) -> str:
    if any("No days to process. Task complete." in msg for msg in run.messages):
        return "No days to process"
    scheduled = next((msg for msg in run.messages if "Scheduled:" in msg), None)
    if scheduled:
        return scheduled.replace("Scheduled: ", "Scheduled ")
    return "Run completed"


def summarize_windows(windows: List[Dict]) -> str:
    preview = windows[:MAX_WINDOWS_IN_MESSAGE]
    text = ", ".join(
        f"{item['window']}:{item['quality']}/q={float(item['queue_peak_pct']):.1f}%/d={item['drops']}"
        for item in preview
    )
    if len(windows) > len(preview):
        text += f", +{len(windows) - len(preview)} more"
    return text


def build_message(run: CompressorRun, windows: List[Dict], verdict: Dict, uploader_now: Optional[Dict]) -> str:
    status = verdict["status"]
    icon = "✅" if status == "OK" else "⚠️"
    run_start_local = run.start.astimezone(ISTANBUL).strftime("%Y-%m-%d %H:%M:%S %Z")
    run_end_local = (run.end or datetime.now(tz=UTC)).astimezone(ISTANBUL).strftime("%Y-%m-%d %H:%M:%S %Z")
    duration = f"{run.duration_seconds}s" if run.duration_seconds is not None else "running"
    impacted = summarize_windows(windows)
    scheduled_start, scheduled_end = scheduled_start_window(run.start)
    lines = [
        f"<b>{icon} Compressor Audit: {'Sorun yok' if status == 'OK' else 'Sorun var gibi'}</b>",
        "",
        "Scheduled window: "
        f"{scheduled_start.astimezone(ISTANBUL).strftime('%Y-%m-%d %H:%M:%S %Z')} -> "
        f"{scheduled_end.astimezone(ISTANBUL).strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"Run: {run_start_local} -> {run_end_local}",
        f"Duration: {duration}",
        f"Compressor: {compressor_summary(run)}",
        f"Windows: {impacted}",
    ]

    if uploader_now:
        lines.append(
            "Uploader now: "
            f"{uploader_now.get('state')} | pending={uploader_now.get('pending_files')} | "
            f"spool={uploader_now.get('spool_size_gb')} GB"
        )

    if verdict["issues"]:
        lines.append("")
        lines.append("<b>Issues</b>")
        lines.extend(f"• {issue}" for issue in verdict["issues"][:8])

    if verdict["notes"]:
        lines.append("")
        lines.append("<b>Notes</b>")
        lines.extend(f"• {note}" for note in verdict["notes"][:8])

    lines.append("")
    lines.append(
        f"Audit time: {datetime.now(tz=ISTANBUL).strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="QuantLab compressor impact audit")
    parser.add_argument("--dry-run", action="store_true", help="Print report instead of sending Telegram")
    args = parser.parse_args()

    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path, override=True)

    try:
        run = scheduled_compressor_run()
        windows = overlapping_windows(run)
        try:
            uploader_now = http_get_json(f"{COLLECTOR_BASE_URL}/collector/uploader/now")
        except Exception:
            uploader_now = None
        verdict = classify(run, windows, uploader_now)
        message = build_message(run, windows, verdict, uploader_now)
    except Exception as exc:
        message = (
            "<b>⚠️ Compressor Audit: Sorun var gibi</b>\n\n"
            f"Audit script failed: {exc}\n"
            f"Audit time: {datetime.now(tz=ISTANBUL).strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )
        verdict = {"status": "ISSUE"}

    if args.dry_run:
        print(message)
        return 0 if verdict["status"] == "OK" else 1

    ok = send_telegram_message(message)
    if not ok:
        raise RuntimeError("Telegram send returned not ok")
    return 0 if verdict["status"] == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
