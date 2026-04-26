#!/usr/bin/env python3
"""
Resumeable backfill runner for missing compact artifacts.

Scans raw vs compact storage directly, retries only gap partitions that are likely
recoverable, and stops before the daily 02:30 UTC compaction window.
"""

import argparse
import json
import os
import signal
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

from compact import CompactionJob, Colors, logger
from manifest_state import get_entry, is_v2_state

UTC = timezone.utc
DEFAULT_STOP_BEFORE_HOUR_UTC = 1
DEFAULT_STOP_BEFORE_MINUTE_UTC = 30
DEFAULT_START_BUFFER_MINUTES = 45
HARD_FAILURE_ERROR_TYPES = frozenset({"DICT_CONFLICT", "SNAPPY_CORRUPT"})

shutdown_requested = False


def signal_handler(sig, frame):
    del frame
    global shutdown_requested
    if not shutdown_requested:
        logger.warning(f"Backfill interrupt received (signal {sig}); finishing current partition then stopping.")
        shutdown_requested = True
    else:
        logger.error("Second interrupt received; exiting immediately.")
        sys.exit(1)


def next_cutoff(hour_utc: int, minute_utc: int, now_utc: Optional[datetime] = None) -> datetime:
    now_utc = now_utc or datetime.now(tz=UTC)
    cutoff = now_utc.replace(hour=hour_utc, minute=minute_utc, second=0, microsecond=0)
    if now_utc >= cutoff:
        cutoff += timedelta(days=1)
    return cutoff


def parse_component(prefix: str, index: int) -> str:
    parts = [part for part in prefix.split("/") if part]
    return parts[index].split("=", 1)[1]


def artifact_partition_tuple_from_key(key: str) -> Optional[Tuple[str, str, str, str]]:
    parts = [part for part in key.split("/") if part]
    if len(parts) != 5:
        return None
    try:
        return (
            parts[3].split("=", 1)[1],
            parts[0].split("=", 1)[1],
            parts[1].split("=", 1)[1],
            parts[2].split("=", 1)[1],
        )
    except Exception:
        return None


def classify_missing_partition(entry: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    if not entry:
        return "retryable", "missing_state"

    if "availability" in entry:
        reason = entry.get("reason_code", "unknown")
        if entry.get("retryable"):
            return "retryable", f"reason_code:{reason}"
        return "hard_failure", f"reason_code:{reason}"

    error_type = entry.get("error_type")
    error_text = str(entry.get("error") or "").lower()
    day_quality = entry.get("day_quality")
    if day_quality in {"BAD", "PARTIAL"}:
        return "hard_failure", f"day_quality:{day_quality.lower()}"
    if error_type in HARD_FAILURE_ERROR_TYPES:
        return "hard_failure", f"error_type:{str(error_type).lower()}"
    if (
        "more than one dictionary" in error_text
        or "snappy" in error_text
        or "corrupt" in error_text
        or "deserialize thrift" in error_text
        or "invalid ttype" in error_text
    ):
        return "hard_failure", "error_text:corrupt_or_dict_conflict"
    return "retryable", "state_retryable"


def should_overwrite_retry(state_status: str) -> bool:
    return state_status in {"success", "skipped"}


class GapScanner:
    def __init__(self, job: CompactionJob, today: str):
        self.job = job
        self.today = today

    def _list_raw_prefixes(self, prefix: str) -> List[str]:
        paginator = self.job.s3_client_raw.get_paginator("list_objects_v2")
        prefixes: List[str] = []
        for page in paginator.paginate(Bucket=self.job.raw_bucket, Prefix=prefix, Delimiter="/"):
            prefixes.extend(cp["Prefix"] for cp in page.get("CommonPrefixes", []))
        return prefixes

    def raw_partitions(self) -> Set[Tuple[str, str, str, str]]:
        partitions: Set[Tuple[str, str, str, str]] = set()
        for ex_prefix in self._list_raw_prefixes("exchange="):
            exchange = parse_component(ex_prefix, 0)
            for st_prefix in self._list_raw_prefixes(ex_prefix + "stream="):
                stream = parse_component(st_prefix, 1)
                for sy_prefix in self._list_raw_prefixes(st_prefix + "symbol="):
                    symbol = parse_component(sy_prefix, 2)
                    for d_prefix in self._list_raw_prefixes(sy_prefix + "date="):
                        date = parse_component(d_prefix, 3)
                        if len(date) == 8 and date.isdigit() and date < self.today:
                            partitions.add((date, exchange, stream, symbol))
        return partitions

    def compact_artifacts(self) -> Set[Tuple[str, str, str, str]]:
        data_parts: Set[Tuple[str, str, str, str]] = set()
        meta_parts: Set[Tuple[str, str, str, str]] = set()
        quality_parts: Set[Tuple[str, str, str, str]] = set()

        paginator = self.job.s3_client_compact.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.job.compact_bucket, Prefix="exchange="):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith((".parquet", ".json")):
                    continue
                partition = artifact_partition_tuple_from_key(key)
                if partition is None:
                    continue
                if key.endswith("/data.parquet"):
                    data_parts.add(partition)
                elif key.endswith("/meta.json"):
                    meta_parts.add(partition)
                elif key.endswith("/quality_day.json"):
                    quality_parts.add(partition)

        return data_parts & meta_parts & quality_parts

    def scan(self) -> Dict[str, Any]:
        raw_parts = self.raw_partitions()
        compact_parts = self.compact_artifacts()
        missing_parts = sorted(raw_parts - compact_parts)
        state = self.job.state_manager._read_state()

        retryable_by_date: DefaultDict[str, List[Dict[str, str]]] = defaultdict(list)
        hard_by_date: DefaultDict[str, List[Dict[str, str]]] = defaultdict(list)
        retryable_statuses = Counter()
        hard_statuses = Counter()
        retryable_reasons = Counter()
        hard_reasons = Counter()

        for date, exchange, stream, symbol in missing_parts:
            entry = get_entry(state, date, exchange, stream, symbol) if is_v2_state(state) else state.get("partitions", {}).get(f"{exchange}/{stream}/{symbol}/{date}")
            kind, reason = classify_missing_partition(entry)
            state_status = (entry or {}).get("reason_code") or (entry or {}).get("status", "missing_state")
            item = {
                "date": date,
                "exchange": exchange,
                "stream": stream,
                "symbol": symbol,
                "state_status": state_status,
                "error_type": (entry or {}).get("error_type"),
                "day_quality": (entry or {}).get("day_quality"),
                "classification_reason": reason,
            }
            if kind == "retryable":
                retryable_by_date[date].append(item)
                retryable_statuses[state_status] += 1
                retryable_reasons[reason] += 1
            else:
                hard_by_date[date].append(item)
                hard_statuses[state_status] += 1
                hard_reasons[reason] += 1

        return {
            "raw_partitions": len(raw_parts),
            "compact_complete_partitions": len(compact_parts),
            "missing_partitions": len(missing_parts),
            "retryable_by_date": {date: parts for date, parts in retryable_by_date.items()},
            "hard_by_date": {date: parts for date, parts in hard_by_date.items()},
            "retryable_statuses": dict(retryable_statuses),
            "hard_statuses": dict(hard_statuses),
            "retryable_reasons": dict(retryable_reasons),
            "hard_reasons": dict(hard_reasons),
        }


def write_report(report_path: Path, report: Dict[str, Any]) -> None:
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def partition_sort_key(item: Dict[str, str]) -> Tuple[str, str, str]:
    return (item["exchange"], item["stream"], item["symbol"])


def format_date_summary(date: str, retryable: List[Dict[str, str]], hard: List[Dict[str, str]]) -> str:
    retryable_preview = ", ".join(
        f"{item['exchange']}/{item['stream']}/{item['symbol']}" for item in sorted(retryable, key=partition_sort_key)[:6]
    )
    hard_preview = ", ".join(
        f"{item['exchange']}/{item['stream']}/{item['symbol']}" for item in sorted(hard, key=partition_sort_key)[:4]
    )
    summary = f"{date}: retryable={len(retryable)}"
    if retryable_preview:
        summary += f" [{retryable_preview}]"
    if hard:
        summary += f" | hard={len(hard)}"
        if hard_preview:
            summary += f" [{hard_preview}]"
    return summary


def main() -> int:
    global shutdown_requested

    parser = argparse.ArgumentParser(description="QuantLab missing compact backfill runner")
    parser.add_argument("--workers", type=int, default=1, help="Reserved for future parallelism; currently only 1 is supported")
    parser.add_argument("--stop-before-hour-utc", type=int, default=DEFAULT_STOP_BEFORE_HOUR_UTC)
    parser.add_argument("--stop-before-minute-utc", type=int, default=DEFAULT_STOP_BEFORE_MINUTE_UTC)
    parser.add_argument("--start-buffer-minutes", type=int, default=DEFAULT_START_BUFFER_MINUTES)
    parser.add_argument("--date-order", choices=["asc", "desc"], default="asc")
    parser.add_argument("--report-only", action="store_true", help="Only scan and print the backlog summary")
    args = parser.parse_args()

    if args.workers != 1:
        parser.error("Only --workers 1 is currently supported for same-VPS backfill safety")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path, override=True)

    s3_endpoint = os.getenv("S3_ENDPOINT")
    compact_endpoint = os.getenv("S3_COMPACT_ENDPOINT")
    raw_access_key = os.getenv("S3_ACCESS_KEY")
    raw_secret_key = os.getenv("S3_SECRET_KEY")
    raw_bucket = os.getenv("S3_BUCKET", "quantlab-raw")
    compact_access_key = os.getenv("S3_COMPACT_ACCESS_KEY") or raw_access_key
    compact_secret_key = os.getenv("S3_COMPACT_SECRET_KEY") or raw_secret_key
    compact_bucket = os.getenv("S3_COMPACT_BUCKET", "quantlab-compact")

    if compact_endpoint and compact_endpoint != s3_endpoint:
        logger.error("S3_COMPACT_ENDPOINT must match S3_ENDPOINT on this deployment")
        return 1

    if not all([s3_endpoint, raw_access_key, raw_secret_key, compact_access_key, compact_secret_key]):
        logger.error("Missing S3 configuration (raw or compact) in .env")
        return 1

    today = datetime.now(tz=UTC).strftime("%Y%m%d")
    cutoff = next_cutoff(args.stop_before_hour_utc, args.stop_before_minute_utc)

    job = CompactionJob(
        s3_endpoint=s3_endpoint,
        raw_access_key=raw_access_key,
        raw_secret_key=raw_secret_key,
        compact_access_key=compact_access_key,
        compact_secret_key=compact_secret_key,
        raw_bucket=raw_bucket,
        compact_bucket=compact_bucket,
    )
    job.check_shutdown = lambda: shutdown_requested
    job.sync_manifest_state(today)

    scanner = GapScanner(job, today)
    scan = scanner.scan()
    retryable_by_date = scan["retryable_by_date"]
    hard_by_date = scan["hard_by_date"]

    report = {
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "today": today,
        "raw_partitions": scan["raw_partitions"],
        "compact_complete_partitions": scan["compact_complete_partitions"],
        "missing_partitions": scan["missing_partitions"],
        "retryable_dates": len(retryable_by_date),
        "retryable_partitions": sum(len(parts) for parts in retryable_by_date.values()),
        "hard_failure_dates": len(hard_by_date),
        "hard_failure_partitions": sum(len(parts) for parts in hard_by_date.values()),
        "retryable_statuses": scan["retryable_statuses"],
        "hard_statuses": scan["hard_statuses"],
        "retryable_reasons": scan["retryable_reasons"],
        "hard_reasons": scan["hard_reasons"],
        "retryable_by_date": retryable_by_date,
        "hard_by_date": hard_by_date,
    }
    report_path = Path(__file__).with_name("last_gap_backfill_report.json")
    write_report(report_path, report)

    logger.info(
        "Gap scan: raw=%s complete=%s missing=%s retryable=%s hard=%s",
        scan["raw_partitions"],
        scan["compact_complete_partitions"],
        scan["missing_partitions"],
        report["retryable_partitions"],
        report["hard_failure_partitions"],
    )
    logger.info("Retryable status breakdown: %s", scan["retryable_statuses"])
    logger.info("Retryable reason breakdown: %s", scan["retryable_reasons"])
    logger.info("Hard failure status breakdown: %s", scan["hard_statuses"])
    logger.info("Hard failure reason breakdown: %s", scan["hard_reasons"])

    ordered_dates = sorted(retryable_by_date)
    if args.date_order == "desc":
        ordered_dates = list(reversed(ordered_dates))

    for date in ordered_dates[:12]:
        logger.info("Backfill backlog: %s", format_date_summary(date, retryable_by_date[date], hard_by_date.get(date, [])))

    if args.report_only:
        return 0

    if not ordered_dates:
        logger.info("No retryable compact gaps detected. Backfill is up to date.")
        return 0

    logger.info(
        "Backfill cutoff: %s (start buffer %sm)",
        cutoff.strftime("%Y-%m-%d %H:%M:%S %Z"),
        args.start_buffer_minutes,
    )

    processed = Counter()
    started_dates: List[str] = []

    for date in ordered_dates:
        if shutdown_requested:
            break
        now = datetime.now(tz=UTC)
        if now + timedelta(minutes=args.start_buffer_minutes) >= cutoff:
            logger.warning("Stopping before cutoff to avoid overlap with daily 02:30 UTC compaction.")
            break

        partitions = sorted(retryable_by_date[date], key=partition_sort_key)
        started_dates.append(date)
        logger.info(
            Colors.colorate(
                f">>> GAP BACKFILL DATE {date} | retryable={len(partitions)} | hard={len(hard_by_date.get(date, []))}",
                Colors.CYAN,
            )
        )
        job.state_manager.cleanup_stale_locks(date)

        for part in partitions:
            if shutdown_requested:
                break
            now = datetime.now(tz=UTC)
            if now + timedelta(minutes=args.start_buffer_minutes) >= cutoff:
                logger.warning("Cutoff buffer reached; stopping before starting the next partition.")
                shutdown_requested = True
                break

            result = job.compact_date_partition(
                exchange=part["exchange"],
                stream=part["stream"],
                symbol=part["symbol"],
                date=part["date"],
                overwrite=should_overwrite_retry(part["state_status"]),
                retry_quarantine=True,
            )
            processed[result.get("status", "unknown")] += 1

        logger.info(
            "Date %s partial summary: %s",
            date,
            dict(processed),
        )

    logger.info("Gap backfill finished. Processed statuses: %s", dict(processed))
    if started_dates:
        logger.info("Dates touched this run: %s", started_dates)
    logger.info("Detailed report written to %s", report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
