#!/usr/bin/env python3
"""Audit raw bucket coverage by exchange/stream/symbol/date."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Set, Tuple

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "collector"))

from config import SYMBOLS  # noqa: E402

UTC = timezone.utc

EXPECTED_STREAMS = {
    "binance": ("bbo", "trade", "mark_price", "funding"),
    "bybit": ("bbo", "trade", "mark_price", "funding", "open_interest"),
    "okx": ("bbo", "trade", "mark_price", "funding", "open_interest"),
}


@dataclass(frozen=True)
class Partition:
    exchange: str
    stream: str
    symbol: str
    date: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit raw bucket coverage gaps")
    parser.add_argument(
        "--report",
        default=str(ROOT / "ops" / "raw_coverage_audit_report.json"),
        help="Path to write JSON report",
    )
    parser.add_argument(
        "--today",
        default=datetime.now(tz=UTC).strftime("%Y%m%d"),
        help="Ignore dates >= today (UTC), format YYYYMMDD",
    )
    parser.add_argument(
        "--limit-days",
        type=int,
        default=0,
        help="Optional limit for printed intermittent gap dates (0 = all)",
    )
    return parser.parse_args()


def load_env() -> Dict[str, str]:
    load_dotenv(ROOT / ".env", override=False)
    return dict(os.environ)


def build_s3_client(env: Dict[str, str]):
    return boto3.client(
        "s3",
        endpoint_url=env["S3_ENDPOINT"],
        aws_access_key_id=env["S3_ACCESS_KEY"],
        aws_secret_access_key=env["S3_SECRET_KEY"],
    )


def parse_partition_key(key: str) -> Optional[Partition]:
    if not key.endswith(".parquet"):
        return None
    parts = [part for part in key.split("/") if part]
    if len(parts) < 5:
        return None
    try:
        exchange = parts[0].split("=", 1)[1].lower()
        stream = parts[1].split("=", 1)[1].lower()
        symbol = parts[2].split("=", 1)[1].lower()
        date = parts[3].split("=", 1)[1]
    except IndexError:
        return None
    if len(date) != 8 or not date.isdigit():
        return None
    return Partition(exchange=exchange, stream=stream, symbol=symbol, date=date)


def list_prefixes(s3_client, bucket: str, prefix: str) -> List[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    prefixes: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        prefixes.extend(item["Prefix"] for item in page.get("CommonPrefixes", []))
    return prefixes


def parse_component(prefix: str, index: int) -> str:
    parts = [part for part in prefix.split("/") if part]
    return parts[index].split("=", 1)[1]


def scan_raw_partitions(s3_client, bucket: str, today: str) -> Set[Partition]:
    partitions: Set[Partition] = set()
    for exchange_prefix in list_prefixes(s3_client, bucket, "exchange="):
        exchange = parse_component(exchange_prefix, 0).lower()
        for stream_prefix in list_prefixes(s3_client, bucket, exchange_prefix + "stream="):
            stream = parse_component(stream_prefix, 1).lower()
            for symbol_prefix in list_prefixes(s3_client, bucket, stream_prefix + "symbol="):
                symbol = parse_component(symbol_prefix, 2).lower()
                for date_prefix in list_prefixes(s3_client, bucket, symbol_prefix + "date="):
                    date = parse_component(date_prefix, 3)
                    if len(date) == 8 and date.isdigit() and date < today:
                        partitions.add(Partition(exchange=exchange, stream=stream, symbol=symbol, date=date))
    return partitions


def build_presence_maps(
    partitions: Iterable[Partition],
) -> Tuple[
    DefaultDict[Tuple[str, str, str], Set[str]],
    DefaultDict[str, Set[str]],
    DefaultDict[Tuple[str, str], Set[str]],
    DefaultDict[Tuple[str, str], Set[str]],
]:
    combo_dates: DefaultDict[Tuple[str, str, str], Set[str]] = defaultdict(set)
    exchange_dates: DefaultDict[str, Set[str]] = defaultdict(set)
    exchange_symbol_dates: DefaultDict[Tuple[str, str], Set[str]] = defaultdict(set)
    exchange_stream_dates: DefaultDict[Tuple[str, str], Set[str]] = defaultdict(set)
    for part in partitions:
        combo_dates[(part.exchange, part.stream, part.symbol)].add(part.date)
        exchange_dates[part.exchange].add(part.date)
        exchange_symbol_dates[(part.exchange, part.symbol)].add(part.date)
        exchange_stream_dates[(part.exchange, part.stream)].add(part.date)
    return combo_dates, exchange_dates, exchange_symbol_dates, exchange_stream_dates


def classify_gaps(
    combo_dates: DefaultDict[Tuple[str, str, str], Set[str]],
    exchange_dates: DefaultDict[str, Set[str]],
    exchange_symbol_dates: DefaultDict[Tuple[str, str], Set[str]],
) -> Dict[str, object]:
    always_missing: List[Dict[str, object]] = []
    intermittent: List[Dict[str, object]] = []
    summary = Counter()

    for exchange, streams in EXPECTED_STREAMS.items():
        exchange_universe = exchange_dates.get(exchange, set())
        for symbol in SYMBOLS:
            symbol_key = symbol.lower()
            symbol_universe = exchange_symbol_dates.get((exchange, symbol_key), set())
            peer_universe = sorted(symbol_universe or exchange_universe)
            for stream in streams:
                present = combo_dates.get((exchange, stream, symbol_key), set())
                if not present:
                    record = {
                        "exchange": exchange,
                        "stream": stream,
                        "symbol": symbol_key,
                        "status": "always_missing_combo",
                        "exchange_active_dates": len(exchange_universe),
                        "symbol_active_dates": len(symbol_universe),
                    }
                    always_missing.append(record)
                    summary["always_missing_combo"] += 1
                    continue

                expected_dates = symbol_universe or exchange_universe
                missing_dates = sorted(expected_dates - present)
                if not missing_dates:
                    summary["complete_combo"] += 1
                    continue

                missing_with_peer = [d for d in missing_dates if d in peer_universe]
                if not missing_with_peer:
                    summary["complete_combo"] += 1
                    continue

                record = {
                    "exchange": exchange,
                    "stream": stream,
                    "symbol": symbol_key,
                    "status": "intermittent_gap",
                    "first_present_date": min(present),
                    "last_present_date": max(present),
                    "missing_dates": missing_with_peer,
                    "missing_count": len(missing_with_peer),
                }
                intermittent.append(record)
                summary["intermittent_gap"] += 1

    intermittent.sort(key=lambda item: (-item["missing_count"], item["exchange"], item["stream"], item["symbol"]))
    always_missing.sort(key=lambda item: (item["exchange"], item["stream"], item["symbol"]))
    return {
        "always_missing": always_missing,
        "intermittent_gaps": intermittent,
        "summary": dict(summary),
    }


def write_report(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def print_summary(payload: Dict[str, object], limit_days: int) -> None:
    summary = payload["summary"]
    print("Raw coverage audit complete.")
    print(f"- complete combos: {summary.get('complete_combo', 0)}")
    print(f"- intermittent gaps: {summary.get('intermittent_gap', 0)}")
    print(f"- always-missing combos: {summary.get('always_missing_combo', 0)}")

    intermittent = payload["intermittent_gaps"]
    if intermittent:
        print("\nTop intermittent gaps:")
        for item in intermittent[:15]:
            missing_dates = item["missing_dates"]
            if limit_days > 0:
                missing_dates = missing_dates[:limit_days]
            print(
                f"- {item['exchange']}/{item['stream']}/{item['symbol']}: "
                f"{item['missing_count']} missing day(s) "
                f"{missing_dates}"
            )

    always_missing = payload["always_missing"]
    if always_missing:
        print("\nAlways-missing combos:")
        for item in always_missing[:20]:
            print(
                f"- {item['exchange']}/{item['stream']}/{item['symbol']} "
                f"(symbol_active_dates={item['symbol_active_dates']}, exchange_active_dates={item['exchange_active_dates']})"
            )


def main() -> int:
    args = parse_args()
    env = load_env()
    required = ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET")
    missing = [key for key in required if not env.get(key)]
    if missing:
        print(f"Missing env keys: {missing}", file=sys.stderr)
        return 1

    s3_client = build_s3_client(env)
    bucket = env["S3_BUCKET"]
    partitions = scan_raw_partitions(s3_client, bucket, args.today)
    combo_dates, exchange_dates, exchange_symbol_dates, exchange_stream_dates = build_presence_maps(partitions)

    payload = classify_gaps(combo_dates, exchange_dates, exchange_symbol_dates)
    payload.update(
        {
            "generated_at_utc": datetime.now(tz=UTC).isoformat(),
            "bucket": bucket,
            "today_utc": args.today,
            "total_partitions": len(partitions),
            "exchange_dates": {k: len(v) for k, v in sorted(exchange_dates.items())},
            "exchange_stream_dates": {f"{k[0]}/{k[1]}": len(v) for k, v in sorted(exchange_stream_dates.items())},
        }
    )

    report_path = Path(args.report)
    write_report(report_path, payload)
    print_summary(payload, args.limit_days)
    print(f"\nReport written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
