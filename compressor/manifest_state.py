"""
State V2 manifest helpers for compacted artifact lookup and scheduling.
"""

from __future__ import annotations

import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
COLLECTOR_DIR = ROOT / "collector"
if str(COLLECTOR_DIR) not in sys.path:
    sys.path.insert(0, str(COLLECTOR_DIR))

from config import SYMBOLS  # noqa: E402

UTC = timezone.utc
STATE_SCHEMA_VERSION = 2
MANIFEST_SCHEMA_VERSION = 1

EXPECTED_STREAMS = {
    "binance": ("bbo", "trade", "mark_price", "funding"),
    "bybit": ("bbo", "trade", "mark_price", "funding", "open_interest"),
    "okx": ("bbo", "trade", "mark_price", "funding", "open_interest"),
}
EXPECTED_SYMBOLS = tuple(symbol.lower() for symbol in SYMBOLS)

REASON_SUCCESS = "success"
REASON_MISSING_RAW = "missing_raw"
REASON_ALWAYS_MISSING_COMBO = "always_missing_combo"
REASON_QUALITY_BAD = "quality_bad"
REASON_QUALITY_PARTIAL = "quality_partial"
REASON_COMPACTION_FAILED = "compaction_failed"
REASON_COMPACTION_QUARANTINED = "compaction_quarantined"
REASON_RESOURCE_LIMIT = "resource_limit"
REASON_IN_PROGRESS = "in_progress"
REASON_LOCKED = "locked"
REASON_STALLED = "stalled"
REASON_ARTIFACT_MISSING = "artifact_missing"

AVAILABLE = "available"
UNAVAILABLE = "unavailable"
PENDING = "pending"

PENDING_REASONS = {REASON_IN_PROGRESS, REASON_LOCKED, REASON_STALLED}
NON_RETRYABLE_REASONS = {
    REASON_SUCCESS,
    REASON_MISSING_RAW,
    REASON_ALWAYS_MISSING_COMBO,
    REASON_QUALITY_BAD,
    REASON_COMPACTION_QUARANTINED,
}
RETRYABLE_REASONS = {
    REASON_QUALITY_PARTIAL,
    REASON_COMPACTION_FAILED,
    REASON_RESOURCE_LIMIT,
    REASON_ARTIFACT_MISSING,
    REASON_IN_PROGRESS,
    REASON_LOCKED,
    REASON_STALLED,
}
CORRUPTION_ERROR_TYPES = {"DICT_CONFLICT", "SNAPPY_CORRUPT"}
RESOURCE_LIMIT_ERROR_TYPES = {"RESOURCE_LIMIT", "INSUFFICIENT_TMP_SPACE"}


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def is_v2_state(state: Optional[Dict[str, Any]]) -> bool:
    return isinstance(state, dict) and state.get("schema_version") == STATE_SCHEMA_VERSION


def base_state() -> Dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "updated_at": utc_now_iso(),
        "progress": {"last_complete_date": None},
        "dates": {},
    }


def base_consumer_manifest() -> Dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "updated_at": utc_now_iso(),
        "dates": {},
    }


def expected_partition_keys() -> Iterator[Tuple[str, str, str]]:
    for exchange, streams in EXPECTED_STREAMS.items():
        for stream in streams:
            for symbol in EXPECTED_SYMBOLS:
                yield exchange, stream, symbol


def artifact_keys(date: str, exchange: str, stream: str, symbol: str) -> Dict[str, str]:
    base = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}"
    return {
        "data_key": f"{base}/data.parquet",
        "meta_key": f"{base}/meta.json",
        "quality_day_key": f"{base}/quality_day.json",
    }


def availability_entry(
    availability: str,
    *,
    retryable: bool,
    reason_code: str,
    updated_at: Optional[str] = None,
    artifacts: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "availability": availability,
        "retryable": bool(retryable),
        "reason_code": reason_code,
        "updated_at": updated_at or utc_now_iso(),
    }
    if availability == AVAILABLE and artifacts:
        entry["artifacts"] = artifacts
    return entry


def default_summary(updated_at: Optional[str] = None) -> Dict[str, Any]:
    return {
        "available_count": 0,
        "unavailable_count": 0,
        "pending_count": 0,
        "retryable_count": 0,
        "fetch_ready": False,
        "updated_at": updated_at or utc_now_iso(),
    }


def ensure_date_grid(state: Dict[str, Any], date: str, *, updated_at: Optional[str] = None) -> Dict[str, Any]:
    dates = state.setdefault("dates", {})
    day = dates.setdefault(date, {"summary": default_summary(updated_at), "exchanges": {}})
    exchanges = day.setdefault("exchanges", {})
    ts = updated_at or utc_now_iso()
    for exchange, streams in EXPECTED_STREAMS.items():
        ex = exchanges.setdefault(exchange, {"streams": {}})
        stream_map = ex.setdefault("streams", {})
        for stream in streams:
            st = stream_map.setdefault(stream, {"symbols": {}})
            symbol_map = st.setdefault("symbols", {})
            for symbol in EXPECTED_SYMBOLS:
                symbol_map.setdefault(
                    symbol,
                    availability_entry(
                        UNAVAILABLE,
                        retryable=True,
                        reason_code=REASON_ARTIFACT_MISSING,
                        updated_at=ts,
                    ),
                )
    return day


def set_entry(state: Dict[str, Any], date: str, exchange: str, stream: str, symbol: str, entry: Dict[str, Any]) -> None:
    day = ensure_date_grid(state, date, updated_at=entry.get("updated_at"))
    day["exchanges"][exchange]["streams"][stream]["symbols"][symbol] = entry


def get_entry(state: Dict[str, Any], date: str, exchange: str, stream: str, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        return (
            state["dates"][date]["exchanges"][exchange]["streams"][stream]["symbols"][symbol]
        )
    except Exception:
        return None


def iter_date_entries(state: Dict[str, Any], date: str) -> Iterator[Tuple[str, str, str, Dict[str, Any]]]:
    day = state.get("dates", {}).get(date, {})
    for exchange, ex_data in day.get("exchanges", {}).items():
        for stream, st_data in ex_data.get("streams", {}).items():
            for symbol, entry in st_data.get("symbols", {}).items():
                yield exchange, stream, symbol, entry


def summarize_date(state: Dict[str, Any], date: str, *, updated_at: Optional[str] = None) -> Dict[str, Any]:
    available = unavailable = pending = retryable = 0
    for _, _, _, entry in iter_date_entries(state, date):
        availability = entry.get("availability")
        if availability == AVAILABLE:
            available += 1
        elif availability == PENDING:
            pending += 1
        else:
            unavailable += 1
        if entry.get("retryable"):
            retryable += 1

    summary = {
        "available_count": available,
        "unavailable_count": unavailable,
        "pending_count": pending,
        "retryable_count": retryable,
        "fetch_ready": pending == 0 and retryable == 0,
        "updated_at": updated_at or utc_now_iso(),
    }
    ensure_date_grid(state, date, updated_at=summary["updated_at"])
    state["dates"][date]["summary"] = summary
    return summary


def recompute_progress(state: Dict[str, Any]) -> Optional[str]:
    last_complete = None
    for date in sorted(state.get("dates", {})):
        summary = state["dates"][date].get("summary", {})
        if summary.get("fetch_ready"):
            last_complete = date
            continue
        break
    state.setdefault("progress", {})["last_complete_date"] = last_complete
    return last_complete


def day_summary(state: Dict[str, Any], date: str) -> Dict[str, Any]:
    return state.get("dates", {}).get(date, {}).get("summary", {})


def day_fetch_ready(state: Dict[str, Any], date: str) -> bool:
    return bool(day_summary(state, date).get("fetch_ready"))


def entry_is_available(entry: Optional[Dict[str, Any]]) -> bool:
    return bool(entry) and entry.get("availability") == AVAILABLE


def entry_is_pending(entry: Optional[Dict[str, Any]]) -> bool:
    return bool(entry) and entry.get("availability") == PENDING


def entry_is_retryable(entry: Optional[Dict[str, Any]]) -> bool:
    return bool(entry) and bool(entry.get("retryable"))


def manifest_status(entry: Optional[Dict[str, Any]]) -> Optional[str]:
    if not entry:
        return None
    availability = entry.get("availability")
    reason = entry.get("reason_code")
    if availability == AVAILABLE:
        return "success"
    if availability == PENDING:
        return reason
    if reason in {REASON_QUALITY_BAD, REASON_COMPACTION_QUARANTINED, REASON_ALWAYS_MISSING_COMBO} and not entry.get("retryable"):
        return "quarantine"
    if reason == REASON_QUALITY_PARTIAL:
        return "partial"
    if reason == REASON_RESOURCE_LIMIT:
        return "failed"
    if reason == REASON_ARTIFACT_MISSING:
        return "failed"
    if reason == REASON_MISSING_RAW:
        return "failed"
    if reason == REASON_COMPACTION_FAILED:
        return "failed"
    return reason


def _legacy_error_type(entry: Dict[str, Any]) -> Optional[str]:
    value = entry.get("error_type")
    return str(value) if value is not None else None


def manifest_entry_from_legacy_partition(entry: Dict[str, Any], *, date: str, exchange: str, stream: str, symbol: str) -> Dict[str, Any]:
    status = entry.get("status")
    day_quality = entry.get("day_quality") or entry.get("day_quality_post")
    error_type = _legacy_error_type(entry)
    skip_reason = entry.get("skip_reason")
    updated_at = entry.get("updated_at") or utc_now_iso()

    if status == "success":
        return availability_entry(
            AVAILABLE,
            retryable=False,
            reason_code=REASON_SUCCESS,
            updated_at=updated_at,
            artifacts=artifact_keys(date, exchange, stream, symbol),
        )

    if status == "skipped":
        if skip_reason in {"already_success", "artifact_exists"}:
            return availability_entry(
                AVAILABLE,
                retryable=False,
                reason_code=REASON_SUCCESS,
                updated_at=updated_at,
                artifacts=artifact_keys(date, exchange, stream, symbol),
            )
        if skip_reason == "partial_day":
            return availability_entry(
                UNAVAILABLE,
                retryable=True,
                reason_code=REASON_QUALITY_PARTIAL,
                updated_at=updated_at,
            )
        if skip_reason == "already_quarantined":
            return availability_entry(
                UNAVAILABLE,
                retryable=False,
                reason_code=REASON_COMPACTION_QUARANTINED,
                updated_at=updated_at,
            )

    if status in {"in_progress", "locked", "stalled"}:
        reason = {
            "in_progress": REASON_IN_PROGRESS,
            "locked": REASON_LOCKED,
            "stalled": REASON_STALLED,
        }[status]
        return availability_entry(PENDING, retryable=True, reason_code=reason, updated_at=updated_at)

    if day_quality == "BAD":
        return availability_entry(UNAVAILABLE, retryable=False, reason_code=REASON_QUALITY_BAD, updated_at=updated_at)

    if day_quality == "PARTIAL" or status == "partial":
        return availability_entry(UNAVAILABLE, retryable=True, reason_code=REASON_QUALITY_PARTIAL, updated_at=updated_at)

    if status == "quarantine":
        if error_type in CORRUPTION_ERROR_TYPES:
            return availability_entry(
                UNAVAILABLE,
                retryable=False,
                reason_code=REASON_COMPACTION_QUARANTINED,
                updated_at=updated_at,
            )
        return availability_entry(
            UNAVAILABLE,
            retryable=False,
            reason_code=REASON_COMPACTION_QUARANTINED,
            updated_at=updated_at,
        )

    if error_type in RESOURCE_LIMIT_ERROR_TYPES:
        return availability_entry(UNAVAILABLE, retryable=True, reason_code=REASON_RESOURCE_LIMIT, updated_at=updated_at)

    if status in {"failed", "download_failed", "aborted", "no_files"}:
        return availability_entry(UNAVAILABLE, retryable=True, reason_code=REASON_COMPACTION_FAILED, updated_at=updated_at)

    return availability_entry(UNAVAILABLE, retryable=True, reason_code=REASON_ARTIFACT_MISSING, updated_at=updated_at)


def flatten_v2_entries(state: Dict[str, Any]) -> Dict[Tuple[str, str, str, str], Dict[str, Any]]:
    flattened: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for date in sorted(state.get("dates", {})):
        for exchange, stream, symbol, entry in iter_date_entries(state, date):
            flattened[(date, exchange, stream, symbol)] = deepcopy(entry)
    return flattened


def extract_state_overrides(state: Dict[str, Any]) -> Dict[Tuple[str, str, str, str], Dict[str, Any]]:
    if is_v2_state(state):
        return flatten_v2_entries(state)

    overrides: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for key, entry in state.get("partitions", {}).items():
        parts = key.split("/")
        if len(parts) != 4:
            continue
        exchange, stream, symbol, date = parts
        overrides[(date, exchange, stream, symbol)] = manifest_entry_from_legacy_partition(
            entry,
            date=date,
            exchange=exchange,
            stream=stream,
            symbol=symbol,
        )
    return overrides


def extract_legacy_day_overrides(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    overrides: Dict[str, Dict[str, Any]] = {}
    for date, entry in state.get("days", {}).items():
        if entry.get("status") == "quarantine":
            overrides[date] = availability_entry(
                UNAVAILABLE,
                retryable=False,
                reason_code=REASON_QUALITY_BAD,
                updated_at=entry.get("updated_at") or utc_now_iso(),
            )
    return overrides


def extract_known_day_quality(state: Dict[str, Any]) -> Dict[str, str]:
    known: Dict[str, str] = {}
    if is_v2_state(state):
        for date, day in state.get("dates", {}).items():
            summary = day.get("summary", {})
            if summary.get("fetch_ready") and summary.get("available_count", 0) == 0 and summary.get("retryable_count", 0) == 0:
                continue
            for _, _, _, entry in iter_date_entries(state, date):
                reason = entry.get("reason_code")
                if reason == REASON_QUALITY_BAD:
                    known[date] = "BAD"
                    break
                if reason == REASON_QUALITY_PARTIAL:
                    known[date] = "PARTIAL"
            else:
                known.setdefault(date, "GOOD")
        return known

    for date, entry in state.get("days", {}).items():
        if entry.get("status") == "quarantine":
            known[date] = "BAD"
    for key, entry in state.get("partitions", {}).items():
        parts = key.split("/")
        if len(parts) != 4:
            continue
        date = parts[3]
        day_quality = entry.get("day_quality") or entry.get("day_quality_post")
        if day_quality in {"BAD", "PARTIAL", "DEGRADED", "GOOD"}:
            known.setdefault(date, day_quality)
    return known


def extract_progress_seed(state: Dict[str, Any]) -> Optional[str]:
    if is_v2_state(state):
        return state.get("progress", {}).get("last_complete_date")
    return state.get("last_compacted_date")


def preserve_progress_seed(state: Dict[str, Any], progress_seed: Optional[str]) -> Optional[str]:
    if not progress_seed or not is_v2_state(state):
        return state.get("progress", {}).get("last_complete_date")
    current = state.setdefault("progress", {}).get("last_complete_date")
    if current is None or progress_seed > current:
        state["progress"]["last_complete_date"] = progress_seed
    return state["progress"]["last_complete_date"]


def result_to_manifest_entry(result: Dict[str, Any], final_status: Optional[str] = None) -> Dict[str, Any]:
    exchange = result["exchange"]
    stream = result["stream"]
    symbol = result["symbol"]
    date = result["date"]
    updated_at = utc_now_iso()
    status = final_status or result.get("status", "unknown")

    if status == "success":
        return availability_entry(
            AVAILABLE,
            retryable=False,
            reason_code=REASON_SUCCESS,
            updated_at=updated_at,
            artifacts=artifact_keys(date, exchange, stream, symbol),
        )

    if status in {"in_progress", "locked", "stalled"}:
        reason = {
            "in_progress": REASON_IN_PROGRESS,
            "locked": REASON_LOCKED,
            "stalled": REASON_STALLED,
        }[status]
        return availability_entry(PENDING, retryable=True, reason_code=reason, updated_at=updated_at)

    if result.get("day_quality") == "BAD":
        return availability_entry(UNAVAILABLE, retryable=False, reason_code=REASON_QUALITY_BAD, updated_at=updated_at)

    if status == "partial" or result.get("day_quality") == "PARTIAL":
        return availability_entry(UNAVAILABLE, retryable=True, reason_code=REASON_QUALITY_PARTIAL, updated_at=updated_at)

    if status == "quarantine":
        return availability_entry(
            UNAVAILABLE,
            retryable=False,
            reason_code=REASON_COMPACTION_QUARANTINED,
            updated_at=updated_at,
        )

    if result.get("error_type") in RESOURCE_LIMIT_ERROR_TYPES:
        return availability_entry(UNAVAILABLE, retryable=True, reason_code=REASON_RESOURCE_LIMIT, updated_at=updated_at)

    return availability_entry(UNAVAILABLE, retryable=True, reason_code=REASON_COMPACTION_FAILED, updated_at=updated_at)


def build_consumer_manifest(state: Dict[str, Any]) -> Dict[str, Any]:
    manifest = base_consumer_manifest()
    manifest["updated_at"] = state.get("updated_at") or utc_now_iso()

    for date in sorted(state.get("dates", {})):
        day = state["dates"].get(date, {})
        manifest_day = manifest["dates"].setdefault(date, {"exchanges": {}})
        manifest_exchanges = manifest_day["exchanges"]

        for exchange, ex_data in day.get("exchanges", {}).items():
            manifest_exchange = manifest_exchanges.setdefault(exchange, {"streams": {}})
            manifest_streams = manifest_exchange["streams"]

            for stream, st_data in ex_data.get("streams", {}).items():
                manifest_stream = manifest_streams.setdefault(stream, {"symbols": {}})
                manifest_symbols = manifest_stream["symbols"]

                for symbol, entry in st_data.get("symbols", {}).items():
                    if entry_is_available(entry):
                        manifest_entry = {
                            "available": True,
                            "artifacts": deepcopy(entry.get("artifacts", {})),
                        }
                    else:
                        manifest_entry = {"available": False}
                    manifest_symbols[symbol] = manifest_entry

    return manifest
