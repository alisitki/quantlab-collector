"""
QuantLab Parquet Compaction Worker
Consolidates small parquet files into single daily files with seq column for deterministic replay.
Uses state-based catch-up and fast discovery.
"""

import os
import tempfile
import json
import socket
import shutil
import uuid
from copy import deepcopy
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple, Any, Callable
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time
import traceback

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

# Streaming k-way merge for bounded memory compaction
from merge_writer import StreamingMergeWriter
from quality_filter import QualityFilter, POST_FILTER_VERSION
from manifest_state import (
    AVAILABLE,
    EXPECTED_STREAMS,
    EXPECTED_SYMBOLS,
    PENDING,
    REASON_ALWAYS_MISSING_COMBO,
    REASON_ARTIFACT_MISSING,
    REASON_COMPACTION_FAILED,
    REASON_COMPACTION_QUARANTINED,
    REASON_IN_PROGRESS,
    REASON_LOCKED,
    REASON_MISSING_RAW,
    REASON_QUALITY_BAD,
    REASON_QUALITY_PARTIAL,
    REASON_RESOURCE_LIMIT,
    REASON_SUCCESS,
    REASON_STALLED,
    STATE_SCHEMA_VERSION,
    availability_entry,
    artifact_keys,
    base_state,
    build_consumer_manifest,
    ensure_date_grid,
    entry_is_pending,
    entry_is_retryable,
    extract_legacy_day_overrides,
    extract_known_day_quality,
    extract_progress_seed,
    extract_state_overrides,
    get_entry,
    is_v2_state,
    manifest_status,
    recompute_progress,
    preserve_progress_seed,
    result_to_manifest_entry,
    set_entry,
    summarize_date,
    utc_now_iso,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress boto3/urllib3 warnings
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)

DEFAULT_MAX_PARALLEL_DOWNLOADS = 8
STATE_FILE_KEY = "compacted/_state.json"


def manifest_key_for_state_key(state_key: str) -> str:
    if state_key.endswith("_state.json"):
        return state_key[:-11] + "_manifest.json"
    if state_key.endswith(".json"):
        return state_key[:-5] + ".manifest.json"
    return f"{state_key}.manifest"


def classify_compaction_error(message: str) -> Tuple[str, str]:
    lowered = (message or "").lower()
    if "more than one dictionary" in lowered:
        return "DICT_CONFLICT", "quarantine"
    if (
        "snappy" in lowered
        or "corrupt" in lowered
        or "deserialize thrift" in lowered
        or "invalid ttype" in lowered
    ):
        return "SNAPPY_CORRUPT", "quarantine"
    if "too many open files" in lowered or "[errno 24]" in lowered:
        return "RESOURCE_LIMIT", "failed"
    if "insufficient temporary space" in lowered:
        return "INSUFFICIENT_TMP_SPACE", "failed"
    return "OTHER", "quarantine"


class InsufficientTempSpaceError(RuntimeError):
    """Raised when the temporary directory cannot safely hold a partition."""

class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

    @staticmethod
    def colorate(text, color):
        return f"{color}{text}{Colors.END}"

class StateManager:
    """Manages compaction state (_state.json) in S3"""
    
    def __init__(self, s3_client, bucket: str, state_key: str = STATE_FILE_KEY):
        self.s3_client = s3_client
        self.bucket = bucket
        self.state_key = state_key
        self.manifest_key = manifest_key_for_state_key(state_key)
        # Serialize updates to the shared state document across multiple workers/processes.
        # Without this, parallel workers can clobber each other's writes (last-write-wins).
        self.state_lock_key = f"{state_key}.lock"

    def _write_state_documents(self, state: Dict[str, Any]) -> None:
        manifest = build_consumer_manifest(state)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self.state_key,
            Body=json.dumps(state, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self.manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

    def persist_state(self, state: Dict[str, Any]) -> None:
        self._write_state_documents(state)

    def _acquire_state_lock(self, wait_seconds: float = 30.0, ttl_seconds: float = 120.0) -> Optional[str]:
        """
        Acquire a best-effort distributed lock for state updates using S3 conditional put.
        Returns a lock token if acquired, or None if lock couldn't be acquired (caller may fallback).
        """
        token = str(uuid.uuid4())
        body = {
            "token": token,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": datetime.utcnow().isoformat() + "Z",
        }

        deadline = time.time() + wait_seconds
        while time.time() < deadline:
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=self.state_lock_key,
                    Body=json.dumps(body).encode("utf-8"),
                    IfNoneMatch="*",
                    ContentType="application/json",
                )
                return token
            except ClientError as e:
                if e.response["Error"]["Code"] not in ["PreconditionFailed", "412"]:
                    raise

                # Lock exists; if it's stale, break it.
                try:
                    resp = self.s3_client.get_object(Bucket=self.bucket, Key=self.state_lock_key)
                    data = json.loads(resp["Body"].read().decode("utf-8") or "{}")
                    started_at_str = (data.get("started_at") or "").replace("Z", "+00:00")
                    if started_at_str:
                        started_at = datetime.fromisoformat(started_at_str).replace(tzinfo=None)
                        if started_at < (datetime.utcnow() - timedelta(seconds=ttl_seconds)):
                            logger.warning(
                                f"State lock stale (> {ttl_seconds}s). Forcing unlock: {self.state_lock_key}"
                            )
                            self.s3_client.delete_object(Bucket=self.bucket, Key=self.state_lock_key)
                            continue
                except Exception:
                    pass

                time.sleep(0.2)

        logger.warning(f"State lock acquisition timed out: {self.state_lock_key}")
        return None

    def _release_state_lock(self, token: str):
        """Release state lock if we still own it."""
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket, Key=self.state_lock_key)
            data = json.loads(resp["Body"].read().decode("utf-8") or "{}")
            if data.get("token") != token:
                return
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
                return
        except Exception:
            # If we can't verify ownership, avoid deleting someone else's lock.
            return

        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=self.state_lock_key)
        except Exception:
            pass

    def _update_state(self, mutate_fn):
        """
        Read-modify-write state with a best-effort distributed lock.
        Falls back to an unlocked update if the lock can't be acquired, to avoid blocking compaction.
        """
        token = self._acquire_state_lock()
        try:
            state = self._read_state()
            progress_seed = extract_progress_seed(state)
            mutate_fn(state)
            preserve_progress_seed(state, progress_seed)
            self._write_state_documents(state)
        finally:
            if token:
                self._release_state_lock(token)
        
    def get_last_compacted_date(self) -> Optional[str]:
        """Read last_compacted_date from S3"""
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket, Key=self.state_key)
            state = json.loads(resp['Body'].read().decode('utf-8'))
            if is_v2_state(state):
                return state.get("progress", {}).get("last_complete_date")
            return state.get('last_compacted_date')
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
            return None
            
    def update_last_compacted_date(self, date_str: str):
        """Update last_compacted_date and current timestamp in S3 for audit"""
        def mutate(state: Dict):
            if is_v2_state(state):
                state.setdefault("progress", {})["last_complete_date"] = date_str
                state["updated_at"] = utc_now_iso()
            else:
                state["last_compacted_date"] = date_str
                state["updated_at"] = datetime.utcnow().isoformat() + "Z"

        self._update_state(mutate)
        logger.info(f"Updated state: last_complete_date={date_str}")
        
    def log_partition_status(self, result: Dict, status: Optional[str] = None):
        """Log individual partition results into state history"""
        def mutate(state: Dict):
            if not is_v2_state(state):
                state.clear()
                state.update(base_state())
            final_status = status or result.get("status", "unknown")
            entry = result_to_manifest_entry(result, final_status)
            set_entry(
                state,
                result["date"],
                result["exchange"],
                result["stream"],
                result["symbol"],
                entry,
            )
            summarize_date(state, result["date"], updated_at=entry["updated_at"])
            state["updated_at"] = entry["updated_at"]
            recompute_progress(state)

        self._update_state(mutate)

    def log_day_status(self, date: str, status: str):
        """Log day-level status (useful for skipping BAD days entirely)"""
        def mutate(state: Dict):
            if not is_v2_state(state):
                state.clear()
                state.update(base_state())
            updated_at = utc_now_iso()
            day = ensure_date_grid(state, date, updated_at=updated_at)
            reason_code = REASON_QUALITY_BAD if status == "quarantine" else REASON_QUALITY_PARTIAL
            retryable = reason_code == REASON_QUALITY_PARTIAL
            for exchange, ex_data in day.get("exchanges", {}).items():
                for stream, st_data in ex_data.get("streams", {}).items():
                    for symbol, entry in st_data.get("symbols", {}).items():
                        if entry.get("availability") == AVAILABLE:
                            continue
                        st_data["symbols"][symbol] = {
                            "availability": "unavailable",
                            "retryable": retryable,
                            "reason_code": reason_code,
                            "updated_at": updated_at,
                        }
            summarize_date(state, date, updated_at=updated_at)
            state["updated_at"] = updated_at
            recompute_progress(state)

        self._update_state(mutate)

    def acquire_lock(self, result: Dict) -> bool:
        """
        Attempt to acquire an atomic lock for a partition.
        Uses S3 If-None-Match: "*" for atomicity.
        """
        key = f"compacted/locks/{result['exchange']}/{result['stream']}/{result['symbol']}/{result['date']}.lock"
        lock_body = {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": datetime.utcnow().isoformat() + "Z",
            "version": "1.1.0"
        }
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(lock_body).encode('utf-8'),
                IfNoneMatch='*',
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ['PreconditionFailed', '412']:
                return False  # Lock already exists
            raise
        except Exception as e:
            logger.error(f"Error acquired lock for {key}: {e}")
            return False

    def release_lock(self, result: Dict):
        """Release the partition lock."""
        key = f"compacted/locks/{result['exchange']}/{result['stream']}/{result['symbol']}/{result['date']}.lock"
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)
        except Exception as e:
            logger.error(f"Failed to release lock {key}: {e}")

    def cleanup_stale_locks(self, target_date: str = None):
        """
        Clear stale locks. 
        TTL: 2 hours.
        Logic:
        1. List all locks in S3.
        2. If lock exists but state NOT in_progress -> remove lock.
        3. If state in_progress but updated_at > 2h -> set aborted/stalled + remove lock.
        """
        prefix = "compacted/locks/"
        token = self._acquire_state_lock()
        try:
            state = self._read_state()
            progress_seed = extract_progress_seed(state)

            resp = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            locks = resp.get("Contents", [])

            now = datetime.utcnow()
            ttl_limit = now - timedelta(hours=2)
            changed = False

            for l in locks:
                lock_key = l["Key"]
                # compacted/locks/exchange/stream/symbol/date.lock
                rel_path = lock_key.replace(prefix, "").replace(".lock", "")
                parts = rel_path.split("/")
                if len(parts) != 4:
                    continue

                p_date = parts[3]
                if target_date and p_date != target_date:
                    continue

                exchange, stream, symbol = parts[0], parts[1], parts[2]
                entry = get_entry(state, p_date, exchange, stream, symbol)

                lock_stale = False
                trigger_reason = ""

                if not entry or manifest_status(entry) != "in_progress":
                    lock_stale = True
                    trigger_reason = f"Status is {manifest_status(entry) if entry else 'missing'}"
                else:
                    updated_at_str = entry.get("updated_at", "").replace("Z", "+00:00")
                    try:
                        updated_at = datetime.fromisoformat(updated_at_str).replace(tzinfo=None)
                        if updated_at < ttl_limit:
                            lock_stale = True
                            trigger_reason = f"Progress STALLED since {updated_at_str}"
                            entry["availability"] = PENDING
                            entry["retryable"] = True
                            entry["reason_code"] = REASON_STALLED
                            entry["updated_at"] = now.isoformat() + "Z"
                            changed = True
                    except Exception:
                        pass

                if lock_stale:
                    logger.warning(f"Cleanup: Removing stale lock {lock_key} | {trigger_reason}")
                    self.s3_client.delete_object(Bucket=self.bucket, Key=lock_key)

            if changed:
                for date in sorted(state.get("dates", {})):
                    summarize_date(state, date)
                state["updated_at"] = utc_now_iso()
                recompute_progress(state)
                preserve_progress_seed(state, progress_seed)
                self._write_state_documents(state)
        except Exception as e:
            logger.error(f"Error during stale lock cleanup: {e}")
        finally:
            if token:
                self._release_state_lock(token)

    def get_partition_status(self, result: Dict) -> Tuple[Optional[str], Optional[datetime]]:
        """Get current status and timestamp for a partition"""
        state = self._read_state()
        if is_v2_state(state):
            entry = get_entry(state, result["date"], result["exchange"], result["stream"], result["symbol"])
        else:
            key = f"{result['exchange']}/{result['stream']}/{result['symbol']}/{result['date']}"
            entry = state.get("partitions", {}).get(key)
        if not entry:
            return None, None
        
        updated_at = None
        if "updated_at" in entry:
            try:
                updated_at = datetime.fromisoformat(entry["updated_at"].replace('Z', '+00:00'))
            except:
                pass
        return manifest_status(entry), updated_at


    def _read_state(self) -> Dict:
        """Helper to read full state or return empty"""
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket, Key=self.state_key)
            return json.loads(resp['Body'].read().decode('utf-8'))
        except:
            return {}


class CompactionJob:
    """Handles compaction of parquet files from raw to compact bucket"""
    
    def __init__(
        self,
        s3_endpoint: str,
        raw_access_key: str,
        raw_secret_key: str,
        compact_access_key: str,
        compact_secret_key: str,
        raw_bucket: str,
        compact_bucket: str,
        state_key: str = STATE_FILE_KEY
    ):
        self.max_parallel_downloads = max(
            1,
            int(os.getenv("COMPRESSOR_MAX_PARALLEL_DOWNLOADS", str(DEFAULT_MAX_PARALLEL_DOWNLOADS))),
        )
        max_pool_connections = max(16, self.max_parallel_downloads * 2)
        config = Config(max_pool_connections=max_pool_connections)
        # Client for reading raw data
        self.s3_client_raw = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=raw_access_key,
            aws_secret_access_key=raw_secret_key,
            config=config
        )
        # Client for writing compact data and managing state
        self.s3_client_compact = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=compact_access_key,
            aws_secret_access_key=compact_secret_key,
            config=config
        )
        self.raw_bucket = raw_bucket
        self.compact_bucket = compact_bucket
        # State and outputs go to compact bucket
        self.state_manager = StateManager(self.s3_client_compact, self.compact_bucket, state_key=state_key)
        self.check_shutdown = lambda: False
        self.temp_dir_base = Path(
            os.getenv("COMPRESSOR_TMPDIR") or os.getenv("TMPDIR") or tempfile.gettempdir()
        )
        self.temp_dir_base.mkdir(parents=True, exist_ok=True)
        self._quality_cache: Dict[str, Dict[str, Any]] = {}
        self._manifest_raw_partitions_cache: Optional[Set[Tuple[str, str, str, str]]] = None
        self._manifest_artifact_cache: Optional[Dict[Tuple[str, str, str, str], Dict[str, str]]] = None
        self._manifest_today_cache: Optional[str] = None

        # Diagnostics mapping
        self._path_to_s3_key = {}

    def _list_prefixes(self, s3_client, bucket: str, prefix: str) -> List[str]:
        paginator = s3_client.get_paginator('list_objects_v2')
        prefixes = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            prefixes.extend(cp['Prefix'] for cp in page.get('CommonPrefixes', []))
        return prefixes

    def _scan_raw_partitions_before(self, today: str) -> Set[Tuple[str, str, str, str]]:
        if self._manifest_raw_partitions_cache is not None and self._manifest_today_cache == today:
            return set(self._manifest_raw_partitions_cache)

        partitions: Set[Tuple[str, str, str, str]] = set()
        for ex_prefix in self._list_prefixes(self.s3_client_raw, self.raw_bucket, "exchange="):
            exchange = ex_prefix.rstrip('/').split('/')[-1].split('=', 1)[1]
            for st_prefix in self._list_prefixes(self.s3_client_raw, self.raw_bucket, ex_prefix + "stream="):
                stream = st_prefix.rstrip('/').split('/')[-1].split('=', 1)[1]
                for sy_prefix in self._list_prefixes(self.s3_client_raw, self.raw_bucket, st_prefix + "symbol="):
                    symbol = sy_prefix.rstrip('/').split('/')[-1].split('=', 1)[1]
                    for d_prefix in self._list_prefixes(self.s3_client_raw, self.raw_bucket, sy_prefix + "date="):
                        date = d_prefix.rstrip('/').split('/')[-1].split('=', 1)[1]
                        if len(date) == 8 and date.isdigit() and date < today:
                            partitions.add((date, exchange, stream, symbol))

        self._manifest_raw_partitions_cache = set(partitions)
        self._manifest_today_cache = today
        return partitions

    def _scan_compact_artifacts_before(self, today: str) -> Dict[Tuple[str, str, str, str], Dict[str, str]]:
        if self._manifest_artifact_cache is not None and self._manifest_today_cache == today:
            return dict(self._manifest_artifact_cache)

        artifact_map: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
        paginator = self.s3_client_compact.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.compact_bucket, Prefix="exchange="):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith((".parquet", ".json")):
                    continue
                parts = [part for part in key.split("/") if part]
                if len(parts) != 5:
                    continue
                try:
                    exchange = parts[0].split("=", 1)[1]
                    stream = parts[1].split("=", 1)[1]
                    symbol = parts[2].split("=", 1)[1]
                    date = parts[3].split("=", 1)[1]
                except Exception:
                    continue
                if date >= today:
                    continue
                bucket_entry = artifact_map.setdefault((date, exchange, stream, symbol), {})
                if key.endswith("/data.parquet"):
                    bucket_entry["data_key"] = key
                elif key.endswith("/meta.json"):
                    bucket_entry["meta_key"] = key
                elif key.endswith("/quality_day.json"):
                    bucket_entry["quality_day_key"] = key

        artifact_map = {
            key: value
            for key, value in artifact_map.items()
            if {"data_key", "meta_key", "quality_day_key"} <= set(value)
        }
        self._manifest_artifact_cache = dict(artifact_map)
        self._manifest_today_cache = today
        return artifact_map

    def invalidate_manifest_caches(self, date: Optional[str] = None):
        del date
        self._manifest_raw_partitions_cache = None
        self._manifest_artifact_cache = None
        self._manifest_today_cache = None

    def _build_manifest_state(self, existing_state: Dict[str, Any], today: str) -> Dict[str, Any]:
        state = base_state()
        overrides = extract_state_overrides(existing_state)
        day_overrides = extract_legacy_day_overrides(existing_state)
        known_day_quality = extract_known_day_quality(existing_state)
        progress_seed = extract_progress_seed(existing_state)
        raw_parts = self._scan_raw_partitions_before(today)
        artifacts = self._scan_compact_artifacts_before(today)

        raw_combo_dates: Dict[Tuple[str, str, str], Set[str]] = {}
        all_dates: Set[str] = set()
        for date, exchange, stream, symbol in raw_parts:
            all_dates.add(date)
            raw_combo_dates.setdefault((exchange, stream, symbol), set()).add(date)
        for date, _, _, _ in artifacts:
            all_dates.add(date)
        for date, _, _, _ in overrides:
            if date < today:
                all_dates.add(date)
        for date in day_overrides:
            if date < today:
                all_dates.add(date)

        for date in sorted(all_dates):
            day = ensure_date_grid(state, date)
            day_quality = known_day_quality.get(date)
            if day_quality is None:
                day_quality = self._fetch_quality_data(date).get("day_quality")
            day_override = day_overrides.get(date)

            for exchange, streams in EXPECTED_STREAMS.items():
                for stream in streams:
                    for symbol in EXPECTED_SYMBOLS:
                        partition_key = (date, exchange, stream, symbol)
                        current_override = deepcopy(overrides.get(partition_key))

                        if partition_key in artifacts:
                            entry = availability_entry(
                                AVAILABLE,
                                retryable=False,
                                reason_code=REASON_SUCCESS,
                                updated_at=(current_override or {}).get("updated_at") or utc_now_iso(),
                                artifacts=artifacts[partition_key],
                            )
                        elif current_override and current_override.get("availability") == PENDING:
                            entry = current_override
                        elif day_quality == "BAD" or day_override:
                            entry = availability_entry(
                                "unavailable",
                                retryable=False,
                                reason_code=REASON_QUALITY_BAD,
                                updated_at=(current_override or day_override or {}).get("updated_at") or utc_now_iso(),
                            )
                        elif day_quality == "PARTIAL":
                            entry = availability_entry(
                                "unavailable",
                                retryable=True,
                                reason_code=REASON_QUALITY_PARTIAL,
                                updated_at=(current_override or {}).get("updated_at") or utc_now_iso(),
                            )
                        elif partition_key in raw_parts:
                            if current_override and current_override.get("reason_code") in {
                                REASON_COMPACTION_QUARANTINED,
                                REASON_RESOURCE_LIMIT,
                                REASON_COMPACTION_FAILED,
                            }:
                                entry = current_override
                            else:
                                entry = availability_entry(
                                    "unavailable",
                                    retryable=True,
                                    reason_code=REASON_ARTIFACT_MISSING,
                                    updated_at=(current_override or {}).get("updated_at") or utc_now_iso(),
                                )
                        elif not raw_combo_dates.get((exchange, stream, symbol)):
                            entry = availability_entry(
                                "unavailable",
                                retryable=False,
                                reason_code=REASON_ALWAYS_MISSING_COMBO,
                                updated_at=(current_override or {}).get("updated_at") or utc_now_iso(),
                            )
                        else:
                            entry = availability_entry(
                                "unavailable",
                                retryable=True,
                                reason_code=REASON_MISSING_RAW,
                                updated_at=(current_override or {}).get("updated_at") or utc_now_iso(),
                            )

                        day["exchanges"][exchange]["streams"][stream]["symbols"][symbol] = entry

            summarize_date(state, date)

        state["updated_at"] = utc_now_iso()
        recompute_progress(state)
        if progress_seed and progress_seed in state.get("dates", {}):
            current = state.get("progress", {}).get("last_complete_date")
            if current is None or progress_seed > current:
                state["progress"]["last_complete_date"] = progress_seed
        return state

    def sync_manifest_state(self, today: Optional[str] = None, force: bool = False) -> Dict[str, Any]:
        today = today or get_today_date()
        existing = self.state_manager._read_state()
        if not is_v2_state(existing) or force:
            manifest = self._build_manifest_state(existing, today)
        else:
            raw_parts = self._scan_raw_partitions_before(today)
            artifacts = self._scan_compact_artifacts_before(today)
            overrides = extract_state_overrides(existing)
            all_dates = {date for date, _, _, _ in raw_parts}
            all_dates.update(date for date, _, _, _ in artifacts)
            all_dates.update(date for date, _, _, _ in overrides if date < today)
            missing_dates = sorted(date for date in all_dates if date not in existing.get("dates", {}))
            if not missing_dates:
                self.state_manager.persist_state(existing)
                return existing
            rebuilt = self._build_manifest_state(existing, today)
            manifest = rebuilt
        self.state_manager.persist_state(manifest)
        return manifest
        
    def compact_date_partition(
        self,
        exchange: str,
        stream: str,
        symbol: str,
        date: str,
        overwrite: bool = False,
        retry_quarantine: bool = False,
    ) -> Dict:
        """Compact a single partition for a specific date"""
        result = {
            'exchange': exchange, 'stream': stream, 'symbol': symbol, 'date': date,
            'status': 'unknown', 'files_processed': 0, 'total_size_bytes': 0,
            'output_size_bytes': 0, 'rows': 0, 'error': None,
            'day_quality': 'UNKNOWN', 'post_filter_version': POST_FILTER_VERSION,
            'skip_reason': None,
            'merge_time': 0, 'upload_time': 0
        }
        local_files: List[Path] = []

        # Derive keys early so we can reconcile state vs. artifacts before doing any work.
        raw_prefix = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/"
        compact_key = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/data.parquet"
        meta_key = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/meta.json"
        quality_key = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/quality_day.json"
        lock_key = f"compacted/locks/{exchange}/{stream}/{symbol}/{date}.lock"
        
        # 1. State Check
        current_status, _ = self.state_manager.get_partition_status(result)
        if current_status == 'success' and not overwrite:
            result['status'] = 'skipped'
            result['skip_reason'] = 'already_success'
            logger.info(Colors.colorate(f"SKIP {symbol} {stream} {date} | Already successful", Colors.BLUE))
            return result

        if current_status == 'quarantine' and not overwrite and not retry_quarantine:
            # Fast skip: known-bad partition (prevents re-downloading + re-failing on every backfill run).
            result['status'] = 'quarantine'
            result['skip_reason'] = 'already_quarantined'
            logger.info(Colors.colorate(f"SKIP {symbol} {date} | Previously quarantined", Colors.BLUE))
            return result

        # Heal corrupted state from older parallel runs: if artifacts already exist but state is missing/stuck,
        # treat as done and update state so the planner won't keep re-scheduling the same partition/day.
        if not overwrite and current_status in [None, 'in_progress', 'stalled']:
            try:
                lock_exists = self._compact_exists(lock_key)
            except Exception:
                lock_exists = True  # Conservative: assume active lock on errors

            if not lock_exists:
                try:
                    artifacts_exist = (
                        self._compact_exists(compact_key)
                        and self._compact_exists(meta_key)
                        and self._compact_exists(quality_key)
                    )
                except Exception:
                    artifacts_exist = False

                if artifacts_exist:
                    meta = {}
                    try:
                        resp = self.s3_client_compact.get_object(Bucket=self.compact_bucket, Key=meta_key)
                        meta = json.loads(resp['Body'].read().decode('utf-8') or '{}')
                    except Exception:
                        pass

                    healed = {
                        'exchange': exchange,
                        'stream': stream,
                        'symbol': symbol,
                        'date': date,
                        'status': 'success',
                        'day_quality': meta.get('day_quality', 'UNKNOWN'),
                        'post_filter_version': meta.get('post_filter_version', POST_FILTER_VERSION),
                        'rows': meta.get('rows', 0) or 0,
                        'total_size_bytes': 0,
                        'error': None,
                    }
                    try:
                        self.state_manager.log_partition_status(healed, status='success')
                    except Exception:
                        pass

                    result['status'] = 'skipped'
                    result['skip_reason'] = 'artifact_exists'
                    result['rows'] = healed['rows']
                    logger.info(Colors.colorate(f"SKIP {symbol} {stream} {date} | Artifacts already exist (state healed)", Colors.BLUE))
                    return result
            
        # 2. Atomic S3 LOCK acquisition
        if not self.state_manager.acquire_lock(result):
            logger.info(Colors.colorate(f"SKIP {symbol} {date} | Locked by other worker", Colors.BLUE))
            result['status'] = 'locked'
            return result

        try:
            raw_files = []
            # 3. Mark as in-progress immediately after lock
            self.state_manager.log_partition_status(result, status='in_progress')

            # 2. Quality Gating
            t_quality = time.perf_counter()
            quality_report = self._fetch_quality_data(date)
            result['t_quality'] = time.perf_counter() - t_quality
            result['day_quality'] = quality_report['day_quality']
            
            if result['day_quality'] == 'BAD':
                result['status'] = 'quarantine'
                self.state_manager.log_partition_status(result)
                q_tag = Colors.colorate("[BAD]", Colors.RED)
                st_tag = Colors.colorate("[QUARANTINE]", Colors.YELLOW)
                logger.warning(f"{st_tag} {symbol} {stream} {date} | {q_tag} day quality")
                return result
            
            if result['day_quality'] == 'PARTIAL':
                result['status'] = 'partial'
                result['skip_reason'] = 'partial_day'
                result['error'] = 'Partial day data, retry expected'
                self.state_manager.log_partition_status(result)
                q_tag = Colors.colorate("[PARTIAL]", Colors.BLUE)
                st_tag = Colors.colorate("[PARTIAL]", Colors.BLUE)
                logger.info(f"{st_tag} {symbol} {stream} {date} | {q_tag} (waiting for more data)")
                return result
            
            # 3. Compaction
            t0 = time.perf_counter()
            raw_files = self._list_raw_files(raw_prefix)
            result['t_list'] = time.perf_counter() - t0
            
            if not raw_files:
                result['status'] = 'no_files'
                self.state_manager.log_partition_status(result)
                return result
            
            result['files_processed'] = len(raw_files)
            result['total_size_bytes'] = sum(f['size'] for f in raw_files)
            
            self._ensure_temp_capacity(result['total_size_bytes'])

            with tempfile.TemporaryDirectory(dir=str(self.temp_dir_base)) as temp_dir:
                t_down = time.perf_counter()
                local_files = self._download_files(raw_files, temp_dir, symbol, date)
                result['t_download'] = time.perf_counter() - t_down
                
                if not local_files:
                    result['status'] = 'download_failed'
                    result['error'] = 'No files downloaded'
                    self.state_manager.log_partition_status(result)
                    return result
                
                output_path = Path(temp_dir) / 'data.parquet'
                
                t_merge = time.perf_counter()
                metadata = self._merge_parquet_files(local_files, output_path, stream)
                result['merge_time'] = time.perf_counter() - t_merge
                
                # Copy breakdown from merger if available
                if 'timings' in metadata:
                    for k, v in metadata['timings'].items():
                        result[f't_merge_{k}'] = v
                
                result['rows'] = metadata['rows']
                result['output_size_bytes'] = output_path.stat().st_size
                
                # POST-WRITE VERIFICATION: Read back before upload
                t_verify = time.perf_counter()
                self._verify_output_integrity(output_path, metadata['rows'])
                result['t_verify'] = time.perf_counter() - t_verify
                
                # ATOMIC UPLOAD sequence
                t_up = time.perf_counter()
                # All files uploaded with .tmp first
                self._upload_to_s3(output_path, compact_key + ".tmp")
                result['t_upload_data'] = time.perf_counter() - t_up
                
                meta_content = {
                    "rows": metadata['rows'],
                    "ts_event_min": metadata['ts_event_min'],
                    "ts_event_max": metadata['ts_event_max'],
                    "sha256": metadata.get('sha256', 'N/A'),
                    "source_files": len(raw_files),
                    "schema_version": 1,
                    "stream_type": stream,
                    "ordering_columns": ["ts_event", "seq"],
                    "day_quality": result['day_quality'],
                    "post_filter_version": POST_FILTER_VERSION
                }
                self._upload_json_to_s3(meta_content, meta_key + ".tmp")
                self._upload_json_to_s3(quality_report, quality_key + ".tmp")
                
                # Finalize: Promotion via copy+delete
                self._finalize_artifacts([compact_key, meta_key, quality_key])
                result['upload_time'] = time.perf_counter() - t_up
                
            result['status'] = 'success'
            self.state_manager.log_partition_status(result)
            self.invalidate_manifest_caches(date)
            
            # Requested: [SUCCESS] symbol stream date | files_in=N | rows=... | merge=..s | up=..s
            s_tag = Colors.colorate("[SUCCESS]", Colors.GREEN)
            timing_str = f"list={result.get('t_list',0):.1f}s | down={result.get('t_download',0):.1f}s | merge={result['merge_time']:.1f}s | up={result['upload_time']:.1f}s"
            fingerprint = f"sha256={metadata.get('sha256', 'N/A')[:8]}..."
            logger.info(f"{s_tag} {symbol} {stream} {date} | {fingerprint} | rows={result['rows']} | {timing_str}")

        except InterruptedError as e:
            result['status'] = 'aborted'
            result['error'] = 'Shutdown requested'
            logger.warning(Colors.colorate(f"UPLOAD SKIPPED due to shutdown: {symbol} {date}", Colors.RED))
            self.state_manager.log_partition_status(result, status='aborted')
            return result

        except InsufficientTempSpaceError as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            result['error_type'] = 'INSUFFICIENT_TMP_SPACE'
            logger.error(Colors.colorate(f"[FAILED] {symbol} {stream} {date} | {e}", Colors.RED))
            self.state_manager.log_partition_status(result, status='failed')
            return result
            
        except Exception as e:
            msg = str(e)
            result['error'] = msg
            result['stacktrace'] = traceback.format_exc()
            
            # Identify Error Type
            error_type, failure_status = classify_compaction_error(msg)
            result['error_type'] = error_type
            result['status'] = failure_status
            
            # Try to find the failing S3 key if path is in the error message
            failing_key = None
            for path_str, s3_key in self._path_to_s3_key.items():
                if path_str in msg:
                    failing_key = s3_key
                    break
            
            # Fallback: if no path in message, use the first file in local_files as a hint
            if not failing_key and local_files:
                failing_key = self._path_to_s3_key.get(str(local_files[0]))
                
            result['failing_key'] = failing_key
            
            # Generate Reproducer Cmd
            reproducer = "N/A"
            if failing_key:
                reproducer = (
                    f"python3 -c \"import boto3, pyarrow.parquet as pq, os; "
                    f"from dotenv import load_dotenv; load_dotenv('../.env'); "
                    f"s3 = boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT'), "
                    f"aws_access_key_id=os.getenv('S3_ACCESS_KEY'), aws_secret_access_key=os.getenv('S3_SECRET_KEY')); "
                    f"s3.download_file(os.getenv('S3_RAW_BUCKET', 'quantlab-raw'), '{failing_key}', 'repro.parquet'); "
                    f"pf = pq.ParquetFile('repro.parquet'); "
                    f"print('Rows:', pf.metadata.num_rows); "
                    f"print('Schema:', pf.schema_arrow)\""
                )
            result['reproducer_cmd'] = reproducer
            
            status_label = "[FAILED]" if failure_status == "failed" else "[QUARANTINE]"
            status_color = Colors.RED if failure_status == "failed" else Colors.YELLOW
            q_tag = Colors.colorate(status_label, status_color)
            logger.error(f"{q_tag} {symbol} {stream} {date} | ERROR_TYPE={error_type}")
            if failing_key:
                logger.error(f"  -> FAILING_RAW_KEY={failing_key}")
                logger.error(f"  -> REPRODUCER_CMD: {reproducer}")
            logger.error(f"  -> MSG: {msg}")
            
            try:
                self.state_manager.log_partition_status(result, status=failure_status)
            except:
                pass
            return result
        
        finally:
            # 4. RELEASE LOCK after terminal state is committed
            self.state_manager.release_lock(result)
            
        return result
    
    def _compact_exists(self, key: str) -> bool:
        try:
            self.s3_client_compact.head_object(Bucket=self.compact_bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def _list_raw_files(self, prefix: str) -> List[Dict]:
        """List files for a specific partition (date-bounded)"""
        files = []
        paginator = self.s3_client_raw.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.raw_bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('.parquet') or '/._' in key or key.split('/')[-1].startswith('._'):
                    continue
                files.append({'key': key, 'size': obj['Size']})
        return files
    
    def _download_files(self, files: List[Dict], download_dir: str, symbol: str, date: str) -> List[Path]:
        local_files = []
        download_path = Path(download_dir)
        self._path_to_s3_key = {}  # Reset for this partition
        
        def download_file(idx: int, key: str) -> Optional[Path]:
            try:
                filename = f"{idx:04d}_{Path(key).name}"
                local_path = download_path / filename
                self.s3_client_raw.download_file(Bucket=self.raw_bucket, Key=key, Filename=str(local_path))
                self._path_to_s3_key[str(local_path)] = key
                return local_path
            except Exception:
                return None
        
        with ThreadPoolExecutor(max_workers=self.max_parallel_downloads) as executor:
            futures = {executor.submit(download_file, idx, f['key']): idx for idx, f in enumerate(files)}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    local_files.append(result)
        
        return sorted(local_files)
    
    def _merge_parquet_files(self, input_files: List[Path], output_path: Path, stream: str) -> dict:
        """
        Merge parquet files using streaming k-way merge.
        
        Uses bounded memory: only one batch per file + output buffer.
        Produces deterministic output sorted by (ts_event, file_idx, row_idx).
        Adds seq column for stable replay ordering.
        
        Schema output: ts_event, seq, ts_recv, exchange, symbol, ...
        seq: 0 to (row_count - 1), monotonically increasing
        """
        stream_norm = (stream or "").lower()
        trade_plain_mode = stream_norm == "trade"
        merger = StreamingMergeWriter(
            input_files=input_files,
            output_path=output_path,
            check_shutdown=self.check_shutdown,
            decode_dictionaries=trade_plain_mode,
            force_plain_output=trade_plain_mode,
            force_disable_fastpath=trade_plain_mode
        )
        return merger.merge()
    
    def _upload_to_s3(self, local_path: Path, s3_key: str):
        self.s3_client_compact.upload_file(Filename=str(local_path), Bucket=self.compact_bucket, Key=s3_key)
    
    def _verify_output_integrity(self, path: Path, expected_rows: int):
        """Verify that output parquet is readable and has expected row count."""
        pf = None
        try:
            pf = pq.ParquetFile(path)
            actual_rows = 0
            for batch in pf.iter_batches(batch_size=100_000):
                actual_rows += batch.num_rows
            
            if actual_rows != expected_rows:
                raise ValueError(f"Row count mismatch: expected {expected_rows}, got {actual_rows}")
            
            # Verify footer integrity
            with open(path, 'rb') as f:
                f.seek(-4, 2)
                if f.read(4) != b'PAR1':
                    raise ValueError("Invalid parquet footer magic")
        except Exception as e:
            raise ValueError(f"Post-write verification failed: {e}") from e
        finally:
            if pf is not None:
                try:
                    pf.close()
                except Exception:
                    pass
    
    def _upload_json_to_s3(self, content: dict, s3_key: str):
        self.s3_client_compact.put_object(
            Bucket=self.compact_bucket,
            Key=s3_key,
            Body=json.dumps(content, indent=2).encode('utf-8'),
            ContentType='application/json'
        )

    def _finalize_artifacts(self, base_keys: List[str]):
        """Promote .tmp artifacts to final keys via copy+delete"""
        for key in base_keys:
            tmp_key = key + ".tmp"
            logger.info(f"Promoting {tmp_key} -> {key}")
            # Copy tmp to final
            self.s3_client_compact.copy_object(
                Bucket=self.compact_bucket,
                CopySource={'Bucket': self.compact_bucket, 'Key': tmp_key},
                Key=key
            )
            # Delete tmp
            self.s3_client_compact.delete_object(
                Bucket=self.compact_bucket,
                Key=tmp_key
            )

    def _fetch_quality_data(self, date_str: str) -> Dict:
        """Fetch all window JSONs for a date and aggregate quality"""
        cached = self._quality_cache.get(date_str)
        if cached is not None:
            return dict(cached)

        quality_prefix = f"quality/date={date_str}/"
        paginator = self.s3_client_raw.get_paginator('list_objects_v2')
        window_results = []
        
        for page in paginator.paginate(Bucket=self.raw_bucket, Prefix=quality_prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                if obj['Key'].endswith('.json'):
                    try:
                        resp = self.s3_client_raw.get_object(Bucket=self.raw_bucket, Key=obj['Key'])
                        window_json = json.loads(resp['Body'].read().decode('utf-8'))
                        assessment = QualityFilter.assess_window(window_json)
                        window_results.append(assessment)
                    except Exception as e:
                        logger.error(f"Error reading quality window {obj['Key']}: {e}")
        
        aggregated = QualityFilter.aggregate_day(window_results)
        self._quality_cache[date_str] = aggregated
        return dict(aggregated)

    def _ensure_temp_capacity(self, total_size_bytes: int):
        required_bytes = int(total_size_bytes * 2.2) + (2 * 1024 ** 3)
        free_bytes = shutil.disk_usage(self.temp_dir_base).free
        if free_bytes < required_bytes:
            raise InsufficientTempSpaceError(
                "Insufficient temporary space under "
                f"{self.temp_dir_base}: free={format_bytes(free_bytes)} "
                f"required={format_bytes(required_bytes)}"
            )

    def discover_dates(self) -> Set[str]:
        """
        Fast O(1) discovery of processed dates in raw bucket using delimiters.
        Walks: exchange=/ -> stream=/ -> symbol=/ -> date=/
        """
        logger.info("Discovering available dates in raw bucket...")
        dates = set()
        
        def list_prefixes(bucket: str, prefix: str) -> List[str]:
            paginator = self.s3_client_raw.get_paginator('list_objects_v2')
            prefixes = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
                for cp in page.get('CommonPrefixes', []):
                    prefixes.append(cp['Prefix'])
            return prefixes

        # Level 1: Exchange
        exchanges = list_prefixes(self.raw_bucket, "exchange=")
        for ex_prefix in exchanges:
            # Level 2: Stream
            streams = list_prefixes(self.raw_bucket, ex_prefix + "stream=")
            for st_prefix in streams:
                # Level 3: Symbol
                symbols = list_prefixes(self.raw_bucket, st_prefix + "symbol=")
                for sy_prefix in symbols:
                    # Level 4: Date
                    date_prefixes = list_prefixes(self.raw_bucket, sy_prefix + "date=")
                    for d_prefix in date_prefixes:
                        # Extract date from "exchange=.../date=YYYYMMDD/"
                        date_str = d_prefix.rstrip('/').split('=')[-1]
                        if len(date_str) == 8 and date_str.isdigit():
                            dates.add(date_str)
        
        return dates

    def discover_partitions_for_date(self, target_date: str) -> List[Dict]:
        """Find all partitions (ex/st/sy) for a specific date using delimiters"""
        partitions = []
        
        def list_prefixes(bucket: str, prefix: str) -> List[str]:
            paginator = self.s3_client_raw.get_paginator('list_objects_v2')
            prefixes = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
                for cp in page.get('CommonPrefixes', []):
                    prefixes.append(cp['Prefix'])
            return prefixes

        exchanges = list_prefixes(self.raw_bucket, "exchange=")
        for ex_prefix in exchanges:
            streams = list_prefixes(self.raw_bucket, ex_prefix + "stream=")
            for st_prefix in streams:
                symbols = list_prefixes(self.raw_bucket, st_prefix + "symbol=")
                for sy_prefix in symbols:
                    target_prefix = f"{sy_prefix}date={target_date}/"
                    # Check if this date exists for this symbol
                    paginator = self.s3_client_raw.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=self.raw_bucket, Prefix=target_prefix, MaxKeys=1):
                        if 'Contents' in page:
                            # Partition found
                            parts = sy_prefix.rstrip('/').split('/')
                            partition = {
                                'exchange': parts[0].split('=')[1],
                                'stream': parts[1].split('=')[1],
                                'symbol': parts[2].split('=')[1],
                                'date': target_date
                            }
                            partitions.append(partition)
                            break
        
        return partitions


def get_yesterday_date() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y%m%d')


def get_today_date() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%d')


def format_bytes(bytes_size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} TB"
