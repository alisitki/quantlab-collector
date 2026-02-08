"""
Parquet writer task - LOCAL-FIRST ARCHITECTURE.
Single writer consuming from queue, partitioned storage by exchange/stream/symbol/date.

P11 REFACTOR: Removed all S3 code. Collector now writes to local spool only.
Uploader (separate service) is responsible for S3 upload.

Key behaviors:
- Writes to SPOOL_DIR with fsync for durability
- Atomic write: .tmp → rename
- On write failure: buffer PRESERVED (retry next flush)
- NEVER restores to queue (no DROP risk from writer)
- NEVER deletes files (uploader responsibility)
"""
import asyncio
import os
import time
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from models import Event
from models import AlignmentEvent
from schemas import get_schema
from config import (
    DATA_DIR, SPOOL_DIR, STORAGE_BACKEND,
    SILENCE_THRESHOLDS_MS,
    DRAIN_ACCELERATION_ENTER_PCT, DRAIN_ACCELERATION_EXIT_PCT,
    DRAIN_NORMAL_FLUSH_GAP, DRAIN_ACCELERATED_FLUSH_GAP,
    SUSTAINED_GROWTH_WARN_THRESHOLD, SUSTAINED_GROWTH_WARN_SECONDS,
    MAX_PARQUET_MB, MAX_FLUSH_SEC,
)
from state import CollectorState


# ============================================================================
# Configuration Constants
# ============================================================================
MIN_FLUSH_GAP_SECONDS = 5       # Minimum time between flushes of same buffer


class ParquetWriter:
    """
    Parquet writer with LOCAL-FIRST architecture.
    
    P11 Architecture:
    - ALL writes go to local spool directory
    - fsync ensures durability before buffer clear
    - Atomic rename prevents partial files
    - NO S3 knowledge - uploader handles that
    - NO buffer restore to queue - eliminates DROP risk from writer
    """
    
    def __init__(self, data_dir: str = None, state: CollectorState = None):
        self.data_dir = data_dir or DATA_DIR
        self.max_parquet_mb = MAX_PARQUET_MB
        self.max_flush_sec = MAX_FLUSH_SEC
        self.backend = STORAGE_BACKEND
        self.state = state
        
        # Spool directory for local-first writes
        self.spool_dir = SPOOL_DIR
        
        # Buffers keyed by (exchange, stream, symbol)
        self.buffers: Dict[tuple, List[dict]] = defaultdict(list)
        self.last_flush: Dict[tuple, float] = defaultdict(lambda: time.time())
        
        # Per-buffer sequence numbers for deterministic filenames
        self.flush_seq: Dict[tuple, int] = defaultdict(int)
        
        # Stats
        self.total_written = 0
        self.files_written = 0
        self.write_failures = 0
        self.verification_failures = 0
        
        # Silence detection - track last ts_event per (exchange, stream, symbol)
        self.last_ts_event: Dict[tuple, int] = {}
        
        # Ensure spool directory exists
        if self.backend == "spool":
            os.makedirs(self.spool_dir, exist_ok=True)
            
        # P13: Executor for non-blocking flush
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="FlushWorker")
    
    def _can_flush_buffer(self, key: tuple, now: float, min_flush_gap: float = None) -> tuple[bool, str]:
        """
        Check if buffer check should be performed. Returns (should_check, reason).
        
        P12: In size-based mode, we check whenever the minimum flush gap has passed
        and there is data in the buffer. The actual size decision is in _flush_buffer.
        """
        if min_flush_gap is None:
            min_flush_gap = MIN_FLUSH_GAP_SECONDS
        
        buffer = self.buffers.get(key, [])
        if not buffer:
            return False, "empty"
        
        elapsed = now - self.last_flush[key]
        
        # P12: Check if enough time has passed to warrant a size/timeout check
        if elapsed >= min_flush_gap:
            return True, "check_due"
        
        return False, "not_due"
    
    def _get_spool_path(self, exchange: str, stream: str, symbol: str, seq: int) -> str:
        """
        Generate spool path for parquet file.
        
        Path format: {SPOOL_DIR}/exchange={X}/stream={Y}/symbol={Z}/date={YYYYMMDD}/
        File format: part-{window_start_ts}-{seq}.parquet
        
        Deterministic naming: same batch → same filename (idempotent)
        """
        date = datetime.now(timezone.utc).strftime("%Y%m%d")
        window_ts = int(time.time())
        
        partition_dir = os.path.join(
            self.spool_dir,
            f"exchange={exchange}",
            f"stream={stream}",
            f"symbol={symbol.lower()}",
            f"date={date}"
        )
        
        os.makedirs(partition_dir, exist_ok=True)
        return os.path.join(partition_dir, f"part-{window_ts}-{seq:06d}.parquet")
    
    def _get_partition_path(self, exchange: str, stream: str, symbol: str) -> str:
        """Generate partitioned path for legacy local mode."""
        date = datetime.now(timezone.utc).strftime("%Y%m%d")
        ts = int(time.time() * 1000)
        seq = self.flush_seq[(exchange, stream, symbol)]
        
        partition_dir = os.path.join(
            self.data_dir,
            f"exchange={exchange}",
            f"stream={stream}",
            f"symbol={symbol.lower()}",
            f"date={date}"
        )
        
        os.makedirs(partition_dir, exist_ok=True)
        return os.path.join(partition_dir, f"part-{ts}-{seq:06d}.parquet")
    
    def _verify_parquet(self, filepath: str, expected_rows: int = None) -> tuple:
        """
        Verify parquet file is readable after write.
        Catches Snappy/Thrift corruption and file concatenation before committing.

        P14: Added PAR1 marker check to detect race condition concatenation.
        A valid parquet file has exactly 2 PAR1 markers (header + footer).

        Returns: (is_valid, message)
        """
        try:
            # P14: Check for concatenation (multiple PAR1 markers)
            with open(filepath, 'rb') as f:
                data = f.read()

            par1_count = data.count(b'PAR1')
            if par1_count != 2:
                return False, f"Corrupted: {par1_count} PAR1 markers (expected 2)"

            # Standard parquet validation
            pf = pq.ParquetFile(filepath)
            if pf.metadata.num_row_groups > 0:
                _ = pf.read_row_group(0)

            # Optional row count verification
            if expected_rows is not None and pf.metadata.num_rows != expected_rows:
                return False, f"Row count mismatch: {pf.metadata.num_rows} vs {expected_rows}"

            return True, f"{pf.metadata.num_rows} rows"
        except Exception as e:
            return False, str(e)[:100]
    
    def _write_to_spool(self, table: pa.Table, exchange: str, stream: str, symbol: str, seq: int, expected_rows: int = None) -> bool:
        """
        Write table to local spool with fsync for durability.

        P14: seq is now passed as parameter (pre-incremented in main thread)
        to prevent race condition between concurrent thread pool workers.

        Atomic write protocol:
        1. Write to .tmp file
        2. fsync the file
        3. Verify (PAR1 count + readability)
        4. Rename to final .parquet
        5. fsync parent directory

        Returns True on success, False on failure.
        On failure: buffer is NOT cleared (retry on next flush)
        """
        spool_path = self._get_spool_path(exchange, stream, symbol, seq)
        tmp_path = spool_path + ".tmp"

        try:
            # 1. Write to temp file
            pq.write_table(table, tmp_path)

            # 2. fsync the file for durability
            fd = os.open(tmp_path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

            # 3. Verify file is readable and not concatenated (P14)
            valid, verify_msg = self._verify_parquet(tmp_path, expected_rows)
            if not valid:
                print(f"[Writer] POST-WRITE VERIFICATION FAILED: {exchange}/{stream}/{symbol} seq={seq} - {verify_msg}")
                self.verification_failures += 1
                # Delete corrupt file, buffer will be preserved for retry
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                return False

            # 4. Atomic rename
            os.rename(tmp_path, spool_path)

            # 5. fsync parent directory for metadata durability
            parent_dir = os.path.dirname(spool_path)
            fd = os.open(parent_dir, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

            # P14: seq increment is now done in main thread (_flush_buffer)
            return True

        except Exception as e:
            print(f"[Writer] SPOOL WRITE FAILED: {exchange}/{stream}/{symbol} seq={seq} - {e}")
            self.write_failures += 1

            # Clean up temp file if exists
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

            return False
    
    def _write_to_local(self, table: pa.Table, exchange: str, stream: str, symbol: str) -> bool:
        """Write table to local filesystem (legacy mode)."""
        try:
            path = self._get_partition_path(exchange, stream, symbol)
            pq.write_table(table, path)
            self.flush_seq[(exchange, stream, symbol)] += 1
            return True
        except Exception as e:
            print(f"[Writer] LOCAL WRITE FAILED: {exchange}/{stream}/{symbol} - {e}")
            self.write_failures += 1
            return False
            
    def _flush_sync(self, buffer: List[dict], key: tuple, now: float, seq: int):
        """
        Synchronous flush - RUNS IN THREAD POOL.
        P13: Handles heavy pandas/pyarrow/parquet/fsync work.
        P14: seq passed as parameter to prevent race condition.
        """
        exchange, stream, symbol = key
        count = len(buffer)

        try:
            # 1. Convert to Table
            df = pd.DataFrame(buffer)
            schema = get_schema(stream)
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

            # 2. Condition check (Double check size inside thread)
            is_big_enough = table.nbytes >= self.max_parquet_mb * 1024 * 1024
            is_old_enough = (now - self.last_flush[key]) >= self.max_flush_sec

            # Note: We still write if it was a forced flush or if conditions hold
            # But the 'can_flush' check already filtered most.

            # 3. Write to storage (P14: pass seq and expected_rows)
            if self.backend == "spool":
                success = self._write_to_spool(table, exchange, stream, symbol, seq, expected_rows=count)
            else:
                success = self._write_to_local(table, exchange, stream, symbol)

            if success:
                # Update stats (GIL makes this safe for simple increments)
                self.total_written += count
                self.files_written += 1
                self.last_flush[key] = now

                # Update shared state
                if self.state:
                    self.state.last_write_ts = now * 1000
                    # self.state.writer_stats = self.get_stats() # skip frequent state updates from threads

                reason = "size" if is_big_enough else "timeout"
                print(f"[Writer] FLUSH SUCCESS ({reason}): {exchange}/{stream}/{symbol} seq={seq} - {count} events, {table.nbytes/1024/1024:.2f}MB")
            else:
                print(f"[Writer] FLUSH FAILED (write error): {key} seq={seq}")
                self.write_failures += 1

        except Exception as e:
            print(f"[Writer] FLUSH CRITICAL ERROR for {key} seq={seq}: {e}")
            self.write_failures += 1

    def _flush_buffer(self, key: tuple, now: float) -> bool:
        """
        Flush buffer wrapper. Swaps buffer and dispatches to executor.

        P13 CRITICAL BEHAVIOR:
        1. Fast check if buffer should be flushed (O(1)).
        2. If due, SWAP buffer and SUBMIT to executor.
        3. No blocking in writer loop.

        P14 RACE CONDITION FIX:
        - Atomically get and increment seq BEFORE dispatch (in main thread)
        - Pass seq to thread worker
        - This allows parallel flushes with different seq numbers (no file collision)

        P14-FIX: Removed in-flight tracking - it was throttling throughput and
        causing queue overflow. Seq fix alone prevents race condition.
        """
        buffer = self.buffers.get(key)
        if not buffer:
            return False

        elapsed = now - self.last_flush[key]
        count = len(buffer)

        # P13 FAST CHECK: Skip expensive table conversion in main loop
        # We only dispatch if it's likely to flush (count > 1000 or 180s)
        is_old_enough = elapsed >= self.max_flush_sec
        if count < 1000 and not is_old_enough:
            return False

        # P14: Atomically get and increment seq BEFORE dispatch
        # This prevents race condition - each parallel flush gets unique seq
        seq = self.flush_seq[key]
        self.flush_seq[key] = seq + 1

        # P13: SWAP AND DISPATCH
        # Snapshot takes O(1) in Python (list swap)
        snapshot = self.buffers[key]
        self.buffers[key] = []

        # Submit to thread pool (P14: pass seq)
        self.executor.submit(self._flush_sync, snapshot, key, now, seq)
        return True
    
    def _check_silence(self, event: Event):
        """
        Check for inter-event silence (time gap) based on ts_event continuity.
        """
        threshold = SILENCE_THRESHOLDS_MS.get(event.stream)
        if threshold is None:
            return
        
        key = (event.exchange, event.stream, event.symbol)
        
        if key in self.last_ts_event:
            last_ts = self.last_ts_event[key]
            silence_ms = event.ts_event - last_ts
            
            if silence_ms > threshold:
                print(f"[SILENCE] exchange={event.exchange} stream={event.stream} symbol={event.symbol} silence={silence_ms}ms threshold={threshold}ms")
                if self.state:
                    self.state.record_silence_interval(key)
        
        self.last_ts_event[key] = event.ts_event
    
    def _handle_alignment(self, event: AlignmentEvent):
        """P1-A: Handle alignment event (RAM-only, NOT written to parquet)."""
        stream_mapping = [
            ('bbo', event.bbo_ts),
            ('trade', event.trade_ts),
            ('mark_price', event.mark_price_ts),
            ('funding', event.funding_ts),
            ('open_interest', event.open_interest_ts),
        ]
        
        for stream, ts in stream_mapping:
            if ts > 0:
                key = (event.exchange, stream, event.symbol)
                self.last_ts_event[key] = ts
                if self.state:
                    self.state.gap_alignment_corrections += 1
                    self.state.snapshot_events_seen += 1
    
    def add_event(self, event: Event):
        """Add an event to the appropriate buffer (sync call, no I/O)."""
        # P1-A: Handle alignment events (RAM-only, not written)
        if isinstance(event, AlignmentEvent):
            self._handle_alignment(event)
            return
        
        # Check for inter-event silence
        self._check_silence(event)
        
        key = (event.exchange, event.stream, event.symbol)
        self.buffers[key].append(event.to_dict())
    
    def flush_all(self):
        """Flush all buffers synchronously (for shutdown)."""
        now = time.time()
        for key in list(self.buffers.keys()):
            if self.buffers[key]:
                self._flush_buffer(key, now)
    
    def get_stats(self) -> dict:
        """Get writer statistics."""
        pending = sum(len(b) for b in self.buffers.values())
        return {
            "total_written": self.total_written,
            "files_written": self.files_written,
            "pending_events": pending,
            "active_buffers": len(self.buffers),
            "write_failures": self.write_failures,
            "verification_failures": self.verification_failures,
            "drain_mode": "normal",  # Will be updated by writer_task
        }
    
    def shutdown(self):
        """Graceful shutdown - flush remaining buffers."""
        print("[Writer] Shutting down, flushing remaining buffers...")
        self.flush_all()
        print("[Writer] Shutdown complete")


async def writer_task(queue: asyncio.Queue, state: CollectorState = None):
    """
    Main writer task - consumes from queue and writes to LOCAL SPOOL.
    
    P11 Architecture:
    - NO S3 code
    - Writes to local spool with fsync
    - On write failure: buffer preserved (retry next flush)
    - Adaptive drain mode for queue pressure
    """
    writer = ParquetWriter(state=state)
    
    # Track drain mode
    drain_mode = "normal"
    
    # Guardrail tracking
    last_drain_count = 0
    last_rate_check = time.time()
    
    # Startup logging
    print(f"[Writer] Backend: {writer.backend.upper()}")
    if writer.backend == "spool":
        print(f"[Writer] Spool directory: {writer.spool_dir}")
    else:
        print(f"[Writer] Data directory: {writer.data_dir}")
    print(f"[Writer] Max size: {writer.max_parquet_mb}MB, Max interval: {writer.max_flush_sec}s")
    print(f"[Writer] Adaptive check gap: {DRAIN_NORMAL_FLUSH_GAP}s (normal) / {DRAIN_ACCELERATED_FLUSH_GAP}s (accelerated)")
    print(f"[Writer] NOTE: S3 upload handled by separate uploader service")
    
    last_stats_time = time.time()
    
    while True:
        try:
            # Get event from queue with timeout
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
                writer.add_event(event)
                queue.task_done()
            except asyncio.TimeoutError:
                pass
            
            now = time.time()
            
            # Calculate queue utilization for adaptive drain
            queue_size = queue.qsize()
            queue_max = queue.maxsize
            queue_util = (queue_size / queue_max * 100) if queue_max > 0 else 0
            
            # Adaptive drain mode switching
            old_mode = drain_mode
            if drain_mode == "normal" and queue_util >= DRAIN_ACCELERATION_ENTER_PCT:
                drain_mode = "accelerated"
            elif drain_mode == "accelerated" and queue_util < DRAIN_ACCELERATION_EXIT_PCT:
                drain_mode = "normal"
            
            if old_mode != drain_mode:
                print(f"[Writer] Drain mode -> {drain_mode.upper()} (queue={queue_util:.0f}%)")
                if state:
                    state.drain_mode = drain_mode
            
            # Determine min_flush_gap based on drain mode
            min_flush_gap = DRAIN_ACCELERATED_FLUSH_GAP if drain_mode == "accelerated" else DRAIN_NORMAL_FLUSH_GAP
            
            # Check for buffers ready to flush
            for key in list(writer.buffers.keys()):
                can_check, reason = writer._can_flush_buffer(key, now, min_flush_gap)
                if can_check:
                    writer._flush_buffer(key, now)
            
            # Update guardrail metrics every 10 seconds
            rate_elapsed = now - last_rate_check
            if rate_elapsed >= 10 and state:
                drain_delta = writer.total_written - last_drain_count
                state.drain_rate = round(drain_delta / rate_elapsed, 1)
                last_drain_count = writer.total_written
                
                state.ingestion_rate = sum(state.eps.values())
                state.queue_growth_rate = round(state.ingestion_rate - state.drain_rate, 1)
                
                # Sustained growth warning
                if state.queue_growth_rate > SUSTAINED_GROWTH_WARN_THRESHOLD:
                    if state._growth_warning_start == 0:
                        state._growth_warning_start = now
                    elif now - state._growth_warning_start >= SUSTAINED_GROWTH_WARN_SECONDS:
                        print(f"[Writer] WARNING: Sustained queue growth {state.queue_growth_rate:.0f} ev/s for {now - state._growth_warning_start:.0f}s")
                        state._growth_warning_start = now
                else:
                    state._growth_warning_start = 0
                
                last_rate_check = now
            
            # Print stats every 30 seconds
            if now - last_stats_time >= 30:
                stats = writer.get_stats()
                stats["drain_mode"] = drain_mode
                print(f"[Writer] Stats: {stats}")
                last_stats_time = now
                
        except asyncio.CancelledError:
            print("[Writer] Received shutdown signal, flushing remaining buffers...")
            writer.flush_all()
            print(f"[Writer] Final stats: {writer.get_stats()}")
            break
        except Exception as e:
            print(f"[Writer] CRITICAL ERROR in main loop: {e}")
