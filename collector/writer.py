"""
Parquet writer task.
Single writer consuming from queue, partitioned storage by exchange/stream/symbol/date.
Supports both local filesystem and S3-compatible object storage.

P3 FIX: Uses ThreadPoolExecutor for S3 uploads to prevent event loop blocking.
P3 HARDENING: Production-grade concurrency control, lifecycle management, and error handling.
"""
import asyncio
import os
import time
import uuid
import random
import tempfile
import shutil
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
    DATA_DIR, BUFFER_SIZE, FLUSH_INTERVAL,
    STORAGE_BACKEND, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_PREFIX
)
from state import CollectorState


# ============================================================================
# P3 HARDENING: Configuration Constants
# ============================================================================
MAX_UPLOAD_WORKERS = 8          # ThreadPoolExecutor max workers
MAX_IN_FLIGHT_UPLOADS = 16      # Semaphore limit for concurrent flushes
MIN_FLUSH_GAP_SECONDS = 5       # Minimum time between flushes of same buffer


# Lazy import boto3 only when needed
_s3_client = None


def _get_s3_client():
    """Get or create S3 client (lazy initialization)."""
    global _s3_client
    if _s3_client is None:
        import boto3
        _s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )
    return _s3_client


class ParquetWriter:
    """
    Parquet writer with async S3 support and production hardening.
    
    P3 Architecture:
    - Synchronous operations (DataFrame conversion, Parquet serialization) run inline
    - S3 uploads are offloaded to ThreadPoolExecutor via run_in_executor
    - Bounded concurrency via asyncio.Semaphore (MAX_IN_FLIGHT_UPLOADS)
    - Explicit exception handling with failure metrics
    - Per-buffer minimum flush gap prevents hot-stream flush storms
    """
    
    def __init__(self, data_dir: str = None, buffer_size: int = None, flush_interval: int = None, state: CollectorState = None):
        self.data_dir = data_dir or DATA_DIR
        self.buffer_size = buffer_size or BUFFER_SIZE
        self.flush_interval = flush_interval or FLUSH_INTERVAL
        self.backend = STORAGE_BACKEND
        self.state = state

        # S3 settings
        self.s3_bucket = S3_BUCKET
        self.s3_prefix = S3_PREFIX
        
        # Temp directory for atomic S3 writes
        self.temp_dir = "/tmp/collector"
        if self.backend == "s3":
            os.makedirs(self.temp_dir, exist_ok=True)
        
        # Buffers keyed by (exchange, stream, symbol)
        self.buffers: Dict[tuple, List[dict]] = defaultdict(list)
        self.last_flush: Dict[tuple, float] = defaultdict(lambda: time.time())
        
        # P3 HARDENING FIX 4: Per-buffer stagger offsets for flush burst protection
        self.flush_stagger_offsets: Dict[tuple, float] = {}
        
        # P3 HARDENING FIX 3: Explicit ThreadPoolExecutor lifecycle
        self._executor: Optional[ThreadPoolExecutor] = None
        if self.backend == "s3":
            self._executor = ThreadPoolExecutor(
                max_workers=MAX_UPLOAD_WORKERS,
                thread_name_prefix="s3_upload"
            )
        
        # P3 HARDENING FIX 2: Semaphore for bounded flush concurrency
        self._flush_sem: Optional[asyncio.Semaphore] = None  # Created lazily in async context
        
        # Stats
        self.total_written = 0
        self.files_written = 0
        self.in_flight_uploads = 0
        
        # P3 HARDENING FIX 5: Failure tracking
        self.upload_failures = 0
        
        # P0: Gap detection - track last ts_event per (exchange, stream, symbol)
        self.last_ts_event: Dict[tuple, int] = {}
        
        # Gap thresholds (milliseconds)
        self.gap_thresholds = {
            'bbo': 5000,
            'trade': 5000,
            'mark_price': 10000,
            'funding': 60000,
            'open_interest': 60000,
        }
    
    def _get_flush_sem(self) -> asyncio.Semaphore:
        """Get or create flush semaphore (must be called from async context)."""
        if self._flush_sem is None:
            self._flush_sem = asyncio.Semaphore(MAX_IN_FLIGHT_UPLOADS)
        return self._flush_sem
    
    def _get_stagger_offset(self, key: tuple) -> float:
        """Get stagger offset for a buffer key to prevent flush bursts."""
        if key not in self.flush_stagger_offsets:
            # Random offset between 0 and half the flush interval
            self.flush_stagger_offsets[key] = random.uniform(0, self.flush_interval / 2)
        return self.flush_stagger_offsets[key]
    
    def _can_flush_buffer(self, key: tuple, now: float) -> tuple[bool, str]:
        """
        Check if buffer can be flushed. Returns (can_flush, reason).
        
        P3 HARDENING FIX 4: Enforces minimum flush gap to prevent hot-stream flush storms.
        A hot stream (e.g., BTCUSDT BBO) could otherwise flush back-to-back under load.
        """
        buffer = self.buffers.get(key, [])
        if not buffer:
            return False, "empty"
        
        elapsed = now - self.last_flush[key]
        
        # Size-based flush (high priority, but still respect minimum gap)
        if len(buffer) >= self.buffer_size:
            if elapsed >= MIN_FLUSH_GAP_SECONDS:
                return True, "size"
            return False, "min_gap"
        
        # Time-based flush (staggered)
        stagger = self._get_stagger_offset(key)
        if elapsed - stagger >= self.flush_interval:
            return True, "time"
        
        return False, "not_due"
        
    def _get_partition_path(self, exchange: str, stream: str, symbol: str) -> str:
        """Generate partitioned path for local output file."""
        date = datetime.now(timezone.utc).strftime("%Y%m%d")
        ts = int(time.time() * 1000)
        file_uuid = uuid.uuid4().hex[:8]
        
        partition_dir = os.path.join(
            self.data_dir,
            f"exchange={exchange}",
            f"stream={stream}",
            f"symbol={symbol.lower()}",
            f"date={date}"
        )
        
        os.makedirs(partition_dir, exist_ok=True)
        return os.path.join(partition_dir, f"part-{ts}-{file_uuid}.parquet")
    
    def _get_s3_object_key(self, exchange: str, stream: str, symbol: str) -> str:
        """Generate S3 object key with guaranteed uniqueness."""
        date = datetime.now(timezone.utc).strftime("%Y%m%d")
        ts = int(time.time() * 1000)
        file_uuid = uuid.uuid4().hex[:8]
        
        path_parts = [
            f"exchange={exchange}",
            f"stream={stream}",
            f"symbol={symbol.lower()}",
            f"date={date}",
            f"part-{ts}-{file_uuid}.parquet"
        ]
        
        if self.s3_prefix:
            return f"{self.s3_prefix.strip('/')}/{'/'.join(path_parts)}"
        return "/".join(path_parts)
    
    def _prepare_flush_data(self, key: tuple) -> Optional[tuple]:
        """
        Prepare data for flush (runs synchronously, but fast).
        Returns (table, exchange, stream, symbol, count, buffer_snapshot) or None.
        
        P3 HARDENING FIX 5: Returns buffer_snapshot for recovery on failure.
        """
        buffer = self.buffers.get(key)
        if not buffer:
            return None
        
        exchange, stream, symbol = key
        count = len(buffer)
        
        # P3 HARDENING FIX 5: Keep snapshot for recovery
        buffer_snapshot = list(buffer)
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(buffer)
            
            # Get schema for this stream type
            schema = get_schema(stream)
            
            # Create PyArrow table
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            
            # Clear buffer immediately (data is now in table)
            self.buffers[key].clear()
            self.last_flush[key] = time.time()
            
            return (table, exchange, stream, symbol, count, buffer_snapshot)
            
        except Exception as e:
            print(f"[Writer] Error preparing flush for {key}: {e}")
            return None
    
    def _write_to_s3_sync(self, table: pa.Table, exchange: str, stream: str, symbol: str) -> bool:
        """
        Synchronous S3 upload - runs in ThreadPoolExecutor.
        
        This method contains the blocking I/O that previously blocked the event loop.
        Now it runs in a separate thread, keeping the event loop responsive.
        """
        temp_path = None
        try:
            # 1. Write to temp file
            temp_uuid = uuid.uuid4().hex
            temp_path = os.path.join(self.temp_dir, f"{temp_uuid}.parquet")
            pq.write_table(table, temp_path)
            
            # 2. Upload to S3
            object_key = self._get_s3_object_key(exchange, stream, symbol)
            s3 = _get_s3_client()
            
            with open(temp_path, "rb") as f:
                s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=object_key,
                    Body=f
                )
            
            return True
            
        except Exception as e:
            # P3 HARDENING FIX 5: Log explicitly (will be caught by caller)
            print(f"[Writer] S3 UPLOAD FAILED: {exchange}/{stream}/{symbol} - {e}")
            raise  # Re-raise so caller can handle recovery
            
        finally:
            # 3. Always clean up temp file
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
    
    async def _write_to_s3_async(self, table: pa.Table, exchange: str, stream: str, symbol: str) -> bool:
        """
        Async wrapper for S3 upload - offloads to ThreadPoolExecutor.
        
        P3 HARDENING FIX 1: Uses get_running_loop() instead of get_event_loop().
        get_event_loop() is deprecated and can fail in nested loop scenarios.
        """
        # FIX 1: get_running_loop() is safe in async context, get_event_loop() is not
        loop = asyncio.get_running_loop()
        
        return await loop.run_in_executor(
            self._executor,
            self._write_to_s3_sync,
            table, exchange, stream, symbol
        )
    
    async def _flush_buffer_async(self, key: tuple) -> bool:
        """
        Async flush with semaphore-bounded concurrency.
        
        P3 HARDENING FIX 2: Semaphore strictly enforces MAX_IN_FLIGHT_UPLOADS.
        P3 HARDENING FIX 5: Failed uploads restore buffer data (no silent loss).
        """
        flush_data = self._prepare_flush_data(key)
        if flush_data is None:
            return False
        
        table, exchange, stream, symbol, count, buffer_snapshot = flush_data
        
        # FIX 2: Semaphore-guarded upload
        async with self._get_flush_sem():
            self.in_flight_uploads += 1
            try:
                if self.backend == "s3":
                    success = await self._write_to_s3_async(table, exchange, stream, symbol)
                else:
                    # Local writes are fast, keep synchronous
                    try:
                        self._write_to_local(table, exchange, stream, symbol)
                        success = True
                    except Exception as e:
                        print(f"[Writer] Local write error for {key}: {e}")
                        success = False
                
                if success:
                    self.total_written += count
                    self.files_written += 1
                    
                    # Update shared state
                    if self.state:
                        self.state.last_write_ts = time.time() * 1000
                        self.state.writer_stats = self.get_stats()
                
                return success
                
            except Exception as e:
                # P3 HARDENING FIX 5: Restore buffer on failure (no data loss)
                print(f"[Writer] FLUSH FAILED for {key}, restoring {count} events: {e}")
                self.upload_failures += 1
                
                # Restore events to buffer
                self.buffers[key].extend(buffer_snapshot)
                
                # Update failure metric in state
                if self.state:
                    self.state.writer_stats = self.get_stats()
                
                return False
                
            finally:
                self.in_flight_uploads -= 1
    
    def _flush_buffer(self, key: tuple):
        """
        Synchronous flush - ONLY for local backend or shutdown.
        For S3, use _flush_buffer_async() instead.
        """
        buffer = self.buffers.get(key)
        if not buffer:
            return
        
        exchange, stream, symbol = key
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(buffer)
            
            # Get schema for this stream type
            schema = get_schema(stream)
            
            # Create PyArrow table
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            
            if self.backend == "s3":
                self._write_to_s3_sync(table, exchange, stream, symbol)
            else:
                self._write_to_local(table, exchange, stream, symbol)
            
            # Update stats
            count = len(buffer)
            self.total_written += count
            self.files_written += 1
            
            # Clear buffer
            self.buffers[key].clear()
            self.last_flush[key] = time.time()
            
            # Update shared state
            if self.state:
                self.state.last_write_ts = time.time() * 1000
                self.state.writer_stats = self.get_stats()
            
        except Exception as e:
            print(f"[Writer] SYNC FLUSH FAILED for {key}: {e}")
            self.upload_failures += 1
    
    def _write_to_local(self, table: pa.Table, exchange: str, stream: str, symbol: str):
        """Write table to local filesystem."""
        path = self._get_partition_path(exchange, stream, symbol)
        pq.write_table(table, path)
    
    def _check_gap(self, event: Event):
        """P0: Check for data gaps based on ts_event continuity."""
        key = (event.exchange, event.stream, event.symbol)
        
        if key in self.last_ts_event:
            last_ts = self.last_ts_event[key]
            gap = event.ts_event - last_ts
            threshold = self.gap_thresholds.get(event.stream, 10000)
            
            if gap > threshold:
                print(f"[GAP DETECTED] exchange={event.exchange} stream={event.stream} symbol={event.symbol} gap={gap}ms threshold={threshold}ms")
                if self.state:
                    self.state.gaps_detected += 1
        
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
            return  # Do NOT write to parquet
        
        # P0: Check for gaps
        self._check_gap(event)
        
        key = (event.exchange, event.stream, event.symbol)
        self.buffers[key].append(event.to_dict())
        
        # Update shared state
        if self.state:
            self.state.update_event(event.stream)
        
        # NOTE: Flush is handled by writer_task, not here (prevents blocking)
    
    def flush_all(self):
        """Flush all buffers synchronously (for shutdown only)."""
        for key in list(self.buffers.keys()):
            if self.buffers[key]:
                self._flush_buffer(key)
    
    def get_stats(self) -> dict:
        """Get writer statistics."""
        pending = sum(len(b) for b in self.buffers.values())
        return {
            "total_written": self.total_written,
            "files_written": self.files_written,
            "pending_events": pending,
            "active_buffers": len(self.buffers),
            "in_flight_uploads": self.in_flight_uploads,
            "upload_failures": self.upload_failures,  # P3 HARDENING: Visible failure count
        }
    
    def cleanup_temp(self):
        """Clean up temp directory on shutdown."""
        if self.backend == "s3" and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                os.makedirs(self.temp_dir, exist_ok=True)
            except Exception:
                pass
    
    def shutdown(self):
        """
        P3 HARDENING FIX 3: Graceful shutdown of ThreadPoolExecutor.
        Called on service stop / SIGTERM. Waits for in-flight uploads to complete.
        """
        if self._executor:
            print("[Writer] Shutting down ThreadPoolExecutor (waiting for in-flight uploads)...")
            self._executor.shutdown(wait=True)
            self._executor = None
            print("[Writer] ThreadPoolExecutor shutdown complete")


async def writer_task(queue: asyncio.Queue, state: CollectorState = None):
    """
    Main writer task - consumes from queue and writes to storage.
    
    P3 Architecture:
    - Main loop processes queue events without blocking
    - Flushes are semaphore-bounded (MAX_IN_FLIGHT_UPLOADS)
    - S3 uploads run in ThreadPoolExecutor, never blocking the event loop
    - Staggered flush scheduling + min flush gap prevents flush bursts
    - Explicit exception handling ensures no silent data loss
    """
    writer = ParquetWriter(state=state)
    
    # Startup logging
    if writer.backend == "s3":
        print(f"[Writer] Backend: S3 (async with ThreadPoolExecutor, max {MAX_UPLOAD_WORKERS} workers)")
        print(f"[Writer] Bucket: {writer.s3_bucket}")
        print(f"[Writer] Endpoint: {S3_ENDPOINT}")
        print(f"[Writer] Prefix: {writer.s3_prefix}")
        print(f"[Writer] Buffer size: {writer.buffer_size}, Flush interval: {writer.flush_interval}s")
        print(f"[Writer] Max in-flight: {MAX_IN_FLIGHT_UPLOADS}, Min flush gap: {MIN_FLUSH_GAP_SECONDS}s")
    else:
        print(f"[Writer] Backend: Local")
        print(f"[Writer] Path: {writer.data_dir}")
    
    last_stats_time = time.time()
    
    while True:
        try:
            # Get event from queue with timeout for periodic flushing
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
                writer.add_event(event)
                queue.task_done()
            except asyncio.TimeoutError:
                pass
            
            # Check for buffers ready to flush
            now = time.time()
            flush_tasks = []
            
            for key in list(writer.buffers.keys()):
                can_flush, reason = writer._can_flush_buffer(key, now)
                if can_flush:
                    flush_tasks.append(writer._flush_buffer_async(key))
            
            # P3 HARDENING FIX 2 & 5: Execute with explicit exception handling
            if flush_tasks:
                results = await asyncio.gather(*flush_tasks, return_exceptions=True)
                
                # FIX 5: Log any unexpected exceptions (not already handled inside _flush_buffer_async)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        print(f"[Writer] UNEXPECTED ERROR in flush task {i}: {result}")
                        writer.upload_failures += 1
            
            # Print stats every 30 seconds
            if now - last_stats_time >= 30:
                stats = writer.get_stats()
                print(f"[Writer] Stats: {stats}")
                last_stats_time = now
                
        except asyncio.CancelledError:
            # FIX 3: Graceful shutdown sequence
            print("[Writer] Received shutdown signal, flushing remaining buffers...")
            
            # Synchronous flush to ensure all data is written
            writer.flush_all()
            writer.cleanup_temp()
            
            # Shutdown executor gracefully
            writer.shutdown()
            
            print(f"[Writer] Final stats: {writer.get_stats()}")
            break
        except Exception as e:
            print(f"[Writer] CRITICAL ERROR in main loop: {e}")
