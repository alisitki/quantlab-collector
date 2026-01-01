"""
Parquet writer task.
Single writer consuming from queue, partitioned storage by exchange/stream/symbol/date.
Supports both local filesystem and S3-compatible object storage.
"""
import asyncio
import os
import time
import uuid
import tempfile
import shutil
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Optional

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
        
        # Stats
        self.total_written = 0
        self.files_written = 0
        
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
    
    def _should_flush(self, key: tuple) -> bool:
        """Check if buffer should be flushed."""
        buffer = self.buffers.get(key, [])
        if not buffer:
            return False
        
        if len(buffer) >= self.buffer_size:
            return True
        
        elapsed = time.time() - self.last_flush[key]
        if elapsed >= self.flush_interval:
            return True
        
        return False
    
    def _flush_buffer(self, key: tuple):
        """Flush a single buffer to storage (local or S3)."""
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
                self._write_to_s3(table, exchange, stream, symbol)
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
            print(f"[Writer] Error writing {key}: {e}")
    
    def _write_to_local(self, table: pa.Table, exchange: str, stream: str, symbol: str):
        """Write table to local filesystem."""
        path = self._get_partition_path(exchange, stream, symbol)
        pq.write_table(table, path)
    
    def _write_to_s3(self, table: pa.Table, exchange: str, stream: str, symbol: str):
        """
        Atomic write to S3:
        1. Write parquet to local temp file
        2. Upload to S3
        3. Delete temp file
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
            
            print(f"[Writer] STARTING UPLOAD to s3://{self.s3_bucket}/{object_key}")
            with open(temp_path, "rb") as f:
                s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=object_key,
                    Body=f
                )
            
            # Debug log
            print(f"[Writer] COMPLETED UPLOAD to s3://{self.s3_bucket}/{object_key}")
            
        finally:
            # 3. Always clean up temp file
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
    
    
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
        """Add an event to the appropriate buffer."""
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
            
        if self._should_flush(key):

            self._flush_buffer(key)
    
    def flush_all(self):
        """Flush all buffers."""
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
        }
    
    def cleanup_temp(self):
        """Clean up temp directory on shutdown."""
        if self.backend == "s3" and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                os.makedirs(self.temp_dir, exist_ok=True)
            except Exception:
                pass


async def writer_task(queue: asyncio.Queue, state: CollectorState = None):
    """Main writer task - consumes from queue and writes to storage."""
    writer = ParquetWriter(state=state)

    
    # Startup logging
    if writer.backend == "s3":
        print(f"[Writer] Backend: S3")
        print(f"[Writer] Bucket: {writer.s3_bucket}")
        print(f"[Writer] Endpoint: {S3_ENDPOINT}")
        print(f"[Writer] Prefix: {writer.s3_prefix}")
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
            
            # Check for time-based flush
            now = time.time()
            for key in list(writer.buffers.keys()):
                if writer._should_flush(key):
                    writer._flush_buffer(key)
            
            # Print stats every 30 seconds
            if now - last_stats_time >= 30:
                stats = writer.get_stats()
                print(f"[Writer] Stats: {stats}")
                last_stats_time = now
                
        except asyncio.CancelledError:
            print("[Writer] Shutting down, flushing remaining buffers...")
            writer.flush_all()
            writer.cleanup_temp()
            print(f"[Writer] Final stats: {writer.get_stats()}")
            break
        except Exception as e:
            print(f"[Writer] Error: {e}")
