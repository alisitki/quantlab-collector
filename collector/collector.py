#!/usr/bin/env python3
"""
Multi-Exchange Futures Data Collector

Collects real-time data from Binance, Bybit, and OKX:
- BBO (Best Bid/Offer)
- Trades
- Open Interest (Bybit, OKX only - Binance doesn't provide via WebSocket)
- Funding Rate
- Mark Price

All data is written to Parquet files partitioned by exchange/stream/symbol/date.
"""
import argparse
import asyncio
import os
import signal
import sys
import time

from config import SYMBOLS, QUEUE_MAXSIZE
from binance_handler import binance_ws_task
from bybit_handler import bybit_ws_task
from okx_handler import okx_ws_task
from writer import writer_task
from status_api import status_api_task
from state import CollectorState
from json_logger import json_log



class Collector:
    def __init__(self, symbols: list[str] = None):
        self.symbols = symbols or SYMBOLS
        self.queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self.tasks = []
        self.running = False
        self.state = CollectorState()
        self.state.take_window_snapshot() # P9: Initialize window base
        self.start_time = self.state.start_time

        
    async def start(self):
        """Start all tasks."""
        self.running = True
        self.start_time = time.time()
        
        print("=" * 60)
        print("Multi-Exchange Futures Data Collector")
        print("=" * 60)
        
        # Parsed Symbols Log
        print(f"[Config] Parsed symbols ({len(self.symbols)}):")
        for sym in self.symbols:
            print(f"  - {sym}")
        
        print(f"[Config] Queue size: {QUEUE_MAXSIZE}")
        print("=" * 60)
        
        # Create tasks
        # P2: Log collector start
        json_log("INFO", "collector_start", details={"symbols": self.symbols or "all"})
        
        self.tasks = [
            asyncio.create_task(binance_ws_task(self.queue, self.symbols, self.state), name="binance"),
            asyncio.create_task(bybit_ws_task(self.queue, self.symbols, self.state), name="bybit"),
            asyncio.create_task(okx_ws_task(self.queue, self.symbols, self.state), name="okx"),
            asyncio.create_task(writer_task(self.queue, state=self.state), name="writer"),
            asyncio.create_task(status_api_task(self.state, self.queue), name="api"),
            asyncio.create_task(self._collector_meta_log(), name="meta"),
            asyncio.create_task(self._heartbeat_task(), name="heartbeat"),  # P2
            asyncio.create_task(self._quality_ledger_task(), name="ledger"), # P9
        ]

        
        # Wait for all tasks
        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            pass
    
    async def _heartbeat_task(self):
        """P2: Emit heartbeat every 30s with state and metrics. P8: Uses data-based ws_connected."""
        import psutil
        from config import HEARTBEAT_TIMEOUT
        pid = os.getpid()
        
        while self.running:
            await asyncio.sleep(30)
            
            # P8: Derive ws_connected from actual message timestamps (truthful)
            ws_connected = self.state.get_ws_connected(HEARTBEAT_TIMEOUT)
            queue_size = self.queue.qsize()
            queue_max = self.queue.maxsize
            
            new_state = self.state.derive_state(
                ws_connected=ws_connected,
                writer_alive=True,
                fatal_error=False,
                queue_size=queue_size,
                queue_maxsize=queue_max
            )
            
            # Check for state change
            if self.state.update_state(new_state):
                json_log("WARN", "state_change", details={
                    "new_state": self.state.current_state.value,
                    "reason": "metrics_threshold"
                })
            
            # Get memory
            try:
                rss_mb = psutil.Process(pid).memory_info().rss / 1024 / 1024
            except:
                rss_mb = 0
            
            # Emit heartbeat (P8: includes per-exchange EPS and truthful ws_connected)
            json_log("INFO", "heartbeat", details={
                "state": self.state.current_state.value,
                "ws_connected": ws_connected,
                "eps_by_exchange": self.state.eps_by_exchange,
                "metrics": {
                    "queue_pct": round(queue_size / queue_max * 100, 1) if queue_max > 0 else 0,
                    "silence_15m": self.state.silence_intervals_15m,
                    "drops_total": self.state.dropped_events,
                    "reconnects_total": sum(self.state.reconnect_counts.values()),
                    "rss_mb": round(rss_mb, 1)
                }
            })
    
    async def _collector_meta_log(self):
        """Print collector meta stats every 10 seconds."""
        last_qsize = 0
        while self.running:
            await asyncio.sleep(10)
            uptime = int(time.time() - self.start_time)
            qsize = self.queue.qsize()
            qrate = (qsize - last_qsize) / 10  # events/sec entering queue
            last_qsize = qsize
            print(f"[Meta] uptime={uptime}s queue={qsize} Δqueue/s={qrate:+.1f}")

    async def _quality_ledger_task(self):
        """P9: Data Quality Ledger - Aligned 15m UTC windows with 1s sampling."""
        from datetime import datetime, timezone
        from config import HEARTBEAT_TIMEOUT, QUALITY_DIR
        
        # Ensure quality directory exists
        os.makedirs(QUALITY_DIR, exist_ok=True)
        
        last_flush_minute = -1
        
        while self.running:
            now = time.time()
            dt = datetime.fromtimestamp(now, tz=timezone.utc)
            
            # 1s Sampling logic
            qsize = self.queue.qsize()
            qmax = self.queue.maxsize
            q_util = (qsize / qmax * 100) if qmax > 0 else 0
            
            # P9: Peak floor based on backpressure mode
            peak_floor = 0
            if self.state.backpressure_mode == "high":
                peak_floor = 80
            elif self.state.backpressure_mode == "critical":
                peak_floor = 95
                
            sampled_q_util = max(q_util, peak_floor)
            self.state.window_queue_samples.append(sampled_q_util)
            self.state.window_backpressure_samples.append(self.state.backpressure_mode)
            
            if self.state.drain_mode == "accelerated":
                self.state.window_accelerated_seconds += 1.0
                
            ws_connected = self.state.get_ws_connected(HEARTBEAT_TIMEOUT)
            for ex, connected in ws_connected.items():
                if not connected:
                    self.state.window_offline_seconds[ex] += 1.0
            
            for ex, eps_val in self.state.eps_by_exchange.items():
                self.state.window_eps_samples[ex].append(eps_val)
                
            # Check for 15m boundary (00, 15, 30, 45 UTC)
            if dt.minute % 15 == 0 and dt.minute != last_flush_minute:
                # Ensure we have at least 60s of data before flushing
                if now - self.state.window_start_time > 60:
                    await self._flush_quality_window(dt)
                    last_flush_minute = dt.minute
            
            await asyncio.sleep(1.0)

    async def _flush_quality_window(self, dt_end):
        """P9: Calculate quality, persist JSON atomically, and take new snapshot."""
        import json
        from datetime import timedelta
        
        window_end = dt_end.replace(second=0, microsecond=0)
        window_start = window_end - timedelta(minutes=15)
        
        deltas = self.state.get_window_deltas()
        
        # Aggregate EPS stats
        eps_stats = {}
        all_samples_present = True
        for ex, samples in self.state.window_eps_samples.items():
            if not samples:
                eps_stats[ex] = {"avg": 0.0, "min": 0.0}
                all_samples_present = False
            else:
                eps_stats[ex] = {
                    "avg": round(sum(samples) / len(samples), 1),
                    "min": round(min(samples), 1)
                }
        
        peak_q = max(self.state.window_queue_samples) if self.state.window_queue_samples else 0.0
        max_offline = max(self.state.window_offline_seconds.values()) if self.state.window_offline_seconds else 0.0
        
        # Quality Classification
        quality = "GOOD"
        
        # BAD: drops OR >120s offline OR >=80% queue
        if deltas["dropped_events"] > 0 or max_offline > 120 or peak_q >= 80:
            quality = "BAD"
        # DEGRADED: missing EPS samples OR (>60s offline OR sustained zero EPS OR sustained accelerated drain OR multi-reconnects)
        else:
            # P10: Nuanced thresholds (v2)
            # Count seconds with zero EPS (silence)
            max_zero_eps_sec = 0
            for ex, samples in self.state.window_eps_samples.items():
                zero_sec = sum(1 for s in samples if s < 0.1)
                max_zero_eps_sec = max(max_zero_eps_sec, zero_sec)
                
            if (not all_samples_present or (
                max_offline > 60 or
                max_zero_eps_sec > 5 or
                self.state.window_accelerated_seconds > 60 or
                deltas["reconnects"] >= 3
            )):
                quality = "DEGRADED"
            
        # Calculate actual duration for this ledger snapshot
        actual_duration = time.time() - self.state.window_start_time
        
        ledger = {
            "window_start": window_start.isoformat().replace("+00:00", "Z"),
            "window_end": window_end.isoformat().replace("+00:00", "Z"),
            "quality": quality,
            "is_partial": self.state.window_is_partial,
            "signals": {
                "actual_duration_seconds": round(actual_duration, 1),
                "offline_seconds_by_exchange": {ex: round(off, 1) for ex, off in self.state.window_offline_seconds.items()},
                "ws_connected_seconds": {ex: round(max(0, actual_duration - off), 1) for ex, off in self.state.window_offline_seconds.items()},
                "eps_by_exchange": eps_stats,
                "dropped_events": deltas["dropped_events"],
                "queue_pct_peak": round(peak_q, 1),
                "drain_mode_accelerated_seconds": round(self.state.window_accelerated_seconds, 1),
                "reconnects": deltas["reconnects"],
                "s3_upload_failures": deltas["s3_upload_failures"]
            }
        }
        
        # Persistence quality/date=YYYYMMDD/window=HHMM.json
        from config import QUALITY_DIR
        date_str = window_start.strftime("%Y%m%d")
        window_str = window_start.strftime("%H%M")
        dir_path = f"{QUALITY_DIR}/date={date_str}"
        os.makedirs(dir_path, exist_ok=True)
        
        file_path = f"{dir_path}/window={window_str}.json"
        tmp_path = f"{file_path}.tmp"
        try:
            with open(tmp_path, "w") as f:
                json.dump(ledger, f, indent=2)
            os.replace(tmp_path, file_path)
            
            json_log("INFO", "quality_ledger_flush", details={
                "window": f"{date_str}-{window_str}",
                "quality": quality,
                "is_partial": self.state.window_is_partial
            })
        except Exception as e:
            json_log("ERROR", "quality_ledger_error", details={"error": str(e)})
        
        # Reset for next window
        self.state.take_window_snapshot()
        self.state.window_is_partial = False
    
    async def stop(self):
        """Stop all tasks gracefully."""
        print("\n[Collector] Shutting down...")
        self.running = False
        
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        print("[Collector] Shutdown complete")


async def main():
    parser = argparse.ArgumentParser(description="Multi-Exchange Futures Data Collector")
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of symbols (default: all configured symbols)"
    )
    args = parser.parse_args()
    
    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    
    collector = Collector(symbols)
    
    # Setup signal handlers
    loop = asyncio.get_running_loop()
    
    def signal_handler():
        asyncio.create_task(collector.stop())
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await collector.start()
    except KeyboardInterrupt:
        await collector.stop()


if __name__ == "__main__":
    # Force line buffering for systemd journal
    sys.stdout.reconfigure(line_buffering=True)
    asyncio.run(main())
