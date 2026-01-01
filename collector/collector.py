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
        ]

        
        # Wait for all tasks
        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            pass
    
    async def _heartbeat_task(self):
        """P2: Emit heartbeat every 30s with state and metrics."""
        import psutil
        pid = os.getpid()
        
        while self.running:
            await asyncio.sleep(30)
            
            # Derive current state
            ws_connected = {'binance': True, 'bybit': True, 'okx': True}  # Simplified
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
            
            # Emit heartbeat
            json_log("INFO", "heartbeat", details={
                "state": self.state.current_state.value,
                "metrics": {
                    "queue_pct": round(queue_size / queue_max * 100, 1) if queue_max > 0 else 0,
                    "gaps_1m": self.state.gaps_detected,
                    "drops_1m": self.state.dropped_events,
                    "reconnects_5m": sum(self.state.reconnect_counts.values()),
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
