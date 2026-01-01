from aiohttp import web
import asyncio
import time
from datetime import datetime, timezone
import psutil
import os

class StatusAPI:
    def __init__(self, state, queue):
        self.state = state
        self.queue = queue
        self.app = web.Application()
        self.app["state"] = self.state
        self.app["queue"] = self.queue
        
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/streams", self.handle_streams)
        self.app.router.add_get("/metrics", self.handle_metrics)
        self.app.router.add_get("/status", self.handle_status)
        
        self.runner = None

    async def handle_health(self, request):
        data = {
            "status": "ok",
            "uptime": time.time() - self.state.start_time,
            "last_event_ts": self.state.last_event_ts,
            "last_write_ts": self.state.last_write_ts
        }
        return web.json_response(data)

    async def handle_streams(self, request):
        from config import SYMBOLS
        data = {
            "symbols": SYMBOLS,
            "streams": {
                "binance": ["bbo", "trade", "mark_price", "funding"],
                "bybit": ["bbo", "trade", "mark_price", "funding", "open_interest"],
                "okx": ["bbo", "trade", "mark_price", "funding", "open_interest"]
            }
        }
        return web.json_response(data)

    async def handle_metrics(self, request):
        queue_size = self.queue.qsize()
        queue_max = self.queue.maxsize
        queue_util = (queue_size / queue_max * 100) if queue_max > 0 else 0
        
        metrics = {
            "queue_size": queue_size,
            "queue_maxsize": queue_max,
            "queue_utilization_pct": round(queue_util, 1),
            "dropped_events_total": self.state.dropped_events,
            "gaps_detected_total": self.state.gaps_detected,
            "ws_reconnect_total": self.state.reconnect_counts,
            "snapshot_fetches_total": self.state.snapshot_fetches_total,
            "snapshot_events_seen": self.state.snapshot_events_seen,
            "gap_alignment_corrections": self.state.gap_alignment_corrections,
            "gap_coverage_ratio": self.state.gap_coverage_ratio(),
            "effective_data_loss_rate_pct": self.state.effective_data_loss_rate(),
            "events_per_sec": self.state.eps,
            "total_events": self.state.total_events,
            "writer": self.state.writer_stats
        }
        
        return web.json_response(metrics)

    async def handle_status(self, request):
        """P2: GET /status - Collector runtime state."""
        queue_size = self.queue.qsize()
        queue_max = self.queue.maxsize
        
        # Get memory
        try:
            rss_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        except:
            rss_mb = 0
        
        return web.json_response({
            "state": self.state.current_state.value if hasattr(self.state, 'current_state') else "UNKNOWN",
            "since": datetime.fromtimestamp(self.state.state_since, timezone.utc).isoformat() if hasattr(self.state, 'state_since') else datetime.now(timezone.utc).isoformat(),
            "metrics": {
                "queue_pct": round(queue_size / queue_max * 100, 1) if queue_max > 0 else 0,
                "gaps_15m": self.state.gaps_detected,
                "drops_15m": self.state.dropped_events,
                "reconnects_15m": sum(self.state.reconnect_counts.values()),
                "rss_mb": round(rss_mb, 1)
            }
        })

    async def start(self, host='127.0.0.1', port=9100):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, host, port)
        await site.start()
        # print(f"[API] Listening on {host}:{port}")

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

async def status_api_task(state, queue):
    api = StatusAPI(state, queue)
    await api.start()
    while True:
        await asyncio.sleep(1)
