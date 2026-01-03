from aiohttp import web
import asyncio
import time
from datetime import datetime, timezone
import psutil
import os

@web.middleware
async def cors_middleware(request, handler):
    if request.method == "OPTIONS":
        response = web.Response(status=200)
    else:
        response = await handler(request)
    
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

class StatusAPI:
    def __init__(self, state, queue):
        self.state = state
        self.queue = queue
        self.app = web.Application(middlewares=[cors_middleware])
        self.app["state"] = self.state
        self.app["queue"] = self.queue
        
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/streams", self.handle_streams)
        self.app.router.add_get("/metrics", self.handle_metrics)
        self.app.router.add_get("/status", self.handle_status)
        
        # P10: Observer APIs (Truth Exposure)
        self.app.router.add_get("/collector/now", self.handle_collector_now)
        self.app.router.add_get("/collector/meta", self.handle_collector_meta)
        self.app.router.add_get("/collector/day/{date}/summary", self.handle_day_summary)
        self.app.router.add_get("/collector/day/{date}/windows", self.handle_day_windows)
        
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
        from config import HEARTBEAT_TIMEOUT
        queue_size = self.queue.qsize()
        queue_max = self.queue.maxsize
        queue_util = (queue_size / queue_max * 100) if queue_max > 0 else 0
        
        metrics = {
            "queue_size": queue_size,
            "queue_maxsize": queue_max,
            "queue_utilization_pct": round(queue_util, 1),
            # Counter semantics: _total = lifetime (since process start)
            "dropped_events_total": self.state.dropped_events,
            # Silence intervals (renamed from gaps_detected)
            "silence_intervals_15m": self.state.silence_intervals_15m,
            "silence_intervals_total": self.state.silence_intervals_total,
            "ws_reconnect_total": self.state.reconnect_counts,
            "snapshot_fetches_total": self.state.snapshot_fetches_total,
            "snapshot_events_seen": self.state.snapshot_events_seen,
            "gap_alignment_corrections": self.state.gap_alignment_corrections,
            "effective_data_loss_rate_pct": self.state.effective_data_loss_rate(),
            "events_per_sec": self.state.eps,
            # P8: Per-exchange event rate (blind spot removal)
            "eps_by_exchange": self.state.eps_by_exchange,
            # P8: Truthful ws_connected (data-based, not socket-based)
            "ws_connected": self.state.get_ws_connected(HEARTBEAT_TIMEOUT),
            "total_events": self.state.total_events,
            "writer": self.state.writer_stats,
            # Backpressure metrics
            "backpressure": {
                "mode": self.state.backpressure_mode,
                "active": self.state.backpressure_active,
                "events_count": self.state.backpressure_events_count,
                "wait_seconds_total": round(self.state.backpressure_wait_seconds_total, 2)
            },
            # P7: Adaptive drain and throughput metrics
            "drain_mode": self.state.drain_mode,
            "throughput": {
                "ingestion_rate": self.state.ingestion_rate,
                "drain_rate": self.state.drain_rate,
                "queue_growth_rate": self.state.queue_growth_rate
            }
        }
        
        return web.json_response(metrics)

    async def handle_status(self, request):
        """P2: GET /status - Collector runtime state. P8: Includes truthful ws_connected."""
        from config import HEARTBEAT_TIMEOUT
        queue_size = self.queue.qsize()
        queue_max = self.queue.maxsize
        
        # Get memory
        try:
            rss_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        except:
            rss_mb = 0
        
        # P8: Truthful ws_connected based on actual data flow
        ws_connected = self.state.get_ws_connected(HEARTBEAT_TIMEOUT)
        
        return web.json_response({
            "state": self.state.current_state.value if hasattr(self.state, 'current_state') else "UNKNOWN",
            "since": datetime.fromtimestamp(self.state.state_since, timezone.utc).isoformat() if hasattr(self.state, 'state_since') else datetime.now(timezone.utc).isoformat(),
            # P8: Truthful ws_connected and per-exchange EPS
            "ws_connected": ws_connected,
            "eps_by_exchange": self.state.eps_by_exchange,
            "metrics": {
                "queue_pct": round(queue_size / queue_max * 100, 1) if queue_max > 0 else 0,
                "silence_15m": self.state.silence_intervals_15m,
                # Counter semantics: _total = lifetime (NOT rolling 15m window)
                "dropped_total": self.state.dropped_events,
                "reconnects_total": sum(self.state.reconnect_counts.values()),
                "rss_mb": round(rss_mb, 1),
                "backpressure_mode": self.state.backpressure_mode,
                "drain_mode": self.state.drain_mode
            }
        })

    async def handle_collector_now(self, request):
        """P10: GET /collector/now - Immediate liveness & operational truth."""
        from config import HEARTBEAT_TIMEOUT
        queue_size = self.queue.qsize()
        queue_max = self.queue.maxsize
        
        try:
            rss_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        except:
            rss_mb = 0
            
        ws_connected = self.state.get_ws_connected(HEARTBEAT_TIMEOUT)
        
        # Determine overall state (BAD > DEGRADED > READY)
        # Truth: BAD if anyone offline > 120s (already handled by derive_state logic ideally, but we'll be explicit)
        state_enum = self.state.current_state.value
        
        return web.json_response({
            "state": state_enum,
            "uptime_seconds": int(time.time() - self.state.start_time),
            "queue_pct": round(queue_size / queue_max * 100, 1) if queue_max > 0 else 0,
            "drain_mode": self.state.drain_mode,
            "ws_connected": ws_connected,
            "eps_by_exchange": self.state.eps_by_exchange,
            "memory_rss_mb": round(rss_mb, 1),
            "last_heartbeat_utc": datetime.fromtimestamp(time.time(), timezone.utc).isoformat().replace("+00:00", "Z")
        })

    async def handle_collector_meta(self, request):
        """P10: GET /collector/meta - Static meta-information for the hardened system."""
        from config import SILENCE_WINDOW_SECONDS
        return web.json_response({
            "trust_epoch_start": "2026-01-03T00:00:00Z",
            "quality_window_minutes": 15,
            "supported_exchanges": ["binance", "bybit", "okx"],
            "priority_streams": ["mark_price", "funding", "open_interest"],
            "non_priority_streams": ["trade", "bbo"],
            "drop_policy": "non-priority dropped under CRITICAL backpressure",
            "guarantees": [
                "No silent exchange-level data loss",
                "No infinite writer blocking",
                "Truthful liveness reporting (data-based)"
            ]
        })

    async def _load_day_windows(self, date_str: str) -> list:
        """Helper to load all quality window JSONs for a specific date."""
        import json
        import glob
        from config import QUALITY_DIR
        dir_path = f"{QUALITY_DIR}/date={date_str}"
        if not os.path.exists(dir_path):
            return []
        
        files = sorted(glob.glob(f"{dir_path}/window=*.json"))
        windows = []
        for f in files:
            try:
                with open(f, "r") as j:
                    windows.append(json.load(j))
            except:
                continue
        return windows

    async def handle_day_summary(self, request):
        """P10: GET /collector/day/:date/summary - Aggregated quality report."""
        date_str = request.match_info.get("date")
        windows = await self._load_day_windows(date_str)
        
        if not windows:
            return web.json_response({"error": "No data found for date", "date": date_str}, status=404)
            
        summary = {
            "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
            "trust_epoch": date_str >= "20260103",
            "overall_quality": "GOOD",
            "window_counts": {"GOOD": 0, "DEGRADED": 0, "BAD": 0},
            "bad_windows": [],
            "total_drops": 0,
            "total_reconnects": 0,
            "total_offline_seconds": {"binance": 0, "bybit": 0, "okx": 0},
            "max_queue_pct": 0.0,
            "accelerated_drain_seconds": 0,
            "recommended_usage": {
                "ml_backtest": True,
                "production_trading": True,
                "notes": ""
            }
        }
        
        for w in windows:
            q = w["quality"]
            summary["window_counts"][q] += 1
            
            # Aggregate totals & find max
            sig = w["signals"]
            summary["total_drops"] += sig["dropped_events"]
            summary["total_reconnects"] += sig["reconnects"]
            summary["max_queue_pct"] = max(summary["max_queue_pct"], sig["queue_pct_peak"])
            summary["accelerated_drain_seconds"] += sig["drain_mode_accelerated_seconds"]
            for ex in summary["total_offline_seconds"]:
                summary["total_offline_seconds"][ex] += sig.get("offline_seconds_by_exchange", {}).get(ex, 0)
            
            # Track bad windows for notes
            if q == "BAD":
                summary["bad_windows"].append(w["window_start"].split("T")[1][:5])

        # P10: Nuanced daily aggregation (v2)
        # GOOD if < 5 DEGRADED and 0 BAD
        if summary["window_counts"]["BAD"] > 0:
            summary["overall_quality"] = "BAD"
        elif summary["window_counts"]["DEGRADED"] >= 5:
            summary["overall_quality"] = "DEGRADED"
        else:
            summary["overall_quality"] = "GOOD"

        # Recommended Usage logic
        if summary["overall_quality"] == "BAD":
            summary["recommended_usage"]["ml_backtest"] = False
            summary["recommended_usage"]["production_trading"] = False
            summary["recommended_usage"]["notes"] = f"CRITICAL: {len(summary['bad_windows'])} BAD windows present: {', '.join(summary['bad_windows'])}"
        elif summary["overall_quality"] == "DEGRADED":
            # Significant enough to block production, but safe for ML
            summary["recommended_usage"]["ml_backtest"] = True
            summary["recommended_usage"]["production_trading"] = False
            summary["recommended_usage"]["notes"] = f"Frequent DEGRADED windows ({summary['window_counts']['DEGRADED']}) detected. Unsafe for execution."
        else:
            # GOOD (allows up to 4 DEGRADED windows)
            summary["recommended_usage"]["ml_backtest"] = True
            summary["recommended_usage"]["production_trading"] = True
            deg_count = summary["window_counts"]["DEGRADED"]
            if deg_count > 0:
                summary["recommended_usage"]["notes"] = f"Nominal quality with {deg_count} minor DEGRADED windows. Safe for production."
            else:
                summary["recommended_usage"]["notes"] = "Perfect data quality (all windows GOOD)."

        return web.json_response(summary)

    async def handle_day_windows(self, request):
        """P10: GET /collector/day/:date/windows - Drill-down data for auditing."""
        date_str = request.match_info.get("date")
        windows = await self._load_day_windows(date_str)
        
        if not windows:
            return web.json_response({"error": "No data found for date", "date": date_str}, status=404)
            
        drilldown = []
        for w in windows:
            sig = w["signals"]
            drilldown.append({
                "window": w["window_start"].split("T")[1][:5],
                "quality": w["quality"],
                "is_partial": w["is_partial"],
                "queue_peak_pct": sig["queue_pct_peak"],
                "offline_seconds": sig.get("offline_seconds_by_exchange", {}),
                "eps": sig["eps_by_exchange"],
                "drops": sig["dropped_events"],
                "reconnects": sig["reconnects"],
                "accelerated_drain_seconds": sig["drain_mode_accelerated_seconds"]
            })
            
        return web.json_response(drilldown)

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
    from config import API_HOST, API_PORT
    api = StatusAPI(state, queue)
    await api.start(host=API_HOST, port=API_PORT)
    while True:
        await asyncio.sleep(1)

