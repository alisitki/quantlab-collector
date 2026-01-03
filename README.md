# QuantLab Multi-Exchange Crypto Data Collector

Production-ready high-performance data collector for USDT-margined perpetual futures from Binance, Bybit, and OKX.

---

## 1️⃣ What the Collector IS
The QuantLab Collector is a **RAW market data ingestion service** designed for high-performance capture of cryptocurrency futures data. It consumes real-time market data exclusively through **WebSocket streams** and writes these events, unmodified, to a RAW Parquet layer. Its sole responsibility is to act as a faithful recorder of the exchange's "WebSocket truth."

---

## 2️⃣ What the Collector is NOT
- **NOT a recovery system**: It does not attempt to mend broken history.
- **NOT a COMPACT layer**: It does not perform deduplication or reordering.
- **NOT a trading engine**: It has no execution or risk management capabilities.
- **NOT self-healing**: It reports its state but does not take autonomous corrective actions.

---

## 3️⃣ Data Semantics (RAW Contract)
- **Data Source**: Exclusively WebSocket stream data.
- **Parquet Schema**: Fixed, versioned (v1), and immutable.
- **Guarantees**: Schema stability, no contamination from REST/snapshot data.
- **Non-Guarantees**: Continuity gaps may occur; ordering across reconnects not guaranteed.

---

## 🚀 Key Features
- **Multi-Exchange**: Concurrent WebSocket streams for Binance, Bybit, and OKX.
- **Normalization**: Unified event schema for BBO, Trades, Mark Price, Funding, and Open Interest.
- **Async S3 Uploads**: ThreadPoolExecutor (8 workers) prevents event loop blocking.
- **Storage**: Highly efficient Parquet storage with S3 integration.
- **Observability**: Structured JSON logging, 30s heartbeats, and a status API.

---

## 4️⃣ Runtime Architecture
- **WebSocket Handlers**: Independent tasks for Binance, Bybit, and OKX.
- **Non-Blocking Queue**: Central buffer (500K capacity) decouples ingestion from storage.
- **Async Writer**: ThreadPoolExecutor-based S3 uploads with bounded concurrency (max 16 in-flight).
- **Silence Detection**: Monitors staleness for mark_price/funding/OI streams (not bbo/trade).
- **Metrics & State Tracking**: Centralized, observable state object updated in real-time.

---

## 5️⃣ Stability Model
- **Non-blocking queue**: Handlers use `put_nowait()` to prevent connection saturation.
- **Controlled Drops**: Explicit data loss measurement preferred over connection instability.
- **P3 Tuning**: `BUFFER_SIZE=5000`, `FLUSH_INTERVAL=60s` reduces S3 calls by ~30x.
- **Baseline**: Queue utilization typically <1%, zero dropped events under normal load.

---

## 6️⃣ Observability Model

### State Model
| State | Meaning |
|-------|---------|
| **READY** | Normal operation |
| **DEGRADED** | Queue >80% OR drops >0 |
| **OFFLINE** | No WebSockets connected |
| **ERROR** | Fatal exception or writer failure |

### Logs (Structured JSON to STDOUT)
- `collector_start`, `ws_connect`, `state_change`, `heartbeat`

### Heartbeat (every 30s)
Primary health signal with queue_pct, silence_15m, drops, reconnects, RSS memory.

---

## 7️⃣ Status API (READ-ONLY)

The collector exposes a read-only HTTP API on port **9100**.

### GET /health
Basic liveness check with uptime and last event timestamps.

```json
{
  "status": "ok",
  "uptime": 147.82,
  "last_event_ts": 1767280498692,
  "last_write_ts": 1767280498691
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Always "ok" if API responds |
| `uptime` | float | Seconds since collector start |
| `last_event_ts` | float | Epoch ms of last event received |
| `last_write_ts` | float | Epoch ms of last S3/local write |

---

### GET /streams
Configuration info: symbols and streams per exchange.

```json
{
  "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT", ...],
  "streams": {
    "binance": ["bbo", "trade", "mark_price", "funding"],
    "bybit": ["bbo", "trade", "mark_price", "funding", "open_interest"],
    "okx": ["bbo", "trade", "mark_price", "funding", "open_interest"]
  }
}
```

---

### GET /metrics
Detailed internal counters for monitoring dashboards.

```json
{
  "queue_size": 23,
  "queue_maxsize": 500000,
  "queue_utilization_pct": 0.0,
  "dropped_events_total": 0,
  "silence_intervals_15m": 18,
  "silence_intervals_total": 18,
  "ws_reconnect_total": {"binance": 0, "bybit": 0, "okx": 0},
  "snapshot_fetches_total": 30,
  "snapshot_events_seen": 70,
  "gap_alignment_corrections": 70,
  "effective_data_loss_rate_pct": 0.0,
  "events_per_sec": {"bbo": 833.3, "trade": 84.1, "mark_price": 60.4, "funding": 10.0, "open_interest": 5.8},
  "total_events": 113046,
  "writer": {
    "total_written": 92909,
    "files_written": 203,
    "pending_events": 19013,
    "active_buffers": 140,
    "in_flight_uploads": 1,
    "upload_failures": 0
  }
}
```

| Field | Description |
|-------|-------------|
| `queue_size` | Current queue depth |
| `dropped_events_total` | Events dropped due to full queue (lifetime) |
| `silence_intervals_15m` | Staleness detections in last 15min (mark_price/funding/OI only) |
| `silence_intervals_total` | Lifetime staleness count |
| `ws_reconnect_total` | Reconnects per exchange |
| `events_per_sec` | Current EPS by stream type |
| `writer.in_flight_uploads` | Concurrent S3 uploads in progress |
| `writer.upload_failures` | S3 upload failures (with retry/restore) |

---

### GET /status
Concise runtime state for quick health checks.

```json
{
  "state": "READY",
  "since": "2026-01-01T15:12:30.890602+00:00",
  "metrics": {
    "queue_pct": 0.0,
    "silence_15m": 18,
    "drops_15m": 0,
    "reconnects_15m": 0,
    "rss_mb": 187.2
  }
}
```

| Field | Description |
|-------|-------------|
| `state` | READY / DEGRADED / OFFLINE / ERROR |
| `since` | ISO timestamp when current state began |
| `queue_pct` | Queue utilization percentage |
| `silence_15m` | Staleness events in 15m window |
| `drops_15m` | Dropped events (should be 0) |
| `rss_mb` | Process memory usage in MB |

---

## 📂 Repository Structure
```
collector/
├── collector.py      # Orchestration and signal handling
├── writer.py         # Async Parquet buffering and S3 upload
├── state.py          # Runtime metrics and state derivation
├── status_api.py     # Read-only HTTP API (port 9100)
├── config.py         # Configuration constants
├── binance_handler.py
├── bybit_handler.py
├── okx_handler.py
└── schemas.py        # Parquet schema definitions
```

---

## 🛠 Setup & Usage

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment (`.env`)
```env
STORAGE_BACKEND=s3
S3_ENDPOINT=https://...
S3_ACCESS_KEY=...
S3_SECRET_KEY=...
S3_BUCKET=quantlab-raw
S3_PREFIX=v3
```

### 3. Start Collector
```bash
python3 collector/collector.py [--symbols BTCUSDT,ETHUSDT]
```

### 4. Systemd Service
```bash
sudo systemctl start quantlab-collector
sudo systemctl status quantlab-collector
journalctl -u quantlab-collector -f
```

---

## 📋 Changelog

### P3 (2026-01-01) - Event Loop Blocking Fix & Production Hardening

**Problem**: Synchronous boto3 S3 uploads blocked asyncio event loop, causing queue saturation and data loss.

**Fixes**:
- **Async S3 Writer**: ThreadPoolExecutor (8 workers) for non-blocking uploads
- **Bounded Concurrency**: asyncio.Semaphore limits to 16 in-flight uploads
- **Config Tuning**: BUFFER_SIZE=5000, FLUSH_INTERVAL=60s
- **Flush Burst Protection**: MIN_FLUSH_GAP_SECONDS=5, staggered scheduling
- **Exception Safety**: Buffer restore on upload failure, `upload_failures` metric
- **Graceful Shutdown**: `executor.shutdown(wait=True)` on SIGTERM

**Metric Rename**:
- `gaps_detected` → `silence_intervals` (detects staleness, NOT data loss)
- Only mark_price/funding/OI monitored; bbo/trade disabled (variable frequency)
- Rolling 15m window implemented

---

## 🔒 Lock Statement
QuantLab Collector is complete, stable, observable, and architecturally final.
This component is LOCKED.
Any future work must occur in downstream layers (COMPACT, Replay, ML).

