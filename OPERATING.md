# QuantLab Collector — Architecture & Operating Documentation

**Status**: LOCKED  
**Version**: 1.0.0  
**Last Updated**: 2026-01-01

---

## 1️⃣ What the Collector IS
The QuantLab Collector is a **RAW market data ingestion service** designed for high-performance capture of cryptocurrency futures data. It consumes real-time market data exclusively through **WebSocket streams** and writes these events, unmodified, to a RAW Parquet layer. Its sole responsibility is to act as a faithful recorder of the exchange's "WebSocket truth." It does not improve data, fix gaps, backfill history, reorder events, deduplicate records, or interpret signals.

---

## 2️⃣ What the Collector is NOT
To maintain strict architectural separation and data integrity, the collector explicitly excludes the following responsibilities:
- **NOT a recovery system**: It does not attempt to mend broken history.
- **NOT a COMPACT layer**: It does not perform deduplication or reordering.
- **NOT a trading engine**: It has no execution or risk management capabilities.
- **NOT a data correction engine**: It records what it sees, including errors or gaps.
- **NOT self-healing/auto-restarting**: It reports its state but does not take autonomous corrective actions.

These responsibilities are deferred to downstream layers (e.g., COMPACT or Replay) to ensure the RAW layer remains an unadulterated source of truth.

---

## 3️⃣ Data Semantics (RAW Contract)
The RAW layer provided by the collector adheres to a strict contract:
- **Data Source**: Exclusively WebSocket stream data.
- **Parquet Schema**: Fixed, versioned (v1), and immutable.
- **Guarantees**:
  - **Schema Stability**: The structure of the stored data will not change.
  - **No Contamination**: ZERO snapshot data, REST markers, or backfill alignment events are written to RAW.
- **Non-Guarantees**:
  - **Continuity**: Gaps may occur due to network or exchange issues.
  - **Ordering**: Event ordering across reconnects or multi-exchange streams is not guaranteed in the RAW layer.

---

## 4️⃣ Runtime Architecture
The system is composed of localized, specialized components:
- **WebSocket Handlers**: Independent tasks for Binance, Bybit, and OKX that subscribe and parse streams.
- **Non-Blocking Ingestion Queue**: A central buffer that decouples stream ingestion from storage writing.
- **Writer Loop**: A high-throughput task that buffers events and performs atomic writes to S3/Local storage.
- **Gap Detection**: A read-only analytical thread that monitors `ts_event` continuity.
- **Snapshot Alignment**: A RAM-only mechanism that fetches REST timestamps post-reconnect to synchronize gap tracking without persisting non-WebSocket data.
- **Metrics & State Tracking**: A centralized, observable state object updated in real-time.

---

## 5️⃣ Stability Model
The collector's stability is built on **pressure relief** rather than backpressure.
- **Non-blocking queue**: Handlers drop events if the storage layer cannot keep up, preventing connection saturation.
- **Controlled Drops**: Explicitly measuring data loss is preferred over risking connection stability.
- **Tuned Throughput**: `FLUSH_INTERVAL` is set to 2s to ensure high S3 write concurrency.
- **Baseline**: Post-rollout queue utilization typically remains <20%, with reconnects reduced to zero under normal operations.

---

## 6️⃣ Observability Model
The collector provides explicit, machine-readable visibility into its operation.

### Logs
All logs are emitted as **structured JSON** to STDOUT:
- `collector_start`: Emitted once on initialization.
- `ws_connect` / `ws_disconnect`: Tracks heartbeat of exchange connections.
- `gap_detected`: Emitted when `ts_event` continuity is broken.
- `queue_full_drop`: Emitted when the ingestion queue saturates.
- `state_change`: Emitted when a health threshold is crossed (e.g., READY → DEGRADED).
- `heartbeat`: Summary metrics log.

### Heartbeat
Emitted every **30 seconds**. It is the primary signal for health and load monitoring.

### State Model
- **READY**: Normal operation.
- **DEGRADED**: High queue utilization (>80%), active event drops, or high gap rate.
- **OFFLINE**: No WebSockets connected.
- **ERROR**: Fatal internal exception or writer failure.

---

## 7️⃣ APIs Exposed (READ-ONLY)
The collector exposes an HTTP interface (Port 9100) for external monitoring:
- **GET /status**: Returns derived state, uptime, and last 15m performance metrics.
- **GET /metrics**: Returns detailed internal counters (JSON format).

**Important**: No control-plane or state-mutating endpoints exist. The API is strictly for inspection.

---

## 8️⃣ Failure & Downtime Handling
When the collector stops or fails, the impact is systematically measured:
- **Gap Measurement**: Continuous `ts_event` monitoring resumes immediately upon restart.
- **Impact Assessment**: Downtime is quantified as an OFFLINE window (e.g., "11 minutes OFFLINE").
- **Verification**: Impact is documented via gap counts and exchange distributions, ensuring the RAW layer remains stable before and after the event.

---

## 9️⃣ Explicit Invariants
The following rules MUST NEVER be violated:
- **RAW schema immutability**: Field structure is final.
- **No REST data in RAW**: Only WebSocket messages are persisted.
- **No Alignment persistence**: Alignment events exist in RAM only.
- **No Auto-healing**: The collector reports loss; it never manufactures data to fill it.

> Any violation of these invariants is a BUG.

---

## 🔒 10️⃣ Lock Statement
QuantLab Collector is complete, stable, observable, and architecturally final.
This component is LOCKED.
Any future work must occur in downstream layers (COMPACT, Replay, ML).
