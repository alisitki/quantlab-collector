# QuantLab Collector: Capability & Usage Document

**Last Updated**: 2026-01-03  
**Status**: Production Mature  
**Trust Epoch**: Post-2026-01-03 (Hardened)

---

## 1. Collector Guarantees

### What This Collector Guarantees
- **Atomic Persistence**: Data is written only when a full buffer is successfully serialized and uploaded to S3.
- **Priority Protection**: Critical streams (*mark_price*, *funding*, *open_interest*) are given head-of-line blocking priority during queue saturation.
- **Bounded Resource Usage**: Memory and thread pools are strictly bounded to prevent OOM crashes during slow-downs.
- **Truthful Liveness**: Status signals (`ws_connected`) reflect actual data flow, not just TCP socket state.
- **Gap Marking**: Incomplete or degraded data intervals are explicitly marked in the **Data Quality Ledger**.

### What Is NOT Guaranteed
- **Zero Drops**: During extreme, sustained market volatility exceeding system throughput, the collector WILL drop non-priority data (trades/BBO) to survive.
- **Infinite Priority Buffer**: If the queue remains at 95%+ for more than 30 seconds, even priority events may be dropped to prevent system-wide deadlock.
- **Real-time Orderbook Reconstruction**: This collector provides snapshot-based BBO/depth, not full L2 incremental sync.

---

## 2. Data Trust per Stream

| Stream | Reliability | Disconnect Behavior | Best For | Safe for backtest? |
|--------|-------------|---------------------|----------|-------------------|
| **trade** | High* | Dropped during overload | Volume profile, HFT research | Yes (check GOOD flag) |
| **bbo** | High* | Dropped during overload | Slippage modeling, Execution | Yes (check GOOD flag) |
| **mark_price**| Critical | Prioritized protection | PnL calculation, Liquidations | Always (if collected) |
| **funding** | Critical | Prioritized protection | Carry cost, Basis trading | Always (if collected) |
| **open_interest**| Critical | Prioritized protection | Sentiment research | Always (if collected) |

*\*Subject to backpressure drops during extreme volatility.*

---

## 3. Failure Behavior & Marking

### Short Disconnect (< 30s)
- **Result**: Temporary data gap.
- **Marking**: Classified as **GOOD** or **DEGRADED** depending on length.
- **Downstream**: Usually safe to interpolate.

### Sustained Overload / Queue Saturation
- **Result**: Non-priority events (trades/BBO) are dropped.
- **Marking**: Window is marked **BAD**.
- **Downstream**: **MUST SKIP** this window for volume-sensitive research.

### S3 Slowdown
- **Result**: Buffer accumulation in RAM, increased queue pressure.
- **Behavior**: Bounded by 30s timeout; if timeout hits, buffer is restored and retry attempt scheduled.
- **Downstream**: Quality might drop to **DEGRADED** due to accelerated drain activation.

---

## 4. How to Use Safely

### The Golden Rule
> **SKIP any 15-minute window marked as "BAD" or "DEGRADED" in the Quality Ledger.**

### Downstream Consumption Policy
- **Machine Learning / Backtesting**: Use windows marked **GOOD** or **DEGRADED**. Always skip **BAD** windows to avoid volume/price bias.
- **Production Execution / Arbitrage**: Use **GOOD** windows only. **DEGRADED** windows may contain latency jitter or connectivity blips that affect execution performance.
- **Monitoring / Reporting**: Both **GOOD** and **DEGRADED** are acceptable for general uptime and status reporting.

### Reliable Signals
- **GOOD**: Zero drops, healthy queue, complete connectivity. Trusted for all use cases.
- **DEGRADED**: No data was dropped, but infrastructure was stressed (reconnects, slow S3, or short outages). Use with caution for latency-sensitive research.
- **BAD**: Data was explicitly dropped or a major outage occurred. Data is incomplete.

---

## 5. Ledger JSON Schema

The Quality Ledger persist JSON files with the following schema:

```json
{
  "window_start": "ISO8601 UTC",
  "window_end": "ISO8601 UTC",
  "quality": "GOOD | DEGRADED | BAD",
  "is_partial": "true | false",
  "signals": {
    "actual_duration_seconds": "float",
    "offline_seconds_by_exchange": {"binance": "float", ...},
    "ws_connected_seconds": {"binance": "float", ...},
    "eps_by_exchange": {
      "binance": {"avg": "float", "min": "float"},
      ...
    },
    "dropped_events": "int",
    "queue_pct_peak": "float",
    "drain_mode_accelerated_seconds": "float",
    "reconnects": "int",
    "s3_upload_failures": "int"
  }
}
```

---

## 6. Explicit Anti-Patterns 🛑

- **ML Training on Unfiltered Data**: Never feed windows marked **BAD** into ML models; the missing trade volume will bias the results.
- **Assuming Reconnect = Data Continuity**: WebSocket reconnects ALWAYS result in a small gap. Check for `silence_intervals` if sub-second continuity is required.
- **Mixing Epochs**: Do NOT mix data collected before 2026-01-03 with hardened data for high-precision research. The legacy data lacks robust drop marking and truthful liveness.

---

## 7. Lifecycle & Trust Epoch

- **Trusted Epoch**: Starts **2026-01-03 10:00 UTC**. All data after this point follows the hardening guarantees and includes the Quality Ledger.
- **Legacy/Untrusted**: Data collected prior to 2026-01-03. This data may contain silent drops, misleading liveness signals, and unrecorded S3 failures. Use for general research ONLY, not for production strategy validation.

---

*"A researcher unaware of data quality is a researcher doomed to find non-existent alpha."*
