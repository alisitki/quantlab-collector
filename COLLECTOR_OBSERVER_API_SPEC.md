# QuantLab Collector: Observer API Specification (v1)

This document provides the technical specification for the **Collector Observer API**. 
The API is the sole source of truth for the collector's operational state and historical data quality.

**Base URL**: `http://<collector-ip>:9100`

---

## 1. GET `/collector/now`
**Purpose**: Immediate real-time status. Used for "Live" dashboards.

### Response Example
```json
{
  "state": "READY",
  "uptime_seconds": 12345,
  "queue_pct": 12.4,
  "drain_mode": "normal",
  "ws_connected": {
    "binance": true,
    "bybit": true,
    "okx": false
  },
  "eps_by_exchange": {
    "binance": 4028.7,
    "bybit": 118.5,
    "okx": 187.0
  },
  "memory_rss_mb": 219.7,
  "last_heartbeat_utc": "2026-01-03T05:59:35Z"
}
```

### Key Fields
- `state`: Overall system health (`READY`, `DEGRADED`, `BAD`).
- `ws_connected`: **Truthful** liveness (True if data was received in last 30s).
- `drain_mode`: `accelerated` means the collector is currently flushing 5x faster to handle a burst.

---

## 2. GET `/collector/meta`
**Purpose**: Static metadata and system capability description.

### Response Example
```json
{
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
}
```

---

## 3. GET `/collector/day/:YYYYMMDD/summary`
**Purpose**: High-level daily trust assessment. Answers "Can I use today's data?".

### Response Example
```json
{
  "date": "2026-01-03",
  "trust_epoch": true,
  "overall_quality": "DEGRADED",
  "window_counts": {
    "GOOD": 72,
    "DEGRADED": 20,
    "BAD": 4
  },
  "bad_windows": ["03:15", "03:30"],
  "total_drops": 372520,
  "total_reconnects": 78,
  "total_offline_seconds": {
    "binance": 134,
    "bybit": 0,
    "okx": 0
  },
  "max_queue_pct": 87.6,
  "accelerated_drain_seconds": 512.0,
  "recommended_usage": {
    "ml_backtest": true,
    "production_trading": false,
    "notes": "BAD windows present between 03:15–03:30 UTC"
  }
}
```

### Aggregation Rules
- `overall_quality`: **BAD** if any window is BAD, **DEGRADED** if no BAD but any DEGRADED, else **GOOD**.
- `ml_backtest`: Set to `false` if `overall_quality` is BAD.
- `production_trading`: Set to `false` if `overall_quality` is not GOOD.

---

## 4. GET `/collector/day/:YYYYMMDD/windows`
**Purpose**: Granular 15-minute window data for auditing or detailed charting.

### Response Example
```json
[
  {
    "window": "03:15",
    "quality": "BAD",
    "is_partial": false,
    "queue_peak_pct": 87.6,
    "offline_seconds": {
      "binance": 92.0,
      "bybit": 0,
      "okx": 0
    },
    "eps": {
      "binance": {"min": 0, "avg": 3200},
      "bybit": {"min": 60, "avg": 80},
      "okx": {"min": 110, "avg": 130}
    },
    "drops": 372520,
    "reconnects": 78,
    "accelerated_drain_seconds": 90.0
  }
]
```

---

## ⚖️ Collector API Contract Guarantees

1. **Opinionated Health**: The collector classifies its own health; the UI should merely render the `quality` flags.
2. **Atomic History**: Every 15-minute window is a complete, immutable unit of observation.
3. **No Inference Required**: UI does not need to calculate deltas or aggregate statuses; the `/summary` endpoint provides the final verdict.
4. **Data-Based Truth**: `ws_connected` and `offline_seconds` are derived from actual message timestamps, not socket state.
5. **UTC-Locked Boundaries**: All window timestamps are UTC-locked at 00, 15, 30, and 45 minutes past the hour.
6. **Trust Epoch Awareness**: `trust_epoch` flag explicitly tells the consumer if the data was collected after the 2026-01-03 resilience hardening.

---
**QuantLab Data Reliability Engineering**
