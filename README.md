# QuantLab Multi-Exchange Crypto Data Collector

Production-ready high-performance data collector for USDT-USDC margined perpetual futures from Binance, Bybit, and OKX.

## Features
- **Multi-Exchange**: Concurrent WebSocket streams for Binance, Bybit, and OKX.
- **Normalization**: Unified event schema for BBO, Trades, Mark Price, Funding, and Open Interest.
- **Reliability**: Non-blocking queue architecture with effective data loss tracking.
- **Storage**: Highly efficient Parquet storage with S3 integration.
- **Alignment**: Post-reconnect RAM-only gap synchronization using REST snapshots.
- **Observability**: Structured JSON logging, 30s heartbeats, and a status API.

## Repository Structure
- `collector/`: Core application logic.
  - `collector.py`: Orchestration and signal handling.
  - `writer.py`: Parquet buffering and S3 upload logic.
  - `state.py`: Runtime metric tracking and state derivation.
  - `status_api.py`: Read-only HTTP status/metrics API.
- `check_raw_sanity.py`: S3 data integrity and schema verification utility.

## Setup
1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment variables in `.env`:
   ```env
   S3_ENDPOINT=...
   S3_BUCKET=...
   S3_ACCESS_KEY=...
   S3_SECRET_KEY=...
   ```

## Usage
Start the collector:
```bash
python3 collector/collector.py
```

Optional symbols:
```bash
python3 collector/collector.py --symbols BTCUSDT,ETHUSDT
```

## Monitoring
- **Heartbeat**: Emits JSON logs every 30s.
- **Status API**: `GET http://localhost:9100/status`
- **Metrics**: `GET http://localhost:9100/metrics`

## Verification
Run the sanity check utility to verify S3 data quality:
```bash
python3 check_raw_sanity.py
```

## RAW Data Semantics
The RAW layer strictly preserves "WebSocket truth". No snapshot data or backfill modifications are ever written to RAW parquet files.
