# QuantLab Parquet Compaction Job (Compressor)

Efficiently consolidates small Parquet files from `quantlab-raw` into daily `data.parquet` files in `quantlab-compact`.

## Key Features
- **State-Based Catch-Up**: Uses `compacted/_state.json` to track progress and automatically process missing days.
- **Fast Discovery (O(1) Startup)**: Uses S3 delimiters to walk directory prefixes instead of scanning all files.
- **Deterministic Order**: Adds a `seq` column (BIGINT, 0 to N-1) after `ts_event` for global monotonic ordering. 
- **Streaming K-Way Merge**: Uses bounded-memory external merge to handle arbitrarily large partitions without OOM.
- **Operational Audit**: The `_state.json` file includes `last_compacted_date` and `updated_at` timestamps for monitoring.
- **Partial Day Protection**: Automatically ignores today's data to avoid compacting incomplete days.

## Technical Specifications

### Memory-Efficient Streaming Merge

The compactor uses an external k-way merge algorithm that:
- Holds **at most one batch per input file** in memory (~50k rows/file)
- Writes output incrementally via ParquetWriter (100k row buffer)
- Target memory usage: **≤ 2-3 GB** for any partition size

#### Tuning Constants (`merge_writer.py`)

| Constant | Default | Description |
|----------|---------|-------------|
| `MERGE_BATCH_SIZE` | 50,000 | Rows per input file batch |
| `MERGE_OUTPUT_BUFFER_SIZE` | 100,000 | Rows before flush to output |
| `MERGE_LOG_INTERVAL` | 1,000,000 | Log progress every N rows |

### Sequence Column (`seq`)
seq provides a stable, monotonic intra-day ordering key that guarantees deterministic replay even when multiple events share the same ts_event value.

### Scheduling (02:30 UTC)
The job is scheduled at **02:30 UTC** for the following reasons:
- **Collector Alignment**: Ensures no collision with the current day's collection process.
- **Day Boundary**: Ensures the previous day is fully closed and all files are flushed to S3.
- **Minimal Risk**: Eliminates partial-day risks while providing fresh compacted data for early-day analysis.
- **Deterministic Context**: Consciously chosen to provide a stable operating window after global exchange activities settle for the specific UTC day.

### Fresh Start Behavior
If no `_state.json` is found (e.g., a cold deployment), the worker will only process **yesterday (UTC)**. After state is created, later runs automatically catch up all missing days in order.

## Installation

1. Copy systemd units:
   ```bash
   sudo cp compressor/quantlab-compact.service /etc/systemd/system/
   sudo cp compressor/quantlab-compact.timer /etc/systemd/system/
   ```
2. Reload and enable:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable quantlab-compact.timer
   sudo systemctl start quantlab-compact.timer
   ```

## Operational Guardrail: Swap

While the streaming merge is designed to stay within 2-3 GB RAM, adding swap provides an extra safety net:

```bash
# Create 8GB swap (one-time)
sudo fallocate -l 8G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# Make permanent
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

> **Note**: Swap is NOT required for correctness - the streaming merge algorithm is designed to work without it.

## CLI Reference

```bash
# Quality Report (Last 14 Days)
python3 run.py --quality-report

# Catch-up Mode (Default: all missing since state)
python3 run.py --mode catch-up --workers 1

# Daily Mode (Yesterday only)
python3 run.py --mode daily --workers 1

# Backfill Mode (Specific range)
python3 run.py --mode backfill --date-from 20260101 --date-to 20260107

# Overwrite existing data
python3 run.py --mode backfill --date-from 20260101 --overwrite
```

## Production Runbook

### Monitoring
1. **Logs**: Check systemd logs for daily status.
   ```bash
   journalctl -u quantlab-compact.service -n 50 --no-pager
   ```
2. **State**: Inspect `s3://quantlab-compact/compacted/_state.json` for partition status.
3. **Quality**: Use `python3 run.py --quality-report` to check for data issues.

### Failure Resolution
- **OOM**: If the process crashes with OOM, check if `MAX_OPEN_FILES` in `merge_writer.py` needs to be lowered (default 500).
- **Thrift Errors**: Occasionally raw files are corrupted. Skip these days or investigate the collector source.
- **S3 Auth**: Verify credentials in `/opt/quantlab/.env`.
- **Re-running a day**: If a day was compacted but needs a fix, use `--overwrite`.
- **Same endpoint policy**: `S3_COMPACT_ENDPOINT` must match `S3_ENDPOINT` on this deployment.
- **Download concurrency**: Tune `COMPRESSOR_MAX_PARALLEL_DOWNLOADS` (default `8`) if the VPS needs lower raw-download pressure.
- **Temp workspace**: Point `COMPRESSOR_TMPDIR` or `TMPDIR` to a roomy filesystem such as `/var/tmp/quantlab-compressor`.

### Scheduling
The job runs via `quantlab-compact.timer` at **02:30 UTC** (05:30 Istanbul).
- Check timer status: `systemctl status quantlab-compact.timer`
- List next runs: `systemctl list-timers`
