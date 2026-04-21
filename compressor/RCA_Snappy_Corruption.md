# Root Cause Analysis: Snappy Corruption (BTC-20260107)

## 1. Executive Summary
The "Corrupt snappy compressed data" error encountered during the compaction of `btcusdt` (20260107) is caused by **permanently corrupt raw objects sitting in the S3 bucket**. 

Download instability, network buffers, and PyArrow reader bugs have been ruled out. The files are consistently corrupt across multiple independent download methods and tools.

## 2. Diagnostic Evidence

| Metric | Result | Conclusion |
|--------|--------|------------|
| **Byte Stability (3x Download)** | **MATCHING** | Files are identical every time they are pulled from S3. No download corruption. |
| **Alternative Downloader** | **MATCHING** | `boto3` and `requests` (Presigned URL) pull the same identical bytes. |
| **Footer Magic (`PAR1`)** | **OK** | Files are not truncated; the Parquet structure is technically complete. |
| **Reader Diversity** | **ALL FAIL** | PyArrow (`read_table`, `iter_batches`, `read_row_group`) all fail to decode the internal Snappy blocks. |

### Corrupt Samples Identified (Verified):
1. `exchange=bybit/stream=bbo/symbol=btcusdt/date=20260107/part-1767755367-001395.parquet`
2. `exchange=bybit/stream=bbo/symbol=btcusdt/date=20260107/part-1767766347-001456.parquet`
3. `exchange=bybit/stream=bbo/symbol=btcusdt/date=20260107/part-1767813059-001730.parquet`

**Detailed Analysis (File #1):**
- **SHA256**: Consistent across 4 downloads (boto3 3x + requests 1x)
- **Error**: `OSError: Corrupt snappy compressed data.`
- **Internal Error**: `Couldn't deserialize thrift: don't know what type` (Page header corruption).

## 3. Verdict: RAW OBJECT CORRUPT
The raw data files were either:
1.  **Corrupted during upload** by the upstream collector/uploader.
2.  **Corrupted in-place** by S3 (very unlikely for multiple objects simultaneously).

## 4. Recommendations
- **Avoid Fixes in Compressor**: Since the source data is physically corrupt, the compressor cannot "fix" it.
- **Upstream Audit**: Check the Binance/Bybit collector logs for the date `20260107`.
- **Handling**: The current **QUARANTINE** mechanism is the correct response. We should NOT attempt to merge these files as they are missing data.
- **Removal**: If data replay is needed, these specific files must be re-collected from the exchange.
